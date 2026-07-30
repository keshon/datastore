package datastore

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// These tests pin down the hardening pass: no data races between Stats and
// commits, no deadlock reading inside View, events that do not alias live
// state, a WAL that refuses to destroy foreign files, and a store that stops
// accepting writes once a log write has failed.

// TestStatsDuringWrites runs Stats concurrently with commits. Under -race this
// used to trip on wal.size, which append mutated outside the state lock while
// Stats read it under RLock.
func TestStatsDuringWrites(t *testing.T) {
	f := newFixture(t, func(o *Options) { o.Sync = SyncNever })

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = f.db.Stats()
			}
		}
	}()

	for i := 0; i < 200; i++ {
		mustPut(t, f.orders, mkOrder(strconv.Itoa(i)))
	}
	close(stop)
	wg.Wait()
}

// TestReadInsideViewDoesNotDeadlock is the regression test for the recursive
// read-lock trap: View holds the read lock for its whole callback, and any
// direct Collection or index method takes it again — which deadlocks the
// moment a writer is queued between the two acquisitions. Reading through the
// transaction (In, InIndex, InUnique, InSorted) must stay safe even with a
// writer waiting.
func TestReadInsideViewDoesNotDeadlock(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, mkOrder("1", func(o *order) { o.Email = "a@example.com" }))
	mustPut(t, f.orders, mkOrder("2"))

	inView := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		<-inView
		writerDone <- f.db.Update(func(tx *Tx) error {
			return In(tx, f.orders).Put(mkOrder("3"))
		})
	}()

	viewDone := make(chan error, 1)
	go func() {
		viewDone <- f.db.View(func(tx *Tx) error {
			close(inView)
			// Give the writer time to reach its state-lock acquisition, so a
			// recursive RLock here would actually deadlock rather than sneak
			// through before the writer queues.
			time.Sleep(100 * time.Millisecond)

			o := In(tx, f.orders)
			if _, ok := o.Get("1"); !ok {
				t.Error("View: order 1 missing")
			}
			if got := len(o.Keys()); got != 2 {
				t.Errorf("View: Keys = %d, want 2", got)
			}
			if got := o.Len(); got != 2 {
				t.Errorf("View: Len = %d, want 2", got)
			}
			n := 0
			for range o.All() {
				n++
			}
			if n != 2 {
				t.Errorf("View: All yielded %d, want 2", n)
			}
			if got := len(InIndex(tx, f.byStatus).Find("new")); got != 2 {
				t.Errorf("View: byStatus = %d, want 2", got)
			}
			if _, ok := InUnique(tx, f.byEmail).Get("a@example.com"); !ok {
				t.Error("View: byEmail miss")
			}
			if got := InSorted(tx, f.byDate).Len(); got != 2 {
				t.Errorf("View: byDate.Len = %d, want 2", got)
			}
			return nil
		})
	}()

	select {
	case err := <-viewDone:
		if err != nil {
			t.Fatalf("View: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("View deadlocked against a queued writer")
	}
	if err := <-writerDone; err != nil {
		t.Fatalf("Update: %v", err)
	}
}

// TestEventsDoNotAliasState mutates a delivered event's Value and Meta and
// checks that neither the stored record nor another subscriber's copy changes.
func TestEventsDoNotAliasState(t *testing.T) {
	f := newFixture(t)
	sub1 := f.db.Watch(WatchOptions{})
	defer sub1.Close()
	sub2 := f.db.Watch(WatchOptions{})
	defer sub2.Close()

	err := f.db.Update(func(tx *Tx) error {
		tx.Note("actor", "user:7")
		return In(tx, f.orders).Put(mkOrder("1", func(o *order) { o.Total = 4200 }))
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	ev1 := <-sub1.Events()
	for i := range ev1.Value {
		ev1.Value[i] = 'X'
	}
	ev1.Meta["actor"] = "vandal"

	got, ok := f.orders.Get("1")
	if !ok || got.Total != 4200 {
		t.Fatalf("stored record corrupted by subscriber: %+v ok=%v", got, ok)
	}
	ev2 := <-sub2.Events()
	if !json.Valid(ev2.Value) {
		t.Fatalf("second subscriber's Value corrupted: %q", ev2.Value)
	}
	if ev2.Meta["actor"] != "user:7" {
		t.Fatalf("second subscriber's Meta corrupted: %v", ev2.Meta)
	}
}

// TestOpenRefusesForeignShortWAL: a wal.log shorter than the header used to be
// rewritten — beginning with Truncate(0) — before the magic was ever checked,
// destroying whatever file happened to be there. It must be an error, and the
// file must survive. A torn header that is a prefix of our own is still ours
// and is repaired.
func TestOpenRefusesForeignShortWAL(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, walName), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}

	db := New(Options{Dir: dir})
	Register[*order](db, "orders")
	err := db.Open()
	if err == nil {
		db.Close()
		t.Fatal("Open succeeded on a foreign wal.log")
	}
	if !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("Open = %v, want ErrInvalidLog", err)
	}
	b, err := os.ReadFile(filepath.Join(dir, walName))
	if err != nil || string(b) != "hi" {
		t.Fatalf("foreign file was modified: %q, %v", b, err)
	}

	// A prefix of our own header is a crash artifact, not a foreign file.
	dir2 := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir2, walName), headerBytes()[:3], 0o644); err != nil {
		t.Fatal(err)
	}
	db2 := New(Options{Dir: dir2})
	Register[*order](db2, "orders")
	if err := db2.Open(); err != nil {
		t.Fatalf("Open on a torn header: %v", err)
	}
	db2.Close()
}

// TestOpenSentinels: a full-size foreign file is ErrInvalidLog, and a log
// written by a newer format version is ErrLogVersion — both machine-checkable,
// because "wrong directory" and "upgrade the binary" deserve different
// handling.
func TestOpenSentinels(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, walName), []byte("GARBAGE-NOT-A-LOG"), 0o644); err != nil {
		t.Fatal(err)
	}
	db := New(Options{Dir: dir})
	Register[*order](db, "orders")
	if err := db.Open(); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("Open on garbage = %v, want ErrInvalidLog", err)
	}

	dir2 := t.TempDir()
	future := headerBytes()
	future[len(walMagic)] = 99 // version 99, little-endian low byte
	if err := os.WriteFile(filepath.Join(dir2, walName), future, 0o644); err != nil {
		t.Fatal(err)
	}
	db2 := New(Options{Dir: dir2})
	Register[*order](db2, "orders")
	if err := db2.Open(); !errors.Is(err, ErrLogVersion) {
		t.Fatalf("Open on a future log = %v, want ErrLogVersion", err)
	}
}

// TestUndecodableRecordIsLogged: a record that fails to decode reads as absent
// — that part is old behaviour — but it must leave a loud log line rather than
// vanish silently from reads and index results.
func TestUndecodableRecordIsLogged(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	f := newFixture(t, func(o *Options) { o.Logger = &logger })
	mustPut(t, f.orders, mkOrder("1"))

	f.db.mu.Lock()
	f.orders.data["1"] = json.RawMessage(`{broken`)
	f.db.mu.Unlock()

	if _, ok := f.orders.Get("1"); ok {
		t.Fatal("undecodable record should read as absent")
	}
	if !strings.Contains(buf.String(), "record_undecodable") {
		t.Fatalf("expected a record_undecodable log line, got: %q", buf.String())
	}
}

// TestUpdateTimeoutReturnsError: a held writer slot is a runtime condition — a
// slow disk, a long compaction — and must surface as ErrWriteTimeout, not kill
// the embedding process with a panic.
func TestUpdateTimeoutReturnsError(t *testing.T) {
	f := newFixture(t, func(o *Options) { o.WriteTimeout = 100 * time.Millisecond })

	started := make(chan struct{})
	hold := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- f.db.Update(func(tx *Tx) error {
			close(started)
			<-hold
			return nil
		})
	}()
	<-started

	err := f.db.Update(func(tx *Tx) error { return nil })
	if !errors.Is(err, ErrWriteTimeout) {
		t.Fatalf("Update = %v, want ErrWriteTimeout", err)
	}

	close(hold)
	if err := <-done; err != nil {
		t.Fatalf("holder Update: %v", err)
	}
}

// TestWALWriteFailureLatches: after a failed append the log may hold a torn
// frame; any commit written after it would be silently discarded by the next
// replay. The store must refuse further writes rather than keep acknowledging
// doomed commits — and reopening must recover everything up to the failure.
func TestWALWriteFailureLatches(t *testing.T) {
	dir := t.TempDir()
	f := openFixture(t, dir)
	mustPut(t, f.orders, mkOrder("1"))

	// Pull the file out from under the log: every append from here fails.
	f.db.wal.f.Close()

	err := f.orders.Put(mkOrder("2"))
	if !errors.Is(err, ErrFailed) {
		t.Fatalf("Put after wal failure = %v, want ErrFailed", err)
	}
	err = f.orders.Put(mkOrder("3"))
	if !errors.Is(err, ErrFailed) {
		t.Fatalf("second Put = %v, want ErrFailed (latched)", err)
	}

	_ = f.db.Close() // returns an error; the point is that it releases the lock

	g := openFixture(t, dir)
	if _, ok := g.orders.Get("1"); !ok {
		t.Fatal("order 1 lost across the failure")
	}
	if _, ok := g.orders.Get("2"); ok {
		t.Fatal("order 2 was never durable and must not reappear")
	}
}

// TestHistoryCorruptArchive: a damaged archive segment must surface as
// ErrCorruptFrame, not read as a shorter-but-complete audit trail.
func TestHistoryCorruptArchive(t *testing.T) {
	f := newFixture(t, func(o *Options) { o.HistorySegments = 4 })
	mustPut(t, f.orders, mkOrder("1"))
	mustPut(t, f.orders, mkOrder("2"))
	if err := f.db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	archives, err := filepath.Glob(filepath.Join(f.dir, archiveGlob))
	if err != nil || len(archives) == 0 {
		t.Fatalf("no archive after compaction: %v", err)
	}
	b, err := os.ReadFile(archives[0])
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0xFF // flip one byte mid-frame
	if err := os.WriteFile(archives[0], b, 0o644); err != nil {
		t.Fatal(err)
	}

	err = f.db.History(0, 0, func(HistoryEntry) error { return nil })
	if !errors.Is(err, ErrCorruptFrame) {
		t.Fatalf("History over a corrupt archive = %v, want ErrCorruptFrame", err)
	}
}

// TestRecoveryReportIsACopy: the report's maps must not alias live DB state.
func TestRecoveryReportIsACopy(t *testing.T) {
	dir := t.TempDir()
	db1 := New(Options{Dir: dir, Sync: SyncNever, CompactInterval: CompactNever})
	w1 := Register[*widgetRec](db1, "widgets", SchemaVersion(1))
	if err := db1.Open(); err != nil {
		t.Fatal(err)
	}
	if err := w1.Put(&widgetRec{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := New(Options{Dir: dir, Sync: SyncNever, CompactInterval: CompactNever})
	w2 := Register[*widgetRec](db2, "widgets", SchemaVersion(2))
	Migrate(w2, 1, func(raw json.RawMessage) (json.RawMessage, error) { return raw, nil })
	if err := db2.Open(); err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	rep := db2.Recovery()
	if rep.Migrated["widgets"] != 1 {
		t.Fatalf("Migrated = %v, want widgets:1", rep.Migrated)
	}
	rep.Migrated["widgets"] = 99
	if again := db2.Recovery(); again.Migrated["widgets"] != 1 {
		t.Fatalf("mutating the report changed the DB's copy: %v", again.Migrated)
	}
}

type widgetRec struct {
	ID string `json:"id"`
}

func (w *widgetRec) Key() string { return w.ID }

// TestTxCollectionSeesOwnWrites: Keys, Len and All inside a transaction layer
// the transaction's staged writes over the committed state.
func TestTxCollectionSeesOwnWrites(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, mkOrder("1"))
	mustPut(t, f.orders, mkOrder("2"))

	err := f.db.Update(func(tx *Tx) error {
		o := In(tx, f.orders)
		if err := o.Put(mkOrder("3")); err != nil {
			return err
		}
		if err := o.Delete("1"); err != nil {
			return err
		}
		keys := o.Keys()
		if len(keys) != 2 || keys[0] != "2" || keys[1] != "3" {
			t.Errorf("Keys = %v, want [2 3]", keys)
		}
		if o.Len() != 2 {
			t.Errorf("Len = %d, want 2", o.Len())
		}
		var seen []string
		for v := range o.All() {
			seen = append(seen, v.ID)
		}
		if len(seen) != 2 || seen[0] != "2" || seen[1] != "3" {
			t.Errorf("All = %v, want [2 3]", seen)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
}
