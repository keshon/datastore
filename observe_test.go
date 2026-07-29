package datastore

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

// A clean open reports clean, with the sequence it opened at.
func TestRecoveryReportOnCleanOpen(t *testing.T) {
	f := newFixture(t)
	if err := f.orders.Put(mkOrder("a")); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}

	f2 := openFixture(t, f.dir)
	rep := f2.db.Recovery()
	if !rep.Clean() || rep.LostData() {
		t.Fatalf("clean reopen should report clean: %+v", rep)
	}
	if rep.SnapshotRecords != 1 {
		t.Fatalf("expected the record to come from the snapshot, got %+v", rep)
	}
}

// The point of the report: a truncated tail must be visible to the application,
// not only in the log.
func TestRecoveryReportSurfacesTruncation(t *testing.T) {
	f := newFixture(t)
	for _, id := range []string{"a", "b"} {
		if err := f.orders.Put(mkOrder(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.db.Compact(); err != nil { // snapshot holds a and b
		t.Fatal(err)
	}
	if err := f.orders.Put(mkOrder("c")); err != nil { // c lives only in the log
		t.Fatal(err)
	}
	if err := f.db.wal.close(); err != nil {
		t.Fatal(err)
	}
	if err := f.db.lock.release(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the frame carrying c.
	path := filepath.Join(f.dir, walName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= walHeaderSize+frameHeaderSize {
		t.Fatalf("expected a frame in the log, size %d", len(raw))
	}
	raw[len(raw)-1] ^= 0xFF // break the checksum
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	f2 := openFixture(t, f.dir)
	rep := f2.db.Recovery()
	if !rep.Truncated {
		t.Fatalf("truncation should be reported: %+v", rep)
	}
	if !rep.LostData() || rep.Clean() {
		t.Fatalf("a truncated log is data loss: %+v", rep)
	}
	if rep.TruncatedReason == "" {
		t.Fatal("the report should say why the log stopped being trusted")
	}
	if _, ok := f2.orders.Get("c"); ok {
		t.Fatal("the record from the corrupt frame should be gone")
	}
	if _, ok := f2.orders.Get("b"); !ok {
		t.Fatal("records before the damage must survive")
	}
}

// OnCommit sees every commit, with a plausible summary.
func TestOnCommitHook(t *testing.T) {
	var mu sync.Mutex
	var seen []CommitStats

	f := newFixture(t, func(o *Options) {
		o.OnCommit = func(cs CommitStats) {
			mu.Lock()
			seen = append(seen, cs)
			mu.Unlock()
		}
	})
	if err := f.orders.Put(mkOrder("a")); err != nil {
		t.Fatal(err)
	}
	if err := f.orders.Put(mkOrder("b")); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("expected 2 commit callbacks, got %d", len(seen))
	}
	last := seen[len(seen)-1]
	if last.Ops != 1 || last.FrameBytes <= 0 || last.Seq == 0 || !last.Synced {
		t.Fatalf("commit stats look wrong: %+v", last)
	}
	if last.WALBytes < int64(last.FrameBytes) {
		t.Fatalf("wal should be at least one frame long: %+v", last)
	}
}

// Outgrowing the budget warns once, and Stats reports the numbers.
func TestMemoryBudgetWarns(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	f := newFixture(t, func(o *Options) {
		o.MaxBytes = 400
		o.WarnAtPercent = 50
		o.Logger = &logger
	})

	for i := range 20 {
		if err := f.orders.Put(mkOrder(string(rune('a' + i)))); err != nil {
			t.Fatal(err)
		}
	}

	if !bytes.Contains(buf.Bytes(), []byte("dataset_approaching_memory_budget")) {
		t.Fatalf("expected a budget warning, log was:\n%s", buf.String())
	}
	if n := bytes.Count(buf.Bytes(), []byte("dataset_approaching_memory_budget")); n != 1 {
		t.Fatalf("budget warning should be logged once per crossing, got %d", n)
	}

	s := f.db.Stats()
	if s.Budget != 400 || s.Bytes <= 0 {
		t.Fatalf("stats should carry the budget and current size: %+v", s)
	}
}

// With no budget configured, nothing is measured against it and nothing warns.
func TestNoBudgetNoWarning(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	f := newFixture(t, func(o *Options) { o.Logger = &logger })

	if err := f.orders.Put(mkOrder("a")); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(buf.Bytes(), []byte("memory_budget")) {
		t.Fatalf("no budget was set, so nothing should warn:\n%s", buf.String())
	}
	if s := f.db.Stats(); s.Budget != 0 {
		t.Fatalf("budget should be zero, got %d", s.Budget)
	}
}
