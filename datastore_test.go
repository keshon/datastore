package datastore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type order struct {
	ID       string   `json:"id"`
	Customer string   `json:"customer"`
	Status   string   `json:"status"`
	Email    string   `json:"email"`
	Created  int64    `json:"created"`
	Total    int      `json:"total"`
	Tags     []string `json:"tags,omitempty"`
}

func (o *order) Key() string { return o.ID }

type fixture struct {
	db       *DB
	orders   *Collection[*order]
	byStatus *Index[*order]
	byTag    *Index[*order]
	byEmail  *UniqueIndex[*order]
	byDate   *SortedIndex[*order]
	dir      string
}

func newFixture(t *testing.T, opts ...func(*Options)) *fixture {
	t.Helper()
	dir := t.TempDir()
	return openFixture(t, dir, opts...)
}

func openFixture(t *testing.T, dir string, opts ...func(*Options)) *fixture {
	t.Helper()

	o := Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1}
	for _, fn := range opts {
		fn(&o)
	}

	db := New(o)
	f := &fixture{db: db, dir: dir}
	f.orders = Register[*order](db, "orders")
	f.byStatus = AddIndex(f.orders, "status", func(o *order) []string { return []string{o.Status} })
	f.byTag = AddIndex(f.orders, "tag", func(o *order) []string { return o.Tags })
	f.byEmail = AddUnique(f.orders, "email", func(o *order) string { return o.Email })
	f.byDate = AddSorted(f.orders, "date", func(o *order) int64 { return o.Created })

	if err := db.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return f
}

func mkOrder(id string, rest ...func(*order)) *order {
	o := &order{ID: id, Customer: "c1", Status: "new", Created: 1000, Total: 100}
	for _, fn := range rest {
		fn(o)
	}
	return o
}

func mustPut(t *testing.T, c *Collection[*order], o *order) {
	t.Helper()
	if err := c.Put(o); err != nil {
		t.Fatalf("Put(%s): %v", o.ID, err)
	}
}

// release drops this process's hold on a database without the orderly shutdown
// Close performs. It is how the tests simulate a process that simply stopped
// existing: no final snapshot, no compaction, just the log exactly as the last
// commit left it.
//
// Recovery from that state is the whole point of the package, so most durability
// tests want this rather than Close.
func release(t *testing.T, db *DB) {
	t.Helper()
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.wal != nil {
		if err := db.wal.close(); err != nil {
			t.Fatalf("release wal: %v", err)
		}
		db.wal = nil
	}
	if db.lock != nil {
		if err := db.lock.release(); err != nil {
			t.Fatalf("release lock: %v", err)
		}
		db.lock = nil
	}
	db.opened = false
	db.closed = true // stop the registered Cleanup from doing anything
}

// --- basics ---------------------------------------------------------------

func TestPutGetDelete(t *testing.T) {
	f := newFixture(t)

	mustPut(t, f.orders, mkOrder("1", func(o *order) { o.Total = 250 }))

	got, ok := f.orders.Get("1")
	if !ok {
		t.Fatal("Get: not found after Put")
	}
	if got.Total != 250 {
		t.Errorf("Total = %d, want 250", got.Total)
	}
	if f.orders.Len() != 1 {
		t.Errorf("Len = %d, want 1", f.orders.Len())
	}

	if err := f.orders.Delete("1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := f.orders.Get("1"); ok {
		t.Error("Get: still found after Delete")
	}
	if err := f.orders.Delete("nonexistent"); err != nil {
		t.Errorf("Delete of an absent key = %v, want nil", err)
	}
}

// Get must hand back a private copy. If it returned the stored pointer, a
// caller mutating it would change the data without the log or the indexes ever
// hearing about it — the exact failure this package is built to make impossible.
func TestGetReturnsACopy(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, mkOrder("1", func(o *order) { o.Status = "new" }))

	first, _ := f.orders.Get("1")
	first.Status = "tampered"
	first.Tags = append(first.Tags, "tampered")

	second, _ := f.orders.Get("1")
	if second.Status != "new" {
		t.Errorf("Status = %q, want %q — Get leaked a shared pointer", second.Status, "new")
	}
	if len(second.Tags) != 0 {
		t.Errorf("Tags = %v, want empty — Get leaked a shared pointer", second.Tags)
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	f := newFixture(t)
	err := f.orders.Put(&order{ID: ""})
	if !errors.Is(err, ErrEmptyKey) {
		t.Errorf("Put with an empty key = %v, want ErrEmptyKey", err)
	}
}

func TestAllIteratesInKeyOrder(t *testing.T) {
	f := newFixture(t)
	for _, id := range []string{"c", "a", "b"} {
		mustPut(t, f.orders, mkOrder(id))
	}
	var seen []string
	for o := range f.orders.All() {
		seen = append(seen, o.ID)
	}
	if strings.Join(seen, ",") != "a,b,c" {
		t.Errorf("All order = %v, want [a b c]", seen)
	}
}

// --- durability -----------------------------------------------------------

func TestStateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	f := openFixture(t, dir)
	for i := 1; i <= 5; i++ {
		mustPut(t, f.orders, mkOrder(fmt.Sprint(i), func(o *order) { o.Total = i * 10 }))
	}
	if err := f.db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	g := openFixture(t, dir)
	if g.orders.Len() != 5 {
		t.Fatalf("after reopen Len = %d, want 5", g.orders.Len())
	}
	got, ok := g.orders.Get("3")
	if !ok || got.Total != 30 {
		t.Errorf("after reopen order 3 = %+v, want Total 30", got)
	}
}

// Closing writes a snapshot, which is the easy path. The interesting one is a
// process that never got to close: state must come back from the log alone.
func TestStateSurvivesWithoutClose(t *testing.T) {
	dir := t.TempDir()

	db := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	orders := Register[*order](db, "orders")
	if err := db.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 1; i <= 3; i++ {
		mustPut(t, orders, mkOrder(fmt.Sprint(i)))
	}
	release(t, db) // no snapshot: recovery has to read the log

	g := openFixture(t, dir)
	if g.orders.Len() != 3 {
		t.Errorf("recovered %d orders from the log, want 3", g.orders.Len())
	}
}

func TestCompactionPreservesEverything(t *testing.T) {
	f := newFixture(t)
	for i := 1; i <= 10; i++ {
		mustPut(t, f.orders, mkOrder(fmt.Sprint(i)))
	}
	if err := f.orders.Delete("5"); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if got := f.db.Stats().WALBytes; got != int64(walHeaderSize) {
		t.Errorf("WAL is %d bytes after compaction, want %d", got, walHeaderSize)
	}
	if f.orders.Len() != 9 {
		t.Errorf("Len = %d after compaction, want 9", f.orders.Len())
	}
	if _, ok := f.orders.Get("5"); ok {
		t.Error("a deleted record came back through compaction")
	}
}

func TestCompactionKeepsAPreviousSnapshot(t *testing.T) {
	f := newFixture(t)
	for i := 0; i < 4; i++ {
		mustPut(t, f.orders, mkOrder(fmt.Sprint(i)))
		if err := f.db.Compact(); err != nil {
			t.Fatal(err)
		}
	}
	seqs, err := listSnapshots(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	// Keeping the previous one is what makes a crash between installing a
	// snapshot and truncating the log survivable.
	if len(seqs) != 2 {
		t.Errorf("kept %d snapshots (%v), want 2", len(seqs), seqs)
	}
}

// A collection that this build does not register must survive untouched.
// Otherwise commenting out one Register call and deploying would delete a
// table at the next compaction, with no error and no way back.
func TestUnregisteredCollectionSurvivesCompaction(t *testing.T) {
	dir := t.TempDir()

	full := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	orders := Register[*order](full, "orders")
	archive := Register[*order](full, "archive")
	if err := full.Open(); err != nil {
		t.Fatal(err)
	}
	mustPut(t, orders, mkOrder("1"))
	mustPut(t, archive, mkOrder("old-1"))
	if err := full.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen without registering "archive", write, and compact.
	partial := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	orders2 := Register[*order](partial, "orders")
	if err := partial.Open(); err != nil {
		t.Fatal(err)
	}
	mustPut(t, orders2, mkOrder("2"))
	if got := partial.Stats().Orphans["archive"]; got != 1 {
		t.Errorf("orphan count = %d, want 1", got)
	}
	if err := partial.Close(); err != nil { // Close compacts
		t.Fatal(err)
	}

	// Register it again: the data must still be there.
	back := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	_ = Register[*order](back, "orders")
	archive2 := Register[*order](back, "archive")
	if err := back.Open(); err != nil {
		t.Fatal(err)
	}
	defer back.Close()

	if _, ok := archive2.Get("old-1"); !ok {
		t.Error("an unregistered collection was destroyed by compaction")
	}
}

func TestBackupIsARestorableDirectory(t *testing.T) {
	f := newFixture(t)
	for i := 1; i <= 3; i++ {
		mustPut(t, f.orders, mkOrder(fmt.Sprint(i)))
	}

	dst := filepath.Join(t.TempDir(), "backup")
	if err := f.db.Backup(dst); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	restored := openFixture(t, dst)
	if restored.orders.Len() != 3 {
		t.Errorf("restored %d orders, want 3", restored.orders.Len())
	}
}

// --- locking --------------------------------------------------------------

func TestSecondOpenIsRefused(t *testing.T) {
	f := newFixture(t)

	second := New(Options{Dir: f.dir})
	_ = Register[*order](second, "orders")
	err := second.Open()
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open = %v, want ErrLocked", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprint(os.Getpid())) {
		t.Errorf("error should name the holding pid, got: %v", err)
	}
}

func TestRegisterAfterOpenPanics(t *testing.T) {
	f := newFixture(t)
	defer func() {
		if recover() == nil {
			t.Error("Register after Open should panic")
		}
	}()
	_ = Register[*order](f.db, "late")
}

func TestReentrantUpdatePanics(t *testing.T) {
	f := newFixture(t, func(o *Options) { o.WriteTimeout = 150 * time.Millisecond })

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("a nested Update should panic rather than deadlock")
		}
		if !strings.Contains(fmt.Sprint(r), "inside another Update") {
			t.Errorf("panic should explain the cause, got: %v", r)
		}
	}()

	_ = f.db.Update(func(tx *Tx) error {
		return f.db.Update(func(*Tx) error { return nil })
	})
}
