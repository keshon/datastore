package datastore

import (
	"errors"
	"testing"
)

// A reader must open a directory the owner still holds, see what was committed,
// and refuse every write.
func TestOpenReaderAlongsideOwner(t *testing.T) {
	dir := t.TempDir()

	owner := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	ownerOrders := Register[*order](owner, "orders")
	if err := owner.Open(); err != nil {
		t.Fatalf("owner open: %v", err)
	}
	defer owner.Close()

	if err := ownerOrders.Put(mkOrder("a")); err != nil {
		t.Fatal(err)
	}

	// The exclusive opener is refused...
	blocked := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	Register[*order](blocked, "orders")
	if err := blocked.Open(); !errors.Is(err, ErrLocked) {
		t.Fatalf("second exclusive open = %v, want ErrLocked", err)
	}

	// ...while the reader is not.
	reader := New(Options{Dir: dir, CompactInterval: -1})
	readerOrders := Register[*order](reader, "orders")
	if err := reader.OpenReader(); err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer reader.Close()

	if !reader.ReadOnly() {
		t.Fatal("ReadOnly() should be true")
	}
	if _, ok := readerOrders.Get("a"); !ok {
		t.Fatal("reader cannot see the committed record")
	}

	if err := readerOrders.Put(mkOrder("nope")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Put through a reader = %v, want ErrReadOnly", err)
	}
	if err := reader.Update(func(tx *Tx) error { return nil }); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Update on a reader = %v, want ErrReadOnly", err)
	}
	if err := reader.Compact(); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Compact on a reader = %v, want ErrReadOnly", err)
	}

	// Reads still work, and the owner is unaffected.
	if err := ownerOrders.Put(mkOrder("b")); err != nil {
		t.Fatalf("owner should still be writable: %v", err)
	}
}

// A reader's view is a point in time: it does not see later commits, and
// reopening picks them up.
func TestReaderSeesSnapshotUntilReopened(t *testing.T) {
	dir := t.TempDir()

	owner := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	oc := Register[*order](owner, "orders")
	if err := owner.Open(); err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if err := oc.Put(mkOrder("first")); err != nil {
		t.Fatal(err)
	}

	reader := New(Options{Dir: dir, CompactInterval: -1})
	rc := Register[*order](reader, "orders")
	if err := reader.OpenReader(); err != nil {
		t.Fatal(err)
	}

	if err := oc.Put(mkOrder("second")); err != nil {
		t.Fatal(err)
	}
	if _, ok := rc.Get("second"); ok {
		t.Fatal("reader should not see writes made after it opened")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	reader2 := New(Options{Dir: dir, CompactInterval: -1})
	rc2 := Register[*order](reader2, "orders")
	if err := reader2.OpenReader(); err != nil {
		t.Fatal(err)
	}
	defer reader2.Close()
	if _, ok := rc2.Get("second"); !ok {
		t.Fatal("a fresh reader should see everything committed so far")
	}
}

// Opening a directory that does not exist is an error, not an empty database:
// a reader pointed at a typo should say so.
func TestOpenReaderMissingDir(t *testing.T) {
	db := New(Options{Dir: t.TempDir() + "/nope"})
	Register[*order](db, "orders")
	if err := db.OpenReader(); err == nil {
		db.Close()
		t.Fatal("expected an error opening a missing directory")
	}
}
