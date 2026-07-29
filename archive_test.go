package datastore

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func archiveCount(t *testing.T, dir string) int {
	t.Helper()
	seqs, err := listArchives(dir)
	if err != nil {
		t.Fatalf("listArchives: %v", err)
	}
	return len(seqs)
}

// With archiving off (the default) compaction still empties the log and leaves
// no files behind.
func TestNoArchivingByDefault(t *testing.T) {
	f := newFixture(t)
	if err := f.orders.Put(mkOrder("a")); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Compact(); err != nil {
		t.Fatal(err)
	}
	if n := archiveCount(t, f.dir); n != 0 {
		t.Fatalf("archiving is off, yet %d segments exist", n)
	}
	if raw := readLog(t, f.dir); len(raw) != walHeaderSize {
		t.Fatalf("log should be empty after compaction, got %d bytes", len(raw))
	}
	if s := f.db.Stats(); s.ArchiveSegments != 0 || s.ArchiveBytes != 0 {
		t.Fatalf("stats should report no archives: %+v", s)
	}
}

// Each compaction sets the log aside, and the fresh one starts empty.
func TestCompactionArchivesTheLog(t *testing.T) {
	f := newFixture(t, func(o *Options) { o.HistorySegments = 10 })

	if err := f.orders.Put(mkOrder("a")); err != nil {
		t.Fatal(err)
	}
	if n := archiveCount(t, f.dir); n != 0 {
		t.Fatalf("no compaction yet, but %d segments exist", n)
	}

	if err := f.db.Compact(); err != nil {
		t.Fatal(err)
	}
	if n := archiveCount(t, f.dir); n != 1 {
		t.Fatalf("after one compaction want 1 segment, got %d", n)
	}
	if raw := readLog(t, f.dir); len(raw) != walHeaderSize {
		t.Fatalf("the fresh log should be empty, got %d bytes", len(raw))
	}

	// The database keeps working through the swap.
	if err := f.orders.Put(mkOrder("b")); err != nil {
		t.Fatalf("write after archiving: %v", err)
	}
	if err := f.db.Compact(); err != nil {
		t.Fatal(err)
	}
	if n := archiveCount(t, f.dir); n != 2 {
		t.Fatalf("after two compactions want 2 segments, got %d", n)
	}

	s := f.db.Stats()
	if s.ArchiveSegments != 2 || s.ArchiveBytes <= 0 {
		t.Fatalf("stats should describe the archives: %+v", s)
	}
}

// The archived frames are already in the snapshot, so recovery must ignore them.
// If they were replayed, deleted records would come back from the dead.
func TestArchivesAreNotReplayed(t *testing.T) {
	dir := t.TempDir()
	f := openFixture(t, dir, func(o *Options) { o.HistorySegments = 10 })

	if err := f.orders.Put(mkOrder("gone")); err != nil {
		t.Fatal(err)
	}
	if err := f.orders.Delete("gone"); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Compact(); err != nil { // archive now holds put+delete
		t.Fatal(err)
	}
	if err := f.orders.Put(mkOrder("kept")); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}

	f2 := openFixture(t, dir, func(o *Options) { o.HistorySegments = 10 })
	if _, ok := f2.orders.Get("gone"); ok {
		t.Fatal("a deleted record came back: archives were replayed")
	}
	if _, ok := f2.orders.Get("kept"); !ok {
		t.Fatal("the surviving record is missing")
	}
	if n := f2.orders.Len(); n != 1 {
		t.Fatalf("expected exactly one record, got %d", n)
	}
}

// Retention by count keeps the newest segments and drops the rest.
func TestArchiveRetentionByCount(t *testing.T) {
	f := newFixture(t, func(o *Options) { o.HistorySegments = 2 })

	for i := range 5 {
		if err := f.orders.Put(mkOrder(string(rune('a' + i)))); err != nil {
			t.Fatal(err)
		}
		if err := f.db.Compact(); err != nil {
			t.Fatal(err)
		}
	}

	seqs, err := listArchives(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(seqs) != 2 {
		t.Fatalf("retention should keep 2 segments, got %d (%v)", len(seqs), seqs)
	}
	// Kept segments must be the newest two.
	if seqs[0] >= seqs[1] {
		t.Fatalf("archives should be listed oldest first, got %v", seqs)
	}
	if seqs[1] < 4 {
		t.Fatalf("the newest segments should have been kept, got %v", seqs)
	}
}

// Retention by age drops segments older than the window.
func TestArchiveRetentionByAge(t *testing.T) {
	f := newFixture(t, func(o *Options) { o.HistoryFor = time.Hour })

	if err := f.orders.Put(mkOrder("old")); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Compact(); err != nil {
		t.Fatal(err)
	}
	if n := archiveCount(t, f.dir); n != 1 {
		t.Fatalf("want 1 segment, got %d", n)
	}

	// Backdate the segment well past the window.
	seqs, _ := listArchives(f.dir)
	path := filepath.Join(f.dir, archiveName(seqs[0]))
	old := time.Now().Add(-3 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}

	if err := f.orders.Put(mkOrder("new")); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Compact(); err != nil { // prunes on the way through
		t.Fatal(err)
	}

	seqs, _ = listArchives(f.dir)
	if len(seqs) != 1 {
		t.Fatalf("the aged-out segment should be gone, leaving 1, got %d (%v)", len(seqs), seqs)
	}
}
