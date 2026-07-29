package datastore

import (
	"encoding/json"
	"errors"
	"testing"
)

func collectHistory(t *testing.T, db *DB, from, to uint64) []HistoryEntry {
	t.Helper()
	var out []HistoryEntry
	if err := db.History(from, to, func(e HistoryEntry) error {
		out = append(out, e)
		return nil
	}); err != nil {
		t.Fatalf("History: %v", err)
	}
	return out
}

// The whole point: read back who changed what, and why, after the fact — and
// across a compaction that would otherwise have thrown the log away.
func TestHistoryReadsAcrossArchiveBoundary(t *testing.T) {
	f := newFixture(t, func(o *Options) { o.HistorySegments = 10 })

	err := f.db.Update(func(tx *Tx) error {
		tx.Note("actor", "user:1")
		tx.Note("reason", "placed")
		return In(tx, f.orders).Put(mkOrder("a", func(o *order) { o.Total = 100 }))
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.db.Compact(); err != nil { // commit 1 now lives in an archive
		t.Fatal(err)
	}
	err = f.db.Update(func(tx *Tx) error {
		tx.Note("actor", "user:2")
		tx.Note("reason", "refunded")
		return In(tx, f.orders).Delete("a")
	})
	if err != nil {
		t.Fatal(err)
	}

	entries := collectHistory(t, f.db, 0, 0)
	if len(entries) != 2 {
		t.Fatalf("want 2 commits (one archived, one live), got %d", len(entries))
	}

	first, second := entries[0], entries[1]
	if first.Seq >= second.Seq {
		t.Fatalf("history must be oldest first, got seq %d then %d", first.Seq, second.Seq)
	}
	if first.Meta["actor"] != "user:1" || first.Meta["reason"] != "placed" {
		t.Fatalf("annotations lost on the archived commit: %+v", first.Meta)
	}
	if second.Meta["actor"] != "user:2" || second.Meta["reason"] != "refunded" {
		t.Fatalf("annotations lost on the live commit: %+v", second.Meta)
	}
	if first.Time.IsZero() {
		t.Fatal("commit time should be recorded")
	}

	// The archived op carries the value as it was written at the time.
	if len(first.Ops) != 1 || first.Ops[0].Delete {
		t.Fatalf("first commit should be one put: %+v", first.Ops)
	}
	var wrote order
	if err := json.Unmarshal(first.Ops[0].Value, &wrote); err != nil {
		t.Fatalf("decode historical value: %v", err)
	}
	if wrote.Total != 100 {
		t.Fatalf("historical value should be what was written, got %+v", wrote)
	}

	// ...and the delete is recorded as a delete, with no value.
	if len(second.Ops) != 1 || !second.Ops[0].Delete || second.Ops[0].Value != nil {
		t.Fatalf("second commit should be a valueless delete: %+v", second.Ops)
	}
	if second.Ops[0].Collection != "orders" || second.Ops[0].Key != "a" {
		t.Fatalf("op should name the collection and key: %+v", second.Ops[0])
	}
}

func TestHistoryRespectsBounds(t *testing.T) {
	f := newFixture(t, func(o *Options) { o.HistorySegments = 10 })
	for _, id := range []string{"a", "b", "c", "d"} {
		if err := f.orders.Put(mkOrder(id)); err != nil {
			t.Fatal(err)
		}
	}

	all := collectHistory(t, f.db, 0, 0)
	if len(all) != 4 {
		t.Fatalf("want 4 commits, got %d", len(all))
	}

	mid := collectHistory(t, f.db, 2, 3)
	if len(mid) != 2 || mid[0].Seq != 2 || mid[1].Seq != 3 {
		t.Fatalf("bounded scan should return seq 2..3, got %+v", mid)
	}

	tail := collectHistory(t, f.db, 3, 0)
	if len(tail) != 2 || tail[0].Seq != 3 {
		t.Fatalf("open-ended scan from 3 should return 3..4, got %+v", tail)
	}
}

// An error from the callback stops the scan and reaches the caller.
func TestHistoryEarlyStop(t *testing.T) {
	f := newFixture(t, func(o *Options) { o.HistorySegments = 10 })
	for _, id := range []string{"a", "b", "c"} {
		if err := f.orders.Put(mkOrder(id)); err != nil {
			t.Fatal(err)
		}
	}

	stop := errors.New("enough")
	seen := 0
	err := f.db.History(0, 0, func(HistoryEntry) error {
		seen++
		if seen == 2 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatalf("History = %v, want the caller's error", err)
	}
	if seen != 2 {
		t.Fatalf("scan should have stopped at 2 entries, saw %d", seen)
	}
}

// Without retention, history still works — it just reaches back only as far as
// the last compaction. Worth pinning so the limitation is understood.
func TestHistoryWithoutArchivingStopsAtCompaction(t *testing.T) {
	f := newFixture(t) // archiving off

	if err := f.orders.Put(mkOrder("forgotten")); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := f.orders.Put(mkOrder("remembered")); err != nil {
		t.Fatal(err)
	}

	entries := collectHistory(t, f.db, 0, 0)
	if len(entries) != 1 {
		t.Fatalf("only the post-compaction commit should remain, got %d", len(entries))
	}
	if entries[0].Ops[0].Key != "remembered" {
		t.Fatalf("wrong commit survived: %+v", entries[0].Ops)
	}
}

// A torn tail on the live log ends history there instead of failing the scan.
func TestHistoryToleratesTornTail(t *testing.T) {
	f := newFixture(t, func(o *Options) { o.HistorySegments = 10 })
	if err := f.orders.Put(mkOrder("good")); err != nil {
		t.Fatal(err)
	}

	before := collectHistory(t, f.db, 0, 0)
	if len(before) != 1 {
		t.Fatalf("baseline should have 1 entry, got %d", len(before))
	}

	// Append a half-written frame header, as a crash mid-append would leave.
	if _, err := f.db.wal.f.WriteAt([]byte{0xFF, 0xFF}, f.db.wal.size); err != nil {
		t.Fatal(err)
	}

	after := collectHistory(t, f.db, 0, 0)
	if len(after) != 1 {
		t.Fatalf("a torn tail should end history, not break it: got %d entries", len(after))
	}
}

func TestHistoryBeforeOpen(t *testing.T) {
	db := New(Options{Dir: t.TempDir()})
	if err := db.History(0, 0, func(HistoryEntry) error { return nil }); !errors.Is(err, ErrNotOpen) {
		t.Fatalf("History before Open = %v, want ErrNotOpen", err)
	}
}
