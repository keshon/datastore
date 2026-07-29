package datastore

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// readLog returns the raw bytes of the current log, for tests that care about
// what actually reached the disk.
func readLog(t *testing.T, dir string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, walName))
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	return raw
}

func TestNoteIsRecordedWithTheCommit(t *testing.T) {
	f := newFixture(t)

	err := f.db.Update(func(tx *Tx) error {
		tx.Note("actor", "user:7")
		tx.Note("reason", "refund")
		return In(tx, f.orders).Put(mkOrder("a"))
	})
	if err != nil {
		t.Fatal(err)
	}

	raw := readLog(t, f.dir)
	for _, want := range []string{`"meta"`, `"actor"`, `"user:7"`, `"reason"`, `"refund"`} {
		if !bytes.Contains(raw, []byte(want)) {
			t.Fatalf("log should carry %s; got:\n%s", want, raw)
		}
	}
}

// The last write of a key wins, and notes describe the commit as a whole.
func TestNoteOverwritesSameKey(t *testing.T) {
	f := newFixture(t)

	err := f.db.Update(func(tx *Tx) error {
		tx.Note("actor", "first")
		tx.Note("actor", "second")
		return In(tx, f.orders).Put(mkOrder("a"))
	})
	if err != nil {
		t.Fatal(err)
	}

	raw := readLog(t, f.dir)
	if bytes.Contains(raw, []byte(`"first"`)) {
		t.Fatalf("the replaced note should not reach the log:\n%s", raw)
	}
	if !bytes.Contains(raw, []byte(`"second"`)) {
		t.Fatalf("the final note is missing:\n%s", raw)
	}
}

// A transaction that rolls back writes nothing at all, annotations included.
func TestNoteOnRollbackRecordsNothing(t *testing.T) {
	f := newFixture(t)
	before := len(readLog(t, f.dir))

	err := f.db.Update(func(tx *Tx) error {
		tx.Note("actor", "ghost")
		if err := In(tx, f.orders).Put(mkOrder("never")); err != nil {
			return err
		}
		return errRollback
	})
	if err != errRollback {
		t.Fatalf("Update = %v, want the rollback error", err)
	}

	raw := readLog(t, f.dir)
	if len(raw) != before {
		t.Fatalf("a rolled-back transaction wrote %d bytes", len(raw)-before)
	}
	if bytes.Contains(raw, []byte("ghost")) {
		t.Fatalf("rolled-back notes reached the log:\n%s", raw)
	}
}

// An unannotated write must be byte-identical to what it was before
// annotations existed: no empty "meta" key in the frame.
func TestUnannotatedFrameHasNoMeta(t *testing.T) {
	f := newFixture(t)
	if err := f.orders.Put(mkOrder("a")); err != nil {
		t.Fatal(err)
	}
	if raw := readLog(t, f.dir); bytes.Contains(raw, []byte(`"meta"`)) {
		t.Fatalf("unannotated frame should omit meta entirely:\n%s", raw)
	}
}

// Note on a read-only transaction is meaningless and must not panic.
func TestNoteInViewIsIgnored(t *testing.T) {
	f := newFixture(t)
	err := f.db.View(func(tx *Tx) error {
		tx.Note("actor", "reader")
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
}
