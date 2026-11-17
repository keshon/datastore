package datastore

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSnapshotIOAllowsReadersAndExcludesWriters(t *testing.T) {
	for _, backup := range []bool{false, true} {
		t.Run(map[bool]string{false: "compact", true: "backup"}[backup], func(t *testing.T) {
			f := newFixture(t)
			mustPut(t, f.orders, &order{ID: "a"})
			entered := make(chan struct{})
			resume := make(chan struct{})
			f.db.saveSnapshot = func(dir string, snap *snapshotFile) error {
				close(entered)
				<-resume
				return writeSnapshot(dir, snap)
			}
			defer func() { f.db.saveSnapshot = writeSnapshot }()
			done := make(chan error, 1)
			dst := t.TempDir()
			go func() {
				if backup {
					done <- f.db.Backup(dst)
					return
				}
				done <- f.db.Compact()
			}()
			<-entered
			read := make(chan bool, 1)
			go func() {
				_, ok := f.orders.Get("a")
				read <- ok
			}()
			select {
			case ok := <-read:
				if !ok {
					t.Error("snapshot record missing")
				}
			case <-time.After(2 * time.Second):
				t.Error("snapshot I/O blocked a reader")
			}
			write := make(chan error, 1)
			go func() { write <- f.orders.Put(&order{ID: "b"}) }()
			select {
			case err := <-write:
				t.Errorf("writer crossed snapshot boundary: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			close(resume)
			err := <-done
			if err != nil {
				t.Fatal(err)
			}
			err = <-write
			if err != nil {
				t.Fatal(err)
			}
			if backup {
				copy := openFixture(t, dst)
				if copy.orders.Len() != 1 {
					t.Fatal("backup includes a write made after its boundary")
				}
			}
		})
	}
}

type failingSnapshotWriter struct {
	remaining int
	err       error
}

func (w *failingSnapshotWriter) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		n := w.remaining
		w.remaining = 0
		return n, w.err
	}
	w.remaining -= len(p)
	return len(p), nil
}

func TestStreamedSnapshotCompatibilityAndWriteErrors(t *testing.T) {
	keys := []string{"plain", "quote\"", "slash\\", "\x00\n", "<>&", "ключ", "\u2028", string([]byte{0xff})}
	rows := make(map[string]json.RawMessage)
	for _, key := range keys {
		rows[key] = json.RawMessage(` {"value":"<>&", "nested":[1,null]} `)
	}
	snap := &snapshotFile{
		Seq:         42,
		TS:          123,
		Counters:    map[string]uint64{"id": 8},
		Versions:    map[string]int{"records": 2},
		Collections: map[string]map[string]json.RawMessage{"records": rows},
	}
	var output bytes.Buffer
	err := encodeSnapshot(&output, snap)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeSnapshot(output.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	// Compare through the legacy encoder to account for invalid UTF-8 keys
	// being normalized and whitespace outside RawMessage values being dropped.
	legacy, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	want, err := decodeSnapshot(wrapSnapshot(legacy))
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range got.Collections["records"] {
		if !json.Valid(raw) {
			t.Fatal("invalid recovered record")
		}
	}
	for _, decoded := range []*snapshotFile{got, want} {
		for key, raw := range decoded.Collections["records"] {
			var value any
			err = json.Unmarshal(raw, &value)
			if err != nil {
				t.Fatal(err)
			}
			normalized, marshalErr := json.Marshal(value)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			decoded.Collections["records"][key] = normalized
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("streamed snapshot differs from legacy payload: %+v", got)
	}
	for _, offset := range []int{0, 10, output.Len() - 1} {
		failure := errors.New("snapshot disk full")
		writer := &failingSnapshotWriter{remaining: offset, err: failure}
		err = encodeSnapshot(writer, snap)
		if !errors.Is(err, failure) {
			t.Fatalf("offset %d: %v", offset, err)
		}
	}
	rows["invalid"] = json.RawMessage(`{broken`)
	err = encodeSnapshot(io.Discard, snap)
	if err == nil || !strings.Contains(err.Error(), "invalid JSON") {
		t.Fatalf("invalid record was accepted: %v", err)
	}
}

func TestSnapshotFailureKeepsWALRecoverable(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, &order{ID: "a"})
	before := f.db.wal.sizeNow()
	failure := errors.New("snapshot unavailable")
	f.db.saveSnapshot = func(string, *snapshotFile) error { return failure }
	err := f.db.Compact()
	if !errors.Is(err, failure) || f.db.wal.sizeNow() != before {
		t.Fatalf("snapshot failure changed WAL: %v", err)
	}
	release(t, f.db)
	recovered := openFixture(t, f.dir)
	if _, ok := recovered.orders.Get("a"); !ok {
		t.Fatal("acknowledged write lost after snapshot failure")
	}
}

func TestSnapshotInstallFailureKeepsWALAndRemovesTemporaryFile(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, &order{ID: "a"})
	before := f.db.wal.sizeNow()
	blocked := filepath.Join(f.dir, snapshotName(f.db.Stats().Seq))
	err := os.Mkdir(blocked, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	err = f.db.Compact()
	if err == nil || f.db.wal.sizeNow() != before {
		t.Fatalf("install failure modified WAL: %v", err)
	}
	temporary, err := filepath.Glob(filepath.Join(f.dir, ".snapshot-*.tmp"))
	if err != nil || len(temporary) != 0 {
		t.Fatalf("temporary snapshots remain: %v, %v", temporary, err)
	}
	err = os.Remove(blocked)
	if err != nil {
		t.Fatal(err)
	}
	release(t, f.db)
	recovered := openFixture(t, f.dir)
	if _, ok := recovered.orders.Get("a"); !ok {
		t.Fatal("write lost after snapshot installation failure")
	}
}

func TestCompactionResetFailureRefusesLaterWrites(t *testing.T) {
	for _, syncFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "header-write", true: "header-sync"}[syncFailure], func(t *testing.T) {
			dev := newCrashDevice()
			f := openFixtureOn(t, t.TempDir(), dev)
			mustPut(t, f.orders, &order{ID: "a"})
			failure := errors.New("reset failure")
			if syncFailure {
				dev.setFailSync(failure)
			} else {
				dev.setFailWrite(failure)
			}
			err := f.db.Compact()
			if !errors.Is(err, failure) {
				t.Fatalf("Compact = %v", err)
			}
			err = f.orders.Put(&order{ID: "b"})
			if !errors.Is(err, ErrFailed) {
				t.Fatalf("later write = %v, want ErrFailed", err)
			}
			release(t, f.db)
			dev.cutPower()
			recovered := openFixtureOn(t, f.dir, dev)
			if _, ok := recovered.orders.Get("a"); !ok {
				t.Fatal("installed snapshot did not preserve acknowledged data")
			}
		})
	}
}

func TestBackupRejectsItsOwnDirectory(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, &order{ID: "a"})
	before := f.db.wal.sizeNow()
	err := f.db.Backup(filepath.Join(f.dir, "."))
	if err == nil || f.db.wal.sizeNow() != before {
		t.Fatalf("self-backup was not refused: %v", err)
	}
	mustPut(t, f.orders, &order{ID: "b"})
	release(t, f.db)
	recovered := openFixture(t, f.dir)
	if recovered.orders.Len() != 2 {
		t.Fatal("self-backup damaged the live log")
	}
}
