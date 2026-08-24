package datastore

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Snapshot integrity.
//
// The log has always been checksummed frame by frame; the snapshot — which
// holds the entire dataset rather than one transaction's worth of it — was
// validated only by whether it parsed. These tests pin the difference. The
// corruption they inject is deliberately the kind JSON cannot notice: a
// character swapped inside a string value, which decodes perfectly into the
// wrong record.

// snapshotPath returns the single snapshot in dir, failing if there is not
// exactly one.
func snapshotPath(t *testing.T, dir string) string {
	t.Helper()
	seqs, err := listSnapshots(dir)
	if err != nil {
		t.Fatalf("listSnapshots: %v", err)
	}
	if len(seqs) != 1 {
		t.Fatalf("want exactly one snapshot, found %d", len(seqs))
	}
	return filepath.Join(dir, snapshotName(seqs[0]))
}

// corruptInPlace replaces the first occurrence of old with new in a file,
// leaving its length and its JSON structure intact.
func corruptInPlace(t *testing.T, path, old, new string) {
	t.Helper()
	if len(old) != len(new) {
		t.Fatalf("corruptInPlace: %q and %q differ in length", old, new)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", filepath.Base(path), err)
	}
	if !strings.Contains(string(b), old) {
		t.Fatalf("%s does not contain %q", filepath.Base(path), old)
	}
	out := strings.Replace(string(b), old, new, 1)
	if err := os.WriteFile(path, []byte(out), 0o644); err != nil {
		t.Fatalf("write %s: %v", filepath.Base(path), err)
	}
}

// TestSnapshotChecksumCatchesSilentCorruption is the test the checksum exists
// for. Without it the store opens happily and serves "c2" as though the
// customer had always been c2.
func TestSnapshotChecksumCatchesSilentCorruption(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, mkOrder("1", func(o *order) { o.Customer = "c1" }))
	if err := f.db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	release(t, f.db)

	path := snapshotPath(t, f.dir)

	// Prove the corruption is invisible to the parser before relying on the
	// checksum to catch it: otherwise this test could pass for the wrong reason.
	corruptInPlace(t, path, `"customer":"c1"`, `"customer":"c2"`)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	var probe snapshotEnvelope
	if err := json.Unmarshal(b, &probe); err != nil {
		t.Fatalf("corrupted snapshot no longer parses, so this test proves nothing: %v", err)
	}

	g := openFixture(t, f.dir)
	rep := g.db.Recovery()

	if len(rep.SnapshotsSkipped) != 1 {
		t.Fatalf("SnapshotsSkipped = %v, want one entry naming the damaged snapshot", rep.SnapshotsSkipped)
	}
	if !strings.Contains(rep.SnapshotsSkipped[0], "checksum") {
		t.Errorf("SnapshotsSkipped[0] = %q, want it to blame the checksum", rep.SnapshotsSkipped[0])
	}
	if !rep.LostData() {
		t.Error("LostData() = false after a snapshot was discarded; the whole dataset went with it")
	}
	if rep.Clean() {
		t.Error("Clean() = true after a snapshot was discarded")
	}
	if _, ok := g.orders.Get("1"); ok {
		t.Error("order 1 is present, so the corrupted snapshot was loaded after all")
	}
}

// TestSnapshotChecksumFallsBackToOlder covers the reason loadSnapshot walks
// backwards at all. Keeping the previous snapshot is only useful if damage in
// the newest one can be detected.
func TestSnapshotChecksumFallsBackToOlder(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, mkOrder("1"))
	if err := f.db.Compact(); err != nil {
		t.Fatalf("first Compact: %v", err)
	}
	older, err := listSnapshots(f.dir)
	if err != nil {
		t.Fatalf("listSnapshots: %v", err)
	}
	firstSeq := older[len(older)-1]

	mustPut(t, f.orders, mkOrder("2"))
	if err := f.db.Compact(); err != nil {
		t.Fatalf("second Compact: %v", err)
	}
	release(t, f.db)

	seqs, err := listSnapshots(f.dir)
	if err != nil {
		t.Fatalf("listSnapshots: %v", err)
	}
	if len(seqs) < 2 {
		t.Fatalf("want two snapshots retained, found %d", len(seqs))
	}
	newest := filepath.Join(f.dir, snapshotName(seqs[len(seqs)-1]))
	corruptInPlace(t, newest, `"status":"new"`, `"status":"NEW"`)

	g := openFixture(t, f.dir)
	rep := g.db.Recovery()

	if rep.SnapshotSeq != firstSeq {
		t.Errorf("recovered from snapshot %d, want the older %d", rep.SnapshotSeq, firstSeq)
	}
	if len(rep.SnapshotsSkipped) != 1 {
		t.Errorf("SnapshotsSkipped = %v, want the damaged newest snapshot", rep.SnapshotsSkipped)
	}
	if _, ok := g.orders.Get("1"); !ok {
		t.Error("order 1 missing; the older snapshot should still hold it")
	}
}

// TestLegacySnapshotLoadsUnverified pins backwards compatibility. A database
// written before the envelope existed must still open, unverified, rather than
// be treated as damage and discarded.
func TestLegacySnapshotLoadsUnverified(t *testing.T) {
	dir := t.TempDir()

	legacy := &snapshotFile{
		Seq: 7,
		TS:  1000,
		Collections: map[string]map[string]json.RawMessage{
			"orders": {"1": json.RawMessage(`{"id":"1","customer":"c1","status":"new","created":1000,"total":100}`)},
		},
	}
	b, err := json.Marshal(legacy) // the bare payload, exactly as older builds wrote it
	if err != nil {
		t.Fatalf("marshal legacy snapshot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, snapshotName(7)), b, 0o644); err != nil {
		t.Fatalf("write legacy snapshot: %v", err)
	}

	f := openFixture(t, dir)
	rep := f.db.Recovery()

	if len(rep.SnapshotsSkipped) != 0 {
		t.Errorf("SnapshotsSkipped = %v, want none: a legacy snapshot is not damage", rep.SnapshotsSkipped)
	}
	if !rep.Clean() {
		t.Error("Clean() = false on a legacy snapshot that loaded fine")
	}
	got, ok := f.orders.Get("1")
	if !ok {
		t.Fatal("order 1 missing from a legacy snapshot")
	}
	if got.Customer != "c1" {
		t.Errorf("Customer = %q, want c1", got.Customer)
	}
}

// TestCompactionUpgradesLegacySnapshot: the format migrates itself. Nobody has
// to run anything.
func TestCompactionUpgradesLegacySnapshot(t *testing.T) {
	dir := t.TempDir()
	legacy := &snapshotFile{
		Seq: 3,
		Collections: map[string]map[string]json.RawMessage{
			"orders": {"1": json.RawMessage(`{"id":"1","status":"new","created":1000}`)},
		},
	}
	b, _ := json.Marshal(legacy)
	if err := os.WriteFile(filepath.Join(dir, snapshotName(3)), b, 0o644); err != nil {
		t.Fatalf("write legacy snapshot: %v", err)
	}

	f := openFixture(t, dir)
	if err := f.db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	seqs, err := listSnapshots(dir)
	if err != nil {
		t.Fatalf("listSnapshots: %v", err)
	}
	newest, err := os.ReadFile(filepath.Join(dir, snapshotName(seqs[len(seqs)-1])))
	if err != nil {
		t.Fatalf("read newest: %v", err)
	}
	var env snapshotEnvelope
	if err := json.Unmarshal(newest, &env); err != nil {
		t.Fatalf("unmarshal newest: %v", err)
	}
	if env.Payload == nil {
		t.Fatal("compaction rewrote the snapshot without an envelope")
	}
	if env.Version != snapshotFormatVersion {
		t.Errorf("envelope version = %d, want %d", env.Version, snapshotFormatVersion)
	}
	if _, err := decodeSnapshot(newest); err != nil {
		t.Errorf("the snapshot this build just wrote does not verify: %v", err)
	}
}

// TestSnapshotFromNewerBuildIsRefused: a future format is not damage, and
// falling back to an older snapshot would serve stale state as current.
func TestSnapshotFromNewerBuildIsRefused(t *testing.T) {
	dir := t.TempDir()

	payload, err := json.Marshal(&snapshotFile{Seq: 9, Collections: map[string]map[string]json.RawMessage{}})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	future := wrapSnapshot(payload)
	future = []byte(strings.Replace(string(future), `{"v":1,`, `{"v":99,`, 1))
	if err := os.WriteFile(filepath.Join(dir, snapshotName(9)), future, 0o644); err != nil {
		t.Fatalf("write future snapshot: %v", err)
	}

	db := New(Options{Dir: dir, Sync: SyncNever, CompactInterval: -1})
	Register[*order](db, "orders")
	err = db.Open()
	if !errors.Is(err, ErrSnapshotVersion) {
		t.Fatalf("Open error = %v, want ErrSnapshotVersion", err)
	}
}

// TestSnapshotEnvelopeRoundTrip checks the property the hand-assembled envelope
// exists to guarantee: the bytes that were summed are the bytes on disk, so a
// payload survives the round trip untouched.
func TestSnapshotEnvelopeRoundTrip(t *testing.T) {
	original := &snapshotFile{
		Seq:      42,
		TS:       1700000000,
		Counters: map[string]uint64{"order": 17},
		Versions: map[string]int{"orders": 2},
		Collections: map[string]map[string]json.RawMessage{
			// Characters the encoder escapes on its way out: marshalling the
			// snapshot turns < and & into < and & even inside a
			// json.RawMessage. That is fine — it is the same JSON value — but it
			// is exactly the kind of re-encoding that would break a checksum
			// computed over the wrong copy of the bytes. The sum is taken after
			// marshalling and the envelope copies the payload through verbatim,
			// so a payload full of escapable characters must still verify.
			"orders": {"1": json.RawMessage(`{"id":"1","note":"a<b>c&d é \"q\""}`)},
		},
	}

	payload, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	b := wrapSnapshot(payload)

	got, err := decodeSnapshot(b)
	if err != nil {
		t.Fatalf("decodeSnapshot: %v", err)
	}
	if got.Seq != original.Seq || got.TS != original.TS {
		t.Errorf("seq/ts = %d/%d, want %d/%d", got.Seq, got.TS, original.Seq, original.TS)
	}
	if got.Counters["order"] != 17 {
		t.Errorf("counter = %d, want 17", got.Counters["order"])
	}
	if got.Versions["orders"] != 2 {
		t.Errorf("version = %d, want 2", got.Versions["orders"])
	}
	// Compare the decoded value, not the raw bytes: the escaping above means
	// the record on disk is spelled differently from the one handed in, while
	// denoting the same thing.
	var gotNote, wantNote struct {
		Note string `json:"note"`
	}
	if err := json.Unmarshal(got.Collections["orders"]["1"], &gotNote); err != nil {
		t.Fatalf("decode round-tripped record: %v", err)
	}
	if err := json.Unmarshal(original.Collections["orders"]["1"], &wantNote); err != nil {
		t.Fatalf("decode original record: %v", err)
	}
	if gotNote.Note != wantNote.Note {
		t.Errorf("note round-tripped as %q, want %q", gotNote.Note, wantNote.Note)
	}

	// A byte flipped inside a string value: still valid JSON, still the wrong
	// data, and nothing but the checksum can tell.
	flipped := make([]byte, len(b))
	copy(flipped, b)
	idx := strings.Index(string(flipped), `"note":"`)
	if idx < 0 {
		t.Fatal("could not locate the note payload to corrupt")
	}
	flipped[idx+len(`"note":"`)] = 'z'
	if json.Unmarshal(flipped, &snapshotEnvelope{}) != nil {
		t.Fatal("the flip broke JSON parsing, so this assertion proves nothing")
	}
	if _, err := decodeSnapshot(flipped); err == nil {
		t.Error("decodeSnapshot accepted a payload with a flipped byte")
	} else if !strings.Contains(err.Error(), "checksum") {
		t.Errorf("error = %v, want it to blame the checksum", err)
	}
}
