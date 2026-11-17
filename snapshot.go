package datastore

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

// snapshotGlob and its pattern must agree; the zero padding keeps lexical and
// numeric order the same, which is what makes listing cheap.
const snapshotGlob = "snapshot-*.json"

var snapshotRe = regexp.MustCompile(`^snapshot-(\d{12})\.json$`)

// snapshotFormatVersion is the envelope this build writes. Version 0 is the
// original format: a bare snapshotFile with no checksum.
const snapshotFormatVersion = 1

// downgradeTrap is a deliberate type conflict, and the reason it is spelled
// "collections" rather than something tidier.
//
// A build older than the envelope decodes a snapshot straight into
// snapshotFile. Handed an envelope it finds none of the fields it knows,
// succeeds anyway, and opens an empty database -- no error, no warning, and
// the log was emptied by the compaction that wrote the snapshot. Silent total
// loss on a downgrade.
//
// Those builds expect collections to be an object. Giving them a string makes
// json.Unmarshal fail, which routes into the path they already have for a
// damaged snapshot: warn, skip, and report it through
// RecoveryReport.SnapshotsSkipped, so LostData is true and somebody is told.
// The database is still empty, but loudly rather than silently, and the string
// says what to do about it.
const downgradeTrap = "this snapshot needs datastore v1.4 or newer; upgrade, " +
	"or restore the backup you took before downgrading"

func snapshotName(seq uint64) string {
	return fmt.Sprintf("snapshot-%012d.json", seq)
}

// snapshotFile is the full state at a point in the sequence.
//
// Versions records the schema version of each collection's records, so a later
// build can tell what it is reading and migrate. A collection absent from the
// map is version 0, which is what every snapshot written before versioning
// existed implies.
type snapshotFile struct {
	Seq         uint64                                `json:"seq"`
	TS          int64                                 `json:"ts"`
	Counters    map[string]uint64                     `json:"counters,omitempty"`
	Versions    map[string]int                        `json:"versions,omitempty"`
	Collections map[string]map[string]json.RawMessage `json:"collections"`
}

// snapshotEnvelope wraps the payload with a checksum over its exact bytes.
//
// The log protects every frame with a CRC, and a snapshot holds strictly more
// than any frame does — the entire dataset — so leaving it unprotected made the
// largest object on disk the only unchecked one. "Does it parse as JSON" is not
// an integrity check: a flipped bit inside a string value, or inside a digit of
// a number, decodes perfectly into the wrong data. It also defeated the
// fallback in loadSnapshot, which can only reach for an older snapshot when it
// can tell the newest one is bad.
//
// The envelope stays JSON rather than becoming a binary frame for the same
// reason the log payloads are JSON: when something has gone wrong, being able
// to read the file with less and jq is worth more than the bytes it costs.
type snapshotEnvelope struct {
	Version int             `json:"v"`
	CRC     uint32          `json:"crc32c"`
	Payload snapshotPayload `json:"payload"`
}

// snapshotPayload borrows the input during decodeSnapshot. Record RawMessages
// are copied by the second decode, so the file buffer is not retained in state.
type snapshotPayload []byte

func (p *snapshotPayload) UnmarshalJSON(b []byte) error {
	*p = b
	return nil
}

func (p snapshotPayload) MarshalJSON() ([]byte, error) {
	return p, nil
}

// listSnapshots returns the sequence numbers present, newest last.
func listSnapshots(dir string) ([]uint64, error) {
	matches, err := filepath.Glob(filepath.Join(dir, snapshotGlob))
	if err != nil {
		return nil, fmt.Errorf("datastore: list snapshots: %w", err)
	}
	var seqs []uint64
	for _, m := range matches {
		sub := snapshotRe.FindStringSubmatch(filepath.Base(m))
		if sub == nil {
			continue
		}
		n, err := strconv.ParseUint(sub[1], 10, 64)
		if err != nil {
			continue
		}
		seqs = append(seqs, n)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

// wrapSnapshot builds the on-disk bytes for an already-encoded payload.
//
// The envelope is assembled by hand rather than by marshalling a struct that
// holds the payload as json.RawMessage. That would work today, but only because
// the encoder happens to re-compact and re-escape an embedded raw message
// exactly the way json.Marshal produced it. A checksum is a durability
// guarantee and must not rest on a library detail that could change: writing
// the payload through verbatim is the only way to be certain the bytes that
// were summed are the bytes that land on disk.
func wrapSnapshot(payload []byte) []byte {
	var buf bytes.Buffer
	buf.Grow(len(payload) + len(downgradeTrap) + 96)
	buf.WriteString(`{"collections":`)
	trap, err := json.Marshal(downgradeTrap)
	if err != nil {
		panic("datastore: encoding a constant string failed: " + err.Error())
	}
	buf.Write(trap)
	buf.WriteString(`,"v":`)
	buf.WriteString(strconv.Itoa(snapshotFormatVersion))
	buf.WriteString(`,"crc32c":`)
	buf.WriteString(strconv.FormatUint(uint64(crc32.Checksum(payload, crcTable)), 10))
	buf.WriteString(`,"payload":`)
	buf.Write(payload)
	buf.WriteString("}\n")
	return buf.Bytes()
}

// decodeSnapshot parses one snapshot file, verifying its checksum when it has
// one.
//
// A file with no payload field is the original format, which predates the
// checksum. It is accepted unverified — refusing would strand every database
// written before this change, and an unverified snapshot is exactly as safe as
// it was then, no less. The next compaction rewrites it in the new format for
// free.
func decodeSnapshot(b []byte) (*snapshotFile, error) {
	var env snapshotEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("undecodable: %w", err)
	}

	if env.Payload == nil {
		var snap snapshotFile
		if err := json.Unmarshal(b, &snap); err != nil {
			return nil, fmt.Errorf("undecodable: %w", err)
		}
		return &snap, nil
	}

	if env.Version > snapshotFormatVersion {
		// Never fall back to an older snapshot here. A snapshot from a newer
		// build means the data on disk is ahead of this binary, and quietly
		// opening at an earlier state would present stale data as current.
		return nil, fmt.Errorf("%w: snapshot format version %d, this build understands %d",
			ErrSnapshotVersion, env.Version, snapshotFormatVersion)
	}

	if got := crc32.Checksum(env.Payload, crcTable); got != env.CRC {
		return nil, fmt.Errorf("checksum %08x, expected %08x", got, env.CRC)
	}

	var snap snapshotFile
	if err := json.Unmarshal(env.Payload, &snap); err != nil {
		return nil, fmt.Errorf("undecodable payload: %w", err)
	}
	return &snap, nil
}

// loadSnapshot reads the newest snapshot that parses and passes its checksum.
//
// A fallback is reported: an older snapshot may lose compacted state if the live
// log no longer bridges the gap. Returns nil when there is no usable snapshot.
func loadSnapshot(dir string) (*snapshotFile, []string, error) {
	seqs, err := listSnapshots(dir)
	if err != nil {
		return nil, nil, err
	}

	var warnings []string
	for i := len(seqs) - 1; i >= 0; i-- {
		path := filepath.Join(dir, snapshotName(seqs[i]))
		b, err := os.ReadFile(path)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("%s unreadable: %v", filepath.Base(path), err))
			continue
		}
		snap, err := decodeSnapshot(b)
		if err != nil {
			if errors.Is(err, ErrSnapshotVersion) {
				return nil, warnings, err
			}
			warnings = append(warnings, fmt.Sprintf("%s %v", filepath.Base(path), err))
			continue
		}
		return snap, warnings, nil
	}
	return nil, warnings, nil
}

// writeSnapshot writes a snapshot atomically: a temporary file in the same
// directory, fsynced, then installed with the platform's durability barriers.
func writeSnapshot(dir string, snap *snapshotFile) error {
	tmp, err := os.CreateTemp(dir, ".snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("datastore: create temp snapshot: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op once the rename succeeds

	// CreateTemp makes the file 0o600, but the snapshot holds the same data as
	// the 0o644 log — a backup job running as another user should not find the
	// full state stricter than the tail of it.
	err = tmp.Chmod(0o644)
	if err != nil {
		tmp.Close()
		return fmt.Errorf("datastore: chmod temp snapshot: %w", err)
	}

	err = encodeSnapshot(tmp, snap)
	if err != nil {
		tmp.Close()
		return fmt.Errorf("datastore: write temp snapshot: %w", err)
	}
	err = tmp.Sync()
	if err != nil {
		tmp.Close()
		return fmt.Errorf("datastore: sync temp snapshot: %w", err)
	}
	err = tmp.Close()
	if err != nil {
		return fmt.Errorf("datastore: close temp snapshot: %w", err)
	}

	final := filepath.Join(dir, snapshotName(snap.Seq))
	err = installSnapshot(tmpPath, final)
	if err != nil {
		return fmt.Errorf("datastore: install snapshot: %w", err)
	}
	err = syncDir(dir)
	if err != nil {
		return fmt.Errorf("datastore: sync directory: %w", err)
	}
	return nil
}

// snapshotJSON keeps the first encoding or I/O error. Raw records are immutable
// while the writer slot is held; streaming them avoids dataset-sized buffers.
type snapshotJSON struct {
	w   io.Writer
	err error
}

func (s *snapshotJSON) text(v string) {
	if s.err != nil {
		return
	}
	_, s.err = io.WriteString(s.w, v)
}

func (s *snapshotJSON) value(v any) {
	if s.err != nil {
		return
	}
	var b []byte
	b, s.err = json.Marshal(v)
	if s.err == nil {
		_, s.err = s.w.Write(b)
	}
}

func (s *snapshotJSON) key(v string) {
	for i := range len(v) {
		c := v[i]
		if c < 0x20 || c >= 0x80 || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			s.value(v)
			return
		}
	}
	s.text(`"`)
	s.text(v)
	s.text(`"`)
}

func encodeSnapshot(w io.Writer, snap *snapshotFile) error {
	buffer := bufio.NewWriterSize(w, 64<<10)
	envelope := snapshotJSON{w: buffer}
	envelope.text(`{"collections":`)
	envelope.value(downgradeTrap)
	envelope.text(`,"v":`)
	envelope.value(snapshotFormatVersion)
	envelope.text(`,"payload":`)
	if envelope.err != nil {
		return envelope.err
	}

	checksum := crc32.New(crcTable)
	checksummed := bufio.NewWriterSize(io.MultiWriter(buffer, checksum), 64<<10)
	payload := snapshotJSON{w: checksummed}
	payload.text(`{"seq":`)
	payload.value(snap.Seq)
	payload.text(`,"ts":`)
	payload.value(snap.TS)
	if len(snap.Counters) != 0 {
		payload.text(`,"counters":`)
		payload.value(snap.Counters)
	}
	if len(snap.Versions) != 0 {
		payload.text(`,"versions":`)
		payload.value(snap.Versions)
	}
	payload.text(`,"collections":{`)
	for i, name := range sortedKeys(snap.Collections) {
		if i != 0 {
			payload.text(",")
		}
		payload.key(name)
		payload.text(":{")
		rows := snap.Collections[name]
		for j, key := range sortedKeys(rows) {
			if j != 0 {
				payload.text(",")
			}
			payload.key(key)
			payload.text(":")
			if !json.Valid(rows[key]) {
				return fmt.Errorf("invalid JSON in %s/%s", name, key)
			}
			if payload.err != nil {
				return payload.err
			}
			_, payload.err = payload.w.Write(rows[key])
		}
		payload.text("}")
	}
	payload.text("}}")
	if payload.err != nil {
		return payload.err
	}
	err := checksummed.Flush()
	if err != nil {
		return err
	}

	envelope.text(`,"crc32c":`)
	envelope.value(checksum.Sum32())
	envelope.text("}\n")
	if envelope.err != nil {
		return envelope.err
	}
	return buffer.Flush()
}

// pruneSnapshots deletes all but the newest `keep`.
//
// keep must be at least 2. Keeping the previous snapshot means that a crash
// between installing a new one and truncating the log still leaves a complete
// (older snapshot + full log) pair to recover from.
func pruneSnapshots(dir string, keep int) error {
	if keep < 2 {
		keep = 2
	}
	seqs, err := listSnapshots(dir)
	if err != nil {
		return err
	}
	if len(seqs) <= keep {
		return nil
	}
	for _, seq := range seqs[:len(seqs)-keep] {
		if err := os.Remove(filepath.Join(dir, snapshotName(seq))); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("datastore: prune snapshot %d: %w", seq, err)
		}
	}
	return nil
}
