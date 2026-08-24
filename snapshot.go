package datastore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
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
	Payload json.RawMessage `json:"payload"`
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
	buf.Grow(len(payload) + 64)
	buf.WriteString(`{"v":`)
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
// Falling back to an older one is safe because the log is only ever truncated
// after a newer snapshot is durable — so an older snapshot plus the surviving
// log still reconstructs the same state. Returns nil when there is none.
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
// directory, fsynced, then renamed over the target, then the directory itself
// fsynced so the rename survives a power cut.
func writeSnapshot(dir string, snap *snapshotFile) error {
	payload, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("datastore: encode snapshot: %w", err)
	}
	b := wrapSnapshot(payload)

	tmp, err := os.CreateTemp(dir, ".snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("datastore: create temp snapshot: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op once the rename succeeds

	// CreateTemp makes the file 0o600, but the snapshot holds the same data as
	// the 0o644 log — a backup job running as another user should not find the
	// full state stricter than the tail of it.
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return fmt.Errorf("datastore: chmod temp snapshot: %w", err)
	}

	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return fmt.Errorf("datastore: write temp snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("datastore: sync temp snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("datastore: close temp snapshot: %w", err)
	}

	final := filepath.Join(dir, snapshotName(snap.Seq))
	if err := os.Rename(tmpPath, final); err != nil {
		return fmt.Errorf("datastore: install snapshot: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("datastore: sync directory: %w", err)
	}
	return nil
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
