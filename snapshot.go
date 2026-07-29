package datastore

import (
	"encoding/json"
	"fmt"
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

func snapshotName(seq uint64) string {
	return fmt.Sprintf("snapshot-%012d.json", seq)
}

// snapshotFile is the full state at a point in the sequence.
type snapshotFile struct {
	Seq         uint64                                `json:"seq"`
	TS          int64                                 `json:"ts"`
	Counters    map[string]uint64                     `json:"counters,omitempty"`
	Collections map[string]map[string]json.RawMessage `json:"collections"`
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

// loadSnapshot reads the newest snapshot that parses.
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
		var snap snapshotFile
		if err := json.Unmarshal(b, &snap); err != nil {
			warnings = append(warnings, fmt.Sprintf("%s undecodable: %v", filepath.Base(path), err))
			continue
		}
		return &snap, warnings, nil
	}
	return nil, warnings, nil
}

// writeSnapshot writes a snapshot atomically: a temporary file in the same
// directory, fsynced, then renamed over the target, then the directory itself
// fsynced so the rename survives a power cut.
func writeSnapshot(dir string, snap *snapshotFile) error {
	b, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("datastore: encode snapshot: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("datastore: create temp snapshot: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op once the rename succeeds

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
