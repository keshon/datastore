package datastore

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"time"
)

// HistoryEntry is one committed transaction as it was recorded in the log.
type HistoryEntry struct {
	Seq  uint64
	Time time.Time
	// Meta is whatever the transaction attached with Tx.Note — who, and why.
	Meta map[string]string
	Ops  []HistoryOp
}

// HistoryOp is one record-level change within a commit.
type HistoryOp struct {
	Collection string
	Key        string
	Delete     bool
	// Value is the record as it was written, still encoded. Nil for a delete.
	// It is the value at that moment, not the current one.
	Value json.RawMessage
}

// errStopHistory unwinds the scan when the caller's range is exhausted.
var errStopHistory = errors.New("datastore: stop history")

// History calls fn for every committed transaction with a sequence in [from, to],
// oldest first, reading across archived segments and then the live log. A to of
// 0 means "no upper bound".
//
// This is the audit trail: each entry carries the annotations the transaction
// attached (Tx.Note) and the values exactly as they were written, which is
// something the current state cannot tell you. Returning an error from fn stops
// the scan and is returned to the caller.
//
// How far back it reaches depends on retention: without Options.HistorySegments
// or HistoryFor, compaction discards the log and only commits since the last
// compaction are visible. Reading is O(bytes on disk) and touches no in-memory
// state, so it is safe to call on a live database — including one opened with
// OpenReader.
func (db *DB) History(from, to uint64, fn func(HistoryEntry) error) error {
	db.mu.RLock()
	opened := db.opened && !db.closed
	dir := db.opts.Dir
	db.mu.RUnlock()
	if !opened {
		return ErrNotOpen
	}

	segments, err := listArchives(dir)
	if err != nil {
		return err
	}

	// Archives oldest first, then the live log, which together are the commits
	// in sequence order.
	paths := make([]string, 0, len(segments)+1)
	for _, seq := range segments {
		paths = append(paths, filepath.Join(dir, archiveName(seq)))
	}
	paths = append(paths, filepath.Join(dir, walName))

	visit := func(fr *walFrame) error {
		if fr.Seq < from {
			return nil
		}
		if to > 0 && fr.Seq > to {
			return errStopHistory
		}
		entry := HistoryEntry{
			Seq:  fr.Seq,
			Time: time.Unix(fr.TS, 0),
			Meta: fr.Meta,
			Ops:  make([]HistoryOp, 0, len(fr.Ops)),
		}
		for _, op := range fr.Ops {
			entry.Ops = append(entry.Ops, HistoryOp{
				Collection: op.Coll,
				Key:        op.Key,
				Delete:     op.Op == opDelete,
				Value:      op.Val,
			})
		}
		return fn(entry)
	}

	for _, path := range paths {
		if err := scanFrames(path, visit); err != nil {
			if errors.Is(err, errStopHistory) {
				return nil
			}
			return err
		}
	}
	return nil
}
