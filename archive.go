package datastore

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// Archived log segments.
//
// Compaction normally truncates the log: its contents are already in the new
// snapshot, so for recovery they are dead weight. But those frames are also the
// only record of *how* the state got here — the sequence of commits, their
// timestamps, and the annotations attached to them (see Tx.Note). Discarding
// them makes an audit trail impossible.
//
// With HistorySegments or HistoryFor set, compaction renames the log aside as
// archive-<seq>.log instead of emptying it, and History reads across the
// archives and the live log.
//
// Archives are history, never recovery input: their frames are already folded
// into the snapshot, so replaying them would double-apply. Recovery reads
// wal.log alone.

const archiveGlob = "archive-*.log"

var archiveRe = regexp.MustCompile(`^archive-(\d{12})\.log$`)

func archiveName(seq uint64) string {
	return fmt.Sprintf("archive-%012d.log", seq)
}

// archivingEnabled reports whether compaction should keep segments.
func (db *DB) archivingEnabled() bool {
	return db.opts.HistorySegments > 0 || db.opts.HistoryFor > 0
}

// listArchives returns the archived segment sequences present, oldest first.
func listArchives(dir string) ([]uint64, error) {
	matches, err := filepath.Glob(filepath.Join(dir, archiveGlob))
	if err != nil {
		return nil, fmt.Errorf("datastore: list archives: %w", err)
	}
	var seqs []uint64
	for _, m := range matches {
		sub := archiveRe.FindStringSubmatch(filepath.Base(m))
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

// archiveLogLocked renames the current log aside and opens a fresh one. The
// caller holds the writer slot and the state lock.
//
// The log must be closed before it is renamed: Windows refuses to rename a file
// that is still open. Ordering is what keeps this safe — the snapshot is already
// durable by the time we are called, so a crash before the rename leaves a log
// that replays onto the snapshot (wasteful, never wrong), and a crash after it
// leaves an archive and no log, which the next Open creates empty.
func (db *DB) archiveLogLocked(seq uint64) error {
	if err := db.wal.close(); err != nil {
		return fmt.Errorf("datastore: close log for archiving: %w", err)
	}
	dst := filepath.Join(db.opts.Dir, archiveName(seq))
	if err := os.Rename(filepath.Join(db.opts.Dir, walName), dst); err != nil && !os.IsNotExist(err) {
		// Reopen so the database stays usable even though archiving failed. If
		// even that fails, the log is gone from under an open database: latch
		// the failure so writes are refused instead of silently impossible.
		w, oerr := openWAL(db.opts.Dir)
		if oerr != nil {
			db.failed = fmt.Errorf("reopen log after failed archiving: %w", oerr)
			db.log.Error().Err(db.failed).Msg("datastore_failed: refusing further writes")
			return fmt.Errorf("datastore: archive log: %w (and reopening failed: %v)", err, oerr)
		}
		db.wal = w
		return fmt.Errorf("datastore: archive log: %w", err)
	}
	w, err := openWAL(db.opts.Dir)
	if err != nil {
		db.failed = fmt.Errorf("open fresh log after archiving: %w", err)
		db.log.Error().Err(db.failed).Msg("datastore_failed: refusing further writes")
		return err
	}
	db.wal = w
	return nil
}

// pruneArchives enforces both retention bounds, whichever bites first. A zero
// bound means "unbounded by this measure".
func (db *DB) pruneArchives(now time.Time) error {
	seqs, err := listArchives(db.opts.Dir)
	if err != nil {
		return err
	}

	drop := make(map[uint64]bool)
	if n := db.opts.HistorySegments; n > 0 && len(seqs) > n {
		for _, seq := range seqs[:len(seqs)-n] {
			drop[seq] = true
		}
	}
	if age := db.opts.HistoryFor; age > 0 {
		cutoff := now.Add(-age)
		for _, seq := range seqs {
			info, err := os.Stat(filepath.Join(db.opts.Dir, archiveName(seq)))
			if err != nil {
				continue
			}
			if info.ModTime().Before(cutoff) {
				drop[seq] = true
			}
		}
	}

	for seq := range drop {
		path := filepath.Join(db.opts.Dir, archiveName(seq))
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("datastore: prune archive %d: %w", seq, err)
		}
		db.log.Debug().Uint64("seq", seq).Msg("archive_pruned")
	}
	return nil
}

// archiveStats reports how much history is retained on disk.
func (db *DB) archiveStats() (count int, bytes int64) {
	seqs, err := listArchives(db.opts.Dir)
	if err != nil {
		return 0, 0
	}
	for _, seq := range seqs {
		info, err := os.Stat(filepath.Join(db.opts.Dir, archiveName(seq)))
		if err != nil {
			continue
		}
		count++
		bytes += info.Size()
	}
	return count, bytes
}
