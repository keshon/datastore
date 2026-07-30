package datastore

import (
	"fmt"
	"os"
)

// OpenReader opens the directory read-only, without taking the exclusive lock.
//
// This is how a second process — a CLI, an inspector, a backup job — reads a
// database while the application that owns it keeps running. It loads the
// newest usable snapshot and replays the log on top, exactly as Open does, but
// it never writes: not the log header, not a compaction, and not the truncation
// of a damaged tail. Damage is reported through Recovery instead of repaired,
// because the log belongs to the process holding the lock.
//
// The view is a point in time. Writes committed by the owner after this call
// are not visible; reopen to see them. Every write method returns ErrReadOnly,
// and Close simply releases memory.
//
// Registration works the same way: New, then Register and the Add*Index calls,
// then OpenReader.
func (db *DB) OpenReader() error {
	db.mu.Lock()
	if db.opened {
		db.mu.Unlock()
		return ErrAlreadyOpen
	}
	if db.opts.Dir == "" {
		db.mu.Unlock()
		return errDirRequired
	}
	db.mu.Unlock()

	if _, err := os.Stat(db.opts.Dir); err != nil {
		return fmt.Errorf("datastore: open %s for reading: %w", db.opts.Dir, err)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	db.readOnly = true
	if err := db.restoreLocked(); err != nil {
		db.readOnly = false
		return err
	}

	// No migration compaction, no background workers: both would write. Records
	// are migrated in memory for this session only, which is what a reader wants
	// — it sees current shapes without touching the owner's files.
	db.diskVersions = nil
	db.opened = true
	db.lastCompact = db.opts.Clock()
	return nil
}

// ReadOnly reports whether the database was opened with OpenReader.
func (db *DB) ReadOnly() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.readOnly
}
