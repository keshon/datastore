package datastore

// RecoveryReport describes what Open found on disk and, more importantly, what
// it could not use.
//
// Recovery is deliberately willing to discard a damaged log tail in order to
// open at all. That is the right trade — a store that refuses to start is worse
// than one that starts having lost the last unsynced commits — but it means a
// successful Open is not proof that nothing was lost. Without this report the
// difference between "opened cleanly" and "opened after dropping three
// acknowledged commits" exists only in a log line, which is not something an
// application can act on.
//
// Check Clean at startup and treat false as an incident: page someone, refuse
// to serve, or at minimum record it somewhere a human will read.
type RecoveryReport struct {
	// SnapshotSeq is the sequence of the snapshot state was restored from;
	// 0 when the directory was empty or held no usable snapshot.
	SnapshotSeq uint64
	// SnapshotRecords counts records loaded from that snapshot.
	SnapshotRecords int
	// SnapshotsSkipped lists snapshots that were unreadable or undecodable and
	// were passed over in favour of an older one. Non-empty means the newest
	// snapshot on disk was damaged.
	SnapshotsSkipped []string

	// WALFrames counts log frames replayed on top of the snapshot.
	WALFrames int

	// Truncated reports that the log had a damaged or out-of-order tail, which
	// was discarded. Everything after TruncatedAt is gone.
	Truncated bool
	// TruncatedAt is the byte offset where the log stopped being trustworthy.
	TruncatedAt int64
	// TruncatedReason explains what was wrong with the frame found there.
	TruncatedReason string

	// Migrated maps collection name to the on-disk schema version it was
	// migrated from. Non-empty means this Open rewrote records.
	Migrated map[string]int

	// Seq is the sequence the database opened at.
	Seq uint64
}

// Clean reports whether recovery completed with nothing skipped, truncated, or
// migrated — that is, the database on disk was exactly what this build expected.
func (r RecoveryReport) Clean() bool {
	return !r.Truncated && len(r.SnapshotsSkipped) == 0 && len(r.Migrated) == 0
}

// LostData reports whether recovery discarded committed work. It is the
// narrower, louder question: migrations are routine, a truncated log is not.
func (r RecoveryReport) LostData() bool {
	return r.Truncated || len(r.SnapshotsSkipped) > 0
}

// Recovery returns what the last Open found. The zero value is returned before
// Open.
func (db *DB) Recovery() RecoveryReport {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.recovery
}
