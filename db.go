package datastore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// SyncMode decides how hard a commit works to reach the disk.
type SyncMode int

const (
	// SyncAlways fsyncs before Update returns. Use it for anything you would
	// be unwilling to lose: orders, customers, money.
	SyncAlways SyncMode = iota

	// SyncInterval fsyncs on a timer, so a crash can lose the last few seconds.
	// Use it for state that is cheap to recreate — sessions, guest carts —
	// which otherwise dominate the log and force constant compaction.
	SyncInterval

	// SyncNever leaves flushing to the operating system. Tests only.
	SyncNever
)

// Options configures a database. Only Dir is required.
type Options struct {
	Dir  string
	Sync SyncMode

	// SyncEvery is the flush period under SyncInterval. Default 5s.
	SyncEvery time.Duration

	// CompactAfterBytes triggers compaction once the log passes this size.
	// Default 8 MiB.
	CompactAfterBytes int64

	// CompactInterval compacts periodically regardless of size. Default 15m.
	// CompactNever (or any negative value) disables the timer; zero means "use
	// the default".
	CompactInterval time.Duration

	// KeepSnapshots is how many to retain. Minimum and default 2 — the
	// previous one is what makes a crash during snapshot installation
	// survivable.
	KeepSnapshots int

	// Logger receives recovery and compaction events. Defaults to a no-op.
	Logger *zerolog.Logger

	// Clock supplies commit timestamps. Defaults to time.Now.
	Clock func() time.Time

	// WriteTimeout bounds how long Update waits for the writer slot before
	// giving up with ErrWriteTimeout. Default 30s. It exists so a transaction
	// that never finishes turns into a diagnosable error instead of a process
	// that hangs forever. (A re-entrant Update on the same goroutine does not
	// even wait: it is detected and panics immediately.)
	WriteTimeout time.Duration

	// MaxBytes is the size this dataset is expected to stay under, in encoded
	// bytes. It enforces nothing: exceeding it logs a warning and shows up in
	// Stats.
	//
	// The whole design rests on the data fitting comfortably in memory, and the
	// failure mode when it stops fitting is an out-of-memory kill with no
	// warning beforehand. Setting a budget converts that into a log line months
	// earlier. Zero disables the check.
	MaxBytes int64

	// WarnAtPercent is the fraction of MaxBytes that triggers the warning.
	// Default 80.
	WarnAtPercent int

	// OnCommit, if set, is called after every successful commit with a summary
	// of the work. It runs on the committing goroutine, so keep it quick and do
	// not touch the database from it — record a metric and return.
	OnCommit func(CommitStats)

	// HistorySegments and HistoryFor retain compacted log segments as
	// archive-<seq>.log rather than discarding them, so History can read past
	// commits and their annotations. Retention is bounded by count and by age,
	// whichever bites first; a zero bound is unbounded by that measure, and
	// leaving both zero (the default) keeps no history at all.
	//
	// This trades disk for an audit trail. Archives are never replayed — their
	// frames are already in the snapshot — so losing them costs history, never
	// state.
	HistorySegments int
	HistoryFor      time.Duration
}

// CompactNever disables periodic compaction when assigned to
// Options.CompactInterval. Compaction still runs when the log outgrows
// CompactAfterBytes and once at Close.
const CompactNever time.Duration = -1

// CommitStats describes one committed transaction, for metrics.
type CommitStats struct {
	Seq        uint64        // sequence assigned to this commit
	Ops        int           // record writes and deletes in the frame
	FrameBytes int           // encoded size of the log frame
	Duration   time.Duration // time spent appending and syncing
	Synced     bool          // whether this commit fsynced before returning
	WALBytes   int64         // log size after the append
}

func (o *Options) applyDefaults() {
	if o.SyncEvery <= 0 {
		o.SyncEvery = 5 * time.Second
	}
	if o.CompactAfterBytes <= 0 {
		o.CompactAfterBytes = 8 << 20
	}
	if o.CompactInterval < 0 {
		o.CompactInterval = 0
	} else if o.CompactInterval == 0 {
		o.CompactInterval = 15 * time.Minute
	}
	if o.KeepSnapshots < 2 {
		o.KeepSnapshots = 2
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 30 * time.Second
	}
	if o.WarnAtPercent <= 0 || o.WarnAtPercent > 100 {
		o.WarnAtPercent = 80
	}
}

// DB is an embedded, single-writer, fully in-memory store with a durable log.
type DB struct {
	opts Options
	log  zerolog.Logger

	// mu guards the in-memory state: records, indexes, sequence, counters.
	// Held briefly — for a read, or to apply an already-durable commit.
	mu sync.RWMutex

	// writeCh serialises writers. A transaction runs without holding mu, so
	// readers are not blocked while it works or while its frame is fsynced.
	writeCh chan struct{}

	// writerGoid identifies the goroutine currently holding writeCh, so a
	// re-entrant Update is caught immediately rather than after WriteTimeout.
	// Best-effort: 0 means unknown, and the timeout remains the backstop.
	writerGoid atomic.Uint64

	colls map[string]anyCollection
	order []string

	// orphans holds records for collections this build did not register.
	//
	// They are carried through compaction untouched. Without that, commenting
	// out a Register call and deploying would silently delete a table at the
	// next compaction — a failure with no error, no log line, and no way back.
	orphans map[string]map[string]json.RawMessage

	// orphanVersions remembers the schema version each orphan was written at, so
	// re-registering the collection later migrates from the right version rather
	// than assuming zero.
	orphanVersions map[string]int

	seq      uint64
	counters map[string]uint64

	// diskVersions holds each collection's on-disk schema version during
	// recovery, so restored records can be migrated as they are applied. It is
	// cleared once Open finishes: from then on everything in memory is current.
	diskVersions map[string]int
	recovery     RecoveryReport

	wal  *wal
	lock *dirLock

	// openLog is how the log file is opened, always osLogOpener outside this
	// package's own tests. The crash-injection tests swap in a device that can
	// lose power, which is the only way to distinguish bytes that were written
	// from bytes that were made durable.
	openLog logOpener

	opened   bool
	closed   bool
	readOnly bool // opened with OpenReader: every write path refuses

	// failed, once set, refuses every further write with ErrFailed. It is set
	// when a log write or sync fails: the log may then hold a torn frame, and
	// anything appended after it would be silently discarded by the next
	// replay while this process kept acknowledging commits. See ErrFailed.
	failed error

	stop        chan struct{}
	wg          sync.WaitGroup
	lastCompact time.Time
	unsynced    bool

	// overBudget tracks whether the budget warning has already been given, so a
	// dataset sitting at the threshold does not log on every commit.
	overBudget bool

	// subs are the live change-feed subscriptions, guarded separately from the
	// state lock so dispatching never contends with readers.
	subsMu sync.RWMutex
	subs   []*Subscription
}

// New prepares a database. It performs no I/O; register collections and
// indexes, then call Open.
func New(opts Options) *DB {
	opts.applyDefaults()

	log := zerolog.Nop()
	if opts.Logger != nil {
		log = *opts.Logger
	}

	return &DB{
		opts:           opts,
		log:            log,
		writeCh:        make(chan struct{}, 1),
		colls:          make(map[string]anyCollection),
		orphans:        make(map[string]map[string]json.RawMessage),
		orphanVersions: make(map[string]int),
		counters:       make(map[string]uint64),
		stop:           make(chan struct{}),
		openLog:        osLogOpener,
	}
}

// fail latches the first write failure. Every later write returns ErrFailed
// wrapping this cause; reopening the database is the way back.
func (db *DB) fail(cause error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.failed == nil {
		db.failed = cause
		db.log.Error().Err(cause).Msg("datastore_failed: refusing further writes")
	}
}

// failedErr reports the latched failure, wrapped in ErrFailed, or nil.
func (db *DB) failedErr() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.failed == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrFailed, db.failed)
}

// Open takes the directory lock, restores state, and starts background work.
func (db *DB) Open() error {
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

	if err := os.MkdirAll(db.opts.Dir, 0o755); err != nil {
		return fmt.Errorf("datastore: create %s: %w", db.opts.Dir, err)
	}

	lock, err := acquireLock(db.opts.Dir)
	if err != nil {
		return err
	}

	db.mu.Lock()
	db.lock = lock
	if err := db.restoreLocked(); err != nil {
		_ = lock.release()
		db.lock = nil
		db.mu.Unlock()
		return err
	}

	// Everything in memory is now at the declared schema version.
	migrated := len(db.recovery.Migrated) > 0
	db.diskVersions = nil

	db.opened = true
	db.lastCompact = db.opts.Clock()
	db.startWorkersLocked()
	db.mu.Unlock()

	// A migration rewrote records, and the log may still hold frames in the old
	// shape. Fold both into a snapshot stamped with the new versions, so the log
	// never carries a mix — and so the work is not repeated on every start.
	// Compact takes the writer slot and the state lock itself, so it must run
	// with neither held.
	if migrated {
		if err := db.Compact(); err != nil {
			return fmt.Errorf("datastore: compact after migration: %w", err)
		}
		db.log.Info().Interface("migrated", db.recovery.Migrated).Msg("datastore_migrated")
	}
	return nil
}

// restoreLocked rebuilds state from the newest usable snapshot plus the log.
func (db *DB) restoreLocked() error {
	snap, warnings, err := loadSnapshot(db.opts.Dir)
	if err != nil {
		return err
	}
	for _, w := range warnings {
		db.log.Warn().Str("dir", db.opts.Dir).Msg("snapshot_skipped: " + w)
	}

	rep := RecoveryReport{SnapshotsSkipped: warnings}

	// diskVersion is the schema version each registered collection was last
	// written at. Absent means 0, which is what pre-versioning snapshots imply.
	// Records below the declared version are migrated as they are applied —
	// snapshot rows and log frames alike, since frames written before the
	// upgrade carry the old shape too.
	db.diskVersions = make(map[string]int, len(db.colls))
	for name, coll := range db.colls {
		v := 0
		if snap != nil {
			v = snap.Versions[name]
		}
		if err := coll.checkVersion(v); err != nil {
			return err
		}
		db.diskVersions[name] = v
		if v < coll.schemaVersion() {
			if rep.Migrated == nil {
				rep.Migrated = make(map[string]int)
			}
			rep.Migrated[name] = v
		}
	}

	var records int
	if snap != nil {
		db.seq = snap.Seq
		rep.SnapshotSeq = snap.Seq
		for k, v := range snap.Counters {
			db.counters[k] = v
		}
		for name, rows := range snap.Collections {
			coll, known := db.colls[name]
			if !known {
				db.orphans[name] = rows
				if v := snap.Versions[name]; v > 0 {
					db.orphanVersions[name] = v
				}
				db.log.Warn().Str("collection", name).Int("records", len(rows)).
					Msg("collection_unregistered: preserved but unreachable")
				continue
			}
			for key, raw := range rows {
				if err := db.applyRestoredLocked(coll, key, raw); err != nil {
					return fmt.Errorf("datastore: restore %s/%s: %w", name, key, err)
				}
				records++
			}
		}
		rep.SnapshotRecords = records
	}

	open := openWAL
	if db.readOnly {
		open = openWALRead
	}
	w, err := open(db.opts.Dir, db.openLog)
	if err != nil {
		return err
	}
	db.wal = w

	res, err := w.replay(db.seq, func(fr *walFrame) error {
		if err := db.applyFrameLocked(fr); err != nil {
			return err
		}
		db.seq = fr.Seq
		return nil
	})
	if err != nil {
		_ = w.close()
		return err
	}

	if res.Truncated {
		// Loud on purpose. A truncated tail means acknowledged-but-unsynced
		// work was lost, and somebody should know which byte it stopped at.
		db.log.Error().
			Str("dir", db.opts.Dir).
			Int64("offset", res.At).
			Uint64("last_seq", db.seq).
			Str("reason", res.Reason).
			Msg("wal_truncated")
	}

	rep.WALFrames = res.Frames
	rep.Truncated = res.Truncated
	rep.TruncatedAt = res.At
	rep.TruncatedReason = res.Reason
	rep.Seq = db.seq
	db.recovery = rep

	db.log.Info().
		Str("dir", db.opts.Dir).
		Uint64("seq", db.seq).
		Int("snapshot_records", records).
		Int("wal_frames", res.Frames).
		Int("migrated_collections", len(rep.Migrated)).
		Msg("datastore_opened")
	return nil
}

// applyRestoredLocked applies one stored record, migrating it up to the declared
// schema version first when it came off disk at an older one.
//
// diskVersions is non-nil only while Open is restoring; it is cleared once the
// database is open, and from then on this is a plain applyPut — records written
// by this build are current by construction and must never be migrated again.
func (db *DB) applyRestoredLocked(coll anyCollection, key string, raw json.RawMessage) error {
	if db.diskVersions == nil {
		return coll.applyPut(key, raw)
	}
	from, tracked := db.diskVersions[coll.collName()]
	if tracked && from < coll.schemaVersion() {
		migrated, err := coll.migrateRaw(from, raw)
		if err != nil {
			return err
		}
		raw = migrated
	}
	return coll.applyPut(key, raw)
}

func (db *DB) applyFrameLocked(fr *walFrame) error {
	for _, op := range fr.Ops {
		coll, known := db.colls[op.Coll]
		if !known {
			rows, ok := db.orphans[op.Coll]
			if !ok {
				rows = make(map[string]json.RawMessage)
				db.orphans[op.Coll] = rows
			}
			if op.Op == opDelete {
				delete(rows, op.Key)
			} else {
				rows[op.Key] = op.Val
			}
			continue
		}
		if op.Op == opDelete {
			coll.applyDelete(op.Key)
			continue
		}
		// During recovery this migrates; once open, diskVersions is empty and
		// applyRestoredLocked is a plain applyPut.
		if err := db.applyRestoredLocked(coll, op.Key, op.Val); err != nil {
			return err
		}
	}
	for k, v := range fr.Counters {
		if v > db.counters[k] {
			db.counters[k] = v
		}
	}
	return nil
}

func (db *DB) startWorkersLocked() {
	if db.opts.Sync == SyncInterval {
		db.wg.Add(1)
		go db.syncLoop()
	}
	if db.opts.CompactInterval > 0 {
		db.wg.Add(1)
		go db.compactLoop()
	}
}

func (db *DB) syncLoop() {
	defer db.wg.Done()
	t := time.NewTicker(db.opts.SyncEvery)
	defer t.Stop()
	for {
		select {
		case <-db.stop:
			return
		case <-t.C:
			// Clear the flag before syncing: a commit landing mid-sync sets it
			// again and is picked up next tick, rather than being lost.
			db.mu.Lock()
			pending := db.unsynced
			db.unsynced = false
			w := db.wal
			db.mu.Unlock()
			if pending && w != nil {
				if err := w.sync(); err != nil {
					// A failed fsync means unknown durability from here on: the
					// kernel may have dropped the dirty pages, and a later sync
					// can report success without them. Stop accepting writes.
					db.fail(fmt.Errorf("interval sync: %w", err))
				}
			}
		}
	}
}

func (db *DB) compactLoop() {
	defer db.wg.Done()
	t := time.NewTicker(db.opts.CompactInterval)
	defer t.Stop()
	for {
		select {
		case <-db.stop:
			return
		case <-t.C:
			if err := db.Compact(); err != nil {
				db.log.Error().Err(err).Msg("compaction_failed")
			}
		}
	}
}

// Dir returns the directory this database owns.
func (db *DB) Dir() string { return db.opts.Dir }

// Close stops background work, compacts, and releases the lock.
func (db *DB) Close() error {
	db.mu.Lock()
	if !db.opened || db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	readOnly := db.readOnly
	db.mu.Unlock()

	close(db.stop)
	db.wg.Wait()
	db.closeSubscriptions()

	// A reader owns nothing on disk: no compaction, no lock to release.
	var err error
	if !readOnly {
		err = db.Compact()
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.wal != nil {
		if cerr := db.wal.close(); err == nil {
			err = cerr
		}
		db.wal = nil
	}
	if db.lock != nil {
		if lerr := db.lock.release(); err == nil {
			err = lerr
		}
		db.lock = nil
	}
	db.opened = false
	return err
}

// Compact folds the log into a fresh snapshot and empties it.
func (db *DB) Compact() error {
	// Compaction must exclude writers, not merely readers.
	//
	// A commit appends and fsyncs its frame *before* taking the state lock to
	// apply it. A compaction that only took the state lock could slip into that
	// window: snapshot the state at sequence N, truncate the log, and destroy
	// the already-durable frame N+1 that the writer is about to apply. Memory
	// would then hold a transaction that no longer exists on disk, and the next
	// restart would silently lose an acknowledged order.
	//
	// Holding the writer slot closes the window.
	db.mu.RLock()
	readOnly := db.readOnly
	db.mu.RUnlock()
	if readOnly {
		return ErrReadOnly
	}
	if err := db.failedErr(); err != nil {
		return err
	}

	if err := db.acquireWriter(); err != nil {
		return err
	}
	defer db.releaseWriter()
	return db.compactExclusive()
}

// compactExclusive does the work, assuming the caller already holds the writer
// slot. Order matters and is not negotiable: write and fsync the new snapshot,
// then truncate the log. A crash between the two leaves a snapshot plus a log
// that replays cleanly on top of it — wasteful, never wrong. The other order
// loses everything since the last snapshot.
func (db *DB) compactExclusive() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.wal == nil {
		return ErrNotOpen
	}

	snap := &snapshotFile{
		Seq:         db.seq,
		TS:          db.opts.Clock().Unix(),
		Counters:    make(map[string]uint64, len(db.counters)),
		Versions:    make(map[string]int, len(db.colls)),
		Collections: make(map[string]map[string]json.RawMessage, len(db.colls)+len(db.orphans)),
	}
	for k, v := range db.counters {
		snap.Counters[k] = v
	}
	for name, coll := range db.colls {
		snap.Collections[name] = coll.dump()
		if v := coll.schemaVersion(); v > 0 {
			snap.Versions[name] = v
		}
	}
	for name, rows := range db.orphans {
		snap.Collections[name] = rows
		if v := db.orphanVersions[name]; v > 0 {
			snap.Versions[name] = v
		}
	}

	if err := writeSnapshot(db.opts.Dir, snap); err != nil {
		return err
	}
	// The snapshot is durable, so the log's contents are now redundant for
	// recovery: either empty it, or keep it aside as history.
	if db.archivingEnabled() {
		if err := db.archiveLogLocked(db.seq); err != nil {
			return err
		}
	} else if err := db.wal.reset(); err != nil {
		return err
	}
	if err := pruneSnapshots(db.opts.Dir, db.opts.KeepSnapshots); err != nil {
		return err
	}
	if db.archivingEnabled() {
		if err := db.pruneArchives(db.opts.Clock()); err != nil {
			return err
		}
	}

	db.lastCompact = db.opts.Clock()
	db.unsynced = false
	db.log.Info().Uint64("seq", db.seq).Msg("datastore_compacted")
	return nil
}

// Backup writes a self-contained copy into dst, which is created if needed.
//
// The result is a directory you can point Options.Dir at. Restoring is copying
// it back — there is no restore command because there is nothing to do.
func (db *DB) Backup(dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("datastore: create %s: %w", dst, err)
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	if !db.opened {
		return ErrNotOpen
	}

	snap := &snapshotFile{
		Seq:         db.seq,
		TS:          db.opts.Clock().Unix(),
		Counters:    make(map[string]uint64, len(db.counters)),
		Versions:    make(map[string]int, len(db.colls)),
		Collections: make(map[string]map[string]json.RawMessage, len(db.colls)+len(db.orphans)),
	}
	for k, v := range db.counters {
		snap.Counters[k] = v
	}
	for name, coll := range db.colls {
		snap.Collections[name] = coll.dump()
		if v := coll.schemaVersion(); v > 0 {
			snap.Versions[name] = v
		}
	}
	for name, rows := range db.orphans {
		snap.Collections[name] = rows
		if v := db.orphanVersions[name]; v > 0 {
			snap.Versions[name] = v
		}
	}

	if err := writeSnapshot(dst, snap); err != nil {
		return err
	}
	// No log is copied: the snapshot already contains every committed change,
	// and an empty directory plus a snapshot is a complete database.
	_ = os.Remove(filepath.Join(dst, walName))
	db.log.Info().Str("dst", dst).Uint64("seq", db.seq).Msg("datastore_backed_up")
	return nil
}

// checkBudgetLocked warns once per crossing when the dataset outgrows the
// configured budget. Called with the state lock held.
func (db *DB) checkBudgetLocked() {
	if db.opts.MaxBytes <= 0 {
		return
	}
	total := db.bytesLocked()
	threshold := db.opts.MaxBytes * int64(db.opts.WarnAtPercent) / 100
	over := total >= threshold
	if over == db.overBudget {
		return // only speak when the answer changes
	}
	db.overBudget = over
	if over {
		db.log.Warn().
			Int64("bytes", total).
			Int64("budget", db.opts.MaxBytes).
			Int("warn_at_percent", db.opts.WarnAtPercent).
			Msg("dataset_approaching_memory_budget")
		return
	}
	db.log.Info().Int64("bytes", total).Int64("budget", db.opts.MaxBytes).
		Msg("dataset_back_within_memory_budget")
}

func (db *DB) bytesLocked() int64 {
	var total int64
	for _, coll := range db.colls {
		total += int64(coll.bytes())
	}
	for _, rows := range db.orphans {
		for k, v := range rows {
			total += int64(len(k) + len(v))
		}
	}
	return total
}

// Stats is a point-in-time summary, for an admin screen or a health check.
type Stats struct {
	Seq         uint64
	Collections map[string]int
	Orphans     map[string]int
	Counters    map[string]uint64
	WALBytes    int64
	LastCompact time.Time

	// Bytes is the encoded size of every record held in memory, and Budget is
	// Options.MaxBytes (0 when unset). Watch the ratio: this design assumes the
	// data fits comfortably in RAM, and this is the number that says whether it
	// still does.
	Bytes  int64
	Budget int64

	// ArchiveSegments and ArchiveBytes describe retained history on disk (see
	// Options.HistorySegments). Zero when archiving is off.
	ArchiveSegments int
	ArchiveBytes    int64
}

func (db *DB) Stats() Stats {
	db.mu.RLock()

	s := Stats{
		Seq:         db.seq,
		Collections: make(map[string]int, len(db.colls)),
		Orphans:     make(map[string]int, len(db.orphans)),
		Counters:    make(map[string]uint64, len(db.counters)),
		LastCompact: db.lastCompact,
	}
	for name, coll := range db.colls {
		s.Collections[name] = coll.count()
	}
	for name, rows := range db.orphans {
		s.Orphans[name] = len(rows)
	}
	for k, v := range db.counters {
		s.Counters[k] = v
	}
	if db.wal != nil {
		s.WALBytes = db.wal.sizeNow()
	}
	s.Bytes = db.bytesLocked()
	s.Budget = db.opts.MaxBytes
	db.mu.RUnlock()

	// Archive stats walk the directory. Filesystem work has no business under
	// the state lock, where it would stall the writer's apply step.
	if db.archivingEnabled() {
		s.ArchiveSegments, s.ArchiveBytes = db.archiveStats()
	}
	return s
}
