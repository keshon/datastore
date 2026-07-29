package datastore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
	// Zero disables the timer.
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
	// panicking. Default 30s. It exists to turn a re-entrant Update — calling
	// Update from inside Update — into a diagnosable panic instead of a
	// process that hangs forever.
	WriteTimeout time.Duration
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

	colls map[string]anyCollection
	order []string

	// orphans holds records for collections this build did not register.
	//
	// They are carried through compaction untouched. Without that, commenting
	// out a Register call and deploying would silently delete a table at the
	// next compaction — a failure with no error, no log line, and no way back.
	orphans map[string]map[string]json.RawMessage

	seq      uint64
	counters map[string]uint64

	wal  *wal
	lock *dirLock

	opened bool
	closed bool

	stop        chan struct{}
	wg          sync.WaitGroup
	lastCompact time.Time
	unsynced    bool
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
		opts:     opts,
		log:      log,
		writeCh:  make(chan struct{}, 1),
		colls:    make(map[string]anyCollection),
		orphans:  make(map[string]map[string]json.RawMessage),
		counters: make(map[string]uint64),
		stop:     make(chan struct{}),
	}
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
		return fmt.Errorf("datastore: Options.Dir is required")
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
	defer db.mu.Unlock()

	db.lock = lock
	if err := db.restoreLocked(); err != nil {
		_ = lock.release()
		db.lock = nil
		return err
	}

	db.opened = true
	db.lastCompact = db.opts.Clock()
	db.startWorkersLocked()
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

	var records int
	if snap != nil {
		db.seq = snap.Seq
		for k, v := range snap.Counters {
			db.counters[k] = v
		}
		for name, rows := range snap.Collections {
			coll, known := db.colls[name]
			if !known {
				db.orphans[name] = rows
				db.log.Warn().Str("collection", name).Int("records", len(rows)).
					Msg("collection_unregistered: preserved but unreachable")
				continue
			}
			for key, raw := range rows {
				if err := coll.applyPut(key, raw); err != nil {
					return fmt.Errorf("datastore: restore %s/%s: %w", name, key, err)
				}
				records++
			}
		}
	}

	w, err := openWAL(db.opts.Dir)
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

	db.log.Info().
		Str("dir", db.opts.Dir).
		Uint64("seq", db.seq).
		Int("snapshot_records", records).
		Int("wal_frames", res.Frames).
		Msg("datastore_opened")
	return nil
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
		if err := coll.applyPut(op.Key, op.Val); err != nil {
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
			db.mu.Lock()
			pending := db.unsynced
			db.unsynced = false
			w := db.wal
			db.mu.Unlock()
			if pending && w != nil {
				if err := w.sync(); err != nil {
					db.log.Error().Err(err).Msg("wal_sync_failed")
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

// Close stops background work, compacts, and releases the lock.
func (db *DB) Close() error {
	db.mu.Lock()
	if !db.opened || db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()

	close(db.stop)
	db.wg.Wait()

	err := db.Compact()

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
	db.acquireWriter()
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
		Collections: make(map[string]map[string]json.RawMessage, len(db.colls)+len(db.orphans)),
	}
	for k, v := range db.counters {
		snap.Counters[k] = v
	}
	for name, coll := range db.colls {
		snap.Collections[name] = coll.dump()
	}
	for name, rows := range db.orphans {
		snap.Collections[name] = rows
	}

	if err := writeSnapshot(db.opts.Dir, snap); err != nil {
		return err
	}
	if err := db.wal.reset(); err != nil {
		return err
	}
	if err := pruneSnapshots(db.opts.Dir, db.opts.KeepSnapshots); err != nil {
		return err
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
		Collections: make(map[string]map[string]json.RawMessage, len(db.colls)+len(db.orphans)),
	}
	for k, v := range db.counters {
		snap.Counters[k] = v
	}
	for name, coll := range db.colls {
		snap.Collections[name] = coll.dump()
	}
	for name, rows := range db.orphans {
		snap.Collections[name] = rows
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

// Stats is a point-in-time summary, for an admin screen or a health check.
type Stats struct {
	Seq         uint64
	Collections map[string]int
	Orphans     map[string]int
	Counters    map[string]uint64
	WALBytes    int64
	LastCompact time.Time
}

func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()

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
		s.WALBytes = db.wal.size
	}
	return s
}
