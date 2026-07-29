package datastore

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// testHookAfterAppend runs between a frame reaching the disk and being applied
// to memory. It is nil outside tests; see the call site in commit.
var testHookAfterAppend func()

type pendingOp struct {
	del bool
	raw json.RawMessage
}

// Tx is a unit of work. Writes made through it become visible — and durable —
// together or not at all.
type Tx struct {
	db    *DB
	write bool
	done  bool

	// holdsLock records that the caller already holds the read lock, which is
	// true inside View. Taking sync.RWMutex.RLock recursively can deadlock
	// against a waiting writer, so reads have to know.
	holdsLock bool

	writes   map[string]map[string]pendingOp
	counters map[string]uint64
	meta     map[string]string
}

// Note attaches context to this transaction — who asked for the change and why.
//
// The pair is recorded in the log alongside the commit and read back through
// History, which is what turns the log into an audit trail: "order 41 was
// refunded by user 7 on Tuesday" is answerable from data the store was already
// writing. A rolled-back transaction records nothing.
//
// Notes describe the commit, not any single record. Later calls with the same
// key replace earlier ones.
func (tx *Tx) Note(key, value string) {
	if !tx.write || tx.done {
		return // a read-only or finished transaction commits nothing
	}
	if tx.meta == nil {
		tx.meta = make(map[string]string, 4)
	}
	tx.meta[key] = value
}

// Update runs fn in a read-write transaction and commits if it returns nil.
//
// One writer runs at a time. fn executes without the state lock held, so
// readers are never blocked by a transaction's body or by its fsync — only by
// the brief moment where an already-durable commit is applied to memory.
//
// Do not call Update from inside fn — including indirectly, through the
// convenience methods Collection.Put and Collection.Delete, which are each a
// transaction of their own. Doing so is caught and panics immediately with an
// explanation; use In(tx, c) to write to a collection from inside a transaction.
func (db *DB) Update(fn func(*Tx) error) error {
	db.acquireWriter()
	defer db.releaseWriter()

	db.mu.RLock()
	ready := db.opened && !db.closed
	readOnly := db.readOnly
	db.mu.RUnlock()
	if !ready {
		return ErrNotOpen
	}
	if readOnly {
		return ErrReadOnly
	}

	tx := &Tx{
		db:       db,
		write:    true,
		writes:   make(map[string]map[string]pendingOp),
		counters: make(map[string]uint64),
	}

	if err := fn(tx); err != nil {
		tx.done = true
		return err
	}
	tx.done = true
	return db.commit(tx)
}

// View runs fn in a read-only transaction. The read lock is held throughout, so
// keep it short and do no I/O inside.
func (db *DB) View(fn func(*Tx) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if !db.opened || db.closed {
		return ErrNotOpen
	}
	tx := &Tx{db: db, holdsLock: true}
	defer func() { tx.done = true }()
	return fn(tx)
}

const reentrantMsg = "datastore: Update called from inside another Update on the same " +
	"goroutine — it can never proceed. Write through In(tx, collection) instead of " +
	"Collection.Put/Delete (each of those is its own transaction)."

func (db *DB) acquireWriter() {
	select {
	case db.writeCh <- struct{}{}:
		db.writerGoid.Store(goid())
		return
	default:
	}

	// The slot is taken. If this very goroutine holds it, waiting is futile: the
	// only outcome is the timeout panic, WriteTimeout from now. Say so at once,
	// while the stack still shows who did it.
	if id := goid(); id != 0 && db.writerGoid.Load() == id {
		panic(reentrantMsg)
	}

	timer := time.NewTimer(db.opts.WriteTimeout)
	defer timer.Stop()
	select {
	case db.writeCh <- struct{}{}:
		db.writerGoid.Store(goid())
	case <-timer.C:
		panic(fmt.Sprintf(
			"datastore: no writer slot after %s. The usual cause is calling Update "+
				"from inside another Update; a transaction must not start one.",
			db.opts.WriteTimeout))
	}
}

// releaseWriter clears the owner before freeing the slot, so the next holder
// cannot have its id overwritten by this one.
func (db *DB) releaseWriter() {
	db.writerGoid.Store(0)
	<-db.writeCh
}

// commit validates, makes the change durable, and only then applies it.
//
// The ordering is the whole design. Everything that can fail — encoding,
// uniqueness — happens before the frame is written. Applying to memory and
// indexes happens after the fsync and cannot fail, so the log and the state
// can never disagree about what happened.
func (db *DB) commit(tx *Tx) error {
	if len(tx.writes) == 0 && len(tx.counters) == 0 {
		return nil
	}

	db.mu.RLock()
	for _, name := range sortedKeys(tx.writes) {
		coll, known := db.colls[name]
		if !known {
			db.mu.RUnlock()
			return fmt.Errorf("datastore: collection %q is not registered", name)
		}
		puts := make(map[string]json.RawMessage)
		dels := make(map[string]struct{})
		for key, op := range tx.writes[name] {
			if op.del {
				dels[key] = struct{}{}
			} else {
				puts[key] = op.raw
			}
		}
		if err := coll.checkUnique(puts, dels); err != nil {
			db.mu.RUnlock()
			return err
		}
	}
	seq := db.seq + 1
	db.mu.RUnlock()

	frame := &walFrame{Seq: seq, TS: db.opts.Clock().Unix()}
	for _, name := range sortedKeys(tx.writes) {
		keys := make([]string, 0, len(tx.writes[name]))
		for k := range tx.writes[name] {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, key := range keys {
			op := tx.writes[name][key]
			if op.del {
				frame.Ops = append(frame.Ops, walOp{Coll: name, Key: key, Op: opDelete})
			} else {
				frame.Ops = append(frame.Ops, walOp{Coll: name, Key: key, Op: opPut, Val: op.raw})
			}
		}
	}
	if len(tx.counters) > 0 {
		frame.Counters = make(map[string]uint64, len(tx.counters))
		for k, v := range tx.counters {
			frame.Counters[k] = v
		}
	}
	if len(tx.meta) > 0 {
		frame.Meta = make(map[string]string, len(tx.meta))
		for k, v := range tx.meta {
			frame.Meta[k] = v
		}
	}

	sync := db.opts.Sync == SyncAlways
	started := time.Now()
	if err := db.wal.append(frame, sync); err != nil {
		return err
	}
	durable := time.Since(started)

	// The frame is now durable but not yet applied. This gap is the most
	// dangerous few microseconds in the package: anything that snapshots the
	// state and truncates the log in here would destroy an acknowledged
	// transaction. Holding the writer slot across it is what prevents that, and
	// this hook is how the test proves the prevention works — a probabilistic
	// test that merely hopes to land in the window passes either way.
	if hook := testHookAfterAppend; hook != nil {
		hook()
	}

	db.mu.Lock()
	if err := db.applyFrameLocked(frame); err != nil {
		db.mu.Unlock()
		// The frame is already durable, so state and log have diverged. That
		// is not recoverable in place; the next Open replays the frame and
		// will fail the same way, which is the honest outcome.
		return fmt.Errorf("datastore: applying a durable frame failed, state is inconsistent: %w", err)
	}
	db.seq = frame.Seq
	if !sync {
		db.unsynced = true
	}
	needCompact := db.wal.size >= db.opts.CompactAfterBytes
	walBytes := db.wal.size
	db.checkBudgetLocked()
	db.mu.Unlock()

	// The change is durable and applied, so a subscriber reading the database on
	// receipt sees it. Dispatch never blocks (see watch.go).
	db.dispatch(frame)

	if db.opts.OnCommit != nil {
		db.opts.OnCommit(CommitStats{
			Seq:        frame.Seq,
			Ops:        len(frame.Ops),
			FrameBytes: frame.encodedSize,
			Duration:   durable,
			Synced:     sync,
			WALBytes:   walBytes,
		})
	}

	if needCompact {
		// compactExclusive, not Compact: this goroutine already holds the
		// writer slot, and Compact would deadlock trying to take it again.
		if err := db.compactExclusive(); err != nil {
			// The commit stands; only the housekeeping failed.
			db.log.Error().Err(err).Msg("compaction_failed")
		}
	}
	return nil
}

// NextID allocates from a named monotonic sequence.
//
// The value is reserved within the transaction and only becomes permanent when
// it commits, so an abandoned transaction does not burn an id.
func (tx *Tx) NextID(name string) uint64 {
	if v, ok := tx.counters[name]; ok {
		tx.counters[name] = v + 1
		return v + 1
	}
	var cur uint64
	tx.withRead(func() { cur = tx.db.counters[name] })
	tx.counters[name] = cur + 1
	return cur + 1
}

// NextInPeriod allocates from a counter that restarts each period, and returns
// both the period label and the value — "2607", 14.
//
// It is the generic form of a human-facing document number: format the two as
// you like ("2607/14", "INV-2607-014"). The period comes from a time layout, so
// "0601" is monthly, "2006" yearly, "20060102" daily.
func (tx *Tx) NextInPeriod(name string, t time.Time, layout string) (string, uint64) {
	period := t.Format(layout)
	return period, tx.NextID(name + ":" + period)
}

func (tx *Tx) withRead(f func()) {
	if tx.holdsLock {
		f()
		return
	}
	tx.db.mu.RLock()
	defer tx.db.mu.RUnlock()
	f()
}

// --- typed access ---------------------------------------------------------

// TxCollection is a collection as seen from inside a transaction: reads see
// this transaction's own uncommitted writes layered over the committed state.
type TxCollection[T Entity] struct {
	tx *Tx
	c  *Collection[T]
}

// In scopes a collection to a transaction.
func In[T Entity](tx *Tx, c *Collection[T]) *TxCollection[T] {
	if c.db != tx.db {
		panic("datastore: collection " + c.name + " belongs to a different database")
	}
	return &TxCollection[T]{tx: tx, c: c}
}

// Get reads through the transaction's own writes first, so a record written
// earlier in the same transaction reads back as written.
func (tc *TxCollection[T]) Get(key string) (T, bool) {
	if ops, ok := tc.tx.writes[tc.c.name]; ok {
		if op, pending := ops[key]; pending {
			if op.del {
				var zero T
				return zero, false
			}
			v, err := tc.c.decode(op.raw)
			if err != nil {
				var zero T
				return zero, false
			}
			return v, true
		}
	}

	var (
		v  T
		ok bool
	)
	tc.tx.withRead(func() { v, ok = tc.c.getLocked(key) })
	return v, ok
}

// Put stages a record. Uniqueness is checked at commit, not here, so a
// transaction may legitimately exchange a unique value between two records.
func (tc *TxCollection[T]) Put(v T) error {
	if err := tc.tx.checkWritable(); err != nil {
		return err
	}
	key := v.Key()
	if key == "" {
		return fmt.Errorf("%w: %s", ErrEmptyKey, tc.c.name)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("datastore: encode %s/%s: %w", tc.c.name, key, err)
	}
	tc.stage(key, pendingOp{raw: raw})
	return nil
}

// Delete stages a removal. Removing an absent key is not an error.
func (tc *TxCollection[T]) Delete(key string) error {
	if err := tc.tx.checkWritable(); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("%w: %s", ErrEmptyKey, tc.c.name)
	}
	tc.stage(key, pendingOp{del: true})
	return nil
}

func (tc *TxCollection[T]) stage(key string, op pendingOp) {
	ops, ok := tc.tx.writes[tc.c.name]
	if !ok {
		ops = make(map[string]pendingOp)
		tc.tx.writes[tc.c.name] = ops
	}
	ops[key] = op
}

func (tx *Tx) checkWritable() error {
	if tx.done {
		return ErrTxDone
	}
	if !tx.write {
		return ErrReadOnly
	}
	return nil
}
