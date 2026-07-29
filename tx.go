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
}

// Update runs fn in a read-write transaction and commits if it returns nil.
//
// One writer runs at a time. fn executes without the state lock held, so
// readers are never blocked by a transaction's body or by its fsync — only by
// the brief moment where an already-durable commit is applied to memory.
//
// Do not call Update from inside fn. There is no way to detect that reliably in
// Go, so instead of hanging forever the writer slot times out and panics with
// an explanation.
func (db *DB) Update(fn func(*Tx) error) error {
	db.acquireWriter()
	defer db.releaseWriter()

	db.mu.RLock()
	ready := db.opened && !db.closed
	db.mu.RUnlock()
	if !ready {
		return ErrNotOpen
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

func (db *DB) acquireWriter() {
	select {
	case db.writeCh <- struct{}{}:
		return
	default:
	}
	timer := time.NewTimer(db.opts.WriteTimeout)
	defer timer.Stop()
	select {
	case db.writeCh <- struct{}{}:
	case <-timer.C:
		panic(fmt.Sprintf(
			"datastore: no writer slot after %s. The usual cause is calling Update "+
				"from inside another Update; a transaction must not start one.",
			db.opts.WriteTimeout))
	}
}

func (db *DB) releaseWriter() { <-db.writeCh }

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

	sync := db.opts.Sync == SyncAlways
	if err := db.wal.append(frame, sync); err != nil {
		return err
	}

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
	db.mu.Unlock()

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

// NextOrderNum returns a human-facing order number: the two-digit year and
// month, a slash, and a counter that restarts each month — "2607/14".
//
// The shape is inherited from the legacy shop and is printed on invoices and
// quoted in support threads, so it is frozen. See docs/conventions.md.
func (tx *Tx) NextOrderNum(t time.Time) string {
	bucket := t.Format("0601")
	return fmt.Sprintf("%s/%d", bucket, tx.NextID("order:"+bucket))
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
