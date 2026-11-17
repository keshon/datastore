package datastore

import (
	"encoding/json"
	"fmt"
	"iter"
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

	writes      map[string]map[string]pendingOp
	counters    map[string]uint64
	meta        map[string]string
	stagedBytes int64
	stagedOps   int
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
// One transaction body runs at a time. fn executes without the state lock
// held, so readers are never blocked by a transaction's body or by its fsync —
// only by the brief moment where an already-durable commit is applied to
// memory.
//
// Under SyncAlways the writer slot is handed back once the frame is in the log,
// before the fsync, so the next transaction runs its body and appends while
// this one is still waiting for the disk. Several commits then share a single
// fsync. Update still returns only when this commit is durable, and nothing is
// visible to any reader before that (see groupcommit.go).
//
// A transaction that reads waits for commits already in flight to become
// durable, so a read-then-write sequence still sees everything that precedes
// it. Write-only transactions — including Collection.Put and Collection.Delete
// — never wait, which is why throughput scales with concurrent writers.
//
// Do not call Update from inside fn — including indirectly, through the
// convenience methods Collection.Put and Collection.Delete, which are each a
// transaction of their own. Doing so is caught and panics immediately with an
// explanation; use In(tx, c) to write to a collection from inside a transaction.
func (db *DB) Update(fn func(*Tx) error) error {
	if err := db.acquireWriter(); err != nil {
		return err
	}
	// The slot is released as soon as the frame is in the log, not when this
	// returns: waiting for the fsync while holding it is what made concurrent
	// commits serialise. commit hands it back at the right moment; the defer is
	// the backstop for every path that returns before then.
	released := false
	release := func() {
		if !released {
			released = true
			db.releaseWriter()
		}
	}
	defer release()

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
	if err := db.failedErr(); err != nil {
		return err
	}

	tx := &Tx{
		db:    db,
		write: true,
	}

	if err := fn(tx); err != nil {
		tx.done = true
		return err
	}
	tx.done = true
	return db.commit(tx, release)
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

func (db *DB) acquireWriter() error {
	select {
	case db.writeCh <- struct{}{}:
		db.writerGoid.Store(goid())
		return nil
	default:
	}

	// The slot is taken. If this very goroutine holds it, waiting is futile —
	// it can never be freed by the goroutine that is waiting for it. That is a
	// programming error, not a runtime condition, so it panics at once, while
	// the stack still shows who did it.
	if id := goid(); id != 0 && db.writerGoid.Load() == id {
		panic(reentrantMsg)
	}

	timer := time.NewTimer(db.opts.WriteTimeout)
	defer timer.Stop()
	select {
	case db.writeCh <- struct{}{}:
		db.writerGoid.Store(goid())
		return nil
	case <-timer.C:
		// A slow disk or a long compaction is a runtime condition, not a bug:
		// report it as an error the caller can handle, retry, or log.
		return fmt.Errorf("%w after %s; if another goroutine is stuck inside "+
			"Update, that is the thing to fix", ErrWriteTimeout, db.opts.WriteTimeout)
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
func (db *DB) commit(tx *Tx, release func()) error {
	if len(tx.writes) == 0 && len(tx.counters) == 0 {
		return nil
	}

	collections := sortedKeys(tx.writes)
	frame := &walFrame{}
	if tx.stagedOps != 0 {
		frame.Ops = make([]walOp, 0, tx.stagedOps)
	}
	db.mu.RLock()
	if db.failed != nil {
		failure := db.failed
		db.mu.RUnlock()
		return fmt.Errorf("%w: %v", ErrFailed, failure)
	}
	for _, name := range collections {
		coll, known := db.colls[name]
		if !known {
			db.mu.RUnlock()
			return fmt.Errorf("datastore: collection %q is not registered", name)
		}
		// Frames in the commit window are in the log and will be applied, so
		// for uniqueness they already exist. Waiting for them instead would be
		// simpler, and would also stop any collection with a unique index from
		// ever batching.
		var err error
		frame.Ops, err = coll.prepare(tx.writes[name], frame.Ops)
		if err != nil {
			db.mu.RUnlock()
			return err
		}
	}
	db.mu.RUnlock()

	// From appendSeq, not seq: seq is what has been applied, which lags
	// whenever frames are in flight. Only the writer slot assigns sequences, so
	// this needs no lock of its own.
	seq := db.appendSeq + 1

	frame.Seq = seq
	frame.TS = db.opts.Clock().Unix()
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

	buf, err := encodeFrame(frame)
	if err != nil {
		return err // nothing touched the disk; the store is still healthy
	}

	sync := db.opts.Sync == SyncAlways
	w := db.wal // keep the WAL owner alive if Close follows the settled frame
	if sync {
		err = db.waitPendingCapacity(int64(len(buf)))
		if err != nil {
			return err
		}
	}
	err = db.failedErr()
	if err != nil {
		return err
	}
	started := time.Now()

	// Append without syncing. Under SyncAlways the fsync happens below, shared
	// with every other commit that lands in the same window; under the other
	// modes it does not happen here at all.
	if err := db.wal.append(buf, false); err != nil {
		// The log may now hold a torn frame. Anything appended after it would
		// be silently dropped by the next replay while this process kept
		// acknowledging commits — so stop accepting writes instead.
		db.fail(err)
		return fmt.Errorf("%w: %v", ErrFailed, err)
	}
	db.appendSeq = seq

	if sync {
		pf := db.enqueue(frame)

		// Hand back the writer slot before waiting. This is the whole point:
		// the next commit runs its body and appends while this fsync is in
		// flight, and then shares it.
		release()

		if err := db.awaitDurable(pf); err != nil {
			return err
		}
		durable := time.Since(started)

		walBytes := w.sizeNow()
		needCompact := walBytes >= db.opts.CompactAfterBytes

		if db.opts.OnCommit != nil {
			db.opts.OnCommit(CommitStats{
				Seq:        frame.Seq,
				Ops:        len(frame.Ops),
				FrameBytes: frame.encodedSize,
				Duration:   durable,
				Synced:     true,
				WALBytes:   walBytes,
			})
		}
		if needCompact {
			// Compact, not compactExclusive: the writer slot was handed back
			// above, so this has to queue for it like anybody else.
			if err := db.Compact(); err != nil {
				db.log.Error().Err(err).Msg("compaction_failed")
			}
		}
		return nil
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
		// The frame is already durable, so state and log have diverged. That is
		// not recoverable in place, and every later write would build on state
		// the log does not describe — so latch the failure. The next Open
		// replays the frame and fails the same way, which is the honest outcome.
		if db.failed == nil {
			db.failed = fmt.Errorf("applying durable frame %d: %w", frame.Seq, err)
			db.log.Error().Err(db.failed).Msg("datastore_failed: refusing further writes")
		}
		db.mu.Unlock()
		return fmt.Errorf("datastore: applying a durable frame failed, state is inconsistent: %w", err)
	}
	db.seq = frame.Seq
	if !sync {
		db.unsynced = true
	}
	walBytes := db.wal.sizeNow()
	needCompact := walBytes >= db.opts.CompactAfterBytes
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
	if tx.counters == nil {
		tx.counters = make(map[string]uint64)
	}
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

// withRead runs f under the state lock, or directly when the caller already
// holds it.
//
// A writing transaction first closes the commit window. Frames that are in the
// log but not yet applied are invisible to every reader, so a read-then-write
// sequence has to wait for them or it would decide against state that is
// already out of date. Read-only transactions need nothing: they can only ever
// see applied state, which is by definition already durable.
func (tx *Tx) withRead(f func()) {
	if tx.write && !tx.db.pendingEmpty() {
		_ = tx.db.drainPending()
	}

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
				// Unreachable in practice: the bytes came from Put's Marshal
				// moments ago. Logged for the same reason as in getLocked.
				tc.tx.db.log.Error().Err(err).Str("collection", tc.c.name).Str("key", key).
					Msg("record_undecodable: hidden from reads")
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
	return tc.stage(key, pendingOp{raw: raw})
}

// Delete stages a removal. Removing an absent key is not an error.
func (tc *TxCollection[T]) Delete(key string) error {
	if err := tc.tx.checkWritable(); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("%w: %s", ErrEmptyKey, tc.c.name)
	}
	return tc.stage(key, pendingOp{del: true})
}

func (tc *TxCollection[T]) stage(key string, op pendingOp) error {
	old, exists := tc.tx.writes[tc.c.name][key]
	bytes := tc.tx.stagedBytes + int64(len(op.raw)-len(old.raw))
	opCount := tc.tx.stagedOps
	if !exists {
		bytes += int64(len(tc.c.name) + len(key))
		opCount++
	}
	options := &tc.tx.db.opts
	if options.MaxTransactionBytes > 0 && bytes > options.MaxTransactionBytes {
		return fmt.Errorf("%w: staged bytes %d exceed %d", ErrTransactionTooLarge, bytes, options.MaxTransactionBytes)
	}
	if options.MaxTransactionOps > 0 && opCount > options.MaxTransactionOps {
		return fmt.Errorf("%w: staged operations %d exceed %d", ErrTransactionTooLarge, opCount, options.MaxTransactionOps)
	}

	if tc.tx.writes == nil {
		tc.tx.writes = make(map[string]map[string]pendingOp)
	}
	ops, ok := tc.tx.writes[tc.c.name]
	if !ok {
		ops = make(map[string]pendingOp)
		tc.tx.writes[tc.c.name] = ops
	}
	ops[key] = op
	tc.tx.stagedBytes = bytes
	tc.tx.stagedOps = opCount
	return nil
}

// Len reports how many records the collection holds as this transaction sees
// it: the committed count adjusted for its own staged writes.
func (tc *TxCollection[T]) Len() int {
	var n int
	tc.tx.withRead(func() {
		n = len(tc.c.data)
		for key, op := range tc.tx.writes[tc.c.name] {
			_, exists := tc.c.data[key]
			if op.del && exists {
				n--
			}
			if !op.del && !exists {
				n++
			}
		}
	})
	return n
}

// Keys returns every key as this transaction sees it, sorted: the committed
// keys, plus its own staged puts, minus its own staged deletes.
func (tc *TxCollection[T]) Keys() []string {
	merged := make(map[string]struct{})
	tc.tx.withRead(func() {
		for k := range tc.c.data {
			merged[k] = struct{}{}
		}
	})
	for key, op := range tc.tx.writes[tc.c.name] {
		if op.del {
			delete(merged, key)
		} else {
			merged[key] = struct{}{}
		}
	}
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// All iterates every record in key order as this transaction sees it,
// including its own uncommitted writes.
func (tc *TxCollection[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, k := range tc.Keys() {
			v, ok := tc.Get(k)
			if !ok {
				continue
			}
			if !yield(v) {
				return
			}
		}
	}
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

// --- indexes inside a transaction -----------------------------------------
//
// Index methods called directly — byCustomer.Find(...) — take the state lock,
// and View holds that lock for its whole callback: calling one from the other
// deadlocks the moment a writer is queued. These wrappers are the way to read
// an index from inside View or Update.
//
// They read committed state. A transaction's own staged writes are not indexed
// until commit, so a record put earlier in the same transaction is not yet
// findable through any index — read it back through In(tx, c).Get instead.

// TxIndex is a multi-valued index as read from inside a transaction.
type TxIndex[T Entity] struct {
	tx *Tx
	i  *Index[T]
}

// InIndex scopes a multi-valued index to a transaction.
func InIndex[T Entity](tx *Tx, i *Index[T]) *TxIndex[T] {
	if i.coll.db != tx.db {
		panic("datastore: index " + i.name + " belongs to a different database")
	}
	return &TxIndex[T]{tx: tx, i: i}
}

// Find returns every committed record carrying the term, in key order.
func (ti *TxIndex[T]) Find(term string) []T {
	var out []T
	ti.tx.withRead(func() { out = ti.i.findLocked(term) })
	return out
}

// All iterates every committed record carrying the term, in key order, decoding
// one at a time. See Index.All.
func (ti *TxIndex[T]) All(term string) iter.Seq[T] {
	return func(yield func(T) bool) {
		ti.tx.withRead(func() { ti.i.iterateLocked(term, yield) })
	}
}

// Count reports how many committed records carry the term.
func (ti *TxIndex[T]) Count(term string) int {
	var n int
	ti.tx.withRead(func() { n = len(ti.i.byTerm[term]) })
	return n
}

// Terms lists every term currently present, sorted.
func (ti *TxIndex[T]) Terms() []string {
	var out []string
	ti.tx.withRead(func() { out = ti.i.termsLocked() })
	return out
}

// TxUniqueIndex is a unique index as read from inside a transaction.
type TxUniqueIndex[T Entity] struct {
	tx *Tx
	u  *UniqueIndex[T]
}

// InUnique scopes a unique index to a transaction.
func InUnique[T Entity](tx *Tx, u *UniqueIndex[T]) *TxUniqueIndex[T] {
	if u.coll.db != tx.db {
		panic("datastore: index " + u.name + " belongs to a different database")
	}
	return &TxUniqueIndex[T]{tx: tx, u: u}
}

// Get returns the committed record holding the term, if any.
func (tu *TxUniqueIndex[T]) Get(term string) (T, bool) {
	var (
		v  T
		ok bool
	)
	tu.tx.withRead(func() { v, ok = tu.u.getLocked(term) })
	return v, ok
}

// TxSortedIndex is a sorted index as read from inside a transaction.
type TxSortedIndex[T Entity] struct {
	tx *Tx
	s  *SortedIndex[T]
}

// InSorted scopes a sorted index to a transaction.
func InSorted[T Entity](tx *Tx, s *SortedIndex[T]) *TxSortedIndex[T] {
	if s.coll.db != tx.db {
		panic("datastore: index " + s.name + " belongs to a different database")
	}
	return &TxSortedIndex[T]{tx: tx, s: s}
}

// Range returns committed records whose key falls in [lo, hi], ascending.
func (ts *TxSortedIndex[T]) Range(lo, hi int64) []T {
	var out []T
	ts.tx.withRead(func() { out = ts.s.rangeLocked(lo, hi) })
	return out
}

// All iterates committed records whose key falls in [lo, hi], ascending,
// decoding one at a time. See SortedIndex.All.
func (ts *TxSortedIndex[T]) All(lo, hi int64) iter.Seq[T] {
	return func(yield func(T) bool) {
		ts.tx.withRead(func() { ts.s.iterateLocked(lo, hi, yield) })
	}
}

// Asc returns up to limit committed records in ascending order, skipping
// offset. A limit of zero means no limit.
func (ts *TxSortedIndex[T]) Asc(limit, offset int) []T {
	var out []T
	ts.tx.withRead(func() { out = ts.s.materialise(window(ts.s.entries, limit, offset, false)) })
	return out
}

// Desc returns up to limit committed records in descending order, skipping
// offset. A limit of zero means no limit.
func (ts *TxSortedIndex[T]) Desc(limit, offset int) []T {
	var out []T
	ts.tx.withRead(func() { out = ts.s.materialise(window(ts.s.entries, limit, offset, true)) })
	return out
}

// Len reports how many committed records are indexed.
func (ts *TxSortedIndex[T]) Len() int {
	var n int
	ts.tx.withRead(func() { n = len(ts.s.entries) })
	return n
}
