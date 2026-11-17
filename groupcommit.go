package datastore

// Group commit.
//
// A commit costs one fsync, and an fsync is roughly 97% of it. Because the
// writer slot was held across the fsync, that cost was also serialised: eight
// goroutines committing concurrently got the same throughput as one. Group
// commit removes the serialisation without removing the fsync — several
// commits share one.
//
// What it does not change is the guarantee. Update still returns only after
// the commit is durable, and nothing becomes visible to a reader before it is
// durable, because frames are applied to memory only after the fsync that
// covers them. The window between "appended" and "applied" is the whole
// mechanism, and everything below exists to keep that window invisible:
//
//   - A frame in the window is in the log but not in memory. Readers, inside a
//     transaction or outside one, never see it.
//   - A transaction that reads waits for the window to close first
//     (see Tx.withRead), so a read-then-write sequence still observes every
//     commit that precedes it. Only write-only transactions batch, which is
//     exactly the shape that needs the throughput.
//   - A unique index is the one constraint that spans transactions, so the
//     check at commit is taught to see frames in the window rather than
//     waiting for them. See Collection.checkUnique.
//   - Compaction drains the window before it snapshots, because truncating the
//     log with frames still in it would destroy commits somebody is waiting on.
//
// Only SyncAlways batches. The other modes never fsync on the commit path, so
// there is nothing to amortise and they keep applying inline.

import (
	"encoding/json"
	"fmt"
	"sync"
)

// pendingFrame is a frame that is in the log but not yet fsynced or applied.
type pendingFrame struct {
	frame *walFrame

	// settled is set by whichever goroutine resolved this frame's fate, under
	// groupMu. err is the outcome: nil once applied.
	settled bool
	err     error
}

// groupState is the coordination between committing goroutines. One of them
// becomes the leader, fsyncs once for everybody waiting, applies the batch in
// sequence order, and wakes them.
type groupState struct {
	mu   sync.Mutex
	cond *sync.Cond

	// pending holds appended-but-unapplied frames in sequence order.
	pending []*pendingFrame

	// flushing remains visible to constraint checks until the batch is applied.
	flushing []*pendingFrame
	bytes    int64 // encoded frames in pending and flushing, guarded by mu

	// syncing reports that a leader is mid-flush. Followers wait rather than
	// starting a second fsync, which would be pure contention.
	syncing bool
}

func newGroupState() *groupState {
	g := &groupState{}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// enqueue registers an appended frame and returns the handle to wait on.
// Called by the writer while it still holds the writer slot, so the order of
// pending matches the order of sequences in the log.
func (db *DB) enqueue(frame *walFrame) *pendingFrame {
	pf := &pendingFrame{frame: frame}
	db.group.mu.Lock()
	db.group.pending = append(db.group.pending, pf)
	db.group.bytes += int64(frame.encodedSize)
	db.group.mu.Unlock()
	return pf
}

// The writer slot prevents another enqueue while this caller waits. Flushing
// does not need that slot, so waiting cannot prevent the window from draining.
func (db *DB) waitPendingCapacity(size int64) error {
	limit := db.opts.MaxPendingBytes
	if limit <= 0 {
		return nil
	}
	if size > limit {
		return fmt.Errorf("%w: WAL frame bytes %d exceed pending limit %d", ErrTransactionTooLarge, size, limit)
	}
	g := db.group
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.bytes > limit-size {
		if g.syncing {
			g.cond.Wait()
			continue
		}
		err := db.flushLocked()
		if err != nil {
			return err
		}
	}
	return nil
}

// awaitDurable blocks until pf is durable and applied, or until the attempt
// fails. The caller must already have released the writer slot: that is what
// lets other commits pile into the same batch.
func (db *DB) awaitDurable(pf *pendingFrame) error {
	g := db.group
	g.mu.Lock()
	defer g.mu.Unlock()

	for !pf.settled {
		if g.syncing {
			// Somebody else is flushing. They may or may not be carrying this
			// frame; if they are not, this loop takes another turn and leads
			// the next flush itself.
			g.cond.Wait()
			continue
		}
		db.flushLocked()
	}
	return pf.err
}

// drainPending empties the window: everything appended is made durable and
// applied before it returns. It is how a reader, or compaction, refuses to
// proceed while frames are still invisible.
//
// Must not be called with the state lock held — the flush it may perform takes
// that lock to apply.
func (db *DB) drainPending() error {
	g := db.group
	g.mu.Lock()
	defer g.mu.Unlock()

	var err error
	for len(g.pending) > 0 || g.syncing {
		if g.syncing {
			g.cond.Wait()
			continue
		}
		if e := db.flushLocked(); e != nil {
			err = e
		}
	}
	return err
}

// flushLocked is the leader's turn. Called with group.mu held and returns with
// it held, but releases it for the slow part so that writers can keep appending
// into the next batch while this fsync is in flight.
func (db *DB) flushLocked() error {
	g := db.group

	batch := g.pending
	g.pending = nil
	g.flushing = batch
	g.syncing = true
	g.mu.Unlock()

	// One fsync for the whole batch. It flushes the file, so it also covers
	// anything appended since the snapshot above; those frames are simply
	// applied by the next flush, which costs nothing and is never wrong.
	err := db.failedErr()
	applied := 0
	if err == nil {
		err = db.wal.sync()
		if err != nil {
			// A failed fsync means durability is unknown from here on: the kernel
			// may have dropped the dirty pages, and a later sync can report success
			// without them. Nothing in the batch is applied, and every writer in it
			// is told so.
			db.fail(fmt.Errorf("group sync: %w", err))
		}
	}
	if err == nil {
		applied, err = db.applyBatch(batch)
	}

	g.mu.Lock()
	g.flushing = nil
	g.syncing = false
	for i, pf := range batch {
		g.bytes -= int64(pf.frame.encodedSize)
		pf.settled = true
		if i < applied {
			pf.err = nil // this one made it, whatever happened after
		} else {
			pf.err = err
		}
	}
	g.cond.Broadcast()
	return err
}

// applyBatch applies durable frames to memory in sequence order and reports how
// many landed. A failure part-way leaves the earlier frames applied, which is
// why the count is returned rather than a bare error: those commits really did
// happen and their callers should be told so.
func (db *DB) applyBatch(batch []*pendingFrame) (int, error) {
	db.mu.Lock()
	applied := 0
	var failure error
	for _, pf := range batch {
		if err := db.applyFrameLocked(pf.frame); err != nil {
			// The frame is already durable, so state and log have diverged.
			// Latch: every later write would build on state the log does not
			// describe. The next Open replays the frame and fails the same way,
			// which is the honest outcome.
			if db.failed == nil {
				db.failed = fmt.Errorf("applying durable frame %d: %w", pf.frame.Seq, err)
				db.log.Error().Err(db.failed).Msg("datastore_failed: refusing further writes")
			}
			failure = fmt.Errorf("datastore: applying a durable frame failed, state is inconsistent: %w", err)
			break
		}
		db.seq = pf.frame.Seq
		applied++
	}
	db.checkBudgetLocked()
	db.mu.Unlock()

	// Dispatch outside the state lock, and only for frames that are both
	// durable and applied: a subscriber reading the database on receipt must
	// find what the event describes.
	for _, pf := range batch[:applied] {
		db.dispatch(pf.frame)
	}
	return applied, failure
}

// pendingOpsFor folds the window down to what it means for one collection:
// the record puts and deletes that are in the log but not yet in memory.
//
// The fold is in sequence order, so a later frame overrides an earlier one,
// which is what makes the result equivalent to having applied them all.
func (db *DB) pendingOpsFor(coll string) (puts map[string]json.RawMessage, dels map[string]struct{}) {
	g := db.group
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, batch := range [2][]*pendingFrame{g.flushing, g.pending} {
		for _, pf := range batch {
			// The caller holds the state read lock. A flushed batch can remain
			// registered during watch dispatch after its records are already applied.
			if pf.frame.Seq <= db.seq {
				continue
			}
			for _, op := range pf.frame.Ops {
				if op.Coll != coll {
					continue
				}
				if op.Op == opDelete {
					if puts != nil {
						delete(puts, op.Key)
					}
					if dels == nil {
						dels = make(map[string]struct{})
					}
					dels[op.Key] = struct{}{}
					continue
				}
				if dels != nil {
					delete(dels, op.Key)
				}
				if puts == nil {
					puts = make(map[string]json.RawMessage)
				}
				puts[op.Key] = op.Val
			}
		}
	}
	return puts, dels
}

// pendingEmpty reports whether the window is closed. Used to skip work in the
// overwhelmingly common case of nothing being in flight.
func (db *DB) pendingEmpty() bool {
	g := db.group
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.pending) == 0 && !g.syncing
}
