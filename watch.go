package datastore

import (
	"bytes"
	"encoding/json"
	"maps"
	"sync"
	"time"
)

// The change feed.
//
// Every commit already funnels through one place, so telling interested parties
// what changed costs almost nothing. The rule that shapes everything here: a
// subscriber must never be able to slow a commit down. Durability is the point
// of this package, and a forgotten channel in some goroutine has no business
// stalling an fsync.
//
// So delivery is best-effort into a bounded queue. When a subscriber falls
// behind, its events are dropped and it is told how many, which is enough for it
// to recover by re-reading the collection. A dropped event is a nuisance; a
// blocked write is an outage.

// Event is one record-level change, delivered after it is both durable and
// applied — so a subscriber that reads the database on receipt sees it.
type Event struct {
	Seq        uint64
	Time       time.Time
	Collection string
	Key        string
	Delete     bool

	// Value is the record as written, still encoded. Nil for a delete. The
	// bytes are the subscriber's own copy — keeping or mutating them is fine.
	Value json.RawMessage

	// Meta is the annotations the committing transaction attached (Tx.Note).
	// The map is the subscriber's own copy.
	Meta map[string]string

	// Dropped counts events discarded between the previous delivered event and
	// this one, because the subscriber was not keeping up. Normally zero; when
	// it is not, the subscriber has missed changes and should re-read what it
	// cares about.
	Dropped uint64
}

// WatchOptions configures a subscription.
type WatchOptions struct {
	// Collections limits delivery to these names. Empty means every collection.
	Collections []string
	// Buffer is how many events may queue before the subscriber is considered
	// behind and events start being dropped. Default 256.
	Buffer int
}

const defaultWatchBuffer = 256

// Subscription is a live feed of changes. Close it when finished; an abandoned
// subscription keeps a buffer alive and makes every commit do a little needless
// work.
type Subscription struct {
	db     *DB
	ch     chan Event
	filter map[string]bool
	start  uint64

	mu        sync.Mutex
	closed    bool
	pending   uint64 // dropped since the last delivered event
	totalDrop uint64
}

// Watch subscribes to changes committed from now on.
//
// It does not replay the past — but it records the sequence it started at, which
// is what makes catching up straightforward: subscribe first (events begin
// queueing immediately), replay History up to StartSeq, then read the channel.
// Nothing falls in the gap, because there is no gap.
//
//	sub := db.Watch(datastore.WatchOptions{})
//	defer sub.Close()
//	err := db.History(0, sub.StartSeq(), func(e datastore.HistoryEntry) error { ... })
//	for ev := range sub.Events() { ... }
func (db *DB) Watch(opts WatchOptions) *Subscription {
	buf := opts.Buffer
	if buf <= 0 {
		buf = defaultWatchBuffer
	}
	var filter map[string]bool
	if len(opts.Collections) > 0 {
		filter = make(map[string]bool, len(opts.Collections))
		for _, name := range opts.Collections {
			filter[name] = true
		}
	}

	db.mu.RLock()
	start := db.seq
	db.mu.RUnlock()

	sub := &Subscription{db: db, ch: make(chan Event, buf), filter: filter, start: start}

	db.subsMu.Lock()
	db.subs = append(db.subs, sub)
	db.subsMu.Unlock()
	return sub
}

// Events is the channel of changes. It is closed when the subscription is
// closed, or when the database closes.
func (s *Subscription) Events() <-chan Event { return s.ch }

// StartSeq is the sequence the database was at when this subscription began;
// every event delivered has a higher sequence. Use it as the upper bound of a
// History scan to catch up without gaps or duplicates.
func (s *Subscription) StartSeq() uint64 { return s.start }

// Dropped is the total number of events discarded because this subscriber was
// not keeping up.
func (s *Subscription) Dropped() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalDrop
}

// Close stops delivery and closes the channel. Safe to call more than once.
func (s *Subscription) Close() {
	s.db.subsMu.Lock()
	for i, other := range s.db.subs {
		if other == s {
			s.db.subs = append(s.db.subs[:i], s.db.subs[i+1:]...)
			break
		}
	}
	s.db.subsMu.Unlock()
	s.shutdown()
}

// shutdown closes the channel exactly once. The subscriber list mutex must not
// be held by the caller of the send path at the same time — see dispatch.
func (s *Subscription) shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}

// wants reports whether this subscription cares about a collection.
func (s *Subscription) wants(collection string) bool {
	return s.filter == nil || s.filter[collection]
}

// offer delivers without ever blocking. A full queue means the subscriber is
// behind: the event is dropped and counted, and the count rides along on the
// next event that does get through.
func (s *Subscription) offer(ev Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	ev.Dropped = s.pending
	select {
	case s.ch <- ev:
		s.pending = 0
	default:
		s.pending++
		s.totalDrop++
	}
}

// dispatch fans a committed frame out to subscribers. It runs on the committing
// goroutine, after the change is durable and applied, and cannot block: every
// send is non-blocking, so the worst a stalled subscriber costs is a dropped
// event and a mutex acquisition.
func (db *DB) dispatch(fr *walFrame) {
	db.subsMu.RLock()
	if len(db.subs) == 0 {
		db.subsMu.RUnlock()
		return // the common case: no subscribers, no work
	}
	subs := make([]*Subscription, len(db.subs))
	copy(subs, db.subs)
	db.subsMu.RUnlock()

	ts := time.Unix(fr.TS, 0)
	for _, op := range fr.Ops {
		// Each subscriber gets its own copy of the value and the annotations.
		// The frame's bytes are also the collection's stored record: handing
		// them out live would let a subscriber mutate the database in place,
		// breaking the package's central promise that everything you get back
		// is yours.
		for _, sub := range subs {
			if !sub.wants(op.Coll) {
				continue
			}
			ev := Event{
				Seq:        fr.Seq,
				Time:       ts,
				Collection: op.Coll,
				Key:        op.Key,
				Delete:     op.Op == opDelete,
				Meta:       maps.Clone(fr.Meta),
			}
			if op.Op != opDelete {
				ev.Value = bytes.Clone(op.Val)
			}
			sub.offer(ev)
		}
	}
}

// closeSubscriptions ends every feed, called when the database closes.
func (db *DB) closeSubscriptions() {
	db.subsMu.Lock()
	subs := db.subs
	db.subs = nil
	db.subsMu.Unlock()

	for _, sub := range subs {
		sub.shutdown()
	}
}
