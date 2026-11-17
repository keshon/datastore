package datastore

import (
	"encoding/json"
	"testing"
	"time"
)

func recvEvent(t *testing.T, sub *Subscription) Event {
	t.Helper()
	select {
	case ev, ok := <-sub.Events():
		if !ok {
			t.Fatal("subscription closed unexpectedly")
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an event")
		return Event{}
	}
}

func TestWatchIgnoresDispatchFromBeforeItsBoundary(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, &order{ID: "past"})
	sub := f.db.Watch(WatchOptions{})
	defer sub.Close()
	// Application precedes dispatch. A subscriber created in between already
	// includes this frame in its history boundary and must not receive it again.
	f.db.dispatch(&walFrame{
		Seq: sub.StartSeq(),
		Ops: []walOp{{Coll: "orders", Key: "past", Op: opPut, Val: json.RawMessage(`{"id":"past"}`)}},
	})
	select {
	case ev := <-sub.Events():
		t.Fatalf("past event crossed subscription boundary: %+v", ev)
	default:
	}
	mustPut(t, f.orders, &order{ID: "future"})
	ev := recvEvent(t, sub)
	if ev.Key != "future" || ev.Seq != sub.StartSeq()+1 {
		t.Fatalf("later commit missing from live feed: %+v", ev)
	}
}

func TestWatchDeliversPutsAndDeletes(t *testing.T) {
	f := newFixture(t)
	sub := f.db.Watch(WatchOptions{})
	defer sub.Close()

	if err := f.db.Update(func(tx *Tx) error {
		tx.Note("actor", "user:9")
		return In(tx, f.orders).Put(mkOrder("a", func(o *order) { o.Total = 55 }))
	}); err != nil {
		t.Fatal(err)
	}

	ev := recvEvent(t, sub)
	if ev.Collection != "orders" || ev.Key != "a" || ev.Delete {
		t.Fatalf("unexpected event: %+v", ev)
	}
	if ev.Seq <= sub.StartSeq() {
		t.Fatalf("event seq %d should be past StartSeq %d", ev.Seq, sub.StartSeq())
	}
	if ev.Meta["actor"] != "user:9" {
		t.Fatalf("event should carry the commit's annotations: %+v", ev.Meta)
	}
	var got order
	if err := json.Unmarshal(ev.Value, &got); err != nil {
		t.Fatalf("decode event value: %v", err)
	}
	if got.Total != 55 {
		t.Fatalf("event value should be what was written: %+v", got)
	}
	if ev.Dropped != 0 {
		t.Fatalf("nothing should have been dropped, got %d", ev.Dropped)
	}

	if err := f.orders.Delete("a"); err != nil {
		t.Fatal(err)
	}
	del := recvEvent(t, sub)
	if !del.Delete || del.Key != "a" || del.Value != nil {
		t.Fatalf("delete event should be valueless: %+v", del)
	}
}

func TestWatchFiltersByCollection(t *testing.T) {
	dir := t.TempDir()
	db := New(Options{Dir: dir, Sync: SyncNever, CompactInterval: -1})
	orders := Register[*order](db, "orders")
	ledger := Register[*order](db, "ledger")
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sub := db.Watch(WatchOptions{Collections: []string{"ledger"}})
	defer sub.Close()

	if err := orders.Put(mkOrder("ignored")); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Put(mkOrder("wanted")); err != nil {
		t.Fatal(err)
	}

	ev := recvEvent(t, sub)
	if ev.Collection != "ledger" || ev.Key != "wanted" {
		t.Fatalf("filter leaked an event from another collection: %+v", ev)
	}
	select {
	case extra := <-sub.Events():
		t.Fatalf("expected no further events, got %+v", extra)
	case <-time.After(100 * time.Millisecond):
	}
}

// The rule the whole design rests on: a subscriber that stops reading must not
// slow writes down. Fill its buffer, keep committing, and assert the writes
// still complete promptly and the drops are reported.
func TestSlowSubscriberNeverBlocksCommits(t *testing.T) {
	f := newFixture(t)
	sub := f.db.Watch(WatchOptions{Buffer: 2})
	defer sub.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 50 { // far more than the buffer, and nobody is reading
			if err := f.orders.Put(mkOrder(string(rune('a'+i%26)) + string(rune('0'+i/26)))); err != nil {
				t.Errorf("put %d: %v", i, err)
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("commits stalled behind a subscriber that stopped reading")
	}

	if sub.Dropped() == 0 {
		t.Fatal("the subscriber fell behind, so drops should have been counted")
	}
	// The next event read reports the gap, so the subscriber can resync.
	ev := recvEvent(t, sub)
	_ = ev
	drained := 0
	for drained < 2 {
		select {
		case e := <-sub.Events():
			drained++
			if e.Dropped > 0 {
				return // gap reported: the subscriber can recover
			}
		case <-time.After(time.Second):
			drained = 2
		}
	}
	if sub.Dropped() == 0 {
		t.Fatal("expected the drop count to be visible")
	}
}

func TestWatchCloseStopsDelivery(t *testing.T) {
	f := newFixture(t)
	sub := f.db.Watch(WatchOptions{})

	if err := f.orders.Put(mkOrder("a")); err != nil {
		t.Fatal(err)
	}
	recvEvent(t, sub)

	sub.Close()
	sub.Close() // idempotent

	if _, ok := <-sub.Events(); ok {
		t.Fatal("channel should be closed after Close")
	}

	// Writing afterwards must not panic on the closed subscription.
	if err := f.orders.Put(mkOrder("b")); err != nil {
		t.Fatalf("commit after unsubscribe: %v", err)
	}
}

func TestDatabaseCloseEndsSubscriptions(t *testing.T) {
	dir := t.TempDir()
	db := New(Options{Dir: dir, Sync: SyncNever, CompactInterval: -1})
	orders := Register[*order](db, "orders")
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sub := db.Watch(WatchOptions{})
	if err := orders.Put(mkOrder("a")); err != nil {
		t.Fatal(err)
	}
	recvEvent(t, sub)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Fatal("expected the feed to be closed by db.Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("db.Close did not close the subscription")
	}
}

// Events arrive in commit order.
func TestWatchPreservesOrder(t *testing.T) {
	f := newFixture(t)
	sub := f.db.Watch(WatchOptions{Buffer: 64})
	defer sub.Close()

	ids := []string{"a", "b", "c", "d", "e"}
	for _, id := range ids {
		if err := f.orders.Put(mkOrder(id)); err != nil {
			t.Fatal(err)
		}
	}
	var lastSeq uint64
	for i, want := range ids {
		ev := recvEvent(t, sub)
		if ev.Key != want {
			t.Fatalf("event %d = %q, want %q (out of order)", i, ev.Key, want)
		}
		if ev.Seq <= lastSeq {
			t.Fatalf("sequence went backwards: %d after %d", ev.Seq, lastSeq)
		}
		lastSeq = ev.Seq
	}
}
