package datastore

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// The catch-up recipe, under concurrent writes.
//
// Watch does not replay history, so a consumer that needs the past composes the
// two: subscribe first (events queue from that moment), replay History up to
// StartSeq, then drain the live channel. The claim being tested is that this
// leaves no gap and produces no duplicates even while commits keep arriving —
// which is what makes a resumable Watch unnecessary.
func TestCatchUpLeavesNoGapAndNoDuplicates(t *testing.T) {
	f := newFixture(t, func(o *Options) { o.HistorySegments = 10 })

	const past = 20
	for i := range past {
		if err := f.orders.Put(mkOrder(fmt.Sprintf("past-%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// Force some of that history into an archive, so catch-up has to cross the
	// boundary the same way a real consumer would.
	if err := f.db.Compact(); err != nil {
		t.Fatal(err)
	}

	// 1. Subscribe. From here on, commits are queued for us.
	sub := f.db.Watch(WatchOptions{Buffer: 1024})
	defer sub.Close()
	startSeq := sub.StartSeq()

	// Writers keep going throughout catch-up: this is the race the recipe has
	// to survive.
	const future = 30
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range future {
			if err := f.orders.Put(mkOrder(fmt.Sprintf("future-%02d", i))); err != nil {
				t.Errorf("put: %v", err)
				return
			}
		}
	}()

	// 2. Replay everything up to the moment we subscribed.
	seen := make(map[string]int)
	var histSeq []uint64
	if err := f.db.History(0, startSeq, func(e HistoryEntry) error {
		histSeq = append(histSeq, e.Seq)
		for _, op := range e.Ops {
			seen[op.Key]++
		}
		return nil
	}); err != nil {
		t.Fatalf("History: %v", err)
	}

	// 3. Then drain live events until we have the rest.
	wg.Wait()
	deadline := time.After(10 * time.Second)
	for len(seen) < past+future {
		select {
		case ev := <-sub.Events():
			if ev.Dropped > 0 {
				t.Fatalf("buffer was large enough; unexpected drop of %d", ev.Dropped)
			}
			if ev.Seq <= startSeq {
				t.Fatalf("live feed replayed seq %d, which History already covered (<= %d)",
					ev.Seq, startSeq)
			}
			seen[ev.Key]++
		case <-deadline:
			t.Fatalf("only saw %d of %d records; the recipe lost events", len(seen), past+future)
		}
	}

	if len(seen) != past+future {
		t.Fatalf("saw %d distinct records, want %d", len(seen), past+future)
	}
	for key, n := range seen {
		if n != 1 {
			t.Fatalf("record %q was seen %d times; catch-up must not duplicate", key, n)
		}
	}
	for _, seq := range histSeq {
		if seq > startSeq {
			t.Fatalf("History returned seq %d beyond the requested bound %d", seq, startSeq)
		}
	}
}
