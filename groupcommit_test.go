package datastore

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type pausedSyncLog struct {
	logFile
	once    sync.Once
	entered chan struct{}
	resume  chan struct{}
	failure error
}

func (f *pausedSyncLog) Sync() error {
	var failure error
	f.once.Do(func() {
		close(f.entered)
		<-f.resume
		failure = f.failure
	})
	if failure != nil {
		return failure
	}
	return f.logFile.Sync()
}

func pauseNextSync(t *testing.T, f *fixture, failure error) (*pausedSyncLog, func()) {
	t.Helper()
	paused := &pausedSyncLog{
		entered: make(chan struct{}),
		resume:  make(chan struct{}),
		failure: failure,
	}
	f.db.wal.mu.Lock()
	paused.logFile = f.db.wal.f
	f.db.wal.f = paused
	f.db.wal.mu.Unlock()
	var once sync.Once
	resume := func() { once.Do(func() { close(paused.resume) }) }
	t.Cleanup(resume)
	return paused, resume
}

func waitPending(t *testing.T, db *DB, n int) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		db.group.mu.Lock()
		pending := len(db.group.pending)
		db.group.mu.Unlock()
		if pending >= n {
			return
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatal("writes did not reach pending batch")
		}
	}
}

func TestGroupSyncFailureRejectsFollowingBatch(t *testing.T) {
	f := newFixture(t)
	failure := errors.New("injected sync failure")
	paused, resume := pauseNextSync(t, f, failure)
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- f.orders.Put(&order{ID: "a"}) }()
	<-paused.entered
	go func() { second <- f.orders.Put(&order{ID: "b"}) }()
	waitPending(t, f.db, 1)
	resume()
	if err := <-first; !errors.Is(err, failure) {
		t.Fatalf("first write: %v", err)
	}
	if err := <-second; !errors.Is(err, ErrFailed) {
		t.Fatalf("following write: %v, want ErrFailed", err)
	}
	if f.orders.Len() != 0 {
		t.Fatal("failed batches became visible")
	}
	err := f.orders.Put(&order{ID: "c"})
	if !errors.Is(err, ErrFailed) {
		t.Fatalf("later write: %v", err)
	}
}

func TestPendingCapacityWaitsBeforeAppend(t *testing.T) {
	f := newFixture(t, func(o *Options) { o.MaxPendingBytes = 256 })
	paused, resume := pauseNextSync(t, f, nil)
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- f.orders.Put(&order{ID: "a"}) }()
	<-paused.entered
	before := f.db.wal.sizeNow()
	started := make(chan struct{})
	go func() {
		second <- f.db.Update(func(tx *Tx) error {
			close(started)
			return In(tx, f.orders).Put(&order{ID: "b"})
		})
	}()
	<-started
	select {
	case err := <-second:
		t.Fatalf("write returned before space was available: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if f.db.wal.sizeNow() != before {
		t.Fatal("over-capacity frame was appended before waiting")
	}
	resume()
	for _, done := range []chan error{first, second} {
		err := <-done
		if err != nil {
			t.Fatal(err)
		}
	}
	if f.orders.Len() != 2 {
		t.Fatal("writes missing after capacity became available")
	}
}

func TestUniqueFoldsFlushingAndPendingInSequenceOrder(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(map[bool]string{false: "move", true: "delete"}[deleted], func(t *testing.T) {
			f := newFixture(t)
			mustPut(t, f.orders, &order{ID: "a", Email: "old"})
			paused, resume := pauseNextSync(t, f, nil)
			first := make(chan error, 1)
			second := make(chan error, 1)
			third := make(chan error, 1)
			go func() { first <- f.orders.Put(&order{ID: "a", Email: "flushing"}) }()
			<-paused.entered
			go func() {
				if deleted {
					second <- f.orders.Delete("a")
					return
				}
				second <- f.orders.Put(&order{ID: "a", Email: "pending"})
			}()
			waitPending(t, f.db, 1)
			go func() { third <- f.orders.Put(&order{ID: "b", Email: "flushing"}) }()
			waitPending(t, f.db, 2)
			conflict := make(chan error, 1)
			go func() { conflict <- f.orders.Put(&order{ID: "c", Email: "flushing"}) }()
			select {
			case err := <-conflict:
				if !errors.Is(err, ErrUniqueViolation) {
					t.Fatalf("pending term conflict = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("conflict was not rejected before sync")
			}
			stats := f.db.Stats()
			if stats.PendingFrames != 3 || stats.PendingBytes <= 0 {
				t.Fatalf("pending stats exclude the flushing batch: %+v", stats)
			}
			resume()
			for _, done := range []chan error{first, second, third} {
				err := <-done
				if err != nil {
					t.Fatal(err)
				}
			}
			err := f.db.Verify()
			if err != nil {
				t.Fatal(err)
			}
			stats = f.db.Stats()
			if stats.PendingBytes != 0 || stats.PendingFrames != 0 {
				t.Fatal("settled frames retained their pending budget")
			}
			release(t, f.db)
			recovered := openFixture(t, f.dir)
			v, ok := recovered.byEmail.Get("flushing")
			if !ok || v.ID != "b" {
				t.Fatal("recovery lost the unique term transfer")
			}
			err = recovered.db.Verify()
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUniqueDuringFlush(t *testing.T) {
	for _, mode := range []string{"insert", "delete", "move-release", "move-claim"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			if mode != "insert" {
				mustPut(t, f.orders, &order{ID: "a", Email: "old"})
			}
			paused := &pausedSyncLog{entered: make(chan struct{}), resume: make(chan struct{})}
			f.db.wal.mu.Lock()
			paused.logFile = f.db.wal.f
			f.db.wal.f = paused
			f.db.wal.mu.Unlock()
			var resume sync.Once
			unpause := func() { resume.Do(func() { close(paused.resume) }) }
			defer unpause()
			first := make(chan error, 1)
			go func() {
				if mode == "delete" {
					first <- f.orders.Delete("a")
				} else {
					first <- f.orders.Put(&order{ID: "a", Email: "new"})
				}
			}()
			select {
			case <-paused.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("flush did not start")
			}
			term := "old"
			conflict := mode == "insert" || mode == "move-claim"
			if conflict {
				term = "new"
			}
			second := make(chan error, 1)
			go func() { second <- f.orders.Put(&order{ID: "b", Email: term}) }()
			// A valid write must reach the next batch while fsync is paused.
			// A conflicting write must return its validation error before it.
			deadline := time.After(5 * time.Second)
			var secondErr error
			returned := false
		wait:
			for {
				select {
				case secondErr = <-second:
					returned = true
					break wait
				case <-deadline:
					t.Fatal("second write neither rejected nor enqueued")
				case <-time.After(time.Millisecond):
					f.db.group.mu.Lock()
					queued := len(f.db.group.pending) > 0
					f.db.group.mu.Unlock()
					if queued {
						break wait
					}
				}
			}
			unpause()
			if err := <-first; err != nil {
				t.Fatal(err)
			}
			if !returned {
				secondErr = <-second
			}
			if conflict {
				if !errors.Is(secondErr, ErrUniqueViolation) {
					t.Fatalf("second write: %v, want unique violation", secondErr)
				}
			} else if secondErr != nil {
				t.Fatal(secondErr)
			}
			check := func(f *fixture) {
				t.Helper()
				want := 2
				if conflict || mode == "delete" {
					want = 1
				}
				if got := f.orders.Len(); got != want {
					t.Fatalf("count = %d, want %d", got, want)
				}
				v, ok := f.byEmail.Get(term)
				owner := "b"
				if conflict {
					owner = "a"
				}
				if !ok || v.ID != owner {
					t.Fatalf("lookup %q = %v, %v; want %s", term, v, ok, owner)
				}
				if err := f.db.Verify(); err != nil {
					t.Fatal(err)
				}
			}
			check(f)
			release(t, f.db)
			check(openFixture(t, f.dir))
		})
	}
}
