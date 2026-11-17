package datastore

import (
	"errors"
	"testing"
	"time"
)

func TestCloseTimeoutRetainsWriterResourcesAndCanRetry(t *testing.T) {
	f := newFixture(t, func(o *Options) { o.WriteTimeout = 20 * time.Millisecond })
	entered := make(chan struct{})
	resume := make(chan struct{})
	written := make(chan error, 1)
	go func() {
		written <- f.db.Update(func(tx *Tx) error {
			close(entered)
			<-resume
			return In(tx, f.orders).Put(&order{ID: "a"})
		})
	}()
	<-entered
	err := f.db.Close()
	if !errors.Is(err, ErrWriteTimeout) {
		t.Errorf("Close = %v, want writer timeout", err)
	}
	// A close attempt must not release the process lock under a live writer.
	other := newUnopenedFixture(f.dir)
	err = other.db.Open()
	if !errors.Is(err, ErrLocked) {
		t.Errorf("second owner Open = %v, want directory lock", err)
	}
	close(resume)
	err = <-written
	if err != nil {
		t.Fatal(err)
	}
	err = f.db.Close()
	if err != nil {
		t.Fatal(err)
	}
	recovered := openFixture(t, f.dir)
	if _, ok := recovered.orders.Get("a"); !ok {
		t.Fatal("active transaction lost during close retry")
	}
}

func TestCloseDrainsInFlightGroupCommit(t *testing.T) {
	f := newFixture(t)
	paused, resume := pauseNextSync(t, f, nil)
	written := make(chan error, 1)
	closed := make(chan error, 1)
	go func() { written <- f.orders.Put(&order{ID: "a"}) }()
	<-paused.entered
	go func() { closed <- f.db.Close() }()
	resume()
	for _, done := range []chan error{written, closed} {
		err := <-done
		if err != nil {
			t.Fatal(err)
		}
	}
	recovered := openFixture(t, f.dir)
	if _, ok := recovered.orders.Get("a"); !ok {
		t.Fatal("settled commit lost during Close")
	}
}
