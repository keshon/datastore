package datastore

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

var errRollback = errors.New("rollback")

// The reason this package exists. Placing an order writes the order, debits a
// balance, consumes a coupon and queues an email; if any part fails, none of it
// may have happened.
func TestTransactionIsAllOrNothing(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, mkOrder("existing"))

	err := f.db.Update(func(tx *Tx) error {
		c := In(tx, f.orders)
		if err := c.Put(mkOrder("a")); err != nil {
			return err
		}
		if err := c.Put(mkOrder("b")); err != nil {
			return err
		}
		if err := c.Delete("existing"); err != nil {
			return err
		}
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		t.Fatalf("Update = %v, want errRollback", err)
	}

	if _, ok := f.orders.Get("a"); ok {
		t.Error("a rolled-back write is visible")
	}
	if _, ok := f.orders.Get("b"); ok {
		t.Error("a rolled-back write is visible")
	}
	if _, ok := f.orders.Get("existing"); !ok {
		t.Error("a rolled-back delete took effect")
	}
	if f.orders.Len() != 1 {
		t.Errorf("Len = %d after rollback, want 1", f.orders.Len())
	}
}

func TestRolledBackTransactionLeavesNoTrace(t *testing.T) {
	dir := t.TempDir()
	f := openFixture(t, dir)

	before := f.db.Stats().WALBytes
	_ = f.db.Update(func(tx *Tx) error {
		_ = In(tx, f.orders).Put(mkOrder("ghost"))
		return errRollback
	})
	if after := f.db.Stats().WALBytes; after != before {
		t.Errorf("a rolled-back transaction wrote %d bytes to the log", after-before)
	}

	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	g := openFixture(t, dir)
	if _, ok := g.orders.Get("ghost"); ok {
		t.Error("a rolled-back write came back after a restart")
	}
}

func TestReadsSeeOwnWrites(t *testing.T) {
	f := newFixture(t)

	err := f.db.Update(func(tx *Tx) error {
		c := In(tx, f.orders)
		if err := c.Put(mkOrder("1", func(o *order) { o.Total = 10 })); err != nil {
			return err
		}
		got, ok := c.Get("1")
		if !ok {
			return errors.New("Get did not see this transaction's own Put")
		}
		if got.Total != 10 {
			return fmt.Errorf("Total = %d, want 10", got.Total)
		}
		if err := c.Delete("1"); err != nil {
			return err
		}
		if _, ok := c.Get("1"); ok {
			return errors.New("Get did not see this transaction's own Delete")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestUniqueViolationRejectsTheWholeTransaction(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, mkOrder("1", func(o *order) { o.Email = "taken@example.com" }))

	err := f.db.Update(func(tx *Tx) error {
		c := In(tx, f.orders)
		if err := c.Put(mkOrder("2", func(o *order) { o.Email = "fine@example.com" })); err != nil {
			return err
		}
		return c.Put(mkOrder("3", func(o *order) { o.Email = "taken@example.com" }))
	})
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("Update = %v, want ErrDuplicateKey", err)
	}
	if _, ok := f.orders.Get("2"); ok {
		t.Error("the valid write in a rejected transaction was applied")
	}
}

func TestDuplicateWithinOneTransactionIsCaught(t *testing.T) {
	f := newFixture(t)
	err := f.db.Update(func(tx *Tx) error {
		c := In(tx, f.orders)
		if err := c.Put(mkOrder("1", func(o *order) { o.Email = "x@example.com" })); err != nil {
			return err
		}
		return c.Put(mkOrder("2", func(o *order) { o.Email = "x@example.com" }))
	})
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("Update = %v, want ErrDuplicateKey", err)
	}
}

// Uniqueness is checked once at commit rather than on every Put, which is what
// makes these legal. Per-Put validation would reject both.
func TestUniqueValuesCanBeExchanged(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, mkOrder("1", func(o *order) { o.Email = "a@example.com" }))
	mustPut(t, f.orders, mkOrder("2", func(o *order) { o.Email = "b@example.com" }))

	err := f.db.Update(func(tx *Tx) error {
		c := In(tx, f.orders)
		if err := c.Put(mkOrder("1", func(o *order) { o.Email = "b@example.com" })); err != nil {
			return err
		}
		return c.Put(mkOrder("2", func(o *order) { o.Email = "a@example.com" }))
	})
	if err != nil {
		t.Fatalf("swapping two unique values: %v", err)
	}

	got, _ := f.byEmail.Get("a@example.com")
	if got.ID != "2" {
		t.Errorf("a@example.com now held by %q, want %q", got.ID, "2")
	}
}

func TestUniqueValueFreedByDeleteInSameTransaction(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, mkOrder("old", func(o *order) { o.Email = "x@example.com" }))

	err := f.db.Update(func(tx *Tx) error {
		c := In(tx, f.orders)
		if err := c.Delete("old"); err != nil {
			return err
		}
		return c.Put(mkOrder("new", func(o *order) { o.Email = "x@example.com" }))
	})
	if err != nil {
		t.Fatalf("reusing a value freed in the same transaction: %v", err)
	}
}

func TestViewIsReadOnly(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, mkOrder("1"))

	err := f.db.View(func(tx *Tx) error {
		if _, ok := In(tx, f.orders).Get("1"); !ok {
			return errors.New("View could not read a committed record")
		}
		return In(tx, f.orders).Put(mkOrder("2"))
	})
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("writing inside View = %v, want ErrReadOnly", err)
	}
}

func TestNextIDIsMonotonicAndDurable(t *testing.T) {
	dir := t.TempDir()
	f := openFixture(t, dir)

	var got []uint64
	for i := 0; i < 3; i++ {
		if err := f.db.Update(func(tx *Tx) error {
			id := tx.NextID("order")
			got = append(got, id)
			return In(tx, f.orders).Put(mkOrder(fmt.Sprint(id)))
		}); err != nil {
			t.Fatal(err)
		}
	}
	if fmt.Sprint(got) != "[1 2 3]" {
		t.Errorf("ids = %v, want [1 2 3]", got)
	}

	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	g := openFixture(t, dir)

	var next uint64
	if err := g.db.Update(func(tx *Tx) error { next = tx.NextID("order"); return In(tx, g.orders).Put(mkOrder("x")) }); err != nil {
		t.Fatal(err)
	}
	if next != 4 {
		t.Errorf("after restart NextID = %d, want 4 — the counter did not survive", next)
	}
}

// An id consumed by a transaction that never commits must not be lost. Order
// numbers with holes in them generate support tickets.
func TestAbandonedTransactionDoesNotBurnAnID(t *testing.T) {
	f := newFixture(t)

	_ = f.db.Update(func(tx *Tx) error {
		tx.NextID("order")
		return errRollback
	})

	var next uint64
	if err := f.db.Update(func(tx *Tx) error {
		next = tx.NextID("order")
		return In(tx, f.orders).Put(mkOrder("1"))
	}); err != nil {
		t.Fatal(err)
	}
	if next != 1 {
		t.Errorf("NextID = %d after a rollback, want 1", next)
	}
}

func TestOrderNumberFormatAndMonthlyReset(t *testing.T) {
	f := newFixture(t)

	jul := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	aug := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	var nums []string
	for _, when := range []time.Time{jul, jul, aug, jul} {
		if err := f.db.Update(func(tx *Tx) error {
			_, seq := tx.NextInPeriod("order", when, "0601")
			n := fmt.Sprintf("%s/%d", when.Format("0601"), seq)
			nums = append(nums, n)
			return In(tx, f.orders).Put(mkOrder(n))
		}); err != nil {
			t.Fatal(err)
		}
	}

	want := "2607/1 2607/2 2608/1 2607/3"
	if got := strings.Join(nums, " "); got != want {
		t.Errorf("order numbers = %q, want %q", got, want)
	}
}

func TestCrossCollectionAtomicity(t *testing.T) {
	dir := t.TempDir()

	db := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	orders := Register[*order](db, "orders")
	ledger := Register[*order](db, "ledger")
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		if err := In(tx, orders).Put(mkOrder("o1")); err != nil {
			return err
		}
		return In(tx, ledger).Put(mkOrder("l1"))
	}); err != nil {
		t.Fatal(err)
	}

	// Both, or neither — never one.
	_, haveOrder := orders.Get("o1")
	_, haveLedger := ledger.Get("l1")
	if !haveOrder || !haveLedger {
		t.Fatalf("committed across collections: order=%v ledger=%v", haveOrder, haveLedger)
	}

	err := db.Update(func(tx *Tx) error {
		if err := In(tx, orders).Put(mkOrder("o2")); err != nil {
			return err
		}
		if err := In(tx, ledger).Put(mkOrder("l2")); err != nil {
			return err
		}
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		t.Fatal(err)
	}
	if _, ok := orders.Get("o2"); ok {
		t.Error("half of a rolled-back cross-collection transaction survived")
	}
	if _, ok := ledger.Get("l2"); ok {
		t.Error("half of a rolled-back cross-collection transaction survived")
	}
}

func TestCollectionFromAnotherDatabasePanics(t *testing.T) {
	f := newFixture(t)
	other := newFixture(t)

	defer func() {
		if recover() == nil {
			t.Error("mixing collections across databases should panic")
		}
	}()
	_ = f.db.Update(func(tx *Tx) error {
		_ = In(tx, other.orders)
		return nil
	})
}

// A re-entrant Update can never proceed, so it must be reported at once rather
// than after WriteTimeout. Collection.Put is the usual accidental route in.
func TestReentrantUpdatePanicsImmediately(t *testing.T) {
	f := newFixture(t)

	done := make(chan any, 1)
	start := time.Now()
	go func() {
		defer func() { done <- recover() }()
		_ = f.db.Update(func(tx *Tx) error {
			return f.orders.Put(mkOrder("boom")) // its own transaction: deadlock
		})
	}()

	select {
	case r := <-done:
		if r == nil {
			t.Fatal("expected a panic from the re-entrant Update")
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("panic took %s; it should be immediate, not after WriteTimeout", elapsed)
		}
		if msg, _ := r.(string); !strings.Contains(msg, "inside another Update") {
			t.Fatalf("panic message should explain the cause, got: %v", r)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("re-entrant Update hung instead of panicking")
	}
}
