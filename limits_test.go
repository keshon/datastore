package datastore

import (
	"errors"
	"testing"
)

func TestTransactionLimitsRejectWithoutAppending(t *testing.T) {
	for _, options := range []func(*Options){
		func(o *Options) { o.MaxTransactionOps = 1 },
		func(o *Options) { o.MaxTransactionBytes = 100 },
		func(o *Options) { o.MaxPendingBytes = 1 },
	} {
		t.Run("limit", func(t *testing.T) {
			f := newFixture(t, options)
			before := f.db.wal.sizeNow()
			err := f.db.Update(func(tx *Tx) error {
				c := In(tx, f.orders)
				stageErr := c.Put(&order{ID: "a"})
				if stageErr != nil {
					return stageErr
				}
				return c.Put(&order{ID: "b"})
			})
			if !errors.Is(err, ErrTransactionTooLarge) {
				t.Fatalf("got %v, want size rejection", err)
			}
			if f.orders.Len() != 0 || f.db.wal.sizeNow() != before {
				t.Fatal("rejected transaction changed state or WAL")
			}
		})
	}
}

func TestTransactionLimitsCountFinalStagedKeys(t *testing.T) {
	f := newFixture(t, func(o *Options) {
		o.MaxTransactionOps = 1
		o.MaxTransactionBytes = 160
	})
	err := f.db.Update(func(tx *Tx) error {
		c := In(tx, f.orders)
		stageErr := c.Put(&order{ID: "a", Customer: "first"})
		if stageErr != nil {
			return stageErr
		}
		stageErr = c.Delete("a")
		if stageErr != nil {
			return stageErr
		}
		return c.Put(&order{ID: "a", Customer: "last"})
	})
	if err != nil {
		t.Fatal(err)
	}
	v, ok := f.orders.Get("a")
	if !ok || v.Customer != "last" {
		t.Fatalf("final staged value = %v", v)
	}
}
