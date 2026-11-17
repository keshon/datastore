package datastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"reflect"
	"sort"
	"testing"
)

func TestTransactionsAgainstModel(t *testing.T) {
	for _, mode := range []SyncMode{SyncAlways, SyncNever} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			for seed := uint64(1); seed <= 3; seed++ {
				t.Run(fmt.Sprint(seed), func(t *testing.T) {
					testTransactionsAgainstModel(t, mode, seed)
				})
			}
		})
	}
}

func testTransactionsAgainstModel(t *testing.T, mode SyncMode, seed uint64) {
	t.Helper()
	dir := t.TempDir()
	open := func() *fixture {
		return openFixture(t, dir, func(o *Options) {
			o.Sync = mode
			o.CompactAfterBytes = 1 << 40
			o.MaxBytes = 1 << 30
		})
	}
	f := open()
	rng := rand.New(rand.NewPCG(seed, seed+1))
	model := make(map[string]order)
	abort := errors.New("rollback")

	for round := range 300 {
		candidate := maps.Clone(model)
		writes := make(map[string]*order)
		for range 1 + rng.IntN(6) {
			key := fmt.Sprint(rng.IntN(32))
			if rng.IntN(4) == 0 {
				delete(candidate, key)
				writes[key] = nil
				continue
			}
			v := order{
				ID:      key,
				Email:   fmt.Sprint(rng.IntN(40)),
				Created: int64(rng.IntN(16)),
				Status:  fmt.Sprint(rng.IntN(4)),
			}
			candidate[key] = v
			writes[key] = &v
		}
		unique := true
		terms := make(map[string]string)
		for key, v := range candidate {
			if _, exists := terms[v.Email]; exists {
				unique = false
			}
			terms[v.Email] = key
		}
		rollback := rng.IntN(10) == 0
		err := f.db.Update(func(tx *Tx) error {
			c := In(tx, f.orders)
			for key, v := range writes {
				var stageErr error
				if v == nil {
					stageErr = c.Delete(key)
				} else {
					stageErr = c.Put(v)
				}
				if stageErr != nil {
					return stageErr
				}
			}
			if c.Len() != len(candidate) {
				t.Fatalf("round %d: staged count differs from model", round)
			}
			if rollback {
				return abort
			}
			return nil
		})
		switch {
		case rollback:
			if !errors.Is(err, abort) {
				t.Fatalf("round %d: rollback: %v", round, err)
			}
		case !unique:
			if !errors.Is(err, ErrUniqueViolation) {
				t.Fatalf("round %d: expected uniqueness rejection, got %v", round, err)
			}
		default:
			if err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
			model = candidate
		}
		checkModel(t, f, model)

		if round%50 == 49 {
			if round%100 == 99 {
				err = f.db.Compact()
				if err != nil {
					t.Fatal(err)
				}
			}
			release(t, f.db)
			f = open()
			checkModel(t, f, model)
		}
	}
}

func checkModel(t *testing.T, f *fixture, model map[string]order) {
	t.Helper()
	if f.orders.Len() != len(model) {
		t.Fatalf("count = %d, want %d", f.orders.Len(), len(model))
	}
	var bytes int64
	wantSorted := make([]*order, 0, len(model))
	for key, want := range model {
		got, ok := f.orders.Get(key)
		if !ok || !reflect.DeepEqual(*got, want) {
			t.Fatalf("key %s: got %v, want %+v", key, got, want)
		}
		raw, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		bytes += int64(len(key) + len(raw))
		wantSorted = append(wantSorted, got)
	}
	if got := f.db.Stats().Bytes; got != bytes {
		t.Fatalf("payload bytes = %d, want %d", got, bytes)
	}
	sort.Slice(wantSorted, func(i, j int) bool {
		if wantSorted[i].Created != wantSorted[j].Created {
			return wantSorted[i].Created < wantSorted[j].Created
		}
		return wantSorted[i].ID < wantSorted[j].ID
	})
	gotSorted := f.byDate.Asc(0, 0)
	if !reflect.DeepEqual(gotSorted, wantSorted) {
		t.Fatal("sorted index differs from model")
	}
	err := f.db.Verify()
	if err != nil {
		t.Fatal(err)
	}
}

func TestOrphanByteAccountingAcrossSnapshotAndWAL(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, &order{ID: "a", Customer: "old"})
	mustPut(t, f.orders, &order{ID: "b"})
	err := f.db.Compact()
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, f.orders, &order{ID: "a", Customer: "replacement"})
	err = f.orders.Delete("b")
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, f.orders, &order{ID: "c"})
	want := f.db.Stats().Bytes
	release(t, f.db)

	partial := New(Options{Dir: f.dir, CompactInterval: CompactNever})
	err = partial.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = partial.Close() })
	stats := partial.Stats()
	if stats.Bytes != want || stats.Orphans["orders"] != 2 {
		t.Fatalf("orphan accounting after recovery = %+v, want %d bytes", stats, want)
	}
	err = partial.Close()
	if err != nil {
		t.Fatal(err)
	}
	recovered := openFixture(t, f.dir)
	if recovered.db.Stats().Bytes != want {
		t.Fatal("orphan compaction changed encoded payload accounting")
	}
	err = recovered.db.Verify()
	if err != nil {
		t.Fatal(err)
	}
}
