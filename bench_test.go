package datastore

// Benchmarks, and the memory-footprint measurement behind Options.MaxBytes.
//
// Two questions this file exists to answer with numbers rather than adjectives:
// how many commits a second the store sustains and what actually limits that,
// and how much RAM a given amount of data really costs — which is not what
// Stats().Bytes reports.

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// benchFixture is the standard fixture without a *testing.T, configured so
// nothing but the operation under test is being measured: no compaction, no
// budget checks.
func benchFixture(tb testing.TB, tweak ...func(*Options)) *fixture {
	tb.Helper()
	opts := append([]func(*Options){func(o *Options) {
		o.CompactAfterBytes = 1 << 40
		o.CompactInterval = CompactNever
	}}, tweak...)

	f := newUnopenedFixture(tb.TempDir(), opts...)
	if err := f.db.Open(); err != nil {
		tb.Fatalf("open: %v", err)
	}
	tb.Cleanup(func() { _ = f.db.Close() })
	return f
}

// --- writes ---------------------------------------------------------------

// BenchmarkCommit is the headline number, and the three modes are the point:
// the gap between SyncAlways and SyncNever is the fsync, and it is most of the
// cost. Everything else in the commit path is noise beside it.
func BenchmarkCommit(b *testing.B) {
	modes := []struct {
		name string
		mode SyncMode
	}{
		{"SyncAlways", SyncAlways},
		{"SyncInterval", SyncInterval},
		{"SyncNever", SyncNever},
	}
	for _, m := range modes {
		b.Run(m.name, func(b *testing.B) {
			f := benchFixture(b, func(o *Options) { o.Sync = m.mode })
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := f.orders.Put(mkOrder(strconv.Itoa(i))); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCommitParallel measures what concurrency buys. Under SyncAlways the
// answer is expected to be "nothing": one writer slot, one fsync per commit, so
// N goroutines queue behind the same disk. That is the number group commit
// would change.
func BenchmarkCommitParallel(b *testing.B) {
	for _, procs := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("goroutines=%d", procs), func(b *testing.B) {
			f := benchFixture(b, func(o *Options) { o.Sync = SyncAlways })
			b.SetParallelism(procs)
			b.ResetTimer()
			var n atomicCounter
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := strconv.FormatUint(n.next(), 10)
					if err := f.orders.Put(mkOrder(id)); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkMultiRecordTransaction: the cost of a commit is dominated by the
// fsync, not by how much rides on it. This is the shape of the argument for
// batching writes into one Update.
func BenchmarkMultiRecordTransaction(b *testing.B) {
	for _, size := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("records=%d", size), func(b *testing.B) {
			f := benchFixture(b, func(o *Options) { o.Sync = SyncAlways })
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := f.db.Update(func(tx *Tx) error {
					oc := In(tx, f.orders)
					for j := 0; j < size; j++ {
						if err := oc.Put(mkOrder(strconv.Itoa(i*size + j))); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(size), "records/op")
		})
	}
}

// --- reads ----------------------------------------------------------------

// BenchmarkGet measures the decode on every read. Values are held encoded so
// that nothing handed out can be mutated behind the store's back; this is what
// that safety costs.
func BenchmarkGet(b *testing.B) {
	f := benchFixture(b, func(o *Options) { o.Sync = SyncNever })
	const n = 10000
	fillOrders(b, f, n)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := f.orders.Get(strconv.Itoa(i % n)); !ok {
			b.Fatal("missing record")
		}
	}
}

// BenchmarkIndexFind decodes every match, so its cost scales with the size of
// the result rather than the size of the collection. An iterator form would let
// a caller stop early.
func BenchmarkIndexFind(b *testing.B) {
	f := benchFixture(b, func(o *Options) { o.Sync = SyncNever })
	const n = 10000
	fillOrders(b, f, n)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := f.byStatus.Find("new"); len(got) != n {
			b.Fatalf("Find returned %d, want %d", len(got), n)
		}
	}
}

// BenchmarkScan is the claim in the README — that at these volumes a linear
// scan is measured in microseconds — put on the record.
func BenchmarkScan(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			f := benchFixture(b, func(o *Options) { o.Sync = SyncNever })
			fillOrders(b, f, n)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				total := 0
				for o := range f.orders.All() {
					total += o.Total
				}
				if total == 0 {
					b.Fatal("scan found nothing")
				}
			}
		})
	}
}

// BenchmarkOpen measures recovery: replaying the log and rebuilding every
// index. This is the startup cost a service pays, and it grows with the
// dataset.
func BenchmarkOpen(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			dir := b.TempDir()
			seed := newUnopenedFixture(dir, func(o *Options) {
				o.Sync = SyncNever
				o.CompactAfterBytes = 1 << 40
				o.CompactInterval = CompactNever
			})
			if err := seed.db.Open(); err != nil {
				b.Fatal(err)
			}
			fillOrders(b, seed, n)
			if err := seed.db.Close(); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f := newUnopenedFixture(dir, func(o *Options) {
					o.Sync = SyncNever
					o.CompactInterval = CompactNever
				})
				if err := f.db.Open(); err != nil {
					b.Fatal(err)
				}
				if err := f.db.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// --- memory ---------------------------------------------------------------

// TestMemoryFootprint measures what a dataset actually costs in RAM, against
// what Stats().Bytes reports.
//
// They are not the same number and the difference is not small. Stats().Bytes
// -- the figure Options.MaxBytes is compared against -- sums len(key) plus
// len(encoded record). That is the payload and nothing else. It does not count
// the map holding the records, the slice header on every json.RawMessage, the
// key stored a second time inside the map, or any index: an Index keeps
// map[string]map[string]struct{} plus map[string][]string, a UniqueIndex two
// more maps, a SortedIndex a slice of entries. Indexes are where most of the
// overhead lives, which is why this measures with and without them.
//
// Run it with -v to see the table. The multiplier it prints is the number to
// apply to MaxBytes when sizing a machine.
func TestMemoryFootprint(t *testing.T) {
	if testing.Short() {
		t.Skip("memory measurement is slow; run without -short")
	}

	t.Logf("%-9s %-8s %-8s %12s %12s %10s %8s",
		"records", "indexes", "recsize", "payload", "heap", "B/record", "factor")
	for _, n := range []int{2000, 10000} {
		for _, withIndexes := range []bool{false, true} {
			for _, pad := range []int{0, 400, 2000} {
				payload, heap := measureFootprint(t, n, withIndexes, pad)
				factor := float64(heap) / float64(payload)
				t.Logf("%-9d %-8v %-8d %12d %12d %10.1f %7.1fx",
					n, withIndexes, int(payload)/n, payload, heap, float64(heap)/float64(n), factor)

				if factor < 1 {
					t.Errorf("heap (%d) below payload (%d): the measurement is wrong", heap, payload)
				}
			}
		}
	}
}

// measureFootprint reports Stats().Bytes and the live heap the database
// occupies, for n records.
func measureFootprint(tb testing.TB, n int, withIndexes bool, pad int) (payload, heap int64) {
	tb.Helper()

	settle := func() uint64 {
		// Twice: the first collection can resurrect finalisable objects, and
		// the second gives a stable figure.
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}

	before := settle()

	db := New(Options{
		Dir:               tb.TempDir(),
		Sync:              SyncNever,
		CompactInterval:   CompactNever,
		CompactAfterBytes: 1 << 40,
	})
	orders := Register[*order](db, "orders")
	if withIndexes {
		AddIndex(orders, "status", func(o *order) []string { return []string{o.Status} })
		AddIndex(orders, "tag", func(o *order) []string { return o.Tags })
		AddUnique(orders, "email", func(o *order) string { return o.Email })
		AddSorted(orders, "date", func(o *order) int64 { return o.Created })
	}
	if err := db.Open(); err != nil {
		tb.Fatalf("open: %v", err)
	}
	defer db.Close()

	err := db.Update(func(tx *Tx) error {
		oc := In(tx, orders)
		for i := 0; i < n; i++ {
			o := mkOrder(strconv.Itoa(i))
			o.Email = fmt.Sprintf("user%d@example.com", i)
			o.Tags = []string{"retail", "web"}
			o.Created = int64(i)
			if pad > 0 {
				o.Customer = strings.Repeat("x", pad)
			}
			if err := oc.Put(o); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		tb.Fatalf("fill: %v", err)
	}

	payload = db.Stats().Bytes
	after := settle()
	runtime.KeepAlive(db)

	return payload, int64(after) - int64(before)
}

// --- helpers --------------------------------------------------------------

func fillOrders(tb testing.TB, f *fixture, n int) {
	tb.Helper()
	err := f.db.Update(func(tx *Tx) error {
		oc := In(tx, f.orders)
		for i := 0; i < n; i++ {
			o := mkOrder(strconv.Itoa(i))
			o.Email = fmt.Sprintf("user%d@example.com", i)
			o.Created = int64(i)
			if err := oc.Put(o); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		tb.Fatalf("fill %d records: %v", n, err)
	}
}

// atomicCounter hands out unique ids to parallel benchmark goroutines without
// them contending on a mutex the benchmark would then be measuring.
type atomicCounter struct{ n atomic.Uint64 }

func (c *atomicCounter) next() uint64 { return c.n.Add(1) }
