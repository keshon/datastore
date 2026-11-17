package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestVerifyWorkingSet(t *testing.T) {
	value := os.Getenv("DS_VERIFY_RECORDS")
	if value == "" {
		t.Skip("set DS_VERIFY_RECORDS to measure sampled verification heap")
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		t.Fatal("DS_VERIFY_RECORDS must be a positive integer")
	}
	snap := scalingSnapshot(t, n)
	dir := t.TempDir()
	err = writeSnapshot(dir, snap)
	if err != nil {
		t.Fatal(err)
	}
	snap = nil
	f := newUnopenedFixture(dir)
	err = f.db.OpenReader()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.db.Close() })
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	ctx, cancel := context.WithCancel(context.Background())
	peakResult := make(chan uint64, 1)
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		peak := before.HeapAlloc
		for {
			var sample runtime.MemStats
			runtime.ReadMemStats(&sample)
			peak = max(peak, sample.HeapAlloc)
			select {
			case <-ctx.Done():
				peakResult <- peak
				return
			case <-ticker.C:
			}
		}
	}()
	started := time.Now()
	err = f.db.Verify()
	elapsed := time.Since(started)
	runtime.ReadMemStats(&after)
	cancel()
	peak := <-peakResult
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("records=%d duration=%s base_heap=%d sampled_peak_heap=%d peak_growth=%d allocated=%d gc_cycles=%d", n, elapsed, before.HeapAlloc, peak, peak-before.HeapAlloc, after.TotalAlloc-before.TotalAlloc, after.NumGC-before.NumGC)
}

func scalingSnapshot(tb testing.TB, n int) *snapshotFile {
	tb.Helper()
	rows := make(map[string]json.RawMessage, n)
	padding := strings.Repeat("x", 960)
	for i := range n {
		key := fmt.Sprintf("%09d", i)
		raw, err := json.Marshal(&order{ID: key, Email: key, Created: int64(i), Customer: padding})
		if err != nil {
			tb.Fatal(err)
		}
		rows[key] = raw
	}
	return &snapshotFile{Seq: 1, Collections: map[string]map[string]json.RawMessage{"orders": rows}}
}

// Opt-in: this writes about 1 GB at one million records and reports process
// heap, allocation and latency measurements without asserting machine timings.
func TestCapacity(t *testing.T) {
	value := os.Getenv("DS_CAPACITY_RECORDS")
	if value == "" {
		t.Skip("set DS_CAPACITY_RECORDS=100000 or 1000000")
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		t.Fatal("DS_CAPACITY_RECORDS must be a positive integer")
	}
	f := newFixture(t, func(o *Options) {
		o.CompactAfterBytes = 1 << 40
		o.MaxBytes = 4 << 30
		o.MaxPendingBytes = 8 << 20
	})
	padding := strings.Repeat("x", 960)
	measure := func(name string, operation func() error) {
		t.Helper()
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		started := time.Now()
		operationErr := operation()
		elapsed := time.Since(started)
		runtime.ReadMemStats(&after)
		if operationErr != nil {
			t.Fatal(operationErr)
		}
		allocated := after.TotalAlloc - before.TotalAlloc
		cycles := after.NumGC - before.NumGC
		runtime.GC()
		runtime.ReadMemStats(&after)
		t.Logf("%s: duration=%s allocated=%d heap=%d gc_cycles=%d", name, elapsed, allocated, after.HeapAlloc, cycles)
	}
	measure("ingest", func() error {
		for start := 0; start < n; start += 1000 {
			batchErr := f.db.Update(func(tx *Tx) error {
				c := In(tx, f.orders)
				for i := start; i < min(start+1000, n); i++ {
					key := fmt.Sprintf("%09d", i)
					v := &order{ID: key, Email: key, Created: int64(i), Customer: padding, Status: "active", Tags: []string{"all"}}
					putErr := c.Put(v)
					if putErr != nil {
						return putErr
					}
				}
				return nil
			})
			if batchErr != nil {
				return batchErr
			}
		}
		return nil
	})
	measure("compact", f.db.Compact)
	release(t, f.db)
	f.orders.clear() // cleanup retains the fixture; release its old dataset before measuring Open
	measure("open", func() error {
		f = openFixture(t, f.dir, func(o *Options) {
			o.CompactAfterBytes = 1 << 40
			o.MaxBytes = 4 << 30
		})
		return nil
	})
	if f.orders.Len() != n {
		t.Fatalf("reopened count = %d, want %d", f.orders.Len(), n)
	}
	measure("overwrite-1000", func() error {
		latencies := make([]time.Duration, 1000)
		for i := range latencies {
			key := fmt.Sprintf("%09d", i%n)
			v := &order{ID: key, Email: key, Created: int64(i % n), Customer: padding, Status: "active", Tags: []string{"all"}}
			started := time.Now()
			putErr := f.orders.Put(v)
			latencies[i] = time.Since(started)
			if putErr != nil {
				return putErr
			}
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		t.Logf("SyncAlways overwrite: p50=%s p95=%s p99=%s", latencies[499], latencies[949], latencies[989])
		return nil
	})
	measure("verify", f.db.Verify)
	stats := f.db.Stats()
	t.Logf("records=%d payload_bytes=%d wal_bytes=%d pending_bytes=%d", n, stats.Bytes, stats.WALBytes, stats.PendingBytes)
}

func BenchmarkSnapshotLarge(b *testing.B) {
	snap := scalingSnapshot(b, 100_000)
	dir := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		err := writeSnapshot(dir, snap)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVerifyLarge(b *testing.B) {
	snap := scalingSnapshot(b, 100_000)
	dir := b.TempDir()
	err := writeSnapshot(dir, snap)
	if err != nil {
		b.Fatal(err)
	}
	snap = nil
	f := newUnopenedFixture(dir)
	err = f.db.OpenReader()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = f.db.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		err = f.db.Verify()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSortedPage(b *testing.B) {
	entries := make([]sortedEntry, 1_000_000)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		page := window(entries, 10, 0, false)
		if len(page) != 10 {
			b.Fatal("incorrect page length")
		}
	}
}

func BenchmarkIndexedOverwrite(b *testing.B) {
	f := benchFixture(b, func(o *Options) { o.Sync = SyncNever })
	v := &order{ID: "a", Email: "a@example.com", Customer: strings.Repeat("x", 960)}
	err := f.orders.Put(v)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		err = f.orders.Put(v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeFrameLarge(b *testing.B) {
	snap := scalingSnapshot(b, 1000)
	frame := &walFrame{Seq: 1, TS: 123}
	for key, raw := range snap.Collections["orders"] {
		frame.Ops = append(frame.Ops, walOp{Coll: "orders", Key: key, Op: opPut, Val: raw})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, err := encodeFrame(frame)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBudgetOverwrite(b *testing.B) {
	for _, n := range []int{1_000, 100_000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			f := benchFixture(b, func(o *Options) { o.Sync = SyncNever })
			for i := range n {
				key := strconv.Itoa(i)
				raw, err := json.Marshal(&order{ID: key})
				if err != nil {
					b.Fatal(err)
				}
				err = f.orders.applyPut(key, raw)
				if err != nil {
					b.Fatal(err)
				}
			}
			f.db.opts.MaxBytes = 1 << 40
			v := &order{ID: "0"}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				err := f.orders.Put(v)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
