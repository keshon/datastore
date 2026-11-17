# Historical measurements

The first section retains figures from repository documentation before 2026-11-05.
The dated hardening measurements below have a recorded fixture and environment.

The older figures appeared in the repository documentation before the 2026-11-05
revision. Hardware, Go version, measurement date and complete output were not
recorded alongside them. They remain historical observations, not current
performance or capacity guarantees.

## Previously published figures

| Operation | Reported observation |
|---|---|
| Commit, `SyncAlways` | fsync accounted for about 97% of elapsed time |
| Commit, `SyncNever` | About 30 microseconds |
| `Get` | About 2 microseconds |
| Full scan | About 2 microseconds per record; about 20 ms for 10,000 |
| `Open` | About 10 microseconds per record; about 100 ms for 10,000 |
| Index rebuilding | Described as taking about a millisecond |
| Group commit | One run reported 320 commits sharing 79 fsyncs |
| Race suite | About 30 seconds |
| Exhaustive recovery sweep | About three minutes |

For an index with 10,000 matches, taking the first ten was reported as 21 ms
and 100,000 allocations through `Find`, versus 1.4 ms and 99 allocations through
`All`. The relevant behavior is early termination: `Find` decodes the complete
set; `All` decodes per step. This does not promise a fixed speedup.

The published heap estimate was:

```text
heap ≈ payload + records × (90 + 145 × indexes) bytes
```

It described about 67 MB of overhead for 100,000 records with four indexes,
and heap-to-payload ratios of 7.4 at 127-byte records and 1.4 at 2 KB records.
These are fixture-specific estimates, not accounting performed by the library.
`Stats.Bytes` measures encoded payload and keys, excluding indexes and other
heap overhead. Do not use the formula as a memory limit.

## Reproduce with current fixtures

Run from the repository root:

```sh
go test -run '^$' -bench . -benchmem .
go test -run '^TestMemoryFootprint$' -count=1 -v .
go test -run '^TestGroupCommitSharesOneFsync$' -count=1 -v .
```

Record the commit, Go version, OS, CPU, filesystem, storage device, fixture and
complete output with new measurements. The group-commit test checks sharing;
it does not require the historical fsync count. See
[conventions](../conventions.md) for correctness checks and
[the guide](../guide.md#capacity-and-observability) for capacity monitoring.

## 2026-11-17 hardening measurements

Environment: Go 1.26.4, Windows/amd64, Intel Core i5-9600KF, six logical CPUs,
32 GB RAM, NTFS on drive D:. The physical storage model and competing desktop
activity were not controlled. Source: base `77da1078d1e420f2ae48bb9ee2c5931b7cf0eb52`
plus the uncommitted hardening changes. Results describe these fixtures, not a
published capacity or latency guarantee. MB and GB below are decimal units.

Snapshot encoding of 100,000 records reduced allocated bytes from about 422 MB
to 1.74 MB per operation. Output is still the readable, checksummed version-1
snapshot; the working set now consists mainly of sorted key references and
bounded output buffers. This benchmark excludes creating its input records.

The budget/overwrite fixture no longer scans all stored payloads after a commit
or shifts the sorted index when its term is unchanged. It measures repeated
overwrites with stable index terms. Random changes to sorted terms still shift
the sorted slice, so these numbers do not describe that workload.

Baseline, before the snapshot and accounting optimizations:

```text
go test -run '^$' -bench 'Benchmark(SnapshotLarge|BudgetOverwrite)$' -benchmem -benchtime=3x -count=3 .
goos: windows
goarch: amd64
pkg: github.com/keshon/datastore
cpu: Intel(R) Core(TM) i5-9600KF CPU @ 3.70GHz
BenchmarkSnapshotLarge-6                  3   872165900 ns/op   421917850 B/op   200044 allocs/op
BenchmarkSnapshotLarge-6                  3   712006000 ns/op   421917872 B/op   200043 allocs/op
BenchmarkSnapshotLarge-6                  3   839067333 ns/op   421919288 B/op   200049 allocs/op
BenchmarkBudgetOverwrite/1000-6           3       65800 ns/op        2992 B/op       30 allocs/op
BenchmarkBudgetOverwrite/1000-6           3       78933 ns/op        2992 B/op       30 allocs/op
BenchmarkBudgetOverwrite/1000-6           3       74867 ns/op        2992 B/op       30 allocs/op
BenchmarkBudgetOverwrite/100000-6         3     1642500 ns/op        3301 B/op       31 allocs/op
BenchmarkBudgetOverwrite/100000-6         3     1819500 ns/op        3301 B/op       31 allocs/op
BenchmarkBudgetOverwrite/100000-6         3     1767033 ns/op        3285 B/op       31 allocs/op
PASS
ok github.com/keshon/datastore 15.871s
```

After optimization; the extra frame benchmark was added subsequently:

```text
go test -run '^$' -bench 'Benchmark(SnapshotLarge|BudgetOverwrite|EncodeFrameLarge)$' -benchmem -benchtime=3x -count=3 .
goos: windows
goarch: amd64
pkg: github.com/keshon/datastore
cpu: Intel(R) Core(TM) i5-9600KF CPU @ 3.70GHz
BenchmarkSnapshotLarge-6                  3   543848500 ns/op     1740704 B/op       40 allocs/op
BenchmarkSnapshotLarge-6                  3   508931733 ns/op     1741061 B/op       45 allocs/op
BenchmarkSnapshotLarge-6                  3   514717533 ns/op     1740677 B/op       39 allocs/op
BenchmarkEncodeFrameLarge-6               3     6202267 ns/op     2528554 B/op        9 allocs/op
BenchmarkEncodeFrameLarge-6               3     5775333 ns/op     2528544 B/op        9 allocs/op
BenchmarkEncodeFrameLarge-6               3     5788800 ns/op     2528581 B/op        9 allocs/op
BenchmarkBudgetOverwrite/1000-6           3       59867 ns/op        2432 B/op       25 allocs/op
BenchmarkBudgetOverwrite/1000-6           3       53900 ns/op        2432 B/op       25 allocs/op
BenchmarkBudgetOverwrite/1000-6           3       50567 ns/op        2544 B/op       26 allocs/op
BenchmarkBudgetOverwrite/100000-6         3       61267 ns/op        2853 B/op       27 allocs/op
BenchmarkBudgetOverwrite/100000-6         3       72300 ns/op        2853 B/op       27 allocs/op
BenchmarkBudgetOverwrite/100000-6         3       54433 ns/op        2853 B/op       27 allocs/op
PASS
ok github.com/keshon/datastore 12.444s
```

For WAL encoding, the same 1,000-record frame fixture was also run for 100
iterations per sample. Only `encodeFrame` was temporarily restored from the
base commit for the baseline; the working file was restored before the current
encoder run. JSON encoder pool growth and GC affect the averages, so compare
the same iteration count. A separate test checks byte-for-byte payload identity.

```text
go test -run '^$' -bench '^BenchmarkEncodeFrameLarge$' -benchmem -benchtime=100x -count=3 .
BASELINE ENCODER
BenchmarkEncodeFrameLarge-6             100     6758175 ns/op     3102972 B/op       10 allocs/op
BenchmarkEncodeFrameLarge-6             100     6257316 ns/op     3233903 B/op       11 allocs/op
BenchmarkEncodeFrameLarge-6             100     5933390 ns/op     3181436 B/op       10 allocs/op
PASS
ok github.com/keshon/datastore 2.939s
CURRENT ENCODER
BenchmarkEncodeFrameLarge-6             100     5492796 ns/op     1199124 B/op        3 allocs/op
BenchmarkEncodeFrameLarge-6             100     5605359 ns/op     1209630 B/op        3 allocs/op
BenchmarkEncodeFrameLarge-6             100     5891700 ns/op     1356344 B/op        4 allocs/op
PASS
ok github.com/keshon/datastore 2.835s
```

### Capacity probe

`TestCapacity` uses about 1 KiB JSON records, two multi-valued indexes, one
unique index and one sorted index. Ingest inserts ascending timestamps in
1,000-record `SyncAlways` transactions. It sets `CompactAfterBytes` to 1 TiB
to measure import separately from compaction; this is not the default 8 MiB
compaction setting. It then compacts, releases the old in-memory dataset,
reopens from snapshot, performs 1,000 single-record overwrites with unchanged
index terms, and runs `Verify`. The old fixture's data is explicitly cleared
before measuring reopen because test cleanup retains the old fixture object.

`heap` is live process heap after a forced GC, not peak RSS. `allocated` is
the total heap allocation during that phase, not additional resident memory.
`gc_cycles` excludes the forced GC before and after each phase. The initial
ingest and recovery allocate decoded objects and index structures even though
the steady-state store retains encoded records. A million-record cold open
still reads the complete snapshot into memory; it is not streaming recovery.

```text
$env:DS_CAPACITY_RECORDS = '100000'
go test . -run '^TestCapacity$' -count=1 -v -timeout=15m
=== RUN TestCapacity
ingest: duration=3.1427356s allocated=734997488 heap=169476528 gc_cycles=21
compact: duration=583.7449ms allocated=1814256 heap=166136056 gc_cycles=0
open: duration=2.8147341s allocated=456668576 heap=165736584 gc_cycles=3
SyncAlways overwrite: p50=1.0083ms p95=1.7336ms p99=4.3399ms
overwrite-1000: duration=974.7861ms allocated=6711824 heap=165764640 gc_cycles=0
verify: duration=1.3683781s allocated=169923024 heap=165750864 gc_cycles=1
records=100000 payload_bytes=107988890 wal_bytes=1157999 pending_bytes=0
--- PASS: TestCapacity (9.62s)
PASS
ok github.com/keshon/datastore 10.703s
```

```text
$env:DS_CAPACITY_RECORDS = '1000000'
go test . -run '^TestCapacity$' -count=1 -v -timeout=15m
=== RUN TestCapacity
ingest: duration=32.023224s allocated=7335492328 heap=1902620656 gc_cycles=32
compact: duration=6.0748571s allocated=16215256 heap=1894561384 gc_cycles=0
open: duration=23.4039388s allocated=5059197584 heap=1893186664 gc_cycles=3
SyncAlways overwrite: p50=1.0105ms p95=1.5906ms p99=2.9212ms
overwrite-1000: duration=908.8369ms allocated=6703824 heap=1893211328 gc_cycles=0
verify: duration=12.096136s allocated=1835030288 heap=1893200960 gc_cycles=1
records=1000000 payload_bytes=1080888890 wal_bytes=1158898 pending_bytes=0
--- PASS: TestCapacity (82.73s)
PASS
ok github.com/keshon/datastore 83.931s
```

No million-record before/after timing comparison was run. The smaller fixture's
different p99 is sampling and environment variation, not evidence that larger
databases improve latency. These tests establish successful operation for the
record/index shape above, not an upper capacity bound or power-loss certification.

### Validation

`go build ./...`, `go vet ./...`, and `git diff --check` passed. The full root
race suite with `DS_FULL_SWEEP=1` and `DS_CRASH_ROUNDS=40` passed in 186.549s.
The frame parser fuzz target ran for 60 seconds with two workers: 4,674
executions, no failure. Additional repeated race tests cover the new group,
snapshot, close and watch boundaries. Linux and macOS builds passed with
`CGO_ENABLED=0`; their tests were not executed on this Windows host.

Changed Go files pass gofmt. A repository-wide `gofmt -l .` also lists pre-existing
CRLF-only files in this checkout; their original bytes were retained to avoid
unrelated working-tree changes. No formatting-only content changes are included.

## 2026-11-17: commit preparation, verification and sorted pages

The comparison baseline is `d4ef3c5`, after the hardening above, on the same
Windows/i5-9600KF host. These changes preserve the public API and WAL format.

`BenchmarkSortedPage` takes ten entries from a million-entry sorted index. Its
temporary allocation fell from 24,002,560 to 240 bytes per operation, with one
allocation in both versions. The result slice now reserves the requested page
size rather than all remaining entries. This benchmark measures the internal
page selection, excluding record decoding.

`BenchmarkIndexedOverwrite` overwrites one roughly 1 KB record with a unique
index, using `SyncNever` to isolate allocation costs. At 10,000 operations per
sample, baseline runs allocated 6,266-6,267 bytes and 29 allocations per operation;
prepared-value reuse allocated 4,902-4,904 bytes and 21 allocations. The private
decoded value is reused for constraint checks and application, including pending
group-commit checks. No caller-owned value is retained. These figures do not
predict durable commit latency, where storage sync remains relevant.

`BenchmarkVerifyLarge` checks 100,000 roughly 1 KB records. Baseline allocated
158,933,323 bytes and 1,001,051 allocations per check; the final implementation
allocated 140,800,009-140,800,089 bytes and 1,000,000 allocations in three samples
of five checks. Verification no longer retains decoded records or rebuilt
indexes, and now checks reverse links and payload accounting too. JSON decoding
still accounts for most allocations. CPU and allocation profiles were inspected
with `go tool pprof`, focusing on verification; profiles also contain fixture
construction and benchmark calibration, so their overall totals are not treated
as verification-only figures. Raw profiles remain temporary local artifacts.

An intermediate streaming implementation was also compared with the baseline
using `DS_VERIFY_RECORDS=100000 go test . -run '^TestVerifyWorkingSet$' -count=3 -v`.
Starting from about 152.74 MB live heap after explicit GC, baseline sampled peak
growth was 141.60-146.79 MB; streaming growth was 139.19-140.09 MB. Both triggered
one GC cycle. Sampling every 5 ms measures Go heap, not exact peak or process RSS.
Removing retained objects did not produce a dramatic observed heap-peak or GC
count improvement in this fixture. Do not infer one from lower allocation totals.

The verification allocation benchmark batch overlapped capacity testing; its
elapsed times are deliberately omitted. Sorted-page and overwrite benchmarks
were repeated afterward without those tests running. Allocation counts are
observations for these fixtures, not performance guarantees.

Capacity tests passed again at both 100,000 and 1,000,000 records, with the same
fixture and compaction threshold as above. The million-record import allocated
5,829,593,360 bytes with 25 GC cycles, versus 7,335,492,328 bytes and 32 cycles in
the first hardening run. This is about 20.5% less allocation in this workload.
The first part of the new capacity run overlapped the verification benchmark;
neither elapsed time nor GC-cycle count is a controlled speed comparison.
Post-GC resident Go heap remained about 1.893 GB, because the stored representation
and index layout have not changed. Verification allocated 1,447,999,696 bytes
versus 1,835,030,288 previously, with one GC cycle in each run. Compaction still
allocated about 16.2 MB with zero GC cycles. Reopening and integrity checks passed;
these remain process tests, not physical power-loss certification.

The full race suite with `DS_FULL_SWEEP=1` and `DS_CRASH_ROUNDS=40` passed in
339.219s. The frame-parser fuzz target ran for 60 seconds with two workers,
2,364 executions and no failure. New tests cover corrupt forward/reverse index
mappings, duplicate sorted entries, unique-value corruption, payload accounting,
preflight rejection before WAL append, value entities, and caller mutation of
fields and nested slices while sync is paused. The last ownership test passed
ten repetitions with the race detector after its nested-slice assertion was added.
`go vet ./...`, `go build ./...`, formatting of the changed Go files, and
`git diff --check` also passed.
