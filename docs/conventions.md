# Development conventions

Run commands from the repository root using the Go version declared in
[`go.mod`](../go.mod) or newer. The module contains the datastore library,
the `dstest` test helper and the runnable `examples/orders` command.

## Local checks

```sh
go build ./...
go vet ./...
go test -race ./...
gofmt -l .
```

The race detector requires a working C toolchain. `gofmt -l .` should produce
no output. After dependency changes, run `go mod tidy` and review `go.mod` and
`go.sum`. CI rejects differences left by tidy.

For the exhaustive recovery sweep, in PowerShell:

```powershell
$env:DS_FULL_SWEEP = '1'
go test -count=1 ./...
Remove-Item Env:DS_FULL_SWEEP
```

In a POSIX shell:

```sh
DS_FULL_SWEEP=1 go test -count=1 ./...
```

Fuzz only the root package; Go fuzzing requires exactly one package:

```sh
go test -run '^$' -fuzz FuzzFrameParsing -fuzztime 60s .
```

Keep failing fuzz inputs as regression seeds under `testdata/fuzz/` when useful.
For performance and heap measurements, see the commands in
[historical measurements](internal/measurements.md#reproduce-with-current-fixtures).
Do not turn one machine's timings into correctness assertions.

The capacity probe is opt-in and writes roughly a gigabyte at one million records.
It uses four indexes, about 1 KiB per record, `SyncAlways`, 1,000-record ingest
transactions and disabled size-triggered compaction during the import:

```powershell
$env:DS_CAPACITY_RECORDS = '100000' # repeat separately with 1000000
go test -run '^TestCapacity$' -count=1 -v -timeout=15m .
Remove-Item Env:DS_CAPACITY_RECORDS
```

It reports ingest, compaction, reopen, overwrite latency, allocation totals, GC
cycles and heap after forced GC. Heap figures are retained live heap, not sampled
peak memory or an enforceable limit. Read the fixture before comparing workloads.
`BenchmarkSnapshotLarge`, `BenchmarkBudgetOverwrite` and
`BenchmarkEncodeFrameLarge` isolate snapshot, budget and WAL encoding costs.

`TestTransactionsAgainstModel` compares deterministic random transactions against
an independent map model, including rejection, rollback, indexes, byte accounting,
compaction and WAL-only recovery. Deterministic group tests pause real WAL sync
to cover flushing/pending uniqueness, failed batches and admission backpressure.

## CI coverage

The source of truth is [ci.yml](../.github/workflows/ci.yml). It runs on pushes
to `main`, pull requests, a weekly schedule and manual dispatch:

| Job | Platforms | Checks |
|---|---|---|
| Test | Linux, Windows, macOS | Build, vet, race suite with `DS_CRASH_ROUNDS=20` |
| Sweep | Linux, Windows | `DS_FULL_SWEEP=1`, `DS_CRASH_ROUNDS=40` |
| Fuzz | Linux | Root frame parser, 300 seconds |
| Format | Linux | gofmt and module tidiness |

Building `./...` compiles the orders example; CI does not run that program.
Race detection covers exercised executions, not every possible interleaving.
The simulated power-loss device models the WAL; it does not certify physical
storage devices or Windows snapshot metadata durability.

## Contracts when changing the implementation

- **Enforced:** `Register` rejects duplicate collection names and registration
  after opening. Index declarations also reject calls after opening. Keep
  declaration and recovery order compatible with those guards.
- **Enforced:** uniqueness is checked before appending a transaction, including
  pending group-commit writes. `TestUniqueViolationRejectsTheWholeTransaction`
  checks whole-transaction rejection.
- **Enforced:** writer acquisition checks for reentrant `Update` on the same
  goroutine using a best-effort goroutine ID. A wait timeout remains the
  fallback. `TestReentrantUpdatePanicsImmediately` exercises the guard.
- **Invariant:** under `SyncAlways`, acknowledge and publish a commit only
  after the covering WAL sync. Preserve sequence ordering across the pending
  window, including failure paths. See [the commit flow](internals.md#commit-flow).
- **Invariant:** index functions must be deterministic and treat records and
  returned term slices as read-only. Rebuilds
  and incremental updates must produce the same indexes; `Verify()` reports
  disagreement. Do not call the database from index functions.
- **Invariant:** use the transaction-scoped `In*` methods within callbacks.
  Direct methods can reacquire locks or start nested transactions. See
  [concurrency](guide.md#concurrency-what-may-be-called-from-where).
- **Practice:** use `dstest.New(t)`, declare collections and indexes, then
  `dstest.Open(t, db)` in consumer tests. Cleanup calls `Verify` and `Close`.
  Its default `SyncNever` is a speed setting, not a durability test.

## Relevant tests

| Contract | Evidence and limit |
|---|---|
| Truncated WAL recovery | `TestRecoveryFromTruncatedLog` checks recovered prefixes and indexes; the full sweep expands cut offsets |
| Process crash recovery | `TestCrashInjection` kills a subprocess and checks acknowledged writes; OS caches survive this test |
| WAL power-loss ordering | `powerloss_test.go` separates volatile and durable bytes and injects writes, sync failures and cuts |
| Group commit | `TestGroupCommitSharesOneFsync` checks flush sharing, not a fixed batching ratio |
| Snapshot integrity | `TestSnapshotChecksumCatchesSilentCorruption`, `TestSnapshotChecksumFallsBackToOlder`, `TestSnapshotFromNewerBuildIsRefused` |
| Migration | `TestMigrationUpgradesRecords`, `TestMigrationAppliesToLogFrames`, `TestMissingMigrationStepFails` |
| Backup | `TestBackupIsARestorableDirectory` checks restored state |
| Reader refresh | `TestReaderSeesSnapshotUntilReopened` checks the loaded view's lifetime |
| Watch catch-up | `TestCatchUpLeavesNoGapAndNoDuplicates` uses retained history and a sufficient buffer |

Extend the relevant tests when changing these contracts. Snapshot and WAL
changes must preserve supported reads and explicitly reject unsupported newer
formats. Windows and Unix have separate locking and directory-sync files;
local checks exercise only the host platform.

## Documentation changes

Keep setup and operation in [guide.md](guide.md), implementation contracts in
[internals.md](internals.md), and contributor workflows here. The
[README](../README.md) provides the minimal start and navigation. API contracts
and defaults belong next to their declarations in Go source. Historical
measurements belong in `docs/internal/`, with their evidence limits stated.
Check relative links and heading fragments after moving content. There is no
documentation generator or dedicated Markdown check in the current CI.
