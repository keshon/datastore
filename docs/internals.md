# Internals

The on-disk format, what durability actually guarantees, and the tests that
hold it to that. Read this before changing anything in the commit path.

---
## On disk

```
dir/
  LOCK                        exclusive, held for the lifetime of the process
  snapshot-000000000412.json  full state as of sequence 412
  wal.log                     framed, append-only, everything after that
  archive-000000000412.log    a past segment, kept only when History is enabled
```

A frame is `u32 length | u32 crc32c | JSON payload`. Recovery loads the newest
snapshot that verifies and replays the log on top, stopping at the first frame
that is short, fails its checksum, or arrives out of sequence — and truncating
there. Stopping is the point: once one decision in the log is unreadable, the
ones after it cannot be trusted to apply to the state we think we have.

**Snapshots are checksummed too.** A snapshot is an envelope carrying a
crc32c over the exact payload bytes:

```json
{"v":1,"crc32c":2882343476,"payload":{"seq":412,"collections":{...}}}
```

"It parsed as JSON" is not an integrity check — a bit flipped inside a string
value, or inside a digit, decodes perfectly into the wrong data. Since a
snapshot holds the whole dataset rather than one transaction's worth of it, an
unchecked one was the largest unguarded object on disk. A snapshot that fails
its checksum is passed over in favour of the previous one and reported through
`RecoveryReport.SnapshotsSkipped`, which makes `LostData()` true.

Snapshots written before this existed are plain payloads with no envelope. They
still load, unverified, and the next compaction rewrites them in the checked
format. A snapshot from a *newer* build fails with `ErrSnapshotVersion` rather
than falling back, because quietly opening at an older snapshot would serve
stale state as current.

Compaction writes a new snapshot, fsyncs it, fsyncs the directory, and only then
truncates the log. A crash between those steps leaves a snapshot plus a log that
replays cleanly on top of it: wasteful, never wrong. The previous snapshot is
kept for the same reason. Creating `wal.log` fsyncs the directory as well —
syncing a file makes its contents durable, not the directory entry naming it.

On Windows `syncDir` is a no-op, because the platform exposes no directory
fsync. Snapshot installation there relies on NTFS metadata ordering rather than
an explicit barrier, so the guarantee is weaker than on Unix.

**Payloads are JSON deliberately.** A binary codec would be smaller and faster,
and at a few writes per minute neither matters — whereas reading the log with
`less` when something has gone wrong matters a great deal.

**Indexes are never written to disk.** They are rebuilt from the records at
`Open` in about a millisecond. Persisting them would add a second thing that can
be corrupt and a second thing to keep in step with the first.

**`Get` returns a freshly decoded copy**, never a shared pointer. That costs
microseconds and removes this design's worst failure mode: a caller mutating a
struct it was handed, the change never reaching the log, and the indexes quietly
disagreeing with the data.

**A collection this build does not register is preserved, not deleted.** Its
records ride through compaction untouched and reappear when it is registered
again. Otherwise removing one `Register` call and deploying would drop a table
with no error and no way back.

---

## Durability, per database

`Options.Sync` is set per directory, not per write:

| Mode | Behaviour | For |
|---|---|---|
| `SyncAlways` | fsync before `Update` returns | orders, customers, money |
| `SyncInterval` | fsync on a timer | sessions, carts — cheap to recreate |
| `SyncNever` | leave it to the OS | tests |

Run two databases when the workloads differ. Sessions churn orders of magnitude
more than orders do and would otherwise dominate the log and force constant
compaction; losing a guest cart on an unclean shutdown is fine, losing an order
is not.

### Group commit

Concurrent commits share one fsync. The writer slot is handed back as soon as a
frame is in the log, so the next transaction runs its body and appends while the
previous one is still waiting for the disk; whichever writer gets there first
fsyncs for everybody waiting and applies the batch in sequence order.

This does not weaken anything above. `Update` still returns only once the commit
is durable, and a frame becomes *visible* only when it is applied, which happens
after the fsync that covers it — so a reader can still never see a commit that
would vanish in a power cut.

Two consequences worth knowing:

- **A transaction that reads waits** for commits already in flight to become
  durable, so a read-then-write sequence still observes everything before it.
  Write-only transactions — including `Collection.Put` and `Collection.Delete` —
  never wait. That is the shape that scales.
- **Only `SyncAlways` batches.** The other modes never fsync on the commit path,
  so there is nothing to amortise.

`TestGroupCommitSharesOneFsync` pins the behaviour by counting fsyncs rather
than by timing anything: 320 concurrent commits cost 79 fsyncs, about 4 commits
per flush. Counting is deterministic, and a wall-clock assertion on a shared CI
runner would not be.

### What `SyncAlways` guarantees

**If `Update` returned `nil`, the commit survives the machine losing power.**
Not just the process dying — the machine. That distinction is the whole point,
and it is tested rather than argued: `powerloss_test.go` runs the log on a
device that keeps what a process can read separate from what is actually on the
platter, where only `fsync` moves bytes between them.

The tests cover a power cut at an arbitrary moment under concurrent writers, a
cut with a sync in flight so the last frame landed only in part (at every
prefix length), an `fsync` that fails, and a write that fails.

Two consequences worth stating plainly:

- **A failed `fsync` is never acknowledged**, and latches `ErrFailed`. There is
  no honest recovery from it in place: on Linux a failed `fsync` can leave the
  kernel having already dropped the dirty pages, so a later `fsync` may report
  success without them ever reaching the disk. Reopen to find out what actually
  survived.
- **`SyncInterval` acknowledges before durability**, by design. A power cut
  costs the commits since the last flush. What survives is always a *prefix* of
  the commit order — you can lose the tail, never a hole in the middle.

---

## Tests

```bash
go test ./... -race
```

```bash
DS_FULL_SWEEP=1 go test ./...
```

```bash
go test ./... -fuzz FuzzFrameParsing -fuzztime 60s
```

The first takes about 30 seconds; the exhaustive sweep about three minutes.

CI runs all of it on Linux, Windows and macOS on every push: the race detector
with 20 crash-injection rounds everywhere, the exhaustive sweep with 40 rounds
on Linux and Windows, and five minutes of fuzzing. Windows is in the matrix
rather than assumed — it has its own file locking, and `syncDir` is a no-op
there, so it is the platform where the durability story is weakest and the code
least exercised.

The crash tests are the reason to trust any of this:

- **`TestRecoveryFromTruncatedLog`** cuts the log short at many offsets —
  concentrated at frame boundaries, every single byte under `DS_FULL_SWEEP=1` —
  and at each one asserts that the recovered state equals *exactly* the prefix
  of commits the store claims it applied, that the count never decreases as the
  cut moves later, and that every index matches a brute-force recomputation.
- **`TestCrashInjection`** runs a child process committing in a loop, kills it
  with a signal it cannot handle, and verifies that every commit the child had
  acknowledged is still there.
- **`powerloss_test.go`** does what `TestCrashInjection` cannot. After `SIGKILL`
  the kernel still owns the page cache and writes it out, so every byte the
  process wrote reaches the disk whether it was fsynced or not — a build that
  never called `fsync` at all passes that test. These run the log on a simulated
  device where only `fsync` makes anything durable, and assert the same
  contract against a machine that simply stops.
- **`FuzzFrameParsing`** feeds garbage to the frame reader.

Do not weaken these to make a change land. Without them this is a JSON file with
extra steps.

`dstest` has the boilerplate for consumers' tests: `dstest.New(t)` gives an
unopened database in a temp directory configured for speed, and `dstest.Open(t,
db)` opens it and arranges for `Verify` plus `Close` when the test ends.
