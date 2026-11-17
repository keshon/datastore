# Internals

Components, state lifetime, disk format and durability contracts. For operation,
see [the guide](guide.md); for checks and contribution rules, see
[conventions](conventions.md).

## Components and state ownership

| Component | Owns |
|---|---|
| `DB` (`db.go`) | Directory options, collection registry, sequence, counters, lifecycle and background workers |
| `Collection[T]` (`collection.go`) | JSON-encoded records and collection schema version |
| Indexes (`index.go`) | Derived in-memory lookup structures and extraction functions |
| `Tx` (`tx.go`) | Callback-scoped staged writes, deletes, counters and annotations |
| WAL (`wal.go`) | Framed append-only commit data and file synchronization |
| Group commit (`groupcommit.go`) | Pending commit window and shared flush coordination |
| Snapshots (`snapshot.go`) | Complete state encoding, checksum envelope and snapshot installation |
| Recovery and migration | Snapshot selection, live-log replay, version conversion and recovery report |
| History and watches | Disk-based audit reads and bounded in-memory event queues |

`New` describes a database without I/O. Collections, indexes and migrations are
declared before `Open` or `OpenReader`. Writable `Open` holds an exclusive
process lock until close, restores state, persists migrations by compaction,
and starts configured background workers. `Close` stops workers and attempts
compaction before releasing resources. Check its returned error.

Records stay encoded; each read decodes a new value, so changing a returned
struct cannot mutate stored state or indexes. Indexes are rebuilt when records
are loaded or replayed. During recovery, sorted indexes accumulate final terms
by record key and sort once after WAL replay, avoiding repeated slice shifts.
Normal updates keep unchanged index terms in place. Index extraction must be
deterministic for a record and must not mutate its input or returned term slices.

Commit preflight decodes each staged put into a collection-owned value once.
Unique constraints, including other pending frames, use these prepared values;
application reuses them after the WAL durability step. Callers cannot access the
prepared values, and each frame drops its decoded references as operations apply.
They are never serialized: WAL replay decodes stored JSON and performs migrations
normally. This avoids validating and applying separately decoded copies. Pending
byte limits still count encoded WAL bytes, not these transient decoded values.

Transactions stage changes until commit. Collection reads through `In(tx, c)`
can see staged writes; transaction-scoped index reads use committed indexes.
There is no MVCC. Unregistered collections and their schema versions are kept
as opaque records and carried through compaction, allowing a later build to
register them again without silently deleting their data.

`OpenReader` restores a loaded view without a writer lock, workers or disk
writes. Migrations happen only in that reader's memory. Later commits do not
refresh its records; close and reopen to reload. History reads disk separately
and is not restricted to the reader's loaded sequence.

## On disk

```text
dir/
  LOCK                        exclusive writer lock, held until close
  snapshot-000000000412.json  full state as of sequence 412
  wal.log                     framed live log
  archive-000000000412.log    retained history segment, if enabled
```

A WAL frame is `u32 length | u32 crc32c | JSON payload`. JSON payloads allow
record inspection without a separate binary codec, although WAL files include
binary framing. Snapshots contain records, sequence, counters and schema versions.
The current snapshot envelope includes a format version, CRC32C over the exact
payload bytes, the payload, and a top-level downgrade marker named `collections`.
See [snapshot.go](../snapshot.go) for its exact encoding.

Recovery loads the newest usable snapshot and replays `wal.log`, skipping
frames already covered by the snapshot. It stops at a short, corrupt or
out-of-sequence frame. Writable recovery truncates that tail; read-only recovery
reports it without changing the files. Inspect `Recovery()` even after a
successful open.

A damaged snapshot is skipped and reported. Falling back to an older snapshot
can lose state if the remaining live log cannot bridge it to the current state;
retained archives are history only and are not replayed during recovery.
`LostData()` flags skipped snapshots or a truncated log; it does not quantify
which acknowledged records were lost. CRC32C detects checksum mismatches;
it does not provide cryptographic authentication or repair damaged data.

### Compaction

Compaction encodes the full dataset, writes a temporary snapshot, syncs the file,
renames it into place, and syncs the directory on Unix before discarding or
archiving the covered live log. At least two snapshots are retained. The
sequence lets replay ignore already-snapshotted frames if a crash leaves both
snapshot and log. The writer slot remains held throughout compaction. Once
pending commits are drained, snapshot encoding borrows immutable record maps
without copying them, then releases the state lock during encoding and file I/O.
Readers can proceed during this phase; writers still wait. Log reset or rotation
uses the state lock again.

Encoding streams the payload and its CRC32C through bounded buffers, then writes
the checksum after the payload in the same version-1 envelope. Keys are sorted
per collection, requiring O(records) temporary key references; payload-sized JSON
buffers are no longer constructed. Decoding borrows the envelope payload from
the input file buffer, while record RawMessages own their bytes. Opening still
reads the complete snapshot file and rebuilds indexes in memory.

Windows `syncDir` remains a no-op. Snapshot installation uses `MoveFileExW` with
`MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH` after syncing and closing the
temporary file. This requests write-through installation, with long-path support,
but does not establish the same directory-sync guarantee as Unix or certify
physical power-loss behavior. See Microsoft's
[MoveFileExW contract](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw).

### Upgrading and downgrading

Legacy snapshots without an envelope load without checksum verification; the
next compaction rewrites them in the checked format. A snapshot with a newer
format version fails with `ErrSnapshotVersion` rather than falling back to a
potentially stale snapshot. Collection schema versions are checked separately;
migrations are described in [the guide](guide.md#schema-changes).

Downgrading below v1.4 is unsupported. Older builds expect top-level
`collections` to be an object; the current envelope writes a string there to
force a decoding error instead of silently accepting an empty database. Those
builds may still skip snapshots and open with missing state, reporting the
problem through recovery. The marker does not make a downgrade safe. Restore
a pre-upgrade backup with its compatible binary when rollback is required.

## Commit flow

1. Acquire the writer slot and run the callback without holding the state lock
   throughout. Reads that depend on earlier pending commits wait for those
   commits to be applied.
2. Encode staged changes and validate uniqueness, including the pending window.
   Returning a callback error appends nothing.
3. Append one frame for the nonempty transaction and assign its sequence.
4. Under `SyncAlways`, enqueue the frame, release the writer slot, flush the
   pending batch and apply covered frames to memory in sequence order.
   Acknowledge only after this transaction is flushed and applied.
5. Under the other sync modes, apply after append without a commit-path flush.
6. Deliver watch events after application. Commit statistics are reported on
   the committing goroutine through `OnCommit` when configured.

Preflight also rejects JSON that cannot decode into the collection type, before
appending anything, including collections with no unique index. This failure does
not latch the database as failed; a later valid transaction can proceed.

The state `RWMutex` protects records, indexes and applied sequence. The writer
slot serializes transaction bodies and log appends; group-commit coordination
allows later writers to append while an earlier writer waits for sync.
Write-only transactions avoid waiting for the pending window to drain on reads.
Use the [concurrency rules](guide.md#concurrency-what-may-be-called-from-where)
when combining direct reads, callbacks and iterators.

### Group commit

Concurrent `SyncAlways` commits can share a flush. Releasing the writer slot
before waiting permits batching without publishing unflushed state. Other
modes do not use this commit-path batching because they do not flush there.
The batching ratio depends on scheduling and I/O; there is no fixed ratio.

Constraint checks fold the flushing batch before the next pending batch, in
sequence order, ignoring frames already applied under the state lock. The
flushing batch stays registered until settlement. A failed flush latches failure;
later queued batches are rejected rather than retried and acknowledged.
`MaxPendingBytes` optionally applies backpressure before append. Staging limits
and pending-frame limits are described in the guide.

## Durability, per database

`Options.Sync` applies to the database directory, not individual writes:

| Mode | Acknowledgment and visibility | Power-loss exposure |
|---|---|---|
| `SyncAlways` | After the covering WAL sync and memory application | Acknowledged writes depend on successful sync, storage honoring it and supported snapshot installation semantics |
| `SyncInterval` | After append and memory application; background sync defaults to every 5 seconds | Unflushed tail can be lost; the timer is not a hard loss bound |
| `SyncNever` | After append and memory application; no periodic WAL flush | No acknowledgment-time power-loss guarantee |

`SyncAlways` is the default. A process kill leaves OS caches alive, so surviving
that test alone does not establish machine power-loss durability. Separate
process-crash and simulated-device tests exercise those different failure models.
The Windows snapshot limitation described above remains relevant in every mode.

A WAL write or sync failure latches a failed state and later writes return
`ErrFailed`; a successful retry of sync cannot establish what survived the
original failure. Close and reopen to recover and inspect the recovery report.
An error during application of an already-durable frame also latches failure:
state and log may disagree, and subsequent recovery may fail on that frame.
A log-reset failure during compaction also latches failure: the installed snapshot
still covers the state, but the live WAL header may be partial.

## Tests

[Test commands, CI coverage and relevant contracts](conventions.md) are maintained
in the contributor guide. The simulated power-loss tests model volatile versus
durable WAL bytes, torn writes, sync failures and concurrent commits. Process
crash tests check acknowledged writes after killing a child process. Snapshot
checksums, fallback, migrations and restored backups have separate tests.
Neither suite replaces restore verification for an actual deployment.

`Verify` decodes records one at a time and recomputes expected index terms. It
checks every expected forward link and each record's reverse terms. Counting
distinct expected links then detects extra multi-index links without retaining
a rebuilt index or scanning each tag list repeatedly. It also checks unique
owners, sorted-entry counts and strict ordering, dangling reverse keys, and cached
payload accounting. Scratch term storage is reused and bounded by the largest
individual term list, rather than retaining all decoded records.
