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
are loaded or replayed. Index extraction must be deterministic for a record.

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
snapshot and log. Full snapshot encoding requires additional memory, and state
application can wait while compaction holds locks.

Windows `syncDir` is a no-op. Snapshot installation therefore lacks the explicit
directory-sync barrier used on Unix. Atomic rename and file-content sync do not
establish the same metadata durability guarantee. The process and simulated WAL
crash tests do not prove every filesystem's power-loss behavior.

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

## Tests

[Test commands, CI coverage and relevant contracts](conventions.md) are maintained
in the contributor guide. The simulated power-loss tests model volatile versus
durable WAL bytes, torn writes, sync failures and concurrent commits. Process
crash tests check acknowledged writes after killing a child process. Snapshot
checksums, fallback, migrations and restored backups have separate tests.
Neither suite replaces restore verification for an actual deployment.
