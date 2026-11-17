# Running and using datastore

Setup, configuration and operation for applications embedding datastore. Start
with the [README quickstart](../README.md#quickstart). Requirements follow
[`go.mod`](../go.mod), which declares Go 1.25.0. Install with
`go get github.com/keshon/datastore` from your application module.

For the on-disk format, the durability guarantees and how they are tested, see
[internals.md](internals.md). For the full API, see
[pkg.go.dev](https://pkg.go.dev/github.com/keshon/datastore).

## Contents

- [Setup and configuration](#setup-and-configuration)
- [Collections and indexes](#collections-and-indexes)
- [Transactions](#transactions)
- [Concurrency: what may be called from where](#concurrency-what-may-be-called-from-where)
- [Schema changes](#schema-changes)
- [Knowing when something went wrong](#knowing-when-something-went-wrong)
- [Capacity and observability](#capacity-and-observability)
- [The log as an audit trail](#the-log-as-an-audit-trail)
- [Watching for changes](#watching-for-changes)
- [Reading from a second process](#reading-from-a-second-process)
- [Backup](#backup)

## Setup and configuration

Create a database with `New`, declare collections, indexes and migrations, then
call `Open`. `New` performs no I/O. `Open` creates the directory if needed,
takes its exclusive writer lock, loads a snapshot and replays the live log.
Check `Recovery()` after opening an existing store, and check the error from
`Close()` during application shutdown. Writable close attempts a final compaction.
If close returns `ErrWriteTimeout`, the active writer still owns its resources;
the database refuses new updates, and `Close` can be retried after that writer
finishes. Do not remove the directory or start another owner before cleanup succeeds.

Use a dedicated persistent `Options.Dir`. Collections, counters, schema versions,
snapshots and the log belong to that directory. Indexes are rebuilt in memory;
index declarations and migration code remain part of the application.

[`Options` and its defaults](../db.go) are the source of truth:

| Option | Default | Behavior |
|---|---|---|
| `Dir` | Required | Directory owned by this database |
| `Sync` | `SyncAlways` | Flush before acknowledgment; see [durability](internals.md#durability-per-database) |
| `SyncEvery` | 5 seconds | Flush interval for `SyncInterval` |
| `CompactAfterBytes` | 8 MiB | WAL size threshold for compaction |
| `CompactInterval` | 15 minutes | Periodic compaction; `CompactNever` disables this timer only |
| `KeepSnapshots` | 2, also the minimum | Number of snapshots retained |
| `Logger` | No-op logger | Recovery, compaction and budget messages |
| `Clock` | `time.Now` | Commit timestamps |
| `WriteTimeout` | 30 seconds | Wait limit for the writer slot, not callback execution |
| `MaxBytes` | Disabled | Encoded payload budget; does not reject writes |
| `WarnAtPercent` | 80 | Percentage of `MaxBytes` that triggers a warning |
| `OnCommit` | `nil` | Commit callback on the committing goroutine |
| `HistorySegments` / `HistoryFor` | Both disabled | Retained compacted WAL segments by count and age |

Zero or negative values select defaults for `SyncEvery`, `CompactAfterBytes`
and `WriteTimeout`. A negative `CompactInterval` disables its timer; zero
selects the default. Size-triggered and close-time compaction still run.
Both enabled history bounds apply; zero disables that particular bound.

For bounded ingestion, `MaxTransactionOps` limits distinct staged collection/key
pairs, and `MaxTransactionBytes` limits their encoded keys, collection names and
JSON values. Replacing a staged key adjusts its existing cost. Both default to
unlimited. A rejected `Put` or `Delete` returns `ErrTransactionTooLarge`; return
that error from the callback to abandon the whole transaction. Metadata, counters
and JSON framing are excluded; the WAL still enforces its separate frame limit.

Under `SyncAlways`, `MaxPendingBytes` bounds encoded frames waiting for group
commit to settle, including the flushing batch. A writer waits before appending
if the next frame would exceed the bound. A frame larger than the entire bound
returns `ErrTransactionTooLarge` without appending. The default is unlimited;
other sync modes have no group-commit window and ignore this option. These
bounds do not cap the heap used by application values, JSON encoding or indexes.

## Collections and indexes

Records implement `Key() string` and must round-trip through JSON. Keep keys
stable and unique within a collection. Declare indexes before opening:

```go
orders := datastore.Register[*Order](db, "orders")
byCustomer := datastore.AddIndex(orders, "customer",
    func(o *Order) []string { return []string{o.Customer} })
byEmail := datastore.AddUnique(orders, "email",
    func(o *Order) string { return o.Email })
byDate := datastore.AddSorted(orders, "date",
    func(o *Order) int64 { return o.Created.Unix() })
```

`AddIndex` maps each term to multiple records; `AddUnique` permits one record
per nonempty term, with an empty term opting out; `AddSorted` orders by `int64`.
Index functions must be deterministic for a record and must not call back into
the database. The [orders example](../examples/orders/main.go) shows all three.

Direct reads return freshly decoded copies:

```go
o, ok := orders.Get("1")
list := byCustomer.Find("cust-7")
o, ok = byEmail.Get("buyer@example.com")
recent := byDate.Desc(10, 0)
```

`Find` decodes the complete matching set. `All` decodes one record per step,
so breaking early avoids decoding the remaining matches. Iterators still hold
read locks; follow the [concurrency rules](#concurrency-what-may-be-called-from-where).
For complete signatures, use the
[API reference](https://pkg.go.dev/github.com/keshon/datastore).

---
## Transactions

This transaction writes an order and debits a loyalty balance atomically. The
fragment assumes the application has declared `orders`, `customers` and its
domain errors:

```go
err := db.Update(func(tx *datastore.Tx) error {
    o := datastore.In(tx, orders)
    c := datastore.In(tx, customers)

    cust, ok := c.Get(customerID)
    if !ok {
        return ErrNoSuchCustomer
    }
    if cust.Bonus < spend {
        return ErrInsufficientBonus
    }
    cust.Bonus -= spend
    if err := c.Put(cust); err != nil {
        return err
    }

    period, seq := tx.NextInPeriod("order", now, "0601")
    return o.Put(&Order{
        ID:  tx.NextID("order"),
        Num: fmt.Sprintf("%s/%d", period, seq),
    })
})
```

A nonempty transaction is encoded as one log frame. Under `SyncAlways`, it is
flushed before acknowledgment; concurrent commits may share that flush.
Returning an error from the callback discards its staged changes and writes
nothing.

Read inside the transaction, through `In(tx, c)`, when the write depends on
what you read. A separate `Get` followed by `Update` can overwrite a concurrent
change: the writer slot serializes transaction bodies, not the earlier read.

Queue outbound email *inside* the transaction as a record, and send it from a
worker outside. An SMTP failure then leaves the committed order intact. Retry
pending records after restart and account for duplicate delivery if the process
crashes after sending but before marking the record complete.

Do not call `Update` or direct `Collection.Put` / `Delete` from inside `Update`.
They attempt to acquire the writer slot already held by that callback. The
runtime attempts to detect same-goroutine reentry and panic immediately; the
writer timeout remains a fallback. Use `In(tx, c)` for writes inside a callback.

---

## Concurrency: what may be called from where

One transaction body runs at a time. `Update` does not hold the state lock
throughout the callback or WAL sync. Applying changes, compaction and other
operations that take the state write lock can block readers. `View` holds a
read lock throughout its callback; keep it short and avoid I/O.

Use transaction-scoped reads inside callbacks:

| Where you are | Read with | Not with |
|---|---|---|
| Outside any transaction | `orders.Get`, `byTag.Find`, `orders.All()` | — |
| Inside `Update` or `View` | `In(tx, orders)`, `InIndex(tx, byTag)`, `InUnique(tx, byEmail)`, `InSorted(tx, byDate)` | `orders.Get`, `byTag.Find` — the direct methods |

`View` holds the read lock for its whole callback, and the direct methods take
that lock again. Go's `sync.RWMutex` is not reentrant: a second `RLock` with a
writer queued in between deadlocks. The `In*` wrappers know the lock is already
held and avoid reacquiring it in `View`.

Iterator and staged-state rules:

- `Collection.All()` holds the read lock for the whole iteration, so its body
  must not write. Collect keys first, then write after the loop.
- Index reads inside a transaction see *committed* state. A record staged
  earlier in the same transaction is not indexed until commit; read it back
  through `In(tx, c).Get`.

---

## Schema changes

Adding a field generally needs no migration when its zero value is acceptable.
Use migrations for renamed fields, changed units or other transformations of
existing records. Each registered transform changes one record's raw JSON;
cross-collection restructuring requires an application-level procedure.

This example upgrades version 1 records to version 2. Unversioned collections
start at version 0; supply every step from the oldest version still on disk:

```go
users := datastore.Register[*User](db, "users", datastore.SchemaVersion(2))
datastore.Migrate(users, 1, func(raw json.RawMessage) (json.RawMessage, error) {
    // v1 stored "name"; v2 stores "full_name"
    var m map[string]json.RawMessage
    if err := json.Unmarshal(raw, &m); err != nil {
        return nil, err
    }
    if v, ok := m["name"]; ok {
        m["full_name"] = v
        delete(m, "name")
    }
    return json.Marshal(m)
})
```

Migrations take and return raw JSON so they can read old field layouts without
requiring the old Go types in the application. Back up an existing deployment
before opening it with a new schema version; verify the result on a separate
copy first. Retain the old binary and its pre-migration backup for rollback.

At `Open`, records below the declared version are carried up one step at a time
— snapshot rows and log frames alike — and the database is compacted before
`Open` returns, so the log never holds a mix of versions and the work is not
repeated on the next start. A collection schema version newer than the one
declared by the application fails to open. `OpenReader` applies migrations only
in memory and does not compact.

---

## Knowing when something went wrong

Writable recovery can discard a damaged log tail, and snapshot fallback can
restore an older state. A successful `Open` alone does not establish that no
data was lost. Inspect the report:

```go
if rep := db.Recovery(); rep.LostData() {
    log.Error().
        Interface("recovery", rep).
        Msg("datastore recovery reported possible data loss")
}
```

The logging fragment uses zerolog's package logger. `LostData()` is true for
skipped snapshots or a truncated log; it cannot identify exactly which
acknowledged records were lost. Investigate the full report before accepting
new work. `Clean()` also requires no migrations, so it is false after a routine
schema upgrade without necessarily indicating data loss.

`db.Verify()` recomputes index terms from records and reports disagreements in
both index directions, ordering, extra entries and payload accounting. It reads
one decoded record at a time instead of retaining a decoded copy of the dataset.
Run it in application tests or behind an admin command when checking integrity.
`dstest.Open` runs it automatically when a test finishes.

If a log write ever fails, the store latches that failure and every later write
returns `ErrFailed`. The log may hold a torn frame, so appending more data could
put acknowledged writes after a frame recovery cannot read. Sync failures also
latch failure. Close and reopen, inspect recovery and verify application state
before resuming service. Preserve a copy of the affected files for investigation.

## Capacity and observability

`Options.MaxBytes` is an advisory budget, not a memory limit. A warning is emitted
on crossing `MaxBytes * WarnAtPercent / 100`; the percentage defaults to 80.
Supply `Options.Logger` to receive messages: the default logger discards them.
`Stats()` reports `Bytes` and `Budget` whether or not logging is configured.

`Stats.Bytes` sums key lengths and encoded record lengths. It excludes maps,
index structures and temporary decoded values. Payload accounting is incremental;
checking the budget does not scan the records after each commit. Snapshots stream
record bytes and checksums, with bounded output buffers and a sorted key list per
collection. Writers wait during compaction; readers can proceed during snapshot
encoding and I/O, but can still wait during log reset or rotation.

`Stats.PendingBytes` and `PendingFrames` include the current flushing batch until
settlement. `LastCompactDuration` measures the most recent successful compaction
after acquiring the writer slot; it excludes time queued for that slot.

For large datasets, tune `CompactAfterBytes` to the workload. The default 8 MiB
threshold is intended for small stores; using it for a gigabyte-scale import
repeatedly writes the growing full snapshot. Batch ingestion into bounded
transactions and measure with the production sync mode. Startup sorts the final
sorted-index entries once; changing a sorted term during ordinary writes still
shifts a slice and costs O(records). Reads still decode independent copies.

Measure your own record and index shapes with `TestMemoryFootprint` and the
opt-in `TestCapacity` described in the [contributor guide](conventions.md).
Payload accounting is not a heap budget.

`Options.OnCommit` runs on the committing goroutine. Keep it short and do not
access the database from it; record metrics and return.

---

## The log as an audit trail

Each nonempty commit records a sequence number and timestamp. Add annotations
with `Tx.Note` and inspect them with `History`:

```go
err := db.Update(func(tx *datastore.Tx) error {
    tx.Note("actor", "user:7")
    tx.Note("reason", "refund")
    return datastore.In(tx, orders).Put(o)
})
```

`History` supplies annotations in `Meta` and record values as originally written:

```go
err := db.History(0, 0, func(e datastore.HistoryEntry) error {
    for _, op := range e.Ops {
        fmt.Printf("%s seq=%d %s/%s by=%s reason=%s\n",
            e.Time.Format(time.RFC3339), e.Seq,
            op.Collection, op.Key, e.Meta["actor"], e.Meta["reason"])
    }
    return nil
})
```

History availability depends on retention. By default compaction
discards the log, so only commits since the last compaction are visible. Set
either bound to keep segments instead:

```go
datastore.Options{
    HistorySegments: 20,                  // keep the newest 20 compacted segments
    HistoryFor:      30 * 24 * time.Hour, // prune older segments by file modification time
}
```

Compaction then renames `wal.log` aside as `archive-<seq>.log` rather than
emptying it. Archives are history only — their frames are already in the
snapshot, so recovery never replays them, and deleting one costs you history,
never state. `Stats` reports `ArchiveSegments` and `ArchiveBytes`.

A damaged archive returns `ErrCorruptFrame`. A torn tail on the live log ends
the scan. Retention age uses segment file modification time and is enforced
when pruning runs, not as an exact per-entry expiry. Handle the returned error.

---

## Watching for changes

```go
sub := db.Watch(datastore.WatchOptions{Collections: []string{"orders"}})
defer sub.Close()

for ev := range sub.Events() {
    if ev.Dropped > 0 {
        // we fell behind by ev.Dropped events: re-read what we care about
    }
    fmt.Println(ev.Seq, ev.Collection, ev.Key, ev.Delete)
}
```

Events are delivered in commit order after changes are applied to memory.
Under `SyncAlways`, the covering flush has also completed. Under `SyncInterval`
or `SyncNever`, event delivery is not evidence of power-loss durability. A
subsequent read sees the applied change or a later update to that record.
Each subscriber gets its own copy of `Value` and `Meta`.

Delivery does not wait for the subscriber to consume events. It is best-effort
into a bounded queue; when the queue fills, events are dropped. Inspect
`Event.Dropped` and `Subscription.Dropped()`. After a drop, reread the relevant
current state or rebuild from retained history if it covers the missing range.

`Watch` does not replay history. Subscribe first, replay retained history up to
the starting sequence, then consume the queue:

```go
sub := db.Watch(datastore.WatchOptions{}) // events start queueing here
defer sub.Close()

if start := sub.StartSeq(); start > 0 { // History's upper bound 0 means unbounded
    if err := db.History(0, start, func(e datastore.HistoryEntry) error {
        // Apply the historical entry to the consumer's state.
        return nil
    }); err != nil {
        return err
    }
}
for ev := range sub.Events() {
    if ev.Dropped > 0 {
        // Rebuild the consumer's state before continuing.
    }
    // Apply the live change.
}
```

Subscribing first divides history and live events at `StartSeq`. This works
only while the required history is retained and the subscription queue does
not overflow during replay. Choose `WatchOptions.Buffer` for the expected
backlog and detect drops. History scans disk files without pinning retention;
coordinate compaction and pruning if the consumer requires a complete audit
sequence. The [catch-up test](../catchup_test.go) exercises a bounded workload
with retained history and a queue large enough for all live events.

---

## Reading from a second process

Only one process may *own* a directory. A second one can still read it:

```go
db := datastore.New(datastore.Options{Dir: dir})
orders := datastore.Register[*Order](db, "orders")
if err := db.OpenReader(); err != nil { ... }
defer db.Close()
```

`OpenReader` takes no lock and never writes — not the log header, not a
compaction, not even the truncation of a damaged tail (damage is reported
through `Recovery` instead of repaired, because the log belongs to whoever holds
the lock). The view is a point in time; reopen to see later commits. Every write
returns `ErrReadOnly`.

Use this for a CLI, inspector or backup job that needs the loaded state while
the writer keeps running. It does not pin a history-retention boundary.

---

## Backup

`db.Backup(dst)` writes a self-contained snapshot of current state, including
counters, schema versions and unregistered collections. It does not copy the
live WAL or archived audit history. It acquires the writer slot, drains earlier
pending commits and streams a stable snapshot. Updates wait for backup I/O;
readers can proceed. The loaded view of an `OpenReader` remains fixed, so its
backup represents that loaded view.

From the application or a read-only inspection program, pass a new, empty
destination distinct from the active database directory and check the returned
error. Reusing an existing database directory can leave unrelated snapshots
beside the backup; `Backup` is not a directory-cleaning operation. Never point
it at the active directory; that destination is explicitly rejected, including
aliases identified by the filesystem as the same directory.

To validate or restore:

1. Copy the backup to a fresh scratch directory. With the intended application
   version and collection declarations, open that directory, inspect
   `Recovery()`, run `Verify()`, and check application-level records and counters.
   A writable open may migrate and compact the copy, so keep the original
   backup untouched. Check the close error.
2. Stop the application owning the destination database and confirm its close
   completed. Preserve its existing directory separately for rollback.
3. Copy the validated backup into a fresh destination, or configure
   `Options.Dir` to point at the restored directory. Do not merge its files with
   an existing database: a newer leftover snapshot or WAL can change recovery.
4. Start the application, inspect recovery, verify indexes and check the expected
   application state before resuming writes. To roll back, stop it again and
   restore the preserved directory together with a compatible application binary.

For an inspector using `OpenReader`, the backup reflects that reader's loaded
state. Reopen the reader if a later point in time is required.
