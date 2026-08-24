# datastore

[![CI](https://github.com/keshon/datastore/actions/workflows/ci.yml/badge.svg)](https://github.com/keshon/datastore/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/keshon/datastore.svg)](https://pkg.go.dev/github.com/keshon/datastore)
[![Go Report Card](https://goreportcard.com/badge/github.com/keshon/datastore)](https://goreportcard.com/report/github.com/keshon/datastore)

An embedded store for data that must not be lost. Everything lives in memory;
every commit is appended to a write-ahead log and fsynced before it is
acknowledged.

```go
db := datastore.New(datastore.Options{Dir: "data/main"})
orders := datastore.Register[*Order](db, "orders")
if err := db.Open(); err != nil {
    return err
}
defer db.Close()

// One frame, one fsync, all-or-nothing.
err := db.Update(func(tx *datastore.Tx) error {
    return datastore.In(tx, orders).Put(&Order{ID: tx.NextID("order")})
})

o, ok := orders.Get("1") // a fresh copy, no transaction needed
```

That is the whole shape of it: describe the database, declare the collections,
open, write in transactions, read directly.

---

## Contents

- [Installation](#installation)
- [The full tour](#the-full-tour)
- [The transaction that justifies the package](#the-transaction-that-justifies-the-package)
- [Concurrency: what may be called from where](#concurrency-what-may-be-called-from-where)
- [Schema changes](#schema-changes)
- [Knowing when something went wrong](#knowing-when-something-went-wrong)
- [The log as an audit trail](#the-log-as-an-audit-trail)
- [Watching for changes](#watching-for-changes)
- [Reading from a second process](#reading-from-a-second-process)
- [What it does not do](#what-it-does-not-do)
- [API](#api)
- [On disk](#on-disk)
- [Durability, per database](#durability-per-database)
- [Backup](#backup)
- [Tests](#tests)

---

## Installation

```bash
go get github.com/keshon/datastore
```

Requires Go 1.25 or newer, as declared in `go.mod`. The language floor is 1.23
(the iterator API uses `iter.Seq`); lower the `go` directive if you need it.

---

## The full tour

Everything the package does, in one program:

```go
package main

import (
    "fmt"
    "log"
    "strconv"
    "time"

    "github.com/keshon/datastore"
)

// A record is an ordinary struct. The only requirement is a Key() method
// returning something unique and stable: it is what addresses the record in the
// log, so it must not change during the record's life.
type Order struct {
    ID         uint64    `json:"id"`
    Num        string    `json:"num"`
    CustomerID string    `json:"customer_id"`
    Total      int       `json:"total"`
    CreatedAt  time.Time `json:"created_at"`
}

func (o *Order) Key() string { return strconv.FormatUint(o.ID, 10) }

func main() {
    // 1. Describe the database. This touches no disk at all.
    db := datastore.New(datastore.Options{
        Dir:      "data/main", // the directory this database owns
        MaxBytes: 64 << 20,    // warn once the dataset outgrows 64 MiB of RAM
    })

    // 2. Declare the collections and indexes BEFORE opening — replaying the log
    //    has to know where records belong and which indexes to rebuild. Calling
    //    any of these after Open panics rather than quietly losing data.
    orders := datastore.Register[*Order](db, "orders", datastore.SchemaVersion(1))

    // Three kinds of index. All are rebuilt at Open, never stored on disk.
    byCustomer := datastore.AddIndex(orders, "customer", // many records per term
        func(o *Order) []string { return []string{o.CustomerID} })
    byNumber := datastore.AddUnique(orders, "num", // at most one record per term
        func(o *Order) string { return o.Num })
    byDate := datastore.AddSorted(orders, "date", // ordered by an int64
        func(o *Order) int64 { return o.CreatedAt.Unix() })

    // 3. Open: take the directory lock, load the newest snapshot, replay the log
    //    on top of it, run any migrations, rebuild the indexes.
    if err := db.Open(); err != nil {
        log.Fatal(err) // ErrLocked if another process already owns this directory
    }
    defer db.Close() // stops workers, compacts, releases the lock

    // Open succeeding does not prove nothing was lost: a damaged log tail is
    // discarded so the store can start at all. Ask, and complain if it happened.
    if rep := db.Recovery(); rep.LostData() {
        log.Printf("datastore lost committed work: %s", rep.TruncatedReason)
    }

    // 4. Write inside a transaction: one log frame, one fsync, all-or-nothing.
    //    Returning an error rolls everything back and writes nothing.
    err := db.Update(func(tx *datastore.Tx) error {
        o := datastore.In(tx, orders) // scope the collection to this transaction

        // Counters are reserved here and only become permanent on commit.
        period, seq := tx.NextInPeriod("order", time.Now(), "0601") // "2607", 1

        return o.Put(&Order{
            ID:         tx.NextID("order"),
            Num:        fmt.Sprintf("%s/%d", period, seq), // "2607/1"
            CustomerID: "cust-7",
            Total:      4200,
            CreatedAt:  time.Now(),
        })
    })
    if err != nil {
        log.Fatal(err)
    }

    // 5. Read. No transaction needed, and everything you get back is a fresh
    //    copy — changing it affects nothing until you Put it back.
    if o, ok := orders.Get("1"); ok {
        fmt.Println("order 1 belongs to", o.CustomerID)
    }

    // Query through the indexes.
    fmt.Println("orders for cust-7:", len(byCustomer.Find("cust-7")))
    if o, ok := byNumber.Get("2607/1"); ok {
        fmt.Println("found by number:", o.ID)
    }
    for _, o := range byDate.Desc(10, 0) { // the ten most recent
        fmt.Println(o.Num, o.Total)
    }

    // Or just loop: at these volumes a linear scan is microseconds.
    total := 0
    for o := range orders.All() {
        total += o.Total
    }
    fmt.Printf("%d orders, %d total\n", orders.Len(), total)
}
```

For a single write there is a shorthand — `orders.Put(o)` and
`orders.Delete(key)` each run as a one-operation transaction. Do not call them
from *inside* `db.Update`: each one starts its own transaction, and there is only
one writer. (Doing it anyway panics immediately and tells you to use
`datastore.In(tx, orders)`.)

---

## The transaction that justifies the package

Placing an order writes the order, debits a loyalty balance, consumes a promo
code and queues a confirmation email. Either all of that happened or none of it
did:

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

One log frame, one fsync. Returning an error from the function rolls everything
back and writes nothing.

Read *inside* the transaction, through `In(tx, c)`, when the write depends on
what you read. `Get` outside `Update` followed by `Put` inside it is a
lost-update waiting to happen: the writer lock serialises the writes, not your
read-then-write pair.

Queue outbound email *inside* the transaction as a record, and send it from a
worker outside. Then a crash after commit still sends the mail, and an SMTP
failure never rolls back an order.

Do not call `Update` from inside `Update`. It can never proceed — the slot it
waits for is the one it already holds — so it is detected and panics immediately
with an explanation rather than deadlocking.

---

## Concurrency: what may be called from where

One writer at a time; readers never block each other. `Update` runs your
function *without* holding the state lock, so a transaction's body and its fsync
never block readers — only the brief moment where an already-durable commit is
applied to memory.

That leaves one rule worth knowing, because getting it wrong hangs rather than
errors:

| Where you are | Read with | Not with |
|---|---|---|
| Outside any transaction | `orders.Get`, `byTag.Find`, `orders.All()` | — |
| Inside `Update` or `View` | `In(tx, orders)`, `InIndex(tx, byTag)`, `InUnique(tx, byEmail)`, `InSorted(tx, byDate)` | `orders.Get`, `byTag.Find` — the direct methods |

`View` holds the read lock for its whole callback, and the direct methods take
that lock again. Go's `sync.RWMutex` is not reentrant: a second `RLock` with a
writer queued in between deadlocks. The `In*` wrappers know the lock is already
held, which is what makes them safe in both contexts — use them and the
distinction never bites you.

Two smaller ones:

- `Collection.All()` holds the read lock for the whole iteration, so its body
  must not write. Collect keys first, then write after the loop.
- Index reads inside a transaction see *committed* state. A record staged
  earlier in the same transaction is not indexed until commit; read it back
  through `In(tx, c).Get`.

---

## Schema changes

Adding a field needs nothing: JSON decoding fills it with the zero value.
Migrations are for the changes decoding cannot guess — a renamed field, a
changed unit, a struct split in two — where the old records are still valid JSON
but no longer mean what the new code thinks.

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

A migration takes raw JSON and returns raw JSON, because the Go type it has to
read no longer exists in this build. That is unpleasant to write and it is the
honest signature; the alternative is a museum of old structs compiled in
forever.

At `Open`, records below the declared version are carried up one step at a time
— snapshot rows and log frames alike — and the database is compacted before
`Open` returns, so the log never holds a mix of versions and the work is not
repeated on the next start. Opening data written by a *newer* build fails
instead of guessing.

## Knowing when something went wrong

Recovery will discard a damaged log tail in order to open at all. That is the
right trade, but it means a successful `Open` is not proof that nothing was
lost. Ask:

```go
if rep := db.Recovery(); rep.LostData() {
    log.Error().
        Str("reason", rep.TruncatedReason).
        Int64("at", rep.TruncatedAt).
        Msg("datastore lost committed work")
}
```

`LostData()` is the question to page someone about. `Clean()` is stricter —
nothing skipped, truncated, *or migrated* — so it is false after a routine
schema bump; treat it as "the disk was exactly what this build expected", not as
an alarm.

`db.Verify()` rebuilds every index from the records and reports disagreements.
The indexes are derived state maintained incrementally, and nothing else ever
checks them against their source; run this in CI or behind an admin command.
`dstest.Open` runs it automatically when a test finishes.

If a log write ever fails, the store latches that failure and every later write
returns `ErrFailed`. The log may hold a torn frame at that point, and anything
written after it would be discarded by the next replay — so refusing is what
keeps "`Update` returned nil" meaning "this survives a crash". Reopen to
recover whatever the log still holds.

Set `Options.MaxBytes` to the size the dataset is expected to stay under. It
enforces nothing — it logs when you cross the threshold and reports `Bytes` and
`Budget` in `Stats`, which turns "OOM one morning" into a line in a log months
earlier. `Options.OnCommit` receives a summary of every commit for metrics.

## The log as an audit trail

Every commit already records a sequence number and a timestamp. Attach context
to it and you have an audit trail from data the store was writing anyway:

```go
db.Update(func(tx *datastore.Tx) error {
    tx.Note("actor", "user:7")
    tx.Note("reason", "refund")
    return datastore.In(tx, orders).Put(o)
})
```

Read it back with `History`, which reports each commit's annotations — the
`Meta` map is exactly what `Note` put there — *and the values as they were
written*, something the current state cannot tell you:

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

**How far back it reaches is a retention choice.** By default compaction
discards the log, so only commits since the last compaction are visible. Set
either bound to keep segments instead:

```go
datastore.Options{
    HistorySegments: 20,                  // keep the newest 20 compacted segments
    HistoryFor:      30 * 24 * time.Hour, // ...and nothing older than 30 days
}
```

Compaction then renames `wal.log` aside as `archive-<seq>.log` rather than
emptying it. Archives are history only — their frames are already in the
snapshot, so recovery never replays them, and deleting one costs you history,
never state. `Stats` reports `ArchiveSegments` and `ArchiveBytes`.

A damaged archive returns `ErrCorruptFrame` rather than reading as a shorter
history: an audit trail with an invisible hole is worse than an error. A torn
tail on the *live* log is a normal crash artifact and simply ends the scan.

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

Events are delivered after the change is both durable and applied, so a
subscriber that reads the database on receipt sees it, and they arrive in commit
order. Each subscriber gets its own copy of `Value` and `Meta`.

**A subscriber can never slow a commit down.** Delivery is best-effort into a
bounded queue; a subscriber that stops reading has events dropped and is told how
many, rather than blocking an fsync. A dropped event is a nuisance, a blocked
write is an outage.

`Watch` does not replay the past — but it records the sequence it started at, so
catching up is a composition rather than a feature:

```go
sub := db.Watch(datastore.WatchOptions{}) // events start queueing here
defer sub.Close()

db.History(0, sub.StartSeq(), func(e datastore.HistoryEntry) error { ... }) // the past
for ev := range sub.Events() { ... }                                        // then live
```

Subscribing *first* is what closes the gap: everything up to `StartSeq` comes
from `History`, everything after it from the feed, with nothing missed and
nothing delivered twice.

## Reading from a second process

Only one process may *own* a directory. A second one can still read it:

```go
db := datastore.New(datastore.Options{Dir: dir})
orders := datastore.Register[*Order](db, "orders")
if err := db.OpenReader(); err != nil { ... }
```

`OpenReader` takes no lock and never writes — not the log header, not a
compaction, not even the truncation of a damaged tail (damage is reported
through `Recovery` instead of repaired, because the log belongs to whoever holds
the lock). The view is a point in time; reopen to see later commits. Every write
returns `ErrReadOnly`.

That is what a CLI, an inspector or a backup job should use while the
application keeps running.

## What it does not do

No query language, no joins, no key range scans, no MVCC, no replication, and no
multi-process *writing*. Declare an index or write a Go loop — at these volumes a
linear scan is measured in microseconds.

It assumes **the whole dataset fits comfortably in RAM**. That assumption is what
justifies deleting most of what a database normally does. When it stops holding,
export: the on-disk format is plain JSON precisely so that stays easy. Note that
compaction marshals the whole dataset before writing it, so peak memory during a
snapshot is roughly twice the data.

Only one process may have a directory open. The lock is advisory but enforced at
`Open`, which is why an admin interface has to live in the same process as the
application rather than beside it.

---

## API

### Database

| Function | Description |
|---|---|
| `New(opts Options) *DB` | Prepares a database. Performs no I/O. |
| `(*DB) Open() error` | Takes the directory lock, restores state, migrates, starts background work. |
| `(*DB) OpenReader() error` | Opens read-only without the lock, for a second process. |
| `(*DB) Close() error` | Stops workers, compacts, releases the lock. Safe to call twice. |
| `(*DB) Update(fn func(*Tx) error) error` | Read-write transaction. Commits if `fn` returns nil. |
| `(*DB) View(fn func(*Tx) error) error` | Read-only transaction; the read lock is held throughout. |
| `(*DB) Compact() error` | Folds the log into a fresh snapshot and empties it. |
| `(*DB) Backup(dst string) error` | Writes a self-contained copy into `dst`. |
| `(*DB) Stats() Stats` | Point-in-time summary for an admin screen or health check. |
| `(*DB) Recovery() RecoveryReport` | What the last `Open` found — and what it could not use. |
| `(*DB) Verify() error` | Rebuilds every index from the records and reports disagreements. |
| `(*DB) History(from, to, fn) error` | Replays past commits, with annotations and the values as written. `to` of 0 means no upper bound. |
| `(*DB) Watch(WatchOptions) *Subscription` | Live change feed; never blocks a commit. |
| `(*DB) ReadOnly() bool` / `(*DB) Dir() string` | How it was opened, and where. |

### Collections and transactions

| Function | Description |
|---|---|
| `Register[T Entity](db, name, opts...) *Collection[T]` | Declares a collection. Before `Open`; panics after. Pass `SchemaVersion(n)` to version its schema. |
| `Migrate[T](c, from, fn)` | Declares the step carrying records from version `from` to `from+1`. |
| `(*Collection[T]) Get(key) (T, bool)` | A freshly decoded copy, never a shared pointer. |
| `(*Collection[T]) Put(v T) error` | Single-operation transaction. |
| `(*Collection[T]) Delete(key) error` | Single-operation transaction. Absent key is not an error. |
| `(*Collection[T]) All() iter.Seq[T]` | Iterates in key order under the read lock; the body must not write. |
| `(*Collection[T]) Keys() []string` / `Len() int` | Sorted key snapshot; record count. |
| `In[T](tx, c) *TxCollection[T]` | Scopes a collection to a transaction. |
| `(*TxCollection[T]) Get / Put / Delete` | Reads see this transaction's own uncommitted writes. |
| `(*TxCollection[T]) Keys / Len / All` | The collection as this transaction sees it, staged writes included. |
| `(*Tx) Note(key, value string)` | Attaches context to the commit, readable later as `Meta` through `History` and `Watch`. |
| `(*Tx) NextID(name string) uint64` | Monotonic counter, permanent only on commit. |
| `(*Tx) NextInPeriod(name, t, layout) (string, uint64)` | A counter that restarts each period — `"0601"` monthly, `"2006"` yearly. |

### Indexes

| Function | Description |
|---|---|
| `AddIndex[T](c, name, func(T) []string) *Index[T]` | Multi-valued. `Find(term) []T`, `Count(term) int`, `Terms() []string`. |
| `AddUnique[T](c, name, func(T) string) *UniqueIndex[T]` | At most one record per term. `Get(term) (T, bool)`. A `""` term opts the record out. |
| `AddSorted[T](c, name, func(T) int64) *SortedIndex[T]` | Ordered by an int64. `Range(lo, hi)`, `Asc(limit, offset)`, `Desc(limit, offset)`, `Len()`. A limit of 0 means no limit. |
| `InIndex` / `InUnique` / `InSorted` | Scope an index to a transaction. Required inside `View`; see [Concurrency](#concurrency-what-may-be-called-from-where). |

All three must be declared before `Open`.

### Options

| Field | Default | Description |
|---|---|---|
| `Dir` | — | Required. Directory the database owns. |
| `Sync` | `SyncAlways` | See [Durability](#durability-per-database). |
| `SyncEvery` | `5s` | Flush period under `SyncInterval`. |
| `CompactAfterBytes` | `8 MiB` | Compact once the log passes this size. |
| `CompactInterval` | `15m` | Periodic compaction. `CompactNever` disables the timer. |
| `KeepSnapshots` | `2` | Snapshots retained. Minimum 2 — the previous one is what makes a crash during installation survivable. |
| `Logger` | no-op | `*zerolog.Logger` receiving recovery and compaction events. |
| `Clock` | `time.Now` | Supplies commit timestamps. |
| `WriteTimeout` | `30s` | How long `Update` waits for the writer slot before returning `ErrWriteTimeout`. |
| `MaxBytes` | `0` (off) | Size the dataset is expected to stay under. Warns; enforces nothing. |
| `WarnAtPercent` | `80` | Fraction of `MaxBytes` that triggers the warning. |
| `OnCommit` | none | Called with a `CommitStats` after each commit, for metrics. |
| `HistorySegments` | `0` (off) | Compacted log segments to retain for `History`. |
| `HistoryFor` | `0` (off) | Maximum age of a retained segment. |

### Errors

| Error | Meaning |
|---|---|
| `ErrNotOpen` | Used before `Open` or after `Close`. |
| `ErrAlreadyOpen` | `Open` called twice. |
| `ErrLocked` | Another process holds the directory lock. |
| `ErrInvalidLog` | `wal.log` in the directory is not a datastore log — wrong `Options.Dir`. |
| `ErrLogVersion` | The log was written by a newer datastore build. |
| `ErrSnapshotVersion` | The snapshot was written by a newer datastore build. Unlike a damaged snapshot this is not fallen back from — an older snapshot would be stale, not merely older. |
| `ErrUniqueViolation` | Unique index violation. Surfaces from `Update`, not from `Put`. |
| `ErrReadOnly` | A write was attempted inside `View`, or against an `OpenReader` database. |
| `ErrTxDone` | Transaction used after it finished. |
| `ErrEmptyKey` | `Key()` returned `""`. Keys address records in the log, so an empty one is unrecoverable rather than merely odd. |
| `ErrWriteTimeout` | The writer slot did not free up within `WriteTimeout`. |
| `ErrFailed` | A log write failed; no further writes are accepted. Reopen to recover. |
| `ErrCorruptFrame` | A frame in an archived segment failed its checksum or length check, reported by `History`. |

`ErrDuplicateKey` and `Version` remain as deprecated aliases for
`ErrUniqueViolation` and `SchemaVersion`.

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

## Backup

`db.Backup(dst)` writes a self-contained directory you can point `Options.Dir`
at. Restoring is copying it back — there is no restore command because there is
nothing to do.

Test the restore on a scratch host before relying on it. An untested backup is a
hypothesis.

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

---

## License

MIT — see [LICENSE](LICENSE).
