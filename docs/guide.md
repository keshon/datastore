# Guide

Everything past the [quickstart](../README.md#quickstart): the parts of the
package you reach for once it is holding real data.

For the on-disk format, the durability guarantees and how they are tested, see
[internals.md](internals.md). For the full API, see
[pkg.go.dev](https://pkg.go.dev/github.com/keshon/datastore).

## Contents

- [The transaction that justifies the package](#the-transaction-that-justifies-the-package)
- [Concurrency: what may be called from where](#concurrency-what-may-be-called-from-where)
- [Schema changes](#schema-changes)
- [Knowing when something went wrong](#knowing-when-something-went-wrong)
- [The log as an audit trail](#the-log-as-an-audit-trail)
- [Watching for changes](#watching-for-changes)
- [Reading from a second process](#reading-from-a-second-process)
- [Backup](#backup)

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

---

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

**`MaxBytes` counts encoded records, not memory.** `Stats.Bytes` sums
`len(key) + len(encoded record)` and nothing else — not the map holding the
records, not the slice header on each one, and not the indexes. Measured on a
four-index collection, actual heap is roughly:

```
heap ≈ payload + records × (90 + 145 × indexes)   bytes
```

So 100,000 records with four indexes cost about 67 MB of overhead on top of the
payload. Note the shape: it is a constant per record, not a multiplier, so the
ratio to `Stats.Bytes` looks alarming for tiny records (7.4× at 127 bytes) and
unremarkable for realistic ones (1.4× at 2 KB). Run `TestMemoryFootprint` to
measure it for your own record shape rather than trusting either number.

---

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

---

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

---

## Backup

`db.Backup(dst)` writes a self-contained directory you can point `Options.Dir`
at. Restoring is copying it back — there is no restore command because there is
nothing to do.

Test the restore on a scratch host before relying on it. An untested backup is a
hypothesis.
