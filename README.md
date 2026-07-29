# datastore

An embedded store for data that must not be lost. Everything lives in memory;
every commit is appended to a write-ahead log and fsynced before it is
acknowledged.

Version 2 is a rewrite. If you are looking for the v1 key-value store, see
[Relationship to v1](#relationship-to-v1) at the bottom.

---

## Installation

```bash
go get github.com/keshon/datastore
```

Requires Go 1.25 or newer, as declared in `go.mod`. The language floor is 1.23
(the iterator API uses `iter.Seq`); lower the `go` directive if you need it.

---

## Usage

```go
db := datastore.New(datastore.Options{Dir: "data/main"})

orders := datastore.Register[*Order](db, "orders")
byCustomer := datastore.AddIndex(orders, "customer",
    func(o *Order) []string { return []string{o.CustomerID} })
byNumber := datastore.AddUnique(orders, "num",
    func(o *Order) string { return o.Num })
byDate := datastore.AddSorted(orders, "date",
    func(o *Order) int64 { return o.CreatedAt.Unix() })

if err := db.Open(); err != nil {
    return err
}
defer db.Close()
```

Records are any type with a `Key() string` method:

```go
type Order struct {
    ID         uint64
    Num        string
    CustomerID string
    CreatedAt  time.Time
}

func (o *Order) Key() string { return strconv.FormatUint(o.ID, 10) }
```

Opening is two-phase: replaying the log needs to know the collections and
indexes before it can rebuild them, so `Register` and `Add*Index` must all
happen before `Open`. Calling them afterwards panics rather than silently
losing data.

---

## The transaction that justifies the package

Placing an order writes the order, debits a loyalty balance, consumes a promo
code and queues a confirmation email. Either all of that happened or none of it
did:

```go
err := db.Update(func(tx *datastore.Tx) error {
    o := datastore.In(tx, orders)
    c := datastore.In(tx, customers)

    cust, _ := c.Get(customerID)
    if cust.Bonus < spend {
        return ErrInsufficientBonus
    }
    cust.Bonus -= spend
    if err := c.Put(cust); err != nil {
        return err
    }
    return o.Put(&Order{ID: tx.NextID("order"), Num: tx.NextOrderNum(now)})
})
```

One log frame, one fsync. Returning an error from the function rolls everything
back and writes nothing.

Queue outbound email *inside* the transaction as a record, and send it from a
worker outside. Then a crash after commit still sends the mail, and an SMTP
failure never rolls back an order.

Do not call `Update` from inside `Update`. There is no reliable way to detect
that in Go, so instead of hanging forever the writer slot times out and panics
with an explanation.

---

## What it does not do

No query language, no joins, no key range scans, no MVCC, no replication, no
multi-process access, no automatic schema migration. Declare an index or write a
Go loop — at these volumes a linear scan is measured in microseconds.

It assumes **the whole dataset fits comfortably in RAM**. That assumption is what
justifies deleting most of what a database normally does. When it stops holding,
export: the on-disk format is plain JSON precisely so that stays easy.

Only one process may have a directory open. The lock is advisory but enforced at
`Open`, which is why an admin interface has to live in the same process as the
application rather than beside it.

---

## API

### Database

| Function | Description |
|---|---|
| `New(opts Options) *DB` | Prepares a database. Performs no I/O. |
| `(*DB) Open() error` | Takes the directory lock, restores state, starts background work. |
| `(*DB) Close() error` | Stops workers, compacts, releases the lock. Safe to call twice. |
| `(*DB) Update(fn func(*Tx) error) error` | Read-write transaction. Commits if `fn` returns nil. |
| `(*DB) View(fn func(*Tx) error) error` | Read-only transaction; the read lock is held throughout. |
| `(*DB) Compact() error` | Folds the log into a fresh snapshot and empties it. |
| `(*DB) Backup(dst string) error` | Writes a self-contained copy into `dst`. |
| `(*DB) Stats() Stats` | Point-in-time summary for an admin screen or health check. |

### Collections and transactions

| Function | Description |
|---|---|
| `Register[T Entity](db, name) *Collection[T]` | Declares a collection. Before `Open`; panics after. |
| `(*Collection[T]) Get(key) (T, bool)` | A freshly decoded copy, never a shared pointer. |
| `(*Collection[T]) Put(v T) error` | Single-operation transaction. |
| `(*Collection[T]) Delete(key) error` | Single-operation transaction. Absent key is not an error. |
| `(*Collection[T]) All() iter.Seq[T]` | Iterates in key order under the read lock; the body must not write. |
| `(*Collection[T]) Keys() []string` / `Len() int` | Sorted key snapshot; record count. |
| `In[T](tx, c) *TxCollection[T]` | Scopes a collection to a transaction. |
| `(*TxCollection[T]) Get / Put / Delete` | Reads see this transaction's own uncommitted writes. |
| `(*Tx) NextID(name string) uint64` | Monotonic counter, permanent only on commit. |
| `(*Tx) NextOrderNum(t time.Time) string` | `NextID` formatted as `YYMM/n`, restarting each month. |

### Indexes

| Function | Description |
|---|---|
| `AddIndex[T](c, name, func(T) []string) *Index[T]` | Multi-valued. `Get(term) []T`, `Count(term) int`, `Terms() []string`. |
| `AddUnique[T](c, name, func(T) string) *UniqueIndex[T]` | At most one record per term. `Get(term) (T, bool)`. A `""` term opts the record out. |
| `AddSorted[T](c, name, func(T) int64) *SortedIndex[T]` | Ordered by an int64. `Range(lo, hi)`, `Asc(limit, offset)`, `Desc(limit, offset)`, `Len()`. |

All three must be declared before `Open`.

### Options

| Field | Default | Description |
|---|---|---|
| `Dir` | — | Required. Directory the database owns. |
| `Sync` | `SyncAlways` | See [Durability](#durability-per-database). |
| `SyncEvery` | `5s` | Flush period under `SyncInterval`. |
| `CompactAfterBytes` | `8 MiB` | Compact once the log passes this size. |
| `CompactInterval` | `15m` | Periodic compaction. Negative disables the timer. |
| `KeepSnapshots` | `2` | Snapshots retained. Minimum 2 — the previous one is what makes a crash during installation survivable. |
| `Logger` | no-op | `*zerolog.Logger` receiving recovery and compaction events. |
| `Clock` | `time.Now` | Supplies commit timestamps. |
| `WriteTimeout` | `30s` | How long `Update` waits for the writer slot before panicking. |

### Errors

| Error | Meaning |
|---|---|
| `ErrNotOpen` | Used before `Open` or after `Close`. |
| `ErrAlreadyOpen` | `Open` called twice. |
| `ErrLocked` | Another process holds the directory lock. |
| `ErrDuplicateKey` | Unique index violation. Surfaces from `Update`, not from `Put`. |
| `ErrNotFound` | Missing key where one was required. |
| `ErrReadOnly` | A write was attempted inside `View`. |
| `ErrTxDone` | Transaction used after it finished. |
| `ErrEmptyKey` | `Key()` returned `""`. Keys address records in the log, so an empty one is unrecoverable rather than merely odd. |
| `ErrCorruptFrame` | A log frame failed its checksum or length check. |

---

## On disk

```
dir/
  LOCK                  exclusive, held for the lifetime of the process
  snapshot-000412.json  full state as of sequence 412
  wal.log               framed, append-only, everything after that
```

A frame is `u32 length | u32 crc32c | JSON payload`. Recovery loads the newest
snapshot that parses and replays the log on top, stopping at the first frame
that is short, fails its checksum, or arrives out of sequence — and truncating
there. Stopping is the point: once one decision in the log is unreadable, the
ones after it cannot be trusted to apply to the state we think we have.

Compaction writes a new snapshot, fsyncs it, and only then truncates the log.
A crash between those two steps leaves a snapshot plus a log that replays
cleanly on top of it: wasteful, never wrong. The previous snapshot is kept for
the same reason.

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

The first takes about 30 seconds; the exhaustive sweep about three minutes and
belongs in CI.

The crash tests are the reason to trust any of this:

- **`TestRecoveryFromTruncatedLog`** cuts the log short at many offsets —
  concentrated at frame boundaries, every single byte under `DS_FULL_SWEEP=1` —
  and at each one asserts that the recovered state equals *exactly* the prefix
  of commits the store claims it applied, that the count never decreases as the
  cut moves later, and that every index matches a brute-force recomputation.
- **`TestCrashInjection`** runs a child process committing in a loop, kills it
  with a signal it cannot handle, and verifies that every commit the child had
  acknowledged is still there.
- **`FuzzFrameParsing`** feeds garbage to the frame reader.

Do not weaken these to make a change land. Without them this is a JSON file with
extra steps.

---

## Relationship to v1

This is a rewrite that keeps v1's good idea — atomic rename plus fsync — and
adds the log, transactions, typed collections and indexes. The v1 API
(`New(ctx, path, opts...)`, `Set`/`Get`/`Delete` over a single JSON file) is
gone; there is no compatibility shim and no automatic conversion. To migrate,
read the old file and write the values into a registered collection.

Two v1 behaviours are deliberately not carried over: it installed a process-wide
signal handler and called `os.Exit` from library code, and its file locking
compiled on neither platform (the Unix build referenced a field that did not
exist; the Windows build returned an error unconditionally, so the default
configuration always failed to open).

---

## License

MIT — see [LICENSE](LICENSE).
