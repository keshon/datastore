# datastore

[![CI](https://github.com/keshon/datastore/actions/workflows/ci.yml/badge.svg)](https://github.com/keshon/datastore/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/keshon/datastore.svg)](https://pkg.go.dev/github.com/keshon/datastore)
[![Go Report Card](https://goreportcard.com/badge/github.com/keshon/datastore)](https://goreportcard.com/report/github.com/keshon/datastore)

An embedded Go store for small datasets that must not be lost.

Everything lives in memory. Every commit is appended to a write-ahead log and
fsynced before it is acknowledged, so **if `Update` returned `nil`, the commit
survives the machine losing power** — not just the process dying. Collections
and indexes are typed, declared in Go, and rebuilt at startup.

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

---

## Is this the right tool?

**Use it when** your dataset fits comfortably in RAM, you need several records
across several collections to commit atomically, and losing an acknowledged
write is not acceptable. Order processing, a desktop app's state, a single-node
service's control plane, a CLI's local database.

**Use something else when** the data outgrows memory, you need SQL, more than
one process must *write*, or you need replication.

| | What you get | What you would still write yourself |
|---|---|---|
| **datastore** | Typed collections, indexes as closures, migrations, atomic multi-collection commits, audit trail, change feed | — |
| [bbolt](https://github.com/etcd-io/bbolt) | B+tree, ACID, larger than RAM | Serialization, every index, migrations, change feed |
| [buntdb](https://github.com/tidwall/buntdb) | In-memory + AOF, ACID, indexes | Typing, migrations, change feed, recovery reporting |
| **SQLite** | Everything, plus SQL | cgo (or a 1M-line transpile), and SQL↔struct mapping |
| [badger](https://github.com/dgraph-io/badger) | LSM, high write throughput, larger than RAM | Typing, indexes, a much heavier footprint |

The niche is narrow on purpose. Every property above — indexes rebuilt at
startup, backup by copying a directory, no buffer pool, no MVCC — is bought by
the assumption that the data fits in memory.

---

## Install

```bash
go get github.com/keshon/datastore
```

Requires Go 1.25 as declared in `go.mod`. The language floor is 1.23 (the
iterators use `iter.Seq`); lower the `go` directive if you need it.

## Quickstart

A record is an ordinary struct with a `Key()` method:

```go
type Order struct {
    ID       uint64    `json:"id"`
    Customer string    `json:"customer"`
    Email    string    `json:"email"`
    Total    int       `json:"total"`
    Created  time.Time `json:"created"`
}

func (o *Order) Key() string { return strconv.FormatUint(o.ID, 10) }
```

Opening is two-phase, because replaying the log has to know the collections and
indexes before it can rebuild them:

```go
db := datastore.New(datastore.Options{Dir: "data/main"})

// Declare everything BEFORE Open. Doing it after panics rather than
// quietly losing data.
orders := datastore.Register[*Order](db, "orders")

byCustomer := datastore.AddIndex(orders, "customer",  // many records per term
    func(o *Order) []string { return []string{o.Customer} })
byEmail := datastore.AddUnique(orders, "email",       // at most one per term
    func(o *Order) string { return o.Email })
byDate := datastore.AddSorted(orders, "date",         // ordered by an int64
    func(o *Order) int64 { return o.Created.Unix() })

if err := db.Open(); err != nil {
    return err
}
defer db.Close()

// Open succeeding is not proof nothing was lost: a damaged log tail is
// discarded so the store can start at all. Ask.
if rep := db.Recovery(); rep.LostData() {
    log.Printf("datastore lost committed work: %s", rep.TruncatedReason)
}
```

Write in a transaction. Everything in it commits together or not at all:

```go
err := db.Update(func(tx *datastore.Tx) error {
    o := datastore.In(tx, orders)
    c := datastore.In(tx, customers)

    cust, ok := c.Get(customerID)
    if !ok {
        return ErrNoSuchCustomer // rolls back, writes nothing
    }
    cust.Bonus -= spend
    if err := c.Put(cust); err != nil {
        return err
    }
    return o.Put(&Order{ID: tx.NextID("order")})
})
```

Read directly — no transaction needed, and every value is a fresh copy:

```go
o, ok := orders.Get("1")
list := byCustomer.Find("cust-7")
o, ok = byEmail.Get("buyer@example.com")
recent := byDate.Desc(10, 0)

for o := range orders.All() { ... }             // every record
for o := range byCustomer.All("cust-7") { ... } // lazy: break stops the work
```

**[`examples/orders`](examples/orders/main.go) is all of the above as one
runnable program**, compiled by CI so it cannot drift:

```bash
go run ./examples/orders
```

---

## What it guarantees

- **Power-loss durability.** Under `SyncAlways`, a commit is fsynced before
  `Update` returns. Tested against a simulated device where only `fsync` makes
  anything durable — not just against `SIGKILL`, which a store that never
  fsynced at all would survive.
- **Atomic multi-collection transactions.** One log frame, one fsync.
- **Nothing visible before it is durable.** A reader can never see a commit that
  a power cut would take back.
- **Corruption is detected, not absorbed.** Every log frame and every snapshot
  carries a crc32c. A damaged snapshot falls back to the previous one and says
  so through `Recovery()`.
- **Group commit.** Concurrent commits share one fsync, without weakening any of
  the above.
- **Indexes are derived, never stored.** `Verify()` rebuilds them from the
  records and reports any drift.

See [docs/internals.md](docs/internals.md) for how each is implemented and
tested.

## What it costs

Measured by this repo's benchmarks — run `go test -bench .` for your own
hardware.

| | |
|---|---|
| Commit, `SyncAlways` | fsync-bound; the fsync is ~97% of it. Concurrent writers share one |
| Commit, `SyncNever` | ~30 µs |
| `Get` | ~2 µs — every read decodes, which is what makes results fresh copies |
| Full scan | ~2 µs/record: microseconds for hundreds, ~20 ms for 10,000 |
| `Open` | ~10 µs/record — 10,000 records is ~100 ms of startup |
| Memory | payload + `records × (90 + 145 × indexes)` bytes |

That memory figure is a **constant per record, not a multiplier** — which is why
the ratio to `Stats().Bytes` looks alarming for tiny records (7.4× at 127 bytes)
and unremarkable for realistic ones (1.4× at 2 KB). 100,000 records with four
indexes is about 67 MB of overhead. `Options.MaxBytes` is compared against
payload only; run `TestMemoryFootprint` to measure your own record shape.

## What it does not do

No query language, no joins, no MVCC, no replication, and no multi-process
*writing*. Declare an index or write a Go loop.

Only one process may have a directory open for writing — the lock is enforced at
`Open` — which is why an admin interface has to live in the same process as the
application. A second process can still
[read](docs/guide.md#reading-from-a-second-process).

Compaction marshals the whole dataset before writing it, so peak memory during a
snapshot is roughly twice the data.

---

## API

### Database

| | |
|---|---|
| `New(Options) *DB` | Describe a database. No I/O. |
| `(*DB) Open() / OpenReader() error` | Take the lock and recover / open read-only. |
| `(*DB) Close() error` | Stop workers, compact, release the lock. |
| `(*DB) Update(func(*Tx) error) error` | Read-write transaction. Commits if it returns nil. |
| `(*DB) View(func(*Tx) error) error` | Read-only transaction. Holds the read lock throughout. |
| `(*DB) Recovery() RecoveryReport` | What the last `Open` found — and what it could not use. |
| `(*DB) Verify() error` | Rebuild indexes from records and report drift. |
| `(*DB) Compact() error` | Fold the log into a fresh snapshot. |
| `(*DB) Backup(dst string) error` | Write a self-contained copy. |
| `(*DB) Watch(WatchOptions) *Subscription` | Change feed. Never blocks a commit. |
| `(*DB) History(from, to, fn) error` | Read the log as an audit trail. |
| `(*DB) Stats() Stats` | Counts, bytes, sequence, archive size. |

### Collections and indexes

| | |
|---|---|
| `Register[T](db, name, ...CollectionOption) *Collection[T]` | Declare a collection. Before `Open`. |
| `SchemaVersion(int)` / `Migrate[T](c, from, fn)` | Declare and migrate a schema version. |
| `AddIndex[T](c, name, func(T) []string)` | Multi-valued: `Find`, `All`, `Count`, `Terms`. |
| `AddUnique[T](c, name, func(T) string)` | At most one record per term: `Get`. A `""` term opts out. |
| `AddSorted[T](c, name, func(T) int64)` | Ordered: `Range`, `All`, `Asc`, `Desc`, `Len`. |
| `(*Collection[T]) Get / Put / Delete / All / Keys / Len` | Direct access, outside a transaction. |
| `In` / `InIndex` / `InUnique` / `InSorted` | Scope a collection or index to a transaction. |

> **One rule worth knowing.** Inside `Update` or `View`, read through
> `In(tx, orders)` — not `orders.Get`. `View` holds the read lock for its whole
> callback and the direct methods take it again; `sync.RWMutex` is not
> reentrant, so that deadlocks once a writer is queued. See
> [docs/guide.md](docs/guide.md#concurrency-what-may-be-called-from-where).

### `Find` versus `All`

Every read decodes, because every result is a fresh copy. `Find` decodes the
whole result set before returning any of it; `All` decodes one record per step:

```go
for o := range byStatus.All("new") {
    if o.Total > limit {
        return o, nil // the rest is never decoded
    }
}
```

Over 10,000 matches, taking the first ten costs 21 ms and 100,000 allocations
through `Find`, and 1.4 ms and 99 allocations through `All`. Iterating the whole
set costs the same either way — the win is entirely in stopping early.

### Key options

| | Default | |
|---|---|---|
| `Dir` | required | The directory this database owns. |
| `Sync` | `SyncAlways` | `SyncAlways`, `SyncInterval`, or `SyncNever`. |
| `MaxBytes` | off | Encoded size to warn past — payload, not heap. |
| `HistorySegments` / `HistoryFor` | off | Retain compacted log segments for `History`. |
| `CompactAfterBytes` | 8 MiB | Log size that triggers compaction. |
| `OnCommit` | nil | Per-commit stats, for metrics. |

Full reference on [pkg.go.dev](https://pkg.go.dev/github.com/keshon/datastore).

---

## Documentation

- **[docs/guide.md](docs/guide.md)** — transactions, concurrency, schema
  migrations, recovery reporting, the audit trail, the change feed, read-only
  opens, backup.
- **[docs/internals.md](docs/internals.md)** — on-disk format, durability
  guarantees per sync mode, group commit, and the test suite that holds them.
- **[examples/orders](examples/orders/main.go)** — a runnable program.

## Tests

```bash
go test ./... -race
```

```bash
DS_FULL_SWEEP=1 go test ./...
```

The first takes about 30 seconds, the exhaustive recovery sweep about three
minutes. The crash and power-loss suites are the reason to trust any of this;
see [docs/internals.md](docs/internals.md#tests) before weakening one to make a
change land.

## License

MIT — see [LICENSE](LICENSE).
