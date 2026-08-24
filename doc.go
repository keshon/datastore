// Package datastore is a small embedded store for data that must not be lost.
//
// It holds everything in memory and appends every commit to a write-ahead log
// before acknowledging it. That combination is only reasonable because of a
// specific assumption: the whole dataset fits comfortably in RAM — a few
// hundred kilobytes today, perhaps twenty megabytes in a decade. Set
// Options.MaxBytes and the store will tell you long before that assumption
// stops holding; note that it counts encoded records rather than heap, and see
// its documentation for the difference. When the assumption does stop holding,
// export: the on-disk format is plain JSON precisely so that stays easy.
//
// # What it gives you
//
//   - Power-loss durability. Under SyncAlways a commit is on disk and fsynced
//     before Update returns, so an acknowledged commit survives the machine
//     stopping, not merely the process. See powerloss_test.go, which runs the
//     log on a device where only fsync makes anything durable.
//   - Group commit. Concurrent commits share one fsync rather than queueing for
//     their own, without weakening any of the above: a frame is applied to
//     memory, and so becomes visible, only after the fsync that covers it.
//   - Real transactions. Several records across several collections commit as
//     one atomic unit, which is what placing an order actually requires:
//     write the order, debit the loyalty balance, consume the promo code, and
//     queue the confirmation email, all or nothing.
//   - Typed collections and indexes, via generics.
//   - Schema migrations, declared next to the collection and run at Open.
//   - An audit trail: annotate a commit with Tx.Note and read it back with
//     History, optionally across retained log segments.
//   - A change feed (Watch) that can never slow a commit down.
//   - A recovery report, so an application can tell whether the last Open lost
//     anything instead of hoping somebody reads the log.
//   - Verify, which rebuilds the indexes from the records and reports drift.
//   - Read-only opens, so a second process can inspect a live database.
//   - Backup by copying a directory.
//
// # What it deliberately does not give you
//
// No query language, no joins, no key range scans, no MVCC, no replication, and
// no multi-process writing. Declare an index or write a Go loop; a scan costs
// roughly 2us per record, nearly all of it the decode that makes every result a
// fresh copy. Use an index once that matters.
//
// # Layout on disk
//
//	dir/
//	  LOCK                        exclusive, held for the lifetime of the process
//	  snapshot-000000000412.json  full state as of sequence 412
//	  wal.log                     framed, append-only, everything after that
//	  archive-000000000412.log    a past log segment, kept only if History is enabled
//
// Recovery loads the newest valid snapshot and replays the log on top of it,
// stopping at the first frame that is short, fails its checksum, or arrives out
// of sequence. Compaction folds the log back into a new snapshot.
//
// Both are checksummed: every log frame carries a crc32c, and a snapshot is an
// envelope carrying one over its payload. A snapshot that fails its checksum is
// passed over for the previous one and reported through
// RecoveryReport.SnapshotsSkipped. Snapshots written before the envelope
// existed still load, unverified, and are rewritten at the next compaction.
//
// Archived segments are history, never recovery input: their frames are already
// in the snapshot, so replaying them would apply the same changes twice.
//
// # Usage
//
// Opening is two-phase, because replaying the log needs to know the
// collections and indexes before it can rebuild them:
//
//	db := datastore.New(datastore.Options{Dir: "data/main"})
//	orders := datastore.Register[*Order](db, "orders")
//	byCustomer := datastore.AddIndex(orders, "customer",
//	    func(o *Order) []string { return []string{o.CustomerID} })
//	if err := db.Open(); err != nil {
//	    return err
//	}
//	defer db.Close()
//
//	err := db.Update(func(tx *datastore.Tx) error {
//	    return datastore.In(tx, orders).Put(&Order{ID: tx.NextID("order")})
//	})
//
// # Reading, and where from
//
// Outside a transaction, read straight off the collection or an index:
// orders.Get(key), byCustomer.Find(term), orders.All(). Every value handed back
// is a fresh copy.
//
// Inside Update or View, read through the transaction instead — In(tx, orders),
// InIndex(tx, byCustomer), InUnique, InSorted. View holds the read lock for its
// whole callback and the direct methods take that lock again; sync.RWMutex is
// not reentrant, so a second acquisition deadlocks once a writer is queued. The
// In* wrappers know the lock is already held. Reading through the transaction is
// also what makes a read-then-write correct, since the pair is then atomic.
package datastore
