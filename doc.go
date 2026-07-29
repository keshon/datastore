// Package datastore is a small embedded store for data that must not be lost.
//
// It holds everything in memory and appends every commit to a write-ahead log
// before acknowledging it. That combination is only reasonable because of a
// specific assumption: the whole dataset fits comfortably in RAM. For the shop
// this was written for, that is a few hundred kilobytes today and perhaps
// twenty megabytes in a decade. The moment that assumption stops holding, the
// right move is to export to something else — the on-disk format is plain JSON
// precisely so that stays easy.
//
// # What it gives you
//
//   - Crash durability. A commit is on disk and fsynced before Update returns.
//   - Real transactions. Several records across several collections commit as
//     one atomic unit, which is what placing an order actually requires:
//     write the order, debit the loyalty balance, consume the promo code, and
//     queue the confirmation email, all or nothing.
//   - Typed collections and indexes, via generics.
//   - Backup by copying a directory.
//
// # What it deliberately does not give you
//
// No query language, no joins, no key range scans, no MVCC, no replication, no
// multi-process access, and no automatic schema migration. Declare an index or
// write a Go loop; at these volumes a linear scan is measured in microseconds.
//
// # Layout on disk
//
//	dir/
//	  LOCK                  exclusive, held for the lifetime of the process
//	  snapshot-000412.json  full state as of sequence 412
//	  wal.log               framed, append-only, everything after that
//
// Recovery loads the newest valid snapshot and replays the log on top of it,
// stopping at the first frame that is short, fails its checksum, or arrives out
// of sequence. Compaction folds the log back into a new snapshot.
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
package datastore
