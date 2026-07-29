// Package dstest provides the boilerplate every consumer's tests would
// otherwise write: a database in a temporary directory, configured for speed,
// closed and verified when the test ends.
package dstest

import (
	"testing"

	"github.com/keshon/datastore"
)

// New returns an unopened database in t.TempDir(), configured for tests:
// no fsync, no periodic compaction, and a no-op logger.
//
// Register collections and indexes on it, then call Open — or use Opened, which
// does that for you. The database is closed when the test finishes.
//
//	db := dstest.New(t)
//	users := datastore.Register[*User](db, "users")
//	dstest.Open(t, db)
func New(t testing.TB, tweak ...func(*datastore.Options)) *datastore.DB {
	t.Helper()
	opts := datastore.Options{
		Dir:             t.TempDir(),
		Sync:            datastore.SyncNever,
		CompactInterval: -1,
	}
	for _, fn := range tweak {
		fn(&opts)
	}
	return datastore.New(opts)
}

// Open opens db, fails the test if it cannot, and arranges for it to be closed
// and integrity-checked when the test finishes.
//
// The closing Verify is the point: it means every test that touches the store
// also asserts, for free, that the indexes still agree with the records.
func Open(t testing.TB, db *datastore.DB) {
	t.Helper()
	if err := db.Open(); err != nil {
		t.Fatalf("dstest: open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Verify(); err != nil {
			t.Errorf("dstest: integrity check failed at close: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Errorf("dstest: close: %v", err)
		}
	})
}

// Dir returns the directory a database was opened in, so a test can reopen the
// same data.
func Dir(db *datastore.DB) string { return db.Dir() }
