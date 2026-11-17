package datastore

import (
	"fmt"
	"strings"
)

// IntegrityError reports disagreements found by Verify.
type IntegrityError struct {
	Problems []string
}

func (e *IntegrityError) Error() string {
	if len(e.Problems) == 1 {
		return "datastore: integrity check failed: " + e.Problems[0]
	}
	return fmt.Sprintf("datastore: integrity check failed with %d problems:\n  %s",
		len(e.Problems), strings.Join(e.Problems, "\n  "))
}

// Verify recomputes index terms from records and checks both index directions,
// including extra entries, sort order and payload accounting. It also reports
// records that cannot be decoded or are stored under a key they do not claim.
//
// The indexes are derived state, maintained incrementally on every write. That
// is fast and it is the one place where a bug could go unnoticed indefinitely:
// nothing else ever compares them against the records they came from. Verify is
// that comparison, made available so it can run in your tests, in CI, or behind
// an admin command — rather than living only inside this package's own suite.
//
// It is read-only and takes the read lock for the duration, so it is safe to
// call on a live database. Work scales with record bytes and index entries;
// writers wait for the whole check, while other readers can proceed.
func (db *DB) Verify() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if !db.opened || db.closed {
		return ErrNotOpen
	}

	var problems []string
	for _, name := range db.order {
		coll, ok := db.colls[name]
		if !ok {
			continue
		}
		problems = append(problems, coll.verify()...)
	}
	if len(problems) == 0 {
		return nil
	}
	return &IntegrityError{Problems: problems}
}
