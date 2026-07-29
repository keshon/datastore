package datastore

import (
	"encoding/json"
	"strings"
	"testing"
)

// A healthy database verifies clean.
func TestVerifyCleanDatabase(t *testing.T) {
	f := newFixture(t)
	for _, id := range []string{"a", "b", "c"} {
		if err := f.orders.Put(mkOrder(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.db.Verify(); err != nil {
		t.Fatalf("a healthy database should verify: %v", err)
	}
}

// Verify earns its keep only if it actually notices drift, so corrupt an index
// behind the collection's back and confirm it is caught.
func TestVerifyDetectsIndexDrift(t *testing.T) {
	f := newFixture(t)
	if err := f.orders.Put(mkOrder("a", func(o *order) { o.Status = "paid" })); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Verify(); err != nil {
		t.Fatalf("baseline should be clean: %v", err)
	}

	// Reach past the API and drop the record from the multi-valued index.
	f.db.mu.Lock()
	f.byStatus.remove("a")
	f.db.mu.Unlock()

	err := f.db.Verify()
	if err == nil {
		t.Fatal("Verify did not notice a record missing from an index")
	}
	var ie *IntegrityError
	if !asIntegrity(err, &ie) {
		t.Fatalf("want *IntegrityError, got %T", err)
	}
	if len(ie.Problems) == 0 || !strings.Contains(ie.Error(), "missing from the index") {
		t.Fatalf("problem should describe the drift: %v", err)
	}
}

// A record stored under a key it does not own is a corruption of the most
// confusing kind; Verify should name it.
func TestVerifyDetectsKeyMismatch(t *testing.T) {
	f := newFixture(t)
	if err := f.orders.Put(mkOrder("a")); err != nil {
		t.Fatal(err)
	}

	f.db.mu.Lock()
	raw, _ := json.Marshal(mkOrder("different"))
	f.db.colls["orders"].(*Collection[*order]).data["a"] = raw
	f.db.mu.Unlock()

	err := f.db.Verify()
	if err == nil || !strings.Contains(err.Error(), "does not own") {
		t.Fatalf("expected a key-ownership problem, got: %v", err)
	}
}

func TestVerifyBeforeOpen(t *testing.T) {
	db := New(Options{Dir: t.TempDir()})
	if err := db.Verify(); err == nil {
		t.Fatal("Verify before Open should fail")
	}
}

func asIntegrity(err error, target **IntegrityError) bool {
	ie, ok := err.(*IntegrityError)
	if ok {
		*target = ie
	}
	return ok
}
