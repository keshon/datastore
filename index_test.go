package datastore

import (
	"fmt"
	"strings"
	"testing"
)

func ids(os []*order) string {
	out := make([]string, len(os))
	for i, o := range os {
		out[i] = o.ID
	}
	return strings.Join(out, ",")
}

func TestIndexTracksUpdates(t *testing.T) {
	f := newFixture(t)

	mustPut(t, f.orders, mkOrder("1", func(o *order) { o.Status = "new" }))
	mustPut(t, f.orders, mkOrder("2", func(o *order) { o.Status = "new" }))
	mustPut(t, f.orders, mkOrder("3", func(o *order) { o.Status = "paid" }))

	if got := ids(f.byStatus.Get("new")); got != "1,2" {
		t.Errorf("status=new -> %q, want %q", got, "1,2")
	}

	// Moving a record between terms must remove it from the old one. Getting
	// this wrong leaves a record findable under a value it no longer has.
	mustPut(t, f.orders, mkOrder("1", func(o *order) { o.Status = "paid" }))
	if got := ids(f.byStatus.Get("new")); got != "2" {
		t.Errorf("after update status=new -> %q, want %q", got, "2")
	}
	if got := ids(f.byStatus.Get("paid")); got != "1,3" {
		t.Errorf("after update status=paid -> %q, want %q", got, "1,3")
	}

	if err := f.orders.Delete("3"); err != nil {
		t.Fatal(err)
	}
	if got := ids(f.byStatus.Get("paid")); got != "1" {
		t.Errorf("after delete status=paid -> %q, want %q", got, "1")
	}
	if n := f.byStatus.Count("paid"); n != 1 {
		t.Errorf("Count = %d, want 1", n)
	}
}

func TestIndexWithSeveralTermsPerRecord(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, mkOrder("1", func(o *order) { o.Tags = []string{"gift", "urgent"} }))
	mustPut(t, f.orders, mkOrder("2", func(o *order) { o.Tags = []string{"gift"} }))

	if got := ids(f.byTag.Get("gift")); got != "1,2" {
		t.Errorf("tag=gift -> %q, want %q", got, "1,2")
	}
	if got := ids(f.byTag.Get("urgent")); got != "1" {
		t.Errorf("tag=urgent -> %q, want %q", got, "1")
	}

	mustPut(t, f.orders, mkOrder("1", func(o *order) { o.Tags = []string{"gift"} }))
	if got := ids(f.byTag.Get("urgent")); got != "" {
		t.Errorf("after dropping a tag, urgent -> %q, want empty", got)
	}
	if terms := f.byTag.Terms(); strings.Join(terms, ",") != "gift" {
		t.Errorf("Terms = %v, want [gift]", terms)
	}
}

func TestUniqueIndexLookup(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, mkOrder("1", func(o *order) { o.Email = "a@example.com" }))

	got, ok := f.byEmail.Get("a@example.com")
	if !ok || got.ID != "1" {
		t.Errorf("unique lookup = %+v/%v, want order 1", got, ok)
	}
	if _, ok := f.byEmail.Get("nobody@example.com"); ok {
		t.Error("unique lookup found a term that was never indexed")
	}
}

// An empty term opts a record out of the index. Without that rule, every
// customer without an email address would collide with every other one.
func TestUniqueIndexIgnoresEmptyTerms(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, mkOrder("1", func(o *order) { o.Email = "" }))
	if err := f.orders.Put(mkOrder("2", func(o *order) { o.Email = "" })); err != nil {
		t.Fatalf("two records with no email should coexist, got %v", err)
	}
}

func TestSortedIndexOrdering(t *testing.T) {
	f := newFixture(t)
	for i, created := range []int64{300, 100, 200, 500, 400} {
		mustPut(t, f.orders, mkOrder(fmt.Sprint(i), func(o *order) { o.Created = created }))
	}

	if got := ids(f.byDate.Asc(0, 0)); got != "1,2,0,4,3" {
		t.Errorf("Asc = %q, want %q", got, "1,2,0,4,3")
	}
	if got := ids(f.byDate.Desc(0, 0)); got != "3,4,0,2,1" {
		t.Errorf("Desc = %q, want %q", got, "3,4,0,2,1")
	}
	if got := ids(f.byDate.Desc(2, 0)); got != "3,4" {
		t.Errorf("Desc(2,0) = %q, want %q", got, "3,4")
	}
	if got := ids(f.byDate.Desc(2, 1)); got != "4,0" {
		t.Errorf("Desc(2,1) = %q, want %q", got, "4,0")
	}
	if got := ids(f.byDate.Range(200, 400)); got != "2,0,4" {
		t.Errorf("Range(200,400) = %q, want %q", got, "2,0,4")
	}
	if got := ids(f.byDate.Range(400, 200)); got != "" {
		t.Errorf("Range with lo>hi = %q, want empty", got)
	}
	if n := f.byDate.Len(); n != 5 {
		t.Errorf("Len = %d, want 5", n)
	}
}

// Records sharing a sort key must have a total, reproducible order — otherwise
// two orders placed in the same second could swap places between restarts and
// a paginated admin list would skip or repeat rows.
func TestSortedIndexBreaksTiesOnKey(t *testing.T) {
	f := newFixture(t)
	for _, id := range []string{"c", "a", "b"} {
		mustPut(t, f.orders, mkOrder(id, func(o *order) { o.Created = 42 }))
	}
	if got := ids(f.byDate.Asc(0, 0)); got != "a,b,c" {
		t.Errorf("tied Asc = %q, want %q", got, "a,b,c")
	}
}

func TestSortedIndexFollowsUpdates(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, mkOrder("1", func(o *order) { o.Created = 100 }))
	mustPut(t, f.orders, mkOrder("2", func(o *order) { o.Created = 200 }))

	mustPut(t, f.orders, mkOrder("1", func(o *order) { o.Created = 300 }))
	if got := ids(f.byDate.Asc(0, 0)); got != "2,1" {
		t.Errorf("after re-dating = %q, want %q", got, "2,1")
	}
	if n := f.byDate.Len(); n != 2 {
		t.Errorf("Len = %d after an update, want 2 — the old entry was not removed", n)
	}
}

// Indexes are never written to disk, so a restart has to rebuild them from the
// records. If it does not, lookups quietly return nothing.
func TestIndexesRebuiltOnReopen(t *testing.T) {
	dir := t.TempDir()
	f := openFixture(t, dir)
	mustPut(t, f.orders, mkOrder("1", func(o *order) {
		o.Status, o.Email, o.Created, o.Tags = "paid", "a@example.com", 500, []string{"gift"}
	}))
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}

	g := openFixture(t, dir)
	if got := ids(g.byStatus.Get("paid")); got != "1" {
		t.Errorf("status index after reopen = %q, want %q", got, "1")
	}
	if got := ids(g.byTag.Get("gift")); got != "1" {
		t.Errorf("tag index after reopen = %q, want %q", got, "1")
	}
	if _, ok := g.byEmail.Get("a@example.com"); !ok {
		t.Error("unique index was not rebuilt on reopen")
	}
	if got := ids(g.byDate.Asc(0, 0)); got != "1" {
		t.Errorf("sorted index after reopen = %q, want %q", got, "1")
	}
}
