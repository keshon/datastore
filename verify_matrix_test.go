package datastore

import "testing"

func TestVerifyDetectsCorruptedIndexLinks(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(*fixture)
	}{
		{"multi-forward-missing", func(f *fixture) { delete(f.byStatus.byTerm["new"], "a") }},
		{"multi-reverse-wrong", func(f *fixture) { f.byStatus.byKey["a"] = []string{"other"} }},
		{"multi-both-wrong", func(f *fixture) {
			delete(f.byStatus.byTerm["new"], "a")
			f.byStatus.byTerm["other"] = map[string]struct{}{"a": {}}
			f.byStatus.byKey["a"] = []string{"other"}
		}},
		{"multi-empty-term", func(f *fixture) { f.byStatus.byTerm["empty"] = map[string]struct{}{} }},
		{"multi-ghost-reverse", func(f *fixture) { f.byStatus.byKey["ghost"] = []string{"new"} }},
		{"unique-forward-missing", func(f *fixture) { delete(f.byEmail.byTerm, "a@example.com") }},
		{"unique-reverse-missing", func(f *fixture) { delete(f.byEmail.byKey, "a") }},
		{"unique-both-wrong", func(f *fixture) {
			delete(f.byEmail.byTerm, "a@example.com")
			f.byEmail.byTerm["other"] = "a"
			f.byEmail.byKey["a"] = "other"
		}},
		{"unique-extra-forward", func(f *fixture) { f.byEmail.byTerm["extra"] = "a" }},
		{"unique-ghost-reverse", func(f *fixture) { f.byEmail.byKey["ghost"] = "extra" }},
		{"sorted-entry-missing", func(f *fixture) { f.byDate.entries = f.byDate.entries[:1] }},
		{"sorted-duplicate", func(f *fixture) { f.byDate.entries[1] = f.byDate.entries[0] }},
		{"sorted-order", func(f *fixture) { f.byDate.entries[0], f.byDate.entries[1] = f.byDate.entries[1], f.byDate.entries[0] }},
		{"sorted-both-wrong", func(f *fixture) {
			f.byDate.entries[0].n = -10
			f.byDate.byKey["a"] = -10
		}},
		{"sorted-reverse-missing", func(f *fixture) { delete(f.byDate.byKey, "a") }},
		{"sorted-ghost-reverse", func(f *fixture) { f.byDate.byKey["ghost"] = 0 }},
		{"payload-accounting", func(f *fixture) { f.orders.encodedBytes++ }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t)
			mustPut(t, f.orders, &order{ID: "a", Email: "a@example.com", Status: "new", Created: 0})
			mustPut(t, f.orders, &order{ID: "b", Email: "b@example.com", Status: "paid", Created: 1})
			err := f.db.Verify()
			if err != nil {
				t.Fatal(err)
			}
			test.corrupt(f)
			err = f.db.Verify()
			if err == nil {
				t.Fatal("corrupted index passed verification")
			}
		})
	}
}

func TestVerifyAllowsRepeatedMultiTermsAndEmptyUniqueTerms(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, &order{ID: "a", Tags: []string{"x", "x", "y"}})
	mustPut(t, f.orders, &order{ID: "b"})
	err := f.db.Verify()
	if err != nil {
		t.Fatal(err)
	}
}

func TestVerifyDetectsDuplicateUniqueRecords(t *testing.T) {
	f := newFixture(t)
	mustPut(t, f.orders, &order{ID: "a", Email: "same"})
	// Bypass commit validation to model corrupt persisted record contents.
	err := f.orders.applyPut("b", []byte(`{"id":"b","email":"same"}`))
	if err != nil {
		t.Fatal(err)
	}
	err = f.db.Verify()
	if err == nil {
		t.Fatal("duplicate unique values passed verification")
	}
}
