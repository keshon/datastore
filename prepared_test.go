package datastore

import (
	"encoding/json"
	"testing"
)

type preflightRecord struct {
	ID     string `json:"id"`
	Broken bool   `json:"-"`
}

func (r *preflightRecord) Key() string { return r.ID }

func (r *preflightRecord) MarshalJSON() ([]byte, error) {
	if r.Broken {
		return []byte("1"), nil
	}
	type plain preflightRecord
	return json.Marshal((*plain)(r))
}

func TestUndecodablePutRejectsBeforeAppendWithoutUniqueIndex(t *testing.T) {
	db := New(Options{Dir: t.TempDir(), CompactInterval: CompactNever})
	rows := Register[*preflightRecord](db, "rows")
	err := db.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	before := db.Stats()
	err = rows.Put(&preflightRecord{ID: "bad", Broken: true})
	if err == nil {
		t.Fatal("non-round-tripping record was accepted")
	}
	after := db.Stats()
	if after.Seq != before.Seq || after.WALBytes != before.WALBytes || rows.Len() != 0 {
		t.Fatal("rejected record changed the WAL or state")
	}
	err = rows.Put(&preflightRecord{ID: "good"})
	if err != nil {
		t.Fatalf("rejected record poisoned future writes: %v", err)
	}
	err = db.Verify()
	if err != nil {
		t.Fatal(err)
	}
}

func TestPreparedPutOwnsItsDecodedRecordUntilDurable(t *testing.T) {
	f := newFixture(t)
	paused, resume := pauseNextSync(t, f, nil)
	v := &order{ID: "a", Email: "original", Customer: "before", Tags: []string{"original-tag"}}
	done := make(chan error, 1)
	go func() { done <- f.orders.Put(v) }()
	<-paused.entered
	if _, exists := f.orders.Get("a"); exists {
		t.Error("prepared record was published before sync")
	}
	v.Email = "mutated"
	v.Customer = "after"
	v.Tags[0] = "mutated-tag"
	resume()
	err := <-done
	if err != nil {
		t.Fatal(err)
	}
	stored, exists := f.byEmail.Get("original")
	if !exists || stored.Customer != "before" {
		t.Fatalf("caller mutation changed prepared data: %v", stored)
	}
	if _, exists = f.byEmail.Get("mutated"); exists {
		t.Fatal("caller mutation changed the unique index")
	}
	if f.byTag.Count("original-tag") != 1 || f.byTag.Count("mutated-tag") != 0 {
		t.Fatal("caller mutation changed prepared nested data")
	}
	err = f.db.Verify()
	if err != nil {
		t.Fatal(err)
	}
	release(t, f.db)
	recovered := openFixture(t, f.dir)
	stored, exists = recovered.byEmail.Get("original")
	if !exists || stored.Customer != "before" {
		t.Fatal("prepared state disagrees with WAL recovery")
	}
}

type preparedValueRecord struct {
	ID   string `json:"id"`
	Term string `json:"term"`
}

func (r preparedValueRecord) Key() string { return r.ID }

func TestPreparedValueEntity(t *testing.T) {
	db := New(Options{Dir: t.TempDir(), CompactInterval: CompactNever})
	rows := Register[preparedValueRecord](db, "rows")
	index := AddUnique(rows, "term", func(v preparedValueRecord) string { return v.Term })
	err := db.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	err = rows.Put(preparedValueRecord{ID: "a", Term: "value"})
	if err != nil {
		t.Fatal(err)
	}
	v, exists := index.Get("value")
	if !exists || v.ID != "a" {
		t.Fatalf("value entity did not apply: %+v", v)
	}
	err = db.Verify()
	if err != nil {
		t.Fatal(err)
	}
}
