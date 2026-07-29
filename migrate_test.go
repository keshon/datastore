package datastore

import (
	"encoding/json"
	"strings"
	"testing"
)

// A v1 record: one "name" field. v2 splits it into "first" and "last".
type person struct {
	ID    string `json:"id"`
	Name  string `json:"name,omitempty"`
	First string `json:"first,omitempty"`
	Last  string `json:"last,omitempty"`
}

func (p *person) Key() string { return p.ID }

func splitName(raw json.RawMessage) (json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	if v, ok := m["name"]; ok {
		var name string
		if err := json.Unmarshal(v, &name); err != nil {
			return nil, err
		}
		first, last, _ := strings.Cut(name, " ")
		m["first"], _ = json.Marshal(first)
		m["last"], _ = json.Marshal(last)
		delete(m, "name")
	}
	return json.Marshal(m)
}

// Write a v1 database, then reopen it as v2 and confirm every record was
// carried across, the report says so, and the on-disk version was restamped.
func TestMigrationUpgradesRecords(t *testing.T) {
	dir := t.TempDir()

	db1 := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	people1 := Register[*person](db1, "people", Version(1))
	if err := db1.Open(); err != nil {
		t.Fatalf("open v1: %v", err)
	}
	if err := people1.Put(&person{ID: "a", Name: "Ada Lovelace"}); err != nil {
		t.Fatal(err)
	}
	if err := people1.Put(&person{ID: "b", Name: "Alan Turing"}); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	people2 := Register[*person](db2, "people", Version(2))
	Migrate(people2, 1, splitName)
	if err := db2.Open(); err != nil {
		t.Fatalf("open v2: %v", err)
	}
	defer db2.Close()

	got, ok := people2.Get("a")
	if !ok {
		t.Fatal("record a missing after migration")
	}
	if got.First != "Ada" || got.Last != "Lovelace" || got.Name != "" {
		t.Fatalf("record a not migrated: %+v", got)
	}
	if b, _ := people2.Get("b"); b.First != "Alan" {
		t.Fatalf("record b not migrated: %+v", b)
	}

	rep := db2.Recovery()
	if from, ok := rep.Migrated["people"]; !ok || from != 1 {
		t.Fatalf("recovery report should record the migration, got %+v", rep.Migrated)
	}
	if rep.Clean() {
		t.Fatal("a migrating open is not Clean")
	}
	if rep.LostData() {
		t.Fatal("a migration must not count as data loss")
	}

	// Reopening at v2 must be a no-op: the version was restamped on disk.
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	db3 := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	people3 := Register[*person](db3, "people", Version(2))
	Migrate(people3, 1, func(json.RawMessage) (json.RawMessage, error) {
		t.Error("migration ran again on an already-migrated database")
		return nil, nil
	})
	if err := db3.Open(); err != nil {
		t.Fatal(err)
	}
	defer db3.Close()
	if !db3.Recovery().Clean() {
		t.Fatalf("second open should be clean, got %+v", db3.Recovery())
	}
}

// Frames written after the last snapshot carry the old shape too, so replay
// must migrate them as well.
func TestMigrationAppliesToLogFrames(t *testing.T) {
	dir := t.TempDir()

	db1 := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	p1 := Register[*person](db1, "people", Version(1))
	if err := db1.Open(); err != nil {
		t.Fatal(err)
	}
	if err := p1.Put(&person{ID: "a", Name: "Grace Hopper"}); err != nil {
		t.Fatal(err)
	}
	// Close without the final compaction would be ideal; instead force the row
	// to live in the log by writing after the snapshot Close() produces.
	if err := db1.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := p1.Put(&person{ID: "b", Name: "Katherine Johnson"}); err != nil {
		t.Fatal(err)
	}
	// Deliberately no Close(): leave the frame in the log, snapshot at "a" only.
	if err := db1.wal.close(); err != nil {
		t.Fatal(err)
	}
	if err := db1.lock.release(); err != nil {
		t.Fatal(err)
	}

	db2 := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	p2 := Register[*person](db2, "people", Version(2))
	Migrate(p2, 1, splitName)
	if err := db2.Open(); err != nil {
		t.Fatalf("open v2: %v", err)
	}
	defer db2.Close()

	b, ok := p2.Get("b")
	if !ok {
		t.Fatal("record from the log frame is missing")
	}
	if b.First != "Katherine" || b.Name != "" {
		t.Fatalf("log-frame record was not migrated: %+v", b)
	}
}

// Opening data written by a newer build must fail rather than silently
// misinterpret it.
func TestRefusesNewerSchemaOnDisk(t *testing.T) {
	dir := t.TempDir()

	db1 := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	p1 := Register[*person](db1, "people", Version(3))
	if err := db1.Open(); err != nil {
		t.Fatal(err)
	}
	if err := p1.Put(&person{ID: "a", First: "New"}); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	Register[*person](db2, "people", Version(2))
	err := db2.Open()
	if err == nil {
		db2.Close()
		t.Fatal("expected an older build to refuse newer data")
	}
	if !strings.Contains(err.Error(), "newer version") {
		t.Fatalf("error should explain the version mismatch, got: %v", err)
	}
}

// A missing step in the chain is a programming error, and it must be reported
// rather than leaving records half-migrated.
func TestMissingMigrationStepFails(t *testing.T) {
	dir := t.TempDir()

	db1 := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	p1 := Register[*person](db1, "people", Version(1))
	if err := db1.Open(); err != nil {
		t.Fatal(err)
	}
	if err := p1.Put(&person{ID: "a", Name: "Ada"}); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1})
	p2 := Register[*person](db2, "people", Version(3))
	Migrate(p2, 2, splitName) // 1->2 is missing
	err := db2.Open()
	if err == nil {
		db2.Close()
		t.Fatal("expected a missing migration step to fail the open")
	}
	if !strings.Contains(err.Error(), "no migration from version 1") {
		t.Fatalf("error should name the missing step, got: %v", err)
	}
}

func TestMigrateAfterOpenPanics(t *testing.T) {
	db := New(Options{Dir: t.TempDir(), Sync: SyncNever, CompactInterval: -1})
	c := Register[*person](db, "people", Version(2))
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	defer func() {
		if recover() == nil {
			t.Fatal("Migrate after Open should panic")
		}
	}()
	Migrate(c, 1, splitName)
}
