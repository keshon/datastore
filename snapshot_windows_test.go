//go:build windows

package datastore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWindowsSnapshotWriteThroughSupportsLongPaths(t *testing.T) {
	dir := t.TempDir()
	for range 4 {
		dir = filepath.Join(dir, strings.Repeat("segment-", 10))
	}
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	f := openFixture(t, dir)
	mustPut(t, f.orders, &order{ID: "a"})
	err = f.db.Compact()
	if err != nil {
		t.Fatal(err)
	}
	// Exercise replacement as well as first installation at the same sequence.
	err = f.db.Compact()
	if err != nil {
		t.Fatal(err)
	}
	release(t, f.db)
	recovered := openFixture(t, dir)
	if _, ok := recovered.orders.Get("a"); !ok {
		t.Fatal("record lost after long-path snapshot installation")
	}
}
