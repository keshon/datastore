package datastore

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"
)

func tempFile(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "datastore-*.json")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	f.Close()
	os.Remove(f.Name()) // let New() create it fresh
	return f.Name()
}

func newStore(t *testing.T) (*DataStore, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ds, err := New(ctx, tempFile(t), WithSaveInterval(time.Hour))
	if err != nil {
		cancel()
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		ds.Close()
	})
	return ds, cancel
}

func TestSetGet(t *testing.T) {
	ds, _ := newStore(t)

	if err := ds.Set("name", "Alice"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	var got string
	ok, err := ds.Get("name", &got)
	if err != nil || !ok || got != "Alice" {
		t.Fatalf("Get: ok=%v err=%v got=%q", ok, err, got)
	}
}

func TestGetMissing(t *testing.T) {
	ds, _ := newStore(t)

	var v string
	ok, err := ds.Get("nope", &v)
	if ok || err != nil {
		t.Fatalf("expected (false, nil), got (%v, %v)", ok, err)
	}
}

func TestDelete(t *testing.T) {
	ds, _ := newStore(t)

	_ = ds.Set("x", 1)
	_ = ds.Delete("x")

	var v int
	ok, _ := ds.Get("x", &v)
	if ok {
		t.Fatal("key should be deleted")
	}
}

func TestKeys(t *testing.T) {
	ds, _ := newStore(t)

	_ = ds.Set("a", 1)
	_ = ds.Set("b", 2)

	keys := ds.Keys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

func TestLen(t *testing.T) {
	ds, _ := newStore(t)

	_ = ds.Set("a", 1)
	_ = ds.Set("b", 2)
	_ = ds.Delete("a")

	if ds.Len() != 1 {
		t.Fatalf("expected len=1, got %d", ds.Len())
	}
}

func TestPersistence(t *testing.T) {
	path := tempFile(t)

	ctx, cancel := context.WithCancel(context.Background())
	ds, err := New(ctx, path, WithSaveInterval(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	_ = ds.Set("key", "value")
	cancel()
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	ds2, err := New(ctx2, path, WithSaveInterval(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer ds2.Close()

	var got string
	ok, err := ds2.Get("key", &got)
	if !ok || err != nil || got != "value" {
		t.Fatalf("persistence failed: ok=%v err=%v got=%q", ok, err, got)
	}
}

func TestTypedValues(t *testing.T) {
	ds, _ := newStore(t)

	type Point struct {
		X, Y int
	}
	_ = ds.Set("pt", Point{1, 2})

	var got Point
	ok, err := ds.Get("pt", &got)
	if !ok || err != nil || got.X != 1 || got.Y != 2 {
		t.Fatalf("typed value: ok=%v err=%v got=%+v", ok, err, got)
	}
}

func TestNoFloat64Surprise(t *testing.T) {
	// Without json.RawMessage storage, integers become float64 after a
	// save/reload cycle. This test ensures that does not happen.
	path := tempFile(t)

	ctx, cancel := context.WithCancel(context.Background())
	ds, _ := New(ctx, path, WithSaveInterval(time.Hour))
	_ = ds.Set("n", 42)
	cancel()
	ds.Close()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	ds2, _ := New(ctx2, path)
	defer ds2.Close()

	var n int
	ok, err := ds2.Get("n", &n)
	if !ok || err != nil || n != 42 {
		t.Fatalf("expected int 42, got ok=%v err=%v n=%v", ok, err, n)
	}
}

func TestConcurrentAccess(t *testing.T) {
	ds, _ := newStore(t)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			_ = ds.Set("key", i)
		}(i)

		go func() {
			defer wg.Done()
			var v int
			_, _ = ds.Get("key", &v)
		}()
	}

	wg.Wait()
}

func TestErrClosedOnWrite(t *testing.T) {
	ds, cancel := newStore(t)
	cancel()
	ds.Close()

	if err := ds.Set("k", "v"); err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestAutoSave(t *testing.T) {
	path := tempFile(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, err := New(ctx, path, WithSaveInterval(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	_ = ds.Set("auto", "saved")

	time.Sleep(200 * time.Millisecond)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("parse file: %v", err)
	}
	if _, ok := m["auto"]; !ok {
		t.Fatal("expected key 'auto' in persisted file")
	}
}
