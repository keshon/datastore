package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// ErrClosed is returned when operations are attempted on a closed DataStore.
var ErrClosed = errors.New("datastore: store is closed")

// Option is a functional option for configuring a DataStore.
type Option func(*DataStore)

// WithSaveInterval sets the interval for periodic saves (default: 5 minutes).
func WithSaveInterval(d time.Duration) Option {
	return func(ds *DataStore) {
		ds.saveInterval = d
	}
}

// WithLogger sets a custom slog.Logger (default: slog.Default()).
func WithLogger(l *slog.Logger) Option {
	return func(ds *DataStore) {
		ds.log = l
	}
}

// DataStore is a thread-safe, persistent key-value store backed by a JSON file.
// It periodically flushes to disk and supports graceful shutdown via context.
//
// Values are stored as json.RawMessage internally to avoid lossy round-trips
// (e.g. numbers becoming float64 after unmarshal).
type DataStore struct {
	data         map[string]json.RawMessage
	file         string
	saveInterval time.Duration
	log          *slog.Logger

	mu     sync.RWMutex
	once   sync.Once
	closed bool
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// New creates a new DataStore backed by filePath.
// The provided context controls the lifecycle of the background save goroutine:
// when ctx is cancelled, the goroutine stops and a final save is performed.
func New(ctx context.Context, filePath string, opts ...Option) (*DataStore, error) {
	ds := &DataStore{
		data:         make(map[string]json.RawMessage),
		file:         filePath,
		saveInterval: 5 * time.Minute,
		log:          slog.Default(),
	}
	for _, o := range opts {
		o(ds)
	}

	if err := ds.initFile(); err != nil {
		return nil, err
	}

	// Derive an internal cancel so Close can stop the autosave goroutine
	// itself; callers no longer have to cancel ctx before calling Close.
	ctx, ds.cancel = context.WithCancel(ctx)

	ds.wg.Add(1)
	go ds.autoSave(ctx)

	return ds, nil
}

// Set stores value under key. value must be JSON-serialisable.
func (ds *DataStore) Set(key string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("datastore: marshal value for key %q: %w", key, err)
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.closed {
		return ErrClosed
	}
	ds.data[key] = raw
	return nil
}

// Get retrieves the value for key and unmarshals it into dest.
// Returns (false, nil) if the key does not exist.
func (ds *DataStore) Get(key string, dest any) (bool, error) {
	ds.mu.RLock()
	raw, exists := ds.data[key]
	ds.mu.RUnlock()

	if !exists {
		return false, nil
	}
	if err := json.Unmarshal(raw, dest); err != nil {
		return true, fmt.Errorf("datastore: unmarshal value for key %q: %w", key, err)
	}
	return true, nil
}

// Delete removes key from the store. No-op if key does not exist.
func (ds *DataStore) Delete(key string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.closed {
		return ErrClosed
	}
	delete(ds.data, key)
	return nil
}

// Keys returns a snapshot of all current keys.
func (ds *DataStore) Keys() []string {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	keys := make([]string, 0, len(ds.data))
	for k := range ds.data {
		keys = append(keys, k)
	}
	return keys
}

// Len returns the number of keys in the store.
func (ds *DataStore) Len() int {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return len(ds.data)
}

// Flush immediately persists the current snapshot to disk. Use it after
// critical mutations that must survive a crash without waiting for the
// periodic autosave.
func (ds *DataStore) Flush() error {
	ds.mu.RLock()
	closed := ds.closed
	ds.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	return ds.saveToFile()
}

// Close performs a final save and marks the store as closed.
// Subsequent writes will return ErrClosed. Safe to call multiple times.
func (ds *DataStore) Close() error {
	var saveErr error
	ds.once.Do(func() {
		ds.mu.Lock()
		ds.closed = true
		ds.mu.Unlock()

		ds.cancel()  // stop the autosave goroutine
		ds.wg.Wait() // wait for autoSave to exit

		if err := ds.saveToFile(); err != nil {
			saveErr = err
		}
		ds.log.Info("datastore closed", "file", ds.file)
	})
	return saveErr
}

// --- internal ---

func (ds *DataStore) initFile() error {
	_, err := os.Stat(ds.file)
	if os.IsNotExist(err) {
		if err := os.WriteFile(ds.file, []byte("{}"), 0o644); err != nil {
			return fmt.Errorf("datastore: create file %q: %w", ds.file, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("datastore: stat file %q: %w", ds.file, err)
	}
	return ds.loadFromFile()
}

func (ds *DataStore) loadFromFile() error {
	raw, err := os.ReadFile(ds.file)
	if err != nil {
		return fmt.Errorf("datastore: read file %q: %w", ds.file, err)
	}

	var data map[string]json.RawMessage
	if err := json.Unmarshal(raw, &data); err != nil {
		return fmt.Errorf("datastore: parse file %q: %w", ds.file, err)
	}

	ds.mu.Lock()
	ds.data = data
	ds.mu.Unlock()
	return nil
}

// saveToFile serialises the current snapshot and writes it atomically.
func (ds *DataStore) saveToFile() error {
	ds.mu.RLock()
	snapshot := make(map[string]json.RawMessage, len(ds.data))
	for k, v := range ds.data {
		snapshot[k] = v
	}
	ds.mu.RUnlock()

	raw, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("datastore: marshal: %w", err)
	}

	tmp := ds.file + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("datastore: write temp file: %w", err)
	}
	if err := os.Rename(tmp, ds.file); err != nil {
		_ = os.Remove(tmp) // best-effort cleanup
		return fmt.Errorf("datastore: rename temp file: %w", err)
	}
	return nil
}

func (ds *DataStore) autoSave(ctx context.Context) {
	defer ds.wg.Done()

	ticker := time.NewTicker(ds.saveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			ds.log.Info("datastore autosave stopping", "reason", ctx.Err())
			return
		case <-ticker.C:
			if err := ds.saveToFile(); err != nil {
				ds.log.Error("datastore autosave failed", "err", err)
			} else {
				ds.log.Debug("datastore autosaved", "file", ds.file, "keys", ds.Len())
			}
		}
	}
}
