# Datastore

Datastore is a lightweight, thread-safe in-memory key-value store for Go, with automatic JSON persistence to disk. Perfect for small applications, bots, or projects that need simple data storage without a database.

---

## Features

- Thread-safe in-memory key-value store backed by `json.RawMessage` — no lossy type round-trips
- Typed get/set via `json.Marshal` / `json.Unmarshal`
- Automatic periodic save to a JSON file (configurable interval)
- Atomic writes via temp file + rename — no corruption on crash
- Graceful shutdown via `context.Context`
- Structured logging with `log/slog`
- Functional options for easy configuration

---

## Installation

```bash
go get github.com/keshon/datastore
```

---

## Usage

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/keshon/datastore"
)

func main() {
    ctx, cancel := context.WithCancel(context.Background())

    // Handle OS signals — datastore does NOT call os.Exit itself
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
    go func() {
        <-sigCh
        cancel()
    }()

    ds, err := datastore.New(ctx, "data.json",
        datastore.WithSaveInterval(time.Minute),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer ds.Close()

    // Store any JSON-serialisable value
    if err := ds.Set("user", map[string]string{"name": "Alice"}); err != nil {
        log.Fatal(err)
    }

    // Retrieve with type safety — no float64 surprises
    var user map[string]string
    ok, err := ds.Get("user", &user)
    if err != nil {
        log.Fatal(err)
    }
    if ok {
        log.Println(user["name"]) // Output: Alice
    }

    // List all keys
    log.Println(ds.Keys()) // Output: [user]

    // Delete a key
    ds.Delete("user")
}
```

---

## API

| Method | Description |
|--------|-------------|
| `New(ctx context.Context, filePath string, opts ...Option) (*DataStore, error)` | Creates a new DataStore. Background save goroutine exits when `ctx` is cancelled. |
| `Set(key string, value any) error` | Stores a value. Returns an error if value is not JSON-serialisable, or if the store is closed. |
| `Get(key string, dest any) (bool, error)` | Retrieves and unmarshals a value into `dest`. Returns `(false, nil)` if the key does not exist. |
| `Delete(key string) error` | Deletes a key. No-op if the key does not exist. |
| `Keys() []string` | Returns a snapshot of all current keys. |
| `Len() int` | Returns the number of keys. |
| `Close() error` | Stops auto-save, waits for the background goroutine, and performs a final save. Safe to call multiple times. |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithSaveInterval(d time.Duration)` | `5m` | Interval between automatic saves. |
| `WithLogger(l *slog.Logger)` | `slog.Default()` | Custom structured logger. |

### Errors

| Error | Description |
|-------|-------------|
| `datastore.ErrClosed` | Returned by `Set` / `Delete` after `Close()` has been called. |

---

## Typed values

Values are stored as `json.RawMessage` internally. This means types survive a save/reload cycle without conversion:

```go
_ = ds.Set("count", 42)

// After reloading from disk:
var n int
ds.Get("count", &n) // n == 42, not 42.0
```

---

## Running tests

```bash
# Run all tests
go test -timeout 30s ./...

# With race detector (recommended)
go test -timeout 30s -race ./...

# Verbose output
go test -timeout 30s -v -race ./...

# Run a specific test
go test -timeout 30s -v -run TestPersistence ./...
```

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
