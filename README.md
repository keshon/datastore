# Datastore

Datastore is a lightweight, thread-safe in-memory key-value store for Go, with automatic JSON persistence to disk. It is perfect for small applications, bots, or projects that need simple data storage without a database.

---

## Features

- Thread-safe in-memory key-value store
- Automatic periodic save to a JSON file
- Manual save and load
- Graceful shutdown with final save
- Simple API for add, get, delete, and list keys

---

## Installation

```bash
go get github.com/keshon/datastore
````

---

## Usage

```go
package main

import (
    "fmt"
    "github.com/keshon/datastore"
)

func main() {
    // Initialize datastore with a file for persistence
    ds, err := datastore.New("data.json")
    if err != nil {
        panic(err)
    }
    defer ds.Close()

    // Add data
    ds.Add("foo", "bar")

    // Get data
    value, ok := ds.Get("foo")
    fmt.Println(value, ok) // Output: bar true

    // List all keys
    keys := ds.Keys()
    fmt.Println(keys) // Output: ["foo"]

    // Delete a key
    ds.Delete("foo")
}
```

---

## API

| Method                                     | Description                                                       |
| ------------------------------------------ | ----------------------------------------------------------------- |
| `New(filePath string) (*DataStore, error)` | Creates a new datastore with optional JSON file persistence.      |
| `Add(key string, value interface{})`       | Add a key-value pair.                                             |
| `Get(key string) (interface{}, bool)`      | Retrieve a value by key. Returns the value and whether it exists. |
| `GetAll() map[string]interface{}`          | Returns a copy of all key-value pairs.                            |
| `Delete(key string)`                       | Deletes a key from the store.                                     |
| `Keys() []string`                          | Returns a list of all keys.                                       |
| `Close() error`                            | Stops auto-save and writes the current data to the file.          |

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

