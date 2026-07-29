package datastore

import (
	"encoding/json"
	"fmt"
)

// Schema migration.
//
// Adding a field needs no migration: JSON decoding fills it with the zero
// value. Migrations are for the changes that decoding cannot guess — a renamed
// field, a changed unit, a struct split in two — where the old records are
// still perfectly readable JSON but no longer mean what the new code thinks.
//
// A migration takes raw JSON and returns raw JSON, because the Go type it has
// to read no longer exists in this build. That is unpleasant to write and it is
// the honest signature: the alternative is keeping a museum of old structs
// compiled into the binary forever.
//
// Declare the version and its migrations at Register time, before Open:
//
//	users := datastore.Register[*User](db, "users", datastore.Version(2))
//	datastore.Migrate(users, 1, func(raw json.RawMessage) (json.RawMessage, error) {
//	    // v1 stored "name"; v2 stores "full_name"
//	    var m map[string]json.RawMessage
//	    if err := json.Unmarshal(raw, &m); err != nil {
//	        return nil, err
//	    }
//	    if v, ok := m["name"]; ok {
//	        m["full_name"] = v
//	        delete(m, "name")
//	    }
//	    return json.Marshal(m)
//	})
//
// At Open, every record stored below the declared version is passed through the
// chain of migrations one version at a time, and the database is compacted
// before Open returns, so the log never mixes schema versions.
//
// Opening data that is *newer* than this build declares fails rather than
// guessing: an older binary cannot know what a later version meant.

// MigrateFunc transforms one record's raw JSON from version n to version n+1.
type MigrateFunc func(raw json.RawMessage) (json.RawMessage, error)

// CollectionOption configures a collection at Register time.
type CollectionOption func(*collectionConfig)

type collectionConfig struct {
	version int
}

// Version declares the schema version of the records this build writes.
// Versions start at 1; an unversioned collection is version 0.
func Version(v int) CollectionOption {
	if v < 0 {
		panic("datastore: schema version must not be negative")
	}
	return func(c *collectionConfig) { c.version = v }
}

// Migrate registers the transform that carries a record from version `from` to
// version from+1. It must be called before Open, and every step between the
// oldest version still on disk and the declared version must exist.
//
// Panics after Open, on a duplicate step, or on a step at or above the
// collection's declared version — all of which would otherwise surface as
// unreadable records much later.
func Migrate[T Entity](c *Collection[T], from int, fn MigrateFunc) {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()

	if c.db.opened {
		panic("datastore: Migrate(" + c.name + ") after Open; declare migrations first")
	}
	if fn == nil {
		panic("datastore: Migrate(" + c.name + ") with a nil function")
	}
	if from < 0 {
		panic("datastore: Migrate(" + c.name + ") from a negative version")
	}
	if from >= c.version {
		panic(fmt.Sprintf(
			"datastore: Migrate(%s, %d) is at or above the declared version %d; "+
				"raise the version passed to Register", c.name, from, c.version))
	}
	if c.migrations == nil {
		c.migrations = make(map[int]MigrateFunc)
	}
	if _, dup := c.migrations[from]; dup {
		panic(fmt.Sprintf("datastore: Migrate(%s, %d) declared twice", c.name, from))
	}
	c.migrations[from] = fn
}

// migrateRaw carries one record from version `from` up to the declared version.
func (c *Collection[T]) migrateRaw(from int, raw json.RawMessage) (json.RawMessage, error) {
	for v := from; v < c.version; v++ {
		fn, ok := c.migrations[v]
		if !ok {
			return nil, fmt.Errorf(
				"datastore: %s holds version %d records but no migration from version %d is declared",
				c.name, from, v)
		}
		next, err := fn(raw)
		if err != nil {
			return nil, fmt.Errorf("datastore: migrate %s v%d->v%d: %w", c.name, v, v+1, err)
		}
		if len(next) == 0 {
			return nil, fmt.Errorf("datastore: migrate %s v%d->v%d returned empty JSON", c.name, v, v+1)
		}
		raw = next
	}
	return raw, nil
}

func (c *Collection[T]) schemaVersion() int { return c.version }

// checkVersion rejects data written by a newer build than this one.
func (c *Collection[T]) checkVersion(onDisk int) error {
	if onDisk > c.version {
		return fmt.Errorf(
			"datastore: %s on disk is schema version %d but this build declares %d; "+
				"refusing to open data written by a newer version",
			c.name, onDisk, c.version)
	}
	return nil
}
