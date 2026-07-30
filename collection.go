package datastore

import (
	"encoding/json"
	"fmt"
	"iter"
	"sort"
)

// Entity is anything the store can hold. The key must be stable for the life of
// the record and unique within its collection.
type Entity interface {
	Key() string
}

// anyCollection is the type-erased view the database keeps, since a DB holds
// collections of many different types.
type anyCollection interface {
	collName() string
	applyPut(key string, raw json.RawMessage) error
	applyDelete(key string)
	checkUnique(puts map[string]json.RawMessage, dels map[string]struct{}) error
	dump() map[string]json.RawMessage
	clear()
	count() int
	bytes() int
	schemaVersion() int
	checkVersion(onDisk int) error
	migrateRaw(from int, raw json.RawMessage) (json.RawMessage, error)
	verify() []string
}

// Collection is a typed set of records.
//
// Values are held encoded and decoded on every read. That costs a few
// microseconds at these volumes and buys the elimination of this package's
// worst failure mode: a caller mutating a struct it got from Get, the change
// never reaching the log, and the indexes quietly disagreeing with the data.
// Everything you get back is yours.
type Collection[T Entity] struct {
	db   *DB
	name string
	data map[string]json.RawMessage

	// version is the schema version this build writes; migrations carries each
	// step from an older on-disk version up to it. See migrate.go.
	version    int
	migrations map[int]MigrateFunc

	multi  []*Index[T]
	unique []*UniqueIndex[T]
	sorted []*SortedIndex[T]
}

// Register declares a collection. It must be called before Open, because
// replaying the log needs somewhere to put the records it finds.
//
// Panics on a duplicate name or after Open — both are programming errors that
// would otherwise surface much later as missing data.
func Register[T Entity](db *DB, name string, opts ...CollectionOption) *Collection[T] {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.opened {
		panic("datastore: Register(" + name + ") after Open; register every collection first")
	}
	if _, dup := db.colls[name]; dup {
		panic("datastore: collection " + name + " registered twice")
	}

	var cfg collectionConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	c := &Collection[T]{
		db:      db,
		name:    name,
		data:    make(map[string]json.RawMessage),
		version: cfg.version,
	}
	db.colls[name] = c
	db.order = append(db.order, name)
	return c
}

func (c *Collection[T]) collName() string { return c.name }
func (c *Collection[T]) count() int       { return len(c.data) }

func (c *Collection[T]) decode(raw json.RawMessage) (T, error) {
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		var zero T
		return zero, fmt.Errorf("datastore: decode %s record: %w", c.name, err)
	}
	return v, nil
}

// Get returns a freshly decoded copy of the record, if present.
func (c *Collection[T]) Get(key string) (T, bool) {
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	return c.getLocked(key)
}

func (c *Collection[T]) getLocked(key string) (T, bool) {
	raw, ok := c.data[key]
	if !ok {
		var zero T
		return zero, false
	}
	v, err := c.decode(raw)
	if err != nil {
		// Unreachable in practice: the value round-tripped through Put. If it
		// happens anyway, a record silently missing from reads and index
		// results is the worst way to find out — say so loudly. Verify reports
		// the same record as undecodable.
		c.db.log.Error().Err(err).Str("collection", c.name).Str("key", key).
			Msg("record_undecodable: hidden from reads")
		var zero T
		return zero, false
	}
	return v, true
}

// Len reports how many records the collection holds.
func (c *Collection[T]) Len() int {
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	return len(c.data)
}

// Keys returns every key, sorted, as a snapshot taken under the read lock.
func (c *Collection[T]) Keys() []string {
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()

	keys := make([]string, 0, len(c.data))
	for k := range c.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// All iterates every record in key order.
//
// The read lock is held for the whole iteration, so the body must not write.
// Break out early if you only need a prefix.
func (c *Collection[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		c.db.mu.RLock()
		defer c.db.mu.RUnlock()

		for _, k := range sortedKeys(c.data) {
			v, ok := c.getLocked(k)
			if !ok {
				continue
			}
			if !yield(v) {
				return
			}
		}
	}
}

// Put stores a record as a single-operation transaction.
func (c *Collection[T]) Put(v T) error {
	return c.db.Update(func(tx *Tx) error { return In(tx, c).Put(v) })
}

// Delete removes a record as a single-operation transaction. Removing a key
// that is not present is not an error.
func (c *Collection[T]) Delete(key string) error {
	return c.db.Update(func(tx *Tx) error { return In(tx, c).Delete(key) })
}

// --- index maintenance, called with the write lock held -------------------

func (c *Collection[T]) applyPut(key string, raw json.RawMessage) error {
	v, err := c.decode(raw)
	if err != nil {
		return err
	}
	// Remove the old entry's index terms first; a record that changed its
	// indexed field must not remain findable under the old value.
	c.removeFromIndexes(key)
	c.data[key] = raw
	for _, i := range c.multi {
		i.add(key, v)
	}
	for _, i := range c.unique {
		i.add(key, v)
	}
	for _, i := range c.sorted {
		i.add(key, v)
	}
	return nil
}

func (c *Collection[T]) applyDelete(key string) {
	c.removeFromIndexes(key)
	delete(c.data, key)
}

func (c *Collection[T]) removeFromIndexes(key string) {
	for _, i := range c.multi {
		i.remove(key)
	}
	for _, i := range c.unique {
		i.remove(key)
	}
	for _, i := range c.sorted {
		i.remove(key)
	}
}

func (c *Collection[T]) clear() {
	c.data = make(map[string]json.RawMessage)
	for _, i := range c.multi {
		i.clear()
	}
	for _, i := range c.unique {
		i.clear()
	}
	for _, i := range c.sorted {
		i.clear()
	}
}

// bytes reports the encoded size of the records held, for the memory budget.
// Keys are counted too; the overhead of the maps themselves is not, so this is
// a floor rather than a true heap measurement.
func (c *Collection[T]) bytes() int {
	total := 0
	for k, v := range c.data {
		total += len(k) + len(v)
	}
	return total
}

// verify recomputes what this collection should look like and reports every
// disagreement it finds, in the form of human-readable problems.
//
// It exists because the indexes are derived state maintained incrementally: if
// a bug ever let them drift from the records, nothing else would notice. Here
// the records are the source of truth and the indexes are rebuilt from scratch
// and compared.
func (c *Collection[T]) verify() []string {
	var problems []string

	fresh := make(map[string]T, len(c.data))
	for key, raw := range c.data {
		v, err := c.decode(raw)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s/%s: undecodable: %v", c.name, key, err))
			continue
		}
		if got := v.Key(); got != key {
			problems = append(problems, fmt.Sprintf(
				"%s/%s: record reports key %q; it is stored under a key it does not own", c.name, key, got))
		}
		fresh[key] = v
	}

	for _, i := range c.multi {
		problems = append(problems, i.verify(fresh)...)
	}
	for _, i := range c.unique {
		problems = append(problems, i.verify(fresh)...)
	}
	for _, i := range c.sorted {
		problems = append(problems, i.verify(fresh)...)
	}
	return problems
}

func (c *Collection[T]) dump() map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(c.data))
	for k, v := range c.data {
		out[k] = v
	}
	return out
}

// checkUnique validates a pending change set against every unique index.
//
// It runs at commit, before anything is written, so a transaction may swap a
// unique value between two records — delete-then-insert, or a straight
// exchange — which per-Put validation would reject.
func (c *Collection[T]) checkUnique(puts map[string]json.RawMessage, dels map[string]struct{}) error {
	if len(c.unique) == 0 || len(puts) == 0 {
		return nil
	}

	decoded := make(map[string]T, len(puts))
	for key, raw := range puts {
		v, err := c.decode(raw)
		if err != nil {
			return err
		}
		decoded[key] = v
	}

	for _, idx := range c.unique {
		pending := make(map[string]string, len(decoded))
		for _, key := range sortedKeys(decoded) {
			term := idx.term(decoded[key])
			if term == "" {
				continue // an empty term opts the record out of the index
			}
			if other, clash := pending[term]; clash {
				return fmt.Errorf("%w: %s.%s = %q on both %q and %q",
					ErrUniqueViolation, c.name, idx.name, term, other, key)
			}
			pending[term] = key

			holder, taken := idx.byTerm[term]
			if !taken || holder == key {
				continue
			}
			// The holder is only really in the way if this transaction is not
			// already removing or replacing it.
			if _, gone := dels[holder]; gone {
				continue
			}
			if _, replaced := puts[holder]; replaced {
				continue
			}
			return fmt.Errorf("%w: %s.%s = %q already held by %q",
				ErrUniqueViolation, c.name, idx.name, term, holder)
		}
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
