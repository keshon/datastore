package datastore

import (
	"fmt"
	"sort"
)

// Indexes are never written to disk. They are rebuilt from the records as the
// snapshot and log are applied, which at these volumes takes about a
// millisecond. Persisting them would add a second thing that can be corrupt,
// and a second thing to keep in step with the first, for no gain.

func mustBeUnopened(db *DB, what string) {
	if db.opened {
		panic("datastore: " + what + " after Open; declare every index first")
	}
}

// --- multi-valued ---------------------------------------------------------

// Index maps a term to every record that produced it. A record may produce
// several terms, or none.
type Index[T Entity] struct {
	coll  *Collection[T]
	name  string
	terms func(T) []string

	byTerm map[string]map[string]struct{}
	byKey  map[string][]string
}

// AddIndex declares a multi-valued index. Must be called before Open.
func AddIndex[T Entity](c *Collection[T], name string, terms func(T) []string) *Index[T] {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	mustBeUnopened(c.db, "AddIndex("+name+")")

	i := &Index[T]{
		coll:   c,
		name:   name,
		terms:  terms,
		byTerm: make(map[string]map[string]struct{}),
		byKey:  make(map[string][]string),
	}
	c.multi = append(c.multi, i)
	return i
}

func (i *Index[T]) add(key string, v T) {
	terms := i.terms(v)
	if len(terms) == 0 {
		return
	}
	i.byKey[key] = terms
	for _, t := range terms {
		set, ok := i.byTerm[t]
		if !ok {
			set = make(map[string]struct{})
			i.byTerm[t] = set
		}
		set[key] = struct{}{}
	}
}

func (i *Index[T]) remove(key string) {
	for _, t := range i.byKey[key] {
		if set, ok := i.byTerm[t]; ok {
			delete(set, key)
			if len(set) == 0 {
				delete(i.byTerm, t)
			}
		}
	}
	delete(i.byKey, key)
}

func (i *Index[T]) clear() {
	i.byTerm = make(map[string]map[string]struct{})
	i.byKey = make(map[string][]string)
}

// Find returns every record carrying the term, in key order.
func (i *Index[T]) Find(term string) []T {
	i.coll.db.mu.RLock()
	defer i.coll.db.mu.RUnlock()
	return i.findLocked(term)
}

// Get returns every record carrying the term, in key order.
//
// Deprecated: renamed to Find. Get elsewhere in this package means "one record,
// by its key or its unique term", and a multi-valued lookup is neither.
func (i *Index[T]) Get(term string) []T { return i.Find(term) }

func (i *Index[T]) findLocked(term string) []T {
	set := i.byTerm[term]
	if len(set) == 0 {
		return nil
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]T, 0, len(keys))
	for _, k := range keys {
		if v, ok := i.coll.getLocked(k); ok {
			out = append(out, v)
		}
	}
	return out
}

// Count reports how many records carry the term, without decoding any of them.
func (i *Index[T]) Count(term string) int {
	i.coll.db.mu.RLock()
	defer i.coll.db.mu.RUnlock()
	return len(i.byTerm[term])
}

// Terms lists every term currently present, sorted.
func (i *Index[T]) Terms() []string {
	i.coll.db.mu.RLock()
	defer i.coll.db.mu.RUnlock()
	return i.termsLocked()
}

func (i *Index[T]) termsLocked() []string {
	out := make([]string, 0, len(i.byTerm))
	for t := range i.byTerm {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// --- unique ---------------------------------------------------------------

// UniqueIndex maps a term to at most one record, enforced at commit.
//
// A record whose term function returns "" is simply not indexed. That is how
// an optional unique field — a customer with no email, say — avoids every such
// record colliding with every other.
type UniqueIndex[T Entity] struct {
	coll *Collection[T]
	name string
	term func(T) string

	byTerm map[string]string // term -> record key
	byKey  map[string]string // record key -> term
}

// AddUnique declares a unique index. Must be called before Open.
func AddUnique[T Entity](c *Collection[T], name string, term func(T) string) *UniqueIndex[T] {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	mustBeUnopened(c.db, "AddUnique("+name+")")

	u := &UniqueIndex[T]{
		coll:   c,
		name:   name,
		term:   term,
		byTerm: make(map[string]string),
		byKey:  make(map[string]string),
	}
	c.unique = append(c.unique, u)
	return u
}

func (u *UniqueIndex[T]) add(key string, v T) {
	t := u.term(v)
	if t == "" {
		return
	}
	u.byTerm[t] = key
	u.byKey[key] = t
}

func (u *UniqueIndex[T]) remove(key string) {
	if t, ok := u.byKey[key]; ok {
		if holder, ok := u.byTerm[t]; ok && holder == key {
			delete(u.byTerm, t)
		}
		delete(u.byKey, key)
	}
}

func (u *UniqueIndex[T]) clear() {
	u.byTerm = make(map[string]string)
	u.byKey = make(map[string]string)
}

// Get returns the record holding the term, if any.
func (u *UniqueIndex[T]) Get(term string) (T, bool) {
	u.coll.db.mu.RLock()
	defer u.coll.db.mu.RUnlock()
	return u.getLocked(term)
}

func (u *UniqueIndex[T]) getLocked(term string) (T, bool) {
	key, ok := u.byTerm[term]
	if !ok {
		var zero T
		return zero, false
	}
	return u.coll.getLocked(key)
}

// --- sorted ---------------------------------------------------------------

type sortedEntry struct {
	n   int64
	key string
}

// SortedIndex orders records by an int64 derived from each — a timestamp, a
// price, a numeric id.
//
// Entries live in one sorted slice, maintained by binary insertion. That is
// O(n) per write because of the shift, which is the right trade at a few
// hundred records and a handful of writes a minute; a tree would be more code
// and more ways to be subtly wrong.
type SortedIndex[T Entity] struct {
	coll *Collection[T]
	name string
	key  func(T) int64

	entries []sortedEntry
	byKey   map[string]int64
}

// AddSorted declares a sorted index. Must be called before Open.
func AddSorted[T Entity](c *Collection[T], name string, key func(T) int64) *SortedIndex[T] {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	mustBeUnopened(c.db, "AddSorted("+name+")")

	s := &SortedIndex[T]{coll: c, name: name, key: key, byKey: make(map[string]int64)}
	c.sorted = append(c.sorted, s)
	return s
}

// search finds where (n, key) belongs. Ties break on the record key so the
// order is total and reproducible — otherwise two records sharing a timestamp
// could swap places between restarts.
func (s *SortedIndex[T]) search(n int64, key string) int {
	return sort.Search(len(s.entries), func(i int) bool {
		if s.entries[i].n != n {
			return s.entries[i].n > n
		}
		return s.entries[i].key >= key
	})
}

func (s *SortedIndex[T]) add(key string, v T) {
	n := s.key(v)
	pos := s.search(n, key)
	s.entries = append(s.entries, sortedEntry{})
	copy(s.entries[pos+1:], s.entries[pos:])
	s.entries[pos] = sortedEntry{n: n, key: key}
	s.byKey[key] = n
}

func (s *SortedIndex[T]) remove(key string) {
	n, ok := s.byKey[key]
	if !ok {
		return
	}
	if pos := s.search(n, key); pos < len(s.entries) && s.entries[pos].key == key {
		s.entries = append(s.entries[:pos], s.entries[pos+1:]...)
	}
	delete(s.byKey, key)
}

func (s *SortedIndex[T]) clear() {
	s.entries = nil
	s.byKey = make(map[string]int64)
}

func (s *SortedIndex[T]) materialise(entries []sortedEntry) []T {
	out := make([]T, 0, len(entries))
	for _, e := range entries {
		if v, ok := s.coll.getLocked(e.key); ok {
			out = append(out, v)
		}
	}
	return out
}

// Range returns records whose key falls in [lo, hi], ascending.
func (s *SortedIndex[T]) Range(lo, hi int64) []T {
	s.coll.db.mu.RLock()
	defer s.coll.db.mu.RUnlock()
	return s.rangeLocked(lo, hi)
}

func (s *SortedIndex[T]) rangeLocked(lo, hi int64) []T {
	if lo > hi {
		return nil
	}
	start := s.search(lo, "")
	end := s.search(hi+1, "")
	if hi == 1<<63-1 {
		end = len(s.entries)
	}
	return s.materialise(s.entries[start:end])
}

// Asc returns up to limit records in ascending order, skipping offset.
// A limit of zero means no limit.
func (s *SortedIndex[T]) Asc(limit, offset int) []T {
	s.coll.db.mu.RLock()
	defer s.coll.db.mu.RUnlock()
	return s.materialise(window(s.entries, limit, offset, false))
}

// Desc returns up to limit records in descending order, skipping offset.
// A limit of zero means no limit.
func (s *SortedIndex[T]) Desc(limit, offset int) []T {
	s.coll.db.mu.RLock()
	defer s.coll.db.mu.RUnlock()
	return s.materialise(window(s.entries, limit, offset, true))
}

// Len reports how many records are indexed.
func (s *SortedIndex[T]) Len() int {
	s.coll.db.mu.RLock()
	defer s.coll.db.mu.RUnlock()
	return len(s.entries)
}

// --- verification ---------------------------------------------------------
//
// Each index rebuilds itself from the given records and reports where the live
// structure disagrees. The records are the source of truth: indexes are derived
// state maintained incrementally, and incremental maintenance is exactly the
// kind of thing that can drift silently. See DB.Verify.

func (i *Index[T]) verify(fresh map[string]T) []string {
	want := make(map[string]map[string]struct{})
	for key, v := range fresh {
		for _, t := range i.terms(v) {
			if want[t] == nil {
				want[t] = make(map[string]struct{})
			}
			want[t][key] = struct{}{}
		}
	}

	var problems []string
	for term, keys := range want {
		got := i.byTerm[term]
		for k := range keys {
			if _, ok := got[k]; !ok {
				problems = append(problems, describeIndexDrift(i.coll.collName(), i.name, term, k, "missing from the index"))
			}
		}
	}
	for term, keys := range i.byTerm {
		for k := range keys {
			if _, ok := want[term][k]; !ok {
				problems = append(problems, describeIndexDrift(i.coll.collName(), i.name, term, k, "indexed but not produced by the record"))
			}
		}
	}
	return problems
}

func (u *UniqueIndex[T]) verify(fresh map[string]T) []string {
	var problems []string
	want := make(map[string]string, len(fresh))
	for key, v := range fresh {
		t := u.term(v)
		if t == "" {
			continue
		}
		if other, clash := want[t]; clash {
			problems = append(problems, describeIndexDrift(u.coll.collName(), u.name, t, key,
				"duplicate term also held by "+other+"; the unique constraint is violated in the data"))
			continue
		}
		want[t] = key
	}

	for term, key := range want {
		if got, ok := u.byTerm[term]; !ok || got != key {
			problems = append(problems, describeIndexDrift(u.coll.collName(), u.name, term, key,
				"index points at "+got+" instead"))
		}
	}
	for term, key := range u.byTerm {
		if _, ok := want[term]; !ok {
			problems = append(problems, describeIndexDrift(u.coll.collName(), u.name, term, key,
				"indexed but no record produces the term"))
		}
	}
	return problems
}

func (s *SortedIndex[T]) verify(fresh map[string]T) []string {
	want := make([]sortedEntry, 0, len(fresh))
	for key, v := range fresh {
		want = append(want, sortedEntry{n: s.key(v), key: key})
	}
	sort.Slice(want, func(a, b int) bool {
		if want[a].n != want[b].n {
			return want[a].n < want[b].n
		}
		return want[a].key < want[b].key
	})

	if len(want) != len(s.entries) {
		return []string{fmt.Sprintf("%s.%s: index holds %d entries, records imply %d",
			s.coll.collName(), s.name, len(s.entries), len(want))}
	}
	var problems []string
	for i := range want {
		if want[i] != s.entries[i] {
			problems = append(problems, fmt.Sprintf(
				"%s.%s: position %d holds (%d,%q), records imply (%d,%q); the order has drifted",
				s.coll.collName(), s.name, i, s.entries[i].n, s.entries[i].key, want[i].n, want[i].key))
		}
	}
	return problems
}

func describeIndexDrift(coll, index, term, key, what string) string {
	return fmt.Sprintf("%s.%s: term %q / record %q: %s", coll, index, term, key, what)
}

func window(entries []sortedEntry, limit, offset int, reverse bool) []sortedEntry {
	n := len(entries)
	if offset < 0 {
		offset = 0
	}
	if offset >= n {
		return nil
	}
	out := make([]sortedEntry, 0, n-offset)
	if reverse {
		for i := n - 1 - offset; i >= 0; i-- {
			out = append(out, entries[i])
			if limit > 0 && len(out) == limit {
				break
			}
		}
		return out
	}
	for i := offset; i < n; i++ {
		out = append(out, entries[i])
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out
}
