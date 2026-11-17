package datastore

import (
	"fmt"
	"iter"
	"slices"
	"sort"
)

// Indexes are derived from records and rebuilt during recovery, never persisted.

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
// The extractor must not mutate its record or later change returned term slices.
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
	if slices.Equal(terms, i.byKey[key]) {
		return
	}
	i.remove(key)
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
// All iterates every record carrying the term, in key order.
//
// Find is the same query returning a slice: it decodes every match before it
// returns any of them, which for a broad term is most of the cost. This decodes
// one record per step, so breaking out early stops the work. Prefer it whenever
// the loop might not run to the end.
//
//	for o := range byStatus.All("new") {
//	    if o.Total > limit {
//	        return o, nil // the rest is never decoded
//	    }
//	}
//
// The read lock is held for the whole iteration, so the body must not write.
func (i *Index[T]) All(term string) iter.Seq[T] {
	return func(yield func(T) bool) {
		i.coll.db.mu.RLock()
		defer i.coll.db.mu.RUnlock()
		i.iterateLocked(term, yield)
	}
}

// iterateLocked yields matches in key order. The caller holds the read lock.
func (i *Index[T]) iterateLocked(term string, yield func(T) bool) {
	set := i.byTerm[term]
	if len(set) == 0 {
		return
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v, ok := i.coll.getLocked(k)
		if !ok {
			continue
		}
		if !yield(v) {
			return
		}
	}
}

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
// The extractor receives an internal record and must not mutate it.
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
	if old, exists := u.byKey[key]; exists && old == t {
		return
	}
	u.remove(key)
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
// O(n) when an indexed value changes because of the shift. Unchanged values
// stay in place; recovery accumulates the final entries and sorts once.
type SortedIndex[T Entity] struct {
	coll *Collection[T]
	name string
	key  func(T) int64

	entries    []sortedEntry
	byKey      map[string]int64
	rebuilding bool // restore accumulates final terms, then sorts once
}

// AddSorted declares a sorted index. Must be called before Open.
// The extractor receives an internal record and must not mutate it.
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
	if s.rebuilding {
		s.byKey[key] = n
		return
	}
	if old, exists := s.byKey[key]; exists && old == n {
		return
	}
	s.remove(key)
	pos := s.search(n, key)
	s.entries = append(s.entries, sortedEntry{})
	copy(s.entries[pos+1:], s.entries[pos:])
	s.entries[pos] = sortedEntry{n: n, key: key}
	s.byKey[key] = n
}

func (s *SortedIndex[T]) remove(key string) {
	if s.rebuilding {
		delete(s.byKey, key)
		return
	}
	n, ok := s.byKey[key]
	if !ok {
		return
	}
	if pos := s.search(n, key); pos < len(s.entries) && s.entries[pos].key == key {
		copy(s.entries[pos:], s.entries[pos+1:])
		last := len(s.entries) - 1
		s.entries[last] = sortedEntry{} // release the removed key's backing string
		s.entries = s.entries[:last]
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
// All iterates records whose key falls in [lo, hi], ascending.
//
// The lazy form of Range: entries are decoded one at a time, so a loop that
// stops early does not pay for the rest of the window. The read lock is held
// for the whole iteration, so the body must not write.
func (s *SortedIndex[T]) All(lo, hi int64) iter.Seq[T] {
	return func(yield func(T) bool) {
		s.coll.db.mu.RLock()
		defer s.coll.db.mu.RUnlock()
		s.iterateLocked(lo, hi, yield)
	}
}

// iterateLocked yields the window in ascending order. The caller holds the
// read lock.
func (s *SortedIndex[T]) iterateLocked(lo, hi int64, yield func(T) bool) {
	if lo > hi {
		return
	}
	start := s.search(lo, "")
	end := s.search(hi+1, "")
	if hi == 1<<63-1 {
		end = len(s.entries)
	}
	for _, e := range s.entries[start:end] {
		v, ok := s.coll.getLocked(e.key)
		if !ok {
			continue
		}
		if !yield(v) {
			return
		}
	}
}

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

func (i *Index[T]) verifyRecord(key string, v T, seen map[string]struct{}) (int, []string) {
	var problems []string
	terms := i.terms(v)
	if !slices.Equal(terms, i.byKey[key]) {
		problems = append(problems, describeIndexDrift(i.coll.name, i.name, "", key, "reverse terms disagree with the record"))
	}
	clear(seen)
	links := 0
	for _, term := range terms {
		if len(terms) > 1 {
			if _, exists := seen[term]; exists {
				continue
			}
			seen[term] = struct{}{}
		}
		links++
		if _, ok := i.byTerm[term][key]; !ok {
			problems = append(problems, describeIndexDrift(i.coll.name, i.name, term, key, "missing from the index"))
		}
	}
	return links, problems
}

func (i *Index[T]) verify(expectedLinks int) []string {
	var problems []string
	for key := range i.byKey {
		if _, exists := i.coll.data[key]; !exists {
			problems = append(problems, describeIndexDrift(i.coll.name, i.name, "", key, "reverse index points at a missing record"))
		}
	}
	links := 0
	for term, keys := range i.byTerm {
		if len(keys) == 0 {
			problems = append(problems, describeIndexDrift(i.coll.name, i.name, term, "", "term has no records"))
		}
		links += len(keys)
	}
	// Every expected link was checked against the records. Equal cardinality
	// then proves there are no extra links, without quadratic scans of tag lists.
	if links != expectedLinks {
		problems = append(problems, fmt.Sprintf("%s.%s: index holds %d links, records imply %d", i.coll.name, i.name, links, expectedLinks))
	}
	return problems
}

func (u *UniqueIndex[T]) verifyRecord(key string, v T) []string {
	var problems []string
	term := u.term(v)
	reverse, indexed := u.byKey[key]
	if term == "" {
		if indexed {
			problems = append(problems, describeIndexDrift(u.coll.name, u.name, reverse, key, "record should not be indexed"))
		}
		return problems
	}
	if !indexed || reverse != term {
		problems = append(problems, describeIndexDrift(u.coll.name, u.name, term, key, "reverse term disagrees with the record"))
	}
	owner, exists := u.byTerm[term]
	if exists && owner == key {
		return problems
	}
	detail := "index points at " + owner + " instead"
	if raw, found := u.coll.data[owner]; found && owner != key {
		other, err := u.coll.decode(raw)
		if err == nil && u.term(other) == term {
			detail = "duplicate term also held by " + owner + "; the unique constraint is violated in the data"
		}
	}
	problems = append(problems, describeIndexDrift(u.coll.name, u.name, term, key, detail))
	return problems
}

func (u *UniqueIndex[T]) verify() []string {
	var problems []string
	for key := range u.byKey {
		if _, exists := u.coll.data[key]; !exists {
			problems = append(problems, describeIndexDrift(u.coll.name, u.name, "", key, "reverse index points at a missing record"))
		}
	}
	for term, key := range u.byTerm {
		_, exists := u.coll.data[key]
		if term == "" || !exists || u.byKey[key] != term {
			problems = append(problems, describeIndexDrift(u.coll.name, u.name, term, key, "indexed but no record produces the term"))
		}
	}
	return problems
}

func (s *SortedIndex[T]) verifyRecord(key string, v T) []string {
	want := s.key(v)
	got, exists := s.byKey[key]
	if !exists || got != want {
		return []string{describeIndexDrift(s.coll.name, s.name, "", key, "reverse sort key disagrees with the record")}
	}
	return nil
}

func (s *SortedIndex[T]) verify() []string {
	var problems []string
	if len(s.entries) != len(s.coll.data) {
		problems = append(problems, fmt.Sprintf("%s.%s: index holds %d entries, records imply %d", s.coll.name, s.name, len(s.entries), len(s.coll.data)))
	}
	for key := range s.byKey {
		if _, exists := s.coll.data[key]; !exists {
			problems = append(problems, describeIndexDrift(s.coll.name, s.name, "", key, "reverse index points at a missing record"))
		}
	}
	for i, entry := range s.entries {
		n, indexed := s.byKey[entry.key]
		_, exists := s.coll.data[entry.key]
		if !indexed || !exists || n != entry.n {
			problems = append(problems, describeIndexDrift(s.coll.name, s.name, "", entry.key, "sorted entry disagrees with the record"))
		}
		if i == 0 {
			continue
		}
		previous := s.entries[i-1]
		if previous.n > entry.n || (previous.n == entry.n && previous.key >= entry.key) {
			problems = append(problems, describeIndexDrift(s.coll.name, s.name, "", entry.key, "entries are duplicated or out of order"))
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
	count := n - offset
	if limit > 0 {
		count = min(count, limit)
	}
	out := make([]sortedEntry, 0, count)
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
