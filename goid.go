package datastore

import (
	"bytes"
	"runtime"
	"strconv"
)

// goid returns the current goroutine's id, or 0 when it cannot be determined.
//
// Go does not expose goroutine identity, so this parses the header line of
// runtime.Stack ("goroutine 123 [running]:"). That is a hack, and it is
// deliberately confined to one purpose: recognising that the goroutine asking
// for the writer slot is the one already holding it, so a re-entrant Update
// panics immediately with an explanation instead of stalling for WriteTimeout
// and then panicking anyway.
//
// It is never used for correctness — every caller treats 0 as "unknown" and
// falls back to the timeout path. It runs on every writer-slot acquisition to
// record the owner; a single-goroutine runtime.Stack is a microsecond or so,
// which is noise next to the fsync every commit already pays.
func goid() uint64 {
	var buf [64]byte
	b := buf[:runtime.Stack(buf[:], false)]

	b, ok := bytes.CutPrefix(b, []byte("goroutine "))
	if !ok {
		return 0
	}
	i := bytes.IndexByte(b, ' ')
	if i <= 0 {
		return 0
	}
	id, err := strconv.ParseUint(string(b[:i]), 10, 64)
	if err != nil {
		return 0
	}
	return id
}
