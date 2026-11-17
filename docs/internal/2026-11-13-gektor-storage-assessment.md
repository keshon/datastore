# GEKTOR storage assessment — 2026-11-13

Reviewed local commit `77da107` on Windows with Go 1.26.4. This is investigation evidence,
not a claim about a later version. No datastore implementation changes were
made. GEKTOR will use SQLite through a driver that does not require cgo.

## Blocking finding: concurrent unique inserts can both succeed

Severity: high. Under default `SyncAlways`, two concurrent writes with distinct
record keys and the same unique-index value can both return nil. `Verify()`
then reports duplicate records and an inconsistent unique index.

An external consumer probe reproduced this in four of four runs, each in its
first round: 16 concurrent inserts, two accepted, exactly one expected. The
probe used only exported APIs and did not alter package internals.

Root cause: `flushLocked` moves `g.pending` into a local batch and clears the
shared slice before synchronization and application (`groupcommit.go`, around
lines 131–133). `pendingOpsFor` examines only `g.pending` (around line 206).
Uniqueness validation therefore misses the batch currently being flushed,
which is not yet in the committed indexes either.

Keep all appended-but-unapplied frames visible to constraint checking,
including the flushing batch. Add a deterministic regression that pauses a
flush, attempts a conflicting insert, then completes the flush. Check both
return values, record count, index lookup, `Verify()` and reopen behavior.
Cover pending deletes and changes of unique terms as well as new inserts.

## Reproduce independently

Create a temporary module requiring `github.com/keshon/datastore`, with a
`replace` pointing at this checkout. Save this as `main.go` and run `go run .`.
The temporary state directory is removed after the probe. Scheduling affects
whether a particular run reaches the window; this is evidence, not the proposed
deterministic regression test.

```go
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/keshon/datastore"
)

type record struct {
	ID    string `json:"id"`
	Token string `json:"token"`
}

func (r *record) Key() string { return r.ID }

func main() {
	err := run()
	if err != nil {
		panic(err)
	}
}

func run() error {
	dir, err := os.MkdirTemp("", "datastore-unique-probe-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	db := datastore.New(datastore.Options{
		Dir:               filepath.Join(dir, "state"),
		CompactInterval:   datastore.CompactNever,
		CompactAfterBytes: 1 << 60,
	})
	records := datastore.Register[*record](db, "records")
	datastore.AddUnique(records, "token", func(r *record) string { return r.Token })
	err = db.Open()
	if err != nil {
		return err
	}
	defer db.Close()

	for round := range 100 {
		start := make(chan struct{})
		var workers sync.WaitGroup
		var accepted atomic.Int32
		for worker := range 16 {
			workers.Go(func() {
				<-start
				r := &record{
					ID:    fmt.Sprintf("%d/%d", round, worker),
					Token: fmt.Sprintf("token-%d", round),
				}
				putErr := records.Put(r)
				if putErr == nil {
					accepted.Add(1)
				}
			})
		}
		close(start)
		workers.Wait()
		if accepted.Load() > 1 {
			verification := db.Verify()
			return fmt.Errorf("round=%d accepted=%d; verify: %v", round, accepted.Load(), verification)
		}
	}
	return db.Verify()
}
```

Observed output included:

```text
round=0 accepted=2 (expected exactly 1)
datastore: integrity check failed with 2 problems:
duplicate term also held by another record; the unique constraint is violated
index points at the other record instead
```

## Fit for GEKTOR

Datastore has useful foundations: typed collections, atomic cross-collection
updates, checksummed WAL framing, acknowledgment after flush in `SyncAlways`,
recovery reports, snapshot checksums, and WAL fault-injection tests.

Its operating envelope differs from the planned workflow store:

- One process owns the database for writing. GEKTOR currently allows daemon,
  CLI and review commands to open storage independently. Adoption would require
  a shared in-process owner and a protocol for other processes to mutate state.
- Records and indexes remain in RAM. Full compaction serializes the dataset
  under the state lock and consumes additional memory. Growing invocation
  history requires retention and workload-specific capacity measurements.
- `OpenReader` loads a view that does not refresh after later commits. It is
  not a live view suitable for polling without reopening.
- Windows `syncDir` is a no-op. Snapshot installation lacks the explicit
  directory synchronization barrier used on Unix. WAL fault injection does
  not establish snapshot metadata durability under physical power loss.
- Watch delivery can drop events. Durable workflow wakeups must use stored
  state rather than treating watches as a durable task queue.

These are design constraints, separate from the reproduced uniqueness bug.
Fixing that bug does not remove the constraints. No comparative throughput or
memory benchmark was performed, so no SQLite speed advantage is claimed.

## Verification and decision

`go test -race ./...` passed (root package: 52.900 seconds); `go vet ./...`
passed. The separate consumer probe still exposed the logical concurrency bug.
The exhaustive sweep and fuzz campaigns were not run during this assessment.

Use SQLite for GEKTOR run/invocation/attempt and queue metadata. Keep artifact
bytes in files. Datastore remains a candidate for bounded application state
owned by a single long-lived process after the uniqueness defect is fixed.

SQLite references: [WAL concurrency](https://sqlite.org/wal.html) and
[atomic commit](https://sqlite.org/atomiccommit.html). SQLite still serializes
writers; adopting it does not remove the need for short transactions, explicit
effect checkpoints, cancellation handling or crash tests.
