package datastore

// Crash injection.
//
// Everything else in this package tests what happens when things go right. This
// file tests the only question that actually matters for a store holding
// orders: after the process dies at an arbitrary moment, is every commit that
// was acknowledged still there, and is nothing that was not acknowledged
// there?
//
// Three angles:
//
//   - TestRecoveryFromTruncatedLog cuts the log at every byte offset and checks
//     recovery against a model. Deterministic and exhaustive; this is the one
//     that finds real bugs.
//   - TestCrashInjection kills a live child process mid-write and verifies that
//     every commit it had acknowledged on stdout survived. Slower and less
//     precise, but it exercises the actual fsync path.
//   - FuzzFrameParsing throws garbage at the frame reader.
//
// Without these, this package is a JSON file with extra steps.

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// checkIndexes recomputes every index by brute force and compares.
//
// This is the assertion that catches index drift — a record findable under a
// value it no longer has, or missing from a term it should carry. Drift is
// invisible until someone runs the query that misses.
func checkIndexes(t *testing.T, f *fixture) {
	t.Helper()

	wantStatus := map[string][]string{}
	wantTag := map[string][]string{}
	wantEmail := map[string]string{}
	var wantDate []sortedEntry

	for o := range f.orders.All() {
		wantStatus[o.Status] = append(wantStatus[o.Status], o.ID)
		for _, tag := range o.Tags {
			wantTag[tag] = append(wantTag[tag], o.ID)
		}
		if o.Email != "" {
			if holder, dup := wantEmail[o.Email]; dup {
				t.Fatalf("model is inconsistent: %q held by both %q and %q", o.Email, holder, o.ID)
			}
			wantEmail[o.Email] = o.ID
		}
		wantDate = append(wantDate, sortedEntry{n: o.Created, key: o.ID})
	}

	for term, want := range wantStatus {
		sort.Strings(want)
		if got := ids(f.byStatus.Get(term)); got != strings.Join(want, ",") {
			t.Errorf("status index %q = %q, brute force says %q", term, got, strings.Join(want, ","))
		}
	}
	for term, want := range wantTag {
		sort.Strings(want)
		if got := ids(f.byTag.Get(term)); got != strings.Join(want, ",") {
			t.Errorf("tag index %q = %q, brute force says %q", term, got, strings.Join(want, ","))
		}
	}
	// Stale terms are as wrong as missing ones.
	for _, term := range f.byStatus.Terms() {
		if _, ok := wantStatus[term]; !ok {
			t.Errorf("status index has a stale term %q", term)
		}
	}
	for _, term := range f.byTag.Terms() {
		if _, ok := wantTag[term]; !ok {
			t.Errorf("tag index has a stale term %q", term)
		}
	}
	for email, want := range wantEmail {
		got, ok := f.byEmail.Get(email)
		if !ok || got.ID != want {
			t.Errorf("unique index %q = %v/%v, brute force says %q", email, got, ok, want)
		}
	}

	sort.Slice(wantDate, func(i, j int) bool {
		if wantDate[i].n != wantDate[j].n {
			return wantDate[i].n < wantDate[j].n
		}
		return wantDate[i].key < wantDate[j].key
	})
	wantIDs := make([]string, len(wantDate))
	for i, e := range wantDate {
		wantIDs[i] = e.key
	}
	if got := ids(f.byDate.Asc(0, 0)); got != strings.Join(wantIDs, ",") {
		t.Errorf("sorted index = %q, brute force says %q", got, strings.Join(wantIDs, ","))
	}
}

// commitPlan is one transaction in a reproducible workload.
type commitPlan struct {
	put *order
	del string
}

func buildWorkload(seed int64, n int) []commitPlan {
	rng := rand.New(rand.NewSource(seed))
	statuses := []string{"new", "paid", "shipped", "cancelled"}
	tags := []string{"gift", "urgent", "fragile"}

	var plans []commitPlan
	var live []string

	for i := 0; i < n; i++ {
		if len(live) > 3 && rng.Intn(4) == 0 {
			victim := live[rng.Intn(len(live))]
			plans = append(plans, commitPlan{del: victim})
			for j, id := range live {
				if id == victim {
					live = append(live[:j], live[j+1:]...)
					break
				}
			}
			continue
		}
		id := fmt.Sprintf("o%03d", i)
		if len(live) > 0 && rng.Intn(3) == 0 {
			id = live[rng.Intn(len(live))] // update an existing record
		} else {
			live = append(live, id)
		}
		o := &order{
			ID:       id,
			Customer: fmt.Sprintf("c%d", rng.Intn(5)),
			Status:   statuses[rng.Intn(len(statuses))],
			Email:    fmt.Sprintf("%s@example.com", id), // unique per record
			Created:  int64(rng.Intn(1000)),
			Total:    rng.Intn(10000),
		}
		if rng.Intn(2) == 0 {
			o.Tags = []string{tags[rng.Intn(len(tags))]}
		}
		plans = append(plans, commitPlan{put: o})
	}
	return plans
}

// cutPoints chooses where to truncate the log.
//
// Cutting at every single byte is the strongest check and takes a couple of
// minutes, so it is opt-in via DS_FULL_SWEEP=1 and belongs in CI. The default
// aims at the offsets where behaviour actually changes: around each frame
// boundary — mid-header, mid-payload, exactly on the seam — plus a uniform
// sample so nothing between frames goes unexamined.
//
// Uniform sampling alone would be worse at a fraction of the cost saved: the
// bugs live at the seams.
func cutPoints(full []byte) []int {
	if os.Getenv("DS_FULL_SWEEP") == "1" {
		cuts := make([]int, 0, len(full))
		for c := walHeaderSize; c <= len(full); c++ {
			cuts = append(cuts, c)
		}
		return cuts
	}

	seen := map[int]bool{}
	add := func(c int) {
		if c >= walHeaderSize && c <= len(full) && !seen[c] {
			seen[c] = true
		}
	}

	// Walk the frames to find the seams.
	for off := walHeaderSize; off+frameHeaderSize <= len(full); {
		length := int(binary.LittleEndian.Uint32(full[off:]))
		if length <= 0 || off+frameHeaderSize+length > len(full) {
			break
		}
		end := off + frameHeaderSize + length
		for _, d := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, length / 2} {
			add(off + d) // inside this frame's header, then its payload
			add(end - d) // just short of the seam
		}
		add(end)
		off = end
	}

	stride := 37
	if testing.Short() {
		stride = 211
	}
	for c := walHeaderSize; c <= len(full); c += stride {
		add(c)
	}
	add(len(full))

	cuts := make([]int, 0, len(seen))
	for c := range seen {
		cuts = append(cuts, c)
	}
	sort.Ints(cuts)
	return cuts
}

// model applies the first k commits the way the store should have.
func model(plans []commitPlan, k int) map[string]*order {
	state := map[string]*order{}
	for i := 0; i < k && i < len(plans); i++ {
		if plans[i].del != "" {
			delete(state, plans[i].del)
			continue
		}
		cp := *plans[i].put
		state[cp.ID] = &cp
	}
	return state
}

// TestRecoveryFromTruncatedLog is the core durability test.
//
// It writes a known workload, then recovers from the log cut short at many
// offsets — concentrated around frame boundaries, and every single byte under
// DS_FULL_SWEEP=1. Three things must hold at every cut:
//
//   - the recovered state equals exactly the prefix of commits the store says
//     it applied, so no transaction is ever half-applied;
//   - that count never decreases as the cut moves later, so a longer log never
//     yields less data;
//   - every index matches a brute-force recomputation.
func TestRecoveryFromTruncatedLog(t *testing.T) {
	const commits = 60

	plans := buildWorkload(1, commits)

	// Build the reference log once.
	src := t.TempDir()
	f := openFixture(t, src, func(o *Options) { o.CompactAfterBytes = 1 << 40 })
	for _, p := range plans {
		err := f.db.Update(func(tx *Tx) error {
			c := In(tx, f.orders)
			if p.del != "" {
				return c.Delete(p.del)
			}
			return c.Put(p.put)
		})
		if err != nil {
			t.Fatalf("building workload: %v", err)
		}
	}
	release(t, f.db)

	full, err := os.ReadFile(filepath.Join(src, walName))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("reference log is %d bytes for %d commits", len(full), commits)

	cuts := cutPoints(full)
	t.Logf("testing %d cut points", len(cuts))

	lastRecovered := -1
	for _, cut := range cuts {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, walName), full[:cut], 0o644); err != nil {
			t.Fatal(err)
		}

		g := openFixture(t, dir, func(o *Options) { o.CompactAfterBytes = 1 << 40 })

		// Ask the store how many commits it applied rather than inferring it
		// from the state. Inference is ambiguous: a workload that adds a record
		// and then deletes it passes through the same state twice, so "the
		// first prefix that matches" can report a smaller number for a longer
		// log and look like a regression that is not there.
		//
		// Seq is the count directly, which also makes the assertion stricter:
		// the state must equal exactly that prefix, not merely some prefix.
		recovered := int(g.db.Stats().Seq)
		if recovered > len(plans) {
			t.Fatalf("cut at %d: recovered seq %d exceeds the %d commits written",
				cut, recovered, len(plans))
		}
		if !sameState(model(plans, recovered), g.orders) {
			t.Fatalf("cut at %d: store reports %d commits but its state does not match "+
				"that prefix — a partial transaction was applied", cut, recovered)
		}
		if recovered < lastRecovered {
			t.Fatalf("cut at %d: recovered %d commits, but a shorter log recovered %d",
				cut, recovered, lastRecovered)
		}
		lastRecovered = recovered
		checkIndexes(t, g)

		// Release without Close: a full Close compacts, and writing a snapshot
		// on each of ten thousand iterations dominates the runtime for no extra
		// coverage. Close-after-recovery is exercised in the crash-injection
		// rounds and in TestStateSurvivesReopen.
		release(t, g.db)
	}

	if lastRecovered != commits {
		t.Errorf("the intact log recovered %d commits, want %d", lastRecovered, commits)
	}
}

func sameState(want map[string]*order, got *Collection[*order]) bool {
	if len(want) != got.Len() {
		return false
	}
	for id, w := range want {
		g, ok := got.Get(id)
		if !ok {
			return false
		}
		if g.Status != w.Status || g.Total != w.Total || g.Created != w.Created ||
			g.Email != w.Email || g.Customer != w.Customer ||
			strings.Join(g.Tags, ",") != strings.Join(w.Tags, ",") {
			return false
		}
	}
	return true
}

// A frame whose payload is corrupted in place — not truncated — must be
// rejected by its checksum, and everything before it must survive.
func TestCorruptedFrameIsRejected(t *testing.T) {
	dir := t.TempDir()
	f := openFixture(t, dir, func(o *Options) { o.CompactAfterBytes = 1 << 40 })
	for i := 0; i < 10; i++ {
		mustPut(t, f.orders, mkOrder(fmt.Sprintf("o%d", i)))
	}
	release(t, f.db)

	path := filepath.Join(dir, walName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a bit in the middle, well past the first frame.
	victim := len(raw) / 2
	raw[victim] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	g := openFixture(t, dir, func(o *Options) { o.CompactAfterBytes = 1 << 40 })
	if g.orders.Len() == 0 {
		t.Error("corruption in the middle discarded everything before it")
	}
	if g.orders.Len() == 10 {
		t.Error("a corrupted frame was applied anyway")
	}
	checkIndexes(t, g)
}

// A commit appends and fsyncs its frame before taking the state lock to apply
// it. A compaction that excluded only readers could slip into that gap,
// snapshot the older state, truncate the log, and destroy an already-
// acknowledged transaction.
//
// This drives a compaction into the gap deliberately, on every single commit.
// An earlier version of this test just ran a compactor in a tight loop and
// hoped to land there; it passed with the bug still present, which is worse
// than no test at all.
func TestCompactionCannotInterleaveWithACommit(t *testing.T) {
	dir := t.TempDir()
	f := openFixture(t, dir, func(o *Options) { o.CompactAfterBytes = 1 << 40 })

	const commits = 40
	var interleaved int

	testHookAfterAppend = func() {
		// Compact from another goroutine: this one holds the writer slot, and
		// with the fix in place Compact must wait for it. If it completes
		// anyway, a compaction ran inside the window and the frame just
		// fsynced has been erased.
		done := make(chan struct{})
		go func() {
			_ = f.db.Compact()
			close(done)
		}()
		select {
		case <-done:
			interleaved++
		case <-time.After(75 * time.Millisecond):
			// Correctly blocked behind this commit.
		}
	}
	t.Cleanup(func() { testHookAfterAppend = nil })

	var acked []string
	for i := 0; i < commits; i++ {
		id := fmt.Sprintf("o%03d", i)
		err := f.db.Update(func(tx *Tx) error {
			return In(tx, f.orders).Put(&order{
				ID: id, Status: "new", Email: id + "@example.com", Created: int64(i),
			})
		})
		if err != nil {
			t.Fatalf("Update(%s): %v", id, err)
		}
		acked = append(acked, id)
	}

	testHookAfterAppend = nil
	if interleaved > 0 {
		t.Errorf("%d compactions ran inside the post-fsync window; "+
			"Compact must hold the writer slot", interleaved)
	}

	release(t, f.db) // no clean shutdown: only what reached disk counts

	g := openFixture(t, dir, func(o *Options) { o.CompactAfterBytes = 1 << 40 })
	missing := 0
	for _, id := range acked {
		if _, ok := g.orders.Get(id); !ok {
			missing++
			if missing <= 5 {
				t.Errorf("commit %q was acknowledged but did not survive", id)
			}
		}
	}
	if missing > 5 {
		t.Errorf("%d of %d acknowledged commits lost in total", missing, len(acked))
	}
	checkIndexes(t, g)
	t.Logf("%d commits, %d lost, %d interleaved compactions", len(acked), missing, interleaved)
}

// The same property under ordinary concurrency rather than a forced schedule:
// four writers against a compactor spinning as fast as it can.
func TestConcurrentCompactionDoesNotLoseCommits(t *testing.T) {
	dir := t.TempDir()
	f := openFixture(t, dir, func(o *Options) { o.CompactAfterBytes = 1 << 40 })

	const writers, perWriter = 4, 80

	var (
		mu        sync.Mutex
		acked     []string
		writeWG   sync.WaitGroup
		compactWG sync.WaitGroup
	)
	stop := make(chan struct{})

	// Deliberately a separate WaitGroup: the compactor runs until it is told to
	// stop, and it is told to stop only once the writers are done. Sharing one
	// group would wait for a goroutine that cannot finish yet.
	compactWG.Add(1)
	go func() {
		defer compactWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := f.db.Compact(); err != nil {
				t.Errorf("Compact: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	for w := 0; w < writers; w++ {
		writeWG.Add(1)
		go func(w int) {
			defer writeWG.Done()
			for i := 0; i < perWriter; i++ {
				id := fmt.Sprintf("w%d-%03d", w, i)
				err := f.db.Update(func(tx *Tx) error {
					return In(tx, f.orders).Put(&order{
						ID: id, Status: "new", Email: id + "@example.com", Created: int64(i),
					})
				})
				if err != nil {
					t.Errorf("Update(%s): %v", id, err)
					return
				}
				// Only recorded after Update returned, so the store has
				// claimed this one is durable.
				mu.Lock()
				acked = append(acked, id)
				mu.Unlock()
			}
		}(w)
	}

	writeWG.Wait()
	close(stop)
	compactWG.Wait()

	release(t, f.db) // no clean shutdown: recovery must rely on what is on disk

	g := openFixture(t, dir, func(o *Options) { o.CompactAfterBytes = 1 << 40 })
	missing := 0
	for _, id := range acked {
		if _, ok := g.orders.Get(id); !ok {
			missing++
			if missing <= 5 {
				t.Errorf("commit %q was acknowledged but did not survive compaction", id)
			}
		}
	}
	if missing > 5 {
		t.Errorf("%d of %d acknowledged commits lost in total", missing, len(acked))
	}
	checkIndexes(t, g)
	t.Logf("%d acknowledged commits, %d lost", len(acked), missing)
}

// TestCrashInjection kills a real child process mid-write.
//
// The child prints an id only after Update has returned, so every id the parent
// reads is a commit the store claimed was durable. All of them must survive.
func TestCrashInjection(t *testing.T) {
	if os.Getenv("DS_CRASH_CHILD") == "1" {
		runCrashChild()
		return
	}
	if testing.Short() {
		t.Skip("crash injection is slow; run without -short")
	}

	rounds := 8
	if v := os.Getenv("DS_CRASH_ROUNDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			rounds = n
		}
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for round := 0; round < rounds; round++ {
		dir := t.TempDir()

		cmd := exec.Command(os.Args[0], "-test.run=TestCrashInjection", "-test.timeout=60s")
		cmd.Env = append(os.Environ(), "DS_CRASH_CHILD=1", "DS_CRASH_DIR="+dir)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		acked := make(chan string, 4096)
		go func() {
			sc := bufio.NewScanner(stdout)
			for sc.Scan() {
				if id, ok := strings.CutPrefix(sc.Text(), "ACK "); ok {
					acked <- id
				}
			}
			close(acked)
		}()

		// Let it get going, then kill it at an arbitrary moment.
		time.Sleep(time.Duration(30+rng.Intn(220)) * time.Millisecond)
		_ = cmd.Process.Kill()
		_ = cmd.Wait()

		var want []string
		for id := range acked {
			want = append(want, id)
		}
		if len(want) == 0 {
			t.Logf("round %d: child died before acknowledging anything, skipping", round)
			continue
		}

		g := openFixture(t, dir, func(o *Options) { o.CompactAfterBytes = 1 << 40 })
		missing := 0
		for _, id := range want {
			if _, ok := g.orders.Get(id); !ok {
				missing++
				if missing <= 3 {
					t.Errorf("round %d: commit %q was acknowledged but is gone after the kill", round, id)
				}
			}
		}
		if missing > 3 {
			t.Errorf("round %d: %d acknowledged commits lost in total", round, missing)
		}
		checkIndexes(t, g)
		if err := g.db.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("round %d: %d acknowledged commits, all present", round, len(want))
	}
}

// runCrashChild commits in a tight loop until it is killed.
func runCrashChild() {
	dir := os.Getenv("DS_CRASH_DIR")

	db := New(Options{Dir: dir, Sync: SyncAlways, CompactInterval: -1, CompactAfterBytes: 1 << 40})
	orders := Register[*order](db, "orders")
	_ = AddIndex(orders, "status", func(o *order) []string { return []string{o.Status} })
	_ = AddIndex(orders, "tag", func(o *order) []string { return o.Tags })
	_ = AddUnique(orders, "email", func(o *order) string { return o.Email })
	_ = AddSorted(orders, "date", func(o *order) int64 { return o.Created })

	if err := db.Open(); err != nil {
		fmt.Fprintln(os.Stderr, "child open:", err)
		os.Exit(1)
	}

	w := bufio.NewWriter(os.Stdout)
	for i := 0; ; i++ {
		id := fmt.Sprintf("o%06d", i)
		err := db.Update(func(tx *Tx) error {
			return In(tx, orders).Put(&order{
				ID:      id,
				Status:  "new",
				Email:   id + "@example.com",
				Created: int64(i),
				Total:   i,
			})
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "child update:", err)
			os.Exit(1)
		}
		// Only after Update returned: the store has claimed this is durable.
		fmt.Fprintln(w, "ACK", id)
		if err := w.Flush(); err != nil {
			os.Exit(0) // parent closed the pipe
		}
	}
}

func FuzzFrameParsing(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0})
	f.Add([]byte(`{"seq":1,"ops":[]}`))

	f.Fuzz(func(t *testing.T, tail []byte) {
		dir := t.TempDir()

		body := make([]byte, 0, walHeaderSize+len(tail))
		body = append(body, walMagic...)
		body = append(body, 1, 0) // version, little endian
		body = append(body, tail...)
		if err := os.WriteFile(filepath.Join(dir, walName), body, 0o644); err != nil {
			t.Fatal(err)
		}

		// The contract is simply: never panic, never hang, never allocate the
		// world because a length prefix said so.
		db := New(Options{Dir: dir, Sync: SyncNever, CompactInterval: -1})
		_ = Register[*order](db, "orders")
		if err := db.Open(); err != nil {
			return
		}
		_ = db.Close()
	})
}

var _ = json.Marshal // keep the import if the fuzz corpus changes
