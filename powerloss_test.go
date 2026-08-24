package datastore

// Power loss.
//
// crash_test.go kills a process with SIGKILL. That proves recovery survives the
// program stopping, but not the machine stopping. After SIGKILL the kernel
// still owns the page cache and writes it out, so every byte the process wrote
// reaches the disk whether it was ever fsynced or not — which means a store
// that never called fsync at all would pass that test.
//
// A power cut does not do that. Bytes written but not synced are gone, and a
// sync that was in flight may have landed only in part. The device below models
// exactly that distinction: it keeps what a running process can see separate
// from what would survive the power going out, and only fsync moves bytes from
// the first to the second.
//
// The contract under test is the only one that matters: if Update returned nil,
// the commit is still there afterwards.
//
// Scope: the device backs the log, which is where acknowledged commits live.
// Snapshots stay on the real filesystem — their integrity is covered by the
// checksum tests in snapshot_test.go, and the ordering that makes compaction
// safe (snapshot durable before the log is truncated) by crash_test.go.

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// errPowerLost is what a handle opened before the cut returns afterwards. The
// machine is gone; a file descriptor from the previous life cannot keep working.
var errPowerLost = errors.New("crashdev: power lost")

// crashDevice is storage that can lose power.
//
// live is what a running process reads back — its own writes included, synced
// or not, exactly as a page cache behaves. durable is what is actually on the
// platter. Sync copies the first onto the second, and that is the only thing
// that does.
type crashDevice struct {
	mu    sync.Mutex
	files map[string]*crashData

	// gen invalidates every handle opened before the last power cut.
	gen int

	// failWrite and failSync fire once, on the next matching call, then clear.
	failWrite error
	failSync  error
}

type crashData struct {
	live    []byte
	durable []byte
}

func newCrashDevice() *crashDevice {
	return &crashDevice{files: make(map[string]*crashData)}
}

// open satisfies logOpener.
func (d *crashDevice) open(path string, create bool) (logFile, int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	data, ok := d.files[path]
	if !ok {
		if !create {
			return nil, 0, &os.PathError{Op: "open", Path: path, Err: os.ErrNotExist}
		}
		data = &crashData{}
		d.files[path] = data
	}
	return &crashFile{dev: d, data: data, gen: d.gen}, int64(len(data.live)), nil
}

// preload seeds a file's durable contents, as though the machine had booted
// with that much of the log on disk.
func (d *crashDevice) preload(path string, durable []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.files[path] = &crashData{
		live:    append([]byte(nil), durable...),
		durable: append([]byte(nil), durable...),
	}
}

// cutPower discards every byte that was written but never synced, and kills
// every open handle.
func (d *crashDevice) cutPower() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, data := range d.files {
		data.live = append([]byte(nil), data.durable...)
	}
	d.gen++
}

// syncNow promotes the file's unsynced bytes, standing in for the interval
// sync goroutine's tick.
func (d *crashDevice) syncNow(path string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if data := d.files[path]; data != nil {
		data.durable = append([]byte(nil), data.live...)
	}
}

// durableBytes is what would be on disk if the power went out now.
func (d *crashDevice) durableBytes(path string) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	if data := d.files[path]; data != nil {
		return append([]byte(nil), data.durable...)
	}
	return nil
}

func (d *crashDevice) setFailSync(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failSync = err
}

func (d *crashDevice) setFailWrite(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failWrite = err
}

// crashFile is one open handle. It dies with the machine.
type crashFile struct {
	dev  *crashDevice
	data *crashData
	gen  int
	pos  int64
	shut bool
}

// checkLocked reports whether this handle may still be used. The caller holds
// dev.mu.
func (f *crashFile) checkLocked() error {
	if f.shut {
		return os.ErrClosed
	}
	if f.gen != f.dev.gen {
		return errPowerLost
	}
	return nil
}

func (f *crashFile) WriteAt(p []byte, off int64) (int, error) {
	f.dev.mu.Lock()
	defer f.dev.mu.Unlock()
	if err := f.checkLocked(); err != nil {
		return 0, err
	}
	if err := f.dev.failWrite; err != nil {
		f.dev.failWrite = nil
		return 0, err
	}
	if end := off + int64(len(p)); int64(len(f.data.live)) < end {
		grown := make([]byte, end)
		copy(grown, f.data.live)
		f.data.live = grown
	}
	copy(f.data.live[off:], p)
	return len(p), nil
}

func (f *crashFile) ReadAt(p []byte, off int64) (int, error) {
	f.dev.mu.Lock()
	defer f.dev.mu.Unlock()
	if err := f.checkLocked(); err != nil {
		return 0, err
	}
	if off >= int64(len(f.data.live)) {
		return 0, io.EOF
	}
	n := copy(p, f.data.live[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *crashFile) Read(p []byte) (int, error) {
	n, err := f.ReadAt(p, f.pos)
	f.pos += int64(n)
	return n, err
}

func (f *crashFile) Seek(offset int64, whence int) (int64, error) {
	f.dev.mu.Lock()
	defer f.dev.mu.Unlock()
	if err := f.checkLocked(); err != nil {
		return 0, err
	}
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		f.pos = int64(len(f.data.live)) + offset
	default:
		return 0, fmt.Errorf("crashdev: bad whence %d", whence)
	}
	return f.pos, nil
}

// Sync is the only operation that makes anything durable.
func (f *crashFile) Sync() error {
	f.dev.mu.Lock()
	defer f.dev.mu.Unlock()
	if err := f.checkLocked(); err != nil {
		return err
	}
	if err := f.dev.failSync; err != nil {
		f.dev.failSync = nil
		return err
	}
	f.data.durable = append([]byte(nil), f.data.live...)
	return nil
}

// Truncate changes what the process sees. It becomes durable at the next Sync,
// which is what the real path does: replay truncates, then syncs.
func (f *crashFile) Truncate(n int64) error {
	f.dev.mu.Lock()
	defer f.dev.mu.Unlock()
	if err := f.checkLocked(); err != nil {
		return err
	}
	switch {
	case n < int64(len(f.data.live)):
		f.data.live = f.data.live[:n]
	case n > int64(len(f.data.live)):
		grown := make([]byte, n)
		copy(grown, f.data.live)
		f.data.live = grown
	}
	return nil
}

// Close does not sync, which is the point: closing a file has never made
// anything durable, on this device or a real one.
func (f *crashFile) Close() error {
	f.dev.mu.Lock()
	defer f.dev.mu.Unlock()
	f.shut = true
	return nil
}

// openFixtureOn opens the standard fixture with its log on dev.
func openFixtureOn(t *testing.T, dir string, dev *crashDevice, opts ...func(*Options)) *fixture {
	t.Helper()
	f := newUnopenedFixture(dir, opts...)
	f.db.openLog = dev.open
	if err := f.db.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = f.db.Close() })
	return f
}

// --- the contract ---------------------------------------------------------

// TestPowerLossKeepsEveryAcknowledgedCommit is the headline. Writers commit
// concurrently, the power goes out at an arbitrary moment, and every commit
// that had been acknowledged before that moment must still be there.
func TestPowerLossKeepsEveryAcknowledgedCommit(t *testing.T) {
	for round := 0; round < 8; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			dir := t.TempDir()
			dev := newCrashDevice()
			f := openFixtureOn(t, dir, dev, func(o *Options) {
				o.CompactAfterBytes = 1 << 40 // never compact: keep it all in the log
			})

			var (
				mu    sync.Mutex
				acked []string
				wg    sync.WaitGroup
			)
			stop := make(chan struct{})

			for w := 0; w < 4; w++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						id := fmt.Sprintf("w%d-%d", worker, i)
						if err := f.orders.Put(mkOrder(id)); err == nil {
							mu.Lock()
							acked = append(acked, id)
							mu.Unlock()
						}
					}
				}(w)
			}

			time.Sleep(time.Duration(5+round*3) * time.Millisecond)
			dev.cutPower()
			close(stop)
			wg.Wait()

			mu.Lock()
			want := append([]string(nil), acked...)
			mu.Unlock()
			if len(want) == 0 {
				t.Skip("no commit was acknowledged before the cut")
			}

			release(t, f.db)

			g := openFixtureOn(t, dir, dev)
			for _, id := range want {
				if _, ok := g.orders.Get(id); !ok {
					t.Fatalf("commit %q was acknowledged and did not survive the power cut "+
						"(%d acknowledged, %d present)", id, len(want), g.orders.Len())
				}
			}
			checkIndexes(t, g)
		})
	}
}

// TestPowerLossDuringSyncTruncatesAtTheTear cuts the power with a sync in
// flight, so only part of the last frame reached the disk — at every possible
// prefix length. Everything committed before it must survive; the torn frame
// must not appear at all.
//
// This is the deterministic counterpart to the randomised test above, and the
// one that would catch an off-by-one in the frame reader.
func TestPowerLossDuringSyncTruncatesAtTheTear(t *testing.T) {
	// Build a reference log: five committed orders, then a sixth.
	refDir := t.TempDir()
	refDev := newCrashDevice()
	refPath := filepath.Join(refDir, walName)
	ref := openFixtureOn(t, refDir, refDev, func(o *Options) {
		o.CompactAfterBytes = 1 << 40
	})
	for i := 1; i <= 5; i++ {
		mustPut(t, ref.orders, mkOrder(strconv.Itoa(i)))
	}
	base := len(refDev.durableBytes(refPath))
	mustPut(t, ref.orders, mkOrder("6"))
	full := refDev.durableBytes(refPath)
	release(t, ref.db)

	frameLen := len(full) - base
	if frameLen <= 0 {
		t.Fatalf("the sixth commit added %d bytes to the log", frameLen)
	}

	for k := 0; k <= frameLen; k++ {
		dir := t.TempDir()
		dev := newCrashDevice()
		dev.preload(filepath.Join(dir, walName), full[:base+k])

		g := openFixtureOn(t, dir, dev, func(o *Options) {
			o.CompactAfterBytes = 1 << 40
		})

		for i := 1; i <= 5; i++ {
			if _, ok := g.orders.Get(strconv.Itoa(i)); !ok {
				t.Fatalf("k=%d: order %d was fully durable and is missing", k, i)
			}
		}
		_, sixth := g.orders.Get("6")
		if want := k == frameLen; sixth != want {
			t.Fatalf("k=%d of %d: order 6 present = %v, want %v", k, frameLen, sixth, want)
		}
		if rep := g.db.Recovery(); k != frameLen && k != 0 && !rep.Truncated {
			t.Errorf("k=%d: recovery did not report a truncated tail", k)
		}
		checkIndexes(t, g)
		release(t, g.db)
	}
}

// TestSyncFailureIsNeverAcknowledged: an fsync that fails must not produce a
// nil error from Update.
//
// This is the classic way a store loses data while looking healthy. On Linux a
// failed fsync can leave the kernel having dropped the dirty pages, so a later
// sync may report success without them ever reaching the disk — which means
// there is no honest recovery from it beyond refusing to continue.
func TestSyncFailureIsNeverAcknowledged(t *testing.T) {
	dir := t.TempDir()
	dev := newCrashDevice()
	f := openFixtureOn(t, dir, dev)

	mustPut(t, f.orders, mkOrder("1"))

	dev.setFailSync(errors.New("simulated fsync failure"))
	if err := f.orders.Put(mkOrder("2")); err == nil {
		t.Fatal("Put returned nil after fsync failed; the commit was acknowledged but is not on disk")
	}

	if err := f.orders.Put(mkOrder("3")); !errors.Is(err, ErrFailed) {
		t.Fatalf("Put after a failed sync = %v, want ErrFailed: durability is unknown from here on", err)
	}

	dev.cutPower()
	release(t, f.db)

	g := openFixtureOn(t, dir, dev)
	if _, ok := g.orders.Get("1"); !ok {
		t.Error("order 1 was acknowledged before the failure and must survive")
	}
	if _, ok := g.orders.Get("2"); ok {
		t.Error("order 2 was never acknowledged and must not reappear")
	}
	if _, ok := g.orders.Get("3"); ok {
		t.Error("order 3 was refused outright and must not reappear")
	}
	checkIndexes(t, g)
}

// TestWriteFailureKeepsDurableStateIntact: a failed append must not cost
// anything that was already durable.
func TestWriteFailureKeepsDurableStateIntact(t *testing.T) {
	dir := t.TempDir()
	dev := newCrashDevice()
	f := openFixtureOn(t, dir, dev)

	mustPut(t, f.orders, mkOrder("1"))
	mustPut(t, f.orders, mkOrder("2"))

	dev.setFailWrite(errors.New("simulated write failure"))
	if err := f.orders.Put(mkOrder("3")); !errors.Is(err, ErrFailed) {
		t.Fatalf("Put with a failing device = %v, want ErrFailed", err)
	}

	dev.cutPower()
	release(t, f.db)

	g := openFixtureOn(t, dir, dev)
	for _, id := range []string{"1", "2"} {
		if _, ok := g.orders.Get(id); !ok {
			t.Errorf("order %s was durable before the write failure and is gone", id)
		}
	}
	if _, ok := g.orders.Get("3"); ok {
		t.Error("order 3 never reached the disk and must not reappear")
	}
	checkIndexes(t, g)
}

// TestIntervalSyncLosesASuffixNeverAHole pins what SyncInterval actually
// promises. It acknowledges before fsyncing, so a power cut costs the commits
// since the last flush — but what survives must still be a prefix of the commit
// order. A hole would mean recovery had applied a frame it should have stopped
// at.
func TestIntervalSyncLosesASuffixNeverAHole(t *testing.T) {
	dir := t.TempDir()
	dev := newCrashDevice()
	path := filepath.Join(dir, walName)

	f := openFixtureOn(t, dir, dev, func(o *Options) {
		o.Sync = SyncInterval
		o.SyncEvery = time.Hour // the ticker must never fire; syncNow stands in
		o.CompactAfterBytes = 1 << 40
	})

	const flushed, total = 10, 25
	for i := 1; i <= total; i++ {
		mustPut(t, f.orders, mkOrder(strconv.Itoa(i)))
		if i == flushed {
			dev.syncNow(path) // the interval tick lands here
		}
	}

	dev.cutPower()
	release(t, f.db)

	g := openFixtureOn(t, dir, dev, func(o *Options) { o.Sync = SyncInterval })

	survivors := 0
	for i := 1; i <= total; i++ {
		if _, ok := g.orders.Get(strconv.Itoa(i)); ok {
			survivors++
		} else {
			break
		}
	}
	if survivors < flushed {
		t.Errorf("%d orders survived, want at least the %d that were flushed", survivors, flushed)
	}
	// Everything past the run of survivors must be absent: no holes.
	for i := survivors + 1; i <= total; i++ {
		if _, ok := g.orders.Get(strconv.Itoa(i)); ok {
			t.Fatalf("order %d survived but order %d did not: recovery left a hole", i, survivors+1)
		}
	}
	if g.orders.Len() != survivors {
		t.Errorf("collection holds %d records, but only %d form the surviving prefix", g.orders.Len(), survivors)
	}
	checkIndexes(t, g)
}
