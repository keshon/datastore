//go:build windows

package datastore

import (
	"os"

	"golang.org/x/sys/windows"
)

// Windows has no flock. LockFileEx locks a byte *range*, and a locked range is
// unreadable to everyone else — so locking the start of the file would make the
// pid we record there unreadable by the very process that wants to report who
// is holding it.
//
// The lock therefore sits at an offset no real content will ever reach. Only
// the exclusivity matters; the position is arbitrary.
//
// v1 of this package returned "file locking is not supported on Windows" here,
// which meant New(DefaultConfig(...)) failed outright on the platform this is
// developed on. Locking is not optional: without it two processes diverge in
// silence.
const (
	lockBytes      = 1
	lockOffsetHigh = 0x8000_0000 // byte 2^63, far past any lock file
)

func lockFile(f *os.File) error {
	ol := &windows.Overlapped{OffsetHigh: lockOffsetHigh}
	return windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, lockBytes, 0, ol,
	)
}

func unlockFile(f *os.File) error {
	ol := &windows.Overlapped{OffsetHigh: lockOffsetHigh}
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, lockBytes, 0, ol)
}
