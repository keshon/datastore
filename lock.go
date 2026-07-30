package datastore

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// lockName is the file whose exclusive lock grants the right to write.
const lockName = "LOCK"

// dirLock is an advisory exclusive lock on a database directory, held for the
// lifetime of the process.
//
// One writer is a load-bearing assumption, not a convenience: the in-memory
// state and the log would diverge immediately if a second process appended to
// the same directory. The lock is what turns that from silent corruption into
// a clear refusal to start.
type dirLock struct {
	f    *os.File
	path string
}

func acquireLock(dir string) (*dirLock, error) {
	path := filepath.Join(dir, lockName)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("datastore: open lock file: %w", err)
	}

	if err := lockFile(f); err != nil {
		f.Close()
		if lockContended(err) {
			return nil, fmt.Errorf("%w: %s (held by pid %s)", ErrLocked, path, readHolder(path))
		}
		// Not contention — locking itself failed (permissions, a filesystem
		// that cannot lock). Report the real error instead of blaming a
		// nonexistent other process.
		return nil, fmt.Errorf("datastore: lock %s: %w", path, err)
	}

	// Record the holder for diagnostics only. Nothing reads this to make a
	// decision — the lock itself is the mechanism.
	if err := f.Truncate(0); err == nil {
		_, _ = f.WriteAt([]byte(strconv.Itoa(os.Getpid())), 0)
		_ = f.Sync()
	}

	return &dirLock{f: f, path: path}, nil
}

func (l *dirLock) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := unlockFile(l.f)
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	l.f = nil

	// The file is deliberately left behind. Removing it races with another
	// process that has just opened it and is about to lock it — that process
	// would then hold a lock on an unlinked inode and a third could take the
	// lock on a fresh file. An empty LOCK file costs nothing.
	return err
}

// readHolder reports the pid recorded in an existing lock file, for error
// messages. Best effort; an unreadable or empty file yields "unknown".
func readHolder(path string) string {
	b, err := os.ReadFile(path)
	if err != nil || len(b) == 0 {
		return "unknown"
	}
	if len(b) > 16 {
		b = b[:16]
	}
	return string(b)
}
