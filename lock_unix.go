//go:build unix

package datastore

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func lockFile(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
}

func unlockFile(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_UN)
}

// lockContended reports whether the lock failed because another process holds
// it, as opposed to locking itself being broken.
func lockContended(err error) bool {
	return errors.Is(err, unix.EWOULDBLOCK)
}
