//go:build unix

package datastore

import "os"

// syncDir flushes a directory entry, so a rename survives power loss.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
