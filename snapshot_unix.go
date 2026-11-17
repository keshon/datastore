//go:build !windows

package datastore

import "os"

func installSnapshot(from, to string) error {
	return os.Rename(from, to)
}
