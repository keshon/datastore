//go:build windows

package datastore

// syncDir is a no-op on Windows.
//
// The standard library has no equivalent of Unix directory fsync here.
// Snapshot installation separately requests MOVEFILE_WRITE_THROUGH after
// syncing the file contents; neither path certifies physical power-loss safety.
func syncDir(dir string) error { return nil }
