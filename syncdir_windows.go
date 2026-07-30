//go:build windows

package datastore

// syncDir is a no-op on Windows.
//
// Windows offers no way to fsync a directory handle through the Go standard
// library — opening a directory for the purpose fails outright. MoveFileEx,
// which os.Rename uses, is atomic on NTFS with respect to crash recovery, so
// the rename either happened or it did not. The durability of the snapshot's
// *contents* comes from the Sync on the temporary file, which does work here.
func syncDir(dir string) error { return nil }
