//go:build windows

package datastore

import (
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

// Request write-through installation after syncing the file contents. This is
// stronger than os.Rename, but does not certify a filesystem against power loss.
func installSnapshot(from, to string) error {
	source, err := snapshotWindowsPath(from)
	if err != nil {
		return err
	}
	target, err := snapshotWindowsPath(to)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(source, target, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}

func snapshotWindowsPath(path string) (*uint16, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(abs, `\\?\`) && !strings.HasPrefix(abs, `\\.\`) {
		if strings.HasPrefix(abs, `\\`) {
			abs = `\\?\UNC\` + abs[2:]
		} else {
			abs = `\\?\` + abs
		}
	}
	return windows.UTF16PtrFromString(abs)
}
