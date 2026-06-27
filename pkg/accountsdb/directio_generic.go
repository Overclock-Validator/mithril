//go:build !linux

package accountsdb

import (
	"fmt"
	"os"
)

// OpenDirect opens a file without O_DIRECT on non-Linux platforms.
// On Linux, this uses O_DIRECT. This fallback is for development/testing only.
func OpenDirect(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag, perm)
}

// Fallocate pre-allocates disk space for a file. On non-Linux platforms this
// falls back to truncating the file to the requested size.
func Fallocate(f *os.File, size int64) error {
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("truncate (fallocate fallback) size=%d: %w", size, err)
	}
	return nil
}
