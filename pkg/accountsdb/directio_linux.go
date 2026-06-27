//go:build linux

package accountsdb

import (
	"fmt"
	"os"
	"syscall"
)

// OpenDirect opens a file with O_DIRECT for bypassing the page cache.
func OpenDirect(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag|syscall.O_DIRECT, perm)
}

// Fallocate pre-allocates disk space for a file.
func Fallocate(f *os.File, size int64) error {
	if err := syscall.Fallocate(int(f.Fd()), 0, 0, size); err != nil {
		return fmt.Errorf("fallocate size=%d: %w", size, err)
	}
	return nil
}
