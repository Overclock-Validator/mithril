//go:build linux

package node

import (
	"os"

	"golang.org/x/sys/unix"
)

// dropFileCache drops one file's clean pages from the page cache.
func dropFileCache(path string) (size int64, ok bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false, nil
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, false, nil
	}
	if err := unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED); err != nil {
		return 0, false, nil
	}
	return info.Size(), true, nil
}
