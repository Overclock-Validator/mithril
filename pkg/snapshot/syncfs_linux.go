//go:build linux

package snapshot

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncFilesystem flushes every dirty page of the filesystem holding dir.
func syncFilesystem(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return unix.Syncfs(int(f.Fd()))
}
