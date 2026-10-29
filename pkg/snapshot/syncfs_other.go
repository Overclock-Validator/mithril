//go:build !linux

package snapshot

import "golang.org/x/sys/unix"

// syncFilesystem flushes dirty data; without syncfs, sync the whole system.
func syncFilesystem(string) error {
	unix.Sync()
	return nil
}
