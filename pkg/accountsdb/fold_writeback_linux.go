//go:build linux

package accountsdb

import (
	"os"

	"golang.org/x/sys/unix"
)

const boundedWritebackSupported = true

// boundedWriteback waits for writeback already started on [0, from) and starts
// it for [from, to). It asks for no flush of the device cache and makes no
// durability claim.
func boundedWriteback(f *os.File, from, to int64) error {
	fd := int(f.Fd())
	if from > 0 {
		if err := unix.SyncFileRange(fd, 0, from, unix.SYNC_FILE_RANGE_WAIT_BEFORE); err != nil {
			return err
		}
	}
	return unix.SyncFileRange(fd, from, to-from, unix.SYNC_FILE_RANGE_WRITE)
}
