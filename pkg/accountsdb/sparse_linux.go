//go:build linux

package accountsdb

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

const sparseCompactionSupported = true

func allocatedBytesFromInfo(info os.FileInfo) int64 { return info.Sys().(*syscall.Stat_t).Blocks * 512 }

func allocatedFileBytes(f *os.File) (int64, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &stat); err != nil {
		return 0, err
	}
	return stat.Blocks * 512, nil
}

func punchDeadPayload(f *os.File, offset, length int64) error {
	return unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, offset, length)
}
