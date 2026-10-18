package accountsdb

import (
	"fmt"
	"syscall"
)

// AvailableDiskBytes excludes filesystem-reserved blocks, even when running
// as root. The reserve belongs to recovery, vote history and other writers.
func AvailableDiskBytes(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, fmt.Errorf("disk space for %s: %w", path, err)
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

func checkDiskBudget(available, required, reserve uint64) error {
	// Subtract only after checking the reserve, to avoid unsigned overflow.
	if available < reserve || required > available-reserve {
		return fmt.Errorf("accountsdb: insufficient disk space: available_bytes=%d required_bytes=%d reserve_bytes=%d; reclaim space before resuming", available, required, reserve)
	}
	return nil
}
