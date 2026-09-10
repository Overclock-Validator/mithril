//go:build linux

package accountsdb

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func readShardedBaseScanFileIdentity(file *os.File) (shardedBaseScanFileIdentity, error) {
	if file == nil {
		return shardedBaseScanFileIdentity{}, fmt.Errorf("accountsdb: nil exact sidecar for Linux identity")
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return shardedBaseScanFileIdentity{}, fmt.Errorf("accountsdb: stat exact sidecar for Linux identity: %w", err)
	}
	if stat.Size <= 0 || stat.Ino == 0 {
		return shardedBaseScanFileIdentity{}, fmt.Errorf(
			"accountsdb: exact sidecar has unavailable Linux identity (size=%d inode=%d)",
			stat.Size,
			stat.Ino,
		)
	}
	return shardedBaseScanFileIdentity{
		Device:           uint64(stat.Dev),
		Inode:            stat.Ino,
		Size:             uint64(stat.Size),
		MTimeSeconds:     stat.Mtim.Sec,
		MTimeNanoseconds: stat.Mtim.Nsec,
		CTimeSeconds:     stat.Ctim.Sec,
		CTimeNanoseconds: stat.Ctim.Nsec,
	}, nil
}
