package accountsdb

import (
	"errors"
	"fmt"
	"math"

	"golang.org/x/sys/unix"
)

// AppendVecFilesystemSpace is the capacity visible to an unprivileged writer
// on the filesystem containing AccountsDB. AvailableBytes deliberately uses
// f_bavail rather than f_bfree so reserved filesystem blocks are not promised
// to the validator.
type AppendVecFilesystemSpace struct {
	AvailableBytes uint64
	TotalBytes     uint64
}

// ReadAppendVecFilesystemSpace returns saturating byte counts for the
// filesystem containing path.
func ReadAppendVecFilesystemSpace(path string) (AppendVecFilesystemSpace, error) {
	if path == "" {
		return AppendVecFilesystemSpace{}, errors.New("accountsdb: empty appendvec filesystem path")
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return AppendVecFilesystemSpace{}, fmt.Errorf("accountsdb: stat appendvec filesystem %s: %w", path, err)
	}
	if stat.Bsize <= 0 {
		return AppendVecFilesystemSpace{}, fmt.Errorf("accountsdb: appendvec filesystem %s reported invalid block size %d", path, stat.Bsize)
	}
	blockSize := uint64(stat.Bsize)
	return AppendVecFilesystemSpace{
		AvailableBytes: saturatingMultiplyUint64(uint64(stat.Bavail), blockSize),
		TotalBytes:     saturatingMultiplyUint64(uint64(stat.Blocks), blockSize),
	}, nil
}

func saturatingMultiplyUint64(left, right uint64) uint64 {
	if left != 0 && right > math.MaxUint64/left {
		return math.MaxUint64
	}
	return left * right
}

func saturatingAddUint64(left, right uint64) uint64 {
	if right > math.MaxUint64-left {
		return math.MaxUint64
	}
	return left + right
}
