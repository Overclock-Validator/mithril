//go:build !linux

package accountsdb

import (
	"errors"
	"os"
)

const boundedWritebackSupported = false

// boundedWriteback has no implementation here; the caller reports the error
// and leaves the segment to its final fsync.
func boundedWriteback(*os.File, int64, int64) error { return errors.ErrUnsupported }
