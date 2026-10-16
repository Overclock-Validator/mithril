//go:build !linux

package accountsdb

import (
	"fmt"
	"os"
)

const sparseCompactionSupported = false

func allocatedBytesFromInfo(info os.FileInfo) int64 { return info.Size() }

func allocatedFileBytes(f *os.File) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func punchDeadPayload(_ *os.File, _, _ int64) error {
	return fmt.Errorf("sparse compaction is not supported on this platform")
}
