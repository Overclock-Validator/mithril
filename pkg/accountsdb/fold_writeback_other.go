//go:build !linux

package accountsdb

import "os"

func boundedWriteback(*os.File, int64, int64) error { return nil }
