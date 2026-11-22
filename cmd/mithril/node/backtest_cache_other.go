//go:build !linux

package node

import "errors"

var errPageCacheUnsupported = errors.New("page cache eviction needs Linux")

func dropFileCache(string) (int64, bool, error) { return 0, false, errPageCacheUnsupported }
