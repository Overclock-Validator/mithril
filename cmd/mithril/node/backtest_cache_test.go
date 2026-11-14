package node

import (
	"errors"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDescribeCacheMode(t *testing.T) {
	require.Equal(t, "warm (page cache as earlier runs left it)", describeCacheMode(cacheWarm, nil))
	require.Equal(t, "cold (eviction did not run)", describeCacheMode(cacheCold, nil))
	got := describeCacheMode(cacheNormal, &cacheEviction{Files: 3, Bytes: 3 << 30})
	require.Contains(t, got, "normal: 3 files, 3.0 GiB evicted from the page cache")
	require.Contains(t, got, "inside replay_init_s")
	require.Contains(t, describeCacheMode(cacheCold, &cacheEviction{Err: errors.New("x")}), "not evicted (x)")
}

func TestEvictPageCacheOffLinux(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("covered by the Linux test")
	}
	dir := t.TempDir()
	writeFiles(t, dir, 2)
	e := evictPageCache(dir)
	require.Error(t, e.Err, "the summary must say nothing was evicted")
	require.Zero(t, e.Files)
}
