//go:build linux

package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEvictPageCacheCountsFiles(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, 3)
	e := evictPageCache(dir)
	require.NoError(t, e.Err)
	require.Equal(t, 3, e.Files)
	require.Equal(t, int64(3*4096), e.Bytes)
}
