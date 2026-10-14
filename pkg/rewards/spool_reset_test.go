package rewards

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewardSpoolReexecutionResetsOnlySelectedPartitions(t *testing.T) {
	root := t.TempDir()
	stale := partitionFilePath(root, 32, 0)
	other := partitionFilePath(root, 64, 0)
	require.NoError(t, os.WriteFile(stale, make([]byte, SpoolRecordSize), 0600))
	require.NoError(t, os.WriteFile(other, []byte("other boundary"), 0600))
	require.NoError(t, resetPartitionedSpoolFiles(root, 32, 1))
	_, err := os.Stat(stale)
	require.True(t, os.IsNotExist(err))
	data, err := os.ReadFile(other)
	require.NoError(t, err)
	require.Equal(t, "other boundary", string(data))
	// Failure to remove a stale partition must abort recalculation, rather than
	// allowing an empty new partition to inherit old rewards.
	require.NoError(t, os.Mkdir(stale, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(stale, "occupied"), []byte{1}, 0600))
	require.ErrorContains(t, resetPartitionedSpoolFiles(root, 32, 1), "reset reward partition")
}
