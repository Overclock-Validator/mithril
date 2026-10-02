package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanAccountsDbDirRemovesReplaySidecars(t *testing.T) {
	root := t.TempDir()
	checkpointDir := filepath.Join(root, "transaction-status-checkpoints")
	rewardDir := filepath.Join(root, "rpc-epoch-rewards")
	require.NoError(t, os.MkdirAll(checkpointDir, 0o755))
	require.NoError(t, os.MkdirAll(rewardDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointDir, "stale.bin"), []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(rewardDir, "stale.json"), []byte("stale"), 0o644))

	CleanAccountsDbDir(root)
	assert.NoDirExists(t, checkpointDir)
	assert.NoDirExists(t, rewardDir)
}
