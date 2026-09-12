package snapshot

import (
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanAccountsDbDirRemovesTransactionStatusCheckpoints(t *testing.T) {
	root := t.TempDir()
	checkpointDir := filepath.Join(root, "transaction-status-checkpoints")
	require.NoError(t, os.MkdirAll(checkpointDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointDir, "stale.bin"), []byte("stale"), 0o644))

	CleanAccountsDbDir(root)
	assert.NoDirExists(t, checkpointDir)
}

func TestSnapshotCleanupAndBuildRespectStoreOwnership(t *testing.T) {
	root := t.TempDir()
	accountsPath := filepath.Join(root, "accounts")
	require.NoError(t, os.Mkdir(accountsPath, 0755))
	sentinel := filepath.Join(accountsPath, "0.1")
	require.NoError(t, os.WriteFile(sentinel, []byte("keep"), 0600))
	guard, err := accountsdb.AcquireExclusiveAccountsDbStore(root)
	require.NoError(t, err)
	CleanAccountsDbDir(root)
	_, _, err = BuildAccountsDbPaths(t.Context(), "missing.snapshot", "", root, nil)
	require.ErrorIs(t, err, accountsdb.ErrAccountsDbInUse)
	require.NoError(t, guard.Close())
	// A completed or interrupted genesis must remain protected after its owner
	// exits, including from direct snapshot-builder calls.
	require.NoError(t, os.WriteFile(filepath.Join(root, state.GenesisInitializingFileName), []byte("intent"), 0600))
	CleanAccountsDbDir(root)
	_, _, err = BuildAccountsDbPaths(t.Context(), "missing.snapshot", "", root, nil)
	require.ErrorContains(t, err, "genesis-origin")
	got, err := os.ReadFile(sentinel)
	require.NoError(t, err)
	require.Equal(t, "keep", string(got))
}
