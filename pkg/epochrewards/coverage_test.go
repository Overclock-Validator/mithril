package epochrewards

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEpochCoverageEmptyDistributionAndRetention(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 128)
	require.NoError(t, err)
	store.RecordCoverage(0, 128, true, false)
	require.NoError(t, store.Prepare(128))
	require.NoError(t, store.SetRooted(128))
	require.False(t, store.EpochAvailable(0), "an active distribution is incomplete")
	store.RecordCoverage(0, 129, false, true)
	require.NoError(t, store.Prepare(129))
	require.False(t, store.EpochAvailable(0), "prepared completion cannot precede account commit")
	require.NoError(t, store.SetRooted(129))
	require.True(t, store.EpochAvailable(0), "a complete epoch may have no payouts")
	reopened, err := Open(dir, 128)
	require.NoError(t, err)
	require.NoError(t, reopened.SetRooted(129))
	require.True(t, reopened.EpochAvailable(0))
	require.NoError(t, reopened.SetRooted(256))
	require.False(t, reopened.EpochAvailable(0), "a retained completion cannot replace a pruned boundary")
	_, err = os.Stat(reopened.batchPath(129))
	require.NoError(t, err, "the completion batch itself remains retained")
}

func TestEpochCoverageRewindAndPartialFoldRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 512)
	require.NoError(t, err)
	store.RecordCoverage(0, 128, true, false)
	require.NoError(t, store.Prepare(128))
	store.RecordCoverage(0, 129, false, true)
	require.NoError(t, store.Prepare(130))

	// Only the first account fold committed. The later duplicate boundary and
	// completion must not hide or replace the committed boundary on reopen.
	store, err = Open(dir, 512)
	require.NoError(t, err)
	require.NoError(t, store.SetRooted(128))
	require.False(t, store.EpochAvailable(0))
	store.RecordCoverage(0, 131, false, true)
	require.NoError(t, store.Prepare(132))
	require.NoError(t, store.Rewind(131))
	require.False(t, store.EpochAvailable(0))
	store.RecordCoverage(0, 133, false, true)
	require.NoError(t, store.Prepare(134))
	require.NoError(t, store.SetRooted(134))
	require.True(t, store.EpochAvailable(0))
	store, err = Open(dir, 512)
	require.NoError(t, err)
	require.NoError(t, store.SetRooted(134))
	require.True(t, store.EpochAvailable(0))
}

func TestEpochCoverageExpiresEvenWhenBothMarkersRemain(t *testing.T) {
	store, err := Open(t.TempDir(), 128)
	require.NoError(t, err)
	store.RecordCoverage(0, 128, true, false)
	store.RecordCoverage(0, 140, false, true)
	require.NoError(t, store.Prepare(200))
	require.NoError(t, store.SetRooted(200))
	require.True(t, store.EpochAvailable(0))
	require.NoError(t, store.SetRooted(256))
	_, err = os.Stat(store.batchPath(200))
	require.NoError(t, err)
	require.False(t, store.EpochAvailable(0), "coverage cannot extend beyond the epoch's retained boundary")
}

func TestEpochCoverageDoesNotInferSnapshotHistory(t *testing.T) {
	store, err := Open(t.TempDir(), 512)
	require.NoError(t, err)
	store.RecordCoverage(0, 150, false, true)
	require.NoError(t, store.Prepare(150))
	require.NoError(t, store.SetRooted(150))
	require.False(t, store.EpochAvailable(0), "inactive rewards after a snapshot do not prove the missing boundary")
	files, err := batchFiles(store.dir)
	require.NoError(t, err)
	require.Empty(t, files)

	// Even a surviving completion event in an older batch is insufficient.
	require.NoError(t, os.WriteFile(store.batchPath(150), []byte(`{"version":1,"through":150,"records":[],"coverage":[{"epoch":0,"slot":150,"boundarySlot":128,"complete":true}]}`), 0o600))
	store, err = Open(store.dir, 512)
	require.NoError(t, err)
	require.NoError(t, store.SetRooted(150))
	require.False(t, store.EpochAvailable(0))
}

func TestEpochCoverageRejectsMalformedBatches(t *testing.T) {
	for _, coverage := range []string{
		`[{"epoch":0,"slot":151,"boundarySlot":128,"complete":true}]`,
		`[{"epoch":0,"slot":150,"boundarySlot":151,"complete":true}]`,
		`[{"epoch":0,"slot":150,"boundarySlot":128,"complete":false}]`,
		`[{"epoch":0,"slot":128,"boundarySlot":128},{"epoch":0,"slot":129,"boundarySlot":129}]`,
	} {
		dir := t.TempDir()
		store, err := Open(dir, 512)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(store.batchPath(150), []byte(`{"version":1,"through":150,"records":[],"coverage":`+coverage+`}`), 0o600))
		_, err = Open(dir, 512)
		require.ErrorContains(t, err, "invalid epoch reward coverage")
	}
}
