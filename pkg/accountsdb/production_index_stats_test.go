package accountsdb

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProductionAccountIndexRuntimeStatsAreCompactAndExact(t *testing.T) {
	config := productionIndexTestConfig()
	root, records := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })

	initial := index.RuntimeStats()
	assert.True(t, initial.Enabled)
	assert.Equal(t, uint64(1), initial.RootGeneration)
	assert.Zero(t, initial.MinimumCoveredSequence)
	assert.Equal(t, uint64(len(records)), initial.BaseKeys)
	assert.Positive(t, initial.BaseArtifactBytes)
	assert.Positive(t, initial.ExtentCatalogEntries)
	assert.Equal(t, uint64(streamIndexMaxExtents), initial.ExtentCatalogCapacity)
	assert.Equal(t, initial.ExtentCatalogCapacity-initial.ExtentCatalogEntries, initial.ExtentCatalogRemaining)
	assert.Zero(t, initial.DeltaKeys)
	assert.Zero(t, initial.DeltaArtifactBytes)
	assert.Zero(t, initial.ObsoleteBaseGenerationsPending)
	assert.Zero(t, initial.HotKeys)
	assert.Positive(t, initial.WALBytes)
	assert.False(t, initial.FatalError)

	updated := AccountIndexEntry{Slot: 900, FileId: 901, Offset: 40}
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(records[0].key, updated),
	}, nil, true))
	hot := index.RuntimeStats()
	assert.Equal(t, uint64(1), hot.HotKeys)
	assert.Equal(t, DefaultShardedMutableBytesPerKey, hot.HotBytes)
	assert.Equal(t, uint64(1), hot.WALSequence)
	assert.Greater(t, hot.WALBytes, initial.WALBytes)
	assert.Zero(t, hot.SealCount)

	require.NoError(t, index.ForceSeal(t.Context()))
	sealed := index.RuntimeStats()
	assert.Greater(t, sealed.RootGeneration, initial.RootGeneration)
	assert.Equal(t, uint64(1), sealed.MinimumCoveredSequence)
	assert.Equal(t, uint64(len(records)), sealed.BaseKeys)
	assert.Equal(t, initial.BaseArtifactBytes, sealed.BaseArtifactBytes)
	assert.Equal(t, uint64(1), sealed.DeltaKeys)
	assert.Positive(t, sealed.DeltaArtifactBytes)
	assert.Zero(t, sealed.HotKeys)
	assert.Zero(t, sealed.HotBytes)
	assert.Equal(t, uint64(1), sealed.SealCount)
	assert.Zero(t, sealed.RebaseCount)
	assert.Zero(t, sealed.RewriteCount)
	assert.Zero(t, sealed.SealsInProgress)
	assert.Zero(t, sealed.RebasesInProgress)
	assert.False(t, sealed.RewriteInProgress)
	assert.Zero(t, sealed.MaintenanceErrors)
	assert.False(t, sealed.FatalError)

	view, err := index.view.Acquire()
	require.NoError(t, err)
	catalog := view.Catalog()
	wantBaseBytes := catalog.SharedExtentCatalog.Size
	var wantDeltaBytes uint64
	for _, shard := range catalog.Shards {
		wantBaseBytes += shard.BaseIndex.Size + shard.BaseRecords.Size
		if shard.DeltaGeneration != 0 {
			wantDeltaBytes += shard.DeltaIndex.Size + shard.DeltaRecords.Size + deltaCheckpointDescriptorSize
		}
	}
	require.NoError(t, view.Close())
	assert.Equal(t, wantBaseBytes, sealed.BaseArtifactBytes)
	assert.Equal(t, wantDeltaBytes, sealed.DeltaArtifactBytes)

	db := &AccountsDb{ProductionIndex: index}
	assert.Equal(t, sealed, db.AccountIndexStats())
	assert.Equal(t, ProductionAccountIndexStats{}, (&AccountsDb{}).AccountIndexStats())
	assert.Equal(t, ProductionAccountIndexStats{}, (*AccountsDb)(nil).AccountIndexStats())

	require.NoError(t, index.Close())
	closed := index.RuntimeStats()
	assert.True(t, closed.Enabled)
	assert.True(t, closed.FatalError)
}

func TestProductionCheckpointBudgetChargesPinnedObsoleteGeneration(t *testing.T) {
	config := productionIndexTestConfig()
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })

	key := productionIndexKeyForShard(index, 0, 600)
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(key, AccountIndexEntry{Slot: 500, FileId: 501, Offset: 8}),
	}, nil, true))
	require.NoError(t, index.ForceSeal(t.Context()))

	pinned, err := index.view.Acquire()
	require.NoError(t, err)
	pinnedClosed := false
	t.Cleanup(func() {
		if !pinnedClosed {
			_ = pinned.Close()
		}
	})
	oldSelected := pinned.Catalog().Shards[0]
	require.NotZero(t, oldSelected.DeltaGeneration)
	oldIndexPath, err := ResolveIndexCatalogArtifactPath(root, oldSelected.DeltaIndex)
	require.NoError(t, err)

	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(key, AccountIndexEntry{Slot: 600, FileId: 601, Offset: 16}),
	}, nil, true))
	require.NoError(t, index.ForceSeal(t.Context()))

	whilePinned := index.RuntimeStats()
	assert.Positive(t, whilePinned.CheckpointSelectedBytes)
	assert.Positive(t, whilePinned.CheckpointObsoleteBytes)
	assert.Equal(
		t,
		whilePinned.CheckpointSelectedBytes+whilePinned.CheckpointBuildReservedBytes+whilePinned.CheckpointObsoleteBytes,
		whilePinned.CheckpointPhysicalBytes,
	)
	assert.GreaterOrEqual(t, whilePinned.CheckpointPhysicalHighWaterBytes, whilePinned.CheckpointPhysicalBytes)
	_, err = os.Stat(oldIndexPath)
	require.NoError(t, err, "pinned generation artifact was deleted early")

	require.NoError(t, pinned.Close())
	pinnedClosed = true
	require.Eventually(t, func() bool {
		stats := index.RuntimeStats()
		_, statErr := os.Stat(oldIndexPath)
		return stats.CheckpointObsoleteBytes == 0 && errors.Is(statErr, os.ErrNotExist)
	}, 5*time.Second, time.Millisecond)
}

func TestSaturatingAccountIndexStatAdd(t *testing.T) {
	assert.Equal(t, uint64(6), saturatingAccountIndexStatAdd(1, 2, 3))
	assert.Equal(t, ^uint64(0), saturatingAccountIndexStatAdd(^uint64(0)-1, 2, 1))
}
