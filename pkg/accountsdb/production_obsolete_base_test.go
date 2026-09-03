package accountsdb

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestObsoleteBaseCleanupFailurePoisonsProductionIndex(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("remove obsolete base failed")
	resource, err := NewIndexGenerationResource(
		"base-generation-7",
		nil,
		func() error { return wantErr },
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, resource.retain())

	index := &ProductionAccountIndex{}
	index.trackObsoleteBase([]*IndexGenerationResource{resource}, 6)
	require.ErrorIs(t, resource.release(), wantErr)

	require.Eventually(t, func() bool {
		err := index.checkUsable()
		return errors.Is(err, ErrProductionAccountIndexPoisoned) &&
			err != nil &&
			containsAll(err.Error(), "reclaim obsolete base", "base-generation-7", wantErr.Error())
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, index.Close(), wantErr)
}

func TestObsoleteBaseGenerationWaitsForCatalogAndSurfacesItsCleanupFailure(t *testing.T) {
	t.Parallel()

	base := mustIndexResource(t, "base-generation-8", nil, nil, nil)
	wantErr := errors.New("remove obsolete extent catalog failed")
	catalog := mustIndexResource(t, "extents-generation-8", nil, nil, func() error {
		return wantErr
	})
	require.NoError(t, base.retain())
	require.NoError(t, catalog.retain())
	require.NoError(t, base.markObsolete())
	require.NoError(t, catalog.markObsolete())

	index := &ProductionAccountIndex{}
	index.trackObsoleteBase([]*IndexGenerationResource{base, catalog}, 7)
	require.NoError(t, base.release())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, index.waitForObsoleteBases(ctx), context.DeadlineExceeded)

	require.ErrorIs(t, catalog.release(), wantErr)
	require.Eventually(t, func() bool {
		err := index.checkUsable()
		return errors.Is(err, ErrProductionAccountIndexPoisoned) &&
			err != nil &&
			containsAll(err.Error(), "obsolete base-generation resource", catalog.Name(), wantErr.Error())
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, index.Close(), wantErr)
}

func TestProductionShutdownJoinsObsoleteCheckpointAccountingWatcher(t *testing.T) {
	t.Parallel()

	budget, err := newCheckpointResourceBudget(1024, 2048, []uint64{0})
	require.NoError(t, err)
	resource := mustIndexResource(t, "obsolete-delta-accounting", nil, nil, nil)
	require.NoError(t, resource.retain())
	index := &ProductionAccountIndex{checkpointBudget: budget}
	index.trackObsoleteCheckpoint(resource, 1)
	require.NoError(t, resource.release())

	err = index.Close()
	require.ErrorIs(t, err, ErrCheckpointResourceAccounting)
	require.ErrorIs(t, index.checkPoison(), ErrProductionAccountIndexPoisoned)
}

func TestRollingRebaseWaitsForPriorObsoleteBaseGeneration(t *testing.T) {
	config := productionIndexTestConfig()
	config.SealKeys = 1
	config.RebaseKeys = 1
	root, records := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	pinned, err := index.view.Acquire()
	require.NoError(t, err)
	pinClosed := false
	defer func() {
		if !pinClosed {
			_ = pinned.Close()
		}
		_ = index.Close()
	}()

	key := records[0].key
	shardID := index.mutable.router.Shard(key)
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(key, AccountIndexEntry{Slot: 700, FileId: 701, Offset: 8}),
	}, nil, true))
	require.Eventually(t, func() bool {
		stats := index.RuntimeStats()
		return stats.RebaseCount == 1 && stats.ObsoleteBaseGenerationsPending == 1
	}, 10*time.Second, time.Millisecond, "first rebase did not leave its reader-pinned base visible")
	firstRebaseRoot, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	firstBaseGeneration := firstRebaseRoot.Shards[shardID].BaseGeneration
	require.Greater(t, firstBaseGeneration, uint64(1))

	// The second checkpoint may publish, but its base merge must stop at the
	// reclamation gate before allocating/publishing another base generation.
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(key, AccountIndexEntry{Slot: 800, FileId: 801, Offset: 16}),
	}, nil, true))
	require.Eventually(t, func() bool {
		stats := index.RuntimeStats()
		return stats.RebaseCount == 1 && stats.RebasesInProgress == 1 &&
			stats.ObsoleteBaseGenerationsPending == 1
	}, 10*time.Second, time.Millisecond, "second rebase did not wait on the obsolete base")
	time.Sleep(25 * time.Millisecond)
	whilePinned, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	require.Equal(t, firstBaseGeneration, whilePinned.Shards[shardID].BaseGeneration)

	require.NoError(t, pinned.Close())
	pinClosed = true
	require.Eventually(t, func() bool {
		stats := index.RuntimeStats()
		return stats.RebaseCount == 2 && stats.ObsoleteBaseGenerationsPending == 0
	}, 10*time.Second, time.Millisecond, "second rebase did not resume after prior base reclamation")
	afterDrain, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	require.Greater(t, afterDrain.Shards[shardID].BaseGeneration, firstBaseGeneration)
}

func containsAll(value string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(value, part) {
			return false
		}
	}
	return true
}
