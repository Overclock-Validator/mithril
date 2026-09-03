package accountsdb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProductionAccountIndexRewindApplyDoesNotHoldPublicationLockWhileWaitingForCapacity(t *testing.T) {
	config := productionIndexTestConfig()
	config.MaxHotKeys = 1
	config.MaxHotBytes = DefaultShardedMutableBytesPerKey
	config.SealKeys = 1
	config.RebaseKeys = 1
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = index.Close() })

	initialRoot, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	realPublish := index.mutable.config.Callbacks.PublishCheckpoint
	publishEntered := make(chan struct{})
	releasePublish := make(chan struct{})
	var releaseOnce sync.Once
	var publishCalls atomic.Uint32
	t.Cleanup(func() { releaseOnce.Do(func() { close(releasePublish) }) })
	index.mutable.config.Callbacks.PublishCheckpoint = func(
		ctx context.Context,
		publication ShardedMutableCheckpointPublication,
	) error {
		if publishCalls.Add(1) == 1 {
			close(publishEntered)
			select {
			case <-releasePublish:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return realPublish(ctx, publication)
	}

	firstMeta := foldMeta{BatchSeq: 10, ThroughSlot: 100, FileId: 1000}
	firstKey := productionIndexKeyForShard(index, 0, 90)
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(firstKey, AccountIndexEntry{Slot: 100, FileId: 1000, Offset: 8}),
	}, &firstMeta, true))
	select {
	case <-publishEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("initial capacity-relieving checkpoint did not reach its publication hook")
	}

	// Hold the mutable writer long enough to know precisely where the rewind
	// goroutine is blocked. The repaired order is applyMu -> mutable writer,
	// leaving publishMu available to the maintenance callback.
	index.mutable.writeMu.Lock()
	writeHeld := true
	defer func() {
		if writeHeld {
			index.mutable.writeMu.Unlock()
		}
		releaseOnce.Do(func() { close(releasePublish) })
	}()

	rewindMeta := foldMeta{BatchSeq: 9, ThroughSlot: 90, FileId: 900}
	rewindKey := productionIndexKeyForShard(index, 1, 91)
	rewindEntry := AccountIndexEntry{Slot: 90, FileId: 900, Offset: 16}
	rewindDone := make(chan error, 1)
	go func() {
		rewindDone <- index.Apply(
			[]deltaIndexMutation{liveDeltaMutation(rewindKey, rewindEntry)},
			&rewindMeta,
			true,
		)
	}()
	require.Eventually(t, func() bool {
		if index.applyMu.TryLock() {
			index.applyMu.Unlock()
			return false
		}
		return true
	}, 5*time.Second, time.Millisecond, "rewind did not acquire apply serialization")
	require.True(t, index.publishMu.TryLock(), "rewind held publishMu before entering mutable.Apply")
	index.publishMu.Unlock()

	index.mutable.writeMu.Unlock()
	writeHeld = false
	select {
	case err := <-rewindDone:
		t.Fatalf("rewind unexpectedly completed before capacity maintenance: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Publishing the first checkpoint frees hot capacity. Before this fix the
	// callback blocked on publishMu while rewind Apply blocked on its progress.
	releaseOnce.Do(func() { close(releasePublish) })
	select {
	case err := <-rewindDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("rewind remained deadlocked after checkpoint publication was released")
	}
	rootAfter, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	assert.NotEqual(t, initialRoot.Lineage, rootAfter.Lineage)
	assert.Equal(t, initialRoot.RoutingKey, rootAfter.RoutingKey)
	assert.Equal(t, rewindMeta.BatchSeq, rootAfter.RootedBatchSequence)
	assert.Equal(t, rewindMeta.ThroughSlot, rootAfter.RootedSlot)
	meta, found := index.ReadFoldMeta()
	require.True(t, found)
	assert.Equal(t, rewindMeta, meta)
	requireProductionCandidate(t, index, rewindKey, rewindEntry, accountIndexSourceDelta)
}

func TestProductionAccountIndexApplySerializationPreventsForwardOvertakingRewindRotation(t *testing.T) {
	config := productionIndexTestConfig()
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = index.Close() })
	initialRoot, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)

	initialMeta := foldMeta{BatchSeq: 10, ThroughSlot: 100, FileId: 1000}
	require.NoError(t, index.Apply(nil, &initialMeta, true))

	index.publishMu.Lock()
	publishHeld := true
	defer func() {
		if publishHeld {
			index.publishMu.Unlock()
		}
	}()
	rewindMeta := foldMeta{BatchSeq: 9, ThroughSlot: 90, FileId: 900}
	rewindDone := make(chan error, 1)
	go func() { rewindDone <- index.Apply(nil, &rewindMeta, true) }()
	require.Eventually(t, func() bool {
		meta, found := index.ReadFoldMeta()
		return found && meta == rewindMeta
	}, 5*time.Second, time.Millisecond, "rewind WAL frame did not become visible")

	forwardMeta := foldMeta{BatchSeq: 11, ThroughSlot: 110, FileId: 1100}
	forwardDone := make(chan error, 1)
	go func() { forwardDone <- index.Apply(nil, &forwardMeta, true) }()
	select {
	case err := <-forwardDone:
		t.Fatalf("forward Apply overtook the rewind root rotation: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	metaWhileBlocked, found := index.ReadFoldMeta()
	require.True(t, found)
	assert.Equal(t, rewindMeta, metaWhileBlocked)

	index.publishMu.Unlock()
	publishHeld = false
	select {
	case err := <-rewindDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("rewind root rotation did not complete")
	}
	select {
	case err := <-forwardDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("forward Apply did not resume after rewind rotation")
	}
	finalMeta, found := index.ReadFoldMeta()
	require.True(t, found)
	assert.Equal(t, forwardMeta, finalMeta)
	rootAfter, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	assert.Equal(t, initialRoot.RoutingKey, rootAfter.RoutingKey)
	assert.Equal(t, rewindMeta.BatchSeq, rootAfter.RootedBatchSequence)
	assert.Equal(t, rewindMeta.ThroughSlot, rootAfter.RootedSlot)
}
