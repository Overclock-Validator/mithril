package accountsdb

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// poisonOnFirstErrContext injects a prior publisher's terminal decision at
// publishCheckpoint's first context check, which is deliberately located
// after publishMu acquisition. waitReady evaluates Done, not Err, so this is a
// deterministic test of the post-serialization usability fence without a
// scheduler-sensitive mutex-wait assertion or a production test hook.
type poisonOnFirstErrContext struct {
	context.Context
	once   sync.Once
	poison func()
}

func (ctx *poisonOnFirstErrContext) Err() error {
	ctx.once.Do(ctx.poison)
	return nil
}

func productionPublicationTestError(operation string) error {
	return &os.PathError{Op: operation, Path: "injected-root-catalog", Err: unix.ENOSPC}
}

func setProductionMaintenanceRetryDelay(index *ProductionAccountIndex, delay time.Duration) {
	index.mutable.stateMu.Lock()
	index.mutable.sealRetryInitial = delay
	index.mutable.sealRetryMaximum = delay
	index.mutable.rebaseRetryInitial = delay
	index.mutable.rebaseRetryMaximum = delay
	index.mutable.stateMu.Unlock()
}

func requireDeltaGenerationPathsExist(t *testing.T, root string, shardID uint32, generation uint64) {
	t.Helper()
	paths := makeDeltaCheckpointPaths(ShardedMutableCheckpointDirectory(root, shardID), generation)
	for _, path := range []string{paths.index, paths.records, paths.descriptor} {
		require.FileExists(t, path)
	}
}

func requireDeltaGenerationPathsAbsentEventually(t *testing.T, root string, shardID uint32, generation uint64) {
	t.Helper()
	paths := makeDeltaCheckpointPaths(ShardedMutableCheckpointDirectory(root, shardID), generation)
	for _, path := range []string{paths.index, paths.records, paths.descriptor} {
		require.Eventually(t, func() bool {
			_, err := os.Lstat(path)
			return errors.Is(err, os.ErrNotExist)
		}, 5*time.Second, time.Millisecond, "artifact %s remained", path)
	}
}

func TestCheckpointPreRenameFailureCleansOnlyCandidateAfterPinnedGenerationDrains(t *testing.T) {
	config := productionIndexTestConfig()
	config.SealKeys = 1
	config.RebaseKeys = config.MaxHotKeys
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	defer func() { _ = index.Close() }()

	key1 := productionIndexKeyForShard(index, 0, 70)
	entry1 := AccountIndexEntry{Slot: 700, FileId: 70, Offset: 8}
	require.NoError(t, index.Apply([]deltaIndexMutation{liveDeltaMutation(key1, entry1)}, nil, true))
	require.NoError(t, index.ForceSeal(t.Context()))
	root1, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	gen1 := root1.Shards[0].DeltaGeneration
	require.NotZero(t, gen1)

	// This view keeps generation 1's mmap and exact files alive after generation
	// 2 replaces it.
	pin1, err := index.view.Acquire()
	require.NoError(t, err)
	pinClosed := false
	defer func() {
		if !pinClosed {
			_ = pin1.Close()
		}
	}()

	key2 := productionIndexKeyForShard(index, 0, 71)
	entry2 := AccountIndexEntry{Slot: 701, FileId: 71, Offset: 16}
	require.NoError(t, index.Apply([]deltaIndexMutation{liveDeltaMutation(key2, entry2)}, nil, true))
	require.NoError(t, index.ForceSeal(t.Context()))
	root2, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	gen2 := root2.Shards[0].DeltaGeneration
	require.Greater(t, gen2, gen1)
	requireDeltaGenerationPathsExist(t, root, 0, gen1)
	requireDeltaGenerationPathsExist(t, root, 0, gen2)

	setProductionMaintenanceRetryDelay(index, time.Hour)
	failedRoot := make(chan *RootIndexCatalog, 1)
	index.publishRoot = func(_ string, candidate *RootIndexCatalog) (bool, error) {
		select {
		case failedRoot <- candidate.Clone():
		default:
		}
		return false, productionPublicationTestError("checkpoint-pre-rename")
	}
	key3 := productionIndexKeyForShard(index, 0, 72)
	entry3 := AccountIndexEntry{Slot: 702, FileId: 72, Offset: 24}
	require.NoError(t, index.Apply([]deltaIndexMutation{liveDeltaMutation(key3, entry3)}, nil, true))

	var rejected *RootIndexCatalog
	select {
	case rejected = <-failedRoot:
	case <-time.After(10 * time.Second):
		t.Fatal("checkpoint root publication was not attempted")
	}
	waitForShardedMutableRetryStats(t, index.mutable, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.Shards[0].SealRetryPending
	})
	rejectedGeneration := rejected.Shards[0].DeltaGeneration
	require.Greater(t, rejectedGeneration, gen2)
	requireDeltaGenerationPathsAbsentEventually(t, root, 0, rejectedGeneration)

	// Neither the currently selected generation nor the older pinned generation
	// may be swept merely because candidate generation 3 failed.
	requireDeltaGenerationPathsExist(t, root, 0, gen1)
	requireDeltaGenerationPathsExist(t, root, 0, gen2)
	payload, ok := pin1.Payload().(*ShardedImmutableIndex)
	require.True(t, ok)
	value, source, found, err := payload.LookupCandidate(key1)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, accountIndexSourceDelta, source)
	assert.Equal(t, entry1, value.Entry)
	require.NoError(t, index.checkUsable())

	require.NoError(t, pin1.Close())
	pinClosed = true
	requireDeltaGenerationPathsAbsentEventually(t, root, 0, gen1)
	requireDeltaGenerationPathsExist(t, root, 0, gen2)
}

func TestCheckpointPostRenameFailurePoisonsAndRetainsSelectedArtifacts(t *testing.T) {
	config := productionIndexTestConfig()
	config.SealKeys = 1
	config.RebaseKeys = config.MaxHotKeys
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)

	index.publishRoot = func(root string, candidate *RootIndexCatalog) (bool, error) {
		return writeRootIndexCatalogAtomicWithDirSync(root, candidate, func(string) error {
			return productionPublicationTestError("checkpoint-post-rename")
		})
	}
	key := productionIndexKeyForShard(index, 0, 73)
	entry := AccountIndexEntry{Slot: 703, FileId: 73, Offset: 32}
	require.NoError(t, index.Apply([]deltaIndexMutation{liveDeltaMutation(key, entry)}, nil, true))
	waitForShardedMutableRetryStats(t, index.mutable, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.FatalError != ""
	})
	assert.ErrorIs(t, index.checkUsable(), ErrProductionAccountIndexPoisoned)
	selected, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	selectedGeneration := selected.Shards[0].DeltaGeneration
	require.NotZero(t, selectedGeneration)
	requireDeltaGenerationPathsExist(t, root, 0, selectedGeneration)
	require.NoError(t, index.Close())

	reopened, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	requireProductionCandidate(t, reopened, key, entry, accountIndexSourceDelta)
}

func TestRebasePreRenameFailureRemovesExactBuildAndRetries(t *testing.T) {
	config := productionIndexTestConfig()
	config.SealKeys = 1
	config.RebaseKeys = 1
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	defer func() { _ = index.Close() }()
	setProductionMaintenanceRetryDelay(index, time.Hour)

	var rebaseAttempts atomic.Int32
	failedRoot := make(chan *RootIndexCatalog, 1)
	index.publishRoot = func(root string, candidate *RootIndexCatalog) (bool, error) {
		selected := candidate.Shards[0]
		if selected.BaseGeneration > 1 && selected.DeltaGeneration == 0 {
			if rebaseAttempts.Add(1) == 1 {
				failedRoot <- candidate.Clone()
				return false, productionPublicationTestError("rebase-pre-rename")
			}
		}
		return writeRootIndexCatalogAtomic(root, candidate)
	}

	key := productionIndexKeyForShard(index, 0, 74)
	entry := AccountIndexEntry{Slot: 704, FileId: 74, Offset: 40}
	require.NoError(t, index.Apply([]deltaIndexMutation{liveDeltaMutation(key, entry)}, nil, true))
	var rejected *RootIndexCatalog
	select {
	case rejected = <-failedRoot:
	case <-time.After(10 * time.Second):
		t.Fatal("rebase root publication was not attempted")
	}
	waitForShardedMutableRetryStats(t, index.mutable, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.Shards[0].RebaseRetryPending
	})
	for _, artifact := range []IndexCatalogArtifact{
		rejected.SharedExtentCatalog,
		rejected.Shards[0].BaseIndex,
		rejected.Shards[0].BaseRecords,
	} {
		path, resolveErr := ResolveIndexCatalogArtifactPath(root, artifact)
		require.NoError(t, resolveErr)
		_, statErr := os.Lstat(path)
		assert.ErrorIs(t, statErr, os.ErrNotExist, "rejected build artifact %s", path)
	}
	require.NoError(t, index.checkUsable())

	index.mutable.stateMu.Lock()
	index.mutable.shards[0].rebaseRetryAt = time.Now()
	index.mutable.stateMu.Unlock()
	index.mutable.wakeMaintenanceLoop()
	waitForShardedMutableRetryStats(t, index.mutable, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.RebaseCount == 1 && stats.Shards[0].CheckpointKeys == 0
	})
	assert.Equal(t, int32(2), rebaseAttempts.Load())
	requireProductionCandidate(t, index, key, entry, accountIndexSourceBase)
}

func TestRebasePostRenameFailurePoisonsAndRetainsSelectedBuild(t *testing.T) {
	config := productionIndexTestConfig()
	config.SealKeys = 1
	config.RebaseKeys = 1
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)

	index.publishRoot = func(root string, candidate *RootIndexCatalog) (bool, error) {
		selected := candidate.Shards[0]
		if selected.BaseGeneration > 1 && selected.DeltaGeneration == 0 {
			return writeRootIndexCatalogAtomicWithDirSync(root, candidate, func(string) error {
				return productionPublicationTestError("rebase-post-rename")
			})
		}
		return writeRootIndexCatalogAtomic(root, candidate)
	}
	key := productionIndexKeyForShard(index, 0, 75)
	entry := AccountIndexEntry{Slot: 705, FileId: 75, Offset: 48}
	require.NoError(t, index.Apply([]deltaIndexMutation{liveDeltaMutation(key, entry)}, nil, true))
	waitForShardedMutableRetryStats(t, index.mutable, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.FatalError != ""
	})
	assert.ErrorIs(t, index.checkUsable(), ErrProductionAccountIndexPoisoned)
	selected, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	assert.Greater(t, selected.Shards[0].BaseGeneration, uint64(1))
	assert.Zero(t, selected.Shards[0].DeltaGeneration)
	for _, artifact := range []IndexCatalogArtifact{
		selected.SharedExtentCatalog,
		selected.Shards[0].BaseIndex,
		selected.Shards[0].BaseRecords,
	} {
		path, resolveErr := ResolveIndexCatalogArtifactPath(root, artifact)
		require.NoError(t, resolveErr)
		require.FileExists(t, path)
	}
	require.NoError(t, index.Close())

	reopened, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	requireProductionCandidate(t, reopened, key, entry, accountIndexSourceBase)
}

func TestRewindPreRenameLineageFailureStillCompletesAndRestartsExactly(t *testing.T) {
	fixture := newProductionAccountsDBFixture(t, nil)
	db := openProductionAccountsDBFixture(t, fixture)
	accountV1 := foldAcct(80, 1, []byte("one"))
	accountV2 := foldAcct(80, 2, []byte("two"))
	_, err := db.CommitBatch(foldDeltas(accounts.SlotDelta{Slot: 101, Delta: []*accounts.Account{accountV1}}), 101, nil, []byte("one"))
	require.NoError(t, err)
	_, err = db.CommitBatch(foldDeltas(accounts.SlotDelta{Slot: 102, Delta: []*accounts.Account{accountV2}}), 102, nil, []byte("two"))
	require.NoError(t, err)
	require.NoError(t, db.ProductionIndex.ForceSeal(t.Context()))

	var publications atomic.Int32
	db.ProductionIndex.publishRoot = func(string, *RootIndexCatalog) (bool, error) {
		publications.Add(1)
		return false, productionPublicationTestError("rewind-pre-rename")
	}
	result, err := db.RewindToBatchBoundary(101)
	require.NoError(t, err)
	assert.Equal(t, uint64(101), result.NewThrough)
	assert.Equal(t, int32(1), publications.Load())
	assert.Equal(t, uint64(1), db.lastBatchSeq)
	assert.Equal(t, uint64(101), db.durableThrough.Load())
	rewound, err := db.GetAccount(101, accountV1.Key)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), rewound.Lamports)
	db.CloseDb()

	db = openProductionAccountsDBFixture(t, fixture)
	defer db.CloseDb()
	recovery, err := db.RecoverFoldState()
	require.NoError(t, err)
	assert.Equal(t, uint64(101), recovery.DurableThrough)
	rewound, err = db.GetAccount(101, accountV1.Key)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), rewound.Lamports)
}

func TestRewindRuntimeIncarnationRejectsPreRewindRebaseWhenLineageRenameFails(t *testing.T) {
	config := productionIndexTestConfig()
	config.SealKeys = 1
	config.RebaseKeys = config.MaxHotKeys // construct and control the rebase below
	root, records := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)

	key := records[0].key
	original := records[0].entry
	forward := AccountIndexEntry{Slot: 800, FileId: 80, Offset: 56}
	headMeta := foldMeta{BatchSeq: 2, ThroughSlot: 800, FileId: 80}
	require.NoError(t, index.Apply([]deltaIndexMutation{liveDeltaMutation(key, forward)}, &headMeta, true))
	require.NoError(t, index.ForceSeal(t.Context()))

	shardID := index.mutable.router.Shard(key)
	index.mutable.stateMu.RLock()
	checkpoint := index.mutable.shards[shardID].checkpoint
	covered := make([]uint64, len(index.mutable.shards))
	for shard := range index.mutable.shards {
		covered[shard] = index.mutable.shards[shard].coveredSeq
	}
	index.mutable.stateMu.RUnlock()
	require.NotNil(t, checkpoint)
	require.NoError(t, checkpoint.Retain())
	physicalCoverage := checkpoint.Checkpoint().CoveredSeq()
	checkpointReleased := false
	defer func() {
		if !checkpointReleased {
			_ = checkpoint.Release()
		}
		_ = index.Close()
	}()

	request := ShardedMutableRebaseRequest{
		ShardID:          shardID,
		Checkpoint:       checkpoint,
		CoveredSequence:  physicalCoverage,
		CoveredSequences: covered,
	}
	// Hold the final publication lock. Entry into rebaseGate happens after the
	// build captures rebaseIncarnation, so observing the token gives the rewind a
	// deterministic pre-rewind builder to invalidate.
	index.publishMu.Lock()
	rebaseDone := make(chan error, 1)
	go func() { rebaseDone <- index.rebaseShard(t.Context(), request) }()
	require.Eventually(t, func() bool { return len(index.rebaseGate) == 1 }, 10*time.Second, time.Millisecond)

	index.publishRoot = func(string, *RootIndexCatalog) (bool, error) {
		return false, productionPublicationTestError("rewind-lineage-pre-rename")
	}
	rewindMeta := foldMeta{BatchSeq: 1, ThroughSlot: 100, FileId: original.FileId}
	rewindDone := make(chan error, 1)
	go func() {
		rewindDone <- index.Apply([]deltaIndexMutation{liveDeltaMutation(key, original)}, &rewindMeta, true)
	}()
	require.Eventually(t, func() bool {
		return index.rebaseIncarnation.Load() == 1
	}, 10*time.Second, time.Millisecond, "rewind never invalidated pre-existing rebase")
	index.publishMu.Unlock()

	require.NoError(t, <-rewindDone, "pre-rename advisory lineage failure must not undo the durable rewind")
	rebaseErr := <-rebaseDone
	assert.ErrorIs(t, rebaseErr, ErrIndexGenerationRejected)
	require.NoError(t, index.checkUsable())
	requireProductionCandidate(t, index, key, original, accountIndexSourceDelta)
	require.NoError(t, checkpoint.Release())
	checkpointReleased = true
	require.NoError(t, index.Close())

	reopened, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	requireProductionCandidate(t, reopened, key, original, accountIndexSourceDelta)
	meta, ok := reopened.ReadFoldMeta()
	require.True(t, ok)
	assert.Equal(t, rewindMeta, meta)
}

func TestQueuedRebaseCannotPublishAfterPriorPublisherPoisonsIndex(t *testing.T) {
	config := productionIndexTestConfig()
	config.SealKeys = 1
	config.RebaseKeys = config.MaxHotKeys // create the checkpoint, then drive its rebase explicitly
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)

	key := productionIndexKeyForShard(index, 0, 93)
	entry := AccountIndexEntry{Slot: 930, FileId: 93, Offset: 8}
	require.NoError(t, index.Apply([]deltaIndexMutation{liveDeltaMutation(key, entry)}, nil, true))
	require.NoError(t, index.ForceSeal(t.Context()))

	shardID := index.mutable.router.Shard(key)
	index.mutable.stateMu.RLock()
	checkpoint := index.mutable.shards[shardID].checkpoint
	covered := make([]uint64, len(index.mutable.shards))
	for shard := range index.mutable.shards {
		covered[shard] = index.mutable.shards[shard].coveredSeq
	}
	index.mutable.stateMu.RUnlock()
	require.NotNil(t, checkpoint)
	require.NoError(t, checkpoint.Retain())
	defer func() {
		require.NoError(t, checkpoint.Release())
		require.NoError(t, index.Close())
	}()

	request := ShardedMutableRebaseRequest{
		ShardID:          shardID,
		Checkpoint:       checkpoint,
		CoveredSequence:  checkpoint.Checkpoint().CoveredSeq(),
		CoveredSequences: covered,
	}
	var publications atomic.Int32
	index.publishRoot = func(root string, candidate *RootIndexCatalog) (bool, error) {
		publications.Add(1)
		return writeRootIndexCatalogAtomic(root, candidate)
	}

	// Entering rebaseGate happens after rebaseShard's first usability check.
	// Holding publishMu therefore gives us a deterministic queued publisher to
	// fence after another publisher reports an ambiguous durable outcome.
	index.publishMu.Lock()
	publishHeld := true
	defer func() {
		if publishHeld {
			index.publishMu.Unlock()
		}
	}()
	rebaseDone := make(chan error, 1)
	go func() { rebaseDone <- index.rebaseShard(t.Context(), request) }()
	require.Eventually(t, func() bool { return len(index.rebaseGate) == 1 }, 10*time.Second, time.Millisecond)

	injected := errors.New("injected prior ambiguous publication")
	assert.ErrorIs(t, index.setPoison(injected), ErrProductionAccountIndexPoisoned)
	index.publishMu.Unlock()
	publishHeld = false

	select {
	case err := <-rebaseDone:
		assert.ErrorIs(t, err, ErrProductionAccountIndexPoisoned)
	case <-time.After(10 * time.Second):
		t.Fatal("queued rebase did not return after publication fence was released")
	}
	assert.Zero(t, publications.Load(), "a queued rebase crossed the poison fence and rewrote the selector")
}

func TestCheckpointPublicationRechecksPoisonUnderSerializationFence(t *testing.T) {
	config := productionIndexTestConfig()
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	defer func() { require.NoError(t, index.Close()) }()

	var publications atomic.Int32
	index.publishRoot = func(root string, candidate *RootIndexCatalog) (bool, error) {
		publications.Add(1)
		return writeRootIndexCatalogAtomic(root, candidate)
	}
	injected := errors.New("injected prior ambiguous publication")
	ctx := &poisonOnFirstErrContext{
		Context: context.Background(),
		poison: func() {
			assert.ErrorIs(t, index.setPoison(injected), ErrProductionAccountIndexPoisoned)
		},
	}

	// The empty publication would fail ordinary structural validation if it
	// crossed the fence. The poison error proves the second usability check ran
	// first, while publishMu was held.
	err = index.publishCheckpoint(ctx, ShardedMutableCheckpointPublication{})
	assert.ErrorIs(t, err, ErrProductionAccountIndexPoisoned)
	assert.Zero(t, publications.Load())
}

func TestQueuedRewindCannotOverwriteSelectorAfterPriorPublisherPoisonsIndex(t *testing.T) {
	config := productionIndexTestConfig()
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	defer func() { require.NoError(t, index.Close()) }()

	initialMeta := foldMeta{BatchSeq: 10, ThroughSlot: 100, FileId: 1000}
	require.NoError(t, index.Apply(nil, &initialMeta, true))
	initialRoot, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)

	var publications atomic.Int32
	index.publishRoot = func(root string, candidate *RootIndexCatalog) (bool, error) {
		publications.Add(1)
		return writeRootIndexCatalogAtomic(root, candidate)
	}
	index.publishMu.Lock()
	publishHeld := true
	defer func() {
		if publishHeld {
			index.publishMu.Unlock()
		}
	}()

	// Observing the regressed fold meta proves the rewind passed its first
	// usability check and durably committed its WAL decision before blocking on
	// publishMu. It must not subsequently overwrite an ambiguous selector.
	rewindMeta := foldMeta{BatchSeq: 9, ThroughSlot: 90, FileId: 900}
	rewindDone := make(chan error, 1)
	go func() { rewindDone <- index.Apply(nil, &rewindMeta, true) }()
	require.Eventually(t, func() bool {
		meta, found := index.ReadFoldMeta()
		return found && meta == rewindMeta
	}, 5*time.Second, time.Millisecond)

	injected := errors.New("injected prior ambiguous publication")
	assert.ErrorIs(t, index.setPoison(injected), ErrProductionAccountIndexPoisoned)
	index.publishMu.Unlock()
	publishHeld = false

	select {
	case err := <-rewindDone:
		assert.ErrorIs(t, err, ErrProductionAccountIndexPoisoned)
	case <-time.After(5 * time.Second):
		t.Fatal("queued rewind did not return after publication fence was released")
	}
	assert.Zero(t, publications.Load(), "a queued rewind crossed the poison fence and rewrote the selector")
	rootAfter, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	assert.Equal(t, initialRoot, rootAfter)
	meta, found := index.ReadFoldMeta()
	require.True(t, found)
	assert.Equal(t, rewindMeta, meta, "the WAL decision remains authoritative despite selector fencing")
}
