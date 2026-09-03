package accountsdb

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type productionIndexFixtureRecord struct {
	key   solana.PublicKey
	entry AccountIndexEntry
}

type productionIndexFixtureSource struct {
	records []productionIndexFixtureRecord
	scans   atomic.Uint64
}

func (source *productionIndexFixtureSource) Scan(
	ctx context.Context,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	source.scans.Add(1)
	for i := range source.records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(source.records[i].key, source.records[i].entry); err != nil {
			return err
		}
	}
	return nil
}

func productionIndexFixtureKey(prefix, ordinal byte) solana.PublicKey {
	var key solana.PublicKey
	// Keep the bootstrap source bytewise sorted and visually distinct. Durable
	// shard placement is deliberately unrelated to this raw prefix.
	key[0] = prefix << 6
	key[7] = ordinal ^ 0x5a
	key[31] = ordinal
	return key
}

func productionIndexFixtureRecords() []productionIndexFixtureRecord {
	records := []productionIndexFixtureRecord{
		{productionIndexFixtureKey(0, 1), AccountIndexEntry{Slot: 100, FileId: 200, Offset: 8}},
		{productionIndexFixtureKey(1, 2), AccountIndexEntry{Slot: 101, FileId: 201, Offset: 16}},
		{productionIndexFixtureKey(2, 3), AccountIndexEntry{Slot: 102, FileId: 202, Offset: 24}},
		{productionIndexFixtureKey(3, 4), AccountIndexEntry{Slot: 103, FileId: 203, Offset: 32}},
	}
	sort.Slice(records, func(i, j int) bool { return bytes.Compare(records[i].key[:], records[j].key[:]) < 0 })
	return records
}

func productionIndexTestConfig() ProductionAccountIndexConfig {
	return ProductionAccountIndexConfig{
		ShardCount:          4,
		MaxHotKeys:          16,
		MaxHotBytes:         16 * DefaultShardedMutableBytesPerKey,
		SealKeys:            8,
		SealMaxAge:          time.Hour,
		RebaseKeys:          16,
		JournalRewriteBytes: 1 << 20,
		CheckpointWorkers:   1,
		MaxConcurrentSeals:  1,
		RebaseWorkers:       1,
	}
}

func initializeProductionIndexFixture(
	t *testing.T,
	config ProductionAccountIndexConfig,
) (string, []productionIndexFixtureRecord) {
	t.Helper()
	root := t.TempDir()
	records := productionIndexFixtureRecords()
	source := &productionIndexFixtureSource{records: records}
	require.NoError(t, InitializeProductionAccountIndex(t.Context(), root, source, config))
	require.Equal(t, uint64(2), source.scans.Load(), "initial base source should be streamed twice")
	return root, records
}

func requireProductionCandidate(
	t *testing.T,
	index *ProductionAccountIndex,
	key solana.PublicKey,
	want AccountIndexEntry,
	wantSource accountIndexSource,
) {
	t.Helper()
	entry, source, found, err := index.LookupCandidate(key)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, want, entry)
	assert.Equal(t, wantSource, source)
}

func productionIndexKeyForShard(
	index *ProductionAccountIndex,
	shardID uint32,
	ordinal uint64,
) solana.PublicKey {
	return shardedMutableTestKeyForRouter(index.mutable.router, shardID, ordinal)
}

func TestProductionAccountIndexInitializeOpenAndBaseLookup(t *testing.T) {
	config := productionIndexTestConfig()
	root, records := initializeProductionIndexFixture(t, config)

	catalog, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), catalog.Generation)
	assert.Equal(t, uint64(0), catalog.CoveredSequence)
	assert.Len(t, catalog.Shards, config.ShardCount)
	for i := range catalog.Shards {
		assert.Equal(t, uint64(1), catalog.Shards[i].BaseGeneration)
		assert.Zero(t, catalog.Shards[i].DeltaGeneration)
	}

	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	for _, record := range records {
		requireProductionCandidate(t, index, record.key, record.entry, accountIndexSourceBase)
	}
	require.NoError(t, index.Close())

	_, err = OpenProductionAccountIndex(root, ProductionAccountIndexConfig{
		ShardCount: 2, MaxHotKeys: 16, MaxHotBytes: 16 * DefaultShardedMutableBytesPerKey,
		SealKeys: 8, SealMaxAge: time.Hour, RebaseKeys: 16,
		JournalRewriteBytes: 1 << 20, CheckpointWorkers: 1, MaxConcurrentSeals: 1, RebaseWorkers: 1,
	})
	require.ErrorContains(t, err, "does not match persisted")
	assert.ErrorIs(t, InitializeProductionAccountIndex(t.Context(), root,
		&productionIndexFixtureSource{records: records}, config), ErrProductionAccountIndexExists)
}

func TestProductionAccountIndexConcurrentSealsPublishOutOfFreezeOrder(t *testing.T) {
	config := productionIndexTestConfig()
	config.SealKeys = 1
	config.MaxConcurrentSeals = 2
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	var releaseOnce sync.Once
	releaseFirst := make(chan struct{})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseFirst) })
		_ = index.Close()
	})

	realPublish := index.mutable.config.Callbacks.PublishCheckpoint
	firstEntered := make(chan struct{}, 1)
	secondPublished := make(chan error, 1)
	index.mutable.config.Callbacks.PublishCheckpoint = func(
		ctx context.Context,
		publication ShardedMutableCheckpointPublication,
	) error {
		if publication.ShardID == 0 && publication.CoveredSequence == 1 {
			firstEntered <- struct{}{}
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		publishErr := realPublish(ctx, publication)
		if publication.ShardID == 1 && publication.CoveredSequence == 2 {
			secondPublished <- publishErr
		}
		return publishErr
	}

	key0 := productionIndexKeyForShard(index, 0, 40)
	key1 := productionIndexKeyForShard(index, 1, 41)
	want0 := AccountIndexEntry{Slot: 400, FileId: 40, Offset: 8}
	want1 := AccountIndexEntry{Slot: 410, FileId: 41, Offset: 16}
	require.NoError(t, index.Apply([]deltaIndexMutation{liveDeltaMutation(key0, want0)}, nil, true))
	select {
	case <-firstEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("first shard publication did not block")
	}
	require.NoError(t, index.Apply([]deltaIndexMutation{liveDeltaMutation(key1, want1)}, nil, true))
	select {
	case publishErr := <-secondPublished:
		require.NoError(t, publishErr)
	case <-time.After(5 * time.Second):
		t.Fatal("newer shard publication did not overtake blocked older publication")
	}

	intermediate, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), intermediate.Shards[0].effectiveCoveredSequence())
	assert.Equal(t, uint64(2), intermediate.Shards[1].effectiveCoveredSequence())
	assert.Equal(t, uint64(2), intermediate.Shards[2].effectiveCoveredSequence())
	assert.Equal(t, uint64(2), intermediate.Shards[3].effectiveCoveredSequence())

	releaseOnce.Do(func() { close(releaseFirst) })
	require.NoError(t, index.ForceSeal(context.Background()))
	assert.Empty(t, index.Stats().FatalError)
	finalRoot, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	wantCoverage := []uint64{1, 2, 2, 2}
	for shardID, want := range wantCoverage {
		assert.Equal(t, want, finalRoot.Shards[shardID].effectiveCoveredSequence(), "shard %d", shardID)
	}
	requireProductionCandidate(t, index, key0, want0, accountIndexSourceDelta)
	requireProductionCandidate(t, index, key1, want1, accountIndexSourceDelta)

	require.NoError(t, index.Close())
	reopened, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	requireProductionCandidate(t, reopened, key0, want0, accountIndexSourceDelta)
	requireProductionCandidate(t, reopened, key1, want1, accountIndexSourceDelta)
}

func TestProductionAccountIndexAtomicMultiShardWALReplayAndTombstone(t *testing.T) {
	config := productionIndexTestConfig()
	root, records := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)

	key0 := productionIndexKeyForShard(index, 0, 20)
	key3 := productionIndexKeyForShard(index, 3, 21)
	entry0 := AccountIndexEntry{Slot: 200, FileId: 300, Offset: 40}
	entry3 := AccountIndexEntry{Slot: 201, FileId: 301, Offset: 48}
	meta := foldMeta{BatchSeq: 1, ThroughSlot: 201, FileId: 301}
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(key0, entry0),
		liveDeltaMutation(key3, entry3),
		tombstoneDeltaMutation(records[1].key),
		retireDeltaMutation(90, 91),
	}, &meta, true))
	requireProductionCandidate(t, index, key0, entry0, accountIndexSourceDelta)
	requireProductionCandidate(t, index, key3, entry3, accountIndexSourceDelta)
	_, source, found, err := index.LookupCandidate(records[1].key)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, accountIndexSourceNone, source)
	assert.True(t, index.IsRetired(90, 91))
	require.NoError(t, index.Close())

	reopened, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	requireProductionCandidate(t, reopened, key0, entry0, accountIndexSourceDelta)
	requireProductionCandidate(t, reopened, key3, entry3, accountIndexSourceDelta)
	_, source, found, err = reopened.LookupCandidate(records[1].key)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, accountIndexSourceNone, source)
	assert.True(t, reopened.IsRetired(90, 91))
	gotMeta, hasMeta := reopened.ReadFoldMeta()
	assert.True(t, hasMeta)
	assert.Equal(t, meta, gotMeta)
}

func TestProductionAccountIndexForcedCheckpointPublishesAndReopens(t *testing.T) {
	config := productionIndexTestConfig()
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)

	key0 := productionIndexKeyForShard(index, 0, 30)
	key2 := productionIndexKeyForShard(index, 2, 31)
	deletedKey := productionIndexKeyForShard(index, 3, 32)
	entry0 := AccountIndexEntry{Slot: 300, FileId: 400, Offset: 56}
	entry2 := AccountIndexEntry{Slot: 301, FileId: 401, Offset: 64}
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(key0, entry0),
		liveDeltaMutation(key2, entry2),
		tombstoneDeltaMutation(deletedKey),
	}, nil, true))
	require.NoError(t, index.ForceSeal(t.Context()))

	catalog, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	assert.Greater(t, catalog.Generation, uint64(1))
	assert.Equal(t, uint64(1), catalog.CoveredSequence)
	assert.NotZero(t, catalog.Shards[0].DeltaGeneration)
	assert.NotZero(t, catalog.Shards[2].DeltaGeneration)
	assert.NotZero(t, catalog.Shards[3].DeltaGeneration)
	for i := range catalog.Shards {
		assert.Equal(t, uint64(1), catalog.Shards[i].effectiveCoveredSequence())
	}
	require.NoError(t, index.Close())

	reopened, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	requireProductionCandidate(t, reopened, key0, entry0, accountIndexSourceDelta)
	requireProductionCandidate(t, reopened, key2, entry2, accountIndexSourceDelta)
	_, _, found, err := reopened.LookupCandidate(deletedKey)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestProductionAccountIndexRollingRebaseAtLowThreshold(t *testing.T) {
	config := productionIndexTestConfig()
	config.MaxHotKeys = 8
	config.MaxHotBytes = 8 * DefaultShardedMutableBytesPerKey
	config.SealKeys = 1
	config.RebaseKeys = 1
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)

	newKey := productionIndexKeyForShard(index, 0, 40)
	deletedKey := productionIndexKeyForShard(index, 1, 41)
	newEntry := AccountIndexEntry{Slot: 400, FileId: 500, Offset: 72}
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(newKey, newEntry),
		tombstoneDeltaMutation(deletedKey),
	}, nil, true))
	require.NoError(t, index.ForceSeal(t.Context()))

	catalog, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, catalog.Generation, uint64(5), "two changed shards each publish delta then base")
	assert.Equal(t, uint64(1), catalog.CoveredSequence)
	for i := range catalog.Shards {
		assert.Equal(t, uint64(1), catalog.Shards[i].effectiveCoveredSequence())
		assert.Zero(t, catalog.Shards[i].DeltaGeneration)
	}
	assert.Greater(t, catalog.Shards[0].BaseGeneration, uint64(1))
	assert.Greater(t, catalog.Shards[1].BaseGeneration, uint64(1))
	requireProductionCandidate(t, index, newKey, newEntry, accountIndexSourceBase)
	_, _, found, err := index.LookupCandidate(deletedKey)
	require.NoError(t, err)
	assert.False(t, found)
	require.NoError(t, index.Close())

	reopened, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	requireProductionCandidate(t, reopened, newKey, newEntry, accountIndexSourceBase)
	_, _, found, err = reopened.LookupCandidate(deletedKey)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestProductionAccountIndexSnapshotPinsOldGenerationAcrossCheckpointPublication(t *testing.T) {
	config := productionIndexTestConfig()
	root, records := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	key := records[0].key
	snapshot, err := index.NewSnapshot([]solana.PublicKey{key})
	require.NoError(t, err)

	newEntry := AccountIndexEntry{Slot: 500, FileId: 600, Offset: 80}
	require.NoError(t, index.Apply([]deltaIndexMutation{liveDeltaMutation(key, newEntry)}, nil, true))
	require.NoError(t, index.ForceSeal(t.Context()))
	requireProductionCandidate(t, index, key, newEntry, accountIndexSourceDelta)

	value, source, found, err := snapshot.LookupAt(0)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, records[0].entry, value.Entry)
	assert.Equal(t, accountIndexSourceBase, source)
	catalog, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	assert.Greater(t, catalog.Generation, uint64(1))
	require.NoError(t, snapshot.Close())
	require.NoError(t, index.Close())
}

func TestProductionAccountIndexRestartTruncatesTornWALTail(t *testing.T) {
	config := productionIndexTestConfig()
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	firstKey := productionIndexKeyForShard(index, 0, 50)
	secondKey := productionIndexKeyForShard(index, 1, 51)
	thirdKey := productionIndexKeyForShard(index, 3, 52)
	firstEntry := AccountIndexEntry{Slot: 600, FileId: 700, Offset: 88}
	require.NoError(t, index.Apply([]deltaIndexMutation{liveDeltaMutation(firstKey, firstEntry)}, nil, true))
	firstEnd := index.Stats().JournalBytes
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(secondKey, AccountIndexEntry{Slot: 601}),
		liveDeltaMutation(thirdKey, AccountIndexEntry{Slot: 602}),
		retireDeltaMutation(601, 701),
	}, nil, true))
	require.NoError(t, index.Close())

	journal := filepath.Join(root, ShardedDeltaIndexJournalFileName)
	require.NoError(t, os.Truncate(journal, int64(firstEnd)+deltaFrameHeaderSize+deltaMutationSize+7))
	reopened, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	requireProductionCandidate(t, reopened, firstKey, firstEntry, accountIndexSourceDelta)
	for _, key := range []solana.PublicKey{secondKey, thirdKey} {
		_, _, found, err := reopened.LookupCandidate(key)
		require.NoError(t, err)
		assert.False(t, found)
	}
	assert.False(t, reopened.IsRetired(601, 701))
	assert.Equal(t, firstEnd, reopened.Stats().JournalBytes)
}

func TestProductionAccountIndexRejectsUnsafeAndOversizedMemoryConfigurations(t *testing.T) {
	invalid := productionIndexTestConfig()
	invalid.MaxHotBytes = invalid.MaxHotKeys
	root := t.TempDir()
	err := InitializeProductionAccountIndex(t.Context(), root,
		&productionIndexFixtureSource{records: productionIndexFixtureRecords()}, invalid)
	require.ErrorContains(t, err, "cannot hold")
	_, statErr := os.Stat(filepath.Join(root, RootIndexCatalogFileName))
	assert.ErrorIs(t, statErr, os.ErrNotExist)

	config := productionIndexTestConfig()
	config.MaxHotKeys = 2
	config.MaxHotBytes = 2 * DefaultShardedMutableBytesPerKey
	config.SealKeys = 2
	config.RebaseKeys = 2
	root, _ = initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = index.Close() })
	mutations := make([]deltaIndexMutation, 3)
	for i := range mutations {
		mutations[i] = liveDeltaMutation(
			productionIndexKeyForShard(index, uint32(i), uint64(60+i)),
			AccountIndexEntry{Slot: uint64(700 + i)},
		)
	}
	err = index.Apply(mutations, nil, true)
	assert.ErrorIs(t, err, ErrShardedMutableCapacity)
	stats := index.Stats()
	assert.Zero(t, stats.JournalSequence)
	assert.Zero(t, stats.HotKeys)
	assert.LessOrEqual(t, stats.HotBytes, config.MaxHotBytes)
	assert.Equal(t, config.MaxHotBytes, stats.MaxHotBytes)
	assert.Equal(t, config.MaxHotKeys, stats.MaxHotKeys)
}

func TestProductionAccountIndexMissingRootAndLegacyMigrationFailClosed(t *testing.T) {
	config := productionIndexTestConfig()
	root := t.TempDir()
	_, err := OpenProductionAccountIndex(root, config)
	require.Error(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, DeltaIndexJournalFileName), []byte("legacy"), 0o644))
	err = InitializeProductionAccountIndex(t.Context(), root,
		&productionIndexFixtureSource{records: productionIndexFixtureRecords()}, config)
	assert.True(t, errors.Is(err, ErrAccountIndexMigrationRequired), "error: %v", err)
}
