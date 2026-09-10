package accountsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func productionEnumerationKey(prefix uint64, suffix byte) solana.PublicKey {
	var key solana.PublicKey
	binary.BigEndian.PutUint64(key[:8], prefix)
	key[8] = suffix
	key[31] = suffix ^ 0xa5
	return key
}

func productionEnumerationRecords(
	keys ...solana.PublicKey,
) ([]productionIndexFixtureRecord, map[solana.PublicKey]AccountIndexEntry) {
	records := make([]productionIndexFixtureRecord, len(keys))
	entries := make(map[solana.PublicKey]AccountIndexEntry, len(keys))
	for i, key := range keys {
		entry := AccountIndexEntry{
			Slot:   uint64(100 + i),
			FileId: uint64(200 + i),
			Offset: uint64(i+1) * 8,
		}
		records[i] = productionIndexFixtureRecord{key: key, entry: entry}
		entries[key] = entry
	}
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].key[:], records[j].key[:]) < 0
	})
	return records, entries
}

func openProductionEnumerationFixture(
	t *testing.T,
	config ProductionAccountIndexConfig,
	records []productionIndexFixtureRecord,
) (string, *ProductionAccountIndex) {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, InitializeProductionAccountIndex(
		t.Context(), root, &productionIndexFixtureSource{records: records}, config,
	))
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = index.Close() })
	return root, index
}

func scanProductionEnumeration(
	t *testing.T,
	index *ProductionAccountIndex,
	start uint64,
	end uint64,
) ([]solana.PublicKey, map[solana.PublicKey]AccountIndexEntry) {
	t.Helper()
	keys := make([]solana.PublicKey, 0)
	entries := make(map[solana.PublicKey]AccountIndexEntry)
	require.NoError(t, index.scanPhysicalAccountIndexRange(
		t.Context(), start, end,
		func(key solana.PublicKey, entry AccountIndexEntry) error {
			keys = append(keys, key)
			entries[key] = entry
			return nil
		},
	))
	return keys, entries
}

func TestProductionAccountIndexRangeEnumerationNewestWinsAndBoundaries(t *testing.T) {
	config := productionIndexTestConfig()
	config.RebaseKeys = 16
	outsideLow := productionEnumerationKey(9, 1)
	updatedThenDeleted := productionEnumerationKey(10, 1)
	deletedThenRevived := productionEnumerationKey(10, 2)
	deltaThenUpdated := productionEnumerationKey(10, 3)
	checkpointOnly := productionEnumerationKey(10, 4)
	baseOnly := productionEnumerationKey(10, 5)
	outsideHigh := productionEnumerationKey(11, 1)
	maxFirst := productionEnumerationKey(^uint64(0), 0)
	maxLast := productionEnumerationKey(^uint64(0), 0xff)
	records, baseEntries := productionEnumerationRecords(
		outsideLow,
		updatedThenDeleted,
		deletedThenRevived,
		baseOnly,
		outsideHigh,
		maxFirst,
		maxLast,
	)
	_, index := openProductionEnumerationFixture(t, config, records)

	checkpointUpdated := AccountIndexEntry{Slot: 500, FileId: 600, Offset: 80}
	deltaInitial := AccountIndexEntry{Slot: 501, FileId: 601, Offset: 88}
	checkpointEntry := AccountIndexEntry{Slot: 502, FileId: 602, Offset: 96}
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(updatedThenDeleted, checkpointUpdated),
		tombstoneDeltaMutation(deletedThenRevived),
		liveDeltaMutation(deltaThenUpdated, deltaInitial),
		liveDeltaMutation(checkpointOnly, checkpointEntry),
		tombstoneDeltaMutation(maxFirst),
	}, nil, true))
	require.NoError(t, index.ForceSeal(t.Context()))

	activeOnly := productionEnumerationKey(10, 0)
	activeEntry := AccountIndexEntry{Slot: 700, FileId: 800, Offset: 104}
	revivedEntry := AccountIndexEntry{Slot: 701, FileId: 801, Offset: 112}
	deltaNewest := AccountIndexEntry{Slot: 702, FileId: 802, Offset: 120}
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(activeOnly, activeEntry),
		tombstoneDeltaMutation(updatedThenDeleted),
		liveDeltaMutation(deletedThenRevived, revivedEntry),
		liveDeltaMutation(deltaThenUpdated, deltaNewest),
	}, nil, true))

	keys, got := scanProductionEnumeration(t, index, 10, 10)
	require.Equal(t, []solana.PublicKey{
		activeOnly,
		deletedThenRevived,
		deltaThenUpdated,
		checkpointOnly,
		baseOnly,
	}, keys)
	assert.Equal(t, activeEntry, got[activeOnly])
	assert.Equal(t, revivedEntry, got[deletedThenRevived])
	assert.Equal(t, deltaNewest, got[deltaThenUpdated])
	assert.Equal(t, checkpointEntry, got[checkpointOnly])
	assert.Equal(t, baseEntries[baseOnly], got[baseOnly])
	assert.NotContains(t, got, updatedThenDeleted)
	assert.NotContains(t, got, outsideLow)
	assert.NotContains(t, got, outsideHigh)

	maxKeys, maxEntries := scanProductionEnumeration(t, index, ^uint64(0), ^uint64(0))
	require.Equal(t, []solana.PublicKey{maxLast}, maxKeys)
	assert.Equal(t, baseEntries[maxLast], maxEntries[maxLast])

	accountsDB := &AccountsDb{ProductionIndex: index}
	materialized, err := accountsDB.KeysBetweenPrefixesContext(t.Context(), 10, 10)
	require.NoError(t, err)
	assert.Equal(t, keys, materialized)
	all, err := accountsDB.AllKeysContext(t.Context())
	require.NoError(t, err)
	assert.Len(t, all, 8) // Seven live base/delta keys plus activeOnly.

	err = index.scanPhysicalAccountIndexRange(t.Context(), 11, 10, func(solana.PublicKey, AccountIndexEntry) error { return nil })
	require.ErrorContains(t, err, "start 11 exceeds end 10")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, index.scanPhysicalAccountIndexRange(cancelled, 10, 10, func(solana.PublicKey, AccountIndexEntry) error { return nil }), context.Canceled)
	sentinel := errors.New("stop enumeration")
	assert.ErrorIs(t, index.scanPhysicalAccountIndexRange(t.Context(), 10, 10, func(solana.PublicKey, AccountIndexEntry) error {
		return sentinel
	}), sentinel)
}

func TestProductionAccountIndexRangeEnumerationReadsFrozenEpoch(t *testing.T) {
	config := productionIndexTestConfig()
	config.SealKeys = 1
	config.RebaseKeys = 16
	baseKey := productionEnumerationKey(20, 1)
	frozenKey := productionEnumerationKey(20, 2)
	overriddenFrozenKey := productionEnumerationKey(20, 3)
	records, baseEntries := productionEnumerationRecords(baseKey)
	_, index := openProductionEnumerationFixture(t, config, records)

	realPublish := index.mutable.config.Callbacks.PublishCheckpoint
	publishEntered := make(chan struct{}, 1)
	releasePublish := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releasePublish) }) })
	index.mutable.config.Callbacks.PublishCheckpoint = func(
		ctx context.Context,
		publication ShardedMutableCheckpointPublication,
	) error {
		select {
		case publishEntered <- struct{}{}:
		default:
		}
		select {
		case <-releasePublish:
		case <-ctx.Done():
			return ctx.Err()
		}
		return realPublish(ctx, publication)
	}

	frozenEntry := AccountIndexEntry{Slot: 900, FileId: 901, Offset: 128}
	oldFrozenEntry := AccountIndexEntry{Slot: 902, FileId: 903, Offset: 136}
	require.NoError(t, index.Apply(
		[]deltaIndexMutation{
			liveDeltaMutation(frozenKey, frozenEntry),
			liveDeltaMutation(overriddenFrozenKey, oldFrozenEntry),
		}, nil, true,
	))
	select {
	case <-publishEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint publication did not reach the blocking hook")
	}
	activeEntry := AccountIndexEntry{Slot: 904, FileId: 905, Offset: 144}
	require.NoError(t, index.Apply(
		[]deltaIndexMutation{liveDeltaMutation(overriddenFrozenKey, activeEntry)}, nil, true,
	))

	keys, entries := scanProductionEnumeration(t, index, 20, 20)
	assert.Equal(t, []solana.PublicKey{baseKey, frozenKey, overriddenFrozenKey}, keys)
	assert.Equal(t, baseEntries[baseKey], entries[baseKey])
	assert.Equal(t, frozenEntry, entries[frozenKey])
	assert.Equal(t, activeEntry, entries[overriddenFrozenKey])

	releaseOnce.Do(func() { close(releasePublish) })
	require.NoError(t, index.ForceSeal(t.Context()))
}

func TestProductionAccountIndexRangeEnumerationPinsGenerationAcrossRebase(t *testing.T) {
	config := productionIndexTestConfig()
	config.ShardCount = 1
	config.SealKeys = 1
	config.RebaseKeys = 1
	firstKey := productionEnumerationKey(30, 1)
	secondKey := productionEnumerationKey(31, 1)
	records, baseEntries := productionEnumerationRecords(firstKey, secondKey)
	root, index := openProductionEnumerationFixture(t, config, records)

	before, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	oldBasePath, err := ResolveIndexCatalogArtifactPath(root, before.Shards[0].BaseRecords)
	require.NoError(t, err)

	visitorEntered := make(chan struct{}, 1)
	releaseVisitor := make(chan struct{})
	scanDone := make(chan error, 1)
	var scanMu sync.Mutex
	scanned := make(map[solana.PublicKey]AccountIndexEntry)
	go func() {
		scanDone <- index.scanPhysicalAccountIndexRange(
			context.Background(), 0, ^uint64(0),
			func(key solana.PublicKey, entry AccountIndexEntry) error {
				if key == firstKey {
					visitorEntered <- struct{}{}
					<-releaseVisitor
				}
				scanMu.Lock()
				scanned[key] = entry
				scanMu.Unlock()
				return nil
			},
		)
	}()
	select {
	case <-visitorEntered:
	case <-time.After(5 * time.Second):
		close(releaseVisitor)
		t.Fatal("range scan did not reach its blocking visitor")
	}

	newEntry := AccountIndexEntry{Slot: 1000, FileId: 1001, Offset: 136}
	require.NoError(t, index.Apply(
		[]deltaIndexMutation{liveDeltaMutation(secondKey, newEntry)}, nil, true,
	))
	sealCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, index.ForceSeal(sealCtx))
	after, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	assert.Greater(t, after.Shards[0].BaseGeneration, before.Shards[0].BaseGeneration)
	assert.Zero(t, after.Shards[0].DeltaGeneration)
	_, err = os.Stat(oldBasePath)
	require.NoError(t, err, "old base must remain while the enumeration pins it")

	close(releaseVisitor)
	select {
	case err := <-scanDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("range scan did not finish")
	}
	scanMu.Lock()
	assert.Equal(t, baseEntries[secondKey], scanned[secondKey], "scan must retain its pre-rebase epoch")
	scanMu.Unlock()
	require.Eventually(t, func() bool {
		_, err := os.Stat(oldBasePath)
		return errors.Is(err, os.ErrNotExist)
	}, 5*time.Second, 10*time.Millisecond, "old base was not reclaimed after the enumeration released its pin")

	_, current := scanProductionEnumeration(t, index, 31, 31)
	assert.Equal(t, newEntry, current[secondKey])
}

func TestAccountsDbRangeEnumerationVisitorDoesNotHoldWriterFence(t *testing.T) {
	config := productionIndexTestConfig()
	key := productionEnumerationKey(40, 1)
	records, _ := productionEnumerationRecords(key)
	_, index := openProductionEnumerationFixture(t, config, records)
	accountsDB := &AccountsDb{ProductionIndex: index}

	visitorEntered := make(chan struct{})
	releaseVisitor := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseVisitor) }) }
	t.Cleanup(release)
	scanDone := make(chan error, 1)
	writerFenceHeld := errors.New("account-index writer fence is held inside visitor")
	go func() {
		scanDone <- accountsDB.ScanKeysBetweenPrefixes(
			context.Background(), 0, ^uint64(0),
			func(solana.PublicKey) error {
				// Keep this regression test unwindable: TryLock reports the old
				// self-deadlocking behavior without actually wedging the test.
				if !accountsDB.accountIndexWriteMu.TryLock() {
					return writerFenceHeld
				}
				accountsDB.accountIndexWriteMu.Unlock()
				// A visitor may synchronously re-enter an AccountsDb index writer.
				if err := accountsDB.applyAccountIndexMutations(nil, nil); err != nil {
					return err
				}
				close(visitorEntered)
				<-releaseVisitor
				return nil
			},
		)
	}()

	select {
	case <-visitorEntered:
	case err := <-scanDone:
		release()
		require.NoError(t, err)
		t.Fatal("range scan completed before its visitor blocked")
	case <-time.After(5 * time.Second):
		release()
		t.Fatal("range scan did not reach its visitor")
	}
	require.True(t, accountsDB.accountIndexWriteMu.TryLock(),
		"a blocked visitor must not retain the logical writer fence")
	accountsDB.accountIndexWriteMu.Unlock()

	release()
	select {
	case err := <-scanDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("range scan did not finish after releasing its visitor")
	}
}

func TestProductionAccountIndexBoundsConcurrentDescriptorHeavyScans(t *testing.T) {
	config := productionIndexTestConfig()
	key := productionEnumerationKey(45, 1)
	records, _ := productionEnumerationRecords(key)
	_, index := openProductionEnumerationFixture(t, config, records)

	visitorEntered := make(chan struct{})
	releaseVisitor := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- index.scanPhysicalAccountIndexRange(
			context.Background(), 0, ^uint64(0),
			func(solana.PublicKey, AccountIndexEntry) error {
				close(visitorEntered)
				<-releaseVisitor
				return nil
			},
		)
	}()
	select {
	case <-visitorEntered:
	case <-time.After(5 * time.Second):
		close(releaseVisitor)
		t.Fatal("first enumeration did not reach its visitor")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := index.scanPhysicalAccountIndexRange(
		waitCtx, 0, ^uint64(0), func(solana.PublicKey, AccountIndexEntry) error { return nil },
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	close(releaseVisitor)
	select {
	case err := <-firstDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("first enumeration did not finish")
	}
}

func TestProductionEnumerationFileDescriptorBudgetFailsClearly(t *testing.T) {
	require.NoError(t, validateProductionEnumerationFileDescriptorBudget(1024, 100, 1188))
	err := validateProductionEnumerationFileDescriptorBudget(1024, 101, 1188)
	require.ErrorContains(t, err, "needs up to 1024 sidecar descriptors")
	require.ErrorContains(t, err, "RLIMIT_NOFILE 1188")
	require.ErrorContains(t, err, "raise the process file-descriptor limit")
}

func TestProductionAccountIndexBlockedVisitorDoesNotBlockUnrelatedShardRebase(t *testing.T) {
	config := productionIndexTestConfig()
	config.ShardCount = 2
	config.SealKeys = 1
	config.RebaseKeys = 1
	records, _ := productionEnumerationRecords(productionEnumerationKey(49, 1))
	root, index := openProductionEnumerationFixture(t, config, records)
	shardZeroKey := productionIndexKeyForShard(index, 0, 500)
	shardOneKey := productionIndexKeyForShard(index, 1, 501)
	oldZeroEntry := AccountIndexEntry{Slot: 1090, FileId: 1091, Offset: 144}
	shardOneEntry := AccountIndexEntry{Slot: 1092, FileId: 1093, Offset: 152}
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(shardZeroKey, oldZeroEntry),
		liveDeltaMutation(shardOneKey, shardOneEntry),
	}, nil, true))
	require.NoError(t, index.ForceSeal(t.Context()))

	before, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	visitorEntered := make(chan struct{})
	releaseVisitor := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseVisitor) }) }
	t.Cleanup(release)
	scanDone := make(chan error, 1)
	var scanMu sync.Mutex
	scanned := make(map[solana.PublicKey]AccountIndexEntry)
	go func() {
		scanDone <- index.scanPhysicalAccountIndexRange(
			context.Background(), 0, ^uint64(0),
			func(key solana.PublicKey, entry AccountIndexEntry) error {
				if key == shardOneKey {
					close(visitorEntered)
					<-releaseVisitor
				}
				scanMu.Lock()
				scanned[key] = entry
				scanMu.Unlock()
				return nil
			},
		)
	}()
	select {
	case <-visitorEntered:
	case err := <-scanDone:
		release()
		require.NoError(t, err)
		t.Fatal("range scan completed before reaching shard one's visitor")
	case <-time.After(5 * time.Second):
		release()
		t.Fatal("range scan did not reach shard one's visitor")
	}

	newEntry := AccountIndexEntry{Slot: 1100, FileId: 1101, Offset: 160}
	require.NoError(t, index.Apply(
		[]deltaIndexMutation{liveDeltaMutation(shardZeroKey, newEntry)}, nil, true,
	))
	sealCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, index.ForceSeal(sealCtx),
		"a visitor holding shard one's read lock must not stall shard zero's root publication")
	after, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	assert.Greater(t, after.Shards[0].BaseGeneration, before.Shards[0].BaseGeneration)
	assert.Zero(t, after.Shards[0].DeltaGeneration)
	assert.Equal(t, before.Shards[1].BaseGeneration, after.Shards[1].BaseGeneration)

	release()
	select {
	case err := <-scanDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("range scan did not finish after releasing its visitor")
	}
	scanMu.Lock()
	assert.Equal(t, oldZeroEntry, scanned[shardZeroKey],
		"the pinned scan must keep its pre-rebase epoch")
	scanMu.Unlock()
	_, current := scanProductionEnumeration(t, index, 0, ^uint64(0))
	assert.Equal(t, newEntry, current[shardZeroKey])
}
