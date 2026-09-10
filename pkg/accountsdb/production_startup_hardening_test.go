package accountsdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProductionAccountIndexStoreLockSerializesStartupOwners(t *testing.T) {
	config := productionIndexTestConfig()
	root, _ := initializeProductionIndexFixture(t, config)
	catalog, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)

	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(root, ProductionAccountIndexLockFileName))

	_, err = OpenProductionAccountIndex(root, config)
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse)
	err = ValidateProductionAccountIndexArtifacts(root)
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse)
	_, err = GarbageCollectShardedImmutableIndexOrphans(root, catalog)
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse)
	err = InitializeProductionAccountIndex(
		t.Context(),
		root,
		&productionIndexFixtureSource{records: productionIndexFixtureRecords()},
		config,
	)
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse)

	require.NoError(t, index.Close())
	require.NoError(t, ValidateProductionAccountIndexArtifacts(root))
	lockInfo, err := os.Lstat(filepath.Join(root, ProductionAccountIndexLockFileName))
	require.NoError(t, err)
	assert.True(t, lockInfo.Mode().IsRegular())
}

func TestAccountsDbOpenAcquiresStoreLockBeforeReadingBootstrapState(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "accounts"), 0o755))
	guard, err := AcquireExclusiveProductionAccountIndexStore(root)
	require.NoError(t, err)

	// Deliberately install an invalid selector while another owner holds the
	// lock. An opener must report exclusion without inspecting this state; in
	// particular it cannot reconcile and cache a stale appendvec high-water ID.
	require.NoError(t, os.WriteFile(
		filepath.Join(root, LargestFileIDFileName), []byte("corrupt"), 0o644,
	))
	_, err = OpenDbWithProductionAccountIndexConfig(root, productionIndexTestConfig())
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse)

	require.NoError(t, guard.Close())
	_, err = OpenDbWithProductionAccountIndexConfig(root, productionIndexTestConfig())
	require.ErrorIs(t, err, ErrInvalidLargestFileID)
}

func TestTransitionalAccountsDbRetainsStoreLockUntilShutdown(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "accounts"), 0o755))
	require.NoError(t, WriteLargestFileID(root, 0))
	require.NoError(t, WriteBootstrapHighFileID(root, 0))
	require.NoError(t, InitializeMutableAccountIndex(root))

	db, err := OpenDbWithProductionAccountIndexConfig(root, productionIndexTestConfig())
	require.NoError(t, err)
	require.Nil(t, db.ProductionIndex)
	require.NotNil(t, db.storeLock)

	_, err = OpenDbWithProductionAccountIndexConfig(root, productionIndexTestConfig())
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse)
	require.NoError(t, db.CloseDb())

	reopened, err := OpenDbWithProductionAccountIndexConfig(root, productionIndexTestConfig())
	require.NoError(t, err)
	require.NoError(t, reopened.CloseDb())
}

func TestProductionAccountIndexStoreGuardSpansOperationsAndTransfers(t *testing.T) {
	root := t.TempDir()
	guard, err := AcquireExclusiveProductionAccountIndexStore(root)
	require.NoError(t, err)

	err = WithExclusiveProductionAccountIndexStore(root, func() error { return nil })
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse)
	require.Error(t, guard.withProductionAccountIndexStoreLock(t.TempDir(), func(*productionAccountIndexStoreLock) error {
		return nil
	}))

	var transferred *productionAccountIndexStoreLock
	err = guard.transferProductionAccountIndexStoreLockOnSuccess(
		root,
		func(lock *productionAccountIndexStoreLock) error {
			transferred = lock
			return nil
		},
	)
	require.NoError(t, err)
	require.NotNil(t, transferred)
	require.NoError(t, guard.Close(), "close after transfer must be idempotent")

	err = WithExclusiveProductionAccountIndexStore(root, func() error { return nil })
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse)
	require.NoError(t, transferred.Close())
	require.NoError(t, WithExclusiveProductionAccountIndexStore(root, func() error { return nil }))
}

func TestProductionAccountIndexStoreGuardBorrowsForInitializeAndTransfersToOpen(t *testing.T) {
	root := t.TempDir()
	guard, err := AcquireExclusiveProductionAccountIndexStore(root)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, guard.Close()) })
	config := productionIndexTestConfig()
	require.NoError(t, InitializeProductionAccountIndexWithStoreGuard(
		t.Context(),
		root,
		&productionIndexFixtureSource{records: productionIndexFixtureRecords()},
		config,
		guard,
	))

	_, err = OpenProductionAccountIndex(root, config)
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse)
	index, err := OpenProductionAccountIndexWithStoreGuard(root, config, guard)
	require.NoError(t, err)
	require.NoError(t, guard.Close(), "successful open transferred the guard")
	_, err = OpenProductionAccountIndex(root, config)
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse)
	require.NoError(t, index.Close())
	reopened, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

func TestGarbageCollectRejectsStaleSelector(t *testing.T) {
	config := productionIndexTestConfig()
	root, _ := initializeProductionIndexFixture(t, config)
	selected, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)

	staleBytes, err := selected.MarshalBinary()
	require.NoError(t, err)
	stale, err := UnmarshalRootIndexCatalog(staleBytes)
	require.NoError(t, err)
	stale.Generation++

	orphanDirectory := filepath.Join(
		root,
		"accounts-index-v2-g00000000000000000002-0123456789abcdef0123456789abcdef",
	)
	require.NoError(t, os.Mkdir(orphanDirectory, 0o755))
	orphan := filepath.Join(orphanDirectory, "shard-0000.stmh")
	require.NoError(t, os.WriteFile(orphan, []byte("unselected"), 0o600))

	_, err = GarbageCollectShardedImmutableIndexOrphans(root, stale)
	require.ErrorIs(t, err, ErrInvalidRootIndexCatalog)
	require.ErrorContains(t, err, "superseded")
	assert.FileExists(t, orphan)
}

func TestInitializeProductionAccountIndexCleansOnlyBeforeRootRename(t *testing.T) {
	config := productionIndexTestConfig()
	injected := errors.New("injected root publication failure")

	t.Run("before rename", func(t *testing.T) {
		root := t.TempDir()
		err := initializeProductionAccountIndex(
			t.Context(),
			root,
			&productionIndexFixtureSource{records: productionIndexFixtureRecords()},
			config,
			func(string, *RootIndexCatalog) (bool, error) {
				return false, injected
			},
		)
		require.ErrorIs(t, err, injected)
		assert.NoFileExists(t, filepath.Join(root, RootIndexCatalogFileName))
		assert.NoFileExists(t, filepath.Join(root, ShardedDeltaIndexJournalFileName))
		assert.Empty(t, productionGenerationDirectoriesForTest(t, root))
		assert.FileExists(t, filepath.Join(root, ProductionAccountIndexLockFileName))
	})

	t.Run("after rename", func(t *testing.T) {
		root := t.TempDir()
		err := initializeProductionAccountIndex(
			t.Context(),
			root,
			&productionIndexFixtureSource{records: productionIndexFixtureRecords()},
			config,
			func(root string, catalog *RootIndexCatalog) (bool, error) {
				return writeRootIndexCatalogAtomicWithDirSync(
					root,
					catalog,
					func(string) error { return injected },
				)
			},
		)
		require.ErrorIs(t, err, injected)
		assert.FileExists(t, filepath.Join(root, RootIndexCatalogFileName))
		assert.FileExists(t, filepath.Join(root, ShardedDeltaIndexJournalFileName))
		assert.NotEmpty(t, productionGenerationDirectoriesForTest(t, root))
		// The injected error models directory-fsync ambiguity. Because rename
		// actually happened, every selected dependency must remain openable.
		require.NoError(t, ValidateProductionAccountIndexArtifacts(root))
	})
}

func productionGenerationDirectoriesForTest(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	var generations []string
	for _, entry := range entries {
		if entry.IsDir() && IsProductionAccountIndexGenerationDirectory(entry.Name()) {
			generations = append(generations, entry.Name())
		}
	}
	return generations
}

func TestOpenRootSelectedDeltaBindsCatalogIdentityWithoutSecondHashPass(t *testing.T) {
	config := productionIndexTestConfig()
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	key := productionIndexKeyForShard(index, 0, 90)
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(key, AccountIndexEntry{Slot: 900}),
	}, nil, true))
	require.NoError(t, index.ForceSeal(context.Background()))
	require.NoError(t, index.Close())

	catalog, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	require.NotZero(t, catalog.Shards[0].DeltaGeneration)
	catalog.Shards[0].DeltaIndex.SHA256[0] ^= 0xff
	require.NoError(t, WriteRootIndexCatalogAtomic(root, catalog))

	_, err = OpenProductionAccountIndex(root, config)
	require.ErrorIs(t, err, ErrInvalidRootIndexCatalog)
	require.ErrorContains(t, err, "root and descriptor index identities differ")
}

func TestProductionStartupPrunesMarkersLeftAfterBasePublication(t *testing.T) {
	config := productionIndexTestConfig()
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	require.NoError(t, index.Apply([]deltaIndexMutation{
		retireDeltaMutation(201, 1),
		retireDeltaMutation(202, 2),
		retireDeltaMutation(203, 3),
		retireDeltaMutation(204, 4),
	}, nil, true))
	require.NoError(t, index.Close())

	// Model a crash after every shard's successor base was published through
	// WAL sequence 1, but before tryPruneRetirementMarkers rewrote the WAL. The
	// marker targets are absent from the fixture base, so this is a safe and
	// structurally accurate durable state for startup recovery.
	catalog, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	catalog.Generation++
	catalog.CoveredSequence = 1
	for shardID := range catalog.Shards {
		catalog.Shards[shardID].BaseCoveredSequence = 1
	}
	require.NoError(t, WriteRootIndexCatalogAtomic(root, catalog))

	tight := config
	tight.MaxHotKeys = 1
	tight.MaxHotBytes = DefaultShardedMutableBytesPerKey
	tight.SealKeys = 1
	tight.RebaseKeys = 1
	restarted, err := OpenProductionAccountIndex(root, tight)
	require.NoError(t, err)
	stats := restarted.Stats()
	assert.Zero(t, stats.RetiredAppendVecs)
	assert.Zero(t, stats.HotBytes)
	assert.Equal(t, uint64(1), stats.RewriteCount)
	require.NoError(t, restarted.Close())

	// Startup published the prune as a replacement WAL; it survives another
	// restart even though there is no longer anything to filter in memory.
	reopened, err := OpenProductionAccountIndex(root, tight)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assert.Zero(t, reopened.Stats().RetiredAppendVecs)
	assert.Zero(t, reopened.Stats().HotBytes)
}
