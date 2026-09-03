package accountsdb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func prepareSnapshotVerifyFixture(
	t *testing.T,
	fixture snapshotVerifyFixture,
) *PreparedSnapshotAccountIndex {
	t.Helper()
	hash, capitalization := snapshotVerifyExpected(t, fixture.selected)
	prepared, err := PrepareVerifiedSnapshotAccountIndexWithStoreGuard(
		t.Context(),
		fixture.root,
		fixture.specs,
		hash,
		capitalization,
		fixture.config,
		fixture.guard,
	)
	require.NoError(t, err)
	require.NotNil(t, prepared)
	return prepared
}

func publishSnapshotVerifyFileHighWater(t *testing.T, fixture snapshotVerifyFixture) {
	t.Helper()
	var largest uint64
	for _, spec := range fixture.specs {
		largest = max(largest, spec.FileID)
	}
	require.NoError(t, WriteLargestFileID(fixture.root, largest))
	require.NoError(t, WriteBootstrapHighFileID(fixture.root, largest))
}

func oneAccountPreparedFixture(t *testing.T) snapshotVerifyFixture {
	t.Helper()
	return newSnapshotVerifyFixture(t, []snapshotVerifyFile{{
		slot: 10, fileID: 20,
		accounts: []*accounts.Account{snapshotVerifyAccount(1, 9, "value")},
	}})
}

func TestPreparedSnapshotAccountIndexAdoptsSameVerifiedImmutableGeneration(t *testing.T) {
	fixture := oneAccountPreparedFixture(t)
	prepared := prepareSnapshotVerifyFixture(t, fixture)
	retainedImmutable := prepared.immutable
	require.NotNil(t, retainedImmutable)
	publishSnapshotVerifyFileHighWater(t, fixture)

	db, err := OpenDbWithPreparedSnapshotAccountIndexAndStoreGuard(
		fixture.root, prepared, fixture.guard,
	)
	require.NoError(t, err)
	require.NotNil(t, db.ProductionIndex)
	require.NoError(t, prepared.Close(), "Close after adoption must be idempotent")

	view, err := db.ProductionIndex.view.Acquire()
	require.NoError(t, err)
	assert.Same(t, retainedImmutable, view.Payload(), "adoption must reuse the verifier-opened generation")
	require.NoError(t, view.Close())

	require.NoError(t, fixture.guard.Close(), "successful adoption empties the guard shell")
	err = WithExclusiveProductionAccountIndexStore(fixture.root, func() error { return nil })
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse)
	require.NoError(t, db.CloseDb())
	require.NoError(t, WithExclusiveProductionAccountIndexStore(fixture.root, func() error { return nil }))
}

func TestPreparedSnapshotAccountIndexPreflightFailureIsReusable(t *testing.T) {
	fixture := oneAccountPreparedFixture(t)
	prepared := prepareSnapshotVerifyFixture(t, fixture)

	// largest_file_id is intentionally absent. This ordinary AccountsDB
	// preflight happens before immutable ownership is consumed.
	_, err := OpenDbWithPreparedSnapshotAccountIndexAndStoreGuard(
		fixture.root, prepared, fixture.guard,
	)
	require.ErrorIs(t, err, ErrInvalidLargestFileID)
	_, configErr := prepared.productionConfig()
	require.NoError(t, configErr, "a pre-consumption failure keeps the prepared handle reusable")

	publishSnapshotVerifyFileHighWater(t, fixture)
	db, err := OpenDbWithPreparedSnapshotAccountIndexAndStoreGuard(
		fixture.root, prepared, fixture.guard,
	)
	require.NoError(t, err)
	require.NoError(t, db.CloseDb())
}

func TestPreparedSnapshotAccountIndexBankhashFailureConsumesRuntimeButRetainsGuard(t *testing.T) {
	fixture := oneAccountPreparedFixture(t)
	prepared := prepareSnapshotVerifyFixture(t, fixture)
	publishSnapshotVerifyFileHighWater(t, fixture)

	bankhashPath := filepath.Join(fixture.root, "bankhash_db")
	require.NoError(t, os.WriteFile(bankhashPath, []byte("not a directory"), 0o644))
	_, err := OpenDbWithPreparedSnapshotAccountIndexAndStoreGuard(
		fixture.root, prepared, fixture.guard,
	)
	require.Error(t, err)
	require.Error(t, func() error {
		_, configErr := prepared.productionConfig()
		return configErr
	}(), "failure after runtime construction deliberately consumes the one-shot prepared handle")
	require.NoError(t, prepared.Close())

	err = WithExclusiveProductionAccountIndexStore(fixture.root, func() error { return nil })
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse, "the guard remains the sole lock owner")
	require.NoError(t, os.Remove(bankhashPath))

	// Retry uses the ordinary fully-verifying open because the prepared runtime
	// was safely torn down. The same guard is still live and transfers normally.
	db, err := OpenDbWithProductionAccountIndexConfigAndStoreGuard(
		fixture.root, fixture.config, fixture.guard,
	)
	require.NoError(t, err)
	require.NoError(t, db.CloseDb())
}

func TestPreparedSnapshotAccountIndexRuntimeConstructionFailureCleansGenerationAndRetainsGuard(t *testing.T) {
	fixture := oneAccountPreparedFixture(t)
	prepared := prepareSnapshotVerifyFixture(t, fixture)
	retained := prepared.immutable
	publishSnapshotVerifyFileHighWater(t, fixture)

	catalog, err := ReadRootIndexCatalog(fixture.root)
	require.NoError(t, err)
	indexPath, err := ResolveIndexCatalogArtifactPath(fixture.root, catalog.Shards[0].BaseIndex)
	require.NoError(t, err)
	orphan := filepath.Join(filepath.Dir(indexPath), "streamhash-parts-1")
	require.NoError(t, os.Symlink(t.TempDir(), orphan))

	_, err = OpenDbWithPreparedSnapshotAccountIndexAndStoreGuard(
		fixture.root, prepared, fixture.guard,
	)
	require.ErrorContains(t, err, "recognized private build path is not a real directory")
	_, configErr := prepared.productionConfig()
	require.Error(t, configErr, "runtime construction consumed the prepared generation")
	for shardID := range retained.shards {
		base := retained.shards[shardID].base
		require.NotNil(t, base)
		base.mu.RLock()
		closed := base.closed
		base.mu.RUnlock()
		assert.True(t, closed, "failed runtime construction must close every retained base")
	}
	err = WithExclusiveProductionAccountIndexStore(fixture.root, func() error { return nil })
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse)

	require.NoError(t, os.Remove(orphan))
	db, err := OpenDbWithProductionAccountIndexConfigAndStoreGuard(
		fixture.root, fixture.config, fixture.guard,
	)
	require.NoError(t, err)
	require.NoError(t, db.CloseDb())
}

func TestPreparedSnapshotAccountIndexRejectsHandoffMutation(t *testing.T) {
	t.Run("root selector", func(t *testing.T) {
		fixture := oneAccountPreparedFixture(t)
		prepared := prepareSnapshotVerifyFixture(t, fixture)
		defer func() { require.NoError(t, prepared.Close()) }()

		catalog, err := ReadRootIndexCatalog(fixture.root)
		require.NoError(t, err)
		catalog.Lineage[0] ^= 0xff
		_, err = writeRootIndexCatalogAtomic(fixture.root, catalog)
		require.NoError(t, err)
		_, err = OpenDbWithPreparedSnapshotAccountIndexAndStoreGuard(
			fixture.root, prepared, fixture.guard,
		)
		assert.ErrorContains(t, err, "root catalog changed")
	})

	t.Run("mutable journal", func(t *testing.T) {
		fixture := oneAccountPreparedFixture(t)
		prepared := prepareSnapshotVerifyFixture(t, fixture)
		defer func() { require.NoError(t, prepared.Close()) }()

		journal, err := os.OpenFile(
			filepath.Join(fixture.root, ShardedDeltaIndexJournalFileName),
			os.O_WRONLY|os.O_APPEND,
			0,
		)
		require.NoError(t, err)
		_, err = journal.Write([]byte{1})
		require.NoError(t, err)
		require.NoError(t, journal.Close())
		_, err = OpenDbWithPreparedSnapshotAccountIndexAndStoreGuard(
			fixture.root, prepared, fixture.guard,
		)
		assert.ErrorContains(t, err, "journal is not fresh")
	})

	for _, artifactName := range []string{"extent catalog", "StreamHash index", "exact scan"} {
		t.Run(artifactName, func(t *testing.T) {
			fixture := oneAccountPreparedFixture(t)
			prepared := prepareSnapshotVerifyFixture(t, fixture)
			defer func() { require.NoError(t, prepared.Close()) }()

			catalog, err := ReadRootIndexCatalog(fixture.root)
			require.NoError(t, err)
			artifact := catalog.SharedExtentCatalog
			switch artifactName {
			case "StreamHash index":
				artifact = catalog.Shards[0].BaseIndex
			case "exact scan":
				artifact = catalog.Shards[0].BaseRecords
			}
			path, err := ResolveIndexCatalogArtifactPath(fixture.root, artifact)
			require.NoError(t, err)
			info, err := os.Stat(path)
			require.NoError(t, err)
			changed := info.ModTime().Add(2 * time.Second)
			require.NoError(t, os.Chtimes(path, changed, changed))
			_, err = OpenDbWithPreparedSnapshotAccountIndexAndStoreGuard(
				fixture.root, prepared, fixture.guard,
			)
			assert.ErrorContains(t, err, "changed after verification")
		})
	}
}

func TestPreparedSnapshotAccountIndexRejectsConfigMismatch(t *testing.T) {
	fixture := oneAccountPreparedFixture(t)
	prepared := prepareSnapshotVerifyFixture(t, fixture)
	defer func() { require.NoError(t, prepared.Close()) }()

	changed := prepared.config
	changed.SealMaxAge++
	called := false
	err := prepared.adopt(
		fixture.root,
		changed,
		prepared.storeLock,
		func(*RootIndexCatalog, *ShardedImmutableIndex) (bool, error) {
			called = true
			return false, nil
		},
	)
	assert.ErrorContains(t, err, "configuration changed")
	assert.False(t, called)
}

func TestPreparedSnapshotAccountIndexRejectsReplacementStoreLock(t *testing.T) {
	fixture := oneAccountPreparedFixture(t)
	prepared := prepareSnapshotVerifyFixture(t, fixture)
	defer func() { require.NoError(t, prepared.Close()) }()
	require.NoError(t, fixture.guard.Close())

	replacement, err := AcquireExclusiveProductionAccountIndexStore(fixture.root)
	require.NoError(t, err)
	defer func() { require.NoError(t, replacement.Close()) }()
	_, err = OpenDbWithPreparedSnapshotAccountIndexAndStoreGuard(
		fixture.root, prepared, replacement,
	)
	assert.ErrorContains(t, err, "does not belong to the held store lock")
}

func TestPreparedSnapshotAccountIndexCloseSerializesWithAdoptionAndIsIdempotent(t *testing.T) {
	fixture := oneAccountPreparedFixture(t)
	prepared := prepareSnapshotVerifyFixture(t, fixture)
	injected := errors.New("injected pre-consumption adoption failure")
	entered := make(chan struct{})
	release := make(chan struct{})
	adoptDone := make(chan error, 1)
	go func() {
		adoptDone <- prepared.adopt(
			fixture.root,
			prepared.config,
			prepared.storeLock,
			func(*RootIndexCatalog, *ShardedImmutableIndex) (bool, error) {
				close(entered)
				<-release
				return false, injected
			},
		)
	}()
	<-entered

	closeDone := make(chan error, 1)
	go func() { closeDone <- prepared.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close raced through active adoption: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	require.ErrorIs(t, <-adoptDone, injected)
	require.NoError(t, <-closeDone)
	require.NoError(t, prepared.Close())

	err := WithExclusiveProductionAccountIndexStore(fixture.root, func() error { return nil })
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse, "prepared Close must not release the guard-owned lock")
}
