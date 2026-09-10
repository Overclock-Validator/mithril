//go:build linux

package accountsdb

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildShardedBaseIdentityTestFixture(t *testing.T) (
	string,
	*ShardedStreamBaseBuildResult,
	PersistentIndexShardRouter,
) {
	t.Helper()
	root := t.TempDir()
	router, err := NewPersistentIndexShardRouter(1, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	records := []streamIndexTestRecord{{
		key:   shardedBaseTestKeysForShard(router, 0, 1)[0],
		entry: AccountIndexEntry{Slot: 42, FileId: 7, Offset: 8},
	}}
	result, err := BuildShardedStreamBaseWithShardCount(
		t.Context(), &streamIndexTestSource{records: records},
		root, 1, 1, testPersistentIndexRoutingKey(), nil, 1,
	)
	require.NoError(t, err)
	return root, result, router
}

func TestShardedBaseOrdinaryReopenUsesAuthenticatedScanIdentity(t *testing.T) {
	root, result, router := buildShardedBaseIdentityTestFixture(t)
	shard, err := OpenShardedStreamBaseShardArtifacts(
		root, result.Shards[0], result.ExtentCatalog, router,
	)
	require.NoError(t, err)
	require.NotEqual(t, shardedBaseScanSeal{}, shard.metadata.ScanSeal)
	assert.Equal(t, shard.metadata.ScanSeal, shard.scanSeal)
	assert.Equal(t, result.Shards[0].Scan.SHA256, shard.scanSeal.SHA256)
	assert.Zero(t, shard.scanFullValidations.Load(), "ordinary reopen must not read the sidecar body")
	visited := 0
	require.NoError(t, shard.Scan(t.Context(), func(_ solana.PublicKey, _ AccountIndexEntry) error {
		visited++
		return nil
	}))
	assert.Equal(t, 1, visited)
	assert.Zero(t, shard.scanFullValidations.Load(), "exact scan must consume the authenticated inode directly")
	require.NoError(t, shard.Scan(t.Context(), func(solana.PublicKey, AccountIndexEntry) error { return nil }))
	assert.Zero(t, shard.scanFullValidations.Load(), "stable immutable generation must not add a validation pre-pass")
	require.NoError(t, shard.Close())
}

func TestShardedBaseRebaseSourceConsumesAuthenticatedScanWithoutPrepass(t *testing.T) {
	root, result, router := buildShardedBaseIdentityTestFixture(t)
	shard, err := OpenShardedStreamBaseShardArtifacts(
		root, result.Shards[0], result.ExtentCatalog, router,
	)
	require.NoError(t, err)
	assert.Zero(t, shard.scanFullValidations.Load())
	source, err := newMergedShardIndexSource(shard, nil)
	require.NoError(t, err)
	require.NoError(t, source.Scan(t.Context(), func(solana.PublicKey, AccountIndexEntry) error { return nil }))
	assert.Zero(t, shard.scanFullValidations.Load(), "rebase must not add a validation pre-pass")
	require.NoError(t, shard.Close())
}

func TestShardedBaseRangeEnumerationConsumesAuthenticatedScanWithoutPrepass(t *testing.T) {
	root, result, router := buildShardedBaseIdentityTestFixture(t)
	shard, err := OpenShardedStreamBaseShardArtifacts(
		root, result.Shards[0], result.ExtentCatalog, router,
	)
	require.NoError(t, err)
	assert.Zero(t, shard.scanFullValidations.Load())
	var lower, upper solana.PublicKey
	for i := range upper {
		upper[i] = 0xff
	}
	cursor, err := newProductionBaseEnumerationCursor(t.Context(), shard, lower, upper)
	require.NoError(t, err)
	require.NoError(t, cursor.Close())
	assert.Zero(t, shard.scanFullValidations.Load(), "rent enumeration must not add a validation pre-pass")
	require.NoError(t, shard.Close())
}

func TestShardedBaseStaleIdentityFallsBackToCompleteVerification(t *testing.T) {
	root, result, router := buildShardedBaseIdentityTestFixture(t)
	first, err := OpenShardedStreamBaseShardArtifacts(
		root, result.Shards[0], result.ExtentCatalog, router,
	)
	require.NoError(t, err)
	originalSeal := first.scanSeal
	require.NoError(t, first.Close())

	scanPath := filepath.Join(root, filepath.FromSlash(result.Shards[0].Scan.RelativePath))
	info, err := os.Stat(scanPath)
	require.NoError(t, err)
	// Changing and then restoring mtime leaves file bytes and the root SHA
	// untouched, but Linux ctime necessarily changes. The authenticated O(1)
	// receipt is therefore stale and open must take the full verification path.
	touched := info.ModTime().Add(-time.Second)
	require.NoError(t, os.Chtimes(scanPath, touched, touched))
	require.NoError(t, os.Chtimes(scanPath, info.ModTime(), info.ModTime()))

	second, err := OpenShardedStreamBaseShardArtifacts(
		root, result.Shards[0], result.ExtentCatalog, router,
	)
	require.NoError(t, err)
	assert.NotEqual(t, originalSeal, second.scanSeal)
	assert.Equal(t, originalSeal.SHA256, second.scanSeal.SHA256)
	assert.Equal(t, second.metadata.ScanSeal, originalSeal)
	assert.Equal(t, uint64(1), second.scanFullValidations.Load(), "stale receipt must force complete open-time validation")
	require.NoError(t, second.Scan(t.Context(), func(solana.PublicKey, AccountIndexEntry) error { return nil }))
	assert.Equal(t, uint64(1), second.scanFullValidations.Load(), "fresh post-validation seal must avoid another pre-pass")
	require.NoError(t, second.Close())
}

func TestShardedBaseCorruptionWithRestoredMtimeFailsClosed(t *testing.T) {
	root, result, router := buildShardedBaseIdentityTestFixture(t)
	scanPath := filepath.Join(root, filepath.FromSlash(result.Shards[0].Scan.RelativePath))
	info, err := os.Stat(scanPath)
	require.NoError(t, err)
	file, err := os.OpenFile(scanPath, os.O_RDWR, 0)
	require.NoError(t, err)
	data, err := os.ReadFile(scanPath)
	require.NoError(t, err)
	data[shardedBaseMetadataSize+1] ^= 0x80
	// Repair both public CRC fields. The record remains structurally valid in
	// the one-shard fixture, so only the authenticated SHA-256 can reject it.
	resealShardedBaseScanForTest(data)
	n, err := file.WriteAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
	require.NoError(t, os.Chtimes(scanPath, info.ModTime(), info.ModTime()))
	after, err := os.Stat(scanPath)
	require.NoError(t, err)
	assert.True(t, after.ModTime().Equal(info.ModTime()), "test must restore mtime")

	shard, err := OpenShardedStreamBaseShardArtifacts(
		root, result.Shards[0], result.ExtentCatalog, router,
	)
	assert.Nil(t, shard)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidCatalogArtifact)
}
