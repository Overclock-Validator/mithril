package accountsdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openProcessFDTargets(t *testing.T) map[string]struct{} {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if errors.Is(err, os.ErrNotExist) {
		t.Skip("/proc/self/fd is unavailable on this platform")
	}
	require.NoError(t, err)
	targets := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue // The process may close an unrelated descriptor mid-scan.
		}
		require.NoError(t, err)
		target = strings.TrimSuffix(target, " (deleted)")
		if filepath.IsAbs(target) {
			targets[filepath.Clean(target)] = struct{}{}
		}
	}
	return targets
}

func requirePathsHaveNoOpenFD(t *testing.T, paths []string) {
	t.Helper()
	targets := openProcessFDTargets(t)
	for _, path := range paths {
		absolute, err := filepath.Abs(path)
		require.NoError(t, err)
		_, open := targets[filepath.Clean(absolute)]
		assert.False(t, open, "immutable artifact retained an FD: %s", absolute)
	}
}

func TestImmutableAccountIndexMappingsDoNotRetainRecordOrScanFDs(t *testing.T) {
	const shards = 16
	root := t.TempDir()
	records := make([]streamIndexTestRecord, 0, shards)
	for shardID := 0; shardID < shards; shardID++ {
		var key solana.PublicKey
		key[0] = byte(shardID << 4)
		key[31] = byte(shardID)
		records = append(records, streamIndexTestRecord{
			key: key,
			entry: AccountIndexEntry{
				Slot: uint64(100 + shardID), FileId: uint64(200 + shardID), Offset: 8,
			},
		})
	}
	build, err := BuildShardedStreamBaseWithShardCount(
		t.Context(), &streamIndexTestSource{records: records}, root, 1, shards,
		testPersistentIndexRoutingKey(), nil, 2,
	)
	require.NoError(t, err)
	router, err := NewPersistentIndexShardRouter(shards, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	openedBases := make([]*ShardedStreamBaseShard, 0, shards)
	baseScanPaths := make([]string, 0, shards)
	for _, artifacts := range build.Shards {
		shard, err := OpenShardedStreamBaseShardArtifacts(
			root, artifacts, build.ExtentCatalog, router,
		)
		require.NoError(t, err)
		openedBases = append(openedBases, shard)
		path, err := ResolveIndexCatalogArtifactPath(root, artifacts.Scan)
		require.NoError(t, err)
		baseScanPaths = append(baseScanPaths, path)
	}
	t.Cleanup(func() {
		for _, shard := range openedBases {
			_ = shard.Close()
		}
	})
	requirePathsHaveNoOpenFD(t, baseScanPaths)

	openedDeltas := make([]*DeltaCheckpoint, 0, shards)
	deltaRecordPaths := make([]string, 0, shards)
	for shardID := 0; shardID < shards; shardID++ {
		directory := filepath.Join(root, "fd-delta", string(rune('a'+shardID)))
		require.NoError(t, os.MkdirAll(directory, 0o755))
		key := deltaCheckpointTestKey(byte(shardID + 1))
		checkpoint, err := BuildDeltaCheckpoint(
			t.Context(), directory, nil,
			map[solana.PublicKey]deltaIndexValue{
				key: {Entry: AccountIndexEntry{Slot: uint64(shardID + 1), FileId: 1, Offset: 8}},
			},
			uint64(shardID+1), 1,
		)
		require.NoError(t, err)
		openedDeltas = append(openedDeltas, checkpoint)
		deltaRecordPaths = append(deltaRecordPaths, makeDeltaCheckpointPaths(directory, checkpoint.Generation()).records)
		value, found, err := checkpoint.Lookup(key)
		require.NoError(t, err)
		require.True(t, found)
		assert.False(t, value.Tombstone)
	}
	t.Cleanup(func() {
		for _, checkpoint := range openedDeltas {
			_ = checkpoint.Close()
		}
	})
	requirePathsHaveNoOpenFD(t, deltaRecordPaths)
}

func TestShardedStreamBaseScanRejectsPathReplacement(t *testing.T) {
	root := t.TempDir()
	key := streamIndexTestKey(0, 1)
	build, err := BuildShardedStreamBaseWithShardCount(
		t.Context(),
		&streamIndexTestSource{records: []streamIndexTestRecord{{
			key: key, entry: AccountIndexEntry{Slot: 1, FileId: 2, Offset: 8},
		}}},
		root, 1, 1, testPersistentIndexRoutingKey(), nil, 1,
	)
	require.NoError(t, err)
	router, err := NewPersistentIndexShardRouter(1, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	shard, err := OpenShardedStreamBaseShardArtifacts(
		root, build.Shards[0], build.ExtentCatalog, router,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = shard.Close() })

	scanPath, err := ResolveIndexCatalogArtifactPath(root, build.Shards[0].Scan)
	require.NoError(t, err)
	originalPath := scanPath + ".original"
	require.NoError(t, os.Rename(scanPath, originalPath))
	contents, err := os.ReadFile(originalPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(scanPath, contents, 0o644))

	err = shard.Scan(t.Context(), func(solana.PublicKey, AccountIndexEntry) error { return nil })
	require.ErrorContains(t, err, "path identity changed")
	// The probabilistic lookup mapping remains valid; only exact enumeration
	// depends on the replaced sidecar path.
	entry, found, err := shard.LookupCandidate(key)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, AccountIndexEntry{Slot: 1, FileId: 2, Offset: 8}, entry)
}

func TestShardedStreamBaseCloseWaitsForDirectScan(t *testing.T) {
	root := t.TempDir()
	key := streamIndexTestKey(0, 1)
	build, err := BuildShardedStreamBaseWithShardCount(
		t.Context(),
		&streamIndexTestSource{records: []streamIndexTestRecord{{
			key: key, entry: AccountIndexEntry{Slot: 1, FileId: 2, Offset: 8},
		}}},
		root, 1, 1, testPersistentIndexRoutingKey(), nil, 1,
	)
	require.NoError(t, err)
	router, err := NewPersistentIndexShardRouter(1, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	shard, err := OpenShardedStreamBaseShardArtifacts(
		root, build.Shards[0], build.ExtentCatalog, router,
	)
	require.NoError(t, err)

	visitorEntered := make(chan struct{})
	releaseVisitor := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseVisitor) }) })
	scanDone := make(chan error, 1)
	go func() {
		scanDone <- shard.Scan(context.Background(), func(solana.PublicKey, AccountIndexEntry) error {
			close(visitorEntered)
			<-releaseVisitor
			return nil
		})
	}()
	select {
	case <-visitorEntered:
	case <-time.After(5 * time.Second):
		releaseOnce.Do(func() { close(releaseVisitor) })
		t.Fatal("scan did not reach its visitor")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- shard.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close raced past an active direct Scan: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(releaseVisitor) })
	select {
	case err := <-scanDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("scan did not finish")
	}
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after Scan released its read lock")
	}
	err = shard.Scan(t.Context(), func(solana.PublicKey, AccountIndexEntry) error { return nil })
	require.ErrorContains(t, err, "closed shard")
	assert.Nil(t, shard.catalog.Load())
	err = shard.rebindExtentCatalog(build.ExtentCatalog)
	require.ErrorContains(t, err, "rebind closed shard")
	assert.Nil(t, shard.catalog.Load(), "catalog rebind must not resurrect a closed shard")
}
