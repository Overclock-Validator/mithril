package accountsdb

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardedStreamBaseRoundTripIncludingEmptyShard(t *testing.T) {
	root := t.TempDir()
	records := shardedBaseTestRecords()
	source := &streamIndexTestSource{records: records}
	result, err := BuildShardedStreamBaseWithShardCount(
		t.Context(), source, root, 1, 8, testPersistentIndexRoutingKey(), nil, 2,
	)
	require.NoError(t, err)
	assert.Equal(t, 2, source.scans, "source must be inventoried and spooled exactly once")
	assert.Equal(t, uint32(8), result.ShardCount)
	assert.Equal(t, testPersistentIndexRoutingKey(), result.RoutingKey)
	assert.Len(t, result.Shards, 8)
	assert.Equal(t, result.ExtentCatalogArtifact, result.ExtentCatalog.Artifact())

	router, err := NewPersistentIndexShardRouter(8, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	wantByShard := make(map[uint32][]streamIndexTestRecord)
	for _, record := range records {
		shardID := router.Shard(record.key)
		wantByShard[shardID] = append(wantByShard[shardID], record)
	}
	for _, artifacts := range result.Shards {
		shard, err := OpenShardedStreamBaseShardArtifacts(root, artifacts, result.ExtentCatalog, router)
		require.NoError(t, err)
		assert.Equal(t, shardedBaseFingerprintBytes, shard.idx.Stats().FingerprintSize)
		assert.Equal(t, shardedBasePayloadBytes, shard.idx.Stats().PayloadSize)
		var scanned []streamIndexTestRecord
		require.NoError(t, shard.Scan(t.Context(), func(key solana.PublicKey, entry AccountIndexEntry) error {
			scanned = append(scanned, streamIndexTestRecord{key: key, entry: entry})
			return nil
		}))
		assert.Equal(t, wantByShard[artifacts.ShardID], scanned)
		assert.Equal(t, uint64(len(scanned)), shard.NumKeys())
		for _, record := range scanned {
			entry, found, err := shard.LookupCandidate(record.key)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, record.entry, entry)
			pinnedEntry, pinnedFound, pinnedErr := shard.lookupCandidatePinned(record.key)
			require.NoError(t, pinnedErr)
			assert.Equal(t, found, pinnedFound)
			assert.Equal(t, entry, pinnedEntry)
		}
		require.NoError(t, shard.Close())
	}
	assert.Less(t, len(wantByShard), router.Count(), "fixture must exercise an empty persistent shard")

	shardZero, err := OpenShardedStreamBaseShardArtifacts(root, result.Shards[0], result.ExtentCatalog, router)
	require.NoError(t, err)
	wrongKey := shardedBaseTestKeysForShard(router, 1, 1)[0]
	if router.Shard(wrongKey) == 0 {
		wrongKey = shardedBaseTestKeysForShard(router, 2, 1)[0]
	}
	_, _, err = shardZero.LookupCandidate(wrongKey)
	require.ErrorContains(t, err, "routes to shard")
	require.NoError(t, shardZero.Close())
}

func TestShardedStreamBaseKeyedRoutingHandlesInterleavedSamePrefix(t *testing.T) {
	const (
		shardCount  = 16
		recordCount = 4096
	)
	root := t.TempDir()
	records := make([]streamIndexTestRecord, recordCount)
	for ordinal := range records {
		var key solana.PublicKey
		key[0], key[1] = 0xab, 0xcd
		binary.BigEndian.PutUint64(key[24:], uint64(ordinal))
		records[ordinal] = streamIndexTestRecord{
			key: key,
			entry: AccountIndexEntry{
				Slot: 1, FileId: 2, Offset: uint64(ordinal+1) * 8,
			},
		}
	}
	result, err := BuildShardedStreamBaseWithShardCount(
		t.Context(), &streamIndexTestSource{records: records}, root, 1,
		shardCount, testPersistentIndexRoutingKey(), nil, 2,
	)
	require.NoError(t, err)
	router, err := NewPersistentIndexShardRouter(shardCount, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	var total uint64
	for _, artifacts := range result.Shards {
		shard, err := OpenShardedStreamBaseShardArtifacts(root, artifacts, result.ExtentCatalog, router)
		require.NoError(t, err)
		assert.Greater(t, shard.NumKeys(), uint64(180), "shard %d", artifacts.ShardID)
		assert.Less(t, shard.NumKeys(), uint64(340), "shard %d", artifacts.ShardID)
		total += shard.NumKeys()
		require.NoError(t, shard.Close())
	}
	assert.Equal(t, uint64(recordCount), total)
}

func BenchmarkShardedStreamBaseLookupCandidatePin(b *testing.B) {
	root := b.TempDir()
	records := shardedBaseTestRecords()
	result, err := BuildShardedStreamBaseWithShardCount(
		context.Background(), &streamIndexTestSource{records: records}, root, 1, 1,
		testPersistentIndexRoutingKey(), nil, 1,
	)
	if err != nil {
		b.Fatal(err)
	}
	router, err := NewPersistentIndexShardRouter(1, testPersistentIndexRoutingKey())
	if err != nil {
		b.Fatal(err)
	}
	shard, err := OpenShardedStreamBaseShardArtifacts(
		root, result.Shards[0], result.ExtentCatalog, router,
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := shard.Close(); err != nil {
			b.Error(err)
		}
	})
	key := records[len(records)/2].key

	b.Run("lifecycle-guarded", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, found, err := shard.LookupCandidate(key); err != nil || !found {
				b.Fatalf("lookup: found=%t err=%v", found, err)
			}
		}
	})
	b.Run("generation-pinned", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, found, err := shard.lookupCandidatePinned(key); err != nil || !found {
				b.Fatalf("lookup: found=%t err=%v", found, err)
			}
		}
	})
}

func TestShardedStreamBaseConsumesExistingRunSource(t *testing.T) {
	root := t.TempDir()
	runDir := t.TempDir()
	records := shardedBaseTestRecords()
	writeStreamIndexTestRun(t, filepath.Join(runDir, "0000.run"), records[:2])
	writeStreamIndexTestRun(t, filepath.Join(runDir, "0001.run"), records[2:])
	source, err := OpenStreamIndexRunSource(runDir)
	require.NoError(t, err)

	result, err := BuildShardedStreamBaseWithShardCount(
		t.Context(), source, root, 1, 4, testPersistentIndexRoutingKey(), nil, 1,
	)
	require.NoError(t, err)
	assert.Len(t, result.Shards, 4)
}

func TestShardedStreamBaseSingleShardRebuildExtendsCatalogCompatibly(t *testing.T) {
	root := t.TempDir()
	initialSource := &streamIndexTestSource{records: shardedBaseTestRecords()}
	initial, err := BuildShardedStreamBaseWithShardCount(
		t.Context(), initialSource, root, 1, 4, testPersistentIndexRoutingKey(), nil, 2,
	)
	require.NoError(t, err)
	oldCatalogLen := initial.ExtentCatalog.Len()

	router, err := NewPersistentIndexShardRouter(4, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	keys := shardedBaseTestKeysForShard(router, 1, 2)
	keyA, keyB := keys[0], keys[1]
	newRecords := []streamIndexTestRecord{
		{key: keyA, entry: AccountIndexEntry{Slot: 200, FileId: 900, Offset: 16}},
		{key: keyB, entry: AccountIndexEntry{Slot: 201, FileId: 901, Offset: 5*streamIndexExtentSize + 24}},
	}
	rebuilt, err := BuildShardedStreamBaseShardWithShardCount(
		t.Context(), &streamIndexTestSource{records: newRecords}, root,
		2, 4, testPersistentIndexRoutingKey(), 1, initial.ExtentCatalog, 2,
	)
	require.NoError(t, err)
	require.True(t, rebuilt.OwnsExtentCatalogArtifact)
	assert.Equal(t, uint32(1), rebuilt.Shard.ShardID)
	assert.Greater(t, rebuilt.ExtentCatalog.Len(), oldCatalogLen)
	assert.NotEqual(t, initial.ExtentCatalogArtifact.RelativePath, rebuilt.ExtentCatalogArtifact.RelativePath)

	// An untouched generation-one shard remains valid against the append-only
	// generation-two catalog.
	oldShard, err := OpenShardedStreamBaseShardArtifacts(root, initial.Shards[0], rebuilt.ExtentCatalog, router)
	require.NoError(t, err)
	require.NoError(t, oldShard.Close())

	newShard, err := OpenShardedStreamBaseShardArtifacts(root, rebuilt.Shard, rebuilt.ExtentCatalog, router)
	require.NoError(t, err)
	for _, record := range newRecords {
		entry, found, err := newShard.LookupCandidate(record.key)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, record.entry, entry)
	}
	require.NoError(t, newShard.Close())

	_, err = OpenShardedStreamBaseShardArtifacts(root, rebuilt.Shard, initial.ExtentCatalog, router)
	require.ErrorContains(t, err, "newer than catalog")
}

func TestShardedStreamBaseSingleShardRebuildReusesCompleteCatalog(t *testing.T) {
	root := t.TempDir()
	initial, err := BuildShardedStreamBaseWithShardCount(
		t.Context(), &streamIndexTestSource{records: shardedBaseTestRecords()},
		root, 1, 4, testPersistentIndexRoutingKey(), nil, 1,
	)
	require.NoError(t, err)
	require.True(t, initial.OwnsExtentCatalogArtifact)

	router, err := NewPersistentIndexShardRouter(4, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	var shardRecords []streamIndexTestRecord
	for _, record := range shardedBaseTestRecords() {
		if router.Shard(record.key) == 1 {
			shardRecords = append(shardRecords, record)
		}
	}
	rebuilt, err := BuildShardedStreamBaseShardWithShardCount(
		t.Context(), &streamIndexTestSource{records: shardRecords}, root,
		2, 4, testPersistentIndexRoutingKey(), 1, initial.ExtentCatalog, 1,
	)
	require.NoError(t, err)
	require.False(t, rebuilt.OwnsExtentCatalogArtifact)
	require.Same(t, initial.ExtentCatalog, rebuilt.ExtentCatalog)
	require.Equal(t, initial.ExtentCatalogArtifact, rebuilt.ExtentCatalogArtifact)
	require.Equal(t, uint64(1), rebuilt.ExtentCatalog.Generation())
	require.Equal(t, uint64(2), rebuilt.Shard.Generation)

	buildDirectory, err := ResolveIndexCatalogArtifactPath(root, rebuilt.Shard.Index)
	require.NoError(t, err)
	_, err = os.Lstat(filepath.Join(filepath.Dir(buildDirectory), "extents.cat"))
	require.ErrorIs(t, err, os.ErrNotExist)

	shard, err := OpenShardedStreamBaseShardArtifacts(root, rebuilt.Shard, rebuilt.ExtentCatalog, router)
	require.NoError(t, err)
	for _, record := range shardRecords {
		entry, found, err := shard.LookupCandidate(record.key)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, record.entry, entry)
	}
	require.NoError(t, shard.Close())
	require.NoError(t, removeRebaseBuild(root, rebuilt))
	assertImmutableArtifactsExist(t, root, initial.ExtentCatalogArtifact)
	assertImmutableArtifactsMissing(t, root, rebuilt.Shard.Index, rebuilt.Shard.Scan)
}

func TestShardedStreamBaseRejectsSemanticScanCorruption(t *testing.T) {
	root := t.TempDir()
	router, err := NewPersistentIndexShardRouter(4, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	keys := shardedBaseTestKeysForShard(router, 0, 2)
	records := []streamIndexTestRecord{
		{key: keys[0], entry: AccountIndexEntry{Slot: 10, FileId: 100, Offset: 8}},
		{key: keys[1], entry: AccountIndexEntry{Slot: 10, FileId: 100, Offset: 16}},
	}
	result, err := BuildShardedStreamBaseWithShardCount(
		t.Context(), &streamIndexTestSource{records: records},
		root, 1, 4, testPersistentIndexRoutingKey(), nil, 2,
	)
	require.NoError(t, err)
	indexPath, err := ResolveIndexCatalogArtifactPath(root, result.Shards[0].Index)
	require.NoError(t, err)
	scanPath, err := ResolveIndexCatalogArtifactPath(root, result.Shards[0].Scan)
	require.NoError(t, err)
	valid, err := os.ReadFile(scanPath)
	require.NoError(t, err)
	require.Equal(t, shardedBaseMetadataSize+2*shardedBaseScanRecordSize, len(valid))

	tests := []struct {
		name   string
		mutate func([]byte) []byte
		want   string
	}{
		{
			name: "out of order",
			mutate: func(data []byte) []byte {
				first := shardedBaseMetadataSize
				second := first + shardedBaseScanRecordSize
				tmp := append([]byte(nil), data[first:second]...)
				copy(data[first:second], data[second:second+shardedBaseScanRecordSize])
				copy(data[second:second+shardedBaseScanRecordSize], tmp)
				resealShardedBaseScanForTest(data)
				return data
			},
			want: "out-of-order",
		},
		{
			name: "wrong route",
			mutate: func(data []byte) []byte {
				wrong := shardedBaseTestKeysForShard(router, 1, 1)[0]
				copy(data[shardedBaseMetadataSize:shardedBaseMetadataSize+32], wrong[:])
				resealShardedBaseScanForTest(data)
				return data
			},
			want: "routes to",
		},
		{
			name: "bad locator",
			mutate: func(data []byte) []byte {
				locator := shardedBaseMetadataSize + 32
				for i := 0; i < shardedBasePayloadBytes; i++ {
					data[locator+i] = 0xff
				}
				resealShardedBaseScanForTest(data)
				return data
			},
			want: "extent ordinal",
		},
		{
			name: "body crc",
			mutate: func(data []byte) []byte {
				data[shardedBaseMetadataSize+32] ^= 1
				return data
			},
			want: "body CRC",
		},
		{
			name:   "truncated",
			mutate: func(data []byte) []byte { return data[:len(data)-1] },
			want:   "scan size",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			corrupt := test.mutate(append([]byte(nil), valid...))
			path := filepath.Join(t.TempDir(), "corrupt.scan")
			require.NoError(t, os.WriteFile(path, corrupt, 0o600))
			shard, err := openShardedStreamBaseShardFiles(indexPath, path, result.ExtentCatalog, router)
			require.ErrorContains(t, err, test.want)
			assert.Nil(t, shard)
		})
	}

	badIdentity := result.Shards[0]
	badIdentity.Scan.SHA256[0] ^= 1
	_, err = OpenShardedStreamBaseShardArtifacts(root, badIdentity, result.ExtentCatalog, router)
	require.ErrorContains(t, err, "identity mismatch")

	wrongKey := testPersistentIndexRoutingKey()
	wrongKey[0] ^= 0xff
	wrongRouter, err := NewPersistentIndexShardRouter(4, wrongKey)
	require.NoError(t, err)
	_, err = OpenShardedStreamBaseShardArtifacts(
		root, result.Shards[0], result.ExtentCatalog, wrongRouter,
	)
	require.ErrorContains(t, err, "routing-key mismatch")
}

func TestShardedStreamBaseScanRejectsSymlinkReplacement(t *testing.T) {
	root := t.TempDir()
	result, err := BuildShardedStreamBaseWithShardCount(
		t.Context(), &streamIndexTestSource{records: shardedBaseTestRecords()},
		root, 1, 1, testPersistentIndexRoutingKey(), nil, 1,
	)
	require.NoError(t, err)
	router, err := NewPersistentIndexShardRouter(1, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	shard, err := OpenShardedStreamBaseShardArtifacts(
		root, result.Shards[0], result.ExtentCatalog, router,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, shard.Close()) }()

	scanPath, err := ResolveIndexCatalogArtifactPath(root, result.Shards[0].Scan)
	require.NoError(t, err)
	want, err := os.ReadFile(scanPath)
	require.NoError(t, err)
	movedPath := scanPath + ".external-target"
	replaced := false
	err = shard.Scan(t.Context(), func(solana.PublicKey, AccountIndexEntry) error {
		if replaced {
			return nil
		}
		replaced = true
		require.NoError(t, os.Rename(scanPath, movedPath))
		require.NoError(t, os.Symlink(movedPath, scanPath))
		return nil
	})
	require.True(t, replaced)
	require.ErrorContains(t, err, "changed while reading")
	got, err := os.ReadFile(movedPath)
	require.NoError(t, err)
	assert.Equal(t, want, got, "rejected symlink replacement must not mutate its target")
	info, err := os.Lstat(scanPath)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink)
}

func TestShardedStreamBaseRejectsInvalidAndChangingSources(t *testing.T) {
	validEntry := AccountIndexEntry{Slot: 1, FileId: 1, Offset: 8}
	first := streamIndexTestKey(0x00, 1)
	second := streamIndexTestKey(0x00, 2)
	tests := []struct {
		name    string
		records []streamIndexTestRecord
		want    string
	}{
		{name: "duplicate", records: []streamIndexTestRecord{{first, validEntry}, {first, validEntry}}, want: "duplicate"},
		{name: "out of order", records: []streamIndexTestRecord{{second, validEntry}, {first, validEntry}}, want: "out-of-order"},
		{name: "unaligned", records: []streamIndexTestRecord{{first, AccountIndexEntry{Slot: 1, FileId: 1, Offset: 9}}}, want: "aligned"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := BuildShardedStreamBaseWithShardCount(
				t.Context(), &streamIndexTestSource{records: test.records},
				t.TempDir(), 1, 4, testPersistentIndexRoutingKey(), nil, 1,
			)
			require.ErrorContains(t, err, test.want)
		})
	}

	router, err := NewPersistentIndexShardRouter(4, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	wrongShardKey := shardedBaseTestKeysForShard(router, 1, 1)[0]
	_, err = BuildShardedStreamBaseShardWithShardCount(
		t.Context(), &streamIndexTestSource{records: []streamIndexTestRecord{{wrongShardKey, validEntry}}},
		t.TempDir(), 1, 4, testPersistentIndexRoutingKey(), 0, nil, 1,
	)
	require.ErrorContains(t, err, "single-shard source")

	changing := &changingShardedBaseSource{
		first:  []streamIndexTestRecord{{key: first, entry: validEntry}},
		second: []streamIndexTestRecord{{key: first, entry: AccountIndexEntry{Slot: 1, FileId: 1, Offset: 16}}},
	}
	_, err = BuildShardedStreamBaseWithShardCount(
		t.Context(), changing, t.TempDir(), 1, 2, testPersistentIndexRoutingKey(), nil, 1,
	)
	require.ErrorContains(t, err, "source changed between scans")
}

func TestPublishUniqueShardedBaseFileNeverClobbers(t *testing.T) {
	dir := t.TempDir()
	partial := filepath.Join(dir, "artifact.partial")
	final := filepath.Join(dir, "artifact")
	require.NoError(t, os.WriteFile(partial, []byte("new"), 0o600))
	require.NoError(t, os.WriteFile(final, []byte("old"), 0o600))
	require.Error(t, publishUniqueShardedBaseFile(partial, final))
	data, err := os.ReadFile(final)
	require.NoError(t, err)
	assert.Equal(t, []byte("old"), data)
}

type changingShardedBaseSource struct {
	mu     sync.Mutex
	scans  int
	first  []streamIndexTestRecord
	second []streamIndexTestRecord
}

func (source *changingShardedBaseSource) Scan(
	ctx context.Context,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) error {
	source.mu.Lock()
	source.scans++
	scan := source.scans
	records := source.first
	if scan > 1 {
		records = source.second
	}
	source.mu.Unlock()
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(record.key, record.entry); err != nil {
			return err
		}
	}
	return nil
}

func shardedBaseTestRecords() []streamIndexTestRecord {
	return []streamIndexTestRecord{
		{key: streamIndexTestKey(0x00, 1), entry: AccountIndexEntry{Slot: 10, FileId: 100, Offset: 8}},
		{key: streamIndexTestKey(0x01, 2), entry: AccountIndexEntry{Slot: 10, FileId: 100, Offset: streamIndexExtentSize + 16}},
		{key: streamIndexTestKey(0x40, 3), entry: AccountIndexEntry{Slot: 20, FileId: 200, Offset: 24}},
		{key: streamIndexTestKey(0xc0, 4), entry: AccountIndexEntry{Slot: 30, FileId: 300, Offset: 2*streamIndexExtentSize + 32}},
	}
}

func shardedBaseTestKeysForShard(
	router PersistentIndexShardRouter,
	shardID uint32,
	count int,
) []solana.PublicKey {
	keys := make([]solana.PublicKey, 0, count)
	for ordinal := uint64(0); len(keys) < count; ordinal++ {
		var key solana.PublicKey
		binary.BigEndian.PutUint64(key[24:], ordinal)
		if router.Shard(key) == shardID {
			keys = append(keys, key)
		}
	}
	return keys
}

func resealShardedBaseScanForTest(data []byte) {
	body := data[shardedBaseMetadataSize:]
	binary.LittleEndian.PutUint32(data[248:252], crc32.Checksum(body, shardedBaseCRC))
	binary.LittleEndian.PutUint32(data[252:256], crc32.Checksum(data[:252], shardedBaseCRC))
}
