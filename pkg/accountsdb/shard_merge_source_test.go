package accountsdb

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestMergedShardIndexSourceNewestWinsAndIsRepeatable(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	keyA := streamIndexTestKey(0x00, 1)
	keyB := streamIndexTestKey(0x00, 2)
	keyC := streamIndexTestKey(0x00, 3)
	oldA := AccountIndexEntry{Slot: 1, FileId: 10, Offset: 8}
	oldC := AccountIndexEntry{Slot: 1, FileId: 10, Offset: 16}
	baseResult, err := BuildShardedStreamBaseWithShardCount(
		t.Context(),
		&streamIndexTestSource{records: []streamIndexTestRecord{
			{key: keyA, entry: oldA},
			{key: keyC, entry: oldC},
		}},
		root,
		1,
		1,
		testPersistentIndexRoutingKey(),
		nil,
		1,
	)
	require.NoError(t, err)
	router, err := NewPersistentIndexShardRouter(1, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	base, err := OpenShardedStreamBaseShardArtifacts(
		root, baseResult.Shards[0], baseResult.ExtentCatalog, router,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })

	newA := AccountIndexEntry{Slot: 2, FileId: 20, Offset: 24}
	newB := AccountIndexEntry{Slot: 2, FileId: 20, Offset: 32}
	delta, err := BuildDeltaCheckpoint(
		t.Context(),
		t.TempDir(),
		nil,
		map[solana.PublicKey]deltaIndexValue{
			keyA: {Entry: newA},
			keyB: {Entry: newB},
			keyC: {Tombstone: true},
		},
		7,
		1,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, delta.Close()) })

	source, err := newMergedShardIndexSource(base, delta)
	require.NoError(t, err)
	want := []streamIndexTestRecord{{key: keyA, entry: newA}, {key: keyB, entry: newB}}
	for range 2 {
		var got []streamIndexTestRecord
		err := source.Scan(t.Context(), func(key solana.PublicKey, entry AccountIndexEntry) error {
			got = append(got, streamIndexTestRecord{key: key, entry: entry})
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestMergedShardIndexSourceHonorsCancellation(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	result, err := BuildShardedStreamBaseWithShardCount(
		t.Context(),
		&streamIndexTestSource{records: shardedBaseTestRecords()[:2]},
		root,
		1,
		1,
		testPersistentIndexRoutingKey(),
		nil,
		1,
	)
	require.NoError(t, err)
	router, err := NewPersistentIndexShardRouter(1, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	base, err := OpenShardedStreamBaseShardArtifacts(root, result.Shards[0], result.ExtentCatalog, router)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	source, err := newMergedShardIndexSource(base, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, source.Scan(ctx, func(solana.PublicKey, AccountIndexEntry) error { return nil }), context.Canceled)
}

func buildMergedShardScanFixture(t testing.TB, count int) (*ShardedStreamBaseShard, []productionIndexBenchmarkRecord) {
	t.Helper()
	root := t.TempDir()
	records := productionIndexBenchmarkRecords(count)
	result, err := BuildShardedStreamBaseWithShardCount(
		t.Context(), &productionIndexBenchmarkSource{records: records},
		root, 1, 1, testPersistentIndexRoutingKey(), nil, 1,
	)
	require.NoError(t, err)
	router, err := NewPersistentIndexShardRouter(1, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	base, err := OpenShardedStreamBaseShardArtifacts(root, result.Shards[0], result.ExtentCatalog, router)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	return base, records
}

func TestMergedShardIndexSourceAcrossReadBoundaries(t *testing.T) {
	t.Parallel()
	// Span multiple 256 KiB reads, including a record split across a refill.
	base, records := buildMergedShardScanFixture(t, 14_000)
	source, err := newMergedShardIndexSource(base, nil)
	require.NoError(t, err)
	for range 2 {
		seen := 0
		err := source.Scan(t.Context(), func(key solana.PublicKey, entry AccountIndexEntry) error {
			require.Less(t, seen, len(records))
			require.Equal(t, records[seen].key, key)
			require.Equal(t, records[seen].entry, entry)
			seen++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, len(records), seen)
	}
}

func TestShardedBaseMergeCursorRejectsDamagedRecords(t *testing.T) {
	t.Parallel()
	base, _ := buildMergedShardScanFixture(t, 7_000)
	encoded, err := os.ReadFile(base.scanPath)
	require.NoError(t, err)
	for _, test := range []struct {
		name   string
		damage func([]byte) []byte
		want   error
	}{
		{
			name: "truncated final record",
			damage: func(data []byte) []byte {
				return data[:len(data)-1]
			},
			want: io.ErrUnexpectedEOF,
		},
		{
			name: "body CRC mismatch",
			damage: func(data []byte) []byte {
				// Change the last key without changing its sort order or locator.
				data[len(data)-shardedBaseScanRecordSize+31] ^= 0x80
				return data
			},
			want: ErrInvalidShardedStreamBase,
		},
		{
			name: "duplicate key",
			damage: func(data []byte) []byte {
				last := len(data) - shardedBaseScanRecordSize
				copy(data[last:last+32], data[last-shardedBaseScanRecordSize:last-shardedBaseScanRecordSize+32])
				return data
			},
			want: ErrInvalidShardedStreamBase,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Exercise the cursor directly so open-time validation cannot mask a
			// missing read-time check. The real immutable fixture stays intact.
			path := filepath.Join(t.TempDir(), "damaged.scan")
			require.NoError(t, os.WriteFile(path, test.damage(append([]byte(nil), encoded...)), 0o600))
			file, err := os.Open(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, file.Close()) })
			cursor := newShardedBaseMergeCursor(base, base.catalog.Load(), file)
			for {
				_, _, found, err := cursor.next()
				if err != nil || !found {
					require.ErrorIs(t, err, test.want)
					break
				}
			}
		})
	}
}

func BenchmarkMergedShardIndexSourceBaseScan(b *testing.B) {
	const count = 1_000_000
	base, _ := buildMergedShardScanFixture(b, count)
	source, err := newMergedShardIndexSource(base, nil)
	require.NoError(b, err)
	b.SetBytes(count * shardedBaseScanRecordSize)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		seen := 0
		err := source.Scan(b.Context(), func(solana.PublicKey, AccountIndexEntry) error {
			seen++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		if seen != count {
			b.Fatalf("visited %d records, want %d", seen, count)
		}
	}
}
