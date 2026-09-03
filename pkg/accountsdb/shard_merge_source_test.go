package accountsdb

import (
	"context"
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
