package accountsdb

import (
	"encoding/binary"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestSparseIndexCacheMatchesCurrentIndex(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	db.foldMu.Lock()
	defer db.foldMu.Unlock()
	rng := rand.New(rand.NewSource(20261024))
	var keys [400][32]byte
	for i := range keys {
		rng.Read(keys[i][:])
	}
	for cycle := 0; cycle < 20; cycle++ {
		for i, k := range keys {
			if rng.Intn(4) == 0 {
				require.NoError(t, db.Index.Delete(k[:], pebble.NoSync))
				continue
			}
			var b [24]byte
			e := AccountIndexEntry{Slot: uint64(cycle), FileId: uint64(i % 7), Offset: uint64(i * 128)}
			e.Marshal(&b)
			require.NoError(t, db.Index.Set(k[:], b[:], pebble.NoSync))
		}
		if cycle%3 == 0 {
			require.NoError(t, db.Index.Flush())
		}
		// New cycle after mutations: both cached presence and absence must refresh.
		var cache sparseIndexCache
		for pass := 0; pass < 3; pass++ {
			for _, i := range rng.Perm(len(keys)) {
				got, found, err := cache.lookup(db, keys[i])
				require.NoError(t, err)
				want, exists, err := (*sparseIndexCache)(nil).lookup(db, keys[i])
				require.NoError(t, err)
				require.Equal(t, exists, found)
				require.Equal(t, want, got)
			}
		}
	}
}

func TestSparseIndexCacheBoundAndMalformedValue(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	db.foldMu.Lock()
	defer db.foldMu.Unlock()
	var cache sparseIndexCache
	for i := 0; i < sparseIndexCacheLimit+10; i++ {
		var key [32]byte
		binary.LittleEndian.PutUint64(key[:8], uint64(i))
		_, found, err := cache.lookup(db, key)
		require.NoError(t, err)
		require.False(t, found)
	}
	require.Len(t, cache.values, sparseIndexCacheLimit)
	key := [32]byte{255, 255, 255}
	require.NoError(t, db.Index.Set(key[:], []byte{1}, pebble.NoSync))
	_, _, err := cache.lookup(db, key)
	require.Error(t, err)
	// Errors are not cached, even with space available.
	cache = sparseIndexCache{}
	_, _, err = cache.lookup(db, key)
	require.Error(t, err)
	require.Empty(t, cache.values)
}
