package accountsdb

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestReplayNegativeRolloverSelection(t *testing.T) {
	key := func(i int) solana.PublicKey {
		var pk solana.PublicKey
		binary.LittleEndian.PutUint64(pk[:], uint64(i))
		return pk
	}
	newDB := func() *AccountsDb {
		db := &AccountsDb{readCacheEpoch: 7, batchAbsentEpoch: 7,
			batchAbsent: make(map[solana.PublicKey]struct{}),
			pendingFold: map[[32]byte]dedupedVersion{key(2): {}},
		}
		for i := 0; i < batchAbsentLimit-1; i++ {
			db.batchAbsent[key(i)] = struct{}{}
		}
		return db
	}
	testCtx := WithPrefetchNegativeRollover(context.Background())
	for _, pref := range []bool{false, true} {
		ctx := context.Background()
		if pref {
			ctx = testCtx
		}
		db := newDB()
		keys := []solana.PublicKey{key(1), key(2), key(10000), key(10001), key(10002), key(10003), key(1), key(10004), key(10001)}
		db.cacheBatchAbsences(ctx, keys, []int{2, 3, 4, 5, 7, 8}, []bool{false, false, false, true, false, false}, []bool{false, false, true, true, true, true, false, false, true}, 7)
		require.Equal(t, pref, db.batchAccountAbsentLocked(keys[0]), "old hit")
		require.Equal(t, pref, db.batchAccountAbsentLocked(keys[2]), "new miss admitted before rollover")
		require.True(t, db.batchAccountAbsentLocked(keys[3]), "overflowing key")
		require.True(t, db.batchAccountAbsentLocked(keys[4]), "later miss")
		for _, k := range []solana.PublicKey{key(0), keys[1], keys[5], keys[7]} {
			require.False(t, db.batchAccountAbsentLocked(k), "unrelated, pending, found or declined")
		}
	}
	db := newDB()
	db.cacheBatchAbsences(testCtx, []solana.PublicKey{key(1), key(10000), key(10001)}, []int{1, 2}, []bool{false, false}, []bool{false, true, true}, 6)
	require.Len(t, db.batchAbsent, batchAbsentLimit-1)
	require.False(t, db.batchAccountAbsentLocked(key(10000)))

	db = newDB()
	keys := make([]solana.PublicKey, batchAbsentLimit+1)
	cold := make([]int, len(keys))
	admit := make([]bool, len(keys))
	for i := range keys {
		keys[i], cold[i], admit[i] = key(i+10000), i, true
	}
	db.cacheBatchAbsences(testCtx, keys, cold, make([]bool, len(keys)), admit, 7)
	require.Len(t, db.batchAbsent, batchAbsentLimit)
	require.False(t, db.batchAccountAbsentLocked(keys[0]), "oversized keeps original overflow behavior")
	require.True(t, db.batchAccountAbsentLocked(keys[len(keys)-1]))
}
