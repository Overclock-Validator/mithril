package accountsdb

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Splitting a batch's cache probes across goroutines must give exactly the
// serial result: the same account for every answered key, the same remaining
// keys in the same order, and the same counters.
func TestParallelBatchCacheProbeMatchesSerial(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	const stored = 3000
	delta := make([]*accounts.Account, stored)
	for i := range delta {
		var key solana.PublicKey
		binary.LittleEndian.PutUint32(key[:], uint32(i+1))
		key[31] = 7
		delta[i] = &accounts.Account{Key: key, Lamports: uint64(i + 1), Data: []byte{byte(i)}}
	}
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: delta}}, 100, nil, nil)
	require.NoError(t, err)

	// A mix of every outcome: cached accounts, accounts only the index has, and
	// keys that do not exist (some of them already in the negative cache).
	var keys []solana.PublicKey
	for i, acct := range delta {
		if i%3 == 0 {
			db.CommonAcctsCache.Delete(acct.Key)
		}
		keys = append(keys, acct.Key)
		if i%5 == 0 {
			var missing solana.PublicKey
			binary.LittleEndian.PutUint32(missing[:], uint32(i+1))
			missing[31] = 9
			keys = append(keys, missing)
		}
	}
	_, _, err = db.GetAccountsBatchSharedWithStats(context.Background(), 100, keys[:len(keys)/2])
	require.NoError(t, err)

	probe := func(serial bool, ctx context.Context) ([]*accounts.Account, []int, BatchReadStats) {
		previous := batchCacheProbeSerial
		batchCacheProbeSerial = serial
		defer func() { batchCacheProbeSerial = previous }()
		out := make([]*accounts.Account, len(keys))
		var stats BatchReadStats
		db.readCacheEpochMu.RLock()
		defer db.readCacheEpochMu.RUnlock()
		return out, db.probeBatchCachesLocked(ctx, keys, out, &stats), stats
	}
	wantOut, wantCold, wantStats := probe(true, context.Background())
	require.NotZero(t, wantStats.CacheHits)
	require.NotZero(t, wantStats.NegativeCacheHits)
	require.NotEmpty(t, wantCold)

	require.Greater(t, batchCacheProbeWorkers(context.Background(), 8*batchCacheProbeChunk), 1, "test needs more than one probe worker")
	for name, ctx := range map[string]context.Context{
		"default workers": context.Background(),
		"limited workers": WithBatchWorkerLimit(context.Background(), 3),
	} {
		gotOut, gotCold, gotStats := probe(false, ctx)
		require.Equal(t, wantCold, gotCold, name)
		require.Equal(t, wantStats, gotStats, name)
		for i := range wantOut {
			if wantOut[i] == nil {
				require.Nil(t, gotOut[i], name)
				continue
			}
			require.Equal(t, wantOut[i].Key, gotOut[i].Key, name)
			require.Equal(t, wantOut[i].Lamports, gotOut[i].Lamports, name)
			require.Equal(t, wantOut[i].Data, gotOut[i].Data, name)
		}
	}

	require.Equal(t, 1, batchCacheProbeWorkers(context.Background(), batchCacheProbeChunk), "small batches stay on the caller")
	require.Equal(t, 1, batchCacheProbeWorkers(WithBatchWorkerLimit(context.Background(), 1), 1<<20))
}
