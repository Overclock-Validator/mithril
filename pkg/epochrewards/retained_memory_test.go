package epochrewards

import (
	"fmt"
	"runtime"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestRetainedRewardsPreserveEpochsAndOptionalCommission(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 100)
	require.NoError(t, err)
	address := solana.PublicKey{31}
	zero, max := uint8(0), uint8(255)
	commissions := []*uint8{nil, &zero, &max}
	want := make([]Record, len(commissions))
	for i, commission := range commissions {
		epoch, slot := uint64(i+1), uint64(10*(i+1))
		require.NoError(t, store.RecordBlock(&b.Block{
			Slot: slot, Epoch: epoch + 1,
			Rewards: []rpc.BlockReward{{
				Pubkey: address, Lamports: int64(i), PostBalance: uint64(100 + i),
				RewardType: rpc.RewardTypeVoting, Commission: commission,
			}},
		}))
		want[i], _ = store.Get(epoch, address.String(), true)
		require.NoError(t, store.Prepare(slot))
		require.NoError(t, store.SetRooted(slot))
	}

	for _, reopen := range []bool{false, true} {
		if reopen {
			store, err = Open(dir, 100)
			require.NoError(t, err)
			require.NoError(t, store.SetRooted(30))
		}
		for _, expected := range want {
			got, ok := store.Get(expected.Epoch, address.String(), false)
			require.True(t, ok)
			require.Equal(t, expected, got)
			published, ok := store.GetPublished(expected.Epoch, address.String())
			require.True(t, ok)
			require.Equal(t, expected, published)
			if published.Commission != nil {
				*published.Commission = 17
				again, ok := store.GetPublished(expected.Epoch, address.String())
				require.True(t, ok)
				require.Equal(t, expected, again, "RPC results must not mutate retained history")
			}
		}
	}
}

// Keep the previous representation here to measure retained heap with the same
// records and map growth, including address strings and per-batch prune keys.
type legacyRewardHistory struct {
	persisted   map[rewardKey]Record
	sourceBatch map[rewardKey]uint64
	batches     map[uint64][]rewardKey
}

func buildRewardHistory(count int, compact bool) any {
	records := make([]Record, count)
	for i := range records {
		records[i] = Record{
			Epoch: 100, EffectiveSlot: 200, Address: fmt.Sprintf("%044d", i),
			Amount: uint64(i), PostBalance: uint64(i + 1000),
		}
	}
	if compact {
		store := &Store{
			persisted: make(map[rewardKey]persistedReward),
			batches:   make(map[uint64][]rewardKey),
		}
		store.install(batch{Through: 200, Records: records})
		return store
	}
	store := &legacyRewardHistory{
		persisted: make(map[rewardKey]Record), sourceBatch: make(map[rewardKey]uint64),
		batches: make(map[uint64][]rewardKey),
	}
	keys := make([]rewardKey, 0, len(records))
	for _, record := range records {
		key := rewardKey{epoch: record.Epoch, address: record.Address}
		store.persisted[key], store.sourceBatch[key] = record, 200
		keys = append(keys, key)
	}
	store.batches[200] = keys
	return store
}

func BenchmarkRewardHistoryRetention(bm *testing.B) {
	for _, count := range []int{100_000, 1_000_000} {
		for _, compact := range []bool{false, true} {
			name := "previous"
			if compact {
				name = "compact"
			}
			bm.Run(fmt.Sprintf("%s/%d", name, count), func(bm *testing.B) {
				var retained uint64
				for i := 0; i < bm.N; i++ {
					runtime.GC()
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					history := buildRewardHistory(count, compact)
					runtime.GC()
					runtime.ReadMemStats(&after)
					retained += after.HeapAlloc - before.HeapAlloc
					runtime.KeepAlive(history)
				}
				bm.ReportMetric(float64(retained)/float64(bm.N*count), "retained-B/reward")
			})
		}
	}
}
