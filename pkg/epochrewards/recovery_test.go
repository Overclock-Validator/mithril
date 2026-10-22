package epochrewards

import (
	"os"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestStoreRestoresCommittedRewardAfterOrphanReadRetry(t *testing.T) {
	store, err := Open(t.TempDir(), 100)
	require.NoError(t, err)
	address := solana.PublicKey{5}
	for _, through := range []uint64{6, 8} {
		require.NoError(t, store.RecordBlock(&b.Block{Slot: 5, Epoch: 1, Rewards: []rpc.BlockReward{{
			Pubkey: address, Lamports: int64(through), RewardType: rpc.RewardTypeStaking,
		}}}))
		require.NoError(t, store.Prepare(through))
	}

	committed := store.batchPath(6)
	require.NoError(t, os.Rename(committed, committed+".saved"))
	require.Error(t, store.SetRooted(6))
	require.FileExists(t, store.batchPath(8), "a failed recovery must leave the orphan available for retry")
	_, ok := store.Get(0, address.String(), false)
	require.False(t, ok)
	require.NoError(t, os.Rename(committed+".saved", committed))
	require.NoError(t, store.SetRooted(6))
	record, ok := store.Get(0, address.String(), false)
	require.True(t, ok)
	require.Equal(t, uint64(6), record.Amount)
}
