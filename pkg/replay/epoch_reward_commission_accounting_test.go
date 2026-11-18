package replay

import (
	"math"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestEpochRewardsCountsUnpayableCommissionWithoutCapitalizingIt(t *testing.T) {
	for _, unpaid := range []uint64{3, 1_167} {
		t.Run(strconv.FormatUint(unpaid, 10), func(t *testing.T) {
			paidKey, missingKey := solana.PublicKey{1}, solana.PublicKey{2}
			paidReward, unpaidReward := new(atomic.Uint64), new(atomic.Uint64)
			paidReward.Store(7)
			unpaidReward.Store(unpaid)
			calculated := &rewards.StreamingRewardsResult{
				NumPartitions: 2, TotalStakerRewards: 11,
				ValidatorRewards: map[solana.PublicKey]*atomic.Uint64{
					paidKey: paidReward, missingKey: unpaidReward,
				},
			}
			block := &b.Block{Slot: 12, BlockHeight: 9, LastBlockhash: solana.Hash{3}}
			initial, err := newEpochRewardsForDistribution(block, 7+unpaid+11, calculated)
			require.NoError(t, err)

			parent := &accounts.Account{Key: paidKey, Lamports: 100}
			updated, parents, deposited := rewards.DistributeVotingRewards(nil, calculated.ValidatorRewards, block.Slot,
				func(key solana.PublicKey) (*accounts.Account, error) {
					if key == paidKey {
						return parent, nil
					}
					return nil, accountsdb.ErrNoAccount
				})
			require.Equal(t, uint64(7), deposited)
			require.Equal(t, uint64(100), parent.Lamports, "retained parent must remain unchanged")
			require.Equal(t, uint64(7)+unpaid, initial.DistributedRewards,
				"a calculated commission is consumed even when its recipient is absent")
			require.Equal(t, uint64(18), capitalizingEpochRewards(deposited, calculated.TotalStakerRewards),
				"unpaid commissions must not inflate the next epoch's capitalization")

			require.NoError(t, replaceInflationRewardRecords(block, updated, parents, rpc.RewardTypeVoting, nil))
			require.Len(t, block.Rewards, 1, "unpaid commissions must not produce a payment record")
			require.Equal(t, paidKey, block.Rewards[0].Pubkey)
			require.Equal(t, int64(7), block.Rewards[0].Lamports)
			require.Equal(t, uint64(107), block.Rewards[0].PostBalance)
		})
	}
}

func TestEpochRewardsRejectsInvalidCalculatedCommissionTotals(t *testing.T) {
	value := func(n uint64) *atomic.Uint64 {
		v := new(atomic.Uint64)
		v.Store(n)
		return v
	}
	for _, tc := range []struct {
		name    string
		rewards map[solana.PublicKey]*atomic.Uint64
		budget  uint64
		wantErr string
	}{
		{"missing", map[solana.PublicKey]*atomic.Uint64{{1}: nil}, 10, "missing calculated commission"},
		{"overflow", map[solana.PublicKey]*atomic.Uint64{{1}: value(math.MaxUint64), {2}: value(1)}, math.MaxUint64, "total overflow"},
		{"over budget", map[solana.PublicKey]*atomic.Uint64{{1}: value(11)}, 10, "exceed epoch reward budget"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newEpochRewardsForDistribution(&b.Block{}, tc.budget,
				&rewards.StreamingRewardsResult{ValidatorRewards: tc.rewards})
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}
