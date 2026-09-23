package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestReplaceInflationRewardRecordsUsesAppliedBalanceDelta(t *testing.T) {
	key := solana.NewWallet().PublicKey()
	block := &b.Block{Slot: 12, Rewards: []rpc.BlockReward{
		{Pubkey: key, Lamports: 9, RewardType: rpc.RewardTypeVoting},
		{Pubkey: key, Lamports: 2, RewardType: rpc.RewardTypeFee},
	}}
	parent := &accounts.Account{Key: key, Lamports: 100}
	updated := &accounts.Account{Key: key, Lamports: 107}

	require.NoError(t, replaceInflationRewardRecords(
		block,
		[]*accounts.Account{updated, nil},
		[]*accounts.Account{parent, nil},
		rpc.RewardTypeVoting,
	))
	require.Len(t, block.Rewards, 2)
	require.Equal(t, rpc.RewardTypeFee, block.Rewards[0].RewardType)
	require.Equal(t, rpc.RewardTypeVoting, block.Rewards[1].RewardType)
	require.Equal(t, int64(7), block.Rewards[1].Lamports)
	require.Equal(t, uint64(107), block.Rewards[1].PostBalance)
}

func TestReplaceInflationRewardRecordsRejectsInvalidAccountPairs(t *testing.T) {
	key := solana.NewWallet().PublicKey()
	block := &b.Block{Slot: 12}
	require.Error(t, replaceInflationRewardRecords(
		block,
		[]*accounts.Account{{Key: key, Lamports: 99}},
		[]*accounts.Account{{Key: key, Lamports: 100}},
		rpc.RewardTypeStaking,
	))
	require.Error(t, replaceInflationRewardRecords(
		block,
		[]*accounts.Account{{Key: key, Lamports: 101}},
		nil,
		rpc.RewardTypeStaking,
	))
}

func TestReplaceInflationRewardRecordsKeepsZeroAmount(t *testing.T) {
	key := solana.NewWallet().PublicKey()
	block := &b.Block{Slot: 12}
	parent := &accounts.Account{Key: key, Lamports: 100}
	updated := &accounts.Account{Key: key, Lamports: 100}

	require.NoError(t, replaceInflationRewardRecords(
		block,
		[]*accounts.Account{updated},
		[]*accounts.Account{parent},
		rpc.RewardTypeVoting,
	))
	require.Len(t, block.Rewards, 1)
	require.Zero(t, block.Rewards[0].Lamports)
	require.Equal(t, uint64(100), block.Rewards[0].PostBalance)
}

func TestReplaceInflationRewardRecordsOmitsEpochRewardsSysvar(t *testing.T) {
	stake := solana.NewWallet().PublicKey()
	block := &b.Block{Slot: 12}
	require.NoError(t, replaceInflationRewardRecords(
		block,
		[]*accounts.Account{
			{Key: stake, Lamports: 105},
			{Key: sealevel.SysvarEpochRewardsAddr, Lamports: 1},
		},
		[]*accounts.Account{
			{Key: stake, Lamports: 100},
			{Key: sealevel.SysvarEpochRewardsAddr, Lamports: 1},
		},
		rpc.RewardTypeStaking,
	))
	require.Len(t, block.Rewards, 1)
	require.Equal(t, stake, block.Rewards[0].Pubkey)
	require.Equal(t, int64(5), block.Rewards[0].Lamports)
}
