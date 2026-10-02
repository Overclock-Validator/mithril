package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochrewards"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestInflationRewardsSurviveCompletionFoldRetirementAndRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := epochrewards.Open(dir, 100)
	require.NoError(t, err)
	require.NoError(t, store.SetRooted(4))
	key := solana.PublicKey{1}
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 5, Epoch: 1, Rewards: []rpc.BlockReward{
		{Pubkey: key, Lamports: 7, PostBalance: 107, RewardType: rpc.RewardTypeVoting},
	}}))
	committer := &fakeCommitter{durable: accounts.NewMemAccounts(), failOn: 7}
	tail := asyncTestTail(committer, 5, 6, 7, 8)
	statuses := NewTransactionStatusCache()
	checkpointDir := t.TempDir()
	require.NoError(t, tail.SetTransactionStatusCheckpointHooks(TransactionStatusCheckpointHooks{
		Capture: statuses.CaptureSnapshotThrough,
		Install: func(through uint64, payload []byte) (*state.TransactionStatusCheckpointRef, error) {
			ref, err := PrepareTransactionStatusCheckpoint(checkpointDir, through, payload)
			if err != nil {
				return nil, err
			}
			return ref, store.Prepare(through)
		},
	}))
	info := &rewards.PartitionedRewardDistributionInfo{}
	var completion partitionedRewardsCompletion
	completion.observeBank(info, testUnwindBankSysvars(t, 7, 50))
	job, err := tail.buildRewardsCompletionFoldJob(completion.slot)
	require.NoError(t, err)
	require.Error(t, runFoldJob(committer, job))
	_, visible := store.Get(0, key.String(), false)
	require.False(t, visible, "a prepared reward must not precede its committed bank")
	require.False(t, completion.retire(&info, store.RootedSlot()))

	committer.failOn = 0
	job, err = tail.buildRewardsCompletionFoldJob(completion.slot)
	require.NoError(t, err)
	require.NoError(t, runFoldJob(committer, job))
	require.NotNil(t, tail.applyFoldJob(job))
	require.NoError(t, store.SetRooted(job.through))
	require.True(t, completion.retire(&info, job.through))
	want, visible := store.Get(0, key.String(), false)
	require.True(t, visible)
	require.Equal(t, uint64(7), want.Amount)

	reopened, err := epochrewards.Open(dir, 100)
	require.NoError(t, err)
	require.NoError(t, reopened.SetRooted(job.through))
	got, visible := reopened.Get(0, key.String(), false)
	require.True(t, visible)
	require.Equal(t, want, got)
}

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
