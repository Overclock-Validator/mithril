package replay

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochrewards"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	bin "github.com/gagliardetto/binary"
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
		nil,
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
		nil,
	))
	require.Error(t, replaceInflationRewardRecords(
		block,
		[]*accounts.Account{{Key: key, Lamports: 101}},
		nil,
		rpc.RewardTypeStaking,
		nil,
	))
}

func TestReplaceInflationRewardRecordsKeepsZeroVotingReward(t *testing.T) {
	key := solana.NewWallet().PublicKey()
	block := &b.Block{Slot: 12}
	parent := &accounts.Account{Key: key, Lamports: 100}
	updated := &accounts.Account{Key: key, Lamports: 100}

	require.NoError(t, replaceInflationRewardRecords(
		block,
		[]*accounts.Account{updated},
		[]*accounts.Account{parent},
		rpc.RewardTypeVoting,
		nil,
	))
	require.Len(t, block.Rewards, 1)
	require.Zero(t, block.Rewards[0].Lamports)
	require.Equal(t, uint64(100), block.Rewards[0].PostBalance)
}

func TestReplaceInflationRewardRecordsOmitsZeroStakingReward(t *testing.T) {
	key := solana.PublicKey{1}
	fee := rpc.BlockReward{Pubkey: key, Lamports: 2, RewardType: rpc.RewardTypeFee}
	block := &b.Block{Slot: 12, Rewards: []rpc.BlockReward{
		fee, {Pubkey: key, Lamports: 9, RewardType: rpc.RewardTypeStaking},
	}}
	parent := &accounts.Account{Key: key, Lamports: 100, Data: []byte{1, 2, 3}}
	updated := parent.Clone()

	require.NoError(t, replaceInflationRewardRecords(block,
		[]*accounts.Account{updated}, []*accounts.Account{parent}, rpc.RewardTypeStaking, nil))
	require.Equal(t, []rpc.BlockReward{fee}, block.Rewards)
	require.Equal(t, parent, updated)
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
		nil,
	))
	require.Len(t, block.Rewards, 1)
	require.Equal(t, stake, block.Rewards[0].Pubkey)
	require.Equal(t, int64(5), block.Rewards[0].Lamports)
}

func TestInflationRewardRecordsUseCalculationTimeCommission(t *testing.T) {
	for _, tc := range []struct {
		name              string
		bps               uint16
		delayed           bool
		basisPoints       bool
		customCollector   bool
		collectorIsVote   bool
		disabledInflation bool
		wantCommission    *uint8
		wantVotingRewards uint64
	}{
		{name: "legacy", bps: 700, wantCommission: uint8Ptr(7), wantVotingRewards: 70},
		{name: "zero", wantCommission: uint8Ptr(0)},
		{name: "full commission", bps: 10_000, wantCommission: uint8Ptr(100), wantVotingRewards: 1_000},
		{name: "delayed", bps: 4_000, delayed: true, wantCommission: uint8Ptr(7), wantVotingRewards: 70},
		{name: "credits only", bps: 700, disabledInflation: true, wantCommission: uint8Ptr(7)},
		{name: "basis points", bps: 1_234, basisPoints: true, wantVotingRewards: 124},
		{name: "custom collector", bps: 700, customCollector: true, wantVotingRewards: 70},
		{name: "custom collector is vote", bps: 700, customCollector: true, collectorIsVote: true, wantVotingRewards: 70},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
			db, err := accountsdb.OpenDb(dir)
			require.NoError(t, err)
			db.InitCaches()
			t.Cleanup(db.CloseDb)
			global.ClearPendingStakePubkeys()
			t.Cleanup(global.ClearPendingStakePubkeys)
			for _, epoch := range []uint64{70, 71} {
				global.ClearEpochStakes(epoch)
				t.Cleanup(func() { global.ClearEpochStakes(epoch) })
			}

			const slot = uint64(7_100)
			vote, stake, collector := solana.PublicKey{81}, solana.PublicKey{82}, solana.PublicKey{83}
			if !tc.customCollector || tc.collectorIsVote {
				collector = vote
			}
			stakeState := sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusStake, Stake: sealevel.StakeStateV2Stake{
				Stake: sealevel.Stake{Delegation: sealevel.Delegation{
					VoterPubkey: vote, StakeLamports: 100, ActivationEpoch: math.MaxUint64, DeactivationEpoch: math.MaxUint64,
				}},
			}}
			var encoded bytes.Buffer
			require.NoError(t, stakeState.MarshalWithEncoder(bin.NewBinEncoder(&encoded)))
			stakeData := make([]byte, 200)
			copy(stakeData, encoded.Bytes())
			stakeAccount := &accounts.Account{Key: stake, Lamports: 1_000, Owner: a.StakeProgramAddr, Data: stakeData}
			stored := make(chan struct{})
			require.NoError(t, db.StoreAccounts([]*accounts.Account{
				stakeAccount, {Key: collector, Lamports: 1_000, Owner: a.VoteProgramAddr},
			}, slot, func() { close(stored) }))
			<-stored
			global.EnqueuePendingStakePubkey(slot, stake)
			_, err = global.FlushPendingStakePubkeysThrough(dir, slot)
			require.NoError(t, err)
			db.RootedDurable = true

			f := &features.Features{}
			if tc.basisPoints {
				f.EnableFeature(features.CommissionRateInBasisPoints, 0)
			}
			if tc.customCollector {
				f.EnableFeature(features.CustomCommissionCollector, 0)
			}
			voteState := &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4, V4: sealevel.VoteState4{
				InflationRewardsCommissionBps: tc.bps, InflationRewardsCollector: collector,
				EpochCredits: []sealevel.EpochCredits{{Epoch: 70, Credits: 1_000}},
			}}
			if tc.delayed {
				f.EnableFeature(features.DelayCommissionUpdates, 0)
				global.PutEpochStakes(70, map[solana.PublicKey]uint64{vote: 100},
					map[solana.PublicKey]*epochstakes.VoteAccount{vote: {InflationRewardsCommissionBps: new(uint16(700))}}, 100)
			}
			newRateEpoch := uint64(0)
			pointValueRewards := uint64(1_000)
			if tc.disabledInflation {
				pointValueRewards = 0
			}
			calculated, err := rewards.CalculateRewardsStreaming(db, slot, &sealevel.SysvarStakeHistory{}, &newRateEpoch,
				map[solana.PublicKey]*sealevel.VoteStateVersions{vote: voteState}, rewards.PointValue{Rewards: pointValueRewards}, 70, [32]byte{},
				&sealevel.SlotCtx{Features: f}, f, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}, rewards.RewardCalculationMode{
					FullAlpenglow: true, RewardEpochDelegatedStakes: map[solana.PublicKey]uint64{vote: 100},
				})
			require.NoError(t, err)
			t.Cleanup(func() { rewards.CleanupPartitionedSpoolFiles(calculated.SpoolDir, slot, calculated.NumPartitions) })
			require.Equal(t, uint64(1), calculated.NumPartitions)
			voting, voteParents, votingAmount := rewards.DistributeVotingRewards(db, calculated.ValidatorRewards, slot, nil)
			require.Equal(t, tc.wantVotingRewards, votingAmount)

			// Distribution must use calculation-time metadata, not the current vote
			// state or a stake account whose delegation changed after calculation.
			voteState.V4.InflationRewardsCommissionBps = 9_900
			stakeState.Stake.Stake.Delegation.VoterPubkey = solana.PublicKey{84}
			currentStake := stakeAccount.Clone()
			require.NoError(t, sealevel.MarshalStakeStakeInto(&stakeState, currentStake.Data))
			staking, stakeParents, stakingAmount, burned, stakeCommissions := rewards.DistributeStakingRewardsFromSpool(
				db, calculated.SpoolDir, slot, 0, slot+1, func(key solana.PublicKey) (*accounts.Account, error) {
					require.Equal(t, stake, key)
					return currentStake, nil
				}, calculated.VoteCommissions, rewards.StakeRewardDistributionConfig{})
			require.Zero(t, burned)
			wantStakingAmount, wantRecords := uint64(1_000)-tc.wantVotingRewards, 2
			if tc.disabledInflation {
				wantStakingAmount, wantRecords = 0, 0
			} else if wantStakingAmount == 0 {
				wantRecords = 1
			}
			require.Equal(t, wantStakingAmount, stakingAmount)
			require.Len(t, staking, 1)
			require.Equal(t, uint64(1_000)+wantStakingAmount, staking[0].Lamports)
			updatedStake, err := sealevel.UnmarshalStakeState(staking[0].Data)
			require.NoError(t, err)
			require.Equal(t, uint64(1_000), updatedStake.Stake.Stake.CreditsObserved)
			untrustedCommission := uint8(99)
			block := &b.Block{Slot: slot + 1, Epoch: 71, Rewards: []rpc.BlockReward{
				{Pubkey: collector, RewardType: rpc.RewardTypeVoting, Commission: &untrustedCommission},
				{Pubkey: stake, RewardType: rpc.RewardTypeStaking, Commission: &untrustedCommission},
			}}
			require.NoError(t, replaceInflationRewardRecords(block, voting, voteParents, rpc.RewardTypeVoting, calculated.VoteCommissions))
			require.NoError(t, replaceInflationRewardRecords(block, staking, stakeParents, rpc.RewardTypeStaking, stakeCommissions))
			require.Len(t, block.Rewards, wantRecords)
			for _, reward := range block.Rewards {
				require.Equal(t, tc.wantCommission, reward.Commission, "reward type %s", reward.RewardType)
			}
			historyDir := t.TempDir()
			history, err := epochrewards.Open(historyDir, 100)
			require.NoError(t, err)
			require.NoError(t, history.RecordBlock(block))
			history.RecordCoverage(70, block.Slot, true, true)
			require.NoError(t, history.Prepare(block.Slot))
			require.NoError(t, history.SetRooted(block.Slot))
			reopened, err := epochrewards.Open(historyDir, 100)
			require.NoError(t, err)
			require.NoError(t, reopened.SetRooted(block.Slot))
			require.True(t, reopened.EpochAvailable(70))
			_, hasStakingReward := reopened.Get(70, stake.String(), false)
			require.Equal(t, wantStakingAmount > 0, hasStakingReward)
			for _, reward := range block.Rewards {
				recorded, ok := reopened.Get(70, reward.Pubkey.String(), false)
				require.True(t, ok, "reward type %s", reward.RewardType)
				require.Equal(t, uint64(reward.Lamports), recorded.Amount)
				require.Equal(t, reward.PostBalance, recorded.PostBalance)
				require.Equal(t, block.Slot, recorded.EffectiveSlot)
				require.Equal(t, tc.wantCommission, recorded.Commission)
			}
		})
	}
}

func uint8Ptr(value uint8) *uint8 { return &value }
