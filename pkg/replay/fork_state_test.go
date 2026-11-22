package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestReplayForkStateOwnsRewardCursor(t *testing.T) {
	ctx := &ReplayCtx{Capitalization: 100, EpochAcctsHash: []byte{1}, HasEpochAcctsHash: true, CurrentFeatures: features.NewFeaturesDefault()}
	info := &rewards.PartitionedRewardDistributionInfo{NumRewardPartitionsRemaining: 3}
	snapshot := captureReplayForkState(9, ctx, true, info, partitionedRewardsCompletion{}, 100, 4)
	ctx.Capitalization, ctx.EpochAcctsHash[0], info.NumRewardPartitionsRemaining = 200, 2, 0
	restored, completion := snapshot.restore(ctx)
	require.Equal(t, uint64(100), ctx.Capitalization)
	require.Equal(t, []byte{1}, ctx.EpochAcctsHash)
	require.Equal(t, uint64(3), restored.NumRewardPartitionsRemaining)
	require.Same(t, restored, completion.info)
	restored.NumRewardPartitionsRemaining, ctx.EpochAcctsHash[0] = 1, 8
	second, _ := snapshot.restore(ctx)
	require.Equal(t, uint64(3), second.NumRewardPartitionsRemaining, "another sibling starts from the same parent cursor")
	require.Equal(t, []byte{1}, ctx.EpochAcctsHash)
}

func TestForkUnwindAcrossEpochRestoresBankOwnedState(t *testing.T) {
	resetVoteStakeDirty()
	t.Cleanup(resetVoteStakeDirty)
	oldCaches := global.CaptureEpochCaches()
	t.Cleanup(func() { global.RestoreEpochCaches(oldCaches) })
	key := solana.PublicKey{0xea, 1}
	global.PutEpochStakes(1, map[solana.PublicKey]uint64{key: 100}, nil, 100)
	tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
	parentCtx := &ReplayCtx{Capitalization: 1000, CurrentFeatures: features.NewFeaturesDefault()}
	tail.Add(9, []*accounts.Account{testAccount(1, 9)}, testHashBytes(9))
	tail.SetContext(9, forkTestContext(9), testUnwindBankSysvars(t, 9, 0))
	tail.SetForkState(9, captureReplayForkState(0, parentCtx, true, nil, partitionedRewardsCompletion{}, 0, 0))
	_, _, err := tail.flush(9)
	require.NoError(t, err)
	global.PutEpochStakes(2, map[solana.PublicKey]uint64{key: 200}, nil, 200)
	tail.Add(10, []*accounts.Account{testAccount(1, 10)}, testHashBytes(10))
	tail.SetContext(10, forkTestContext(10), testUnwindBankSysvars(t, 10, 100))
	active := &rewards.PartitionedRewardDistributionInfo{NumRewardPartitionsRemaining: 3}
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 10, LeaderScheduleSlotOffset: 10}
	snapshot := tail.retainedForkState(10)
	rs, bank, reason := tryInLoopUnwind(&CertifiedSwitch{Slot: 10}, tail, &state.MithrilState{LastRootedSlot: 9}, schedule, 1, active)
	require.Empty(t, reason)
	require.Equal(t, uint64(9), rs.ParentSlot)
	require.Equal(t, uint64(9), bank.Slot())
	require.Equal(t, uint64(100), global.EpochTotalStake(1))
	_, exists := global.EpochStakesSnapshot(2)
	require.False(t, exists, "discarded future generation is not retained by replay")
	restored, completion := snapshot.restore(parentCtx)
	require.Nil(t, restored)
	require.Zero(t, completion.slot)
	require.Equal(t, uint64(1000), parentCtx.Capitalization)
}

func TestRestoredForkParentReadsRetainedAccountsAndSysvars(t *testing.T) {
	oldCaches := global.CaptureEpochCaches()
	t.Cleanup(func() { global.RestoreEpochCaches(oldCaches) })
	key := solana.PublicKey{0xec, 1}
	global.PutEpochStakes(1, map[solana.PublicKey]uint64{key: 123}, nil, 123)
	tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
	acct := testAccount(1, 700)
	tail.Add(7, []*accounts.Account{acct}, testHashBytes(7))
	bank := testUnwindBankSysvars(t, 7, 456)
	rs := &ResumeState{ParentSlot: 7, LamportsPerSignature: 5000, PrevLamportsPerSignature: 4500, NumSignatures: 12}
	parent, err := restoredParentSlotCtx(rs, bank, tail, nil, &state.MithrilState{ManifestFeeRateGovernor: &state.ManifestFeeRateGovernorSeed{}}, 0, &ReplayCtx{CurrentFeatures: features.NewFeaturesDefault()})
	require.NoError(t, err)
	require.Same(t, bank, parent.BankSysvars())
	loaded, err := parent.GetAccountFromAccountsDb(acct.Key)
	require.NoError(t, err)
	require.Equal(t, uint64(700), loaded.Lamports, "must read retained overlay, without a durable DB")
	epochAcct, err := epochRewardAccountLoader(nil, 8, parent, nil)(sealevel.SysvarEpochRewardsAddr)
	require.NoError(t, err)
	expected, ok := bank.CloneAccount(sealevel.SysvarEpochRewardsAddr)
	require.True(t, ok)
	require.Equal(t, expected.Data, epochAcct.Data)
	require.Equal(t, uint64(123), parent.TotalEpochStake)
	require.Equal(t, uint64(5000), parent.FeeRateGovernor.LamportsPerSignature)
}
