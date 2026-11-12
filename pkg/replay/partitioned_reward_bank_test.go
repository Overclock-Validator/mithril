package replay

import (
	"math"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/bankhash"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type rewardBankReader map[solana.PublicKey]*accounts.Account

func (r rewardBankReader) GetAccount(_ uint64, key solana.PublicKey) (*accounts.Account, error) {
	if acct := r[key]; acct != nil {
		return acct.Clone(), nil
	}
	return nil, accountsdb.ErrNoAccount
}

func rewardBankFixture(t *testing.T) (*sealevel.SlotCtx, rewardBankReader, *rewards.PartitionedRewardDistributionInfo, solana.PublicKey) {
	t.Helper()
	stakeKey := solana.PublicKey{41}
	state := &sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusStake}
	state.Stake.Stake.Delegation = sealevel.Delegation{StakeLamports: 1000, ActivationEpoch: 1, DeactivationEpoch: math.MaxUint64}
	state.Stake.Stake.CreditsObserved = 7
	stake := &accounts.Account{Key: stakeKey, Owner: addresses.StakeProgramAddr, Lamports: 2000, Data: make([]byte, 200)}
	require.NoError(t, sealevel.MarshalStakeStakeInto(state, stake.Data))
	sysvar := sealevel.SysvarEpochRewards{Active: true, NumPartitions: 2, DistributionStartingBlockHeight: 501, ParentBlockhash: [32]byte{9}, TotalRewards: 53, DistributedRewards: 20}
	epochAcct := &accounts.Account{Key: sealevel.SysvarEpochRewardsAddr, Owner: addresses.SysvarOwnerAddr, Lamports: 1, Data: encodeEpochRewardsForTest(t, sysvar)}
	reader := rewardBankReader{stakeKey: stake, epochAcct.Key: epochAcct}
	spool := rewards.NewPartitionedSpoolWriters(t.TempDir(), 100, 2)
	require.NoError(t, spool.WriteRecord(&rewards.SpoolRecord{StakePubkey: stakeKey, VotePubkey: solana.PublicKey{55}, RewardLamports: 11, CreditsObserved: 9}))
	// A closed stake burns the reward: sysvar progress includes it, capitalization does not.
	require.NoError(t, spool.WriteRecord(&rewards.SpoolRecord{StakePubkey: solana.PublicKey{42}, RewardLamports: 9, CreditsObserved: 9}))
	require.NoError(t, spool.WriteRecord(&rewards.SpoolRecord{StakePubkey: stakeKey, RewardLamports: 13, CreditsObserved: 10, PartitionIndex: 1}))
	require.NoError(t, spool.Close())
	plan, err := rewards.NewPartitionedRewardPlan(spool.SpoolDir(), spool.Slot(), sysvar, map[solana.PublicKey]uint8{{55}: 5})
	require.NoError(t, err)
	f := &features.Features{}
	f.EnableFeature(features.AccountsLtHash, 0)
	f.EnableFeature(features.RemoveAccountsDeltaHash, 0)
	parent := &sealevel.SlotCtx{Slot: 100, Features: f, UnrootedRead: reader, Accounts: accounts.NewMemAccounts()}
	bank, err := sealevel.NewBankSysvars(100, epochAcct)
	require.NoError(t, err)
	require.NoError(t, parent.PublishBankSysvars(bank))
	return parent, reader, &rewards.PartitionedRewardDistributionInfo{Plan: plan, SpoolDir: spool.SpoolDir(), SpoolSlot: spool.Slot(), NumRewardPartitionsRemaining: 2}, stakeKey
}

func newRewardLeaderChild(t *testing.T, parent *sealevel.SlotCtx, slot uint64) *sealevel.SlotCtx {
	t.Helper()
	child := &sealevel.SlotCtx{Slot: slot, ParentSlot: parent.Slot, Epoch: 12, Features: parent.Features.Clone(),
		UnrootedRead: parent.UnrootedRead, Accounts: accounts.NewMemAccounts(), ParentAccts: accounts.NewMemAccounts(),
		AcctMapsMu: &sync.Mutex{}, ModifiedAccts: make(map[solana.PublicKey]bool), WritableAccts: make(map[solana.PublicKey]bool), AcctsLtHash: new(lthash.LtHash)}
	bank, err := parent.BankSysvars().Derive(slot)
	require.NoError(t, err)
	require.NoError(t, child.PublishBankSysvars(bank))
	return child
}

func TestLeaderRewardPartitionReplayParityAndDiscard(t *testing.T) {
	oldCache := sealevel.SysvarCache
	t.Cleanup(func() { sealevel.SysvarCache = oldCache; ResetLocalLeaderCommits() })
	parent, reader, info, key := rewardBankFixture(t)
	original := reader[key].Clone()
	originalEpoch := reader[sealevel.SysvarEpochRewardsAddr].Clone()
	child := newRewardLeaderChild(t, parent, 105) // skipped slots do not skip partition zero
	block := &b.Block{Slot: 105, ParentSlot: 100, BlockHeight: 501, Epoch: 12, FromLocalProduction: true}
	receipt, err := PrepareLeaderEpochRewards(child, block, info.Plan)
	require.NoError(t, err)
	require.NotNil(t, receipt)
	require.Equal(t, uint64(2), info.NumRewardPartitionsRemaining, "speculative production cannot advance replay progress")
	require.Equal(t, oldCache, sealevel.SysvarCache, "producer cannot publish the global sysvar cache")
	paid, err := child.GetAccount(key)
	require.NoError(t, err)
	require.Equal(t, uint64(2011), paid.Lamports, "transactions must see rewards before they execute")
	state, err := sealevel.UnmarshalStakeState(paid.Data)
	require.NoError(t, err)
	require.Equal(t, uint64(9), state.Stake.Stake.CreditsObserved)
	bankRewards, ok := child.BankSysvars().EpochRewards()
	require.True(t, ok)
	require.True(t, bankRewards.Active)
	require.Equal(t, uint64(40), bankRewards.DistributedRewards)
	require.Equal(t, original, reader[key])
	require.Equal(t, originalEpoch, reader[sealevel.SysvarEpochRewardsAddr])

	// Abandoning the first bank and constructing a sibling reuses partition zero.
	sibling := newRewardLeaderChild(t, parent, 108)
	siblingReceipt, err := PrepareLeaderEpochRewards(sibling, &b.Block{Slot: 108, BlockHeight: 501, Epoch: 12}, info.Plan)
	require.NoError(t, err)
	require.Equal(t, receipt.updated, siblingReceipt.updated)

	replayCtx := &ReplayCtx{CurrentFeatures: parent.Features, Capitalization: 5000}
	updated, parents, _ := distributePartitionedEpochRewardsForSlot(nil, parent, nil, replayCtx, info, 105, 501, 12)
	require.Equal(t, uint64(5011), replayCtx.Capitalization, "burned rewards must not capitalize")
	require.Equal(t, uint64(1), info.NumRewardPartitionsRemaining)
	block.EpochUpdatedAccts, block.ParentEpochUpdatedAccts = coalesceEpochAccountUpdates(updated, parents)
	require.NoError(t, receipt.validate(block))

	// Model a transaction changing the rewarded stake after preparation. Both
	// finalizers must use its final image but subtract the original parent image.
	paid.Lamports += 3
	require.NoError(t, child.SetAccount(key, paid))
	produced := &b.Block{Slot: 105}
	receipt.Attach(produced)
	writable, modified := compileLeaderAccounts(child, produced, nil)
	require.NoError(t, ensureParentAccountsForModified(child, modified))
	gotHash := bankhash.CalculateBankHash(child, writable, modified, [32]byte{1}, 0, [32]byte{2})
	remote := newRewardLeaderChild(t, parent, 105)
	var expectedModified []*accounts.Account
	for i, account := range updated {
		if account == nil {
			continue
		}
		account = account.Clone()
		if account.Key == key {
			account.Lamports += 3
		}
		require.NoError(t, remote.ParentAccts.SetAccountWithoutLock(account.Key, parents[i].Clone()))
		expectedModified = append(expectedModified, account)
	}
	wantHash := bankhash.CalculateBankHash(remote, nil, expectedModified, [32]byte{1}, 0, [32]byte{2})
	require.Equal(t, wantHash, gotHash, "leader/replay LtHash and bank hash must agree")
	require.Equal(t, remote.AcctsLtHash.Hash(), child.AcctsLtHash.Hash())
	child.FinalBankhash = gotHash
	RegisterLocalLeaderCommit(child, receipt)
	adopted, err := adoptLocalLeaderBlock(block, nil, NewTransactionStatusCache(), nil)
	require.NoError(t, err)
	require.Same(t, child, adopted)
	got, err := adopted.GetAccount(key)
	require.NoError(t, err)
	require.Equal(t, uint64(2014), got.Lamports, "handoff cannot apply the reward again")
	require.Equal(t, uint64(5011), replayCtx.Capitalization)
}

func TestRewardPlanCompletesByBankHeightAndSurvivesUntilDurability(t *testing.T) {
	oldCache := sealevel.SysvarCache
	t.Cleanup(func() { sealevel.SysvarCache = oldCache })
	parent, _, info, _ := rewardBankFixture(t)
	ctx := &ReplayCtx{CurrentFeatures: parent.Features}
	first, _, _ := distributePartitionedEpochRewardsForSlot(nil, parent, nil, ctx, info, 105, 501, 12)
	values := rewardBankReader{}
	for _, acct := range first {
		if acct != nil {
			values[acct.Key] = acct
		}
	}
	parent = newRewardLeaderChild(t, parent, 105)
	parent.UnrootedRead = values
	bank, err := parent.BankSysvars().WithAccounts(values[sealevel.SysvarEpochRewardsAddr])
	require.NoError(t, err)
	require.NoError(t, parent.PublishBankSysvars(bank))
	last, _, _ := distributePartitionedEpochRewardsForSlot(nil, parent, nil, ctx, info, 130, 502, 12)
	require.Zero(t, info.NumRewardPartitionsRemaining)
	require.Equal(t, uint64(24), ctx.Capitalization)
	completed := decodeEpochRewardsForTest(t, last[len(last)-1].Data)
	require.False(t, completed.Active)
	require.Equal(t, uint64(53), completed.DistributedRewards)
	// A sibling of the completing bank can still read the final partition.
	sibling := newRewardLeaderChild(t, parent, 131)
	_, err = PrepareLeaderEpochRewards(sibling, &b.Block{Slot: 131, BlockHeight: 502, Epoch: 12}, info.Plan)
	require.NoError(t, err)
	bank, err = sealevel.NewBankSysvars(130, last[len(last)-1])
	require.NoError(t, err)
	var completion partitionedRewardsCompletion
	completion.observeBank(info, bank)
	require.False(t, completion.retire(&info, 129))
	plan := info.Plan
	require.True(t, completion.retire(&info, 130))
	require.Nil(t, info)
	_, err = PrepareLeaderEpochRewards(newRewardLeaderChild(t, parent, 132), &b.Block{Slot: 132, BlockHeight: 502, Epoch: 12}, plan)
	require.ErrorContains(t, err, "unavailable reward partition")
}

func TestLeaderRewardsRejectMissingOrMismatchedPlanAndHandoff(t *testing.T) {
	parent, _, info, _ := rewardBankFixture(t)
	child := newRewardLeaderChild(t, parent, 105)
	block := &b.Block{Slot: 105, ParentSlot: 100, BlockHeight: 501, Epoch: 12}
	_, err := PrepareLeaderEpochRewards(child, block, nil)
	require.ErrorContains(t, err, "no matching partition plan")
	require.Empty(t, child.ModifiedAccts)
	_, err = PrepareLeaderEpochRewards(child, &b.Block{Slot: 105, BlockHeight: 503, Epoch: 12}, info.Plan)
	require.ErrorContains(t, err, "beyond")
	require.Empty(t, child.ModifiedAccts)
	receipt, err := PrepareLeaderEpochRewards(child, block, info.Plan)
	require.NoError(t, err)
	receipt.Attach(block)
	block.ParentEpochUpdatedAccts[0].Lamports++
	require.ErrorContains(t, receipt.validate(block), "differs")
	child.FinalBankhash = make([]byte, 32)
	RegisterLocalLeaderCommit(child, receipt)
	t.Cleanup(ResetLocalLeaderCommits)
	_, err = adoptLocalLeaderBlock(block, nil, NewTransactionStatusCache(), nil)
	require.ErrorContains(t, err, "differs")
}
