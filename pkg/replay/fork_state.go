package replay

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

// replayForkState is the small, immutable replay bookkeeping attached to a
// retained bank. Large epoch stake maps and reward partition files are shared
// immutable data; mutable reward progress is copied on capture and restore.
type replayForkState struct {
	epoch            uint64
	ctx              ReplayCtx
	rewardsEnabled   bool
	rewards          *rewards.PartitionedRewardDistributionInfo
	completionSlot   uint64
	rewardsHoldBelow uint64
	rewardWindow     uint64
}

func cloneRewardProgress(info *rewards.PartitionedRewardDistributionInfo) *rewards.PartitionedRewardDistributionInfo {
	if info == nil {
		return nil
	}
	copy := *info
	return &copy
}

func captureReplayForkState(epoch uint64, ctx *ReplayCtx, enabled bool, info *rewards.PartitionedRewardDistributionInfo, completion partitionedRewardsCompletion, hold, window uint64) *replayForkState {
	copy := *ctx
	copy.EpochAcctsHash = append([]byte(nil), ctx.EpochAcctsHash...)
	s := &replayForkState{epoch: epoch, ctx: copy, rewardsEnabled: enabled, rewards: cloneRewardProgress(info), rewardsHoldBelow: hold, rewardWindow: window}
	if completion.info == info {
		s.completionSlot = completion.slot
	}
	return s
}

func (s *replayForkState) restore(ctx *ReplayCtx) (*rewards.PartitionedRewardDistributionInfo, partitionedRewardsCompletion) {
	*ctx = s.ctx
	ctx.EpochAcctsHash = append([]byte(nil), s.ctx.EpochAcctsHash...)
	info := cloneRewardProgress(s.rewards)
	return info, partitionedRewardsCompletion{info: info, slot: s.completionSlot}
}

// restoredParentSlotCtx reconstructs only the parent bank shell. It must never
// reload votes/stakes from durable storage: the journal and epoch-cache view
// have already restored the selected parent, including its unrooted writes.
func restoredParentSlotCtx(rs *ResumeState, sysvars *sealevel.BankSysvars, tail *unrootedTail, db *accountsdb.AccountsDb, persisted *state.MithrilState, epoch uint64, ctx *ReplayCtx) (*sealevel.SlotCtx, error) {
	if rs == nil || sysvars == nil || sysvars.Slot() != rs.ParentSlot {
		return nil, fmt.Errorf("missing exact fork parent snapshot")
	}
	fees := reconstructFeeRateGovernor(persisted)
	if fees == nil {
		return nil, fmt.Errorf("missing fork parent fee governor")
	}
	fees.LamportsPerSignature = rs.LamportsPerSignature
	fees.PrevLamportsPerSignature = rs.PrevLamportsPerSignature
	seed := &b.Block{}
	if err := seedEpochStakesForExecution(seed, epoch); err != nil {
		return nil, err
	}
	parent := &sealevel.SlotCtx{
		Accounts: accounts.NewMemAccounts(), AccountsDb: db, UnrootedRead: tail,
		Slot: rs.ParentSlot, Epoch: epoch, Features: ctx.CurrentFeatures,
		FinalBankhash: append([]byte(nil), rs.ParentBankhash...), AcctsLtHash: rs.AcctsLtHash,
		FeeRateGovernor: fees, NumSignatures: rs.NumSignatures,
		Blockhash: rs.LastBlockhash, LastBlockhash: rs.LastBlockhash,
		LatestEvictedBlockhash: rs.EvictedBlockhash,
		VoteAccts:              seed.EpochStakesPerVoteAcct, TotalEpochStake: seed.TotalEpochStake,
		VoteTimestamps: make(map[solana.PublicKey]sealevel.BlockTimestamp),
	}
	for pk, vote := range global.VoteCache() {
		if vote != nil && vote.LastTimestamp() != nil {
			parent.VoteTimestamps[pk] = *vote.LastTimestamp()
		}
	}
	if err := sysvars.RangeAccountViews(func(pk solana.PublicKey, acct *accounts.Account) error {
		key := [32]byte(pk)
		return parent.Accounts.SetAccount(&key, acct.Clone())
	}); err != nil {
		return nil, err
	}
	if err := parent.PublishBankSysvars(sysvars); err != nil {
		return nil, err
	}
	return parent, nil
}
