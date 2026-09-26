package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

// partitionedRewardsCompletion is replay-thread-owned, process-local evidence
// that a successfully executed bank contains all effects of this distribution.
// It is not a checkpoint or signing authority. Until that bank is durable,
// tryInLoopUnwind must still reject even a zero-remaining distribution: its
// spool has been consumed and cannot be rolled back with the account overlay.
type partitionedRewardsCompletion struct {
	info *rewards.PartitionedRewardDistributionInfo
	slot uint64
}

// limitPromotion keeps the boundary replayable until a successfully verified
// completion bank is eligible for promotion. Consuming the last spool changes
// the RAM counter before footer verification and is not completion evidence.
func (c *partitionedRewardsCompletion) limitPromotion(info *rewards.PartitionedRewardDistributionInfo, boundary, through uint64) uint64 {
	if boundary == 0 || info == nil {
		return through
	}
	if info.NumRewardPartitionsRemaining != 0 || c.info != info || c.slot == 0 || through < c.slot {
		return min(through, boundary-1)
	}
	return through
}

// observeBank must run only after successful block execution/publication, using
// that bank's immutable sysvars (never the speculative global sysvar cache).
// If the first completed bank lacks evidence, recording a later descendant is
// conservative: retirement then waits for that later bank to become durable.
func (c *partitionedRewardsCompletion) observeBank(info *rewards.PartitionedRewardDistributionInfo, bank *sealevel.BankSysvars) {
	if c.info != info {
		*c = partitionedRewardsCompletion{info: info}
	}
	if info == nil || c.slot != 0 || info.NumRewardPartitionsRemaining != 0 || bank == nil || bank.Slot() == 0 {
		return
	}
	epochRewards, ok := bank.EpochRewards()
	if ok && !epochRewards.Active {
		c.slot = bank.Slot()
	}
}

// retire is called only when replay applies a successfully committed fold and
// advances LastRootedSlot. Finality, an enqueued/in-flight fold, and a failed
// commit do not acknowledge durability. At this boundary every rewards effect
// is in AccountsDB; in-memory switches above it cannot undo distribution.
// Switches at/below it still take durable recovery, whose persisted
// EpochRewards validation remains unchanged. Restart loses this optional
// evidence and reconstructs state through the existing recovery path.
func (c *partitionedRewardsCompletion) retire(info **rewards.PartitionedRewardDistributionInfo, durableSlot uint64) bool {
	if *info == nil || *info != c.info || c.slot == 0 || durableSlot < c.slot || (*info).NumRewardPartitionsRemaining != 0 {
		return false
	}
	*info = nil
	*c = partitionedRewardsCompletion{}
	return true
}
