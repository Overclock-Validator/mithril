package replay

import "github.com/Overclock-Validator/mithril/pkg/rewards"

// rewardPublicationTargets separates already verified reads from recoverable
// disk state. The caller must apply execution verification and exact finality
// before passing verifiedThrough; only disk waits for every reward partition.
func (t *unrootedTail) rewardPublicationTargets(completion *partitionedRewardsCompletion, info *rewards.PartitionedRewardDistributionInfo, boundary, verifiedThrough uint64) (readThrough, durableThrough uint64) {
	return t.executedSlotAtOrBefore(verifiedThrough), completion.limitPromotion(info, boundary, verifiedThrough)
}

// executedSlotAtOrBefore selects a complete retained bank at the verified
// prefix. Skipped slots and unwound banks have no context and cannot advance an
// RPC epoch. The scan is bounded by the existing unrooted-tail halt cap.
func (t *unrootedTail) executedSlotAtOrBefore(through uint64) uint64 {
	var slot uint64
	for held, ctx := range t.contexts {
		if ctx != nil && ctx.Slot == held && held <= through && held > slot {
			slot = held
		}
	}
	return slot
}
