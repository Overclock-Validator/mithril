package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/stretchr/testify/require"
)

func TestRewardPublicationDoesNotWaitForDistribution(t *testing.T) {
	tail := asyncTestTail(nil, 256, 260)
	info := &rewards.PartitionedRewardDistributionInfo{NumRewardPartitionsRemaining: 1}
	var completion partitionedRewardsCompletion
	for _, verified := range []uint64{255, 256, 257, 260} {
		read, durable := tail.rewardPublicationTargets(&completion, info, 256, verified)
		require.Equal(t, tail.executedSlotAtOrBefore(verified), read)
		require.Equal(t, uint64(255), durable, "disk must retain the replayable boundary while rewards are incomplete")
	}
	info.NumRewardPartitionsRemaining = 0
	completion.observeBank(info, testUnwindBankSysvars(t, 260, 100))
	read, durable := tail.rewardPublicationTargets(&completion, info, 256, 260)
	require.Equal(t, uint64(260), read)
	require.Equal(t, uint64(260), durable, "verified completion permits disk promotion")
}

func TestRewardReadSlotUsesSurvivingExecutedBank(t *testing.T) {
	tail := asyncTestTail(nil, 255, 257, 260)
	require.Equal(t, uint64(255), tail.executedSlotAtOrBefore(256), "a finalized skipped epoch boundary is not a new bank")
	require.Equal(t, uint64(257), tail.executedSlotAtOrBefore(259))
	require.Equal(t, uint64(260), tail.executedSlotAtOrBefore(1000))
	parent, _ := tail.unwind(257)
	require.NotNil(t, parent)
	require.Equal(t, uint64(255), parent.Slot)
	require.Equal(t, uint64(255), tail.executedSlotAtOrBefore(1000), "finality ahead of a rewound high-water mark cannot restore discarded context")
	delete(tail.contexts, 255)
	require.Zero(t, tail.executedSlotAtOrBefore(1000), "a retired durable context needs no new in-memory publication")
}
