package rewards

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/require"
)

func TestCalculateNumRewardPartitionsSchedulesEmptyPartition(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000}
	require.Equal(t, uint64(1), CalculateNumRewardPartitions(0, nil, schedule, 0))
	require.Equal(t, uint64(1), CalculateNumRewardPartitions(1, nil, schedule, 0))
	require.Equal(t, uint64(1), CalculateNumRewardPartitions(4096, nil, schedule, 0))
	require.Equal(t, uint64(2), CalculateNumRewardPartitions(4097, nil, schedule, 0))
}

func TestRewardPartitionsFollowSlotTimeFeatureAtNextEpoch(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000}
	for _, tc := range []struct {
		gate   features.FeatureGate
		target uint64
	}{
		{features.ReduceSlotTimeTo350ms, 3584},
		{features.ReduceSlotTimeTo300ms, 3072},
		{features.ReduceSlotTimeTo250ms, 2560},
		{features.ReduceSlotTimeTo200ms, 2048},
	} {
		t.Run(tc.gate.Name, func(t *testing.T) {
			f := features.NewFeaturesDefault()
			f.EnableFeature(tc.gate, 100)
			require.Equal(t, uint64(1), CalculateNumRewardPartitions(tc.target+1, f, schedule, 99))
			require.Equal(t, uint64(1), CalculateNumRewardPartitions(tc.target+1, f, schedule, 100))
			require.Equal(t, uint64(1), CalculateNumRewardPartitions(tc.target+1, f, schedule, 431999))
			require.Equal(t, uint64(1), CalculateNumRewardPartitions(tc.target, f, schedule, 432000))
			require.Equal(t, uint64(2), CalculateNumRewardPartitions(tc.target+1, f, schedule, 432000))
			require.Equal(t, uint64(1), CalculateNumRewardPartitions(0, f, schedule, 432000))
		})
	}
}

func TestRewardPartitionsDoNotRevertToSlowerSlotTime(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.ReduceSlotTimeTo200ms, 50)
	f.EnableFeature(features.ReduceSlotTimeTo350ms, 150)
	require.Equal(t, uint64(2), CalculateNumRewardPartitions(2049, f, schedule, 100))
	require.Equal(t, uint64(2), CalculateNumRewardPartitions(2049, f, schedule, 200))
}

func TestRewardPartitionsMatchTestnetEpoch1048(t *testing.T) {
	// Public testnet's slot 447212256 records 251 partitions for 512,639
	// stake rewards under the effective 200ms slot-time feature. The old
	// unconditional 4096-account budget produced 126 and a different bank hash.
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, Warmup: true,
		FirstNormalEpoch: 14, FirstNormalSlot: 524256}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.ReduceSlotTimeTo200ms, schedule.FirstSlotInEpoch(1046))
	require.Equal(t, uint64(251), CalculateNumRewardPartitions(512639, f, schedule, 447212256))
}

func TestRewardPartitionCapUsesEpochScheduleAndWarmup(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 54000}
	require.Equal(t, uint64(5400), CalculateNumRewardPartitions(math.MaxUint64, nil, schedule, 54000))
	schedule.SlotsPerEpoch = 8
	require.Equal(t, uint64(1), CalculateNumRewardPartitions(math.MaxUint64, nil, schedule, 8))
	schedule = &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, Warmup: true,
		FirstNormalEpoch: 14, FirstNormalSlot: 524256}
	require.Equal(t, uint64(1), CalculateNumRewardPartitions(100000, nil, schedule, 524255))
	require.Equal(t, uint64(25), CalculateNumRewardPartitions(100000, nil, schedule, 524256))
}
