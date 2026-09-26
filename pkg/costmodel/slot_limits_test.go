package costmodel

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/require"
)

func TestSlotLimitsMatchAgaveTable(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}
	for _, tc := range []struct {
		name                                  string
		gate                                  features.FeatureGate
		account, block, data, shreds, entries uint64
	}{
		{"400ms", features.FeatureGate{}, 24_000_000, 60_000_000, 100_000_000, 32768, 20 * 1024 * 1024},
		{"350ms", features.ReduceSlotTimeTo350ms, 21_000_000, 52_500_000, 87_500_000, 28672, 18_350_080},
		{"300ms", features.ReduceSlotTimeTo300ms, 18_000_000, 45_000_000, 75_000_000, 24576, 15_728_640},
		{"250ms", features.ReduceSlotTimeTo250ms, 15_000_000, 37_500_000, 62_500_000, 20480, 13_107_200},
		{"200ms", features.ReduceSlotTimeTo200ms, 12_000_000, 30_000_000, 50_000_000, 16384, 10_485_760},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := features.NewFeaturesDefault()
			if tc.name != "400ms" {
				f.EnableFeature(tc.gate, 50)
			}
			before, err := LimitsForSlot(f, schedule, 99)
			require.NoError(t, err)
			require.Equal(t, DefaultLimits(), before)
			for _, raise := range []bool{false, true} {
				if raise {
					f.EnableFeature(features.RaiseBlockLimitsTo100m, 0)
				}
				got, err := LimitsForSlot(f, schedule, 100)
				require.NoError(t, err)
				account, block := tc.account, tc.block
				if raise {
					account = account * 100 / 60
					block = block * 100 / 60
				}
				require.Equal(t, account, got.WritableAccountCost)
				require.Equal(t, block, got.BlockCost)
				require.Equal(t, tc.data, got.AllocatedDataSizeDelta)
				require.Equal(t, tc.entries-EntryHeaderBytes, got.MaxEntryBytes)
				require.LessOrEqual(t, got.MaxEntryBytes, PackEntryBytesMax(tc.shreds, MaxMicroblockBytes))
				require.Equal(t, uint64(DefaultTargetBatchBytes), got.MaxBatchBytes)
			}
		})
	}
}

func TestSlotLimitsDoNotLengthenSlotsForLaterGates(t *testing.T) {
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.ReduceSlotTimeTo200ms, 50)
	f.EnableFeature(features.ReduceSlotTimeTo350ms, 150)
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}
	for _, slot := range []uint64{100, 199, 200, 400} {
		limits, err := LimitsForSlot(f, schedule, slot)
		require.NoError(t, err)
		require.Equal(t, uint64(30_000_000), limits.BlockCost)
	}
}

func TestSlotLimitsActivationDuringWarmup(t *testing.T) {
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.ReduceSlotTimeTo200ms, 40)
	// Epoch 0: [0,32), epoch 1: [32,96), normal epoch 2: [96,224).
	schedule := &sealevel.SysvarEpochSchedule{Warmup: true, SlotsPerEpoch: 128, FirstNormalEpoch: 2, FirstNormalSlot: 96}
	before, err := LimitsForSlot(f, schedule, 95)
	require.NoError(t, err)
	require.Equal(t, uint64(60_000_000), before.BlockCost)
	after, err := LimitsForSlot(f, schedule, 96)
	require.NoError(t, err)
	require.Equal(t, uint64(30_000_000), after.BlockCost)
}

func TestSlotLimitsRequireScheduleForActiveReductions(t *testing.T) {
	f := features.NewFeaturesDefault()
	_, err := LimitsForSlot(f, nil, 10)
	require.NoError(t, err)
	f.EnableFeature(features.ReduceSlotTimeTo200ms, 0)
	_, err = LimitsForSlot(f, nil, 10)
	require.Error(t, err)
	_, err = LimitsForSlot(f, &sealevel.SysvarEpochSchedule{}, 10)
	require.Error(t, err)
}
