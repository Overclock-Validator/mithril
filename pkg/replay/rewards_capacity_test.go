package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/require"
)

// Mainnet epoch 1071 needed 527 partition banks after its boundary bank.
// The old 512-bank cap halted before completion, although finality and the
// byte budget were healthy. Exercise retention, the atomic fold and retirement
// with skipped slot numbers, rather than merely checking the new limit value.
func TestMainnetRewardsWindowCanCompleteBeyondNormalCapacity(t *testing.T) {
	const boundary, partitions = uint64(462672000), uint64(527)
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := newUnrootedTail(&fakeDurable{}, fc, unrootedTailHaltCap, 128, "")
	info := &rewards.PartitionedRewardDistributionInfo{NumRewardPartitionsRemaining: partitions}
	var completion partitionedRewardsCompletion
	tail.reserveRewardWindow(partitions)
	var last uint64
	for bank := uint64(0); bank <= partitions; bank++ {
		last = boundary + bank + bank/10 // skipped slots don't create banks
		tail.Add(last, []*accounts.Account{testAccount(1, bank)}, testHashBytes(byte(bank)))
		tail.SetContext(last, &state.ResumeContext{Slot: last})
		if bank > 0 {
			info.NumRewardPartitionsRemaining--
		}
		require.False(t, tail.OverCap(), "distribution halted at bank %d", bank)
		require.Equal(t, boundary-1, completion.limitPromotion(info, boundary, last), "no partial reward window may become durable")
	}
	require.Equal(t, 528, tail.overlay.HeldSlots())
	completion.observeBank(info, testUnwindBankSysvars(t, last, 50))
	require.Equal(t, boundary-1, completion.limitPromotion(info, boundary, last-1), "completion must wait for finality")
	require.Equal(t, last, completion.limitPromotion(info, boundary, last))
	job, err := tail.buildRewardsCompletionFoldJob(last)
	require.NoError(t, err)
	require.Equal(t, 528, job.slotCount)
	require.NoError(t, runFoldJob(fc, job))
	require.False(t, completion.retire(&info, boundary-1), "worker completion alone is not applied durability")
	tail.applyFoldJob(job)
	require.True(t, completion.retire(&info, last))
	tail.reserveRewardWindow(0)
	require.Equal(t, []uint64{last}, fc.throughs, "all rewards must share one durable commit")
	require.Zero(t, tail.overlay.HeldSlots())
	require.Equal(t, uint64(unrootedTailHaltCap), tail.slotLimit())
	for bank := uint64(1); bank <= unrootedTailHaltCap+1; bank++ {
		tail.Add(last+bank, nil, testHashBytes(0))
	}
	require.True(t, tail.OverCap(), "ordinary stalled finality must still halt after retirement")
}

func TestRewardsWindowStillEnforcesMemoryAndFinalityLimits(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		tail := newUnrootedTail(&fakeDurable{}, nil, unrootedTailHaltCap, 128, "")
		tail.reserveRewardWindow(527)
		tail.maxBytes = 4096
		addMemorySlot(tail, 1, 4096)
		require.True(t, tail.OverCap(), "reward reservation must not override the byte budget")
	})
	t.Run("finality never reaches completion", func(t *testing.T) {
		tail := newUnrootedTail(&fakeDurable{}, nil, unrootedTailHaltCap, 128, "")
		tail.reserveRewardWindow(527)
		limit := tail.slotLimit()
		for bank := uint64(1); bank <= limit; bank++ {
			tail.Add(bank, nil, testHashBytes(0))
		}
		require.False(t, tail.OverCap())
		tail.Add(limit+1, nil, testHashBytes(0))
		require.True(t, tail.OverCap(), "a reward hold must not allow unbounded stalled replay")
	})
	t.Run("overflow", func(t *testing.T) {
		tail := newUnrootedTail(&fakeDurable{}, nil, unrootedTailHaltCap, 128, "")
		tail.reserveRewardWindow(^uint64(0))
		require.Equal(t, ^uint64(0), tail.slotLimit(), "overflow must not wrap to a disabled or small cap")
		tail.maxBytes = 4096
		addMemorySlot(tail, 1, 4096)
		require.True(t, tail.OverCap())
	})
}
