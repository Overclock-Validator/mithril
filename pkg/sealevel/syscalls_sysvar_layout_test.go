package sealevel

import (
	"encoding/binary"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

// Replace the old sysvars.so fixture, which expected packed EpochSchedule
// u64 fields at offsets 17/25. The syscall ABI is repr(C): offsets 24/32.
func TestSyscallSysvarLayouts(t *testing.T) {
	vm, ctx := newMemSyscallVM(t, make([]byte, 512), nil)
	ctx.Accounts = accounts.NewMemAccounts()
	clock := SysvarClock{Slot: 1234, EpochStartTimestamp: 2222, Epoch: 1111, LeaderScheduleEpoch: 100000, UnixTimestamp: 3}
	rent := SysvarRent{LamportsPerUint8Year: 12, ExemptionThreshold: 34, BurnPercent: 56}
	schedule := SysvarEpochSchedule{SlotsPerEpoch: 1111, LeaderScheduleSlotOffset: 2222, Warmup: true, FirstNormalEpoch: 4444, FirstNormalSlot: 5555}
	rewards := SysvarEpochRewards{DistributionStartingBlockHeight: 1234, NumPartitions: 4321, TotalRewards: 5656, DistributedRewards: 6767, Active: true}
	rewards.TotalPoints.Lo = 0x0123456789abcdef
	rewards.TotalPoints.Hi = 0xfedcba9876543210
	rewards.ParentBlockhash[0] = 0x73
	for _, key := range [][32]byte{SysvarClockAddr, SysvarRentAddr, SysvarEpochScheduleAddr, SysvarEpochRewardsAddr, SysvarLastRestartSlotAddr} {
		require.NoError(t, ctx.Accounts.SetAccount(&key, &accounts.Account{Key: key, Lamports: 1}))
	}
	WriteClockSysvar(&ctx.Accounts, clock)
	WriteRentSysvar(&ctx.Accounts, rent)
	WriteEpochScheduleSysvar(&ctx.Accounts, schedule)
	WriteEpochRewardsSysvar(&ctx.Accounts, rewards)
	WriteLastRestartSlotSysvar(&ctx.Accounts, SysvarLastRestartSlot{LastRestartSlot: 989898})
	for _, tc := range []struct {
		name string
		call func(sbpf.VM, uint64) (uint64, error)
		want []byte
	}{
		{"clock", SyscallGetClockSysvarImpl, appendU64s(1234, 2222, 1111, 100000, 3)},
		{"rent", SyscallGetRentSysvarImpl, append(appendU64s(12, math.Float64bits(34)), 56, 0, 0, 0, 0, 0, 0, 0)},
		{"schedule", SyscallGetEpochScheduleSysvarImpl, appendU64s(1111, 2222, 1, 4444, 5555)},
		{"last restart", SyscallGetLastRestartSlotSysvarImpl, appendU64s(989898)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.call(vm, sbpf.VaddrInput)
			require.NoError(t, err)
			got, err := vm.Translate(sbpf.VaddrInput, uint64(len(tc.want)), false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			clear(got)
		})
	}
	_, err := SyscallGetEpochRewardsSysvarImpl(vm, sbpf.VaddrInput)
	require.NoError(t, err)
	got, err := vm.Translate(sbpf.VaddrInput, 96, false)
	require.NoError(t, err)
	want := make([]byte, 96)
	copy(want, appendU64s(1234, 4321))
	copy(want[16:48], rewards.ParentBlockhash[:])
	copy(want[48:], appendU64s(rewards.TotalPoints.Lo, rewards.TotalPoints.Hi, 5656, 6767))
	want[80] = 1
	require.Equal(t, want, got)
}
func appendU64s(values ...uint64) []byte {
	var b []byte
	for _, v := range values {
		b = binary.LittleEndian.AppendUint64(b, v)
	}
	return b
}
