package sealevel

import (
	"encoding/binary"
	"math"
	"testing"

	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/stretchr/testify/require"
)

// Agave's serialization regression asserts that the initial mapped region
// ends at the account's current data length. The 10 KiB resize affordance is
// reserved virtual address space, not readable account data.
func TestAgaveVasaInitialAccountRegionExcludesResizeAffordance(t *testing.T) {
	for _, directMapping := range []bool{false, true} {
		name := "copied"
		if directMapping {
			name = "direct"
		}
		t.Run(name, func(t *testing.T) {
			ctx := serializationTestContext(t, 1)
			ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
			if directMapping {
				ctx.Features.EnableFeature(features.AccountDataDirectMapping, 0)
			}
			ctx.TransactionContext.Accounts.Accounts[1].Owner = a.BpfLoaderUpgradeableAddr
			ctx.TransactionContext.Accounts.Accounts[1].Data = []byte("agave")

			input, _, metas, regions, err := serializeParametersAligned(ctx)
			require.NoError(t, err)
			require.Len(t, metas, 2) // The duplicate points at the same account.
			require.GreaterOrEqual(t, len(regions), 3)
			dataRegion := regions[1]
			require.Equal(t, metas[0].vmDataAddr, sbpf.VaddrInput+dataRegion.Offset)
			require.EqualValues(t, 5, dataRegion.RegionSize)
			require.EqualValues(t, 5+MaxPermittedDataIncrease, dataRegion.AddressSpaceReserved)
			require.False(t, dataRegion.Writable)

			meter := cu.NewComputeMeter(1_000)
			vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram}, &sbpf.VMOpts{
				Input: input, InputRegions: regions, HeapMax: 32 * 1024,
				ComputeMeter: &meter, DisableStackFrameGaps: true,
			})
			defer vm.Finish()
			dataAddr := metas[0].vmDataAddr
			lengthBytes, err := vm.Translate(dataAddr-8, 8, false)
			require.NoError(t, err)
			require.EqualValues(t, 5, binary.LittleEndian.Uint64(lengthBytes))
			data, err := vm.Translate(dataAddr, 5, false)
			require.NoError(t, err)
			require.Equal(t, []byte("agave"), data)
			_, err = vm.Translate(dataAddr+5, 1, false)
			require.Error(t, err)
			_, err = vm.Translate(dataAddr+5, 1, true)
			require.Error(t, err)
			_, err = vm.Translate(dataAddr-1, 2, false)
			require.Error(t, err, "one access cannot span metadata and account data")
			_, err = vm.Translate(dataAddr-1, 2, true)
			require.Error(t, err, "one write cannot span metadata and account data")
		})
	}
}

func TestAgaveVasaAccountRegionCannotSpanInstructionTail(t *testing.T) {
	ctx := serializationTestContext(t, 1)
	ctx.TransactionContext.Accounts.Accounts[1].Data = make([]byte, 8)
	vm, metas, regions, _ := copiedVasaTestVM(t, ctx)
	dataAddr := metas[0].vmDataAddr
	reserved := regions[1].AddressSpaceReserved

	// Make the last reserved byte readable. Its immediate neighbor is mapped
	// metadata, but Agave rejects a single access across the region boundary.
	require.NoError(t, vm.Write8(dataAddr+reserved-1, 0x7f))
	last, err := vm.Read8(dataAddr + reserved - 1)
	require.NoError(t, err)
	require.Equal(t, byte(0x7f), last)
	_, err = vm.Read8(dataAddr + reserved)
	require.NoError(t, err, "the following rent-epoch metadata is separately readable")
	_, err = vm.Translate(dataAddr+reserved-1, 2, false)
	require.Error(t, err)
	_, err = vm.Translate(dataAddr+reserved-1, 2, true)
	require.Error(t, err)
}

// Agave's mem_ops::test_is_nonoverlapping includes adjacency, partial
// overlap, and address arithmetic near the integer limit. These decisions
// control whether memcpy accepts an account-data address pair.
func TestAgaveMemOpsIsNonoverlapping(t *testing.T) {
	for dst := uint64(0); dst < 8; dst++ {
		require.True(t, isNonOverlapping(10, 3, dst, 3), "dst=%d", dst)
	}
	for dst := uint64(8); dst < 13; dst++ {
		require.False(t, isNonOverlapping(10, 3, dst, 3), "dst=%d", dst)
	}
	for dst := uint64(13); dst < 20; dst++ {
		require.True(t, isNonOverlapping(10, 3, dst, 3), "dst=%d", dst)
	}
	require.True(t, isNonOverlapping(255, 3, 254, 1))
	require.False(t, isNonOverlapping(255, 2, 254, 3))
	require.True(t, isNonOverlapping(math.MaxUint64, 3, math.MaxUint64-1, 1))
	require.False(t, isNonOverlapping(math.MaxUint64, 2, math.MaxUint64-1, 3))
}

// Agave v4.4.1 checks the account length and transaction growth budget before
// rejecting mutation of a readonly or externally owned account. The latter
// errors depend on write permission, with readonly taking precedence.
func TestAgaveAccountResizeErrorPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name          string
		newLen        uint64
		readonly      bool
		growthAtLimit bool
		want          error
	}{
		{name: "external-growth", newLen: 6, want: InstrErrExternalAccountDataModified},
		{name: "external-noop", newLen: 5, want: InstrErrExternalAccountDataModified},
		{name: "readonly-external", newLen: 6, readonly: true, want: InstrErrReadonlyDataModified},
		{name: "oversized-external", newLen: MaxPermittedDataLength + 1, want: InstrErrInvalidRealloc},
		{name: "growth-limit-external", newLen: 6, growthAtLimit: true, want: InstrErrMaxAccountsDataAllocationsExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := serializationTestContext(t, 1)
			ctx.TransactionContext.Accounts.Accounts[1].Owner = [32]byte{0xee}
			if tc.readonly {
				ix, err := ctx.TransactionContext.CurrentInstructionCtx()
				require.NoError(t, err)
				ix.InstructionAccounts[0].IsWritable = false
			}
			if tc.growthAtLimit {
				ctx.TransactionContext.AccountsResizeDelta = maxAccountDataGrowthPerTransaction
			}
			ix, err := ctx.TransactionContext.CurrentInstructionCtx()
			require.NoError(t, err)
			acct, err := ix.BorrowInstructionAccount(ctx.TransactionContext, 0)
			require.NoError(t, err)
			defer ctx.TransactionContext.Accounts.Unlock(1)
			require.ErrorIs(t, acct.CanDataBeResized(tc.newLen, ctx.Features), tc.want)
			require.ErrorIs(t, acct.SetDataLength(tc.newLen, ctx.Features), tc.want)
		})
	}
}
