package sealevel

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/stretchr/testify/require"
)

// Port of the access cases in Agave v4.4.1
// program-runtime/src/serialization.rs::test_access_violation_handler. Its
// account regions are built directly; here they come from Mithril's actual
// ABI-v1 serializer so the same checks exercise our mapping and write handler.
func TestAgaveV441DirectMappedAccessViolationHandler(t *testing.T) {
	ctx := serializationTestContext(t, 6)
	ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
	ctx.Features.EnableFeature(features.AccountDataDirectMapping, 0)
	tx := ctx.TransactionContext
	instr, err := tx.CurrentInstructionCtx()
	require.NoError(t, err)

	// Instruction account indexes 0..5 correspond to transaction indexes 1..6.
	// The final duplicate in serializationTestContext does not create a region.
	tx.Accounts.Accounts[1].Data = make([]byte, 4) // readonly
	tx.Accounts.Accounts[2].Data = make([]byte, 4) // writable, initially shared
	tx.Accounts.Accounts[3].Data = nil             // writable, growth target
	tx.Accounts.Accounts[4].Data = make([]byte, MaxPermittedDataLength-0x100)
	tx.Accounts.Accounts[5].Data = nil
	tx.Accounts.Accounts[6].Data = make([]byte, 0x3000)
	instr.InstructionAccounts[0].IsWritable = false
	tx.Accounts.Shared[2] = true
	sharedParent := tx.Accounts.Accounts[2]

	input, _, metas, regions, err := serializeParametersAligned(ctx)
	require.NoError(t, err)
	ctx.ComputeMeter = cu.NewComputeMeter(100_000)
	vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram}, &sbpf.VMOpts{
		Input: input, InputRegions: regions, Context: ctx, ComputeMeter: &ctx.ComputeMeter,
		HeapMax: 32 * 1024, DisableStackFrameGaps: true,
	})
	t.Cleanup(vm.Finish)
	readonly, shared, empty, nearMax := metas[0].vmDataAddr, metas[1].vmDataAddr, metas[2].vmDataAddr, metas[3].vmDataAddr

	t.Run("reads_and_readonly_writes", func(t *testing.T) {
		_, err = vm.Read32(readonly)
		require.NoError(t, err)
		_, err = vm.Read32(shared)
		require.NoError(t, err)
		_, err = vm.Read32(readonly + 4)
		require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, err, regions), InstrErrAccountDataTooSmall)
		require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, vm.Write32(readonly, 1), regions), InstrErrReadonlyDataModified)
		require.Len(t, tx.Accounts.Accounts[1].Data, 4)
		require.False(t, tx.Accounts.Touched[1])
	})

	t.Run("writable_shared_account_detaches_on_first_store", func(t *testing.T) {
		require.True(t, tx.Accounts.Shared[2])
		require.NoError(t, vm.Write32(shared, 0x44332211))
		require.False(t, tx.Accounts.Shared[2])
		require.True(t, tx.Accounts.Touched[2])
		require.NotSame(t, sharedParent, tx.Accounts.Accounts[2])
		require.Equal(t, []byte{0, 0, 0, 0}, sharedParent.Data)
		require.Equal(t, []byte{0x11, 0x22, 0x33, 0x44}, tx.Accounts.Accounts[2].Data)
		require.Zero(t, tx.AccountsResizeDelta)
	})

	t.Run("growth_is_store_triggered_and_zero_filled", func(t *testing.T) {
		_, err = vm.Read32(shared + 4)
		require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, err, regions), InstrErrInvalidRealloc)
		require.Len(t, tx.Accounts.Accounts[2].Data, 4)

		require.NoError(t, vm.Write32(shared+4, 0x88776655))
		require.Len(t, tx.Accounts.Accounts[2].Data, 4+MaxPermittedDataIncrease)
		require.EqualValues(t, MaxPermittedDataIncrease, tx.AccountsResizeDelta)
		require.Equal(t, []byte{0x55, 0x66, 0x77, 0x88}, tx.Accounts.Accounts[2].Data[4:8])
		require.Zero(t, tx.Accounts.Accounts[2].Data[8])
		require.Zero(t, tx.Accounts.Accounts[2].Data[len(tx.Accounts.Accounts[2].Data)-1])
		_, err = vm.Read32(shared + 4)
		require.NoError(t, err)
	})

	t.Run("account_maximum_truncates_reserved_growth", func(t *testing.T) {
		require.NoError(t, vm.Write32(nearMax+MaxPermittedDataLength-4, 0))
		require.Len(t, tx.Accounts.Accounts[4].Data, MaxPermittedDataLength)
		require.EqualValues(t, MaxPermittedDataIncrease+0x100, tx.AccountsResizeDelta)
		_, err = vm.Read32(nearMax + MaxPermittedDataLength)
		require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, err, regions), InstrErrInvalidRealloc)
		require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, vm.Write32(nearMax+MaxPermittedDataLength, 0), regions), InstrErrInvalidRealloc)
		require.Len(t, tx.Accounts.Accounts[4].Data, MaxPermittedDataLength)
	})

	t.Run("transaction_budget_truncates_reserved_growth", func(t *testing.T) {
		// Burn the same budget as Agave: 10 MiB and 10 MiB minus 0x3000.
		for _, instructionIndex := range []uint64{4, 5} {
			acct, borrowErr := instr.BorrowInstructionAccount(tx, instructionIndex)
			require.NoError(t, borrowErr)
			require.NoError(t, acct.SetDataLength(MaxPermittedDataLength, ctx.Features))
			acct.Drop()
		}
		require.EqualValues(t, maxAccountDataGrowthPerTransaction-0x700, tx.AccountsResizeDelta)
		require.NoError(t, vm.Write32(empty+0x500, 0x11223344))
		require.Len(t, tx.Accounts.Accounts[3].Data, 0x700)
		require.EqualValues(t, maxAccountDataGrowthPerTransaction, tx.AccountsResizeDelta)
		require.Zero(t, tx.Accounts.Accounts[3].Data[0])
		require.Equal(t, []byte{0x44, 0x33, 0x22, 0x11}, tx.Accounts.Accounts[3].Data[0x500:0x504])
		require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, vm.Write32(empty+0x700, 0), regions), InstrErrInvalidRealloc)
	})
}

// The same address-space rules also apply when the VM input owns a copy of
// account data. A store within the old length changes that copy; a growth
// store changes the runtime account immediately and charges the transaction.
func TestAgaveV441CopiedAccountAccessAndGrowth(t *testing.T) {
	ctx := serializationTestContext(t, 6)
	ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
	require.False(t, ctx.Features.IsActive(features.AccountDataDirectMapping))
	tx := ctx.TransactionContext
	instr, err := tx.CurrentInstructionCtx()
	require.NoError(t, err)

	tx.Accounts.Accounts[1].Data = make([]byte, 4)
	tx.Accounts.Accounts[2].Data = make([]byte, 4)
	tx.Accounts.Accounts[3].Data = nil
	tx.Accounts.Accounts[4].Data = make([]byte, MaxPermittedDataLength-0x100)
	tx.Accounts.Accounts[5].Data = nil
	tx.Accounts.Accounts[6].Data = make([]byte, 0x3000)
	instr.InstructionAccounts[0].IsWritable = false
	tx.Accounts.Shared[2] = true
	sharedParent := tx.Accounts.Accounts[2]

	vm, metas, regions, _ := copiedVasaTestVM(t, ctx)
	readonly, shared, empty, nearMax := metas[0].vmDataAddr, metas[1].vmDataAddr, metas[2].vmDataAddr, metas[3].vmDataAddr

	t.Run("permission_and_read_boundaries", func(t *testing.T) {
		_, err := vm.Read32(readonly)
		require.NoError(t, err)
		_, err = vm.Read32(shared)
		require.NoError(t, err)
		_, err = vm.Read32(readonly + 4)
		require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, err, regions), InstrErrAccountDataTooSmall)
		require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, vm.Write32(readonly, 1), regions), InstrErrReadonlyDataModified)
		require.False(t, tx.Accounts.Touched[1])
	})

	t.Run("growth_requires_a_store", func(t *testing.T) {
		require.NoError(t, vm.Write32(shared, 0x44332211))
		value, err := vm.Read32(shared)
		require.NoError(t, err)
		require.Equal(t, uint32(0x44332211), value)
		require.True(t, tx.Accounts.Shared[2], "a store within the copied input does not yet touch the runtime account")
		require.Equal(t, []byte{0, 0, 0, 0}, sharedParent.Data)
		_, err = vm.Read32(shared + 4)
		require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, err, regions), InstrErrInvalidRealloc)
		require.Len(t, tx.Accounts.Accounts[2].Data, 4)
		require.NoError(t, vm.Write32(shared+4, 0x88776655))
		require.False(t, tx.Accounts.Shared[2])
		require.NotSame(t, sharedParent, tx.Accounts.Accounts[2])
		require.Equal(t, []byte{0, 0, 0, 0}, sharedParent.Data)
		require.Len(t, tx.Accounts.Accounts[2].Data, 4+MaxPermittedDataIncrease)
		require.EqualValues(t, MaxPermittedDataIncrease, tx.AccountsResizeDelta)
		value, err = vm.Read32(shared + 4)
		require.NoError(t, err)
		require.Equal(t, uint32(0x88776655), value)
	})

	t.Run("account_maximum", func(t *testing.T) {
		require.NoError(t, vm.Write32(nearMax+MaxPermittedDataLength-4, 0))
		require.Len(t, tx.Accounts.Accounts[4].Data, MaxPermittedDataLength)
		require.EqualValues(t, MaxPermittedDataIncrease+0x100, tx.AccountsResizeDelta)
		_, err := vm.Read32(nearMax + MaxPermittedDataLength)
		require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, err, regions), InstrErrInvalidRealloc)
		require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, vm.Write32(nearMax+MaxPermittedDataLength, 0), regions), InstrErrInvalidRealloc)
	})

	t.Run("transaction_budget", func(t *testing.T) {
		for _, instructionIndex := range []uint64{4, 5} {
			acct, borrowErr := instr.BorrowInstructionAccount(tx, instructionIndex)
			require.NoError(t, borrowErr)
			require.NoError(t, acct.SetDataLength(MaxPermittedDataLength, ctx.Features))
			acct.Drop()
		}
		require.EqualValues(t, maxAccountDataGrowthPerTransaction-0x700, tx.AccountsResizeDelta)
		require.NoError(t, vm.Write32(empty+0x500, 0x11223344))
		require.Len(t, tx.Accounts.Accounts[3].Data, 0x700)
		require.EqualValues(t, maxAccountDataGrowthPerTransaction, tx.AccountsResizeDelta)
		value, err := vm.Read32(empty + 0x500)
		require.NoError(t, err)
		require.Equal(t, uint32(0x11223344), value)
		require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, vm.Write32(empty+0x700, 0), regions), InstrErrInvalidRealloc)
	})
}

// Agave's access handler first enlarges an account to the available cap and
// then lets the VM reject a write that still extends beyond it. This matters
// for the handler's state even though the failed instruction cannot commit.
func TestAgaveV441FailedWriteClampsRegionBeforeFault(t *testing.T) {
	for _, direct := range []bool{false, true} {
		name := "copied"
		if direct {
			name = "direct"
		}
		t.Run(name, func(t *testing.T) {
			ctx := serializationTestContext(t, 1)
			ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
			if direct {
				ctx.Features.EnableFeature(features.AccountDataDirectMapping, 0)
			}
			ctx.TransactionContext.Accounts.Accounts[1].Data = make([]byte, MaxPermittedDataLength-1)
			input, _, metas, regions, err := serializeParametersAligned(ctx)
			require.NoError(t, err)
			ctx.ComputeMeter = cu.NewComputeMeter(100_000)
			vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram}, &sbpf.VMOpts{
				Input: input, InputRegions: regions, Context: ctx, ComputeMeter: &ctx.ComputeMeter,
				HeapMax: 32 * 1024, DisableStackFrameGaps: true,
			})
			defer vm.Finish()
			dataAddr := metas[0].vmDataAddr
			require.ErrorIs(t, vm.Write8(dataAddr+MaxPermittedDataLength, 0xff), InstrErrInvalidRealloc)
			require.Len(t, ctx.TransactionContext.Accounts.Accounts[1].Data, MaxPermittedDataLength)
			require.EqualValues(t, 1, ctx.TransactionContext.AccountsResizeDelta)
			require.EqualValues(t, MaxPermittedDataLength, regions[1].RegionSize)
		})
	}
}
