package sealevel

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func copiedVasaTestVM(t *testing.T, ctx *ExecutionCtx) (*sbpf.Interpreter, []serializedAcctMetadata, []sbpf.InputRegion, []byte) {
	t.Helper()
	ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
	require.False(t, ctx.Features.IsActive(features.AccountDataDirectMapping))
	input, _, metas, regions, err := serializeParametersAligned(ctx)
	require.NoError(t, err)
	ctx.ComputeMeter = cu.NewComputeMeter(100_000)
	vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram}, &sbpf.VMOpts{
		Input: input, InputRegions: regions, Context: ctx, ComputeMeter: &ctx.ComputeMeter,
		HeapMax: 32 * 1024, DisableStackFrameGaps: true,
	})
	t.Cleanup(vm.Finish)
	return vm, metas, regions, input
}

func TestVasaCopiedGrowthRejectsAccountAndTransactionLimits(t *testing.T) {
	for _, tc := range []struct {
		name        string
		accountLen  int
		resizeDelta int64
	}{
		{"maximum_account_length", MaxPermittedDataLength, 0},
		{"exhausted_transaction_budget", 5, maxAccountDataGrowthPerTransaction},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := serializationTestContext(t, 1)
			ctx.TransactionContext.Accounts.Accounts[1].Data = make([]byte, tc.accountLen)
			ctx.TransactionContext.AccountsResizeDelta = tc.resizeDelta
			vm, metas, regions, _ := copiedVasaTestVM(t, ctx)
			err := vm.Write8(metas[0].vmDataAddr+uint64(tc.accountLen), 0xaa)
			require.ErrorIs(t, err, InstrErrInvalidRealloc)
			require.Len(t, ctx.TransactionContext.Accounts.Accounts[1].Data, tc.accountLen)
			require.Equal(t, tc.resizeDelta, ctx.TransactionContext.AccountsResizeDelta)
			require.Equal(t, uint64(tc.accountLen), regions[1].RegionSize)
		})
	}
}

func TestVasaCopiedGrowthChargesAndReleasesBudget(t *testing.T) {
	ctx := serializationTestContext(t, 1)
	vm, metas, regions, input := copiedVasaTestVM(t, ctx)
	tx := ctx.TransactionContext
	dataAddr := metas[0].vmDataAddr
	require.NoError(t, vm.Write8(dataAddr+5, 0xaa))
	require.Len(t, tx.Accounts.Accounts[1].Data, 5+MaxPermittedDataIncrease)
	require.EqualValues(t, MaxPermittedDataIncrease, tx.AccountsResizeDelta)
	require.Equal(t, uint64(5+MaxPermittedDataIncrease), regions[1].RegionSize)
	require.NoError(t, deserializeParametersAligned(ctx, input, metas))
	require.Len(t, tx.Accounts.Accounts[1].Data, 5)
	require.Zero(t, tx.AccountsResizeDelta)
}

func TestVasaCopiedLengthMetadataRespectsTransactionBudget(t *testing.T) {
	ctx := serializationTestContext(t, 1)
	ctx.TransactionContext.AccountsResizeDelta = maxAccountDataGrowthPerTransaction
	vm, metas, _, input := copiedVasaTestVM(t, ctx)
	// A program can enlarge AccountInfo.data_len without touching the growth
	// area. Deserialization must still apply the transaction growth cap.
	require.NoError(t, vm.Write64(metas[0].vmDataAddr-8, 6))
	require.ErrorIs(t, deserializeParametersAligned(ctx, input, metas), InstrErrMaxAccountsDataAllocationsExceeded)
	require.Len(t, ctx.TransactionContext.Accounts.Accounts[1].Data, 5)
	require.EqualValues(t, maxAccountDataGrowthPerTransaction, ctx.TransactionContext.AccountsResizeDelta)
}

func TestVasaCopiedGrowthClampsPartialTransactionBudget(t *testing.T) {
	ctx := serializationTestContext(t, 1)
	ctx.TransactionContext.AccountsResizeDelta = maxAccountDataGrowthPerTransaction - 2
	vm, metas, regions, _ := copiedVasaTestVM(t, ctx)
	dataAddr := metas[0].vmDataAddr
	require.NoError(t, vm.Write8(dataAddr+5, 0xaa))
	require.Equal(t, uint64(7), regions[1].RegionSize)
	require.Len(t, ctx.TransactionContext.Accounts.Accounts[1].Data, 7)
	require.EqualValues(t, maxAccountDataGrowthPerTransaction, ctx.TransactionContext.AccountsResizeDelta)
	require.ErrorIs(t, vm.Write8(dataAddr+7, 0xbb), InstrErrInvalidRealloc)
}

func TestVasaCopiedMemcopyDestinationGrowthMakesSourceReadable(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(sbpf.VM, uint64, uint64, uint64) (uint64, error)
	}{
		{"memmove", SyscallMemmoveImpl},
		{"memcpy", SyscallMemcpyImpl},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := serializationTestContext(t, 1)
			vm, metas, _, input := copiedVasaTestVM(t, ctx)
			dataAddr := metas[0].vmDataAddr
			_, err := vm.Read8(dataAddr + 5)
			require.Error(t, err)
			ret, err := tc.call(vm, dataAddr+6, dataAddr+5, 1)
			require.NoError(t, err)
			require.Zero(t, ret)
			require.Zero(t, input[6+dataAddr-sbpf.VaddrInput])
			require.Len(t, ctx.TransactionContext.Accounts.Accounts[1].Data, 5+MaxPermittedDataIncrease)
		})
	}
}

func TestVasaCopiedGrowthPermissionRefreshAfterCPI(t *testing.T) {
	ctx := serializationTestContext(t, 1)
	tx := ctx.TransactionContext
	programKey := tx.Accounts.Accounts[0].Key
	account := tx.Accounts.Accounts[1]
	account.Owner = solana.PublicKey{0xee}
	vm, metas, _, _ := copiedVasaTestVM(t, ctx)
	dataAddr := metas[0].vmDataAddr
	require.Error(t, vm.Write8(dataAddr+5, 0xaa))

	// Model a CPI that gives this account to the current program and grows it.
	account.Owner = programKey
	account.Data = make([]byte, 61)
	tx.AccountsResizeDelta = 56
	instrCtx, err := tx.CurrentInstructionCtx()
	require.NoError(t, err)
	borrowed, err := instrCtx.BorrowInstructionAccount(tx, 0)
	require.NoError(t, err)
	require.NoError(t, updateCallerAccountRegion(vm, ctx, &CallerAccount{
		OriginalDataLen: 5, VmDataAddr: dataAddr,
	}, borrowed, false))
	borrowed.Drop()
	require.NoError(t, vm.Write8(dataAddr+61, 0xbb))
	require.Len(t, tx.Accounts.Accounts[1].Data, 5+MaxPermittedDataIncrease)

	// A later CPI can revoke ownership; the previous write permission must go.
	account.Owner = solana.PublicKey{0xee}
	borrowed, err = instrCtx.BorrowInstructionAccount(tx, 0)
	require.NoError(t, err)
	require.NoError(t, updateCallerAccountRegion(vm, ctx, &CallerAccount{
		OriginalDataLen: 5, VmDataAddr: dataAddr,
	}, borrowed, false))
	borrowed.Drop()
	require.Error(t, vm.Write8(dataAddr, 0xcc))
}

func TestVasaCopiedProgramRejectsStorePastMaximumAccountLength(t *testing.T) {
	ctx := serializationTestContext(t, 1)
	ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
	account := ctx.TransactionContext.Accounts.Accounts[1]
	account.Data = make([]byte, MaxPermittedDataLength)
	ctx.TransactionContext.ComputeBudgetLimits = &ComputeBudgetLimits{UpdatedHeapBytes: 32 * 1024}
	ctx.ComputeMeter = cu.NewComputeMeter(100_000)
	encode := func(op, dst, src uint8, off int16, imm uint32) sbpf.Slot {
		return sbpf.Slot(op) | sbpf.Slot(dst)<<8 | sbpf.Slot(src)<<12 | sbpf.Slot(uint16(off))<<16 | sbpf.Slot(imm)<<32
	}
	text := []sbpf.Slot{
		encode(sbpf.OpStb, 1, 0, 96, 0xbb), // First byte of the account's data.
		encode(sbpf.OpMov64Reg, 2, 1, 0, 0),
		encode(sbpf.OpAdd64Imm, 2, 0, 0, 96+MaxPermittedDataLength),
		encode(sbpf.OpStb, 2, 0, 0, 0xaa), // First byte beyond the 10 MiB limit.
		encode(sbpf.OpMov64Imm, 0, 0, 0, 0),
		encode(sbpf.OpExit, 0, 0, 0, 0),
	}
	program := &sbpf.Program{Text: text, TextVA: sbpf.VaddrProgram, TextBytes: make([]byte, len(text)*8)}
	for i, slot := range text {
		binary.LittleEndian.PutUint64(program.TextBytes[i*8:], uint64(slot))
	}
	require.NoError(t, program.Verify())
	require.ErrorIs(t, executeLoadedProgram(ctx, program, executionSyscalls(ctx)), InstrErrInvalidRealloc)
	require.Zero(t, account.Data[0], "the instruction must not commit its earlier store")
}

func TestVasaCopiedProgramReportsTransactionGrowthLimit(t *testing.T) {
	ctx := serializationTestContext(t, 1)
	ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
	ctx.TransactionContext.AccountsResizeDelta = maxAccountDataGrowthPerTransaction
	ctx.TransactionContext.ComputeBudgetLimits = &ComputeBudgetLimits{UpdatedHeapBytes: 32 * 1024}
	ctx.ComputeMeter = cu.NewComputeMeter(100_000)
	encode := func(op, dst uint8, off int16, imm uint32) sbpf.Slot {
		return sbpf.Slot(op) | sbpf.Slot(dst)<<8 | sbpf.Slot(uint16(off))<<16 | sbpf.Slot(imm)<<32
	}
	text := []sbpf.Slot{
		encode(sbpf.OpStdw, 1, 88, 6), // AccountInfo.data_len, old length 5.
		encode(sbpf.OpMov64Imm, 0, 0, 0),
		encode(sbpf.OpExit, 0, 0, 0),
	}
	program := &sbpf.Program{Text: text, TextVA: sbpf.VaddrProgram, TextBytes: make([]byte, len(text)*8)}
	for i, slot := range text {
		binary.LittleEndian.PutUint64(program.TextBytes[i*8:], uint64(slot))
	}
	require.NoError(t, program.Verify())
	require.ErrorIs(t, executeLoadedProgram(ctx, program, executionSyscalls(ctx)), InstrErrMaxAccountsDataAllocationsExceeded)
	require.Len(t, ctx.TransactionContext.Accounts.Accounts[1].Data, 5)
}
