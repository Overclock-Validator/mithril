package sealevel

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	feat "github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestSyscallParameterAddressRangeRestricted(t *testing.T) {
	features := feat.NewFeaturesDefault()
	execCtx := &ExecutionCtx{Features: *features}

	require.False(t, syscallParameterAddressRangeRestricted(execCtx, sbpf.VaddrInput-1, 1))

	features.EnableFeature(feat.SyscallParameterAddressRestrictions, 0)
	require.True(t, syscallParameterAddressRangeRestricted(execCtx, sbpf.VaddrInput, 0))
	require.True(t, syscallParameterAddressRangeRestricted(execCtx, sbpf.VaddrInput-1, 1))
	require.False(t, syscallParameterAddressRangeRestricted(execCtx, sbpf.VaddrInput-2, 1))
}

func TestCheckCpiDataLengthSyscallParameterAddressRestrictions(t *testing.T) {
	features := feat.NewFeaturesDefault()
	execCtx := &ExecutionCtx{Features: *features}

	require.NoError(t, checkCpiDataLength(execCtx, MaxPermittedDataIncrease+2, 1, true))

	features.EnableFeature(feat.SyscallParameterAddressRestrictions, 0)
	require.NoError(t, checkCpiDataLength(execCtx, 1, 1, true))
	require.ErrorIs(t, checkCpiDataLength(execCtx, 2, 1, true), InstrErrInvalidRealloc)
	require.NoError(t, checkCpiDataLength(execCtx, MaxPermittedDataIncrease+1, 1, false))
	require.ErrorIs(t, checkCpiDataLength(execCtx, MaxPermittedDataIncrease+2, 1, false), InstrErrInvalidRealloc)
}

func TestTranslateSerializedAccountDataDirectMappingSkipsSerializedBytes(t *testing.T) {
	features := feat.NewFeaturesDefault()
	features.EnableFeature(feat.VirtualAddressSpaceAdjustments, 0)
	features.EnableFeature(feat.AccountDataDirectMapping, 0)
	execCtx := &ExecutionCtx{Features: *features}

	meter := cu.NewComputeMeter(1)
	vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram, Funcs: map[uint32]int64{}}, &sbpf.VMOpts{
		Input:        []byte{0x42},
		Context:      execCtx,
		ComputeMeter: &meter,
	})
	defer vm.Finish()

	data, err := translateSerializedAccountData(vm, execCtx, sbpf.VaddrInput+42, 1)
	require.NoError(t, err)
	require.Nil(t, data)

	features.DisableFeature(feat.AccountDataDirectMapping)
	_, err = translateSerializedAccountData(vm, execCtx, sbpf.VaddrInput+42, 1)
	require.Error(t, err)
}

func TestUpdateCallerAccountRegionPreservesFirstWriteClone(t *testing.T) {
	features := feat.NewFeaturesDefault()
	features.EnableFeature(feat.VirtualAddressSpaceAdjustments, 0)
	features.EnableFeature(feat.AccountDataDirectMapping, 0)

	programKey := solana.PublicKey{1}
	accountKey := solana.PublicKey{2}
	parent := &accounts.Account{Key: accountKey, Owner: programKey, Lamports: 1, Data: []byte{7}}
	program := &accounts.Account{Key: programKey, Executable: true, Lamports: 1}
	txAccounts := NewTransactionAccountsFromRefs(
		[]*accounts.Account{parent, program},
		[]bool{true, true},
	)
	txCtx := NewTransactionCtx(*txAccounts, 4, 4)
	instrCtx := &InstructionCtx{}
	instrCtx.Configure(
		[]uint64{1},
		[]InstructionAccount{{
			IndexInTransaction: 0,
			IndexInCaller:      0,
			IndexInCallee:      0,
			IsWritable:         true,
		}},
		nil,
	)
	txCtx.InstructionTrace = []InstructionCtx{*instrCtx}
	txCtx.InstructionStack = []uint64{0}
	execCtx := &ExecutionCtx{Features: *features, TransactionContext: txCtx}

	borrowed, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	require.NoError(t, err)
	data, writable, onWrite, err := directMappedAccountData(execCtx, borrowed, 1)
	require.NoError(t, err)
	require.False(t, writable)
	require.NotNil(t, onWrite)

	meter := cu.NewComputeMeter(1)
	vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram, Funcs: map[uint32]int64{}}, &sbpf.VMOpts{
		Input:        []byte{0},
		Context:      execCtx,
		ComputeMeter: &meter,
		InputRegions: []sbpf.InputRegion{{
			Offset:               0,
			RegionSize:           1,
			AddressSpaceReserved: 1,
			Writable:             writable,
			AccountIndex:         0,
			Data:                 data,
			OnWrite:              onWrite,
		}},
	})
	defer vm.Finish()

	err = updateCallerAccountRegion(vm, execCtx, &CallerAccount{
		OriginalDataLen: 1,
		VmDataAddr:      sbpf.VaddrInput,
	}, borrowed, false)
	require.NoError(t, err)
	borrowed.Drop()

	require.NoError(t, vm.Write8(sbpf.VaddrInput, 9))
	require.Equal(t, byte(7), parent.Data[0], "the shared parent account must remain immutable")
	require.Equal(t, byte(9), txCtx.Accounts.Accounts[0].Data[0])
	require.False(t, txCtx.Accounts.Shared[0])
	require.True(t, txCtx.Accounts.Touched[0], "the direct-mapped write must be published")
}
