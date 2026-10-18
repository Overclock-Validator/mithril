package sealevel

import (
	"bytes"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Slot 450897580 created a 61-byte PDA via CPI, then appended 1210 bytes.
// CPI return must replace the region's growth handler along with its backing
// data and permissions: an initially external account has no growth handler.
func TestDirectMappingGrowthAfterCPI(t *testing.T) {
	for _, initiallyOwned := range []bool{false, true} {
		name := "newly_owned"
		if initiallyOwned {
			name = "already_owned"
		}
		t.Run(name, func(t *testing.T) {
			execCtx, instrCtx, vm, parent := directMappingCPITestVM(t, initiallyOwned)
			txCtx := execCtx.TransactionContext
			// Model a CPI creating/replacing account data and assigning ownership.
			current, err := txCtx.Accounts.Touch(0)
			require.NoError(t, err)
			current.Owner = txCtx.Accounts.Accounts[1].Key
			current.Data = bytes.Repeat([]byte{7}, 61)
			borrowed, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
			require.NoError(t, err)
			require.NoError(t, updateCallerAccountRegion(vm, execCtx, &CallerAccount{
				VmDataAddr: sbpf.VaddrInput,
			}, borrowed, false))
			borrowed.Drop()

			// Growth is write-triggered; reading the reserved space still fails.
			_, err = vm.Read8(sbpf.VaddrInput + 61)
			require.Error(t, err)
			appended := bytes.Repeat([]byte{9}, 1210)
			require.NoError(t, vm.Write(sbpf.VaddrHeap, appended))
			_, err = SyscallMemcpyImpl(vm, sbpf.VaddrInput+61, sbpf.VaddrHeap, uint64(len(appended)))
			require.NoError(t, err)
			require.Equal(t, bytes.Repeat([]byte{7}, 61), current.Data[:61])
			require.Equal(t, appended, current.Data[61:1271])
			require.Len(t, current.Data, MaxPermittedDataIncrease)
			require.Error(t, vm.Write8(sbpf.VaddrInput+MaxPermittedDataIncrease, 1))
			require.Empty(t, parent.Data, "CPI and growth must preserve the shared parent")
			require.True(t, txCtx.Accounts.Touched[0])
		})
	}
}

func TestDirectMappingCPIRevokesGrowthPermission(t *testing.T) {
	execCtx, instrCtx, vm, parent := directMappingCPITestVM(t, true)
	txCtx := execCtx.TransactionContext
	// Prime the writable fast-path cache before the CPI changes ownership.
	require.NoError(t, vm.Write8(sbpf.VaddrInput, 5))
	current, err := txCtx.Accounts.Touch(0)
	require.NoError(t, err)
	current.Owner = solana.PublicKey{3}
	current.Data = []byte{7}
	borrowed, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	require.NoError(t, err)
	require.NoError(t, updateCallerAccountRegion(vm, execCtx, &CallerAccount{
		VmDataAddr: sbpf.VaddrInput,
	}, borrowed, false))
	borrowed.Drop()

	// The pre-CPI handler must no longer authorize either writes or growth.
	require.Error(t, vm.Write8(sbpf.VaddrInput, 9))
	require.Error(t, vm.Write8(sbpf.VaddrInput+1, 9))
	require.Equal(t, []byte{7}, current.Data)
	require.Empty(t, parent.Data)
}

func directMappingCPITestVM(t *testing.T, initiallyOwned bool) (*ExecutionCtx, *InstructionCtx, *sbpf.Interpreter, *accounts.Account) {
	t.Helper()
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
	f.EnableFeature(features.AccountDataDirectMapping, 0)
	programKey := solana.PublicKey{1}
	parent := &accounts.Account{Key: solana.PublicKey{2}, Lamports: 1}
	if initiallyOwned {
		parent.Owner = programKey
	}
	program := &accounts.Account{Key: programKey, Lamports: 1, Executable: true}
	txAccounts := NewTransactionAccountsFromRefs([]*accounts.Account{parent, program}, []bool{true, true})
	txCtx := NewTransactionCtx(*txAccounts, 4, 4)
	instrCtx := &InstructionCtx{}
	instrCtx.Configure([]uint64{1}, []InstructionAccount{{IsWritable: true}}, nil)
	txCtx.InstructionTrace = []InstructionCtx{*instrCtx}
	txCtx.InstructionStack = []uint64{0}
	execCtx := &ExecutionCtx{Features: *f, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000)}
	borrowed, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	require.NoError(t, err)
	data, writable, onWrite, err := directMappedAccountData(execCtx, borrowed, MaxPermittedDataIncrease)
	require.NoError(t, err)
	borrowed.Drop()
	vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram}, &sbpf.VMOpts{
		Context: execCtx, ComputeMeter: &execCtx.ComputeMeter, HeapMax: 32 * 1024,
		InputRegions: []sbpf.InputRegion{{
			AddressSpaceReserved: MaxPermittedDataIncrease,
			Data:                 data, Writable: writable, OnWrite: onWrite,
		}},
	})
	t.Cleanup(vm.Finish)
	return execCtx, instrCtx, vm, parent
}
