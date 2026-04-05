package sealevel

import (
	"bytes"

	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/gagliardetto/solana-go"
)

func executionCtx(vm sbpf.VM) *ExecutionCtx {
	return vm.VMContext().(*ExecutionCtx)
}

func transactionCtx(vm sbpf.VM) *TransactionCtx {
	return vm.VMContext().(*ExecutionCtx).TransactionContext
}

func (t *TransactionCtx) newVMOpts(params *Params) *sbpf.VMOpts {
	execution := &ExecutionCtx{
		Log: new(LogRecorder),
	}
	var buf bytes.Buffer
	params.Serialize(&buf)
	return &sbpf.VMOpts{
		HeapMax: 32 * 1024,
		Syscalls: sbpf.SyscallRegistry(func(u uint32) (sbpf.Syscall, bool) {
			return Syscalls(&params.Features, false, u)
		}),
		Context: execution,
		MaxCU:   1_400_000,
		Input:   buf.Bytes(),
	}
}

// NewReservedAcctsSet contains reserved account addresses that should not be writable.
var NewReservedAcctsSet = map[solana.PublicKey]struct{}{
	a.AddressLookupTableAddr:    {},
	a.ComputeBudgetProgramAddr:  {},
	a.Ed25519PrecompileAddr:     {},
	a.LoaderV4Addr:              {},
	a.Secp256kPrecompileAddr:    {},
	a.ZkElgamalProofProgramAddr: {},
	a.ZkTokenProofProgramAddr:   {},
	SysvarEpochRewardsAddr:      {},
	SysvarLastRestartSlotAddr:   {},
	a.SysvarOwnerAddr:           {},
}

func IsWritable(am *AccountMeta, f *features.Features) bool {
	if !am.IsWritable {
		return false
	}

	if IsNativeProgram(am.Pubkey) || IsSysvar(am.Pubkey) {
		return false
	}

	if f.IsActive(features.AddNewReservedAccountKeys) {
		if _, isReserved := NewReservedAcctsSet[am.Pubkey]; isReserved {
			return false
		}
	}

	if f.IsActive(features.EnableSecp256r1Precompile) {
		if am.Pubkey == a.Secp256r1PrecompileAddr {
			return false
		}
	}

	return true
}

func IsSysvar(pubkey solana.PublicKey) bool {
	if pubkey == SysvarClockAddr || pubkey == SysvarEpochScheduleAddr ||
		pubkey == SysvarFeesAddr || pubkey == SysvarInstructionsAddr ||
		pubkey == SysvarRecentBlockHashesAddr || pubkey == SysvarRentAddr ||
		pubkey == a.SysvarRewardsAddr || pubkey == SysvarSlotHashesAddr ||
		pubkey == SysvarSlotHistoryAddr || pubkey == SysvarStakeHistoryAddr {
		return true
	} else {
		return false
	}
}

func IsNativeProgram(pubkey solana.PublicKey) bool {
	if pubkey == a.SystemProgramAddr || pubkey == a.BpfLoaderUpgradeableAddr ||
		pubkey == a.BpfLoader2Addr || pubkey == a.BpfLoaderDeprecatedAddr ||
		pubkey == a.VoteProgramAddr || pubkey == a.StakeProgramAddr ||
		pubkey == a.ConfigProgramAddr || pubkey == a.StakeProgramConfigAddr ||
		pubkey == a.NativeLoaderAddr {
		return true
	} else {
		return false
	}
}

func InstructionAcctsFromAccountMetas(instrAcctMetas []AccountMeta, txAccounts TransactionAccounts) []InstructionAccount {
	acctIndexLookup := make(map[solana.PublicKey]uint64, len(txAccounts.Accounts))
	for idx, acct := range txAccounts.Accounts {
		if _, exists := acctIndexLookup[acct.Key]; !exists {
			acctIndexLookup[acct.Key] = uint64(idx)
		}
	}

	instrAccts := make([]InstructionAccount, 0, len(instrAcctMetas))
	idxInCalleeByTxIndex := make(map[uint64]uint64, len(instrAcctMetas))
	missingIndex := uint64(len(txAccounts.Accounts))

	for instrAcctIdx, accountMeta := range instrAcctMetas {
		idxInTx, exists := acctIndexLookup[accountMeta.Pubkey]
		if !exists {
			idxInTx = missingIndex
		}

		idxInCallee, exists := idxInCalleeByTxIndex[idxInTx]
		if !exists {
			idxInCallee = uint64(instrAcctIdx)
			idxInCalleeByTxIndex[idxInTx] = idxInCallee
		}

		instrAccts = append(instrAccts, InstructionAccount{
			IndexInTransaction: idxInTx,
			IndexInCaller:      idxInTx,
			IndexInCallee:      idxInCallee,
			IsSigner:           accountMeta.IsSigner,
			IsWritable:         accountMeta.IsWritable,
		})
	}

	return instrAccts
}
