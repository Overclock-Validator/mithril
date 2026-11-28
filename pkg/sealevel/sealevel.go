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

// sysvarAddrs and nativeProgramAddrs are the keys IsSysvar and
// IsNativeProgram accept. The writability filter below is built from the same
// tables, so it cannot miss one.
var sysvarAddrs = [...]solana.PublicKey{
	SysvarClockAddr, SysvarEpochScheduleAddr, SysvarFeesAddr, SysvarInstructionsAddr,
	SysvarRecentBlockHashesAddr, SysvarRentAddr, a.SysvarRewardsAddr, SysvarSlotHashesAddr,
	SysvarSlotHistoryAddr, SysvarStakeHistoryAddr,
}

var nativeProgramAddrs = [...]solana.PublicKey{
	a.SystemProgramAddr, a.BpfLoaderUpgradeableAddr, a.BpfLoader2Addr, a.BpfLoaderDeprecatedAddr,
	a.VoteProgramAddr, a.StakeProgramAddr, a.ConfigProgramAddr, a.StakeProgramConfigAddr,
	a.NativeLoaderAddr,
}

// writabilityFilter has a bit set for the low 12 bits of every key IsWritable
// can demote (native programs, sysvars, reserved keys, the secp256r1
// precompile). A key whose bit is clear is none of them, so IsWritable skips
// the comparisons; a set bit only means the full check runs.
var writabilityFilter = func() (filter [64]uint64) {
	add := func(k solana.PublicKey) {
		i := writabilityFilterIndex(&k)
		filter[i>>6] |= 1 << (i & 63)
	}
	for _, k := range sysvarAddrs {
		add(k)
	}
	for _, k := range nativeProgramAddrs {
		add(k)
	}
	for k := range NewReservedAcctsSet {
		add(k)
	}
	add(a.Secp256r1PrecompileAddr)
	return filter
}()

func writabilityFilterIndex(k *solana.PublicKey) uint {
	return uint(k[0]) | uint(k[1]&0x0f)<<8
}

func IsWritable(am *AccountMeta, f *features.Features) bool {
	if !am.IsWritable {
		return false
	}

	if i := writabilityFilterIndex(&am.Pubkey); writabilityFilter[i>>6]&(1<<(i&63)) == 0 {
		return true
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
	for i := range sysvarAddrs {
		if pubkey == sysvarAddrs[i] {
			return true
		}
	}
	return false
}

func IsNativeProgram(pubkey solana.PublicKey) bool {
	for i := range nativeProgramAddrs {
		if pubkey == nativeProgramAddrs[i] {
			return true
		}
	}
	return false
}

func InstructionAcctsFromAccountMetas(instrAcctMetas []AccountMeta, txAccounts TransactionAccounts) []InstructionAccount {
	var instrAccts []InstructionAccount

	for instrAcctIdx, accountMeta := range instrAcctMetas {
		idxInTx := -1
		for pos, acct := range txAccounts.Accounts {
			a := *acct
			if a.Key == accountMeta.Pubkey {
				idxInTx = pos
				break
			}
		}
		if idxInTx == -1 {
			idxInTx = len(txAccounts.Accounts)
		}

		accts := instrAccts[:instrAcctIdx]
		idxInCallee := -1
		for pos, instrAcct := range accts {
			if instrAcct.IndexInTransaction == uint64(idxInTx) {
				idxInCallee = pos
				break
			}
		}
		if idxInCallee == -1 {
			idxInCallee = instrAcctIdx
		}

		newInstrAcct := InstructionAccount{IndexInTransaction: uint64(idxInTx), IndexInCaller: uint64(idxInTx), IndexInCallee: uint64(idxInCallee), IsSigner: accountMeta.IsSigner, IsWritable: accountMeta.IsWritable}
		instrAccts = append(instrAccts, newInstrAcct)
	}

	return instrAccts
}
