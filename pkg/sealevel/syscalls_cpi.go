package sealevel

import (
	"bytes"
	"encoding/binary"

	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/mlog"

	//"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/gagliardetto/solana-go"
)

const (
	MaxSigners                 = 16
	MaxCpiInstructionDataLen   = 10 * 1024
	MaxCpiInstructionAccounts  = 255
	MaxCpiAccountInfos         = 128
	MaxCpiAccountInfosSimd0339 = 255
	AccountInfoByteSize        = 80

	solAccountInfoCDataLenOffset    = 16
	solAccountInfoRustDataLenOffset = 32
)

func checkInstructionSize(execCtx *ExecutionCtx, numAccounts uint64, dataLen uint64) error {
	if execCtx.Features.IsActive(features.LoosenCpiSizeRestriction) {
		if dataLen > MaxCpiInstructionDataLen {
			return SyscallErrMaxInstructionDataLenExceeded
		}

		if numAccounts > MaxCpiInstructionAccounts {
			return SyscallErrMaxInstructionAccountsExceeded
		}
	} else {
		size := safemath.SaturatingAddU64(safemath.SaturatingMulU64(numAccounts, AccountMetaSize), dataLen)
		if size > cu.CUMaxCpiInstructionSize {
			return SyscallErrInstructionTooLarge
		}
	}
	return nil
}

func translateInstructionC(vm sbpf.VM, addr uint64) (Instruction, error) {
	ixData, err := vm.Translate(addr, SolInstructionCStructSize, false)
	if err != nil {
		return Instruction{}, err
	}

	byteReader := bytes.NewReader(ixData)
	var ix SolInstructionC

	err = ix.Unmarshal(byteReader)
	if err != nil {
		return Instruction{}, err
	}

	err = checkInstructionSize(executionCtx(vm), ix.AccountsLen, ix.DataLen)
	if err != nil {
		return Instruction{}, err
	}

	pkData, err := vm.Translate(ix.ProgramIdAddr, solana.PublicKeyLength, false)
	if err != nil {
		return Instruction{}, err
	}
	programId := solana.PublicKeyFromBytes(pkData)

	accountMetasData, err := vm.Translate(ix.AccountsAddr, SolAccountMetaCSize*ix.AccountsLen, false)
	if err != nil {
		return Instruction{}, err
	}

	accountMetaReader := bytes.NewReader(accountMetasData)

	var accountMetas []SolAccountMetaC
	for count := uint64(0); count < ix.AccountsLen; count++ {
		var am SolAccountMetaC
		err = am.Unmarshal(accountMetaReader)
		if err != nil {
			return Instruction{}, err
		}
		accountMetas = append(accountMetas, am)
	}

	data, err := vm.Translate(ix.DataAddr, ix.DataLen, false)
	if err != nil {
		return Instruction{}, err
	}

	execCtx := executionCtx(vm)
	if execCtx.Features.IsActive(features.LoosenCpiSizeRestriction) {
		totalCuTranslationCost := ix.DataLen / cu.CUCpiBytesPerUnit

		if execCtx.Features.IsActive(features.IncreaseCpiAccountInfoLimit) {
			acctMetaTranslationCost := safemath.SaturatingMulU64(ix.AccountsLen, AccountMetaSize) / cu.CUCpiBytesPerUnit
			totalCuTranslationCost = safemath.SaturatingAddU64(totalCuTranslationCost, acctMetaTranslationCost)
		}

		err = execCtx.ComputeMeter.Consume(totalCuTranslationCost)
		if err != nil {
			return Instruction{}, err
		}
	}

	accounts := make([]AccountMeta, 0, len(accountMetas))
	for _, accountMeta := range accountMetas {
		if accountMeta.IsSigner > 1 || accountMeta.IsWritable > 1 {
			return Instruction{}, SyscallErrInvalidArgument
		}

		pubkeyData, err := vm.Translate(accountMeta.PubkeyAddr, solana.PublicKeyLength, false)
		if err != nil {
			return Instruction{}, err
		}
		pubkey := solana.PublicKeyFromBytes(pubkeyData)

		var isSigner bool
		var isWritable bool
		if accountMeta.IsSigner == 1 {
			isSigner = true
		}
		if accountMeta.IsWritable == 1 {
			isWritable = true
		}

		newAccountMeta := AccountMeta{Pubkey: pubkey, IsSigner: isSigner, IsWritable: isWritable}
		accounts = append(accounts, newAccountMeta)
	}

	return Instruction{Accounts: accounts, Data: data, ProgramId: programId}, nil
}

func translateInstructionRust(vm sbpf.VM, addr uint64) (Instruction, error) {
	ixData, err := vm.Translate(addr, SolInstructionRustStructSize, false)
	if err != nil {
		return Instruction{}, err
	}

	byteReader := bytes.NewReader(ixData)

	var ix SolInstructionRust
	err = ix.Unmarshal(byteReader)
	if err != nil {
		return Instruction{}, err
	}

	err = checkInstructionSize(executionCtx(vm), ix.Accounts.Len, ix.Data.Len)
	if err != nil {
		return Instruction{}, err
	}

	accountMetasData, err := vm.Translate(ix.Accounts.Addr, AccountMetaSize*ix.Accounts.Len, false)
	if err != nil {
		return Instruction{}, err
	}

	byteReader.Reset(accountMetasData)
	accountMetas := make([]AccountMeta, 0, ix.Accounts.Len)

	for i := uint64(0); i < ix.Accounts.Len; i++ {
		var accountMeta AccountMeta
		err = accountMeta.Unmarshal(byteReader)
		if err != nil {
			return Instruction{}, err
		}
		accountMetas = append(accountMetas, accountMeta)
	}

	data, err := vm.Translate(ix.Data.Addr, ix.Data.Len, false)
	if err != nil {
		return Instruction{}, err
	}

	execCtx := executionCtx(vm)
	if execCtx.Features.IsActive(features.LoosenCpiSizeRestriction) {
		totalCuTranslationCost := ix.Data.Len / cu.CUCpiBytesPerUnit

		if execCtx.Features.IsActive(features.IncreaseCpiAccountInfoLimit) {
			acctMetaTranslationCost := safemath.SaturatingMulU64(ix.Accounts.Len, AccountMetaSize) / cu.CUCpiBytesPerUnit
			totalCuTranslationCost = safemath.SaturatingAddU64(totalCuTranslationCost, acctMetaTranslationCost)
		}

		err = execCtx.ComputeMeter.Consume(totalCuTranslationCost)
		if err != nil {
			return Instruction{}, err
		}
	}

	return Instruction{Accounts: accountMetas, Data: data, ProgramId: ix.Pubkey}, nil
}

func translateSigners(vm sbpf.VM, programId solana.PublicKey, signersSeedsAddr, signersSeedsLen uint64) ([]solana.PublicKey, error) {

	if signersSeedsLen == 0 {
		return nil, nil
	}

	if signersSeedsLen > MaxSigners {
		return nil, SyscallErrTooManySigners
	}

	ssLen := safemath.SaturatingMulU64(signersSeedsLen, SolSignerSeedsCSize)
	signerSeedsMem, err := vm.Translate(signersSeedsAddr, ssLen, false)
	if err != nil {
		return nil, err
	}

	byteReader := bytes.NewReader(signerSeedsMem)
	var signerSeeds []VectorDescrC
	for count := uint64(0); count < signersSeedsLen; count++ {
		var s VectorDescrC
		err = s.Unmarshal(byteReader)
		if err != nil {
			return nil, err
		}
		signerSeeds = append(signerSeeds, s)
	}

	var pdas []solana.PublicKey

	for _, signerSeed := range signerSeeds {

		if signerSeed.Len > MaxSeeds {
			return nil, SyscallErrMaxSeedLengthExceeded
		}

		sz := safemath.SaturatingMulU64(signerSeed.Len, SolSignerSeedsCSize)
		mem, err := vm.Translate(signerSeed.Addr, sz, false)
		if err != nil {
			return nil, err
		}

		seedReader := bytes.NewReader(mem)
		var seeds []VectorDescrC

		for i := uint64(0); i < signerSeed.Len; i++ {
			var seed VectorDescrC
			err = seed.Unmarshal(seedReader)
			if err != nil {
				return nil, err
			}
			seeds = append(seeds, seed)
		}

		var seedBytes [][]byte

		for _, seed := range seeds {
			seedFragmentMem, err := vm.Translate(seed.Addr, seed.Len, false)
			if err != nil {
				return nil, err
			}
			seedBytes = append(seedBytes, seedFragmentMem)
		}

		pubkey, err := solana.CreateProgramAddress(seedBytes, programId)
		if err != nil {
			return nil, PubkeyErrInvalidSeeds
		}
		pdas = append(pdas, pubkey)
	}

	return pdas, nil
}

func isBpfLoaderUpgradebleUpgradeInstr(data []byte) bool {
	return len(data) != 0 && data[0] == 3
}

func isBpfLoaderUpgradebleSetAuthorityInstr(data []byte) bool {
	return len(data) != 0 && data[0] == 4
}

func isBpfLoaderUpgradebleSetAuthorityCheckedInstr(data []byte) bool {
	return len(data) != 0 && data[0] == 7
}

func isBpfLoaderUpgradebleCloseInstr(data []byte) bool {
	return len(data) != 0 && data[0] == 5
}

func isPrecompile(programId solana.PublicKey) bool {
	if programId == a.Secp256kPrecompileAddr || programId == a.Ed25519PrecompileAddr {
		return true
	} else {
		return false
	}
}

func checkAuthorizedProgram(execCtx *ExecutionCtx, programId solana.PublicKey, instructionData []byte) error {

	if programId == base58.MustDecodeFromString("NativeLoader1111111111111111111111111111111") ||
		programId == solana.BPFLoaderProgramID ||
		programId == solana.BPFLoaderDeprecatedProgramID ||
		(programId == solana.BPFLoaderUpgradeableProgramID &&
			!(isBpfLoaderUpgradebleUpgradeInstr(instructionData) ||
				isBpfLoaderUpgradebleSetAuthorityInstr(instructionData) ||
				(execCtx.Features.IsActive(features.EnableBpfLoaderSetAuthorityCheckedIx) && isBpfLoaderUpgradebleSetAuthorityCheckedInstr(instructionData)) ||
				isBpfLoaderUpgradebleCloseInstr(instructionData))) ||
		isPrecompile(programId) {
		return SyscallErrProgramNotSupported
	}

	return nil
}

func checkAccountInfos(execCtx *ExecutionCtx, numAccountInfos uint64) error {
	if execCtx.Features.IsActive(features.LoosenCpiSizeRestriction) {
		var maxAccountInfos uint64
		if execCtx.Features.IsActive(features.IncreaseCpiAccountInfoLimit) {
			maxAccountInfos = MaxCpiAccountInfosSimd0339
		} else if execCtx.Features.IsActive(features.IncreaseTxAccountLockLimit) {
			maxAccountInfos = MaxCpiAccountInfos
		} else {
			maxAccountInfos = 64
		}
		if numAccountInfos > maxAccountInfos {
			return SyscallErrMaxInstructionAccountInfosExceeded
		}
	} else {
		adjustedLen := safemath.SaturatingMulU64(numAccountInfos, solana.PublicKeyLength)
		if adjustedLen > cu.CUMaxCpiInstructionSize {
			return SyscallErrTooManyAccounts
		}
	}
	return nil
}

func cpiInvokeUnits(f *features.Features) uint64 {
	if f != nil && f.IsActive(features.IncreaseCpiAccountInfoLimit) {
		return cu.CUInvokeUnitsSimd0339
	}
	return cu.CUInvokeUnits
}

func syscallParameterAddressRangeRestricted(execCtx *ExecutionCtx, addr, size uint64) bool {
	return execCtx.Features.IsActive(features.SyscallParameterAddressRestrictions) &&
		safemath.SaturatingAddU64(addr, size) >= sbpf.VaddrInput
}

func currentSerializedAccountMetadata(execCtx *ExecutionCtx, indexInCaller uint64) (serializedAcctMetadata, error) {
	if len(execCtx.serializedAccountMetadataStack) == 0 {
		return serializedAcctMetadata{}, InstrErrMissingAccount
	}
	accountMetadatas := execCtx.serializedAccountMetadataStack[len(execCtx.serializedAccountMetadataStack)-1]
	if indexInCaller >= uint64(len(accountMetadatas)) {
		return serializedAcctMetadata{}, InstrErrMissingAccount
	}
	return accountMetadatas[indexInCaller], nil
}

func checkAccountInfoPointer(vmAddr, expectedVmAddr uint64) error {
	if vmAddr != expectedVmAddr {
		return SyscallErrInvalidPointer
	}
	return nil
}

func accountDataLenForCpi(execCtx *ExecutionCtx, refToLenInVm []byte, fallbackLen uint64) uint64 {
	if execCtx.Features.IsActive(features.SyscallParameterAddressRestrictions) {
		return binary.LittleEndian.Uint64(refToLenInVm)
	}
	return fallbackLen
}

func checkCpiDataLength(execCtx *ExecutionCtx, dataLen, originalDataLen uint64, isLoaderDeprecated bool) error {
	if !execCtx.Features.IsActive(features.SyscallParameterAddressRestrictions) {
		return nil
	}

	reserved := originalDataLen
	if !isLoaderDeprecated {
		reserved = safemath.SaturatingAddU64(reserved, MaxPermittedDataIncrease)
	}
	if dataLen > reserved {
		return InstrErrInvalidRealloc
	}
	return nil
}

type inputBackingTranslator interface {
	TranslateInput(addr uint64, size uint64) ([]byte, error)
}

type inputRegionLengthUpdater interface {
	SetInputRegionLength(addr uint64, length uint64, writable bool) bool
}

type inputRegionDataUpdater interface {
	SetInputRegionData(addr uint64, data []byte, length uint64, writable bool) bool
}

func translateSerializedAccountData(vm sbpf.VM, execCtx *ExecutionCtx, addr, size uint64) ([]byte, error) {
	if accountDataDirectMappingActive(execCtx) {
		return nil, nil
	}
	if execCtx.Features.IsActive(features.VirtualAddressSpaceAdjustments) {
		if translator, ok := vm.(inputBackingTranslator); ok {
			return translator.TranslateInput(addr, size)
		}
	}
	return vm.Translate(addr, size, true)
}

func translateAccountInfosC(vm sbpf.VM, accountInfosAddr, accountInfosLen uint64) ([]SolAccountInfoC, []solana.PublicKey, error) {
	size := safemath.SaturatingMulU64(accountInfosLen, SolAccountInfoCSize)
	execCtx := executionCtx(vm)
	if syscallParameterAddressRangeRestricted(execCtx, accountInfosAddr, size) {
		return nil, nil, SyscallErrInvalidPointer
	}

	accountInfosData, err := vm.Translate(accountInfosAddr, size, false)
	if err != nil {
		return nil, nil, err
	}

	reader := bytes.NewReader(accountInfosData)
	accountInfos := make([]SolAccountInfoC, 0, accountInfosLen)

	for count := uint64(0); count < accountInfosLen; count++ {
		var acctInfo SolAccountInfoC
		err = acctInfo.Unmarshal(reader)
		if err != nil {
			return nil, nil, err
		}
		accountInfos = append(accountInfos, acctInfo)
	}

	err = checkAccountInfos(execCtx, uint64(len(accountInfos)))
	if err != nil {
		return nil, nil, err
	}

	if execCtx.Features.IsActive(features.IncreaseCpiAccountInfoLimit) {
		acctInfosBytes := safemath.SaturatingMulU64(uint64(len(accountInfos)), AccountInfoByteSize)
		err = execCtx.ComputeMeter.Consume(acctInfosBytes / cu.CUCpiBytesPerUnit)
		if err != nil {
			return nil, nil, err
		}
	}

	accountInfoKeys := make([]solana.PublicKey, 0, len(accountInfos))
	for _, acctInfo := range accountInfos {
		keyData, err := vm.Translate(acctInfo.KeyAddr, 32, false)
		if err != nil {
			return nil, nil, err
		}
		key := solana.PublicKeyFromBytes(keyData)
		accountInfoKeys = append(accountInfoKeys, key)
	}

	return accountInfos, accountInfoKeys, nil
}

func translateAccountInfosRust(vm sbpf.VM, accountInfosAddr, accountInfosLen uint64) ([]SolAccountInfoRust, []solana.PublicKey, error) {
	size := safemath.SaturatingMulU64(accountInfosLen, SolAccountInfoRustSize)
	execCtx := executionCtx(vm)
	if syscallParameterAddressRangeRestricted(execCtx, accountInfosAddr, size) {
		return nil, nil, SyscallErrInvalidPointer
	}

	accountInfosData, err := vm.Translate(accountInfosAddr, size, false)
	if err != nil {
		return nil, nil, err
	}

	reader := bytes.NewReader(accountInfosData)
	accountInfos := make([]SolAccountInfoRust, 0, accountInfosLen)

	for count := uint64(0); count < accountInfosLen; count++ {
		var acctInfo SolAccountInfoRust
		err = acctInfo.Unmarshal(reader)
		if err != nil {
			return nil, nil, err
		}
		accountInfos = append(accountInfos, acctInfo)
	}

	err = checkAccountInfos(execCtx, uint64(len(accountInfos)))
	if err != nil {
		return nil, nil, err
	}

	if execCtx.Features.IsActive(features.IncreaseCpiAccountInfoLimit) {
		acctInfosBytes := safemath.SaturatingMulU64(uint64(len(accountInfos)), AccountInfoByteSize)
		err = execCtx.ComputeMeter.Consume(acctInfosBytes / cu.CUCpiBytesPerUnit)
		if err != nil {
			return nil, nil, err
		}
	}

	accountInfoKeys := make([]solana.PublicKey, 0, len(accountInfos))
	for _, acctInfo := range accountInfos {
		keyData, err := vm.Translate(acctInfo.PubkeyAddr, 32, false)
		if err != nil {
			return nil, nil, err
		}
		key := solana.PublicKeyFromBytes(keyData)
		accountInfoKeys = append(accountInfoKeys, key)
	}

	return accountInfos, accountInfoKeys, nil
}

func callerAccountFromAccountInfoC(vm sbpf.VM, execCtx *ExecutionCtx, indexInCaller, callerAcctIdx uint64, accountInfo SolAccountInfoC, accountInfosAddr uint64, isLoaderDeprecated bool) (CallerAccount, error) {
	originalDataLen := accountInfo.DataLen
	var accountMetadata serializedAcctMetadata
	var err error
	usesSerializedAccountMetadata := execCtx.Features.IsActive(features.SyscallParameterAddressRestrictions) ||
		execCtx.Features.IsActive(features.VirtualAddressSpaceAdjustments)
	if usesSerializedAccountMetadata {
		accountMetadata, err = currentSerializedAccountMetadata(execCtx, indexInCaller)
		if err != nil {
			return CallerAccount{}, err
		}
		originalDataLen = accountMetadata.originalDataLen
	}
	if execCtx.Features.IsActive(features.SyscallParameterAddressRestrictions) {
		if err = checkAccountInfoPointer(accountInfo.KeyAddr, accountMetadata.vmKeyAddr); err != nil {
			return CallerAccount{}, err
		}
		if err = checkAccountInfoPointer(accountInfo.OwnerAddr, accountMetadata.vmOwnerAddr); err != nil {
			return CallerAccount{}, err
		}
		if err = checkAccountInfoPointer(accountInfo.LamportsAddr, accountMetadata.vmLamportsAddr); err != nil {
			return CallerAccount{}, err
		}
		if err = checkAccountInfoPointer(accountInfo.DataAddr, accountMetadata.vmDataAddr); err != nil {
			return CallerAccount{}, err
		}
	}

	lamports, err := vm.Translate(accountInfo.LamportsAddr, 8, true)
	if err != nil {
		return CallerAccount{}, err
	}

	owner, err := vm.Translate(accountInfo.OwnerAddr, solana.PublicKeyLength, true)
	if err != nil {
		return CallerAccount{}, err
	}

	dataLenVmAddr := accountInfosAddr + (callerAcctIdx * SolAccountInfoCSize) + solAccountInfoCDataLenOffset
	if syscallParameterAddressRestricted(execCtx, dataLenVmAddr) {
		return CallerAccount{}, SyscallErrInvalidPointer
	}

	refToLenInVm, err := vm.Translate(dataLenVmAddr, 8, true)
	if err != nil {
		return CallerAccount{}, err
	}

	dataLen := accountDataLenForCpi(execCtx, refToLenInVm, accountInfo.DataLen)
	err = checkCpiDataLength(execCtx, dataLen, originalDataLen, isLoaderDeprecated)
	if err != nil {
		return CallerAccount{}, err
	}

	cost := dataLen / cu.CUCpiBytesPerUnit
	err = execCtx.ComputeMeter.Consume(cost)
	if err != nil {
		return CallerAccount{}, err
	}

	serializedData, err := translateSerializedAccountData(vm, execCtx, accountInfo.DataAddr, dataLen)
	if err != nil {
		return CallerAccount{}, err
	}

	callerAcct := CallerAccount{Lamports: lamports, Owner: owner, OriginalDataLen: originalDataLen,
		SerializedData: serializedData, VmDataAddr: accountInfo.DataAddr, RefToLenInVm: refToLenInVm}

	return callerAcct, nil
}

func callerAccountFromAccountInfoRust(vm sbpf.VM, execCtx *ExecutionCtx, indexInCaller uint64, accountInfo SolAccountInfoRust, isLoaderDeprecated bool) (CallerAccount, error) {
	var accountMetadata serializedAcctMetadata
	var err error
	usesSerializedAccountMetadata := execCtx.Features.IsActive(features.SyscallParameterAddressRestrictions) ||
		execCtx.Features.IsActive(features.VirtualAddressSpaceAdjustments)
	if usesSerializedAccountMetadata {
		accountMetadata, err = currentSerializedAccountMetadata(execCtx, indexInCaller)
		if err != nil {
			return CallerAccount{}, err
		}
	}
	if execCtx.Features.IsActive(features.SyscallParameterAddressRestrictions) {
		if err = checkAccountInfoPointer(accountInfo.PubkeyAddr, accountMetadata.vmKeyAddr); err != nil {
			return CallerAccount{}, err
		}
		if err = checkAccountInfoPointer(accountInfo.OwnerAddr, accountMetadata.vmOwnerAddr); err != nil {
			return CallerAccount{}, err
		}
		if syscallParameterAddressRestricted(execCtx, accountInfo.LamportsBoxAddr) {
			return CallerAccount{}, SyscallErrInvalidPointer
		}
	}

	lamportsBoxData, err := vm.Translate(accountInfo.LamportsBoxAddr, RefCellRustSize, false)
	if err != nil {
		return CallerAccount{}, err
	}

	reader := bytes.NewReader(lamportsBoxData)

	var lamportsBox RefCellRust
	err = lamportsBox.Unmarshal(reader)
	if err != nil {
		return CallerAccount{}, err
	}

	if execCtx.Features.IsActive(features.SyscallParameterAddressRestrictions) {
		if err = checkAccountInfoPointer(lamportsBox.Addr, accountMetadata.vmLamportsAddr); err != nil {
			return CallerAccount{}, err
		}
	}

	lamports, err := vm.Translate(lamportsBox.Addr, 8, true)
	if err != nil {
		return CallerAccount{}, err
	}

	owner, err := vm.Translate(accountInfo.OwnerAddr, solana.PublicKeyLength, true)
	if err != nil {
		return CallerAccount{}, err
	}

	if syscallParameterAddressRestricted(execCtx, accountInfo.DataBoxAddr) {
		return CallerAccount{}, SyscallErrInvalidPointer
	}

	dataBoxBytes, err := vm.Translate(accountInfo.DataBoxAddr, RefCellVecRustSize, false)
	if err != nil {
		return CallerAccount{}, err
	}

	reader.Reset(dataBoxBytes)

	var dataBox RefCellVecRust
	err = dataBox.Unmarshal(reader)
	if err != nil {
		return CallerAccount{}, err
	}

	originalDataLen := dataBox.Len
	if usesSerializedAccountMetadata {
		originalDataLen = accountMetadata.originalDataLen
	}
	if execCtx.Features.IsActive(features.SyscallParameterAddressRestrictions) {
		if err = checkAccountInfoPointer(dataBox.Addr, accountMetadata.vmDataAddr); err != nil {
			return CallerAccount{}, err
		}
	}

	dataLenVmAddr := safemath.SaturatingAddU64(accountInfo.DataBoxAddr, solAccountInfoRustDataLenOffset)
	if syscallParameterAddressRestricted(execCtx, dataLenVmAddr) {
		return CallerAccount{}, SyscallErrInvalidPointer
	}

	refToLenInVm, err := vm.Translate(dataLenVmAddr, 8, true)
	if err != nil {
		return CallerAccount{}, err
	}

	dataLen := accountDataLenForCpi(execCtx, refToLenInVm, dataBox.Len)
	err = checkCpiDataLength(execCtx, dataLen, originalDataLen, isLoaderDeprecated)
	if err != nil {
		return CallerAccount{}, err
	}

	cost := dataLen / cu.CUCpiBytesPerUnit
	err = execCtx.ComputeMeter.Consume(cost)
	if err != nil {
		return CallerAccount{}, err
	}

	serializedData, err := translateSerializedAccountData(vm, execCtx, dataBox.Addr, dataLen)
	if err != nil {
		return CallerAccount{}, err
	}

	callerAcct := CallerAccount{Lamports: lamports, Owner: owner, OriginalDataLen: originalDataLen,
		SerializedData: serializedData, VmDataAddr: dataBox.Addr, RefToLenInVm: refToLenInVm}

	return callerAcct, nil
}

func updateCalleeAccount(vm sbpf.VM, execCtx *ExecutionCtx, callerAccount CallerAccount, calleeAccount *BorrowedAccount) (bool, error) {
	var err error
	mustUpdateCaller := false

	callerLamports := binary.LittleEndian.Uint64(callerAccount.Lamports)
	if calleeAccount.Account.Lamports != callerLamports {
		err = calleeAccount.SetLamports(callerLamports, execCtx.Features)
		if err != nil {
			return false, err
		}
	}

	if execCtx.Features.IsActive(features.VirtualAddressSpaceAdjustments) {
		directMapping := accountDataDirectMappingActive(execCtx)
		prevLen := uint64(len(calleeAccount.Data()))
		postLen := binary.LittleEndian.Uint64(callerAccount.RefToLenInVm)
		if prevLen != postLen {
			if !directMapping && postLen < prevLen {
				previousSerializedData, translateErr := translateSerializedAccountData(vm, execCtx, callerAccount.VmDataAddr, prevLen)
				if translateErr != nil {
					return false, translateErr
				}
				if uint64(len(previousSerializedData)) < prevLen {
					return false, InstrErrAccountDataTooSmall
				}
				for i := postLen; i < prevLen; i++ {
					previousSerializedData[i] = 0
				}
			}
			err = calleeAccount.SetDataLength(postLen, execCtx.Features)
			if err != nil {
				return false, err
			}
			mustUpdateCaller = true
		}

		if !directMapping && calleeAccount.DataCanBeChanged(execCtx.Features) == nil {
			if uint64(len(callerAccount.SerializedData)) < postLen {
				return false, InstrErrAccountDataTooSmall
			}
			err = calleeAccount.SetData(execCtx.Features, callerAccount.SerializedData[:postLen])
			if err != nil {
				return false, err
			}
		}
	} else {
		err1 := calleeAccount.CanDataBeResized(uint64(len(callerAccount.SerializedData)))
		err2 := calleeAccount.DataCanBeChanged(execCtx.Features)

		if err1 != nil {
			err = err1
		} else if err2 != nil {
			err = err2
		}

		// can't change data
		if err != nil {
			if !bytes.Equal(callerAccount.SerializedData, calleeAccount.Data()) {
				return false, err
			}
			err = nil
		} else {
			err = calleeAccount.SetData(execCtx.Features, callerAccount.SerializedData)
			if err != nil {
				return false, err
			}
		}
	}

	if calleeAccount.Owner() != solana.PublicKeyFromBytes(callerAccount.Owner) {
		err = calleeAccount.SetOwner(execCtx.Features, solana.PublicKeyFromBytes(callerAccount.Owner))
		if err == nil {
			mustUpdateCaller = true
		}
	}

	return mustUpdateCaller, err
}

func updateCallerAccount(vm sbpf.VM, callerAcct *CallerAccount, calleeAcct *BorrowedAccount, isLoaderDeprecated bool) error {
	binary.LittleEndian.PutUint64(callerAcct.Lamports, calleeAcct.Lamports())
	copy(callerAcct.Owner, calleeAcct.Account.Owner[:])

	prevLen := binary.LittleEndian.Uint64(callerAcct.RefToLenInVm)
	postLen := uint64(len(calleeAcct.Data()))
	execCtx := executionCtx(vm)
	syscallParameterAddressRestrictions := execCtx.Features.IsActive(features.SyscallParameterAddressRestrictions)
	directMapping := accountDataDirectMappingActive(execCtx)
	maxPermittedIncrease := uint64(MaxPermittedDataIncrease)
	if syscallParameterAddressRestrictions && isLoaderDeprecated {
		maxPermittedIncrease = 0
	}
	addressSpaceReservedForAccount := safemath.SaturatingAddU64(callerAcct.OriginalDataLen, maxPermittedIncrease)

	if postLen > addressSpaceReservedForAccount && (syscallParameterAddressRestrictions || prevLen != postLen) {
		return InstrErrInvalidRealloc
	}

	if prevLen != postLen {
		if !directMapping && postLen < prevLen {
			if uint64(len(callerAcct.SerializedData)) < postLen {
				return InstrErrAccountDataTooSmall
			}
			for i := postLen; i < uint64(len(callerAcct.SerializedData)); i++ {
				callerAcct.SerializedData[i] = 0
			}
		}

		if !directMapping {
			sd, err := translateSerializedAccountData(vm, execCtx, callerAcct.VmDataAddr, postLen)
			if err != nil {
				return err
			}
			callerAcct.SerializedData = sd
		}

		binary.LittleEndian.PutUint64(callerAcct.RefToLenInVm, postLen)

		ptrAddr := safemath.SaturatingSubU64(callerAcct.VmDataAddr, 8)
		serializedLenSlice, err := vm.Translate(ptrAddr, 8, false)
		if err != nil {
			return err
		}

		binary.LittleEndian.PutUint64(serializedLenSlice, postLen)
	}

	if directMapping {
		return nil
	}

	toSlice := callerAcct.SerializedData
	fromSlice := calleeAcct.Data()

	if uint64(len(fromSlice)) < postLen {
		return SyscallErrInvalidLength
	}

	fromSlice = fromSlice[:postLen]

	if len(toSlice) != len(fromSlice) {
		return InstrErrAccountDataTooSmall
	}

	copy(toSlice, fromSlice)

	return nil
}

func updateCallerAccountRegion(vm sbpf.VM, execCtx *ExecutionCtx, callerAcct *CallerAccount, calleeAcct *BorrowedAccount, isLoaderDeprecated bool) error {
	reserved := callerAcct.OriginalDataLen
	if !isLoaderDeprecated {
		reserved = safemath.SaturatingAddU64(reserved, MaxPermittedDataIncrease)
	}
	if reserved == 0 {
		return nil
	}

	writable := calleeAcct.DataCanBeChanged(execCtx.Features) == nil
	if accountDataDirectMappingActive(execCtx) {
		// A direct-mapped account can still reference the transaction's shared
		// parent state here.  Keep the caller region read-only in that case so
		// its existing OnWrite hook performs the first-write clone.  Marking the
		// region writable would let the caller mutate the shared backing slice
		// without setting TransactionAccounts.Touched, so the successful write
		// would be omitted from publication and the accounts LtHash.
		if calleeAcct.IndexInTransaction < uint64(len(calleeAcct.TxCtx.Accounts.Shared)) &&
			calleeAcct.TxCtx.Accounts.Shared[calleeAcct.IndexInTransaction] {
			writable = false
		}
		updater, ok := vm.(inputRegionDataUpdater)
		if !ok {
			return nil
		}
		if !updater.SetInputRegionData(callerAcct.VmDataAddr, calleeAcct.Data(), uint64(len(calleeAcct.Data())), writable) {
			return InstrErrMissingAccount
		}
	} else {
		updater, ok := vm.(inputRegionLengthUpdater)
		if !ok {
			return nil
		}
		if !updater.SetInputRegionLength(callerAcct.VmDataAddr, uint64(len(calleeAcct.Data())), writable) {
			return InstrErrMissingAccount
		}
	}
	return nil
}

func translateAndUpdateAccountsC(vm sbpf.VM, instructionAccts []InstructionAccount, programIndices []uint64, accountInfoKeys []solana.PublicKey, accountInfos []SolAccountInfoC, accountInfosAddr uint64, isLoaderDeprecated bool) (TranslatedAccounts, error) {
	execCtx := executionCtx(vm)
	txCtx := execCtx.TransactionContext
	syscallParameterAddressRestrictions := execCtx.Features.IsActive(features.SyscallParameterAddressRestrictions)

	ixCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return nil, err
	}

	accounts := make(TranslatedAccounts, 0)

	idx := len(programIndices) - 1
	if idx < 0 {
		mlog.Log.Debugf("translateAndUpdateAccountsC InstrErrMissingAccount [1]")
		return nil, InstrErrMissingAccount
	}
	programAcctIdx := programIndices[idx]
	accounts = append(accounts, TranslatedAccount{IndexOfAccount: programAcctIdx, CallerAccount: nil})

	for instructionAcctIdx, instructionAcct := range instructionAccts {
		if uint64(instructionAcctIdx) != instructionAcct.IndexInCallee {
			continue
		}

		err := func() error {
			calleeAcct, err := ixCtx.BorrowInstructionAccount(txCtx, instructionAcct.IndexInCaller)
			if err != nil {
				return err
			}
			defer calleeAcct.Drop()

			accountKey, err := txCtx.KeyOfAccountAtIndex(instructionAcct.IndexInTransaction)
			if err != nil {
				return err
			}

			if calleeAcct.IsExecutable() {
				cost := uint64(len(calleeAcct.Data()) / cu.CUCpiBytesPerUnit)
				err = execCtx.ComputeMeter.Consume(cost)
				if err != nil {
					return InstrErrComputationalBudgetExceeded
				}
				accounts = append(accounts, TranslatedAccount{IndexOfAccount: instructionAcct.IndexInCaller, CallerAccount: nil})
				return nil
			}

			for index, accountInfoKey := range accountInfoKeys {
				if accountKey != accountInfoKey {
					continue
				}

				accountInfo := accountInfos[index]
				callerAcct, err := callerAccountFromAccountInfoC(vm, execCtx, instructionAcct.IndexInCaller, uint64(index), accountInfo, accountInfosAddr, isLoaderDeprecated)
				if err != nil {
					return err
				}

				mustUpdateCaller := false
				if !syscallParameterAddressRestrictions {
					mustUpdateCaller, err = updateCalleeAccount(vm, execCtx, callerAcct, calleeAcct)
					if err != nil {
						return err
					}
				}

				var c *CallerAccount
				if instructionAcct.IsWritable || syscallParameterAddressRestrictions {
					c = &callerAcct
				}
				accounts = append(accounts, TranslatedAccount{
					IndexOfAccount:      instructionAcct.IndexInCaller,
					CallerAccount:       c,
					UpdateCallerAccount: instructionAcct.IsWritable,
					UpdateCallerRegion:  instructionAcct.IsWritable || mustUpdateCaller || syscallParameterAddressRestrictions,
				})
				return nil
			}

			return InstrErrMissingAccount
		}()
		if err != nil {
			return nil, err
		}
	}

	if syscallParameterAddressRestrictions {
		for accountIdx := range accounts {
			account := &accounts[accountIdx]
			if account.CallerAccount == nil {
				continue
			}

			calleeAcct, err := ixCtx.BorrowInstructionAccount(txCtx, account.IndexOfAccount)
			if err != nil {
				return nil, err
			}
			mustUpdateCaller, err := updateCalleeAccount(vm, execCtx, *account.CallerAccount, calleeAcct)
			calleeAcct.Drop()
			if err != nil {
				return nil, err
			}
			account.UpdateCallerRegion = account.UpdateCallerAccount || mustUpdateCaller
		}
	}

	return accounts, nil
}

func translateAndUpdateAccountsRust(vm sbpf.VM, instructionAccts []InstructionAccount, programIndices []uint64, accountInfoKeys []solana.PublicKey, accountInfos []SolAccountInfoRust, accountInfosAddr uint64, isLoaderDeprecated bool) (TranslatedAccounts, error) {
	execCtx := executionCtx(vm)
	txCtx := execCtx.TransactionContext
	syscallParameterAddressRestrictions := execCtx.Features.IsActive(features.SyscallParameterAddressRestrictions)

	ixCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return nil, err
	}

	accounts := make(TranslatedAccounts, 0)

	idx := len(programIndices) - 1
	if idx < 0 {
		return nil, InstrErrMissingAccount
	}
	programAcctIdx := programIndices[idx]
	accounts = append(accounts, TranslatedAccount{IndexOfAccount: programAcctIdx, CallerAccount: nil})

	for instructionAcctIdx, instructionAcct := range instructionAccts {
		if uint64(instructionAcctIdx) != instructionAcct.IndexInCallee {
			continue
		}

		err := func() error {
			calleeAcct, err := ixCtx.BorrowInstructionAccount(txCtx, instructionAcct.IndexInCaller)
			if err != nil {
				return err
			}
			defer calleeAcct.Drop()

			accountKey, err := txCtx.KeyOfAccountAtIndex(instructionAcct.IndexInTransaction)
			if err != nil {
				return err
			}

			if calleeAcct.IsExecutable() {
				cost := uint64(len(calleeAcct.Data()) / cu.CUCpiBytesPerUnit)
				err = execCtx.ComputeMeter.Consume(cost)
				if err != nil {
					return InstrErrComputationalBudgetExceeded
				}
				accounts = append(accounts, TranslatedAccount{IndexOfAccount: instructionAcct.IndexInCaller, CallerAccount: nil})
				return nil
			}

			for index, accountInfoKey := range accountInfoKeys {
				if accountKey != accountInfoKey {
					continue
				}

				accountInfo := accountInfos[index]
				callerAcct, err := callerAccountFromAccountInfoRust(vm, execCtx, instructionAcct.IndexInCaller, accountInfo, isLoaderDeprecated)
				if err != nil {
					return err
				}

				mustUpdateCaller := false
				if !syscallParameterAddressRestrictions {
					mustUpdateCaller, err = updateCalleeAccount(vm, execCtx, callerAcct, calleeAcct)
					if err != nil {
						return err
					}
				}

				var c *CallerAccount
				if instructionAcct.IsWritable || syscallParameterAddressRestrictions {
					c = &callerAcct
				}
				accounts = append(accounts, TranslatedAccount{
					IndexOfAccount:      instructionAcct.IndexInCaller,
					CallerAccount:       c,
					UpdateCallerAccount: instructionAcct.IsWritable,
					UpdateCallerRegion:  instructionAcct.IsWritable || mustUpdateCaller || syscallParameterAddressRestrictions,
				})
				return nil
			}

			return InstrErrMissingAccount
		}()
		if err != nil {
			return nil, err
		}
	}

	if syscallParameterAddressRestrictions {
		for accountIdx := range accounts {
			account := &accounts[accountIdx]
			if account.CallerAccount == nil {
				continue
			}

			calleeAcct, err := ixCtx.BorrowInstructionAccount(txCtx, account.IndexOfAccount)
			if err != nil {
				return nil, err
			}
			mustUpdateCaller, err := updateCalleeAccount(vm, execCtx, *account.CallerAccount, calleeAcct)
			calleeAcct.Drop()
			if err != nil {
				return nil, err
			}
			account.UpdateCallerRegion = account.UpdateCallerAccount || mustUpdateCaller
		}
	}

	return accounts, nil
}

func translateAccountsC(vm sbpf.VM, instructionAccts []InstructionAccount, programIndices []uint64, accountInfosAddr uint64, accountInfosLen uint64, isLoaderDeprecated bool) (TranslatedAccounts, error) {
	accountInfos, accountInfoKeys, err := translateAccountInfosC(vm, accountInfosAddr, accountInfosLen)
	if err != nil {
		return nil, err
	}

	return translateAndUpdateAccountsC(vm, instructionAccts, programIndices, accountInfoKeys, accountInfos, accountInfosAddr, isLoaderDeprecated)
}

func translateAccountsRust(vm sbpf.VM, instructionAccts []InstructionAccount, programIndices []uint64, accountInfosAddr uint64, accountInfosLen uint64, isLoaderDeprecated bool) (TranslatedAccounts, error) {
	accountInfos, accountInfoKeys, err := translateAccountInfosRust(vm, accountInfosAddr, accountInfosLen)
	if err != nil {
		return nil, err
	}

	return translateAndUpdateAccountsRust(vm, instructionAccts, programIndices, accountInfoKeys, accountInfos, accountInfosAddr, isLoaderDeprecated)
}

// SyscallInvokeSignedCImpl is an implementation of the sol_invoke_signed_c syscall
func SyscallInvokeSignedCImpl(vm sbpf.VM, instructionAddr, accountInfosAddr, accountInfosLen, signerSeedsAddr, signerSeedsLen uint64) (uint64, error) {
	execCtx := executionCtx(vm)
	err := execCtx.ComputeMeter.Consume(cpiInvokeUnits(&execCtx.Features))
	if err != nil {
		return syscallCuErr()
	}

	// translate instruction
	ix, err := translateInstructionC(vm, instructionAddr)
	if err != nil {
		return syscallErr(err)
	}

	txCtx := transactionCtx(vm)
	instructionCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return syscallErr(err)
	}

	callerProgramId, err := instructionCtx.LastProgramKey(txCtx)

	// translate signers
	signers, err := translateSigners(vm, callerProgramId, signerSeedsAddr, signerSeedsLen)
	if err != nil {
		return syscallErr(err)
	}

	var isLoaderDeprecated bool
	lastProgramAcct, err := instructionCtx.BorrowLastProgramAccount(txCtx)
	if err != nil {
		return syscallErr(err)
	}
	if lastProgramAcct.Owner() == a.BpfLoaderDeprecatedAddr {
		isLoaderDeprecated = true
	}
	lastProgramAcct.Drop()

	instructionAccts, programIndices, err := execCtx.PrepareInstruction(ix, signers)
	if err != nil {
		return syscallErr(err)
	}

	err = checkAuthorizedProgram(execCtx, ix.ProgramId, ix.Data)
	if err != nil {
		return syscallErr(err)
	}

	accounts, err := translateAccountsC(vm, instructionAccts, programIndices, accountInfosAddr, accountInfosLen, isLoaderDeprecated)
	if err != nil {
		return syscallErr(err)
	}

	err = execCtx.ProcessInstruction(ix.Data, instructionAccts, programIndices)
	if err != nil {
		return syscallErr(err)
	}

	for _, acct := range accounts {
		if acct.CallerAccount != nil && acct.UpdateCallerAccount {
			var calleeAcct *BorrowedAccount
			calleeAcct, err = instructionCtx.BorrowInstructionAccount(txCtx, acct.IndexOfAccount)
			if err != nil {
				return syscallErr(err)
			}
			err = updateCallerAccount(vm, acct.CallerAccount, calleeAcct, isLoaderDeprecated)
			calleeAcct.Drop()
			if err != nil {
				return syscallErr(err)
			}
		}
	}

	if execCtx.Features.IsActive(features.VirtualAddressSpaceAdjustments) {
		for _, acct := range accounts {
			if acct.CallerAccount == nil || !acct.UpdateCallerRegion {
				continue
			}
			var calleeAcct *BorrowedAccount
			calleeAcct, err = instructionCtx.BorrowInstructionAccount(txCtx, acct.IndexOfAccount)
			if err != nil {
				return syscallErr(err)
			}
			err = updateCallerAccountRegion(vm, execCtx, acct.CallerAccount, calleeAcct, isLoaderDeprecated)
			calleeAcct.Drop()
			if err != nil {
				return syscallErr(err)
			}
		}
	}

	return syscallSuccess(0)
}

// SyscallInvokeSignedRustImpl is an implementation of the sol_invoke_signed_rust syscall
func SyscallInvokeSignedRustImpl(vm sbpf.VM, instructionAddr, accountInfosAddr, accountInfosLen, signerSeedsAddr, signerSeedsLen uint64) (uint64, error) {
	//mlog.Log.Debugf("SyscallInvokeSignedRust")

	execCtx := executionCtx(vm)
	err := execCtx.ComputeMeter.Consume(cpiInvokeUnits(&execCtx.Features))
	if err != nil {
		return syscallCuErr()
	}

	// translate instruction
	ix, err := translateInstructionRust(vm, instructionAddr)
	if err != nil {
		return syscallErr(err)
	}

	txCtx := transactionCtx(vm)
	instructionCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return syscallErr(err)
	}

	callerProgramId, err := instructionCtx.LastProgramKey(txCtx)

	// translate signers
	signers, err := translateSigners(vm, callerProgramId, signerSeedsAddr, signerSeedsLen)
	if err != nil {
		return syscallErr(err)
	}

	var isLoaderDeprecated bool
	lastProgramAcct, err := instructionCtx.BorrowLastProgramAccount(txCtx)
	if err != nil {
		return syscallErr(err)
	}
	if lastProgramAcct.Owner() == a.BpfLoaderDeprecatedAddr {
		isLoaderDeprecated = true
	}
	lastProgramAcct.Drop()

	instructionAccts, programIndices, err := execCtx.PrepareInstruction(ix, signers)
	if err != nil {
		return syscallErr(err)
	}

	err = checkAuthorizedProgram(execCtx, ix.ProgramId, ix.Data)
	if err != nil {
		return syscallErr(err)
	}

	accounts, err := translateAccountsRust(vm, instructionAccts, programIndices, accountInfosAddr, accountInfosLen, isLoaderDeprecated)
	if err != nil {
		return syscallErr(err)
	}

	err = execCtx.ProcessInstruction(ix.Data, instructionAccts, programIndices)
	if err != nil {
		return syscallErr(err)
	}

	for _, acct := range accounts {
		if acct.CallerAccount != nil && acct.UpdateCallerAccount {
			var calleeAcct *BorrowedAccount
			calleeAcct, err = instructionCtx.BorrowInstructionAccount(txCtx, acct.IndexOfAccount)
			if err != nil {
				return syscallErr(err)
			}
			err = updateCallerAccount(vm, acct.CallerAccount, calleeAcct, isLoaderDeprecated)
			calleeAcct.Drop()
			if err != nil {
				return syscallErr(err)
			}
		}
	}

	if execCtx.Features.IsActive(features.VirtualAddressSpaceAdjustments) {
		for _, acct := range accounts {
			if acct.CallerAccount == nil || !acct.UpdateCallerRegion {
				continue
			}
			var calleeAcct *BorrowedAccount
			calleeAcct, err = instructionCtx.BorrowInstructionAccount(txCtx, acct.IndexOfAccount)
			if err != nil {
				return syscallErr(err)
			}
			err = updateCallerAccountRegion(vm, execCtx, acct.CallerAccount, calleeAcct, isLoaderDeprecated)
			calleeAcct.Drop()
			if err != nil {
				return syscallErr(err)
			}
		}
	}

	return syscallSuccess(0)
}
