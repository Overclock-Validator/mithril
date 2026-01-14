package sealevel

import (
	"bytes"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

const (
	LoaderV4InstrTypeWrite = iota
	LoaderV4InstrTypeCopy
	LoaderV4InstrTypeSetProgramLength
	LoaderV4InstrTypeDeploy
	LoaderV4InstrTypeRetract
	LoaderV4InstrTypeTransferAuthority
	LoaderV4InstrTypeFinalize
)

const (
	LoaderV4StatusRetracted = iota
	LoaderV4StatusDeployed
	LoaderV4StatusFinalized
)

const (
	loaderV4ProgramDataOffset = 48
)

const (
	deploymentCooldownInSlots = 1
)

type LoaderV4Write struct {
	Offset uint32
	Bytes  []byte
}

type LoaderV4Copy struct {
	DestinationOffset uint32
	SourceOffset      uint32
	Length            uint32
}

type LoaderV4SetProgramLength struct {
	NewSize uint32
}

type LoaderV4State struct {
	Slot                       uint64
	AuthorityAddrOrNextVersion solana.PublicKey
	Status                     uint64
}

func (write *LoaderV4Write) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	write.Offset, err = decoder.ReadUint32(bin.LE)
	if err != nil {
		return err
	}

	var bytesLen uint64
	bytesLen, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	write.Bytes, err = decoder.ReadBytes(int(bytesLen))
	return err
}

func (copy *LoaderV4Copy) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	copy.DestinationOffset, err = decoder.ReadUint32(bin.LE)
	if err != nil {
		return err
	}

	copy.SourceOffset, err = decoder.ReadUint32(bin.LE)
	if err != nil {
		return err
	}

	copy.Length, err = decoder.ReadUint32(bin.LE)
	return err
}

func (setProgramLen *LoaderV4SetProgramLength) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	setProgramLen.NewSize, err = decoder.ReadUint32(bin.LE)
	return err
}

func (state *LoaderV4State) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	state.Slot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	var pk []byte
	pk, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	state.AuthorityAddrOrNextVersion = solana.PublicKeyFromBytes(pk)

	state.Status, err = decoder.ReadUint64(bin.LE)
	return err
}

func (state *LoaderV4State) Marshal() []byte {
	writer := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(writer)

	encoder.WriteUint64(state.Slot, bin.LE)
	encoder.WriteBytes(state.AuthorityAddrOrNextVersion[:], false)
	encoder.WriteUint64(state.Status, bin.LE)

	return writer.Bytes()
}

func decodeLoaderV4State(data []byte) (*LoaderV4State, error) {
	var state LoaderV4State
	decoder := bin.NewBinDecoder(data)

	err := state.UnmarshalWithDecoder(decoder)
	if err != nil {
		return nil, InstrErrAccountDataTooSmall
	}

	return &state, nil
}

func setLoaderV4AcctState(acct *BorrowedAccount, state *LoaderV4State) error {
	stateSlice, err := acct.DataMutable(nil)
	if err != nil {
		return err
	}

	newStateBytes := state.Marshal()
	copy(stateSlice[:loaderV4ProgramDataOffset], newStateBytes)
	return nil
}

func loaderV4DecodeStateAndCheckProgramAcct(instrCtx *InstructionCtx, program *BorrowedAccount, authorityAddr solana.PublicKey) (*LoaderV4State, error) {
	if program.Owner() != addresses.LoaderV4Addr {
		return nil, InstrErrInvalidAccountOwner
	}

	state, err := decodeLoaderV4State(program.Data())
	if err != nil {
		return nil, err
	}

	if !program.IsWritable() {
		return nil, InstrErrInvalidArgument
	}

	isSigner, err := instrCtx.IsInstructionAccountSigner(1)
	if err != nil {
		return nil, err
	} else if !isSigner {
		return nil, InstrErrMissingRequiredSignature
	}

	if state.AuthorityAddrOrNextVersion != authorityAddr {
		return nil, InstrErrIncorrectAuthority
	}

	if state.Status == LoaderV4StatusFinalized {
		return nil, InstrErrImmutable
	}

	return state, nil
}

func LoaderV4Execute(execCtx *ExecutionCtx) error {
	if !execCtx.Features.IsActive(features.EnableLoaderV4) {
		return InstrErrUnsupportedProgramId
	}

	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	instrData := instrCtx.Data
	programId, err := instrCtx.LastProgramKey(txCtx)
	if err != nil {
		return err
	}

	// program management code path
	if programId == addresses.LoaderV4Addr {
		err = execCtx.ComputeMeter.Consume(cu.CULoaderV4ComputeUnits)
		if err != nil {
			return InstrErrComputationalBudgetExceeded
		}

		if len(instrData) < 1 {
			return InstrErrInvalidInstructionData
		}

		decoder := bin.NewBinDecoder(instrData)
		instrType := instrData[0]

		switch instrType {
		case LoaderV4InstrTypeWrite:
			{
				var writeInstr LoaderV4Write
				err = writeInstr.UnmarshalWithDecoder(decoder)
				if err != nil {
					return InstrErrInvalidInstructionData
				}

				err = LoaderV4ProcessWrite(execCtx, writeInstr.Offset, writeInstr.Bytes)
			}

		case LoaderV4InstrTypeCopy:
			{
				var copyInstr LoaderV4Copy
				err = copyInstr.UnmarshalWithDecoder(decoder)
				if err != nil {
					return err
				}

				err = LoaderV4ProcessCopy(execCtx, copyInstr.DestinationOffset, copyInstr.SourceOffset, copyInstr.Length)
			}

		case LoaderV4InstrTypeSetProgramLength:
			{
				var setProgLenInstr LoaderV4SetProgramLength
				err = setProgLenInstr.UnmarshalWithDecoder(decoder)
				if err != nil {
					return err
				}

				err = LoaderV4ProcessSetProgramLength(execCtx, setProgLenInstr.NewSize)
			}

		case LoaderV4InstrTypeDeploy:
			{
				err = LoaderV4ProcessDeploy(execCtx)
			}

		case LoaderV4InstrTypeRetract:
			{
				err = LoaderV4ProcessRetract(execCtx)
			}

		case LoaderV4InstrTypeTransferAuthority:
			{
				err = LoaderV4ProcessTransferAuthority(execCtx)
			}

		case LoaderV4InstrTypeFinalize:
			{
				err = LoaderV4ProcessFinalize(execCtx)
			}

		}
	} else { // execute bpf program code path
		var program *BorrowedAccount
		program, err = instrCtx.BorrowLastProgramAccount(txCtx)
		if err != nil {
			return err
		}

		var loadedProgram *sbpf.Program
		var programBytes []byte

		programCacheEntry, hasLoadedProgram := execCtx.SlotCtx.AccountsDb.MaybeGetProgramFromCache(program.Key())
		if hasLoadedProgram {
			if programCacheEntry.DeploymentSlot >= execCtx.SlotCtx.Slot {
				return InstrErrInvalidAccountData
			}
			loadedProgram = programCacheEntry.Program
		} else {
			programDataAcct, err := execCtx.SlotCtx.GetAccount(program.Key())
			if err != nil {
				programDataAcct, err = execCtx.SlotCtx.GetAccountFromAccountsDb(program.Key())
				if err != nil {
					return InstrErrUnsupportedProgramId
				}
			}

			state, err := decodeLoaderV4State(programDataAcct.Data)
			if err != nil {
				return InstrErrUnsupportedProgramId
			}

			if state.Status == LoaderV4StatusRetracted {
				return InstrErrUnsupportedProgramId
			}
			if state.Slot >= execCtx.SlotCtx.Slot {
				return InstrErrUnsupportedProgramId
			}

			programBytes = programDataAcct.Data[loaderV4ProgramDataOffset:]
			accountsdb.RecordProgramCacheMissSize(uint64(len(programBytes)))
		}

		syscallRegistry := sbpf.SyscallRegistry(func(u uint32) (sbpf.Syscall, bool) {
			return Syscalls(&execCtx.Features, false, u)
		})

		program.Drop()

		// two cases here: we're either executing from the program cache, so from a pre-parsed/loaded program, or from bytes if
		// the the program was not found in the cache.
		if hasLoadedProgram {
			err = executeLoadedProgram(execCtx, loadedProgram, syscallRegistry)
		} else {
			err = executeProgramFromBytes(execCtx, program.Key(), programBytes, syscallRegistry)
		}
	}

	return err
}

func LoaderV4ProcessWrite(execCtx *ExecutionCtx, offset uint32, bytes []byte) error {
	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	program, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer program.Drop()

	authorityIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(1)
	if err != nil {
		return err
	}

	authorityAddr, err := txCtx.KeyOfAccountAtIndex(authorityIdx)
	if err != nil {
		return err
	}

	var state *LoaderV4State
	state, err = loaderV4DecodeStateAndCheckProgramAcct(instrCtx, program, authorityAddr)
	if err != nil {
		return err
	}

	if state.Status != LoaderV4StatusRetracted {
		return InstrErrInvalidArgument
	}

	var programData []byte
	programData, err = program.DataMutable(execCtx.Features)
	if err != nil {
		return err
	}

	destinationOffset := uint64(offset) + loaderV4ProgramDataOffset
	if uint64(len(programData)) < (destinationOffset + uint64(len(bytes))) {
		return InstrErrAccountDataTooSmall
	}

	copy(programData[destinationOffset:], bytes)

	return nil
}

func LoaderV4ProcessCopy(execCtx *ExecutionCtx, destinationOffset uint32, sourceOffset uint32, length uint32) error {
	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	program, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer program.Drop()

	authorityIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(1)
	if err != nil {
		return err
	}

	authorityAddr, err := txCtx.KeyOfAccountAtIndex(authorityIdx)
	if err != nil {
		return err
	}

	srcProgram, err := instrCtx.BorrowInstructionAccount(txCtx, 2)
	if err != nil {
		return err
	}
	defer srcProgram.Drop()

	state, err := loaderV4DecodeStateAndCheckProgramAcct(instrCtx, program, authorityAddr)
	if err != nil {
		return err
	}

	if state.Status != LoaderV4StatusRetracted {
		return InstrErrInvalidArgument
	}

	srcOwner := srcProgram.Owner()
	var lenToAdd uint64

	if srcOwner == addresses.LoaderV4Addr {
		lenToAdd = loaderV4ProgramDataOffset
	} else if srcOwner == addresses.BpfLoaderUpgradeableAddr {
		lenToAdd = upgradeableLoaderSizeOfProgramDataMetaData
	} else if srcOwner != addresses.BpfLoaderDeprecatedAddr && srcOwner != addresses.BpfLoader2Addr {
		return InstrErrInvalidArgument
	}

	srcOffset := uint64(sourceOffset) + lenToAdd

	if uint64(len(srcProgram.Data())) < (srcOffset + uint64(length)) {
		return InstrErrAccountDataTooSmall
	}
	data := srcProgram.Data()[srcOffset : srcOffset+uint64(length)]

	var programData []byte
	programData, err = program.DataMutable(execCtx.Features)
	if err != nil {
		return err
	}

	dstOffset := uint64(destinationOffset) + loaderV4ProgramDataOffset
	if uint64(len(programData)) < (dstOffset + uint64(length)) {
		return InstrErrAccountDataTooSmall
	}

	copy(programData[dstOffset:], data)

	return nil
}

func LoaderV4ProcessSetProgramLength(execCtx *ExecutionCtx, newLen uint32) error {
	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	program, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer program.Drop()

	authorityIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(1)
	if err != nil {
		return err
	}

	authorityAddr, err := txCtx.KeyOfAccountAtIndex(authorityIdx)
	if err != nil {
		return err
	}

	isInitialization := uint64(len(program.Data())) < loaderV4ProgramDataOffset
	if isInitialization {
		if program.Owner() != addresses.LoaderV4Addr {
			return InstrErrInvalidAccountOwner
		}

		if !program.IsWritable() {
			return InstrErrInvalidArgument
		}

		isSigner, err := instrCtx.IsInstructionAccountSigner(1)
		if err != nil {
			return err
		} else if !isSigner {
			return InstrErrMissingRequiredSignature
		}
	} else {
		state, err := loaderV4DecodeStateAndCheckProgramAcct(instrCtx, program, authorityAddr)
		if err != nil {
			return err
		}

		if state.Status != LoaderV4StatusRetracted {
			return InstrErrInvalidArgument
		}
	}

	var requiredLamports uint64
	if newLen != 0 {
		rent := SysvarCache.Rent.Sysvar
		requiredLamports = max(rent.MinimumBalance(uint64(newLen)+loaderV4ProgramDataOffset), 1)
	}

	programLamports := program.Lamports()
	if programLamports < requiredLamports {
		return InstrErrInsufficientFunds
	} else if programLamports > requiredLamports {
		recipient, err := instrCtx.BorrowInstructionAccount(txCtx, 2)
		if err == nil {
			defer recipient.Drop()
			isWritable, err := instrCtx.IsInstructionAccountWritable(2)
			if err != nil {
				return err
			} else if !isWritable {
				return InstrErrInvalidArgument
			}

			lamportsToReceive := programLamports - requiredLamports
			err = program.CheckedSubLamports(lamportsToReceive, execCtx.Features)
			if err != nil {
				return err
			}

			err = recipient.CheckedAddLamports(lamportsToReceive, execCtx.Features)
			if err != nil {
				return err
			}
		} else if newLen == 0 {
			return InstrErrInvalidArgument
		}
	}

	if newLen == 0 {
		err = program.SetDataLength(0, execCtx.Features)
		if err != nil {
			return err
		}
	} else {
		err = program.SetDataLength(uint64(newLen)+loaderV4ProgramDataOffset, execCtx.Features)
		if err != nil {
			return err
		}

		if isInitialization {
			err = program.SetExecutable(execCtx.Features, true)
			if err != nil {
				return err
			}

			var newState LoaderV4State
			newState.Slot = 0
			newState.Status = LoaderV4StatusRetracted
			newState.AuthorityAddrOrNextVersion = authorityAddr
			err = setLoaderV4AcctState(program, &newState)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func LoaderV4ProcessDeploy(execCtx *ExecutionCtx) error {
	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	program, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer program.Drop()

	authorityIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(1)
	if err != nil {
		return err
	}

	authorityAddr, err := txCtx.KeyOfAccountAtIndex(authorityIdx)
	if err != nil {
		return err
	}

	state, err := loaderV4DecodeStateAndCheckProgramAcct(instrCtx, program, authorityAddr)
	if err != nil {
		return err
	}

	clock := SysvarCache.Clock.Sysvar
	currentSlot := clock.Slot

	if state.Slot != 0 && (state.Slot+deploymentCooldownInSlots) > currentSlot {
		return InstrErrInvalidArgument
	}

	if state.Status != LoaderV4StatusRetracted {
		return InstrErrInvalidArgument
	}

	if uint64(len(program.Data())) < loaderV4ProgramDataOffset {
		return InstrErrAccountDataTooSmall
	}

	programData := program.Data()[loaderV4ProgramDataOffset:]
	programObj, err := deployProgram(execCtx, programData)
	if err != nil {
		return err
	}

	state.Slot = currentSlot
	state.Status = LoaderV4StatusDeployed
	err = setLoaderV4AcctState(program, state)
	if err != nil {
		return err
	}

	entry := &accountsdb.ProgramCacheEntry{Program: programObj, DeploymentSlot: currentSlot}
	execCtx.SlotCtx.AccountsDb.AddProgramToCache(program.Key(), entry)

	return nil
}

func LoaderV4ProcessRetract(execCtx *ExecutionCtx) error {
	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	program, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer program.Drop()

	authorityIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(1)
	if err != nil {
		return err
	}

	authorityAddr, err := txCtx.KeyOfAccountAtIndex(authorityIdx)
	if err != nil {
		return err
	}

	state, err := loaderV4DecodeStateAndCheckProgramAcct(instrCtx, program, authorityAddr)
	if err != nil {
		return err
	}

	clock := SysvarCache.Clock.Sysvar
	currentSlot := clock.Slot

	if (state.Slot + deploymentCooldownInSlots) > currentSlot {
		return InstrErrInvalidArgument
	}

	if state.Status != LoaderV4StatusDeployed {
		return InstrErrInvalidArgument
	}

	state.Status = LoaderV4StatusRetracted
	err = setLoaderV4AcctState(program, state)
	if err != nil {
		return err
	}

	execCtx.SlotCtx.AccountsDb.RemoveProgramFromCache(program.Key())
	return nil
}

func LoaderV4ProcessTransferAuthority(execCtx *ExecutionCtx) error {
	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	program, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer program.Drop()

	authorityIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(1)
	if err != nil {
		return err
	}

	authorityAddr, err := txCtx.KeyOfAccountAtIndex(authorityIdx)
	if err != nil {
		return err
	}

	newAuthorityIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(2)
	if err != nil {
		return err
	}

	newAuthorityAddr, err := txCtx.KeyOfAccountAtIndex(newAuthorityIdx)
	if err != nil {
		return err
	}

	state, err := loaderV4DecodeStateAndCheckProgramAcct(instrCtx, program, authorityAddr)
	if err != nil {
		return err
	}

	isSigner, err := instrCtx.IsInstructionAccountSigner(2)
	if err != nil {
		return err
	} else if !isSigner {
		return InstrErrMissingRequiredSignature
	}

	if state.AuthorityAddrOrNextVersion == newAuthorityAddr {
		return InstrErrInvalidArgument
	}

	state.AuthorityAddrOrNextVersion = newAuthorityAddr
	return nil
}

func LoaderV4ProcessFinalize(execCtx *ExecutionCtx) error {
	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	program, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}

	authorityIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(1)
	if err != nil {
		return err
	}

	authorityAddr, err := txCtx.KeyOfAccountAtIndex(authorityIdx)
	if err != nil {
		return err
	}

	state, err := loaderV4DecodeStateAndCheckProgramAcct(instrCtx, program, authorityAddr)
	if err != nil {
		return err
	}

	if state.Status != LoaderV4StatusDeployed {
		return InstrErrInvalidArgument
	}

	program.Drop()

	nextVersion, err := instrCtx.BorrowInstructionAccount(txCtx, 2)
	if err != nil {
		return err
	}

	if nextVersion.Owner() != addresses.LoaderV4Addr {
		return InstrErrInvalidAccountOwner
	}

	stateOfNextVersion, err := decodeLoaderV4State(nextVersion.Data())
	if err != nil {
		return err
	}

	if stateOfNextVersion.AuthorityAddrOrNextVersion != authorityAddr {
		return InstrErrIncorrectAuthority
	}

	if stateOfNextVersion.Status == LoaderV4StatusFinalized {
		return InstrErrImmutable
	}

	addrOfNextVersion := nextVersion.Key()
	nextVersion.Drop()

	program, err = instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer program.Drop()

	state, err = decodeLoaderV4State(program.Data())
	if err != nil {
		return err
	}
	state.AuthorityAddrOrNextVersion = addrOfNextVersion
	state.Status = LoaderV4StatusFinalized
	err = setLoaderV4AcctState(program, state)

	return err
}
