package sealevel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/loader"
	"github.com/Overclock-Validator/mithril/pkg/util"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

const (
	UpgradeableLoaderInstrTypeInitializeBuffer = iota
	UpgradeableLoaderInstrTypeWrite
	UpgradeableLoaderInstrTypeDeployWithMaxDataLen
	UpgradeableLoaderInstrTypeUpgrade
	UpgradeableLoaderInstrTypeSetAuthority
	UpgradeableLoaderInstrTypeClose
	UpgradeableLoaderInstrTypeExtendProgram
	UpgradeableLoaderInstrTypeSetAuthorityChecked
)

const (
	UpgradeableLoaderStateTypeUninitialized = iota
	UpgradeableLoaderStateTypeBuffer
	UpgradeableLoaderStateTypeProgram
	UpgradeableLoaderStateTypeProgramData
)

// instructions
type UpgradeableLoaderInstrWrite struct {
	Offset uint32
	Bytes  []byte
}

type UpgradeableLoaderInstrDeployWithMaxDataLen struct {
	MaxDataLen uint64
}

type UpgradeableLoaderInstrExtendProgram struct {
	AdditionalBytes uint32
}

// upgradeable loader account states
type UpgradeableLoaderStateBuffer struct {
	AuthorityAddress *solana.PublicKey
}

type UpgradeableLoaderStateProgram struct {
	ProgramDataAddress solana.PublicKey
}

type UpgradeableLoaderStateProgramData struct {
	Slot                    uint64
	UpgradeAuthorityAddress *solana.PublicKey
}

type UpgradeableLoaderState struct {
	Type        uint32
	Buffer      UpgradeableLoaderStateBuffer
	Program     UpgradeableLoaderStateProgram
	ProgramData UpgradeableLoaderStateProgramData
}

const upgradeableLoaderSizeOfBufferMetaData = 37
const upgradeableLoaderSizeOfProgram = 36
const upgradeableLoaderSizeOfProgramDataMetaData = 45
const upgradeableLoaderSizeOfUninitialized = 4

func upgradeableLoaderSizeOfProgramData(programLen uint64) uint64 {
	return safemath.SaturatingAddU64(upgradeableLoaderSizeOfProgramDataMetaData, programLen)
}

func upgradeableLoaderSizeOfBuffer(programLen uint64) uint64 {
	return safemath.SaturatingAddU64(upgradeableLoaderSizeOfBufferMetaData, programLen)
}

func (write *UpgradeableLoaderInstrWrite) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	write.Offset, err = decoder.ReadUint32(bin.LE)
	if err != nil {
		return err
	}

	bytesLen, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	bytes, err := decoder.ReadBytes(int(bytesLen))
	if err != nil {
		return err
	}

	write.Bytes = bytes
	return nil
}

func (write *UpgradeableLoaderInstrWrite) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint32(UpgradeableLoaderInstrTypeWrite, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteUint32(write.Offset, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteBytes(write.Bytes, true)
	return err
}

func (deploy *UpgradeableLoaderInstrDeployWithMaxDataLen) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	deploy.MaxDataLen, err = decoder.ReadUint64(bin.LE)
	return err
}

func (deploy *UpgradeableLoaderInstrDeployWithMaxDataLen) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint32(UpgradeableLoaderInstrTypeDeployWithMaxDataLen, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(deploy.MaxDataLen, bin.LE)
	return err
}

func (extendProgram *UpgradeableLoaderInstrExtendProgram) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	extendProgram.AdditionalBytes, err = decoder.ReadUint32(bin.LE)
	return err
}

func (extendProgram *UpgradeableLoaderInstrExtendProgram) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint32(UpgradeableLoaderInstrTypeExtendProgram, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteUint32(extendProgram.AdditionalBytes, bin.LE)
	return err
}

func (buffer *UpgradeableLoaderStateBuffer) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	hasPubkey, err := ReadBool(decoder)
	if err != nil {
		return err
	}

	if hasPubkey {
		pkBytes, err := decoder.ReadBytes(solana.PublicKeyLength)
		if err != nil {
			return err
		}
		pk := solana.PublicKeyFromBytes(pkBytes)
		buffer.AuthorityAddress = pk.ToPointer()
	}
	return nil
}

func (buffer *UpgradeableLoaderStateBuffer) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	if buffer.AuthorityAddress != nil {
		err = encoder.WriteBool(true)
		if err != nil {
			return err
		}

		authAddr := *buffer.AuthorityAddress
		err = encoder.WriteBytes(authAddr.Bytes(), false)
	} else {
		err = encoder.WriteBool(false)
	}

	return err
}

func (program *UpgradeableLoaderStateProgram) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	pkBytes, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(program.ProgramDataAddress[:], pkBytes)

	return nil
}

func (program *UpgradeableLoaderStateProgram) MarshalWithEncoder(encoder *bin.Encoder) error {
	err := encoder.WriteBytes(program.ProgramDataAddress[:], false)
	return err
}

func (programData *UpgradeableLoaderStateProgramData) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	programData.Slot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	hasPubkey, err := ReadBool(decoder)
	if err != nil {
		return err
	}

	if hasPubkey {
		pkBytes, err := decoder.ReadBytes(solana.PublicKeyLength)
		if err != nil {
			return err
		}
		pk := solana.PublicKeyFromBytes(pkBytes)
		programData.UpgradeAuthorityAddress = pk.ToPointer()
	}

	return nil
}

func (programData *UpgradeableLoaderStateProgramData) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error
	err = encoder.WriteUint64(programData.Slot, bin.LE)
	if err != nil {
		return err
	}

	if programData.UpgradeAuthorityAddress != nil {
		err = encoder.WriteBool(true)
		if err != nil {
			return err
		}
		upgradeAuthAddr := *programData.UpgradeAuthorityAddress
		err = encoder.WriteBytes(upgradeAuthAddr.Bytes(), false)
	} else {
		err = encoder.WriteBool(false)
	}

	return err
}

func (state *UpgradeableLoaderState) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	state.Type, err = decoder.ReadUint32(bin.LE)
	if err != nil {
		return err
	}

	switch state.Type {
	case UpgradeableLoaderStateTypeUninitialized:
		{
			// nothing to deserialize
		}

	case UpgradeableLoaderStateTypeBuffer:
		{
			err = state.Buffer.UnmarshalWithDecoder(decoder)
		}

	case UpgradeableLoaderStateTypeProgram:
		{
			err = state.Program.UnmarshalWithDecoder(decoder)
		}

	case UpgradeableLoaderStateTypeProgramData:
		{
			err = state.ProgramData.UnmarshalWithDecoder(decoder)
		}

	default:
		{
			err = InstrErrInvalidAccountData
		}
	}

	return err
}

func (state *UpgradeableLoaderState) MarshalWithEncoder(encoder *bin.Encoder) error {
	err := encoder.WriteUint32(state.Type, bin.LE)
	if err != nil {
		return err
	}

	switch state.Type {
	case UpgradeableLoaderStateTypeUninitialized:
		{
			// nothing to serialize
		}

	case UpgradeableLoaderStateTypeBuffer:
		{
			err = state.Buffer.MarshalWithEncoder(encoder)
		}

	case UpgradeableLoaderStateTypeProgram:
		{
			err = state.Program.MarshalWithEncoder(encoder)
		}

	case UpgradeableLoaderStateTypeProgramData:
		{
			err = state.ProgramData.MarshalWithEncoder(encoder)
		}

	default:
		{
			panic("attempting to serialize up invalid upgradeable loader state - programming error")
		}
	}
	return err
}

func UnmarshalUpgradeableLoaderState(data []byte) (*UpgradeableLoaderState, error) {
	state := new(UpgradeableLoaderState)
	decoder := bin.NewBinDecoder(data)

	err := state.UnmarshalWithDecoder(decoder)
	if err != nil {
		return nil, InstrErrInvalidAccountData
	} else {
		return state, nil
	}
}

func marshalUpgradeableLoaderState(state *UpgradeableLoaderState) ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buffer)

	err := state.MarshalWithEncoder(encoder)
	if err != nil {
		return nil, err
	} else {
		return buffer.Bytes(), nil
	}
}

func setUpgradeableLoaderAccountState(acct *BorrowedAccount, state *UpgradeableLoaderState, f features.Features) error {
	acctStateBytes, err := marshalUpgradeableLoaderState(state)
	if err != nil {
		return err
	}

	newStateBytes := make([]byte, len(acct.Data()))
	copy(newStateBytes, acct.Data())
	copy(newStateBytes, acctStateBytes)

	err = acct.SetState(f, newStateBytes)
	return err
}

func writeProgramData(execCtx *ExecutionCtx, programDataOffset uint64, bytes []byte) error {
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

	writeEnd := safemath.SaturatingAddU64(programDataOffset, uint64(len(bytes)))
	if uint64(len(program.Data())) < writeEnd {
		//mlog.Log.Debugf("write overflow. acct data len = %d, writeOffset = %d", len(program.Data()), writeEnd)
		return InstrErrAccountDataTooSmall
	}

	data, err := program.DataMutable(execCtx.Features)
	if err != nil {
		return err
	}

	copy(data[programDataOffset:writeEnd], bytes)
	return nil
}

func deployProgram(execCtx *ExecutionCtx, programData []byte) (*sbpf.Program, error) {
	syscallRegistry := sbpf.SyscallRegistry(func(u uint32) (sbpf.Syscall, bool) {
		return Syscalls(&execCtx.Features, true, u)
	})

	loader, err := loader.NewLoaderWithSyscalls(programData, syscallRegistry, true, &execCtx.Features)
	if err != nil {
		//mlog.Log.Debugf("failed to create loader: %s", err)
		return nil, err
	}

	program, err := loader.Load()
	if err != nil {
		//mlog.Log.Debugf("failed to load program: %s", err)
		return nil, err
	}

	err = program.Verify()
	if err != nil {
		mlog.Log.Errorf("failed to verify program: %s", err)
		return nil, err
	}

	return program, nil
}

const (
	kibibyte   uint64 = 1024
	pageSizeKB uint64 = 32
)

func calculateHeapCost(heapSize uint32, heapCost uint64) uint64 {
	roundedHeapSize := uint64(heapSize)
	roundedHeapSize += pageSizeKB*kibibyte - 1

	divisor := pageSizeKB * kibibyte
	if divisor == 0 {
		panic("programming error - PAGE_SIZE_KB * KIBIBYTE must be > 0. should be impossible.")
	}

	// any heap size <= default heap size has zero cost
	roundedHeapSize = roundedHeapSize / divisor
	if roundedHeapSize == 0 {
		return 0
	}

	roundedHeapSize -= 1
	return roundedHeapSize * heapCost
}

const MaxInstructionAccounts = 255
const MaxPermittedDataIncrease = 1024 * 10

type serializeAcct struct {
	isDuplicate bool
	indexOfAcct uint64
	acct        *BorrowedAccount
}

type serializedAcctMetadata struct {
	originalDataLen uint64
	vmDataAddr      uint64
	vmKeyAddr       uint64
	vmLamportsAddr  uint64
	vmOwnerAddr     uint64
}

func serializeParametersAligned(execCtx *ExecutionCtx) ([]byte, []uint64, error) {
	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return nil, nil, err
	}

	numIxAccts := instrCtx.NumberOfInstructionAccounts()
	if numIxAccts > MaxInstructionAccounts {
		return nil, nil, InstrErrMaxAccountsExceeded
	}

	programAcct, err := instrCtx.BorrowLastProgramAccount(txCtx)
	if err != nil {
		return nil, nil, err
	}
	programId := programAcct.Key()
	programAcct.Drop()

	instrData := instrCtx.Data

	accts := make([]serializeAcct, instrCtx.NumberOfInstructionAccounts())
	defer func() {
		for _, sa := range accts {
			if sa.acct != nil {
				sa.acct.Drop()
			}
		}
	}()
	for instrAcctIdx := uint64(0); instrAcctIdx < instrCtx.NumberOfInstructionAccounts(); instrAcctIdx++ {
		isDupe, idxInCallee, err := instrCtx.IsInstructionAccountDuplicate(instrAcctIdx)
		if err != nil {
			return nil, nil, err
		}
		if isDupe {
			accts[int(instrAcctIdx)] = serializeAcct{isDuplicate: true, indexOfAcct: idxInCallee}
		} else {
			acct, err := instrCtx.BorrowInstructionAccount(txCtx, instrAcctIdx)
			if err != nil {
				return nil, nil, err
			}

			accts[int(instrAcctIdx)] = serializeAcct{indexOfAcct: instrAcctIdx, acct: acct}
		}
	}

	size := uint64(8)

	for _, acct := range accts {
		size += 1 // dup

		if acct.isDuplicate {
			size += 7 // padding to 64-bit aligned
		} else {
			dataLen := uint64(len(acct.acct.Data()))
			alignmentMask := uint64(7) // (alignment - 1)
			alignedDataLen := dataLen + (-dataLen & alignmentMask)
			size += 1                      // is_signer
			size += 1                      // is_writable
			size += 1                      // executable
			size += 4                      // original_data_len
			size += solana.PublicKeyLength // key
			size += solana.PublicKeyLength // owner
			size += 8                      // lamports
			size += 8                      // data len
			size += MaxPermittedDataIncrease
			size += 8 // rent epoch
			size += alignedDataLen
		}
	}

	size += 8 + uint64(len(instrData)) // data len
	size += solana.PublicKeyLength     // program id

	var serializedData []byte
	if execCtx.SlotCtx.SerializedParameterArena != nil {
		arenaData, _ := execCtx.SlotCtx.SerializedParameterArena.AllocN(size)
		serializedData = arenaData[:0]
	} else {
		serializedData = make([]byte, 0, size) // No arena configured
	}
	serializedData = binary.LittleEndian.AppendUint64(serializedData, uint64(len(accts)))

	preLens := make([]uint64, len(accts))
	for i, acct := range accts {
		borrowedAcct := acct.acct
		l := len(serializedData)
		if acct.isDuplicate { // duplicate
			serializedData = serializedData[:l+8]
			position := acct.indexOfAcct
			serializedData[l] = byte(position)
			preLens[i] = preLens[position]
		} else { // not a duplicate
			dataLen := uint64(len(borrowedAcct.Data()))
			numPaddingBytes := ReallocSpace + util.AlignUp(dataLen, 8) - dataLen
			serializedData = serializedData[:l+
				8+ /*not duplicate, signer, writable, executable, 4 bytes padding*/
				32+ /*account pubkey*/
				32+ /*owner pubkey*/
				8+ /*lamports*/
				8+ /*acct data len*/
				len(borrowedAcct.Data())+ /*acct data*/
				int(numPaddingBytes)+
				8 /*rent epoch*/]
			serializedData[l] = 0xff
			if borrowedAcct.IsSigner() {
				serializedData[l+1] = 1
			} else {
				serializedData[l+1] = 0
			}

			if borrowedAcct.IsWritable() {
				serializedData[l+2] = 1
			} else {
				serializedData[l+2] = 0
			}

			if borrowedAcct.IsExecutable() {
				serializedData[l+3] = 1
			} else {
				serializedData[l+3] = 0
			}

			// original_data_len, u32, filled with 0's
			clear(serializedData[l+4 : l+8])

			{
				acctKey := [32]byte(borrowedAcct.Key())
				copy(serializedData[l+8:l+40], acctKey[:])
				owner := [32]byte(borrowedAcct.Owner())
				copy(serializedData[l+40:l+72], owner[:])
			}

			// lamports
			binary.LittleEndian.PutUint64(serializedData[l+72:l+80], borrowedAcct.Lamports())

			// acct data len
			preLens[i] = dataLen
			binary.LittleEndian.PutUint64(serializedData[l+80:l+88], dataLen)

			// data in account
			copy(serializedData[l+88:l+88+len(borrowedAcct.Data())], borrowedAcct.Data())

			// zero the padding
			paddingStart := l + 88 + len(borrowedAcct.Data())
			paddingEnd := l + 88 + len(borrowedAcct.Data()) + int(numPaddingBytes)
			clear(serializedData[paddingStart:paddingEnd])

			// rent epoch
			var rentEpoch uint64
			if execCtx.Features.IsActive(features.MaskOutRentEpochInVmSerialization) {
				rentEpoch = math.MaxUint64
			} else {
				rentEpoch = borrowedAcct.RentEpoch()
			}
			binary.LittleEndian.PutUint64(serializedData[len(serializedData)-8:], rentEpoch)
		}
	}

	l := len(serializedData)
	serializedData = serializedData[:len(serializedData)+
		8+ /*instr data len*/
		len(instrData)+ /*instr data*/
		32 /*program key*/]

	binary.LittleEndian.PutUint64(serializedData[l:l+8], uint64(len(instrData)))
	copy(serializedData[l+8:l+8+len(instrData)], instrData)
	copy(serializedData[len(serializedData)-32:], programId[:])

	// sanity check for expected len vs. serialized data size
	if uint64(len(serializedData)) != size {
		panic(fmt.Sprintf("mismatch between serialized data and expected length: len(serializedData) = %d, expected size = %d", uint64(len(serializedData)), size))
	}

	return serializedData, preLens, nil
}

func deserializeParametersAligned(execCtx *ExecutionCtx, parameterBytes []byte, preLens []uint64) error {
	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	var off uint64

	off += 8 // number of accounts
	for instrAcctIdx := uint64(0); instrAcctIdx < instrCtx.NumberOfInstructionAccounts(); instrAcctIdx++ {
		preLen := preLens[instrAcctIdx]

		isDupe, _, err := instrCtx.IsInstructionAccountDuplicate(instrAcctIdx)
		if err != nil {
			return err
		}
		off += 1 // position
		if isDupe {
			off += 7 // padding to 64-bit aligned
		} else {
			borrowedAcct, err := instrCtx.BorrowInstructionAccount(txCtx, instrAcctIdx)
			if err != nil {
				return err
			}
			defer borrowedAcct.Drop()

			off += 1                      // is_signer
			off += 1                      // is_writable
			off += 1                      // executable
			off += 4                      // original_data_len
			off += solana.PublicKeyLength // key

			if uint64(len(parameterBytes)) < (off + solana.PublicKeyLength) {
				return InstrErrInvalidArgument
			}

			owner := parameterBytes[off : off+solana.PublicKeyLength]
			off += solana.PublicKeyLength // owner

			if uint64(len(parameterBytes)) < (off + 8) {
				return InstrErrInvalidArgument
			}
			lamports := binary.LittleEndian.Uint64(parameterBytes[off:])

			if borrowedAcct.Lamports() != lamports {
				err = borrowedAcct.SetLamports(lamports, execCtx.Features)
				if err != nil {
					return err
				}
			}
			off += 8 // lamports

			if uint64(len(parameterBytes)) < (off + 8) {
				return InstrErrInvalidArgument
			}
			postLen := binary.LittleEndian.Uint64(parameterBytes[off:])
			off += 8 // data length

			if safemath.SaturatingSubU64(postLen, preLen) > MaxPermittedDataIncrease ||
				postLen > MaxPermittedDataLength {
				//mlog.Log.Debugf("preLen = %d, postLen = %d, max increase = %d", preLen, postLen, MaxPermittedDataIncrease)
				return InstrErrInvalidRealloc
			}

			//alignmentMask := uint64(7) // (alignment - 1)
			alignmentOffset := util.AlignUp(preLen, 8) - preLen

			if uint64(len(parameterBytes)) < (off + postLen) {
				return InstrErrInvalidArgument
			}
			data := parameterBytes[off : off+postLen]

			resizeErr := borrowedAcct.CanDataBeResized(postLen)
			changedErr := borrowedAcct.DataCanBeChanged(execCtx.Features)

			if resizeErr != nil || changedErr != nil {
				acctBytes := borrowedAcct.Data()
				if !bytes.Equal(acctBytes, data) {
					return fmt.Errorf("data cannot be changed, but did anyway")
				}
			} else {
				err = borrowedAcct.SetData(execCtx.Features, data)
				if err != nil {
					return err
				}
			}

			off += preLen

			off += MaxPermittedDataIncrease
			off += alignmentOffset
			off += 8 // rent epoch

			ownerPk := solana.PublicKeyFromBytes(owner)
			if borrowedAcct.Owner() != ownerPk {
				err = borrowedAcct.SetOwner(execCtx.Features, ownerPk)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func serializeParametersUnaligned(execCtx *ExecutionCtx) ([]byte, []uint64, error) {
	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return nil, nil, err
	}

	numIxAccts := instrCtx.NumberOfInstructionAccounts()
	if numIxAccts > MaxInstructionAccounts {
		return nil, nil, InstrErrMaxAccountsExceeded
	}

	programAcct, err := instrCtx.BorrowLastProgramAccount(txCtx)
	if err != nil {
		return nil, nil, err
	}
	programId := programAcct.Key()
	programAcct.Drop()

	instrData := instrCtx.Data
	var preLens []uint64

	accts := make([]serializeAcct, 0, instrCtx.NumberOfInstructionAccounts())
	for instrAcctIdx := uint64(0); instrAcctIdx < instrCtx.NumberOfInstructionAccounts(); instrAcctIdx++ {
		isDupe, idxInCallee, err := instrCtx.IsInstructionAccountDuplicate(instrAcctIdx)
		if err != nil {
			return nil, nil, err
		}
		if isDupe {
			sa := serializeAcct{isDuplicate: true, indexOfAcct: idxInCallee}
			accts = append(accts, sa)
		} else {
			acct, err := instrCtx.BorrowInstructionAccount(txCtx, instrAcctIdx)
			if err != nil {
				return nil, nil, err
			}
			defer acct.Drop()

			sa := serializeAcct{indexOfAcct: instrAcctIdx, acct: acct}
			accts = append(accts, sa)
		}
	}

	size := uint64(8)

	for _, acct := range accts {
		size += 1 // dup

		if !acct.isDuplicate {
			dataLen := uint64(len(acct.acct.Data()))

			size += 1                      // is_signer
			size += 1                      // is_writable
			size += solana.PublicKeyLength // key
			size += 8                      // lamports
			size += 8                      // data len
			size += solana.PublicKeyLength // owner
			size += 1                      // executable
			size += 8                      // rent epoch
			size += dataLen
		}
	}

	size += 8 + uint64(len(instrData)) // data len
	size += solana.PublicKeyLength     // program id

	var serializedData []byte
	if execCtx.SlotCtx.SerializedParameterArena != nil {
		arenaData, _ := execCtx.SlotCtx.SerializedParameterArena.AllocN(size)
		serializedData = arenaData[:0] // Use arena slice with zero length but full capacity
	} else {
		serializedData = make([]byte, 0, size) // No arena configured
	}
	serializedData = binary.LittleEndian.AppendUint64(serializedData, uint64(len(accts)))

	for _, acct := range accts {
		borrowedAcct := acct.acct
		if acct.isDuplicate { // duplicate
			position := acct.indexOfAcct
			serializedData = append(serializedData, byte(position))
			preLens = append(preLens, preLens[position])
		} else { // not a duplicate
			serializedData = append(serializedData, 0xff)

			if borrowedAcct.IsSigner() {
				serializedData = append(serializedData, 1)
			} else {
				serializedData = append(serializedData, 0)
			}

			if borrowedAcct.IsWritable() {
				serializedData = append(serializedData, 1)
			} else {
				serializedData = append(serializedData, 0)
			}

			// acct key
			acctKey := [32]byte(borrowedAcct.Key())
			acctKeySlice := acctKey[:]
			serializedData = append(serializedData, acctKeySlice...)

			// lamports
			serializedData = binary.LittleEndian.AppendUint64(serializedData, borrowedAcct.Lamports())

			// acct data len
			dataLen := uint64(len(borrowedAcct.Data()))
			preLens = append(preLens, dataLen)
			serializedData = binary.LittleEndian.AppendUint64(serializedData, dataLen)

			// data in account
			serializedData = append(serializedData, borrowedAcct.Data()...)

			// owner
			owner := [32]byte(borrowedAcct.Owner())
			ownerSlice := owner[:]
			serializedData = append(serializedData, ownerSlice...)

			if borrowedAcct.IsExecutable() {
				serializedData = append(serializedData, 1)
			} else {
				serializedData = append(serializedData, 0)
			}

			// rent epoch
			var rentEpoch uint64
			if execCtx.Features.IsActive(features.MaskOutRentEpochInVmSerialization) {
				rentEpoch = math.MaxUint64
			} else {
				rentEpoch = borrowedAcct.RentEpoch()
			}
			serializedData = binary.LittleEndian.AppendUint64(serializedData, rentEpoch)
		}
	}

	// instr data len
	serializedData = binary.LittleEndian.AppendUint64(serializedData, uint64(len(instrData)))

	// instr data
	serializedData = append(serializedData, instrData...)

	// program id
	programIdSlice := programId[:]
	serializedData = append(serializedData, programIdSlice...)

	// sanity check for expected len vs. serialized data size
	if uint64(len(serializedData)) != size {
		panic("mismatch between serialized data and expected length")
	}

	return serializedData, preLens, nil
}

func deserializeParametersUnaligned(execCtx *ExecutionCtx, parameterBytes []byte, preLens []uint64) error {
	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	var off uint64

	off += 8 // number of accounts
	for instrAcctIdx := uint64(0); instrAcctIdx < instrCtx.NumberOfInstructionAccounts(); instrAcctIdx++ {
		preLen := preLens[instrAcctIdx]

		isDupe, _, err := instrCtx.IsInstructionAccountDuplicate(instrAcctIdx)
		if err != nil {
			return err
		}
		off += 1 // position
		if !isDupe {
			borrowedAcct, err := instrCtx.BorrowInstructionAccount(txCtx, instrAcctIdx)
			if err != nil {
				return err
			}
			defer borrowedAcct.Drop()

			off += 1                      // is_signer
			off += 1                      // is_writable
			off += solana.PublicKeyLength // key

			lamports := binary.LittleEndian.Uint64(parameterBytes[off:])

			if borrowedAcct.Lamports() != lamports {
				err = borrowedAcct.SetLamports(lamports, execCtx.Features)
				if err != nil {
					return err
				}
			}
			off += 8 // lamports

			off += 8 // data length

			if uint64(len(parameterBytes)) < (off + preLen) {
				return InstrErrInvalidArgument
			}
			data := parameterBytes[off : off+preLen]

			resizeErr := borrowedAcct.CanDataBeResized(uint64(len(data)))
			changedErr := borrowedAcct.DataCanBeChanged(execCtx.Features)

			if resizeErr != nil || changedErr != nil {
				acctBytes := borrowedAcct.Data()
				if len(acctBytes) != len(data) {
					return fmt.Errorf("data cannot be changed, but did anyway")
				}
				for count := range acctBytes {
					if acctBytes[count] != data[count] {
						return fmt.Errorf("data cannot be changed, but did anyway")
					}
				}
			} else {
				err = borrowedAcct.SetData(execCtx.Features, data)
				if err != nil {
					return err
				}
			}

			off += preLen

			off += solana.PublicKeyLength // owner
			off += 1                      // executable
			off += 8                      // rent epoch
		}
	}

	return nil
}

func executeLoadedProgram(execCtx *ExecutionCtx, program *sbpf.Program, syscallRegistry sbpf.SyscallRegistry) error {
	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	programAcct, err := instrCtx.BorrowLastProgramAccount(txCtx)
	if err != nil {
		return err
	}
	programId := programAcct.Key()
	isLoaderDeprecated := programAcct.Owner() == a.BpfLoaderDeprecatedAddr

	programAcct.Drop()

	heapSize := execCtx.TransactionContext.ComputeBudgetLimits.UpdatedHeapBytes
	heapCostResult := calculateHeapCost(heapSize, cu.CUHeapCostDefault)
	err = execCtx.ComputeMeter.Consume(heapCostResult)
	if err != nil {
		return err
	}

	var parameterBytes []byte
	var preLens []uint64

	if isLoaderDeprecated {
		parameterBytes, preLens, err = serializeParametersUnaligned(execCtx)
		if err != nil {
			return err
		}
	} else {
		parameterBytes, preLens, err = serializeParametersAligned(execCtx)
		if err != nil {
			return err
		}
	}

	opts := &sbpf.VMOpts{
		HeapMax:      int(heapSize),
		Input:        parameterBytes,
		Syscalls:     syscallRegistry,
		MaxCU:        int(execCtx.ComputeMeter.Remaining()),
		ComputeMeter: &execCtx.ComputeMeter,
		Context:      execCtx,
		TxSignature:  execCtx.TransactionContext.Signature,
		ProgramId:    programId,
	}

	start := time.Now()
	interpreter := sbpf.NewInterpreter(program, opts)
	defer interpreter.Finish()
	metrics.GlobalBlockReplay.SbpfInterpreterNew.AddTimingSince(start)
	start = time.Now()
	ret, _, runErr := interpreter.Run()
	metrics.GlobalBlockReplay.SbpfInterpreterRun.AddTimingSince(start)

	if runErr != nil {
		//mlog.Log.Debugf("program execution result: %s", runErr)
	} else if ret != 0 {
		runErr = fmt.Errorf("program execution (%s) returned failure: %d", programId, ret)
		//mlog.Log.Debugf("program execution (%s) returned failure: %d", programId, ret)
	} else {
		//mlog.Log.Debugf("program execution (%s) returned success", programId)
	}

	/*
		_, returnData := execCtx.TransactionContext.ReturnData()
		if len(returnData) != 0 {
			base64.StdEncoding.EncodeToString(returnData)
			mlog.Log.Debugf("Program return %s %s", returnedDataProgId, encodedStr)
		}*/

	// deserialize data
	if runErr == nil {
		if isLoaderDeprecated {
			err = deserializeParametersUnaligned(execCtx, parameterBytes, preLens)
			if err != nil {
				//mlog.Log.Debugf("failed to deserialize (unaligned), %s", err)
				return InstrErrInvalidArgument
			}
		} else {
			err = deserializeParametersAligned(execCtx, parameterBytes, preLens)
			if err != nil {
				//mlog.Log.Debugf("failed to deserialize (aligned), %s", err)
				return InstrErrInvalidArgument
			}
		}
	}

	return runErr
}

func executeProgramFromBytes(execCtx *ExecutionCtx, programAddr solana.PublicKey, programData []byte, syscallRegistry sbpf.SyscallRegistry) error {
	start := time.Now()
	loader, err := loader.NewLoaderWithSyscalls(programData, syscallRegistry, false, &execCtx.Features)
	if err != nil {
		return err
	}

	program, err := loader.Load()
	if err != nil {
		return err
	}

	entry := &accountsdb.ProgramCacheEntry{Program: program}
	execCtx.SlotCtx.AccountsDb.AddProgramToCache(programAddr, entry)

	metrics.GlobalBlockReplay.AddProgramToCache.AddTimingSince(start)

	return executeLoadedProgram(execCtx, program, syscallRegistry)
}

func BpfLoaderProgramExecute(execCtx *ExecutionCtx) error {
	//mlog.Log.Debugf("BpfLoaderProgramExecute")

	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	programAcct, err := instrCtx.BorrowLastProgramAccount(txCtx)
	if err != nil {
		return err
	}
	defer programAcct.Drop()

	if programAcct.Owner() == a.NativeLoaderAddr {
		programId, err := instrCtx.LastProgramKey(txCtx)
		if err != nil {
			return err
		}
		if programId == a.BpfLoaderUpgradeableAddr {
			err = execCtx.ComputeMeter.Consume(cu.CUUpgradeableLoaderComputeUnits)
			if err != nil {
				return err
			}
			err = ProcessUpgradeableLoaderInstruction(execCtx)
			return err
		} else if programId == a.BpfLoader2Addr {
			err = execCtx.ComputeMeter.Consume(cu.CUDefaultLoaderComputeUnits)
			if err != nil {
				return err
			}
			//mlog.Log.Debugf("BPF loader 2 mgmt no longer supported")
			return InstrErrUnsupportedProgramId
		} else if programId == a.BpfLoaderDeprecatedAddr {
			err = execCtx.ComputeMeter.Consume(cu.CUDeprecatedLoaderComputeUnits)
			if err != nil {
				return err
			}
			return InstrErrUnsupportedProgramId
		} else {
			return InstrErrUnsupportedProgramId
		}
	}

	if !programAcct.IsExecutable() {
		//mlog.Log.Debugf("program %s is not executable", programAcct)
		return InstrErrUnsupportedProgramId
	}

	var programBytes []byte
	var loadedProgram *sbpf.Program
	var hasLoadedProgram bool
	var programAcctKey solana.PublicKey

	programOwner := programAcct.Owner()

	if programOwner == a.BpfLoader2Addr || programOwner == a.BpfLoaderDeprecatedAddr {
		var programCacheEntry *accountsdb.ProgramCacheEntry
		programCacheEntry, hasLoadedProgram = execCtx.SlotCtx.AccountsDb.MaybeGetProgramFromCache(programAcct.Key())
		if hasLoadedProgram {
			programAcctKey = programAcct.Key()
			loadedProgram = programCacheEntry.Program
		} else { // program is not cached
			if len(programAcct.Data()) == 0 {
				var paTmp *accounts.Account
				paTmp, err = execCtx.SlotCtx.GetAccount(programAcct.Key())

				if err != nil {
					paTmp, err = execCtx.SlotCtx.GetAccountFromAccountsDb(programAcct.Key())
					if err != nil {
						//mlog.Log.Debugf("unable to get account %s from accountsdb", programAcct.Key())
						return InstrErrUnsupportedProgramId
					}
				}
				programBytes = paTmp.Data
			} else {
				programBytes = programAcct.Data()
			}
			programAcctKey = programAcct.Key()
			accountsdb.RecordProgramCacheMissSize(uint64(len(programBytes)))
		}
	} else if programOwner == a.BpfLoaderUpgradeableAddr {
		var programAcctState *UpgradeableLoaderState

		if len(programAcct.Data()) == 0 {
			start := time.Now()
			var paTmp *accounts.Account
			paTmp, err = execCtx.SlotCtx.GetAccount(programAcct.Key())

			if err != nil {
				paTmp, err = execCtx.SlotCtx.GetAccountFromAccountsDb(programAcct.Key())
				if err != nil {
					return InstrErrUnsupportedProgramId
				}
			}
			programAcctState, err = UnmarshalUpgradeableLoaderState(paTmp.Data)
			if err != nil {
				return err
			}
			metrics.GlobalBlockReplay.GetProgramAccount.AddTimingSince(start)
		} else {
			programAcctState, err = UnmarshalUpgradeableLoaderState(programAcct.Data())
			if err != nil {
				return err
			}
		}

		start := time.Now()
		var programCacheEntry *accountsdb.ProgramCacheEntry
		programCacheEntry, hasLoadedProgram = execCtx.SlotCtx.AccountsDb.MaybeGetProgramFromCache(programAcctState.Program.ProgramDataAddress)
		if hasLoadedProgram {
			if programCacheEntry.DeploymentSlot >= execCtx.SlotCtx.Slot {
				return InstrErrInvalidAccountData
			}
			programAcctKey = programAcctState.Program.ProgramDataAddress
			loadedProgram = programCacheEntry.Program
			metrics.GlobalBlockReplay.GetProgramDataCached.AddTimingSince(start)
		} else { // program is not cached
			programDataAcct, err := execCtx.SlotCtx.GetAccount(programAcctState.Program.ProgramDataAddress)
			if err != nil {
				programDataAcct, err = execCtx.SlotCtx.GetAccountFromAccountsDb(programAcctState.Program.ProgramDataAddress)
				if err != nil {
					return InstrErrUnsupportedProgramId
				}
				metrics.GlobalBlockReplay.GetProgramDataUncachedAccountsDb.AddTimingSince(start)
			} else {
				metrics.GlobalBlockReplay.GetProgramDataUncachedAccounts.AddTimingSince(start)
			}

			start = time.Now()
			programDataAcctState, err := UnmarshalUpgradeableLoaderState(programDataAcct.Data)
			if err != nil {
				return err
			}

			if programDataAcctState.Type == UpgradeableLoaderStateTypeUninitialized {
				return InstrErrInvalidAccountData
			}

			programDataSlot := programDataAcctState.ProgramData.Slot
			if programDataSlot >= execCtx.SlotCtx.Slot {
				return InstrErrInvalidAccountData
			}

			programAcctKey = programAcctState.Program.ProgramDataAddress
			programBytes = programDataAcct.Data[upgradeableLoaderSizeOfProgramDataMetaData:]
			metrics.GlobalBlockReplay.GetProgramDataUncachedMarshal.AddTimingSince(start)
			accountsdb.RecordProgramCacheMissSize(uint64(len(programBytes)))
		}
	} else {
		return InstrErrUnsupportedProgramId
	}

	programAcct.Drop()

	syscallRegistry := sbpf.SyscallRegistry(func(u uint32) (sbpf.Syscall, bool) {
		return Syscalls(&execCtx.Features, false, u)
	})

	// two cases here: we're either executing from the program cache, so from a pre-parsed/loaded program, or from bytes if
	// the the program was not found in the cache.
	if hasLoadedProgram {
		err = executeLoadedProgram(execCtx, loadedProgram, syscallRegistry)
	} else {
		err = executeProgramFromBytes(execCtx, programAcctKey, programBytes, syscallRegistry)
	}

	return err
}

func UpgradeableLoaderInitializeBuffer(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx) error {
	//mlog.Log.Debugf("InitializeBuffer instr")
	err := instrCtx.CheckNumOfInstructionAccounts(2)
	if err != nil {
		return err
	}

	buffer, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer buffer.Drop()

	state, err := UnmarshalUpgradeableLoaderState(buffer.Data())
	if err != nil {
		return err
	}

	if state.Type != UpgradeableLoaderStateTypeUninitialized {
		//mlog.Log.Debugf("Buffer account already initialized")
		return InstrErrAccountAlreadyInitialized
	}

	authorityKeyIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(1)
	if err != nil {
		return err
	}

	authorityKey, err := txCtx.KeyOfAccountAtIndex(authorityKeyIdx)
	if err != nil {
		return err
	}

	state.Type = UpgradeableLoaderStateTypeBuffer
	state.Buffer.AuthorityAddress = authorityKey.ToPointer()

	err = setUpgradeableLoaderAccountState(buffer, state, execCtx.Features)

	return err
}

func UpgradeableLoaderWrite(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx, write UpgradeableLoaderInstrWrite) error {
	//mlog.Log.Debugf("Write instr")

	err := instrCtx.CheckNumOfInstructionAccounts(2)
	if err != nil {
		return err
	}

	buffer, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer buffer.Drop()

	state, err := UnmarshalUpgradeableLoaderState(buffer.Data())
	if err != nil {
		return err
	}

	if state.Type == UpgradeableLoaderStateTypeBuffer {
		if state.Buffer.AuthorityAddress == nil {
			//mlog.Log.Debugf("Buffer is immutable")
			return InstrErrImmutable
		}

		authorityKeyIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(1)
		if err != nil {
			return err
		}
		authorityKey, err := txCtx.KeyOfAccountAtIndex(authorityKeyIdx)
		if err != nil {
			return err
		}
		if *state.Buffer.AuthorityAddress != authorityKey {
			//mlog.Log.Debugf("Incorrect buffer authority provided")
			return InstrErrIncorrectAuthority
		}

		isSigner, err := instrCtx.IsInstructionAccountSigner(1)
		if err != nil {
			//mlog.Log.Debugf("Buffer authority did not sign")
			return err
		}

		if !isSigner {
			return InstrErrMissingRequiredSignature
		}
	} else {
		//mlog.Log.Debugf("Invalid buffer account")
		return InstrErrInvalidAccountData
	}

	buffer.Drop()

	err = writeProgramData(execCtx, upgradeableLoaderSizeOfBufferMetaData+uint64(write.Offset), write.Bytes)
	return err
}

func UpgradeableLoaderDeployWithMaxDataLen(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx, deploy UpgradeableLoaderInstrDeployWithMaxDataLen) error {
	err := instrCtx.CheckNumOfInstructionAccounts(4)
	if err != nil {
		return err
	}

	payerKeyIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(0)
	if err != nil {
		return err
	}
	payerKey, err := txCtx.KeyOfAccountAtIndex(payerKeyIdx)
	if err != nil {
		return err
	}

	programDataKeyIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(1)
	if err != nil {
		return err
	}
	programDataKey, err := txCtx.KeyOfAccountAtIndex(programDataKeyIdx)
	if err != nil {
		return err
	}

	err = checkAcctForRentSysvar(txCtx, instrCtx, 4)
	if err != nil {
		return err
	}

	rent, err := ReadRentSysvar(execCtx)
	if err != nil {
		return err
	}

	err = checkAcctForClockSysvar(txCtx, instrCtx, 5)
	if err != nil {
		return err
	}

	clock, err := ReadClockSysvar(execCtx)
	if err != nil {
		return err
	}

	err = instrCtx.CheckNumOfInstructionAccounts(8)
	if err != nil {
		return err
	}

	var authorityKey *solana.PublicKey
	authorityIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(7)
	if err == nil {
		k, err := txCtx.KeyOfAccountAtIndex(authorityIdx)
		if err != nil {
			return err
		}
		authorityKey = k.ToPointer()
	}

	// validate program account
	program, err := instrCtx.BorrowInstructionAccount(txCtx, 2)
	if err != nil {
		return err
	}
	defer program.Drop()

	programAcctState, err := UnmarshalUpgradeableLoaderState(program.Data())
	if err != nil {
		return err
	}

	if programAcctState.Type != UpgradeableLoaderStateTypeUninitialized {
		return InstrErrAccountAlreadyInitialized
	}

	if len(program.Data()) < upgradeableLoaderSizeOfProgram {
		return InstrErrAccountDataTooSmall
	}

	if program.Lamports() < rent.MinimumBalance(uint64(len(program.Data()))) {
		return InstrErrExecutableAccountNotRentExempt
	}

	newProgramId := program.Key()
	program.Drop()

	// validate buffer account
	buffer, err := instrCtx.BorrowInstructionAccount(txCtx, 3)
	if err != nil {
		return err
	}
	defer buffer.Drop()

	bufferAcctState, err := UnmarshalUpgradeableLoaderState(buffer.Data())
	if err != nil {
		return err
	}

	if bufferAcctState.Type != UpgradeableLoaderStateTypeBuffer {
		return InstrErrInvalidArgument
	}

	if bufferAcctState.Buffer.AuthorityAddress != nil && authorityKey != nil &&
		*bufferAcctState.Buffer.AuthorityAddress != *authorityKey {
		return InstrErrIncorrectAuthority
	}

	isSigner, err := instrCtx.IsInstructionAccountSigner(7)
	if err != nil {
		return err
	}
	if !isSigner {
		return InstrErrMissingRequiredSignature
	}

	bufferKey := buffer.Key()
	bufferDataOffset := uint64(upgradeableLoaderSizeOfBufferMetaData)
	bufferDataLen := safemath.SaturatingSubU64(uint64(len(buffer.Data())), bufferDataOffset)
	programDataDataOffset := uint64(upgradeableLoaderSizeOfProgramDataMetaData)
	programDataLen := upgradeableLoaderSizeOfProgramData(deploy.MaxDataLen)

	if uint64(len(buffer.Account.Data)) < upgradeableLoaderSizeOfBufferMetaData || bufferDataLen == 0 {
		return InstrErrInvalidAccountData
	}

	buffer.Drop()

	if deploy.MaxDataLen < bufferDataLen {
		return InstrErrAccountDataTooSmall
	}

	if programDataLen > MaxPermittedDataLength {
		return InstrErrInvalidArgument
	}

	seed := make([][]byte, 1)
	seed[0] = make([]byte, solana.PublicKeyLength)
	copy(seed[0], newProgramId[:])

	programId, err := instrCtx.LastProgramKey(txCtx)
	if err != nil {
		return err
	}

	derivedAddr, bumpSeed, _ := solana.FindProgramAddress(seed, programId)
	if derivedAddr != programDataKey {
		return InstrErrInvalidArgument
	}

	buffer, err = instrCtx.BorrowInstructionAccount(txCtx, 3)
	if err != nil {
		return err
	}

	payer, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer payer.Drop()

	payer.CheckedAddLamports(buffer.Lamports(), execCtx.Features)
	buffer.SetLamports(0, execCtx.Features)

	buffer.Drop()
	payer.Drop()

	//ownerId := programId

	var lamports uint64
	minBalance := rent.MinimumBalance(programDataLen)
	lamports = max(minBalance, 1)
	createAcctInstr := newCreateAccountInstruction(payerKey, programDataKey, lamports, programDataLen, programId)
	createAcctInstr.Accounts = append(createAcctInstr.Accounts, AccountMeta{Pubkey: bufferKey, IsSigner: false, IsWritable: true})

	callerProgramId, err := instrCtx.LastProgramKey(txCtx)
	if err != nil {
		return err
	}

	var seeds [][]byte
	seeds = append(seeds, newProgramId[:])
	seeds = append(seeds, []byte{bumpSeed})

	signer, err := solana.CreateProgramAddress(seeds, callerProgramId)
	if err != nil {
		return PubkeyErrInvalidSeeds
	}

	var signers []solana.PublicKey
	signers = append(signers, signer)

	err = execCtx.NativeInvoke(*createAcctInstr, signers)
	if err != nil {
		return err
	}

	bufferData := buffer.Data()
	if uint64(len(bufferData)) < bufferDataOffset {
		return InstrErrAccountDataTooSmall
	}

	buffer, err = instrCtx.BorrowInstructionAccount(txCtx, 3)
	if err != nil {
		return err
	}

	loadedProgram, err := deployProgram(execCtx, bufferData[bufferDataOffset:])
	if err != nil {
		return InstrErrInvalidAccountData
	}

	buffer.Drop()

	programData, err := instrCtx.BorrowInstructionAccount(txCtx, 1)
	if err != nil {
		return err
	}
	defer programData.Drop()

	programDataState := &UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData,
		ProgramData: UpgradeableLoaderStateProgramData{Slot: clock.Slot, UpgradeAuthorityAddress: authorityKey}}

	err = setUpgradeableLoaderAccountState(programData, programDataState, execCtx.Features)
	if err != nil {
		return err
	}

	dstEnd := safemath.SaturatingAddU64(programDataDataOffset, bufferDataLen)
	if uint64(len(programData.Data())) < dstEnd {
		return InstrErrAccountDataTooSmall
	}
	if uint64(len(programData.Data())) < bufferDataOffset {
		return InstrErrAccountDataTooSmall
	}

	dstSlice, err := programData.DataMutable(execCtx.Features)
	if err != nil {
		return err
	}

	buffer, err = instrCtx.BorrowInstructionAccount(txCtx, 3)
	if err != nil {
		return err
	}

	srcSlice := buffer.Account.Data[bufferDataOffset:]
	copy(dstSlice[programDataDataOffset:dstEnd], srcSlice)

	err = buffer.SetDataLength(upgradeableLoaderSizeOfBuffer(0), execCtx.Features)
	if err != nil {
		return err
	}

	buffer.Drop()
	programData.Drop()

	programState := &UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram,
		Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataKey}}

	program, err = instrCtx.BorrowInstructionAccount(txCtx, 2)
	if err != nil {
		return err
	}

	err = setUpgradeableLoaderAccountState(program, programState, execCtx.Features)
	if err != nil {
		return err
	}

	if !execCtx.Features.IsActive(features.DeprecateExecutableMetaUpdateInBpfLoader) {
		err = program.SetExecutable(execCtx.Features, true)
		if err != nil {
			return err
		}
	}

	//mlog.Log.Debugf("deployed program: %s", newProgramId)

	entry := &accountsdb.ProgramCacheEntry{Program: loadedProgram, DeploymentSlot: clock.Slot}
	execCtx.SlotCtx.AccountsDb.AddProgramToCache(programDataKey, entry)

	return nil
}

func UpgradeableLoaderUpgrade(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx) error {
	//mlog.Log.Debugf("UpgradeableLoaderUpgrade")

	err := instrCtx.CheckNumOfInstructionAccounts(3)
	if err != nil {
		return err
	}

	programDataKeyIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(0)
	if err != nil {
		return err
	}
	programDataKey, err := txCtx.KeyOfAccountAtIndex(programDataKeyIdx)
	if err != nil {
		return err
	}

	err = checkAcctForRentSysvar(txCtx, instrCtx, 4)
	if err != nil {
		return err
	}

	rent, err := ReadRentSysvar(execCtx)
	if err != nil {
		return err
	}

	err = checkAcctForClockSysvar(txCtx, instrCtx, 5)
	if err != nil {
		return err
	}

	clock, err := ReadClockSysvar(execCtx)
	if err != nil {
		return err
	}

	err = instrCtx.CheckNumOfInstructionAccounts(7)
	if err != nil {
		return err
	}

	authorityKeyIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(6)
	if err != nil {
		return err
	}
	authorityKey, err := txCtx.KeyOfAccountAtIndex(authorityKeyIdx)
	if err != nil {
		return err
	}

	program, err := instrCtx.BorrowInstructionAccount(txCtx, 1)
	if err != nil {
		return err
	}
	defer program.Drop()

	if !program.IsExecutable() {
		return InstrErrAccountNotExecutable
	}

	if !program.IsWritable() {
		return InstrErrInvalidArgument
	}

	programId, err := instrCtx.LastProgramKey(txCtx)
	if err != nil {
		return err
	}
	if program.Owner() != programId {
		return InstrErrIncorrectProgramId
	}

	programState, err := UnmarshalUpgradeableLoaderState(program.Data())
	if err != nil {
		return err
	}

	if programState.Type == UpgradeableLoaderStateTypeProgram {
		if programState.Program.ProgramDataAddress != programDataKey {
			return InstrErrInvalidArgument
		}
	} else {
		return InstrErrInvalidAccountData
	}

	program.Drop()

	buffer, err := instrCtx.BorrowInstructionAccount(txCtx, 2)
	if err != nil {
		return err
	}
	defer buffer.Drop()

	bufferState, err := UnmarshalUpgradeableLoaderState(buffer.Data())
	if err != nil {
		return err
	}

	if bufferState.Type == UpgradeableLoaderStateTypeBuffer {
		if bufferState.Buffer.AuthorityAddress == nil || *bufferState.Buffer.AuthorityAddress != authorityKey {
			return InstrErrIncorrectAuthority
		}
		isSigner, err := instrCtx.IsInstructionAccountSigner(6)
		if err != nil {
			return err
		}
		if !isSigner {
			return InstrErrMissingRequiredSignature
		}
	} else {
		return InstrErrInvalidArgument
	}

	bufferLamports := buffer.Lamports()
	bufferDataOffset := uint64(upgradeableLoaderSizeOfBufferMetaData)
	bufferDataLen := safemath.SaturatingSubU64(uint64(len(buffer.Data())), bufferDataOffset)
	if len(buffer.Data()) < upgradeableLoaderSizeOfBufferMetaData || bufferDataLen == 0 {
		return InstrErrInvalidAccountData
	}

	buffer.Drop()

	programData, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer programData.Drop()

	var programDataBalanceRequired uint64
	minBalance := rent.MinimumBalance(uint64(len(programData.Data())))
	programDataBalanceRequired = max(minBalance, 1)

	if len(programData.Data()) < int(upgradeableLoaderSizeOfProgramData(bufferDataLen)) {
		return InstrErrAccountDataTooSmall
	}

	if safemath.SaturatingAddU64(programData.Lamports(), bufferLamports) < programDataBalanceRequired {
		return InstrErrInsufficientFunds
	}

	programDataState, err := UnmarshalUpgradeableLoaderState(programData.Data())
	if err != nil {
		return err
	}

	if programDataState.Type == UpgradeableLoaderStateTypeProgramData {
		if clock.Slot == programDataState.ProgramData.Slot {
			return InstrErrInvalidArgument
		}
		if programDataState.ProgramData.UpgradeAuthorityAddress == nil {
			return InstrErrImmutable
		}
		if *programDataState.ProgramData.UpgradeAuthorityAddress != authorityKey {
			return InstrErrIncorrectAuthority
		}
		isSigner, err := instrCtx.IsInstructionAccountSigner(6)
		if err != nil {
			return err
		}
		if !isSigner {
			return InstrErrMissingRequiredSignature
		}
	} else {
		return InstrErrInvalidAccountData
	}
	programData.Drop()

	buffer, err = instrCtx.BorrowInstructionAccount(txCtx, 2)
	if err != nil {
		return err
	}

	bufferData := buffer.Data()
	if uint64(len(bufferData)) < bufferDataOffset {
		return InstrErrAccountDataTooSmall
	}
	loadedProgram, err := deployProgram(execCtx, bufferData[bufferDataOffset:])
	if err != nil {
		return InstrErrInvalidAccountData
	}
	buffer.Drop()

	programData, err = instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}

	programDataNewState := &UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: clock.Slot, UpgradeAuthorityAddress: &authorityKey}}
	err = setUpgradeableLoaderAccountState(programData, programDataNewState, execCtx.Features)
	if err != nil {
		return err
	}

	programDataDataOffset := uint64(upgradeableLoaderSizeOfProgramDataMetaData)
	dstEnd := safemath.SaturatingAddU64(programDataDataOffset, bufferDataLen)
	if uint64(len(programData.Data())) < dstEnd {
		return InstrErrAccountDataTooSmall
	}
	if uint64(len(programData.Data())) < bufferDataOffset {
		return InstrErrAccountDataTooSmall
	}

	buffer, err = instrCtx.BorrowInstructionAccount(txCtx, 2)
	if err != nil {
		return err
	}

	dstSlice, err := programData.DataMutable(execCtx.Features)
	if err != nil {
		return err
	}

	copy(dstSlice[programDataDataOffset:dstEnd], buffer.Account.Data[bufferDataOffset:])
	for idx := dstEnd; idx < uint64(len(programData.Account.Data)); idx++ {
		programData.Account.Data[idx] = 0
	}

	spill, err := instrCtx.BorrowInstructionAccount(txCtx, 3)
	if err != nil {
		return err
	}
	defer spill.Drop()

	spillLamports := safemath.SaturatingSubU64(safemath.SaturatingAddU64(programData.Lamports(), bufferLamports), programDataBalanceRequired)
	err = spill.CheckedAddLamports(spillLamports, execCtx.Features)
	if err != nil {
		return err
	}

	err = buffer.SetLamports(0, execCtx.Features)
	if err != nil {
		return err
	}
	err = programData.SetLamports(programDataBalanceRequired, execCtx.Features)
	if err != nil {
		return err
	}

	err = buffer.SetDataLength(upgradeableLoaderSizeOfBuffer(0), execCtx.Features)
	if err != nil {
		return err
	}

	//mlog.Log.Debugf("upgraded program %s", program.Key())

	entry := &accountsdb.ProgramCacheEntry{Program: loadedProgram, DeploymentSlot: clock.Slot}
	execCtx.SlotCtx.AccountsDb.AddProgramToCache(programData.Key(), entry)

	return nil
}

func UpgradeableLoaderSetAuthority(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx) error {
	//mlog.Log.Debugf("SetAuthority instr")

	err := instrCtx.CheckNumOfInstructionAccounts(2)
	if err != nil {
		return err
	}

	account, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer account.Drop()

	presentAuthorityKeyIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(1)
	if err != nil {
		return err
	}

	presentAuthorityKey, err := txCtx.KeyOfAccountAtIndex(presentAuthorityKeyIdx)
	if err != nil {
		return err
	}

	var newAuthority *solana.PublicKey
	newAuthorityIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(2)
	if err == nil {
		pk, err := txCtx.KeyOfAccountAtIndex(newAuthorityIdx)
		if err == nil {
			newAuthority = &pk
		}
	}

	accountState, err := UnmarshalUpgradeableLoaderState(account.Data())
	if err != nil {
		return err
	}

	switch accountState.Type {
	case UpgradeableLoaderStateTypeBuffer:
		{
			//mlog.Log.Debugf("buffer account")
			if newAuthority == nil {
				//mlog.Log.Debugf("buffer authority not optional")
				return InstrErrIncorrectAuthority
			}

			if accountState.Buffer.AuthorityAddress == nil {
				//mlog.Log.Debugf("buffer is immutable")
				return InstrErrImmutable
			}

			if *accountState.Buffer.AuthorityAddress != presentAuthorityKey {
				//mlog.Log.Debugf("incorrect buffer authority provided")
				return InstrErrIncorrectAuthority
			}

			isSigner, err := instrCtx.IsInstructionAccountSigner(1)
			if err != nil {
				return err
			}

			if !isSigner {
				//mlog.Log.Debugf("upgrade authority did not sign")
				return InstrErrMissingRequiredSignature
			}

			accountState.Buffer.AuthorityAddress = newAuthority
			err = setUpgradeableLoaderAccountState(account, accountState, execCtx.Features)
			if err != nil {
				return err
			}
		}

	case UpgradeableLoaderStateTypeProgramData:
		{
			//mlog.Log.Debugf("ProgramData account")
			if accountState.ProgramData.UpgradeAuthorityAddress == nil {
				//mlog.Log.Debugf("program not upgradeable")
				return InstrErrImmutable
			}

			if *accountState.ProgramData.UpgradeAuthorityAddress != presentAuthorityKey {
				//mlog.Log.Debugf("incorrect upgrade authority provided")
				return InstrErrIncorrectAuthority
			}

			isSigner, err := instrCtx.IsInstructionAccountSigner(1)
			if err != nil {
				return err
			}

			if !isSigner {
				//mlog.Log.Debugf("upgrade authority did not sign")
				return InstrErrMissingRequiredSignature
			}

			accountState.ProgramData.UpgradeAuthorityAddress = newAuthority
			err = setUpgradeableLoaderAccountState(account, accountState, execCtx.Features)
			if err != nil {
				return err
			}
		}

	default:
		{
			//mlog.Log.Debugf("account does not support authorities")
			return InstrErrInvalidArgument
		}
	}

	/*	var na string
		if newAuthority != nil {
			na = newAuthority.String()
		} else {
			na = "nil"
		}
		mlog.Log.Debugf("new authority: %s", na)*/

	return nil
}

func UpgradeableLoaderSetAuthorityChecked(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx) error {
	//mlog.Log.Debugf("SetAuthorityChecked instr")

	if !execCtx.Features.IsActive(features.EnableBpfLoaderSetAuthorityCheckedIx) {
		return InstrErrInvalidInstructionData
	}

	err := instrCtx.CheckNumOfInstructionAccounts(3)
	if err != nil {
		return err
	}

	account, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer account.Drop()

	presentAuthorityKeyIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(1)
	if err != nil {
		return err
	}

	presentAuthorityKey, err := txCtx.KeyOfAccountAtIndex(presentAuthorityKeyIdx)
	if err != nil {
		return err
	}

	newAuthorityIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(2)
	if err != nil {
		return err
	}

	newAuthority, err := txCtx.KeyOfAccountAtIndex(newAuthorityIdx)
	if err != nil {
		return err
	}

	accountState, err := UnmarshalUpgradeableLoaderState(account.Data())
	if err != nil {
		return err
	}

	switch accountState.Type {
	case UpgradeableLoaderStateTypeBuffer:
		{
			if accountState.Buffer.AuthorityAddress == nil {
				//mlog.Log.Debugf("buffer is immutable")
				return InstrErrImmutable
			}

			if *accountState.Buffer.AuthorityAddress != presentAuthorityKey {
				//mlog.Log.Debugf("incorrect buffer authority provided")
				return InstrErrIncorrectAuthority
			}

			isSigner, err := instrCtx.IsInstructionAccountSigner(1)
			if err != nil {
				return err
			}

			if !isSigner {
				//mlog.Log.Debugf("buffer authority did not sign")
				return InstrErrMissingRequiredSignature
			}

			isSigner, err = instrCtx.IsInstructionAccountSigner(2)
			if err != nil {
				return err
			}

			if !isSigner {
				//mlog.Log.Debugf("new authority did not sign")
				return InstrErrMissingRequiredSignature
			}

			accountState.Buffer.AuthorityAddress = &newAuthority
			err = setUpgradeableLoaderAccountState(account, accountState, execCtx.Features)
			if err != nil {
				return err
			}
		}

	case UpgradeableLoaderStateTypeProgramData:
		{
			if accountState.ProgramData.UpgradeAuthorityAddress == nil {
				//mlog.Log.Debugf("program not upgradeable")
				return InstrErrImmutable
			}

			if *accountState.ProgramData.UpgradeAuthorityAddress != presentAuthorityKey {
				//mlog.Log.Debugf("incorrect upgrade authority provided")
				return InstrErrIncorrectAuthority
			}

			isSigner, err := instrCtx.IsInstructionAccountSigner(1)
			if err != nil {
				return err
			}

			if !isSigner {
				//mlog.Log.Debugf("buffer authority did not sign")
				return InstrErrMissingRequiredSignature
			}

			isSigner, err = instrCtx.IsInstructionAccountSigner(2)
			if err != nil {
				return err
			}

			if !isSigner {
				//mlog.Log.Debugf("new authority did not sign")
				return InstrErrMissingRequiredSignature
			}

			accountState.ProgramData.UpgradeAuthorityAddress = &newAuthority
			err = setUpgradeableLoaderAccountState(account, accountState, execCtx.Features)
			if err != nil {
				return err
			}
		}

	default:
		{
			//mlog.Log.Debugf("account does not support authorities")
			return InstrErrInvalidArgument
		}
	}

	//mlog.Log.Debugf("new authority: %s", newAuthority)

	return nil
}

func closeAcctCommon(authorityAddr *solana.PublicKey, txCtx *TransactionCtx, instrCtx *InstructionCtx, f features.Features) error {
	if authorityAddr == nil {
		//mlog.Log.Debugf("Account is immutable")
		return InstrErrImmutable
	}

	idxInTx, err := instrCtx.IndexOfInstructionAccountInTransaction(2)
	if err != nil {
		return err
	}

	auth, err := txCtx.KeyOfAccountAtIndex(idxInTx)
	if err != nil {
		return err
	}

	if *authorityAddr != auth {
		//mlog.Log.Debugf("Incorrect authority provided")
		return InstrErrIncorrectAuthority
	}

	isSigner, err := instrCtx.IsInstructionAccountSigner(2)
	if err != nil {
		return err
	}

	if !isSigner {
		//mlog.Log.Debugf("Authority did not sign")
		return InstrErrMissingRequiredSignature
	}

	closeAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer closeAcct.Drop()

	recipientAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 1)
	if err != nil {
		return err
	}
	defer recipientAcct.Drop()

	err = recipientAcct.CheckedAddLamports(closeAcct.Lamports(), f)
	if err != nil {
		return err
	}

	err = closeAcct.SetLamports(0, f)
	if err != nil {
		return err
	}

	newUninitialized := &UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeUninitialized}
	err = setUpgradeableLoaderAccountState(closeAcct, newUninitialized, f)

	return err
}

func UpgradeableLoaderClose(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx) error {
	//mlog.Log.Debugf("Close instr")

	err := instrCtx.CheckNumOfInstructionAccounts(2)
	if err != nil {
		return err
	}

	idx1, err1 := instrCtx.IndexOfInstructionAccountInTransaction(0)
	if err1 != nil {
		return err1
	}

	idx2, err2 := instrCtx.IndexOfInstructionAccountInTransaction(1)
	if err2 != nil {
		return err2
	}

	if idx1 == idx2 {
		//mlog.Log.Debugf("recipient is the same as the account being closed")
		return InstrErrInvalidArgument
	}

	closeAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer closeAcct.Drop()

	closeKey := closeAcct.Key()

	closeAcctState, err := UnmarshalUpgradeableLoaderState(closeAcct.Data())
	if err != nil {
		return err
	}

	err = closeAcct.SetDataLength(upgradeableLoaderSizeOfUninitialized, execCtx.Features)
	if err != nil {
		return err
	}

	switch closeAcctState.Type {
	case UpgradeableLoaderStateTypeUninitialized:
		{
			recipientAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 1)
			if err != nil {
				return err
			}
			defer recipientAcct.Drop()

			err = recipientAcct.CheckedAddLamports(closeAcct.Lamports(), execCtx.Features)
			if err != nil {
				return err
			}

			err = closeAcct.SetLamports(0, execCtx.Features)
			if err != nil {
				return err
			}

			//mlog.Log.Debugf("closed uninitialized %s", closeKey)
		}

	case UpgradeableLoaderStateTypeBuffer:
		{
			err = instrCtx.CheckNumOfInstructionAccounts(3)
			if err != nil {
				//mlog.Log.Debugf("(buffer) not enough instruction accounts (%d)", instrCtx.NumberOfInstructionAccounts())
				return err
			}

			closeAcct.Drop()

			err = closeAcctCommon(closeAcctState.Buffer.AuthorityAddress, txCtx, instrCtx, execCtx.Features)
			if err != nil {
				return err
			}

			//mlog.Log.Debugf("closed buffer %s", closeKey)
		}

	case UpgradeableLoaderStateTypeProgramData:
		{
			err = instrCtx.CheckNumOfInstructionAccounts(4)
			if err != nil {
				//mlog.Log.Debugf("(ProgramData) not enough instruction accounts (%d)", instrCtx.NumberOfInstructionAccounts())
				return err
			}

			closeAcct.Drop()

			programAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 3)
			if err != nil {
				return err
			}
			defer programAcct.Drop()

			//programKey := programAcct.Key()

			if !programAcct.IsWritable() {
				//mlog.Log.Debugf("program account is not writable")
				return InstrErrInvalidArgument
			}

			programId, err := instrCtx.LastProgramKey(txCtx)
			if err != nil {
				return err
			}

			if programAcct.Owner() != programId {
				//mlog.Log.Debugf("program account not owned by loader")
				return InstrErrIncorrectProgramId
			}

			clock, err := ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			if clock.Slot == closeAcctState.ProgramData.Slot {
				//mlog.Log.Debugf("program was deployed in this block already")
				return InstrErrInvalidArgument
			}

			programAcctState, err := UnmarshalUpgradeableLoaderState(programAcct.Data())
			if err != nil {
				return err
			}

			switch programAcctState.Type {
			case UpgradeableLoaderStateTypeProgram:
				{
					if programAcctState.Program.ProgramDataAddress != closeKey {
						//mlog.Log.Debugf("ProgramData account does not match ProgramData account")
						return InstrErrInvalidArgument
					}

					programAcct.Drop()

					err = closeAcctCommon(closeAcctState.ProgramData.UpgradeAuthorityAddress, txCtx, instrCtx, execCtx.Features)
					if err != nil {
						return err
					}
					execCtx.SlotCtx.AccountsDb.RemoveProgramFromCache(closeKey)
				}

			default:
				{
					//mlog.Log.Debugf("Invalid Program account")
					return InstrErrInvalidArgument
				}
			}

			//mlog.Log.Debugf("Closed program %s", programKey)
		}

	default:
		{
			//mlog.Log.Debugf("Account does not support closing")
			return InstrErrInvalidArgument
		}
	}

	return nil
}

func UpgradeableLoaderExtendProgram(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx, additionalBytes uint32) error {
	//mlog.Log.Debugf("ExtendProgram instr")

	if additionalBytes == 0 {
		//mlog.Log.Debugf("Additional bytes must be greater than 0")
		return InstrErrInvalidInstructionData
	}

	programDataAcctIdx := uint64(0)
	programAcctIdx := uint64(1)
	optionalPayerAcctIdx := uint64(3)

	programDataAcct, err := instrCtx.BorrowInstructionAccount(txCtx, programDataAcctIdx)
	if err != nil {
		return err
	}
	defer programDataAcct.Drop()

	programDataKey := programDataAcct.Key()

	programId, err := instrCtx.LastProgramKey(txCtx)
	if err != nil {
		return err
	}

	if programId != programDataAcct.Owner() {
		//mlog.Log.Debugf("ProgramData owner is invalid")
		return InstrErrInvalidAccountOwner
	}

	if !programDataAcct.IsWritable() {
		//mlog.Log.Debugf("ProgramData is not writable")
		return InstrErrInvalidArgument
	}

	programAcct, err := instrCtx.BorrowInstructionAccount(txCtx, programAcctIdx)
	if err != nil {
		return err
	}
	defer programAcct.Drop()

	if !programAcct.IsWritable() {
		//mlog.Log.Debugf("Program account is not writable")
		return InstrErrInvalidArgument
	}

	if programAcct.Owner() != programId {
		//mlog.Log.Debugf("Program account is not owned by the loader")
		return InstrErrInvalidAccountOwner
	}

	programAcctState, err := UnmarshalUpgradeableLoaderState(programAcct.Data())
	if err != nil {
		return err
	}

	switch programAcctState.Type {
	case UpgradeableLoaderStateTypeProgram:
		{
			if programAcctState.Program.ProgramDataAddress != programDataKey {
				//mlog.Log.Debugf("Program account does not match ProgramData account")
				return InstrErrInvalidArgument
			}
		}
	default:
		{
			//mlog.Log.Debugf("Invalid Program account")
			return InstrErrInvalidAccountData
		}
	}

	programAcct.Drop()

	oldLen := uint64(len(programDataAcct.Data()))
	newLen := safemath.SaturatingAddU64(oldLen, uint64(additionalBytes))
	if newLen > MaxPermittedDataLength {
		//mlog.Log.Debugf("Extended ProgramData length of %d bytes exceeds max account data length of %d bytes", newLen, MaxPermittedDataLength)
		return InstrErrInvalidRealloc
	}

	clock, err := ReadClockSysvar(execCtx)
	if err != nil {
		return err
	}

	clockSlot := clock.Slot

	programDataAcctState, err := UnmarshalUpgradeableLoaderState(programDataAcct.Data())
	if err != nil {
		return err
	}

	if programDataAcctState.Type == UpgradeableLoaderStateTypeProgramData {
		if clockSlot == programDataAcctState.ProgramData.Slot {
			//mlog.Log.Debugf("Program was extended in this block already")
			return InstrErrInvalidArgument
		}

		if programDataAcctState.ProgramData.UpgradeAuthorityAddress == nil {
			//mlog.Log.Debugf("Cannot extend ProgramData accounts that are not upgradeable")
			return InstrErrImmutable
		}
	} else {
		//mlog.Log.Debugf("ProgramData state is invalid")
		return InstrErrInvalidAccountData
	}

	rent, err := ReadRentSysvar(execCtx)
	if err != nil {
		return err
	}

	balance := programDataAcct.Lamports()
	minBalance := max(rent.MinimumBalance(newLen), 1)
	requiredPayment := safemath.SaturatingSubU64(minBalance, balance)

	programDataAcct.Drop()

	if requiredPayment > 0 {
		payerKeyIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(optionalPayerAcctIdx)
		if err != nil {
			return err
		}
		payerKey, err := txCtx.KeyOfAccountAtIndex(payerKeyIdx)
		if err != nil {
			return err
		}

		txInstr := newTransferInstruction(payerKey, programDataKey, requiredPayment)
		err = execCtx.NativeInvoke(*txInstr, nil)
		if err != nil {
			return err
		}
	}

	programDataAcct, err = instrCtx.BorrowInstructionAccount(txCtx, programDataAcctIdx)
	if err != nil {
		return err
	}

	err = programDataAcct.SetDataLength(newLen, execCtx.Features)
	if err != nil {
		return err
	}

	programBytes := programDataAcct.Data()
	if uint64(len(programBytes)) < upgradeableLoaderSizeOfProgramDataMetaData {
		return InstrErrAccountDataTooSmall
	}
	loadedProgram, err := deployProgram(execCtx, programBytes[upgradeableLoaderSizeOfProgramDataMetaData:])
	if err != nil {
		//mlog.Log.Debugf("deploy program failed")
		return InstrErrInvalidAccountData
	}

	programDataAcctState.ProgramData.Slot = clockSlot
	err = setUpgradeableLoaderAccountState(programDataAcct, programDataAcctState, execCtx.Features)
	if err != nil {
		return err
	}

	//mlog.Log.Debugf("Extended ProgramData account by %d bytes", additionalBytes)

	entry := &accountsdb.ProgramCacheEntry{Program: loadedProgram, DeploymentSlot: clock.Slot}
	execCtx.SlotCtx.AccountsDb.AddProgramToCache(programDataAcct.Key(), entry)

	return nil
}

func ProcessUpgradeableLoaderInstruction(execCtx *ExecutionCtx) error {
	//mlog.Log.Debugf("BPF loader program mgmt")

	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	instrData := instrCtx.Data

	_, err = instrCtx.LastProgramKey(txCtx)
	if err != nil {
		return err
	}

	decoder := bin.NewBinDecoder(instrData)

	instrType, err := decoder.ReadUint32(bin.LE)
	if err != nil {
		return InstrErrInvalidInstructionData
	}

	switch instrType {
	case UpgradeableLoaderInstrTypeInitializeBuffer:
		{
			err = UpgradeableLoaderInitializeBuffer(execCtx, txCtx, instrCtx)
		}

	case UpgradeableLoaderInstrTypeWrite:
		{
			var write UpgradeableLoaderInstrWrite
			err = write.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			err = UpgradeableLoaderWrite(execCtx, txCtx, instrCtx, write)
		}

	case UpgradeableLoaderInstrTypeDeployWithMaxDataLen:
		{
			var deploy UpgradeableLoaderInstrDeployWithMaxDataLen
			err = deploy.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			err = UpgradeableLoaderDeployWithMaxDataLen(execCtx, txCtx, instrCtx, deploy)
		}

	case UpgradeableLoaderInstrTypeUpgrade:
		{
			err = UpgradeableLoaderUpgrade(execCtx, txCtx, instrCtx)
		}

	case UpgradeableLoaderInstrTypeSetAuthority:
		{
			err = UpgradeableLoaderSetAuthority(execCtx, txCtx, instrCtx)
		}

	case UpgradeableLoaderInstrTypeSetAuthorityChecked:
		{
			err = UpgradeableLoaderSetAuthorityChecked(execCtx, txCtx, instrCtx)
		}

	case UpgradeableLoaderInstrTypeClose:
		{
			err = UpgradeableLoaderClose(execCtx, txCtx, instrCtx)
		}

	case UpgradeableLoaderInstrTypeExtendProgram:
		{
			var extend UpgradeableLoaderInstrExtendProgram
			err = extend.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			err = UpgradeableLoaderExtendProgram(execCtx, txCtx, instrCtx, extend.AdditionalBytes)
		}
	default:
		{
			err = InstrErrInvalidInstructionData
		}
	}

	return err
}
