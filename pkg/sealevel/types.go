package sealevel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

const SolInstructionCStructSize = 40

type SolInstructionC struct {
	ProgramIdAddr uint64
	AccountsAddr  uint64
	AccountsLen   uint64
	DataAddr      uint64
	DataLen       uint64
}

const SolInstructionRustStructSize = 80

type SolInstructionRust struct {
	Accounts VectorDescrRust
	Data     VectorDescrRust
	Pubkey   solana.PublicKey
}

type Instruction struct {
	Accounts  []AccountMeta
	Data      []byte
	ProgramId solana.PublicKey
}

const AccountMetaSize = 34

type AccountMeta struct {
	Pubkey     solana.PublicKey
	IsSigner   bool
	IsWritable bool
}

const SolAccountMetaCSize = 16

type SolAccountMetaC struct {
	PubkeyAddr uint64
	IsWritable byte
	IsSigner   byte
}

const SolAccountMetaRustSize = 34

type SolAccountMetaRust struct {
	Pubkey     solana.PublicKey
	IsSigner   byte
	IsWritable byte
}

const SolSignerSeedsCSize = 16

type VectorDescrC struct {
	Addr uint64
	Len  uint64
}

type VectorDescrRust struct {
	Addr uint64
	Cap  uint64
	Len  uint64
}

type InstructionAccount struct {
	IndexInTransaction uint64
	IndexInCaller      uint64
	IndexInCallee      uint64
	IsSigner           bool
	IsWritable         bool
}

const SolAccountInfoCSize = 56

type SolAccountInfoC struct {
	KeyAddr      uint64
	LamportsAddr uint64
	DataLen      uint64
	DataAddr     uint64
	OwnerAddr    uint64
	RentEpoch    uint64
	IsSigner     bool
	IsWritable   bool
	Executable   bool
}

const SolAccountInfoRustSize = 48

type SolAccountInfoRust struct {
	PubkeyAddr      uint64 // points to uchar[32]
	LamportsBoxAddr uint64 // points to Rc with embedded RefCell which points to u64
	DataBoxAddr     uint64 // points to Rc with embedded RefCell which contains slice which points to bytes
	OwnerAddr       uint64 // points to uchar[32]
	RentEpoch       uint64
	IsSigner        byte
	IsWritable      byte
	Executable      byte
}

const RefCellRustSize = 32

type RefCellRust struct {
	Strong uint64
	Weak   uint64
	Borrow uint64
	Addr   uint64
}

const RefCellVecRustSize = 40

type RefCellVecRust struct {
	Strong uint64
	Weak   uint64
	Borrow uint64
	Addr   uint64
	Len    uint64
}

type TranslatedAccounts []TranslatedAccount

type TranslatedAccount struct {
	IndexOfAccount      uint64
	CallerAccount       *CallerAccount
	UpdateCallerAccount bool
	UpdateCallerRegion  bool
}

type CallerAccount struct {
	Lamports        []byte
	Owner           []byte
	OriginalDataLen uint64
	SerializedData  []byte
	VmDataAddr      uint64
	RefToLenInVm    []byte
}

const ProcessedSiblingInstructionSize = 16

type ProcessedSiblingInstruction struct {
	DataLen     uint64
	AccountsLen uint64
}

func (accountMeta *AccountMeta) Unmarshal(buf io.Reader) error {
	var accountMetaBytes [AccountMetaSize]byte

	_, err := buf.Read(accountMetaBytes[:])
	if err != nil {
		return err
	}

	copy(accountMeta.Pubkey[:], accountMetaBytes[:32])
	if accountMetaBytes[32] > 1 || accountMetaBytes[33] > 1 {
		return InstrErrInvalidArgument
	}
	accountMeta.IsSigner = accountMetaBytes[32] != 0
	accountMeta.IsWritable = accountMetaBytes[33] != 0

	return nil
}

func (accountMeta *AccountMeta) Marshal() []byte {
	var acctMetaBytes [AccountMetaSize]byte
	copy(acctMetaBytes[:32], accountMeta.Pubkey[:])

	if accountMeta.IsSigner {
		acctMetaBytes[32] = 1
	}

	if accountMeta.IsWritable {
		acctMetaBytes[33] = 1
	}

	return acctMetaBytes[:]
}

func (accountMeta *SolAccountMetaC) Unmarshal(buf io.Reader) error {
	var acctMetaBytes [SolAccountMetaCSize]byte

	_, err := buf.Read(acctMetaBytes[:])
	if err != nil {
		return err
	}

	accountMeta.PubkeyAddr = binary.LittleEndian.Uint64(acctMetaBytes[:8])
	accountMeta.IsWritable = acctMetaBytes[8]
	accountMeta.IsSigner = acctMetaBytes[9]

	return err
}

// just for testing
func (accountMeta *SolAccountMetaC) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	var err error
	err = binary.Write(buf, binary.LittleEndian, accountMeta.PubkeyAddr)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, accountMeta.IsWritable)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, accountMeta.IsSigner)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (solInstr *SolInstructionC) Unmarshal(buf io.Reader) error {
	var instrBytes [SolInstructionCStructSize]byte

	_, err := buf.Read(instrBytes[:])
	if err != nil {
		return err
	}

	solInstr.ProgramIdAddr = binary.LittleEndian.Uint64(instrBytes[:8])
	solInstr.AccountsAddr = binary.LittleEndian.Uint64(instrBytes[8:16])
	solInstr.AccountsLen = binary.LittleEndian.Uint64(instrBytes[16:24])
	solInstr.DataAddr = binary.LittleEndian.Uint64(instrBytes[24:32])
	solInstr.DataLen = binary.LittleEndian.Uint64(instrBytes[32:40])

	return nil
}

// just for testing
func (solInstr *SolInstructionC) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.LittleEndian, solInstr.ProgramIdAddr)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, solInstr.AccountsAddr)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, solInstr.AccountsLen)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, solInstr.DataAddr)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, solInstr.DataLen)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (solInstr *SolInstructionRust) Unmarshal(buf io.Reader) error {
	err := solInstr.Accounts.Unmarshal(buf)
	if err != nil {
		return err
	}

	err = solInstr.Data.Unmarshal(buf)
	if err != nil {
		return err
	}

	_, err = buf.Read(solInstr.Pubkey[:])
	return err
}

func (vectorDescr *VectorDescrC) Unmarshal(buf io.Reader) error {
	var vectorBytes [16]byte
	_, err := buf.Read(vectorBytes[:])
	if err != nil {
		return err
	}

	vectorDescr.Addr = binary.LittleEndian.Uint64(vectorBytes[:8])
	vectorDescr.Len = binary.LittleEndian.Uint64(vectorBytes[8:16])

	return nil
}

// just for testing
func (vectorDescr *VectorDescrC) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.LittleEndian, vectorDescr.Addr)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, vectorDescr.Len)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (vectorDescr *VectorDescrRust) Unmarshal(buf io.Reader) error {
	var vectorBytes [24]byte
	_, err := buf.Read(vectorBytes[:])
	if err != nil {
		return err
	}

	vectorDescr.Addr = binary.LittleEndian.Uint64(vectorBytes[:8])
	vectorDescr.Cap = binary.LittleEndian.Uint64(vectorBytes[8:16])
	vectorDescr.Len = binary.LittleEndian.Uint64(vectorBytes[16:24])

	return nil
}

func (accountInfo *SolAccountInfoC) Unmarshal(buf io.Reader) error {
	var acctInfoBytes [SolAccountInfoCSize]byte
	_, err := buf.Read(acctInfoBytes[:])
	if err != nil {
		return err
	}

	accountInfo.KeyAddr = binary.LittleEndian.Uint64(acctInfoBytes[:8])
	accountInfo.LamportsAddr = binary.LittleEndian.Uint64(acctInfoBytes[8:16])
	accountInfo.DataLen = binary.LittleEndian.Uint64(acctInfoBytes[16:24])
	accountInfo.DataAddr = binary.LittleEndian.Uint64(acctInfoBytes[24:32])
	accountInfo.OwnerAddr = binary.LittleEndian.Uint64(acctInfoBytes[32:40])
	accountInfo.RentEpoch = binary.LittleEndian.Uint64(acctInfoBytes[40:48])
	accountInfo.IsSigner = acctInfoBytes[48] != 0
	accountInfo.IsWritable = acctInfoBytes[49] != 0
	accountInfo.Executable = acctInfoBytes[50] != 0

	return nil
}

func (accountInfo *SolAccountInfoRust) Unmarshal(buf io.Reader) error {
	var err error
	var accountInfoBytes [SolAccountInfoRustSize]byte

	_, err = buf.Read(accountInfoBytes[:])
	if err != nil {
		return err
	}

	accountInfo.PubkeyAddr = binary.LittleEndian.Uint64(accountInfoBytes[:8])
	accountInfo.LamportsBoxAddr = binary.LittleEndian.Uint64(accountInfoBytes[8:16])
	accountInfo.DataBoxAddr = binary.LittleEndian.Uint64(accountInfoBytes[16:24])
	accountInfo.OwnerAddr = binary.LittleEndian.Uint64(accountInfoBytes[24:32])
	accountInfo.RentEpoch = binary.LittleEndian.Uint64(accountInfoBytes[32:40])
	accountInfo.IsSigner = accountInfoBytes[40]
	accountInfo.IsWritable = accountInfoBytes[41]
	accountInfo.Executable = accountInfoBytes[42]

	return nil
}

func (refCell *RefCellRust) Unmarshal(buf io.Reader) error {
	var refCellBytes [RefCellRustSize]byte

	_, err := buf.Read(refCellBytes[:])
	if err != nil {
		return err
	}

	refCell.Strong = binary.LittleEndian.Uint64(refCellBytes[:8])
	refCell.Weak = binary.LittleEndian.Uint64(refCellBytes[8:16])
	refCell.Borrow = binary.LittleEndian.Uint64(refCellBytes[16:24])
	refCell.Addr = binary.LittleEndian.Uint64(refCellBytes[24:32])

	return nil
}

func (refCellVec *RefCellVecRust) Unmarshal(buf io.Reader) error {
	var err error
	var refCellVecBytes [RefCellVecRustSize]byte

	_, err = buf.Read(refCellVecBytes[:])
	if err != nil {
		return err
	}

	refCellVec.Strong = binary.LittleEndian.Uint64(refCellVecBytes[:8])
	refCellVec.Weak = binary.LittleEndian.Uint64(refCellVecBytes[8:16])
	refCellVec.Borrow = binary.LittleEndian.Uint64(refCellVecBytes[16:24])
	refCellVec.Addr = binary.LittleEndian.Uint64(refCellVecBytes[24:32])
	refCellVec.Len = binary.LittleEndian.Uint64(refCellVecBytes[32:40])

	return nil
}

func (psi *ProcessedSiblingInstruction) Unmarshal(buf io.Reader) error {
	var psiBytes [ProcessedSiblingInstructionSize]byte

	_, err := buf.Read(psiBytes[:])
	if err != nil {
		return err
	}

	psi.DataLen = binary.LittleEndian.Uint64(psiBytes[:8])
	psi.AccountsLen = binary.LittleEndian.Uint64(psiBytes[8:16])

	return nil
}

func (psi *ProcessedSiblingInstruction) Marshal() []byte {
	var psiBytes [ProcessedSiblingInstructionSize]byte

	binary.LittleEndian.PutUint64(psiBytes[:8], psi.DataLen)
	binary.LittleEndian.PutUint64(psiBytes[8:16], psi.AccountsLen)

	return psiBytes[:]
}

func ReadBool(decoder *bin.Decoder) (bool, error) {
	if decoder.Remaining() < 1 {
		return false, fmt.Errorf("fewer than 1 byte remaining")
	}

	b, err := decoder.ReadByte()
	if err != nil {
		err = fmt.Errorf("readBool, %s", err)
	}

	if b != 0 && b != 1 {
		return false, fmt.Errorf("malformed bool")
	}

	return b != 0, nil
}
