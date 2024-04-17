package sealevel

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/gagliardetto/solana-go"
)

const SolInstructionStructSize = 40

type SolInstruction struct {
	programIdAddr uint64
	accountsAddr  uint64
	accountsLen   uint64
	dataAddr      uint64
	dataLen       uint64
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

type SolAccountMeta struct {
	PubkeyAddr uint64
	IsSigner   byte
	IsWritable byte
}

const SolSignerSeedsCSize = 16

type VectorDescrC struct {
	Addr uint64
	Len  uint64
}

type InstructionAccount struct {
	IndexInTransaction uint64
	IndexInCaller      uint64
	IndexInCallee      uint64
	IsSigner           bool
	IsWritable         bool
}

const SolAccountInfoSize = 51

type SolAccountInfo struct {
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

type TranslatedAccounts []TranslatedAccount

type TranslatedAccount struct {
	IndexOfAccount uint64
	CallerAccount  *CallerAccount
}

type CallerAccount struct {
	Lamports          uint64
	Owner             solana.PublicKey
	SerializedData    *[]byte
	SerializedDataLen uint64
	VmDataAddr        uint64
	RefToLenInVm      uint64
	Executable        bool
	RentEpoch         uint64
}

func (accountMeta *AccountMeta) Unmarshal(buf io.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &accountMeta.Pubkey)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &accountMeta.IsSigner)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &accountMeta.IsWritable)
	if err != nil {
		return err
	}
	return nil
}

func (accountMeta *AccountMeta) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	var err error
	err = binary.Write(buf, binary.LittleEndian, accountMeta.Pubkey)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, accountMeta.IsSigner)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, accountMeta.IsWritable)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (accountMeta *SolAccountMeta) Unmarshal(buf io.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &accountMeta.PubkeyAddr)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &accountMeta.IsSigner)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &accountMeta.IsWritable)
	if err != nil {
		return err
	}
	return nil
}

func (accountMeta *SolAccountMeta) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	var err error
	err = binary.Write(buf, binary.LittleEndian, accountMeta.PubkeyAddr)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, accountMeta.IsSigner)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, accountMeta.IsWritable)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (solInstr *SolInstruction) Unmarshal(buf io.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &solInstr.programIdAddr)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &solInstr.accountsAddr)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &solInstr.accountsLen)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &solInstr.dataAddr)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &solInstr.dataLen)
	if err != nil {
		return err
	}

	return nil
}

func (solInstr *SolInstruction) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.LittleEndian, solInstr.programIdAddr)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, solInstr.accountsAddr)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, solInstr.accountsLen)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, solInstr.dataAddr)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, solInstr.dataLen)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (vectorDescr *VectorDescrC) Unmarshal(buf io.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &vectorDescr.Addr)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &vectorDescr.Len)
	if err != nil {
		return err
	}
	return nil
}

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

func (accountInfo *SolAccountInfo) Unmarshal(buf io.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &accountInfo.KeyAddr)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &accountInfo.LamportsAddr)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &accountInfo.DataLen)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &accountInfo.DataAddr)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &accountInfo.OwnerAddr)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &accountInfo.RentEpoch)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &accountInfo.IsSigner)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &accountInfo.IsWritable)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &accountInfo.Executable)
	if err != nil {
		return err
	}

	return nil
}
