package transactionhistory

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/wincode"
	"github.com/gagliardetto/solana-go"
)

const (
	MaxMetadataBytes   = 1 << 20
	metadataAccounts   = 256
	metadataCPIEntries = 64
	// This optional-history ceiling matches the bounded execution collector.
	metadataCPIDataBytes = 10 << 10
	metadataErrorBytes   = 2048
	metadataReturnBytes  = 1024
	tokenBalanceBytes    = 107
)

// Metadata contains committed execution facts, not an RPC response or a finality
// claim. Version 1 records balances and inner instructions even when empty. It
// does not record the complete runtime log stream; RPC logMessages must be null.
type Metadata struct {
	AccountKeys                         []solana.PublicKey
	Fee                                 uint64
	Error                               json.RawMessage
	PreBalances, PostBalances           []uint64
	InnerInstructions                   []InnerInstructionGroup
	ComputeUnitsConsumed                uint64
	ReturnData                          *ReturnData
	PreTokenBalances, PostTokenBalances []TokenBalance
}

type InnerInstructionGroup struct {
	Index        uint8
	Instructions []InnerInstruction
}

type InnerInstruction struct {
	ProgramIDIndex uint8
	Accounts       []uint8
	Data           []byte
	StackHeight    uint32
}

type ReturnData struct {
	ProgramID solana.PublicKey
	Data      []byte
}

// TokenBalance preserves raw units and the mint decimals at this transaction's
// position. Historical balances do not apply Token-2022 live-account UI scaling.
type TokenBalance struct {
	AccountIndex           uint16
	Mint, Owner, ProgramID solana.PublicKey
	Amount                 uint64
	Decimals               uint8
}

var errMetadata = errors.New("invalid transaction history metadata")

func (m *Metadata) encodedSize() (int, error) {
	if m == nil || len(m.AccountKeys) == 0 || len(m.AccountKeys) > metadataAccounts ||
		len(m.PreBalances) != len(m.AccountKeys) || len(m.PostBalances) != len(m.AccountKeys) ||
		len(m.Error) > metadataErrorBytes || !json.Valid(m.Error) {
		return 0, errMetadata
	}
	status := bytes.TrimSpace(m.Error)
	if !bytes.Equal(status, []byte("null")) && status[0] != '"' && status[0] != '{' {
		return 0, errMetadata
	}
	size := 4 + 8 + 8 + 8 + len(m.Error) + 2 + 48*len(m.AccountKeys) + 2 + 1
	if len(m.InnerInstructions) > metadataCPIEntries {
		return 0, errMetadata
	}
	entries := 0
	for i, group := range m.InnerInstructions {
		if len(group.Instructions) == 0 || len(group.Instructions) > metadataCPIEntries-entries ||
			(i > 0 && group.Index <= m.InnerInstructions[i-1].Index) {
			return 0, errMetadata
		}
		entries += len(group.Instructions)
		size += 3
		for _, instruction := range group.Instructions {
			if int(instruction.ProgramIDIndex) >= len(m.AccountKeys) || len(instruction.Accounts) > 255 || len(instruction.Data) > metadataCPIDataBytes {
				return 0, errMetadata
			}
			for _, index := range instruction.Accounts {
				if int(index) >= len(m.AccountKeys) {
					return 0, errMetadata
				}
			}
			size += 21 + len(instruction.Accounts) + len(instruction.Data)
		}
	}
	for _, balances := range [][]TokenBalance{m.PreTokenBalances, m.PostTokenBalances} {
		if len(balances) > len(m.AccountKeys) {
			return 0, errMetadata
		}
		for i, balance := range balances {
			if int(balance.AccountIndex) >= len(m.AccountKeys) || (i > 0 && balance.AccountIndex <= balances[i-1].AccountIndex) {
				return 0, errMetadata
			}
		}
		size += 2 + tokenBalanceBytes*len(balances)
	}
	if m.ReturnData != nil {
		if len(m.ReturnData.Data) > metadataReturnBytes {
			return 0, errMetadata
		}
		size += 40 + len(m.ReturnData.Data)
	}
	if size > MaxMetadataBytes {
		return 0, errMetadata
	}
	return size, nil
}

// MarshalBinary validates counts and the complete size before allocating the
// owned encoding. Metadata uses the block's fixed capture contract version.
func (m *Metadata) MarshalBinary() ([]byte, error) {
	size, err := m.encodedSize()
	if err != nil {
		return nil, err
	}
	w := wincode.NewWriter(size)
	w.WriteU32(CaptureVersion)
	w.WriteU64(m.Fee)
	w.WriteU64(m.ComputeUnitsConsumed)
	w.WriteByteVec(m.Error)
	w.WriteU16(uint16(len(m.AccountKeys)))
	for i, key := range m.AccountKeys {
		w.WriteBytes(key[:])
		w.WriteU64(m.PreBalances[i])
		w.WriteU64(m.PostBalances[i])
	}
	w.WriteU16(uint16(len(m.InnerInstructions)))
	for _, group := range m.InnerInstructions {
		w.WriteU8(group.Index)
		w.WriteU16(uint16(len(group.Instructions)))
		for _, instruction := range group.Instructions {
			w.WriteU8(instruction.ProgramIDIndex)
			w.WriteU32(instruction.StackHeight)
			w.WriteByteVec(instruction.Accounts)
			w.WriteByteVec(instruction.Data)
		}
	}
	for _, balances := range [][]TokenBalance{m.PreTokenBalances, m.PostTokenBalances} {
		w.WriteU16(uint16(len(balances)))
		for _, balance := range balances {
			w.WriteU16(balance.AccountIndex)
			w.WriteBytes(balance.Mint[:])
			w.WriteBytes(balance.Owner[:])
			w.WriteBytes(balance.ProgramID[:])
			w.WriteU64(balance.Amount)
			w.WriteU8(balance.Decimals)
		}
	}
	if m.ReturnData == nil {
		w.WriteU8(0)
	} else {
		w.WriteU8(1)
		w.WriteBytes(m.ReturnData.ProgramID[:])
		w.WriteByteVec(m.ReturnData.Data)
	}
	return w.Bytes(), nil
}

// UnmarshalMetadata checks every length before allocating and returns owned
// slices. A malformed or unsupported record cannot become a partial receipt.
func UnmarshalMetadata(data []byte) (*Metadata, error) {
	if len(data) > MaxMetadataBytes {
		return nil, errMetadata
	}
	r := wincode.NewReader(data)
	version, err := r.ReadU32()
	if err != nil || version != CaptureVersion {
		return nil, errMetadata
	}
	m := &Metadata{}
	if m.Fee, err = r.ReadU64(); err != nil {
		return nil, err
	}
	if m.ComputeUnitsConsumed, err = r.ReadU64(); err != nil {
		return nil, err
	}
	if m.Error, err = r.ReadByteVec(metadataErrorBytes); err != nil {
		return nil, err
	}
	count, err := metadataCount(r, metadataAccounts, 48)
	if err != nil {
		return nil, err
	}
	rows, err := r.ReadBytes(count * 48)
	if err != nil {
		return nil, err
	}
	m.AccountKeys = make([]solana.PublicKey, count)
	m.PreBalances, m.PostBalances = make([]uint64, count), make([]uint64, count)
	for i := range m.AccountKeys {
		row := rows[i*48 : (i+1)*48]
		copy(m.AccountKeys[i][:], row[:32])
		m.PreBalances[i] = binary.LittleEndian.Uint64(row[32:40])
		m.PostBalances[i] = binary.LittleEndian.Uint64(row[40:48])
	}
	count, err = metadataCount(r, metadataCPIEntries, 3)
	if err != nil {
		return nil, err
	}
	m.InnerInstructions = make([]InnerInstructionGroup, count)
	remaining := metadataCPIEntries
	for i := range m.InnerInstructions {
		group := &m.InnerInstructions[i]
		if group.Index, err = r.ReadU8(); err != nil {
			return nil, err
		}
		count, err = metadataCount(r, remaining, 21)
		if err != nil {
			return nil, err
		}
		remaining -= count
		group.Instructions = make([]InnerInstruction, count)
		for j := range group.Instructions {
			instruction := &group.Instructions[j]
			if instruction.ProgramIDIndex, err = r.ReadU8(); err != nil {
				return nil, err
			}
			if instruction.StackHeight, err = r.ReadU32(); err != nil {
				return nil, err
			}
			if instruction.Accounts, err = r.ReadByteVec(255); err != nil {
				return nil, err
			}
			if instruction.Data, err = r.ReadByteVec(metadataCPIDataBytes); err != nil {
				return nil, err
			}
		}
	}
	if m.PreTokenBalances, err = readTokenBalances(r); err != nil {
		return nil, err
	}
	if m.PostTokenBalances, err = readTokenBalances(r); err != nil {
		return nil, err
	}
	present, err := r.ReadU8()
	if err != nil || present > 1 {
		return nil, errMetadata
	}
	if present == 1 {
		program, err := r.ReadBytes(32)
		if err != nil {
			return nil, err
		}
		value, err := r.ReadByteVec(metadataReturnBytes)
		if err != nil {
			return nil, err
		}
		m.ReturnData = &ReturnData{Data: value}
		copy(m.ReturnData.ProgramID[:], program)
	}
	if err := r.EnsureEOF(); err != nil {
		return nil, err
	}
	if _, err := m.encodedSize(); err != nil {
		return nil, err
	}
	return m, nil
}

func metadataCount(r *wincode.Reader, maxCount, minimumWidth int) (int, error) {
	count, err := r.ReadU16()
	if err != nil {
		return 0, err
	}
	if int(count) > maxCount || int(count)*minimumWidth > r.Remaining() {
		return 0, fmt.Errorf("%w: count %d", errMetadata, count)
	}
	return int(count), nil
}

func readTokenBalances(r *wincode.Reader) ([]TokenBalance, error) {
	count, err := metadataCount(r, metadataAccounts, tokenBalanceBytes)
	if err != nil {
		return nil, err
	}
	rows, err := r.ReadBytes(count * tokenBalanceBytes)
	if err != nil {
		return nil, err
	}
	balances := make([]TokenBalance, count)
	for i := range balances {
		row := rows[i*tokenBalanceBytes : (i+1)*tokenBalanceBytes]
		balance := &balances[i]
		balance.AccountIndex = binary.LittleEndian.Uint16(row[:2])
		copy(balance.Mint[:], row[2:34])
		copy(balance.Owner[:], row[34:66])
		copy(balance.ProgramID[:], row[66:98])
		balance.Amount = binary.LittleEndian.Uint64(row[98:106])
		balance.Decimals = row[106]
	}
	return balances, nil
}
