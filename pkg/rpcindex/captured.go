// Package rpcindex turns replayed blocks into the personal RPC indexes.
//
// Replay records full metadata only for transactions the watch filter
// matches (Captured) and hands each completed block to the Indexer, which
// keeps unfinalized blocks in memory and writes finalized ones to rpcdb.
package rpcindex

import (
	"encoding/binary"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
)

var (
	// SPLTokenProgram and Token2022Program own token accounts and mints.
	SPLTokenProgram  = solana.MustPublicKeyFromBase58("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
	Token2022Program = solana.MustPublicKeyFromBase58("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
)

// InnerInstruction is one CPI, with indices into the transaction's keys.
type InnerInstruction struct {
	ProgramIDIndex uint8
	Accounts       []uint8
	Data           []byte
	StackHeight    uint32
}

// InnerInstructions groups the CPIs made by one top-level instruction.
type InnerInstructions struct {
	Index        uint8
	Instructions []InnerInstruction
}

// TokenBalance is a token account's state before or after a transaction.
type TokenBalance struct {
	AccountIndex uint8
	Mint         solana.PublicKey
	Owner        solana.PublicKey
	ProgramID    solana.PublicKey
	Amount       uint64
	Decimals     uint8
}

// TokenAccountChange is a token account of a watched wallet that a block
// created or changed (Closed false), or closed or reassigned (Closed true).
type TokenAccountChange struct {
	Owner   solana.PublicKey
	Mint    solana.PublicKey
	Account solana.PublicKey
	Program solana.PublicKey
	Closed  bool
}

// Captured is the metadata replay recorded for one watched transaction.
type Captured struct {
	Signature solana.Signature
	// Keys are the transaction's account keys, static keys first, then keys
	// loaded from address lookup tables.
	Keys []solana.PublicKey
	// Err is the transaction error, nil on success.
	Err error
	// ErrJSON is the error in Agave's wire form, such as
	// {"InstructionError":[0,{"Custom":1}]}; nil on success.
	ErrJSON      []byte
	Fee          uint64
	ComputeUnits uint64
	PreBalances  []uint64
	PostBalances []uint64
	PreTokens    []TokenBalance
	PostTokens   []TokenBalance
	Logs         []string
	Inner        []InnerInstructions
}

// TokenAccountState decodes an initialized SPL Token or Token-2022 account.
func TokenAccountState(a *accounts.Account) (mint, owner solana.PublicKey, amount uint64, program solana.PublicKey, ok bool) {
	if a == nil {
		return mint, owner, 0, program, false
	}
	program = solana.PublicKey(a.Owner)
	mint, owner, amount, ok = TokenAccountFields(program, a.Lamports, a.Data)
	return mint, owner, amount, program, ok
}

// TokenAccountFields decodes an initialized token account from its owning
// program, lamports and data.
func TokenAccountFields(program solana.PublicKey, lamports uint64, data []byte) (mint, owner solana.PublicKey, amount uint64, ok bool) {
	if lamports == 0 || len(data) < 165 {
		return mint, owner, 0, false
	}
	switch program {
	case SPLTokenProgram:
		if len(data) > 165 {
			return mint, owner, 0, false // only Token-2022 accounts carry extensions
		}
	case Token2022Program:
		// With extensions, byte 165 is the account type; a mint with
		// extensions is padded to the same length and has type 1.
		if len(data) > 165 && data[165] != 2 {
			return mint, owner, 0, false
		}
	default:
		return mint, owner, 0, false
	}
	if data[108] == 0 { // AccountState::Uninitialized
		return mint, owner, 0, false
	}
	copy(mint[:], data[0:32])
	copy(owner[:], data[32:64])
	return mint, owner, binary.LittleEndian.Uint64(data[64:72]), true
}

// MintDecimals decodes a mint account's decimals.
func MintDecimals(a *accounts.Account) (uint8, bool) {
	if a == nil || len(a.Data) < 82 {
		return 0, false
	}
	program := solana.PublicKey(a.Owner)
	if program != SPLTokenProgram && program != Token2022Program {
		return 0, false
	}
	if a.Data[45] == 0 { // is_initialized
		return 0, false
	}
	return a.Data[44], true
}
