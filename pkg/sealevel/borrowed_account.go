package sealevel

import (
	"github.com/gagliardetto/solana-go"
	"go.firedancer.io/radiance/pkg/accounts"
	"go.firedancer.io/radiance/pkg/features"
	"go.firedancer.io/radiance/pkg/safemath"
)

type BorrowedAccount struct {
	TxCtx              *TransactionCtx
	InstrCtx           *InstructionCtx
	IndexInTransaction uint64
	IndexInInstruction uint64
	Account            *accounts.Account
}

func (acct *BorrowedAccount) Owner() solana.PublicKey {
	return acct.Account.Owner
}

func (acct *BorrowedAccount) Lamports() uint64 {
	return acct.Account.Lamports
}

func (acct *BorrowedAccount) Touch() error {
	err := acct.TxCtx.Accounts.Touch(acct.IndexInTransaction)
	if err != nil {
		return err
	}
	return nil
}

func (acct *BorrowedAccount) Data() []byte {
	return acct.Account.Data
}

func (acct *BorrowedAccount) SetData(features features.Features, data []byte) error {
	err := acct.DataCanBeChanged(features)
	if err != nil {
		return err
	}
	err = acct.Touch()
	if err != nil {
		return err
	}

	acct.Account.SetData(data)
	return nil
}

func (acct *BorrowedAccount) IsZeroed() bool {
	if len(acct.Data()) == 0 {
		return true
	}

	for _, b := range acct.Data() {
		if b != 0 {
			return false
		}
	}

	return true
}

func (acct *BorrowedAccount) SetOwner(f features.Features, owner solana.PublicKey) error {
	if !acct.IsOwnedByCurrentProgram() {
		return InstrErrModifiedProgramId
	}

	if !acct.IsWritable() {
		return InstrErrModifiedProgramId
	}

	if acct.IsExecutable(f) {
		return InstrErrModifiedProgramId
	}

	if !acct.IsZeroed() {
		return InstrErrModifiedProgramId
	}

	if acct.Owner() == owner {
		return nil
	}

	err := acct.Touch()
	if err != nil {
		return err
	}

	acct.Account.Owner = owner
	return nil
}

func (acct *BorrowedAccount) IsSigner() bool {
	instrCtx := acct.InstrCtx
	if acct.IndexInInstruction < instrCtx.NumberOfProgramAccounts() {
		return false
	}

	instrAcctIdx := safemath.SaturatingSubU64(acct.IndexInInstruction, instrCtx.NumberOfProgramAccounts())
	isSigner, err := instrCtx.IsInstructionAccountSigner(instrAcctIdx)
	if err != nil {
		return false
	}
	return isSigner
}

func (acct *BorrowedAccount) Key() solana.PublicKey {
	key, err := acct.TxCtx.KeyOfAccountAtIndex(acct.IndexInTransaction)
	if err != nil {
		panic("supposedly impossible failure")
	}
	return key
}

func (acct *BorrowedAccount) IsExecutable(features features.Features) bool {
	return acct.Account.IsBuiltin() || acct.Account.IsExecutable(features)
}

func (acct *BorrowedAccount) IsWritable() bool {
	instrCtx := acct.InstrCtx
	if acct.IndexInInstruction < instrCtx.NumberOfProgramAccounts() {
		return false
	}

	instrAcctIdx := safemath.SaturatingSubU64(acct.IndexInInstruction, instrCtx.NumberOfProgramAccounts())
	writable, err := instrCtx.IsInstructionAccountWritable(instrAcctIdx)
	if err != nil {
		return false
	}

	return writable
}

func (acct *BorrowedAccount) IsOwnedByCurrentProgram() bool {
	lastProgramKey, err := acct.InstrCtx.LastProgramKey(acct.TxCtx)
	if err != nil {
		return false
	}
	return lastProgramKey == acct.Owner()
}

func (acct *BorrowedAccount) DataCanBeChanged(features features.Features) error {
	if acct.IsExecutable(features) {
		return InstrErrExecutableDataModified
	}
	if !acct.IsWritable() {
		return InstrErrReadonlyDataModified
	}
	if !acct.IsOwnedByCurrentProgram() {
		return InstrErrExternalAccountDataModified
	}
	return nil
}

const MaxPermittedDataLength = 10 * 1024 * 1024

func (acct *BorrowedAccount) CanDataBeResized(newLen uint64) error {
	oldLen := len(acct.Data())
	if newLen != uint64(oldLen) && !acct.IsOwnedByCurrentProgram() {
		return InstrErrAccountDataSizeChanged
	}

	if newLen > MaxPermittedDataLength {
		return InstrErrInvalidRealloc
	}

	// TODO: support 'per-transaction maximum'

	return nil
}