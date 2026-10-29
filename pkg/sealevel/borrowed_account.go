package sealevel

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/util"
	"github.com/gagliardetto/solana-go"
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

func (acct *BorrowedAccount) RentEpoch() uint64 {
	return acct.Account.RentEpoch
}

func (acct *BorrowedAccount) Touch() error {
	touchedAcct, err := acct.TxCtx.Accounts.Touch(acct.IndexInTransaction)
	if err != nil {
		return err
	}
	acct.Account = touchedAcct
	return nil
}

func (acct *BorrowedAccount) Data() []byte {
	return acct.Account.Data
}

func (acct *BorrowedAccount) SetData(features features.Features, data []byte) error {
	newLen := uint64(len(data))
	err := acct.CanDataBeResized(newLen)
	if err != nil {
		return err
	}

	err = acct.DataCanBeChanged(features)
	if err != nil {
		return err
	}
	replacedAcct, oldLen, err := acct.TxCtx.Accounts.replaceData(acct.IndexInTransaction, data)
	if err != nil {
		return err
	}

	acct.Account = replacedAcct
	acct.TxCtx.AccountsResizeDelta = (acct.TxCtx.AccountsResizeDelta + int64(newLen)) - int64(oldLen)

	return nil
}

func (acct *BorrowedAccount) SetState(f features.Features, data []byte) error {
	err := acct.DataCanBeChanged(f)
	if err != nil {
		return err
	}
	err = acct.Touch()
	if err != nil {
		return err
	}

	if len(data) > len(acct.Data()) {
		return InstrErrAccountDataTooSmall
	}

	copy(acct.Account.Data, data)
	return nil
}

func (acct *BorrowedAccount) DataMutable(f features.Features) ([]byte, error) {
	err := acct.DataCanBeChanged(f)
	if err != nil {
		return nil, err
	}

	err = acct.Touch()
	if err != nil {
		return nil, err
	}

	return acct.Account.Data, nil
}

func (acct *BorrowedAccount) ExtendFromSlice(f features.Features, data []byte) error {
	newLen := safemath.SaturatingAddU64(uint64(len(acct.Account.Data)), uint64(len(data)))
	err := acct.CanDataBeResized(newLen)
	if err != nil {
		return err
	}

	err = acct.DataCanBeChanged(f)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	err = acct.Touch()
	if err != nil {
		return err
	}

	acct.UpdateAccountsResizeDelta(newLen)

	acct.Account.Data = append(acct.Account.Data, data...)

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

	if !f.IsActive(features.RemoveAccountsExecutableFlagChecks) && acct.IsExecutable() {
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

func (acct *BorrowedAccount) IsExecutable() bool {
	return acct.Account.IsExecutable()
}

func (acct *BorrowedAccount) AccountExists() bool {
	defaultPubkey := solana.PublicKey{}
	hasOwner := acct.Owner() != defaultPubkey

	return acct.Lamports() > 0 || len(acct.Data()) > 0 || hasOwner
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

func (acct *BorrowedAccount) DataCanBeChanged(f features.Features) error {
	if !f.IsActive(features.RemoveAccountsExecutableFlagChecks) && acct.IsExecutable() {
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

func (acct *BorrowedAccount) SetDataLength(newLength uint64, f features.Features) error {
	err := acct.CanDataBeResized(newLength)
	if err != nil {
		return err
	}

	err = acct.DataCanBeChanged(f)
	if err != nil {
		return err
	}

	if uint64(len(acct.Data())) == newLength {
		return nil
	}

	err = acct.Touch()
	if err != nil {
		return err
	}
	acct.UpdateAccountsResizeDelta(newLength)
	acct.Account.Resize(newLength, 0)

	return nil
}

func (acct *BorrowedAccount) UpdateAccountsResizeDelta(newLength uint64) {
	acct.TxCtx.AccountsResizeDelta = (acct.TxCtx.AccountsResizeDelta + int64(newLength)) - int64(len(acct.Data()))
}

func (acct *BorrowedAccount) SetLamports(lamports uint64, f features.Features) error {
	if !acct.IsOwnedByCurrentProgram() && lamports < acct.Lamports() {
		return InstrErrExternalAccountLamportSpend
	}

	if !acct.IsWritable() {
		return InstrErrReadonlyLamportChange
	}

	if !f.IsActive(features.RemoveAccountsExecutableFlagChecks) && acct.IsExecutable() {
		return InstrErrExecutableLamportChange
	}

	if acct.Lamports() == lamports {
		return nil
	}

	err := acct.Touch()
	if err != nil {
		return err
	}

	acct.Account.SetLamports(lamports)
	return nil
}

func (acct *BorrowedAccount) SetExecutable(f features.Features, isExecutable bool) error {
	if !acct.TxCtx.Rent.IsExempt(acct.Lamports(), uint64(len(acct.Data()))) {
		return InstrErrExecutableAccountNotRentExempt
	}

	if !acct.IsOwnedByCurrentProgram() {
		return InstrErrExecutableModified
	}

	if !acct.IsWritable() {
		return InstrErrExecutableModified
	}

	// can't remove executable flag
	if !f.IsActive(features.RemoveAccountsExecutableFlagChecks) && acct.IsExecutable() && !isExecutable {
		return InstrErrExecutableModified
	}

	if acct.IsExecutable() == isExecutable {
		return nil
	}

	err := acct.Touch()
	if err != nil {
		return err
	}

	acct.Account.SetExecutable(isExecutable)
	return nil
}

func (acct *BorrowedAccount) CheckedSubLamports(lamports uint64, f features.Features) error {
	newLamports, err := safemath.CheckedSubU64(acct.Lamports(), lamports)
	if err != nil {
		return InstrErrArithmeticOverflow
	}
	return acct.SetLamports(newLamports, f)
}

func (acct *BorrowedAccount) CheckedAddLamports(lamports uint64, f features.Features) error {
	newLamports, err := safemath.CheckedAddU64(acct.Lamports(), lamports)
	if err != nil {
		return InstrErrArithmeticOverflow
	}
	return acct.SetLamports(newLamports, f)
}

func (acct *BorrowedAccount) IsRentExemptAtDataLength(len uint64) bool {
	return acct.TxCtx.Rent.IsExempt(acct.Lamports(), len)
}

func (acct *BorrowedAccount) Drop() {
	acct.TxCtx.Accounts.Unlock(acct.IndexInTransaction)
}

func (acct BorrowedAccount) String() string {
	return fmt.Sprintf("acct - slot: %d, pubkey: %s, owner: %s, lamports: %d, executable: %t, isWritable = %t, isSigner = %t, rent epoch: %d, data len: %d, data hash: %s\n", acct.Account.Slot, acct.Account.Key, solana.PublicKeyFromBytes(acct.Account.Owner[:]), acct.Account.Lamports, acct.Account.Executable, acct.IsWritable(), acct.IsSigner(), acct.Account.RentEpoch, len(acct.Account.Data), solana.HashFromBytes(util.CalculateAcctHash(*acct.Account)))
}
