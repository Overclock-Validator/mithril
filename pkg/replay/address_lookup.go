package replay

import (
	"errors"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

var (
	TxErrAddressLookupTableNotFound     = errors.New("TxErrAddressLookupTableNotFound")
	TxErrInvalidAddressLookupTableOwner = errors.New("TxErrInvalidAddressLookupTableOwner")
	TxErrInvalidAddressLookupTableData  = errors.New("TxErrInvalidAddressLookupTableData")
	TxErrInvalidAddressLookupTableIndex = errors.New("TxErrInvalidAddressLookupTableIndex")
)

// activeLookupTable applies the Solana protocol's table rules to a table
// account as read for slot, judging deactivation against the SlotHashes of
// the bank executing at slot. It returns the table's active addresses:
//
//   - a missing table, or a deactivated one, fails with AddressLookupTableNotFound;
//   - a table not owned by the address lookup table program fails with
//     InvalidAddressLookupTableOwner, and one that does not decode with
//     InvalidAddressLookupTableData.
func activeLookupTable(acct *accounts.Account, slot uint64, slotHashes sealevel.SysvarSlotHashes) (solana.PublicKeySlice, *TransactionError) {
	if acct == nil || acct.Lamports == 0 {
		return nil, &TransactionError{ErrorType: TransactionErrorAddressLookupTableNotFound, InstructionError: TxErrAddressLookupTableNotFound}
	}
	if acct.Owner != addresses.AddressLookupTableAddr {
		return nil, &TransactionError{ErrorType: TransactionErrorInvalidAddressLookupTableOwner, InstructionError: TxErrInvalidAddressLookupTableOwner}
	}
	table, err := sealevel.UnmarshalAddressLookupTable(acct.Data)
	if err != nil {
		return nil, &TransactionError{ErrorType: TransactionErrorInvalidAddressLookupTableData, InstructionError: TxErrInvalidAddressLookupTableData}
	}
	// A deactivated table may be closed at any point, so it is reported as
	// missing rather than with an error of its own.
	if table.Meta.Status(slot, slotHashes).Status == sealevel.AddressLookupTableStatusTypeDeactivated {
		return nil, &TransactionError{ErrorType: TransactionErrorAddressLookupTableNotFound, InstructionError: TxErrAddressLookupTableNotFound}
	}

	activeLen := len(table.Addresses)
	if slot <= table.Meta.LastExtendedSlot {
		activeLen = int(table.Meta.LastExtendedSlotStartIndex)
	}
	if activeLen > len(table.Addresses) {
		return nil, &TransactionError{ErrorType: TransactionErrorInvalidAddressLookupTableData, InstructionError: TxErrInvalidAddressLookupTableData}
	}
	return table.Addresses[:activeLen], nil
}

// checkLookupIndexes fails with InvalidAddressLookupTableIndex if the lookup
// uses an index at or past the table's active length.
func checkLookupIndexes(active solana.PublicKeySlice, lookup *solana.MessageAddressTableLookup) *TransactionError {
	for _, indexes := range [2][]uint8{lookup.WritableIndexes, lookup.ReadonlyIndexes} {
		for _, idx := range indexes {
			if int(idx) >= len(active) {
				return &TransactionError{ErrorType: TransactionErrorInvalidAddressLookupTableIndex, InstructionError: TxErrInvalidAddressLookupTableIndex}
			}
		}
	}
	return nil
}

// HasAddressTableLookups reports whether tx is a v0 transaction that loads
// accounts from address lookup tables.
func HasAddressTableLookups(tx *solana.Transaction) bool {
	return tx != nil && tx.Message.GetVersion() == solana.MessageVersionV0 && tx.Message.AddressTableLookups.NumLookups() != 0
}

// ResolveAddressTableLookups returns a copy of tx whose address-table lookups
// are resolved for the slot of slotCtx, the bank the transaction executes in.
//
// Tables are read from the bank's parent state, never from writes made in the
// slot itself, and give the same addresses the slot's own state would: a
// table extended during the slot keeps the new addresses inactive until the
// slot has passed, because the address lookup table program records the
// address count at the slot's first extension as the active length; a table
// deactivated during the slot is still deactivating and usable; and a table
// created during the slot has no active address yet. Reading the parent also
// keeps the result independent of which transactions of the slot ran first.
//
// Lookups follow the Solana protocol's rules:
//
//   - a missing table, or a deactivated one, fails with AddressLookupTableNotFound;
//   - a table not owned by the address lookup table program fails with
//     InvalidAddressLookupTableOwner, and one that does not decode with
//     InvalidAddressLookupTableData;
//   - an index at or past the table's active length fails with
//     InvalidAddressLookupTableIndex.
//
// tx is never modified: resolution appends the loaded addresses to the account
// keys, and the original must stay unresolved for entry encoding. The copy
// serializes identically to the original, since only static keys are encoded.
// Transactions without lookups are returned as is.
func ResolveAddressTableLookups(slotCtx *sealevel.SlotCtx, tx *solana.Transaction) (*solana.Transaction, *TransactionError) {
	if !HasAddressTableLookups(tx) {
		return tx, nil
	}
	if tx.Message.IsResolved() {
		// Its loaded addresses came from some other bank, which cannot be
		// vouched for here.
		return nil, &TransactionError{ErrorType: TransactionErrorSanitizeFailure, InstructionError: TxErrSanitizeFailure}
	}
	slotHashes, ok := slotHashesForAddressLookups(slotCtx)
	if !ok {
		return nil, &TransactionError{ErrorType: TransactionErrorSanitizeFailure, InstructionError: TxErrSanitizeFailure}
	}

	tables := make(map[solana.PublicKey]solana.PublicKeySlice, len(tx.Message.AddressTableLookups))
	for i := range tx.Message.AddressTableLookups {
		lookup := &tx.Message.AddressTableLookups[i]
		active, txErr := activeLookupTable(parentLookupTableAccount(slotCtx, lookup.AccountKey), slotCtx.Slot, slotHashes)
		if txErr == nil {
			txErr = checkLookupIndexes(active, lookup)
		}
		if txErr != nil {
			return nil, txErr
		}
		tables[lookup.AccountKey] = active
	}

	resolved := *tx
	resolved.Message = tx.Message
	resolved.Message.AccountKeys = append(solana.PublicKeySlice(nil), tx.Message.AccountKeys...)
	if err := resolved.Message.SetAddressTables(tables); err != nil {
		return nil, &TransactionError{ErrorType: TransactionErrorInvalidAddressLookupTableIndex, InstructionError: TxErrInvalidAddressLookupTableIndex}
	}
	if err := resolved.Message.ResolveLookups(); err != nil {
		return nil, &TransactionError{ErrorType: TransactionErrorInvalidAddressLookupTableIndex, InstructionError: TxErrInvalidAddressLookupTableIndex}
	}
	return &resolved, nil
}

// parentLookupTableAccount reads a table from slotCtx's parent state: the
// unrooted chain the bank was built on, then durable AccountsDb. It returns
// nil if the table does not exist there.
func parentLookupTableAccount(slotCtx *sealevel.SlotCtx, key solana.PublicKey) *accounts.Account {
	if slotCtx.UnrootedRead == nil && slotCtx.AccountsDb == nil {
		return nil
	}
	acct, err := slotCtx.GetAccountFromAccountsDb(key)
	if err != nil {
		return nil
	}
	return acct
}

// slotHashesForAddressLookups prefers the bank's pinned SlotHashes, as the
// leader's bank carries them, and falls back to the process-wide sysvar cache.
func slotHashesForAddressLookups(slotCtx *sealevel.SlotCtx) (sealevel.SysvarSlotHashes, bool) {
	if bankSysvars := slotCtx.BankSysvars(); bankSysvars != nil {
		return bankSysvars.SlotHashes()
	}
	if sealevel.SysvarCache.SlotHashes.Sysvar == nil {
		return nil, false
	}
	return *sealevel.SysvarCache.SlotHashes.Sysvar, true
}
