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
