package accountsdb

import (
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
)

// ErrAccountDataLimit means an optional read was refused before allocating or
// copying account data larger than its caller's limit. It is not account absence.
var ErrAccountDataLimit = errors.New("account data limit exceeded")

// GetAccountWithDataLimit preserves GetAccount's source precedence and ownership
// semantics, but rejects oversized data before cloning or decoding it. The limit
// bounds this read's data allocation, not memory already retained by the store.
func (accountsDb *AccountsDb) GetAccountWithDataLimit(slot uint64, pubkey solana.PublicKey, maxDataBytes uint64) (*accounts.Account, error) {
	if accountsDb == nil {
		return nil, ErrNoAccount
	}
	accountsDb.appendVecReadMu.RLock()
	defer accountsDb.appendVecReadMu.RUnlock()
	acct, err := accountsDb.getStoreInProgressAccountWithDataLimit(pubkey, maxDataBytes)
	if err != nil || acct != nil {
		return acct, err
	}
	return accountsDb.getStoredAccountPinnedWithDataLimit(slot, pubkey, maxDataBytes)
}

func (accountsDb *AccountsDb) getStoreInProgressAccountWithDataLimit(pubkey solana.PublicKey, maxDataBytes uint64) (*accounts.Account, error) {
	accountsDb.inProgressStoreRequestsMu.Lock()
	defer accountsDb.inProgressStoreRequestsMu.Unlock()
	for e := accountsDb.inProgressStoreRequests.Back(); e != nil; e = e.Prev() {
		if acct := e.Value.(storeRequest).m[pubkey]; acct != nil {
			if uint64(len(acct.Data)) > maxDataBytes {
				return nil, accountDataLimitError(uint64(len(acct.Data)), maxDataBytes)
			}
			return acct.Clone(), nil
		}
	}
	return nil, nil
}

func accountDataLimitError(size, limit uint64) error {
	return fmt.Errorf("%w: %d bytes exceeds %d", ErrAccountDataLimit, size, limit)
}
