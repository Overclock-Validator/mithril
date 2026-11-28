package accountsdb

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
)

// ErrAccountDataLimit means an optional read was refused before allocating or
// copying account data larger than its caller's limit. It is not account absence.
var ErrAccountDataLimit = errors.New("account data limit exceeded")

// accountDataBudget charges every output occurrence and attempted cold decode.
// Reservations are monotonic: failed reads and discarded views are not refunded.
// A nil budget preserves the existing unbounded replay/read behavior.
type accountDataBudget struct {
	limit   uint64
	charged atomic.Uint64
}

func (b *accountDataBudget) reserve(size uint64) error {
	if b == nil {
		return nil
	}
	for {
		used := b.charged.Load()
		if size > b.limit-used {
			return accountDataLimitError(size, b.limit-used)
		}
		if b.charged.CompareAndSwap(used, used+size) {
			return nil
		}
	}
}

// GetAccountWithDataLimit preserves GetAccount's source precedence and ownership
// semantics, but rejects oversized data before cloning or decoding it. The limit
// bounds this read's data allocation, not memory already retained by the store.
func (accountsDb *AccountsDb) GetAccountWithDataLimit(slot uint64, pubkey solana.PublicKey, maxDataBytes uint64) (*accounts.Account, error) {
	if accountsDb == nil {
		return nil, ErrNoAccount
	}
	accountsDb.appendVecReadMu.RLock()
	defer accountsDb.appendVecReadMu.RUnlock()
	return accountsDb.getStoredAccountPinnedWithDataLimit(slot, pubkey, maxDataBytes)
}

func accountDataLimitError(size, limit uint64) error {
	return fmt.Errorf("%w: %d bytes exceeds %d", ErrAccountDataLimit, size, limit)
}
