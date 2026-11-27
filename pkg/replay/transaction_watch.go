package replay

import (
	"errors"
	"fmt"

	"github.com/gagliardetto/solana-go"
)

// maxWatchedAddresses bounds the watch set. Matching is one map probe per
// account key of every transaction in a block.
const maxWatchedAddresses = 4096

// TransactionWatchSet limits history capture to transactions that name one of
// its addresses. A nil set captures every transaction. It is immutable.
type TransactionWatchSet struct {
	addresses map[solana.PublicKey]struct{}
	// signature, when set, selects that one transaction instead.
	signature *solana.Signature
}

// watchSignature selects exactly the transaction with this signature.
func watchSignature(signature solana.Signature) *TransactionWatchSet {
	return &TransactionWatchSet{signature: &signature}
}

// NewTransactionWatchSet parses base58 addresses. No addresses means no watch
// set (nil): every transaction is captured.
func NewTransactionWatchSet(addresses []string) (*TransactionWatchSet, error) {
	if len(addresses) == 0 {
		return nil, nil
	}
	if len(addresses) > maxWatchedAddresses {
		return nil, fmt.Errorf("at most %d watched addresses are supported, got %d", maxWatchedAddresses, len(addresses))
	}
	set := &TransactionWatchSet{addresses: make(map[solana.PublicKey]struct{}, len(addresses))}
	for _, text := range addresses {
		key, err := solana.PublicKeyFromBase58(text)
		if err != nil {
			return nil, fmt.Errorf("watched address %q: %w", text, err)
		}
		set.addresses[key] = struct{}{}
	}
	if len(set.addresses) != len(addresses) {
		return nil, errors.New("watched addresses contain a duplicate")
	}
	return set, nil
}

// Len is the number of watched addresses; zero for a nil set.
func (set *TransactionWatchSet) Len() int {
	if set == nil {
		return 0
	}
	return len(set.addresses)
}

// touches reports whether the transaction names a watched address. Replay
// resolves address-table lookups first, so looked-up keys count.
func (set *TransactionWatchSet) touches(tx *solana.Transaction) bool {
	if tx == nil {
		return false
	}
	if set.signature != nil {
		return len(tx.Signatures) > 0 && tx.Signatures[0] == *set.signature
	}
	for _, key := range tx.Message.AccountKeys {
		if _, ok := set.addresses[key]; ok {
			return true
		}
	}
	return false
}

// selection marks the transactions to capture and returns them. A nil set
// selects all and returns a nil mask.
func (set *TransactionWatchSet) selection(txs []*solana.Transaction) (mask []bool, selected []*solana.Transaction) {
	if set == nil {
		return nil, txs
	}
	mask = make([]bool, len(txs))
	for i, tx := range txs {
		if set.touches(tx) {
			mask[i] = true
			selected = append(selected, tx)
		}
	}
	return mask, selected
}
