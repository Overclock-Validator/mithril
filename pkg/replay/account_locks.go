package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
)

const (
	// MaxTxAccountLocks is the protocol's account lock limit once
	// IncreaseTxAccountLockLimit is active.
	MaxTxAccountLocks = 128
	// legacyTxAccountLockLimit applies before IncreaseTxAccountLockLimit.
	legacyTxAccountLockLimit = 64
	// accountLockSetThreshold is the key count from which a set beats a
	// pairwise scan for duplicate detection.
	accountLockSetThreshold = 32
)

// TransactionAccountLockLimit returns how many accounts a transaction may
// lock under f.
func TransactionAccountLockLimit(f *features.Features) int {
	if f != nil && f.IsActive(features.IncreaseTxAccountLockLimit) {
		return MaxTxAccountLocks
	}
	return legacyTxAccountLockLimit
}

// ValidateAccountLocks applies the Solana protocol's account lock rules: a
// transaction naming more accounts than the lock limit fails with
// TooManyAccountLocks, otherwise one naming any account twice fails with
// AccountLoadedTwice. Such a transaction can never be processed or charged,
// and a block containing one is invalid. solana-go only rejects duplicate
// keys for v1 messages, so legacy and v0 messages must be checked here.
//
// The account count includes address-table lookups. The keys of a v0 message
// whose lookups are not resolved yet are unknown, so only its static keys are
// checked for duplicates; check the resolved message again once its loaded
// addresses are appended.
func ValidateAccountLocks(tx *solana.Transaction, f *features.Features) *TransactionError {
	msg := &tx.Message
	numAccounts := len(msg.AccountKeys)
	if msg.GetVersion() == solana.MessageVersionV0 && !msg.IsResolved() {
		numAccounts += msg.AddressTableLookups.NumLookups()
	}
	if numAccounts > TransactionAccountLockLimit(f) {
		return &TransactionError{ErrorType: TransactionErrorTooManyAccountLocks}
	}
	if hasDuplicateAccountKeys(msg.AccountKeys) {
		return &TransactionError{ErrorType: TransactionErrorAccountLoadedTwice}
	}
	return nil
}

func hasDuplicateAccountKeys(keys solana.PublicKeySlice) bool {
	if len(keys) >= accountLockSetThreshold {
		seen := make(map[solana.PublicKey]struct{}, len(keys))
		for _, key := range keys {
			if _, ok := seen[key]; ok {
				return true
			}
			seen[key] = struct{}{}
		}
		return false
	}
	for i := 1; i < len(keys); i++ {
		for j := 0; j < i; j++ {
			if keys[i] == keys[j] {
				return true
			}
		}
	}
	return false
}
