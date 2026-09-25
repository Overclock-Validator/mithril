package accounts

import (
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/gagliardetto/solana-go"
)

type MemAccounts struct {
	Map map[[32]byte]*Account
	mu  *sync.RWMutex
}

// A miss is an ordinary step when falling back to parent accounts. Defer the
// diagnostic encoding until it is needed, and copy the key so callers can reuse it.
type missingMemAccountError struct {
	key [32]byte
}

func (e *missingMemAccountError) Error() string {
	return "no such account " + base58.Encode(e.key[:]) + " found"
}

func NewMemAccounts() MemAccounts {
	return MemAccounts{
		Map: make(map[[32]byte]*Account),
		mu:  &sync.RWMutex{},
	}
}

func NewMemAccountsWithLen(len uint64) MemAccounts {
	return MemAccounts{
		Map: make(map[[32]byte]*Account, len),
		mu:  &sync.RWMutex{},
	}
}

func (m MemAccounts) GetAccount(pubkey *[32]byte) (*Account, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	acct, ok := m.Map[*pubkey]
	if !ok {
		return nil, &missingMemAccountError{key: *pubkey}
	}
	return acct, nil
}

func (m MemAccounts) GetAccountWithoutLock(pubkey solana.PublicKey) (*Account, error) {
	acct, ok := m.Map[pubkey]
	if !ok {
		return nil, &missingMemAccountError{key: pubkey}
	}
	return acct, nil
}

func (m MemAccounts) SetAccount(pubkey *[32]byte, acct *Account) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Map[*pubkey] = acct
	return nil
}

func (m MemAccounts) SetTransactionAccounts(accountStates []*Account, touched []bool) error {
	if err := validateTransactionAccountBatch(accountStates, touched); err != nil {
		return err
	}
	m.setTransactionAccounts(accountStates, touched)
	return nil
}

func (m MemAccounts) setTransactionAccounts(accountStates []*Account, touched []bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for idx, acct := range accountStates {
		if touched[idx] {
			m.Map[acct.Key] = transactionAccountForStorage(acct)
		}
	}
}

func (m MemAccounts) SetAccountWithoutLock(pubkey solana.PublicKey, acct *Account) error {
	m.Map[pubkey] = acct
	return nil
}

func (m MemAccounts) AllAccounts() []*Account {
	m.mu.RLock()
	defer m.mu.RUnlock()
	accts := make([]*Account, 0, len(m.Map))

	for _, acct := range m.Map {
		accts = append(accts, acct)
	}

	return accts
}
