package accounts

import (
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
)

func TestMemAccountMissingErrorRetainsLookupKey(t *testing.T) {
	mem := NewMemAccounts()
	key := [32]byte{}
	_, lockedErr := mem.GetAccount(&key)
	_, unlockedErr := mem.GetAccountWithoutLock(solana.PublicKey(key))
	key[0] = 99 // Lookup callers may reuse their key storage before reporting an error.
	for _, err := range []error{lockedErr, unlockedErr} {
		if err == nil || err.Error() != "no such account 11111111111111111111111111111111 found" {
			t.Fatalf("missing error lost its original key: %v", err)
		}
	}
}

func TestMemAccountsReadsAreConcurrent(t *testing.T) {
	mem := NewMemAccounts()
	var key [32]byte
	key[0] = 1
	if err := mem.SetAccount(&key, &Account{Key: key}); err != nil {
		t.Fatalf("set account: %v", err)
	}

	// Hold one read lock while GetAccount acquires another. This models
	// parallel transactions falling through an overlay to an immutable parent.
	mem.mu.RLock()
	done := make(chan error, 1)
	go func() {
		_, err := mem.GetAccount(&key)
		done <- err
	}()

	select {
	case err := <-done:
		mem.mu.RUnlock()
		if err != nil {
			t.Fatalf("concurrent read: %v", err)
		}
	case <-time.After(time.Second):
		mem.mu.RUnlock()
		t.Fatal("account read serialized behind another reader")
	}
}
