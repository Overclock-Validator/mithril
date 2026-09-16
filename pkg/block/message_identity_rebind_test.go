package block

import (
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestPreparedTransactionMessageIdentitiesRebindToCopies(t *testing.T) {
	first, second := identityTestTransaction(1), identityTestTransaction(2)
	prepared, err := (&Block{Transactions: []*solana.Transaction{first, second}}).PrepareTransactionMessageIdentities()
	if err != nil {
		t.Fatalf("prepare identities: %v", err)
	}
	copyOf := func(tx *solana.Transaction) *solana.Transaction {
		message := tx.Message
		message.AccountKeys = append(solana.PublicKeySlice(nil), tx.Message.AccountKeys...)
		return &solana.Transaction{Signatures: tx.Signatures, Message: message}
	}
	copies := []*solana.Transaction{copyOf(first), copyOf(second)}

	rebound, err := prepared.Rebind(copies)
	if err != nil {
		t.Fatalf("rebind: %v", err)
	}
	if rebound.Len() != 2 || rebound.Identity(0) != prepared.Identity(0) || rebound.Identity(1) != prepared.Identity(1) {
		t.Fatalf("rebound identities differ: %+v vs %+v", rebound, prepared)
	}
	if !rebound.matches(copies) {
		t.Fatal("rebound set is not bound to the copies")
	}
	if rebound.matches([]*solana.Transaction{first, second}) {
		t.Fatal("rebound set must not claim the originals")
	}
	if !prepared.matches([]*solana.Transaction{first, second}) {
		t.Fatal("rebinding must not alter the original set")
	}

	if _, err := prepared.Rebind(copies[:1]); err == nil {
		t.Fatal("a shorter copy list must be rejected")
	}
	wrongHash := copyOf(second)
	wrongHash.Message.RecentBlockhash = solana.Hash{0x56}
	if _, err := prepared.Rebind([]*solana.Transaction{copies[0], wrongHash}); err == nil {
		t.Fatal("a copy with a different recent blockhash must be rejected")
	}
	if _, err := prepared.Rebind([]*solana.Transaction{copies[0], nil}); err == nil {
		t.Fatal("a nil copy must be rejected")
	}
	var none *PreparedTransactionMessageIdentities
	if _, err := none.Rebind(copies); err == nil {
		t.Fatal("a nil prepared set must be rejected")
	}
}
