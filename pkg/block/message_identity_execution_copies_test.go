package block

import (
	"errors"
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestPreparedTransactionMessageIdentitiesExecutionCopies(t *testing.T) {
	first, second := identityTestTransaction(1), identityTestTransaction(2)
	originals := []*solana.Transaction{first, second}
	prepared, err := (&Block{Transactions: originals}).PrepareTransactionMessageIdentities()
	if err != nil {
		t.Fatalf("prepare identities: %v", err)
	}

	copies, rebound, err := prepared.ExecutionCopies()
	if err != nil {
		t.Fatalf("execution copies: %v", err)
	}
	if len(copies) != 2 || copies[0] == first || copies[1] == second {
		t.Fatalf("copies must be distinct objects: %v", copies)
	}
	for i, tx := range copies {
		if &tx.Message.AccountKeys[0] == &originals[i].Message.AccountKeys[0] {
			t.Fatalf("copy %d shares the original's account-key storage", i)
		}
		if len(tx.Signatures) != 1 || tx.Signatures[0] != originals[i].Signatures[0] {
			t.Fatalf("copy %d signatures differ", i)
		}
		if tx.Message.RecentBlockhash != originals[i].Message.RecentBlockhash || len(tx.Message.Instructions) != 1 {
			t.Fatalf("copy %d message differs", i)
		}
	}
	if rebound.Len() != 2 || rebound.Identity(0) != prepared.Identity(0) || rebound.Identity(1) != prepared.Identity(1) {
		t.Fatalf("rebound identities differ: %+v vs %+v", rebound, prepared)
	}
	if !rebound.matches(copies) {
		t.Fatal("rebound set is not bound to the copies")
	}
	if rebound.matches(originals) {
		t.Fatal("rebound set must not claim the originals")
	}
	if !prepared.matches(originals) {
		t.Fatal("making copies must not alter the original set")
	}

	// Resolving a v0 copy leaves the original unresolved and still resolvable.
	tableID := solana.PublicKey{0x70}
	lookup := identityTestTransaction(3)
	lookup.Message.SetAddressTableLookups([]solana.MessageAddressTableLookup{{AccountKey: tableID, WritableIndexes: []byte{0}}})
	if _, err := lookup.Message.SetVersion(solana.MessageVersionV0); err != nil {
		t.Fatalf("set v0: %v", err)
	}
	staticKeys := len(lookup.Message.AccountKeys)
	prepared, err = (&Block{Transactions: []*solana.Transaction{lookup}}).PrepareTransactionMessageIdentities()
	if err != nil {
		t.Fatalf("prepare v0 identity: %v", err)
	}
	copies, _, err = prepared.ExecutionCopies()
	if err != nil {
		t.Fatalf("v0 execution copy: %v", err)
	}
	tables := map[solana.PublicKey]solana.PublicKeySlice{tableID: {{0x71}}}
	if err := copies[0].Message.SetAddressTables(tables); err != nil {
		t.Fatalf("resolve copy: %v", err)
	}
	if err := copies[0].Message.ResolveLookups(); err != nil {
		t.Fatalf("resolve copy: %v", err)
	}
	if !copies[0].Message.IsResolved() || len(copies[0].Message.AccountKeys) != staticKeys+1 {
		t.Fatal("copy did not resolve")
	}
	if lookup.Message.IsResolved() || len(lookup.Message.AccountKeys) != staticKeys {
		t.Fatal("resolving the copy touched the original")
	}
	if err := lookup.Message.SetAddressTables(tables); err != nil {
		t.Fatalf("the original must still accept its own resolution: %v", err)
	}

	// An already-resolved v0 transaction is refused.
	prepared, err = (&Block{Transactions: []*solana.Transaction{copies[0]}}).PrepareTransactionMessageIdentities()
	if err != nil {
		t.Fatalf("prepare resolved identity: %v", err)
	}
	if _, _, err := prepared.ExecutionCopies(); !errors.Is(err, ErrTransactionAlreadyResolved) {
		t.Fatalf("resolved input must be refused, got %v", err)
	}

	var none *PreparedTransactionMessageIdentities
	if _, _, err := none.ExecutionCopies(); err == nil {
		t.Fatal("a nil prepared set must be rejected")
	}
}
