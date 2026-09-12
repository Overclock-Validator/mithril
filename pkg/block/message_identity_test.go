package block

import (
	"encoding/json"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
)

func identityTestTransaction(messageByte byte) *solana.Transaction {
	return &solana.Transaction{
		Signatures: []solana.Signature{{messageByte}},
		Message: solana.Message{
			Header:          solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys:     []solana.PublicKey{{messageByte}, {0x44}},
			RecentBlockhash: solana.Hash{0x55},
			Instructions: []solana.CompiledInstruction{{
				ProgramIDIndex: 1,
				Accounts:       []uint16{0},
				Data:           []byte{messageByte},
			}},
		},
	}
}

func TestTransactionMessageIdentitiesReuseAcrossShallowBlockCopy(t *testing.T) {
	block := &Block{Transactions: []*solana.Transaction{
		identityTestTransaction(1),
		identityTestTransaction(2),
	}}
	first, err := block.PrepareTransactionMessageIdentities()
	if err != nil {
		t.Fatalf("prepare identities: %v", err)
	}
	second, err := block.PrepareTransactionMessageIdentities()
	if err != nil {
		t.Fatalf("reuse identities: %v", err)
	}
	if first != second {
		t.Fatal("same block did not reuse prepared identity storage")
	}

	candidate := *block
	copied, err := candidate.PrepareTransactionMessageIdentities()
	if err != nil {
		t.Fatalf("reuse identities through shallow copy: %v", err)
	}
	if first != copied {
		t.Fatal("shallow pre-consensus block copy did not share immutable identities")
	}
}

func TestTransactionMessageIdentitiesInvalidateOnTransactionSliceChange(t *testing.T) {
	tests := map[string]func([]*solana.Transaction) []*solana.Transaction{
		"replace": func(transactions []*solana.Transaction) []*solana.Transaction {
			transactions[0] = identityTestTransaction(3)
			return transactions
		},
		"reorder": func(transactions []*solana.Transaction) []*solana.Transaction {
			transactions[0], transactions[1] = transactions[1], transactions[0]
			return transactions
		},
		"append": func(transactions []*solana.Transaction) []*solana.Transaction {
			return append(transactions, identityTestTransaction(3))
		},
		"truncate": func(transactions []*solana.Transaction) []*solana.Transaction {
			return transactions[:1]
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			block := &Block{Transactions: []*solana.Transaction{
				identityTestTransaction(1),
				identityTestTransaction(2),
			}}
			if _, err := block.PrepareTransactionMessageIdentities(); err != nil {
				t.Fatalf("prepare original identities: %v", err)
			}
			originalCache := block.transactionDerivedState.messageIdentities
			block.MarkTransactionSignaturesVerified()
			block.Transactions = mutate(block.Transactions)

			identities, err := block.PrepareTransactionMessageIdentities()
			if err != nil {
				t.Fatalf("rebuild identities: %v", err)
			}
			if block.transactionDerivedState.messageIdentities == originalCache {
				t.Fatal("transaction slice change reused stale identity cache")
			}
			if block.TransactionSignaturesVerified() {
				t.Fatal("transaction slice change retained signature-verification trust")
			}
			if identities.Len() != len(block.Transactions) {
				t.Fatalf("identity count = %d, want %d", identities.Len(), len(block.Transactions))
			}
			for index, tx := range block.Transactions {
				want, err := txstatus.IdentityForTransaction(tx)
				if err != nil {
					t.Fatalf("hash transaction %d: %v", index, err)
				}
				if identities.Identity(index) != want {
					t.Fatalf("identity %d = %+v, want %+v", index, identities.Identity(index), want)
				}
			}
		})
	}
}

func TestTransactionMessageIdentitiesAreNotSerialized(t *testing.T) {
	original := &Block{Slot: 42, Transactions: []*solana.Transaction{identityTestTransaction(1)}}
	if _, err := original.PrepareTransactionMessageIdentities(); err != nil {
		t.Fatalf("prepare identities: %v", err)
	}

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal block: %v", err)
	}
	var decoded Block
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal block: %v", err)
	}
	if decoded.transactionDerivedState != nil {
		t.Fatal("prepared identities crossed a serialization boundary")
	}
	if _, err := decoded.PrepareTransactionMessageIdentities(); err != nil {
		t.Fatalf("prepare identities after serialization: %v", err)
	}
}

func TestFixupTxVersionsInvalidatesTransactionDerivedState(t *testing.T) {
	tx := identityTestTransaction(1)
	block := &Block{
		Transactions: []*solana.Transaction{tx},
		Versions:     []uint8{uint8(solana.MessageVersionV0)},
	}
	before, err := block.PrepareTransactionMessageIdentities()
	if err != nil {
		t.Fatalf("prepare legacy identity: %v", err)
	}
	block.MarkTransactionSignaturesVerified()

	if err := block.FixupTxVersions(); err != nil {
		t.Fatalf("fix up transaction versions: %v", err)
	}

	if tx.Message.GetVersion() != solana.MessageVersionV0 {
		t.Fatalf("message version = %d, want v0", tx.Message.GetVersion())
	}
	if block.transactionDerivedState.messageIdentities != nil {
		t.Fatal("version fixup retained prepared identities")
	}
	if block.TransactionSignaturesVerified() {
		t.Fatal("version fixup retained signature-verification trust")
	}
	after, err := block.PrepareTransactionMessageIdentities()
	if err != nil {
		t.Fatalf("prepare v0 identity: %v", err)
	}
	if before.Identity(0) == after.Identity(0) {
		t.Fatal("legacy and v0 canonical messages produced the same identity")
	}
}

func TestFixupTxVersionsRejectsMismatchedMetadata(t *testing.T) {
	block := &Block{
		Transactions: []*solana.Transaction{identityTestTransaction(1)},
		Versions:     []uint8{uint8(solana.MessageVersionLegacy), uint8(solana.MessageVersionV0)},
	}
	if err := block.FixupTxVersions(); err == nil {
		t.Fatal("mismatched transaction-version metadata was accepted")
	}
}

func TestFixupTxVersionsFailureIsAtomic(t *testing.T) {
	first := identityTestTransaction(1)
	second := identityTestTransaction(2)
	block := &Block{
		Transactions: []*solana.Transaction{first, second},
		Versions:     []uint8{uint8(solana.MessageVersionV0), 99},
	}
	if _, err := block.PrepareTransactionMessageIdentities(); err != nil {
		t.Fatalf("prepare identities: %v", err)
	}
	prepared := block.transactionDerivedState.messageIdentities
	block.MarkTransactionSignaturesVerified()

	if err := block.FixupTxVersions(); err == nil {
		t.Fatal("invalid transaction version was accepted")
	}
	if first.Message.GetVersion() != solana.MessageVersionLegacy || second.Message.GetVersion() != solana.MessageVersionLegacy {
		t.Fatal("failed fixup partially changed transaction versions")
	}
	if block.transactionDerivedState.messageIdentities != prepared {
		t.Fatal("failed fixup invalidated prepared identities")
	}
	if !block.TransactionSignaturesVerified() {
		t.Fatal("failed fixup invalidated signature-verification trust")
	}
}
