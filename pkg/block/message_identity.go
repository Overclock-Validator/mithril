package block

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
)

// transactionState initializes the nonserialized holder under a short global
// lock. Message hashing itself is protected only by the per-block state lock,
// so unrelated blocks can prepare concurrently.
func (b *Block) transactionState() *transactionDerivedState {
	transactionDerivedStateInitMu.Lock()
	defer transactionDerivedStateInitMu.Unlock()
	if b.transactionDerivedState == nil {
		b.transactionDerivedState = &transactionDerivedState{}
	}
	return b.transactionDerivedState
}

// Len returns the number of identities in this immutable prepared set.
func (prepared *PreparedTransactionMessageIdentities) Len() int {
	if prepared == nil {
		return 0
	}
	return len(prepared.identities)
}

// Identity returns one prepared identity by transaction index.
func (prepared *PreparedTransactionMessageIdentities) Identity(index int) txstatus.TransactionMessageIdentity {
	return prepared.identities[index]
}

// MatchesBlock reports whether this prepared set is still bound to the
// block's ordered transaction pointers, message versions, and blockhashes.
// Signed message contents otherwise remain subject to Block's immutability
// contract; detecting arbitrary in-place edits would require hashing again.
func (prepared *PreparedTransactionMessageIdentities) MatchesBlock(block *Block) bool {
	return block != nil && prepared.matches(block.Transactions)
}

// Rebind returns the same identities bound to copies of the same ordered
// transactions: copies[i] must carry the message version and recent
// blockhash identity i was prepared for. Streaming execution runs
// stream-owned copies of a block's transactions (so address-table resolution
// never touches the block's own objects) while proving the block by the
// originals; the copies share the originals' identities.
func (prepared *PreparedTransactionMessageIdentities) Rebind(copies []*solana.Transaction) (*PreparedTransactionMessageIdentities, error) {
	if prepared == nil || len(copies) != len(prepared.identities) || len(copies) != len(prepared.versions) {
		return nil, fmt.Errorf("prepared identities do not cover %d transaction copies", len(copies))
	}
	for index, tx := range copies {
		if tx == nil || tx.Message.GetVersion() != prepared.versions[index] ||
			tx.Message.RecentBlockhash != prepared.identities[index].RecentBlockhash {
			return nil, fmt.Errorf("transaction copy %d does not match its prepared identity", index)
		}
	}
	return &PreparedTransactionMessageIdentities{
		transactions: append([]*solana.Transaction(nil), copies...),
		versions:     append([]solana.MessageVersion(nil), prepared.versions...),
		identities:   append([]txstatus.TransactionMessageIdentity(nil), prepared.identities...),
	}, nil
}

// Slice returns the prepared identities for transactions [from, to) as an
// independent prepared set bound to that sub-slice.
func (prepared *PreparedTransactionMessageIdentities) Slice(from, to int) *PreparedTransactionMessageIdentities {
	if prepared == nil || from < 0 || to > len(prepared.identities) || from > to {
		return nil
	}
	return &PreparedTransactionMessageIdentities{
		transactions: prepared.transactions[from:to:to],
		versions:     prepared.versions[from:to:to],
		identities:   prepared.identities[from:to:to],
	}
}
