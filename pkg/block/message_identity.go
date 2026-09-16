package block

import "github.com/Overclock-Validator/mithril/pkg/txstatus"

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
