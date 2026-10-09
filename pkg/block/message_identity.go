package block

import (
	"errors"
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

// ErrTransactionAlreadyResolved reports a v0 transaction that already carries
// address-table resolution where an unresolved, wire-decoded object is
// required.
var ErrTransactionAlreadyResolved = errors.New("transaction address-table lookups are already resolved")

// ExecutionCopies returns execution copies of the transactions this set is
// bound to, together with the same identities bound to those copies.
//
// Streaming replay executes a block's transactions before the block is
// complete. Execution resolves a v0 transaction's address-table lookups in
// place (solana-go's SetAddressTables refuses a second call and ResolveLookups
// appends the looked-up keys to AccountKeys), so a bank that later turns out
// not to be the block's — or the block's, on a different parent — must never
// have run the block's own objects. The copies are made here, from the
// originals this set was prepared for, so an identity can only ever be
// attached to a copy of the very transaction it was computed from: each copy
// shares the original's signatures and instructions and takes the message by
// value with its own account-key slice, which is all that resolution mutates.
// The identity itself (canonical message hash, recent blockhash) is therefore
// the original's by construction; nothing here re-authenticates the signed
// message, and callers must not treat the copies as independently verified.
//
// A bound transaction that already carries resolution is refused with
// ErrTransactionAlreadyResolved: its account keys were derived elsewhere,
// against a parent this bank cannot vouch for.
func (prepared *PreparedTransactionMessageIdentities) ExecutionCopies() ([]*solana.Transaction, *PreparedTransactionMessageIdentities, error) {
	if prepared == nil || len(prepared.transactions) != len(prepared.identities) || len(prepared.transactions) != len(prepared.versions) {
		return nil, nil, errors.New("prepared identities are not bound to their transactions")
	}
	copies := make([]*solana.Transaction, len(prepared.transactions))
	for index, tx := range prepared.transactions {
		if tx == nil {
			return nil, nil, fmt.Errorf("transaction %d is nil", index)
		}
		if tx.Message.GetVersion() == solana.MessageVersionV0 && tx.Message.IsResolved() {
			return nil, nil, fmt.Errorf("transaction %d: %w", index, ErrTransactionAlreadyResolved)
		}
		message := tx.Message
		message.AccountKeys = append(solana.PublicKeySlice(nil), tx.Message.AccountKeys...)
		copies[index] = &solana.Transaction{Signatures: tx.Signatures, Message: message}
	}
	return copies, &PreparedTransactionMessageIdentities{
		transactions: copies,
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
