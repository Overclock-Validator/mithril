package block

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

// PrepareVerifiedTransactionMessageIdentities binds identities from joined
// signature-verification requests to the exact ordered transaction slice they
// were computed for. Every identity must be a verified result for the
// transaction at the same index; failed/partial requests cannot seed a set.
func PrepareVerifiedTransactionMessageIdentities(transactions []*solana.Transaction, identities []txverify.VerifiedMessageIdentity) (*PreparedTransactionMessageIdentities, error) {
	if len(identities) != len(transactions) {
		return nil, fmt.Errorf("verified message identities do not cover transactions")
	}
	prepared := &PreparedTransactionMessageIdentities{
		transactions: append([]*solana.Transaction(nil), transactions...),
		versions:     make([]solana.MessageVersion, len(identities)),
		identities:   make([]txstatus.TransactionMessageIdentity, len(identities)),
	}
	for i, tx := range transactions {
		identity, ok := identities[i].ForTransaction(tx)
		if !ok {
			return nil, fmt.Errorf("verified message identity does not match transaction %d", i)
		}
		prepared.versions[i] = tx.Message.GetVersion()
		prepared.identities[i] = identity
	}
	return prepared, nil
}

// CacheVerifiedTransactionMessageIdentities publishes identities from joined
// signature-verification requests. Every result must cover the exact ordered
// transaction slice. Failed/partial requests and obsolete prefetch generations
// cannot seed the cache. It does not replace block-wide duplicate/status checks.
func (b *Block) CacheVerifiedTransactionMessageIdentities(identities []txverify.VerifiedMessageIdentity) error {
	if b == nil {
		return fmt.Errorf("verified message identities do not cover block transactions")
	}
	prepared, err := PrepareVerifiedTransactionMessageIdentities(b.Transactions, identities)
	if err != nil {
		if len(identities) != len(b.Transactions) {
			return fmt.Errorf("verified message identities do not cover block transactions")
		}
		return err
	}
	state := b.transactionState()
	state.mu.Lock()
	defer state.mu.Unlock()
	state.messageIdentities = prepared
	return nil
}
