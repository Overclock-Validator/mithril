package block

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

// CacheVerifiedTransactionMessageIdentities publishes identities from joined
// signature-verification requests. Every result must cover the exact ordered
// transaction slice. Failed/partial requests and obsolete prefetch generations
// cannot seed the cache. It does not replace block-wide duplicate/status checks.
func (b *Block) CacheVerifiedTransactionMessageIdentities(identities []txverify.VerifiedMessageIdentity) error {
	if b == nil || len(identities) != len(b.Transactions) {
		return fmt.Errorf("verified message identities do not cover block transactions")
	}
	prepared := &PreparedTransactionMessageIdentities{
		transactions: append([]*solana.Transaction(nil), b.Transactions...),
		versions:     make([]solana.MessageVersion, len(identities)),
		identities:   make([]txstatus.TransactionMessageIdentity, len(identities)),
	}
	for i, tx := range b.Transactions {
		identity, ok := identities[i].ForTransaction(tx)
		if !ok {
			return fmt.Errorf("verified message identity does not match transaction %d", i)
		}
		prepared.versions[i] = tx.Message.GetVersion()
		prepared.identities[i] = identity
	}
	state := b.transactionState()
	state.mu.Lock()
	defer state.mu.Unlock()
	state.messageIdentities = prepared
	return nil
}
