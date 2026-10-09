package txverify

import (
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
)

// VerifiedMessageIdentity is an immutable result of signature verification.
// Its zero value is unusable. Signed message contents must remain immutable;
// as with Block's existing cache, arbitrary in-place edits require invalidation.
type VerifiedMessageIdentity struct {
	transaction *solana.Transaction
	version     solana.MessageVersion
	identity    txstatus.TransactionMessageIdentity
	verified    bool
}

// ForTransaction checks the binding without reserializing. Address-table
// resolution is allowed because it does not change the canonical message.
func (v VerifiedMessageIdentity) ForTransaction(tx *solana.Transaction) (txstatus.TransactionMessageIdentity, bool) {
	if !v.verified || tx == nil || tx != v.transaction || tx.Message.GetVersion() != v.version || tx.Message.RecentBlockhash != v.identity.RecentBlockhash {
		return txstatus.TransactionMessageIdentity{}, false
	}
	return v.identity, true
}
