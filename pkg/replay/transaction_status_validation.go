package replay

import (
	"math"

	b "github.com/Overclock-Validator/mithril/pkg/block"
)

// transactionStatusValidation records a successful ancestor scan under cache.mu.
// It authorizes skipping only that scan, never the coverage, parent or exact
// block-identity checks. The receipt is private, bound to one cache instance and
// one immutable identity set, and checked under the publication lock.
//
// Mutating the visible index, binding the tip, rooting/pruning or restoring
// invalidates earlier receipts. In particular, committing and then unwinding to
// the same tip cannot resurrect one. Snapshot/Agave constructors create a new
// cache instance; this receipt is neither persisted nor usable after recovery.
// This optimization changes no crash-recovery or durable-checkpoint guarantee.
type transactionStatusValidation struct {
	cache      *TransactionStatusCache
	identities *b.PreparedTransactionMessageIdentities
	version    uint64
}

func (v transactionStatusValidation) reusableForLocked(c *TransactionStatusCache, identities *b.PreparedTransactionMessageIdentities) bool {
	return v.cache == c && v.identities == identities &&
		v.version == c.validationVersion && c.validationVersion != math.MaxUint64
}

// invalidateValidationLocked requires exclusive access (mu, or an unpublished
// constructor). Saturation permanently disables reuse instead of wrapping into
// an old generation. Even empty commits invalidate, because they change lineage.
func (c *TransactionStatusCache) invalidateValidationLocked() {
	if c.validationVersion != math.MaxUint64 {
		c.validationVersion++
	}
}
