package sealevel

import "math"

// BankTransactionCounts records transactions processed in this bank, including
// failures and zero-fee no-ops. ComputeUnits sums the runtime-reported value;
// SIMD-0290 no-ops contribute their requested limit without instruction execution.
// The serial replay owner or producer commit lock updates it, never execution
// workers. It is not consensus or disk state; a discarded bank discards its
// counts. Readers use the settled value copy.
type BankTransactionCounts struct {
	Total, NonVote, Failed, NonVoteFailed uint64
	ComputeUnits                          uint64
	Saturated                             bool
}

// Record adds one included transaction result; overflow disables further accumulation.
func (c *BankTransactionCounts) Record(vote, failed bool, computeUnits uint64) {
	if c.Saturated {
		return
	}
	if c.Total == math.MaxUint64 || computeUnits > math.MaxUint64-c.ComputeUnits {
		c.Saturated = true
		return
	}
	c.Total++
	c.ComputeUnits += computeUnits
	if !vote {
		c.NonVote++
	}
	if failed {
		c.Failed++
		if !vote {
			c.NonVoteFailed++
		}
	}
}
