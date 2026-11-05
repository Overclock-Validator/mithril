package blockprod

import "github.com/Overclock-Validator/mithril/pkg/costmodel"

// LeaderBankCosts describes accepted bank contents, including native rebates
// and the bank's effective limits. It is not execution work spent on rejected
// or discarded attempts. EntryBytes counts serialized entries, not wire bytes.
type LeaderBankCosts struct {
	BlockCost              uint64
	AllocatedDataSizeDelta uint64
	EntryBytes             uint64
	Transactions           uint64
	Limits                 costmodel.Limits
}

// closedCostObservation copies scalars after admission closes. The caller only
// attaches them to a successful terminal, whose real path has already frozen
// the bank. A closed failed bank alone does not establish that outcome.
func (b *WorkingBank) closedCostObservation() (LeaderBankCosts, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.accepting || b.costs == nil || b.entries == nil {
		return LeaderBankCosts{}, false
	}
	entryBytes := b.entries.SlotBytes()
	if entryBytes < 0 {
		return LeaderBankCosts{}, false
	}
	return LeaderBankCosts{
		BlockCost: b.costs.BlockCost(), AllocatedDataSizeDelta: b.costs.AllocatedDataSizeDelta(),
		EntryBytes: uint64(entryBytes), Transactions: uint64(len(b.forgedTxs)), Limits: b.costs.Limits(),
	}, true
}
