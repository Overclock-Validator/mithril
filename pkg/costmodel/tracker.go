package costmodel

import (
	"github.com/gagliardetto/solana-go"
)

// ExceedReason reports which block limit a transaction would breach.
type ExceedReason int

const (
	ExceedNone ExceedReason = iota
	ExceedBlockCost
	ExceedWritableAccountCost
	ExceedAllocatedDataSize
	ExceedBatchBytes
)

func (r ExceedReason) String() string {
	switch r {
	case ExceedBlockCost:
		return "block_cost"
	case ExceedWritableAccountCost:
		return "writable_account_cost"
	case ExceedAllocatedDataSize:
		return "allocated_data_size"
	case ExceedBatchBytes:
		return "batch_bytes"
	default:
		return "none"
	}
}

// CostTracker enforces per-slot block production budgets.
type CostTracker struct {
	limits Limits

	blockCost              uint64
	perWritableAccountCost map[solana.PublicKey]uint64
	allocatedDataSizeDelta uint64
}

func NewCostTracker(limits Limits) *CostTracker {
	return &CostTracker{
		limits:                 limits,
		perWritableAccountCost: make(map[solana.PublicKey]uint64),
	}
}

func (t *CostTracker) Limits() Limits {
	return t.limits
}

func (t *CostTracker) BlockCost() uint64 {
	return t.blockCost
}

func (t *CostTracker) WritableAccountCost(pk solana.PublicKey) uint64 {
	return t.perWritableAccountCost[pk]
}

func (t *CostTracker) AllocatedDataSizeDelta() uint64 {
	return t.allocatedDataSizeDelta
}

// WouldExceed returns whether admitting cost would breach a limit.
func (t *CostTracker) WouldExceed(cost TransactionCost) ExceedReason {
	sum := cost.Sum()
	if t.blockCost+sum > t.limits.BlockCost {
		return ExceedBlockCost
	}
	for _, pk := range cost.WritableAccounts {
		next := t.perWritableAccountCost[pk] + sum
		if next > t.limits.WritableAccountCost {
			return ExceedWritableAccountCost
		}
	}
	if t.allocatedDataSizeDelta+cost.AllocatedAccountsDataSize > t.limits.AllocatedDataSizeDelta {
		return ExceedAllocatedDataSize
	}
	return ExceedNone
}

// Record commits cost after a transaction is forged into the working bank.
func (t *CostTracker) Record(cost TransactionCost) {
	sum := cost.Sum()
	t.blockCost += sum
	for _, pk := range cost.WritableAccounts {
		t.perWritableAccountCost[pk] += sum
	}
	t.allocatedDataSizeDelta += cost.AllocatedAccountsDataSize
}

// Rebate replaces reserved execution and loaded-accounts cost with the actual
// usage. Signature, write-lock, and instruction-data costs are left as recorded.
func (t *CostTracker) Rebate(estimated TransactionCost, actualExec, actualLoaded uint64) {
	estimatedUsage := estimated.ProgramsExecutionCost + estimated.LoadedAccountsDataSizeCost
	actualUsage := actualExec + actualLoaded
	if actualUsage == estimatedUsage {
		return
	}
	if actualUsage < estimatedUsage {
		t.adjustUsage(estimated.WritableAccounts, estimatedUsage-actualUsage, false)
		return
	}
	t.adjustUsage(estimated.WritableAccounts, actualUsage-estimatedUsage, true)
}

func (t *CostTracker) adjustUsage(writable []solana.PublicKey, delta uint64, add bool) {
	if delta == 0 {
		return
	}
	if add {
		t.blockCost += delta
		for _, pk := range writable {
			t.perWritableAccountCost[pk] += delta
		}
		return
	}
	t.blockCost = satSub(t.blockCost, delta)
	for _, pk := range writable {
		t.perWritableAccountCost[pk] = satSub(t.perWritableAccountCost[pk], delta)
	}
}

func satSub(v, delta uint64) uint64 {
	if v < delta {
		return 0
	}
	return v - delta
}
