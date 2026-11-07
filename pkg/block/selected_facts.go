package block

import "math"

// SelectedFeeFacts contains charged transaction fees, not the collector's net
// deposit, voting rewards, tips or execution compute units.
type SelectedFeeFacts struct {
	ExecutionLamports uint64
	PriorityLamports  uint64
	TotalLamports     uint64
}

// SelectedNativeCost contains the frozen local bank's accepted native cost and
// effective block limit, not execution CU or work spent on rejected attempts.
type SelectedNativeCost struct {
	BlockCost      uint64
	BlockCostLimit uint64
}

// SelectedEconomics is a detached execution-owner observation. Its publication
// still requires successful replay and exact selected/folded block evidence.
type SelectedEconomics struct {
	FeesKnown       bool
	Fees            SelectedFeeFacts
	FeeBurnKnown    bool
	FeeBurnLamports uint64
	NativeCostKnown bool
	NativeCost      SelectedNativeCost
}

type selectedEconomicsContext struct {
	value        SelectedEconomics
	local        bool
	sealed       bool
	slot         uint64
	blockID      [32]byte
	bankhash     [32]byte
	transactions uint64
}

// EnableSelectedEconomics starts a fresh private request before execution.
// The execution owner must not reset an adopted locally produced block.
func (b *Block) EnableSelectedEconomics() {
	if b != nil {
		b.selectedEconomics = &selectedEconomicsContext{}
	}
}

// EnableSelectedLocalEconomics requests facts before local commit. They remain
// unknown until successful finalization seals the exact handed-off identity.
func (b *Block) EnableSelectedLocalEconomics() {
	b.EnableSelectedEconomics()
	if b != nil {
		b.selectedEconomics.local = true
	}
}

// ResetSelectedFees invalidates a prior fee result before distribution starts.
// Only the execution owner may mutate this context, before publishing copies.
func (b *Block) ResetSelectedFees() {
	if b != nil && b.selectedEconomics != nil {
		b.selectedEconomics.value.FeesKnown = false
		b.selectedEconomics.value.Fees = SelectedFeeFacts{}
		b.selectedEconomics.value.FeeBurnKnown = false
		b.selectedEconomics.value.FeeBurnLamports = 0
		b.selectedEconomics.value.NativeCostKnown = false
		b.selectedEconomics.value.NativeCost = SelectedNativeCost{}
		b.selectedEconomics.sealed = false
	}
}

// RecordSelectedFeeBurn retains the distribution result only when it belongs
// to a known, arithmetically consistent charged-fee total.
func (b *Block) RecordSelectedFeeBurn(burn uint64) {
	if b == nil || b.selectedEconomics == nil {
		return
	}
	v := &b.selectedEconomics.value
	v.FeeBurnKnown, v.FeeBurnLamports = false, 0
	if v.FeesKnown && burn <= v.Fees.TotalLamports {
		v.FeeBurnKnown, v.FeeBurnLamports = true, burn
	}
}

// RecordSelectedFees records a checked triple only after successful fee
// distribution. Invalid arithmetic remains unknown, never a measured zero.
func (b *Block) RecordSelectedFees(execution, priority, total uint64) {
	if b == nil || b.selectedEconomics == nil {
		return
	}
	b.ResetSelectedFees()
	if execution > math.MaxUint64-priority || execution+priority != total {
		return
	}
	b.selectedEconomics.value.FeesKnown = true
	b.selectedEconomics.value.Fees = SelectedFeeFacts{
		ExecutionLamports: execution, PriorityLamports: priority, TotalLamports: total,
	}
}

// SelectedEconomics returns values without exposing its mutable private request.
// False means capture was not requested. Local requests require the exact
// carrier arguments to SelectedEconomicsFor and are unknown through this getter.
func (b *Block) SelectedEconomics() (SelectedEconomics, bool) {
	if b == nil || b.selectedEconomics == nil {
		return SelectedEconomics{}, false
	}
	if b.selectedEconomics.local || b.FromLocalProduction {
		return SelectedEconomics{}, true
	}
	return b.selectedEconomics.value, true
}

// SealSelectedLocalEconomics records detached frozen cost before local handoff.
// The producer calls it only after commit, canonical-parent recheck and both
// final broadcasts succeed. This method never changes protocol block fields.
func (b *Block) SealSelectedLocalEconomics(bankSlot uint64, bankhash [32]byte, transactions uint64, cost SelectedNativeCost) bool {
	if b == nil || b.selectedEconomics == nil || !b.selectedEconomics.local || b.selectedEconomics.sealed {
		return false
	}
	if bankSlot != b.Slot || bankhash == ([32]byte{}) || !b.HasExpectedBankhash || b.ExpectedBankhash != bankhash ||
		!b.HasAlpenglowBlockID || b.AlpenglowBlockID == ([32]byte{}) || transactions != uint64(len(b.Transactions)) {
		return false
	}
	c := b.selectedEconomics
	c.slot, c.blockID, c.bankhash, c.transactions = bankSlot, b.AlpenglowBlockID, bankhash, transactions
	c.value.NativeCost, c.value.NativeCostKnown = cost, true
	c.sealed = true
	return true
}

// SelectedEconomicsFor returns values for the actual selected carrier. A local
// mismatch leaves both fees and cost unknown without affecting replay/adoption.
// A remote request never gains local cost, even when its leader is this node.
func (b *Block) SelectedEconomicsFor(slot uint64, blockID, bankhash [32]byte, transactions uint64) (SelectedEconomics, bool) {
	if b == nil || b.selectedEconomics == nil {
		return SelectedEconomics{}, false
	}
	c := b.selectedEconomics
	if b.Slot != slot || transactions != uint64(len(b.Transactions)) {
		return SelectedEconomics{}, true
	}
	if !c.local {
		return b.SelectedEconomics()
	}
	if !c.sealed || c.slot != slot || c.blockID != blockID || c.bankhash != bankhash || c.transactions != transactions ||
		!b.HasAlpenglowBlockID || b.AlpenglowBlockID != c.blockID || !b.HasExpectedBankhash || b.ExpectedBankhash != c.bankhash {
		return SelectedEconomics{}, true
	}
	return c.value, true
}
