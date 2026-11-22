package replay

import (
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// SelectedSlotCapacity bounds the detached recent projection, not the existing
// 512-entry settlement window or its 1,024 pending-carrier limit.
const SelectedSlotCapacity = 256

// SelectedReplayFacts belongs to one successfully published replay result.
// ReplayResultAt is the original SlotReplayResult.At, not the later completion
// of post-processing or a fold clock. It retains its monotonic component.
type SelectedReplayFacts struct {
	ReplayResultAt              time.Time
	Epoch, FirstSlot, SlotCount uint64
	ExecutionLeader             solana.PublicKey
	FromLocalProduction         bool
	// Economics contains independent knownness for charged fees and the
	// matching local producer's frozen native cost. Neither is net revenue.
	Economics block.SelectedEconomics
}

// SelectedSlotRecord is one exact folded layer. Gap means the carrier or its
// replay facts are unavailable/conflicting; it never means a skipped slot.
// Economics knownness is independent and does not itself make a gap.
// Gap records retain only their fold evidence, not untrusted carrier values.
type SelectedSlotRecord struct {
	CertificateFoldEvidence
	Gap                                    bool
	Facts                                  SelectedReplayFacts
	ParentSlot, ExecutionParentSlot        uint64
	ParentBlockID, ExecutionParentBankhash solana.Hash
	HasParentBlockID                       bool
	Transactions                           sealevel.BankTransactionCounts
}

// SelectedSlotSnapshot is a detached oldest-to-newest projection. Dates on
// facts remain historical; UpdatedAt is the existing settlement observation.
// It contains only this replay attempt, not restart-persistent block history.
type SelectedSlotSnapshot struct {
	RunID                            string
	Generation, Attempt, Rewinds     uint64
	StartedAt, ResetAt, UpdatedAt    time.Time
	WindowEntries, OmittedFromWindow uint64
	FactConflicts                    uint64
	// CoverageIncomplete concerns retained record facts or lost settlement
	// evidence, not complete slot/epoch history. Numeric gaps are not skipped
	// rows; OmittedFromWindow separately counts older unprojected entries.
	// Individual economics knownness is separate from this coverage flag.
	CoverageIncomplete bool
	Records            []SelectedSlotRecord
}

// SelectedSlotsSnapshot returns a detached cached copy. It never accesses the
// live settlement owner; a rewind fences the last copy until the next publish.
func (p *Progress) SelectedSlotsSnapshot() (SelectedSlotSnapshot, bool) {
	if p == nil {
		return SelectedSlotSnapshot{}, false
	}
	view, d := p.selectedSlots.Load(), p.diagnostics.Load()
	if view == nil || d == nil {
		return SelectedSlotSnapshot{}, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	s, ok := p.Snapshot()
	if !ok || d.exhausted || view.Generation != d.generation || view.Attempt != d.attempt ||
		view.Rewinds != d.rewinds || s.Attempt != view.Attempt || s.Rewinds != view.Rewinds {
		return SelectedSlotSnapshot{}, false
	}
	copy := *view
	copy.Records = append([]SelectedSlotRecord(nil), view.Records...)
	return copy, true
}

// enableSelectedSlots pins the existing diagnostics owner and attempt. The
// settlement owner remains replay-goroutine-only; no second pending store exists.
func (p *Progress) enableSelectedSlots(r *CertificateOutcomes) {
	if p == nil || r == nil {
		return
	}
	d := p.diagnostics.Load()
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	s, ok := p.Snapshot()
	if ok && !d.exhausted && d.generation != 0 && d.attempt == s.Attempt && d.rewinds == s.Rewinds {
		r.selectedOwner, r.selectedAttempt = d, s.Attempt
	}
}

func selectedReplayFacts(at time.Time, b *block.Block, schedule *sealevel.SysvarEpochSchedule, carrier CertificateCarrier) (SelectedReplayFacts, bool) {
	counts := carrier.Transactions
	if b == nil || carrier.Slot != b.Slot || !admissionScheduleValid(schedule) || !selectedCountsValid(counts) ||
		(!counts.Saturated && counts.Total != uint64(len(b.Transactions))) {
		return SelectedReplayFacts{}, false
	}
	epoch, index := schedule.GetEpochAndSlotIndex(b.Slot)
	if epoch != b.Epoch || index > b.Slot {
		return SelectedReplayFacts{}, false
	}
	facts := SelectedReplayFacts{ReplayResultAt: at, Epoch: epoch, FirstSlot: b.Slot - index,
		SlotCount: schedule.SlotsInEpoch(epoch), ExecutionLeader: b.Leader, FromLocalProduction: b.FromLocalProduction}
	facts.Economics, _ = b.SelectedEconomicsFor(carrier.Slot, carrier.BlockID, carrier.Bankhash, counts.Total)
	return facts, facts.valid(b.Slot)
}

func (f SelectedReplayFacts) valid(slot uint64) bool {
	return !f.ReplayResultAt.IsZero() && f.ReplayResultAt.Year() >= 1 && f.ReplayResultAt.Year() <= 9999 &&
		f.SlotCount != 0 && f.FirstSlot <= slot && slot-f.FirstSlot < f.SlotCount &&
		f.SlotCount-1 <= math.MaxUint64-f.FirstSlot && f.Epoch <= f.FirstSlot &&
		(f.Epoch == 0) == (f.FirstSlot == 0)
}

func selectedCountsValid(c sealevel.BankTransactionCounts) bool {
	return c.NonVote <= c.Total && c.Failed <= c.Total && c.NonVoteFailed <= c.NonVote &&
		c.NonVoteFailed <= c.Failed && c.Failed-c.NonVoteFailed <= c.Total-c.NonVote
}

// observeSelectedFacts only augments a successfully staged exact carrier.
// Conflicting optional facts never alter ordinary carrier equality or outcomes.
func (r *CertificateOutcomes) observeSelectedFacts(c CertificateCarrier, facts SelectedReplayFacts) bool {
	if r == nil || r.selectedOwner == nil {
		return false
	}
	key := certificateCarrierKey{c.Slot, c.BlockID}
	pending, ok := r.pending[key]
	if !ok || pending.conflict || pending.carrier != c {
		return false
	}
	if pending.selectedConflict {
		return false
	}
	// Invalid or absent economics never erase otherwise valid replay facts.
	// Native cost is independent of fee knownness and may exceed its limit.
	fees := facts.Economics.Fees
	if !facts.Economics.FeesKnown || fees.ExecutionLamports > math.MaxUint64-fees.PriorityLamports ||
		fees.ExecutionLamports+fees.PriorityLamports != fees.TotalLamports {
		facts.Economics.FeesKnown, facts.Economics.Fees = false, block.SelectedFeeFacts{}
	}
	if !facts.Economics.FeesKnown || !facts.Economics.FeeBurnKnown || facts.Economics.FeeBurnLamports > facts.Economics.Fees.TotalLamports {
		facts.Economics.FeeBurnKnown, facts.Economics.FeeBurnLamports = false, 0
	}
	if !facts.FromLocalProduction || !facts.Economics.NativeCostKnown {
		facts.Economics.NativeCostKnown, facts.Economics.NativeCost = false, block.SelectedNativeCost{}
	}
	if !facts.valid(c.Slot) || c.Bankhash.IsZero() || !selectedCountsValid(c.Transactions) ||
		(pending.selected != nil && *pending.selected != facts) {
		pending.selected, pending.selectedConflict = nil, true
		if r.selectedConflicts != math.MaxUint64 {
			r.selectedConflicts++
		}
		r.pending[key] = pending
		return false
	}
	if pending.selected == nil {
		copy := facts
		pending.selected = &copy
		r.pending[key] = pending
	}
	return true
}

// publishSelectedSlots runs on replay after either existing settlement path.
// The snapshot contains explicit gaps, including missing optional source data.
func (p *Progress) publishSelectedSlots(r *CertificateOutcomes) {
	if p == nil || r == nil || r.selectedOwner == nil {
		return
	}
	d := p.diagnostics.Load()
	if d != r.selectedOwner {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	s, ok := p.Snapshot()
	if !ok || d.exhausted || d.generation == 0 || d.attempt != r.selectedAttempt ||
		s.Attempt != d.attempt || s.Rewinds != d.rewinds || r.summary.SampledAt.IsZero() {
		return
	}
	count := min(r.count, SelectedSlotCapacity)
	view := &SelectedSlotSnapshot{RunID: d.runID, Generation: d.generation, Attempt: d.attempt, Rewinds: d.rewinds,
		StartedAt: d.startedAt, ResetAt: d.resetAt, UpdatedAt: r.summary.SampledAt,
		WindowEntries: uint64(r.count), OmittedFromWindow: uint64(r.count - count), FactConflicts: r.selectedConflicts,
		CoverageIncomplete: r.coverageLost || r.selectedConflicts != 0,
		Records:            make([]SelectedSlotRecord, count)}
	for i := range count {
		entry := r.window[(r.start+r.count-count+i)%len(r.window)]
		row := SelectedSlotRecord{CertificateFoldEvidence: entry.fold, Gap: true}
		if !entry.gap && entry.selected != nil && !entry.selectedConflict {
			row.Gap, row.Facts = false, *entry.selected
			row.ParentSlot, row.ParentBlockID, row.HasParentBlockID = entry.carrier.ParentSlot, entry.carrier.ParentBlockID, entry.carrier.HasParentBlockID
			row.ExecutionParentSlot, row.ExecutionParentBankhash = entry.carrier.ExecutionParentSlot, entry.carrier.ExecutionParentBankhash
			row.Transactions = entry.carrier.Transactions
		}
		view.CoverageIncomplete = view.CoverageIncomplete || row.Gap
		view.Records[i] = row
	}
	p.selectedSlots.Store(view)
}
