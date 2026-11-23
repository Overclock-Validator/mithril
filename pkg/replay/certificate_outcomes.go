package replay

import (
	"sort"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

const (
	certificatePendingLimit = 1024
	certificateWindowLimit  = 512
)

// CertificateCarrier is an executed carrier, not proof of its settlement.
type CertificateCarrier struct {
	Slot                    uint64
	BlockID, Bankhash       solana.Hash
	ParentSlot              uint64
	ParentBlockID           solana.Hash
	HasParentBlockID        bool
	ExecutionParentSlot     uint64
	ExecutionParentBankhash solana.Hash
	Observation             block.CertificateObservation
	BankTime                int64
	ActivityKnown           bool
	Transactions            sealevel.BankTransactionCounts
}

// CertificateFoldEvidence identifies an actual committed overlay layer and the
// surviving executed block ID. A missing ID cannot establish a carrier match.
type CertificateFoldEvidence struct {
	Slot       uint64
	Bankhash   solana.Hash
	BlockID    solana.Hash
	HasBlockID bool
}

// CertificateOutcomeSummary counts the retained settled-carrier window. Final
// certificates are first observed within that window, not necessarily first
// carried on the chain. Dropped is cumulative within this replay attempt;
// CoverageIncomplete remains true after unenumerable fold evidence is lost,
// until a new attempt starts. Neither is a lifetime network score.
type CertificateOutcomeSummary struct {
	SampledAt                      time.Time
	Provenance                     string
	VoteAccount                    solana.PublicKey
	CoverageIncomplete             bool
	HasWindow                      bool
	FirstCarrier, LastCarrier      uint64
	Carriers, Gaps, Pending        uint64
	SlotsWithoutCarrier            uint64
	Dropped                        uint64
	RewardIncluded                 uint64
	RewardExcluded                 uint64
	RewardUnknown                  uint64
	RewardIneligible               uint64
	FinalFast, FinalSlow           uint64
	FastMember, FastNonmember      uint64
	FastMembershipUnknown          uint64
	FinalDuplicates, FinalVariants uint64
	FinalConflict                  uint64
}

type certificateCarrierKey struct {
	slot uint64
	id   solana.Hash
}

type certificateWindowEntry struct {
	slot    uint64
	fold    CertificateFoldEvidence
	carrier CertificateCarrier
	gap     bool
}

type pendingCertificateCarrier struct {
	carrier  CertificateCarrier
	conflict bool
}

// CertificateOutcomes is owned by the replay loop. Callers must serialize all
// methods and publish Snapshot's value copy to concurrent readers.
type CertificateOutcomes struct {
	subject        solana.PublicKey
	pending        map[certificateCarrierKey]pendingCertificateCarrier
	window         [certificateWindowLimit]certificateWindowEntry
	start          int
	count          int
	dropped        uint64
	coverageLost   bool
	predecessor    CertificateFoldEvidence
	hasPredecessor bool
	summary        CertificateOutcomeSummary
}

// NewCertificateOutcomes binds observations to one fixed vote account. A zero
// account retains only ordinary carrier ancestry for non-voting monitoring.
func NewCertificateOutcomes(subject solana.PublicKey) *CertificateOutcomes {
	return &CertificateOutcomes{subject: subject, pending: make(map[certificateCarrierKey]pendingCertificateCarrier)}
}

// Observe stages one successful carrier. False marks a dropped observation;
// a subsequent fold of its layer produces a gap rather than an exclusion.
func (r *CertificateOutcomes) Observe(c CertificateCarrier) bool {
	if r == nil {
		return false
	}
	key := certificateCarrierKey{c.Slot, c.BlockID}
	if c.Observation.VoteAccount != r.subject || c.BlockID == (solana.Hash{}) {
		r.dropped++
		r.summary.Dropped = r.dropped
		return false
	}
	if r.count > 0 && c.Slot <= r.window[(r.start+r.count-1)%len(r.window)].slot {
		for i := 0; i < r.count; i++ {
			entry := r.window[(r.start+i)%len(r.window)]
			if entry.slot == c.Slot && !entry.gap && entry.carrier == c {
				return true
			}
		}
		r.dropped++
		r.summary.Dropped = r.dropped
		return false
	}
	if old, ok := r.pending[key]; ok {
		if !old.conflict && old.carrier == c {
			return true
		}
		old.conflict = true
		r.pending[key] = old
		r.dropped++
		r.summary.Dropped = r.dropped
		return false
	}
	if len(r.pending) >= certificatePendingLimit {
		r.dropped++
		r.summary.Dropped = r.dropped
		return false
	}
	r.pending[key] = pendingCertificateCarrier{carrier: c}
	r.summary.Pending = uint64(len(r.pending))
	return true
}

// RewindFrom discards the abandoned speculative suffix.
func (r *CertificateOutcomes) RewindFrom(slot uint64) {
	if r == nil {
		return
	}
	for key := range r.pending {
		if key.slot >= slot {
			delete(r.pending, key)
		}
	}
	r.summary.Pending = uint64(len(r.pending))
}

// Settle consumes concrete committed layers, ordered here by carrier slot.
// It must only be called after successful fold application for those layers.
func (r *CertificateOutcomes) Settle(evidence []CertificateFoldEvidence, at time.Time) {
	if r == nil || len(evidence) == 0 {
		return
	}
	if len(evidence) > certificatePendingLimit {
		r.coverageLost = true
		r.dropped++
		clear(r.pending)
		r.refresh(at)
		return
	}
	ordered := append([]CertificateFoldEvidence(nil), evidence...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Slot < ordered[j].Slot })
	processed := make(map[uint64]struct{}, len(ordered))
	for i := 0; i < len(ordered); {
		end := i + 1
		for end < len(ordered) && ordered[end].Slot == ordered[i].Slot {
			end++
		}
		e := ordered[i]
		conflictingEvidence := false
		for j := i + 1; j < end; j++ {
			if ordered[j] != e {
				conflictingEvidence = true
			}
		}
		if r.count > 0 && e.Slot <= r.window[(r.start+r.count-1)%len(r.window)].slot {
			// ponytail: bounded to 1,024 x 512 slot comparisons per fold;
			// add a slot index only if measured fold latency warrants it.
			found := false
			for j := 0; j < r.count; j++ {
				entry := &r.window[(r.start+j)%len(r.window)]
				if entry.slot == e.Slot {
					found = true
					if conflictingEvidence || entry.fold != e {
						entry.fold.HasBlockID = false
					}
					if conflictingEvidence || !e.HasBlockID || entry.gap || entry.carrier.BlockID != e.BlockID || entry.carrier.Bankhash != e.Bankhash {
						entry.gap = true
					}
					break
				}
			}
			if !found {
				r.coverageLost = true
				if r.hasPredecessor && r.predecessor.Slot == e.Slot && (conflictingEvidence || r.predecessor != e) {
					r.predecessor.HasBlockID = false
				}
			}
			i = end
			continue
		}
		key := certificateCarrierKey{e.Slot, e.BlockID}
		p, ok := r.pending[key]
		gap := conflictingEvidence || !e.HasBlockID || !ok || p.conflict || p.carrier.Bankhash != e.Bankhash
		processed[e.Slot] = struct{}{}
		if conflictingEvidence {
			e.HasBlockID = false
		}
		r.append(certificateWindowEntry{slot: e.Slot, fold: e, carrier: p.carrier, gap: gap})
		i = end
	}
	for key := range r.pending {
		if _, ok := processed[key.slot]; ok {
			delete(r.pending, key)
		}
	}
	r.refresh(at)
}

func (r *CertificateOutcomes) append(entry certificateWindowEntry) {
	if r.count == len(r.window) {
		r.predecessor, r.hasPredecessor = r.window[r.start].fold, true
		r.window[r.start] = entry
		r.start = (r.start + 1) % len(r.window)
		return
	}
	r.window[(r.start+r.count)%len(r.window)] = entry
	r.count++
}

type certificateTarget struct {
	epoch, slot uint64
}

// refresh runs only after owner mutations, never in a viewer request.
func (r *CertificateOutcomes) refresh(at time.Time) {
	s := CertificateOutcomeSummary{SampledAt: at.UTC(), Provenance: "selected_and_folded", VoteAccount: r.subject, CoverageIncomplete: r.coverageLost,
		Pending: uint64(len(r.pending)), Dropped: r.dropped}
	if r.count == 0 {
		r.summary = s
		return
	}
	s.HasWindow = true
	s.FirstCarrier = r.window[r.start].slot
	s.LastCarrier = r.window[(r.start+r.count-1)%len(r.window)].slot
	s.Carriers = uint64(r.count)
	// Subtract before adding one to avoid overflow across the full uint64 span.
	s.SlotsWithoutCarrier = (s.LastCarrier - s.FirstCarrier) - uint64(r.count-1)
	if s.SlotsWithoutCarrier != 0 {
		s.CoverageIncomplete = true
	}
	// First pass detects equivocal target IDs before counting either one.
	targets := make(map[certificateTarget]solana.Hash, r.count)
	conflicts := make(map[certificateTarget]bool)
	for i := 0; i < r.count; i++ {
		e := r.window[(r.start+i)%len(r.window)]
		f := e.carrier.Observation.Final
		if e.gap || !f.Present {
			continue
		}
		key := certificateTarget{f.TargetEpoch, f.TargetSlot}
		id := e.carrier.Observation.FinalBlockID
		if old, ok := targets[key]; ok && old != id {
			conflicts[key] = true
		} else if !ok {
			targets[key] = id
		}
	}
	seen := make(map[certificateTarget]block.CertificateObservation, len(targets))
	for i := 0; i < r.count; i++ {
		e := r.window[(r.start+i)%len(r.window)]
		if e.gap {
			s.Gaps++
			s.CoverageIncomplete = true
			s.RewardUnknown++
			continue
		}
		o := e.carrier.Observation
		m := o.Reward
		switch {
		case e.slot < rewardcerts.SlotsForReward || m.TargetSlot != e.slot-rewardcerts.SlotsForReward || !m.AdmissionKnown || m.Included && !m.Present:
			s.RewardUnknown++
		case !m.Eligible:
			s.RewardIneligible++
		case m.Included:
			s.RewardIncluded++
		default:
			s.RewardExcluded++
		}
		f := o.Final
		if !f.Present {
			continue
		}
		key := certificateTarget{f.TargetEpoch, f.TargetSlot}
		if previous, ok := seen[key]; ok {
			s.FinalDuplicates++
			if previous.FinalFast != o.FinalFast || previous.Final != o.Final {
				s.FinalVariants++
			}
			continue
		}
		seen[key] = o
		if conflicts[key] || o.FinalBlockID == (solana.Hash{}) {
			s.FinalConflict++
			continue
		}
		if !o.FinalFast {
			s.FinalSlow++
			continue
		}
		s.FinalFast++
		switch {
		case !f.AdmissionKnown || !f.Eligible:
			s.FastMembershipUnknown++
		case f.Included:
			s.FastMember++
		default:
			s.FastNonmember++
		}
	}
	if s.RewardUnknown != 0 || s.FastMembershipUnknown != 0 || s.FinalConflict != 0 {
		s.CoverageIncomplete = true
	}
	r.summary = s
}

// Snapshot returns a detached, bounded window summary.
func (r *CertificateOutcomes) Snapshot() CertificateOutcomeSummary {
	if r == nil {
		return CertificateOutcomeSummary{}
	}
	return r.summary
}
