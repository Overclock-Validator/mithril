package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// leaderSnapshot joins the bounded retained fold receipts with one immutable
// schedule view. It runs on the replay owner after settlement, never per viewer.
func (r *CertificateOutcomes) leaderSnapshot(identity solana.PublicKey, through uint64, epochSchedule *sealevel.SysvarEpochSchedule) LeaderOutcomeSummary {
	if r == nil || identity.IsZero() || r.summary.SampledAt.IsZero() || epochSchedule == nil {
		return LeaderOutcomeSummary{}
	}
	first := uint64(0)
	if through >= leaderOutcomeWindowSlots-1 {
		first = through - (leaderOutcomeWindowSlots - 1)
	}
	// Derive requested epochs from at most 512 slots, including warmup epochs.
	// Never walk an unchecked epoch interval or build a schedule for telemetry.
	var epochs []uint64
	for offset := uint64(0); offset <= through-first; offset++ {
		epoch := epochSchedule.GetEpoch(first + offset)
		if len(epochs) == 0 || epochs[len(epochs)-1] != epoch {
			epochs = append(epochs, epoch)
		}
	}
	schedules := global.LeaderSchedulesForEpochs(epochs)
	lookup := func(slot uint64) (solana.PublicKey, bool) {
		epoch := epochSchedule.GetEpoch(slot)
		for i, candidate := range epochs {
			if candidate == epoch && schedules[i] != nil {
				return schedules[i].LeaderForSlot(slot)
			}
		}
		return solana.PublicKey{}, false
	}
	carriers, predecessor := r.foldedCarriers()
	s := ComputeLeaderOutcomes(identity, through, carriers, predecessor, lookup)
	s.SampledAt, s.Provenance = r.summary.SampledAt, r.summary.Provenance
	s.CoverageIncomplete = s.CoverageIncomplete || r.coverageLost
	return s
}

// foldedCarriers copies retained receipts on the replay owner. Missing ancestry
// stays a barrier even when the fold itself establishes a produced block.
func (r *CertificateOutcomes) foldedCarriers() ([]LeaderOutcomeCarrier, *CertificateFoldEvidence) {
	carriers := make([]LeaderOutcomeCarrier, 0, r.count)
	for i := 0; i < r.count; i++ {
		e := r.window[(r.start+i)%len(r.window)]
		c := LeaderOutcomeCarrier{CertificateFoldEvidence: e.fold}
		if !e.gap {
			c.ParentSlot, c.ExecutionParentSlot = e.carrier.ParentSlot, e.carrier.ExecutionParentSlot
			c.ParentBlockID, c.HasParentBlockID = e.carrier.ParentBlockID, e.carrier.HasParentBlockID
			c.ExecutionParentBankhash = e.carrier.ExecutionParentBankhash
		}
		carriers = append(carriers, c)
	}
	var predecessor *CertificateFoldEvidence
	if r.hasPredecessor {
		p := r.predecessor
		predecessor = &p
	}
	return carriers, predecessor
}
