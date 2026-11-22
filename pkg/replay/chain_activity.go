package replay

import (
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

// ChainActivitySummary describes (AnchorSlot, LastSlot] on one continuous
// selected-and-folded chain. Bank Clock timestamps, not local replay elapsed
// time, define the interval. The first block is only a timestamp anchor.
// The owner walks at most 512 retained carriers, stopping at ten bank seconds.
// A short startup interval is valid but is reported with its actual duration.
type ChainActivitySummary struct {
	SampledAt            time.Time
	Available            bool
	AnchorSlot, LastSlot uint64
	StartTime, EndTime   int64
	Blocks               uint64
	Transactions         sealevel.BankTransactionCounts
}

func (r *CertificateOutcomes) activitySnapshot(through uint64) ChainActivitySummary {
	if r == nil || r.summary.SampledAt.IsZero() {
		return ChainActivitySummary{}
	}
	entry := func(i int) certificateWindowEntry { return r.window[(r.start+i)%len(r.window)] }
	if r.count == 0 || entry(r.count-1).slot != through {
		return ChainActivitySummary{SampledAt: r.summary.SampledAt}
	}
	return activitySnapshotAt(r.count-1, r.summary.SampledAt, entry)
}

// ActivityHistory computes bounded history from an already-detached snapshot.
// It is intended for dashboard workers, not replay's settlement path.
func (s SelectedSlotSnapshot) ActivityHistory() []ChainActivitySummary {
	if s.UpdatedAt.IsZero() || len(s.Records) < 2 || len(s.Records) > SelectedSlotCapacity {
		return nil
	}
	entry := func(i int) certificateWindowEntry {
		r := s.Records[i]
		return certificateWindowEntry{slot: r.Slot, fold: r.CertificateFoldEvidence, gap: r.Gap,
			carrier: CertificateCarrier{Slot: r.Slot, BlockID: r.BlockID, Bankhash: r.Bankhash,
				ParentSlot: r.ParentSlot, ParentBlockID: r.ParentBlockID, HasParentBlockID: r.HasParentBlockID,
				ExecutionParentSlot: r.ExecutionParentSlot, ExecutionParentBankhash: r.ExecutionParentBankhash,
				BankTime: r.BankTime, ActivityKnown: r.BankTime > 0, Transactions: r.Transactions}}
	}
	var out []ChainActivitySummary
	for i := len(s.Records) - 1; i > 0 && len(out) < 32; {
		interval := activitySnapshotAt(i, s.UpdatedAt, entry)
		if !interval.Available {
			break
		}
		out = append(out, interval)
		for i > 0 && s.Records[i].Slot > interval.AnchorSlot {
			i--
		}
		if s.Records[i].Slot != interval.AnchorSlot {
			break
		}
	}
	return out
}

func activitySnapshotAt(last int, sampledAt time.Time, entry func(int) certificateWindowEntry) ChainActivitySummary {
	unknown := ChainActivitySummary{SampledAt: sampledAt}
	if last < 1 {
		return unknown
	}
	valid := func(e certificateWindowEntry) bool {
		c := e.carrier
		t := c.Transactions
		return !e.gap && e.slot == c.Slot && e.slot == e.fold.Slot && e.fold.HasBlockID && c.BlockID == e.fold.BlockID && c.Bankhash == e.fold.Bankhash &&
			c.ActivityKnown && c.BankTime > 0 && !t.Saturated && t.NonVote <= t.Total && t.Failed <= t.Total &&
			t.NonVoteFailed <= t.NonVote && t.NonVoteFailed <= t.Failed && t.Failed-t.NonVoteFailed <= t.Total-t.NonVote
	}
	child := entry(last)
	// The fold frontier is the last actual bank, even when slots were skipped.
	// A missing carrier there must not refresh an older interval.
	if !valid(child) {
		return unknown
	}
	s := ChainActivitySummary{SampledAt: sampledAt, LastSlot: child.slot, EndTime: child.carrier.BankTime}
	for i := last - 1; i >= 0; i-- {
		parent := entry(i)
		c, p := child.carrier, parent.carrier
		if !valid(parent) || p.Slot >= c.Slot || !c.HasParentBlockID || c.ParentSlot != p.Slot || c.ExecutionParentSlot != p.Slot ||
			c.ParentBlockID != p.BlockID || c.ExecutionParentBankhash != p.Bankhash || p.BankTime > c.BankTime {
			return unknown
		}
		for _, pair := range []struct {
			sum   *uint64
			value uint64
		}{
			{&s.Transactions.Total, c.Transactions.Total}, {&s.Transactions.NonVote, c.Transactions.NonVote},
			{&s.Transactions.Failed, c.Transactions.Failed}, {&s.Transactions.NonVoteFailed, c.Transactions.NonVoteFailed},
			{&s.Transactions.ComputeUnits, c.Transactions.ComputeUnits},
		} {
			if pair.value > math.MaxUint64-*pair.sum {
				return unknown
			}
			*pair.sum += pair.value
		}
		s.Blocks++
		s.AnchorSlot, s.StartTime = p.Slot, p.BankTime
		if s.EndTime-s.StartTime >= 10 {
			break
		}
		child = parent
	}
	s.Available = s.EndTime > s.StartTime
	if !s.Available {
		return unknown
	}
	return s
}
