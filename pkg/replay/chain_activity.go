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
	unknown := ChainActivitySummary{SampledAt: r.summary.SampledAt}
	if r.count < 2 {
		return unknown
	}
	entry := func(i int) certificateWindowEntry { return r.window[(r.start+i)%len(r.window)] }
	valid := func(e certificateWindowEntry) bool {
		c := e.carrier
		t := c.Transactions
		return !e.gap && e.slot == c.Slot && e.slot == e.fold.Slot && e.fold.HasBlockID && c.BlockID == e.fold.BlockID && c.Bankhash == e.fold.Bankhash &&
			c.ActivityKnown && c.BankTime > 0 && !t.Saturated && t.NonVote <= t.Total && t.Failed <= t.Total &&
			t.NonVoteFailed <= t.NonVote && t.NonVoteFailed <= t.Failed && t.Failed-t.NonVoteFailed <= t.Total-t.NonVote
	}
	child := entry(r.count - 1)
	// The fold frontier is the last actual bank, even when slots were skipped.
	// A missing carrier there must not refresh an older interval.
	if !valid(child) || child.slot != through {
		return unknown
	}
	s := ChainActivitySummary{SampledAt: r.summary.SampledAt, LastSlot: child.slot, EndTime: child.carrier.BankTime}
	for i := r.count - 2; i >= 0; i-- {
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
