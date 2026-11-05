package replay

import (
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// NextKnownAssignment is the earliest assignment found in the captured local
// current/next-epoch schedules at or after ReferenceSlot. Schedules may be
// missing or partial: neither a hit nor its absence proves the actual next
// assignment. SearchedThrough bounds the search, not continuous coverage.
type NextKnownAssignment struct {
	SampledAt        time.Time
	Identity         solana.PublicKey
	ReferenceSlot    uint64
	SearchedThrough  uint64
	NextKnownSlot    uint64
	HasNextKnownSlot bool
}

func nextKnownAssignment(now time.Time, identity solana.PublicKey, reference uint64, epochs *sealevel.SysvarEpochSchedule) NextKnownAssignment {
	if identity.IsZero() || epochs == nil || epochs.SlotsPerEpoch == 0 {
		return NextKnownAssignment{}
	}
	epoch, index := epochs.GetEpochAndSlotIndex(reference)
	length := epochs.SlotsInEpoch(epoch)
	if length == 0 || index >= length {
		return NextKnownAssignment{}
	}
	// Subtract before adding; the final representable epoch can be truncated.
	end := reference + min(length-index-1, math.MaxUint64-reference)
	requested := [2]uint64{epoch, epoch}
	count := 1
	nextStart, nextEnd := end, end
	if end != math.MaxUint64 && epoch != math.MaxUint64 {
		nextStart = end + 1
		if epochs.GetEpoch(nextStart) == epoch+1 {
			if nextLength := epochs.SlotsInEpoch(epoch + 1); nextLength != 0 {
				nextEnd = nextStart + min(nextLength-1, math.MaxUint64-nextStart)
				requested[1], count = epoch+1, 2
			}
		}
	}
	views := global.LeaderSchedulesForEpochs(requested[:count])
	s := NextKnownAssignment{SampledAt: now.UTC(), Identity: identity, ReferenceSlot: reference, SearchedThrough: end}
	if len(views) == 2 {
		s.SearchedThrough = nextEnd
	}
	for i, view := range views {
		from, through := reference, end
		if i == 1 {
			from, through = nextStart, nextEnd
		}
		if slot, ok := view.NextSlotForLeader(identity, from); ok && slot >= from && slot <= through {
			s.NextKnownSlot, s.HasNextKnownSlot = slot, true
			break // Current epoch precedes every candidate in the next epoch.
		}
	}
	return s
}

// observeNextAssignment stages only scalars on the replay owner. The following
// completed/skipped store publishes them together with the consumed progress.
func (p *Progress) observeNextAssignment(now time.Time, holder *identity.Holder, consumed uint64, epochs *sealevel.SysvarEpochSchedule) {
	if p == nil {
		return
	}
	p.nextAssignment = NextKnownAssignment{}
	if holder != nil && consumed != math.MaxUint64 {
		p.nextAssignment = nextKnownAssignment(now, holder.PublicKey(), consumed+1, epochs)
	}
}
