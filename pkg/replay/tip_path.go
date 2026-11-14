package replay

import (
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

// tipPathInput is what the replay loop knows about one turbine block when it
// writes the block's row. Instants are wall clock; zero means unknown.
type tipPathInput struct {
	slot, parentSlot uint64
	receivedNanos    int64 // kernel receive time of the shred that completed the block
	fullNanos        int64 // the block was assembled full
	fullToReady      time.Duration
	haveReady        bool
	parent           streamingFrontierMark // the last replayed block when this one was admitted
	admittedAt       time.Time
	replayStart      time.Time // SlotReplay's start
	handedOffAt      time.Time // the replay result reached consensus
}

func newTipPathInput(block *b.Block, ingress *b.TurbineIngressTimings, parent streamingFrontierMark, admittedAt, replayStart, handedOffAt time.Time) tipPathInput {
	in := tipPathInput{
		slot:          block.Slot,
		parentSlot:    block.ParentSlot,
		receivedNanos: block.ShredFullReceivedNanos,
		fullNanos:     block.ShredFullNanos,
		parent:        parent,
		admittedAt:    admittedAt,
		replayStart:   replayStart,
		handedOffAt:   handedOffAt,
	}
	if ingress != nil {
		in.fullToReady, in.haveReady = ingress.FullToReady, true
	}
	return in
}

// recordTipPath divides the block's full-to-replayed latency into consecutive
// intervals (see metrics.TipPath). It records nothing for a block that did not
// arrive as shreds or whose result never reached consensus.
func recordTipPath(rec *metrics.TipPath, in tipPathInput) {
	if in.fullNanos <= 0 || in.handedOffAt.IsZero() {
		return
	}
	full := in.fullNanos
	handedOff := in.handedOffAt.UnixNano()
	if handedOff <= full {
		return
	}
	if in.receivedNanos > 0 {
		rec.ShredReceivedNanos = in.receivedNanos
		if in.receivedNanos <= full {
			rec.ReceiveToFull.AddTiming(time.Duration(full - in.receivedNanos))
		}
	}

	// Walk forward from full to the hand-off, taking each interval from the
	// cursor to its end instant (clamped so the intervals never overlap).
	cursor := full
	var placed int64
	take := func(t *metrics.Timing, end int64) {
		end = min(end, handedOff)
		if end <= cursor {
			return
		}
		t.AddTiming(time.Duration(end - cursor))
		placed += end - cursor
		cursor = end
	}

	admitted := nanosOf(in.admittedAt)
	if in.haveReady {
		take(&rec.FullToReady, full+in.fullToReady.Nanoseconds())
		if admitted > 0 {
			// From ready to admission: first the parent's missing shreds (the
			// network), then the parent's replay, then the loop itself.
			parentKnown := in.parent.slot == in.parentSlot && in.parent.fullNanos > 0 && !in.parent.replayedAt.IsZero()
			if parentKnown {
				take(&rec.WaitParentArrival, min(in.parent.fullNanos, admitted))
				take(&rec.WaitParentReplay, min(in.parent.replayedAt.UnixNano(), admitted))
				take(&rec.LoopUntilAdmitted, admitted)
			} else {
				take(&rec.WaitUnclassified, admitted)
			}
		}
	}
	if admitted > 0 && cursor >= admitted {
		take(&rec.AdmittedToReplay, nanosOf(in.replayStart))
		take(&rec.ReplayToHandoff, handedOff)
	}
	if rest := handedOff - full - placed; rest > 0 {
		rec.Unattributed.AddTiming(time.Duration(rest))
	}
}
