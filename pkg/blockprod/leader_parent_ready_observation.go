package blockprod

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
)

// LeaderParentReadyObservation is a detached notification of the parent selected
// for this producer's own window, including late selections and handovers. It
// does not establish that a bank opened, replay adopted the parent, or the current
// identity still owns that window. Dates are the existing activation clocks;
// handovers keep the window's original ReadyAt.
type LeaderParentReadyObservation struct {
	WindowStartSlot, ParentSlot uint64
	ParentBlockID               solana.Hash
	ReadyAt, ObservedAt         time.Time
	Activation                  string
}

func (l *LeaderLoop) observeParentReadyLocked(parent alpenglow.BlockProductionParent, activation string, now time.Time) {
	if l.observeParentReady == nil {
		return
	}
	l.observeParentReady(LeaderParentReadyObservation{
		WindowStartSlot: l.productionWindow.startSlot,
		ParentSlot:      parent.Parent.Slot, ParentBlockID: parent.Parent.Hash,
		ReadyAt: parent.ReadyAt, ObservedAt: now, Activation: activation,
	})
}
