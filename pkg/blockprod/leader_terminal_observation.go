package blockprod

import (
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/gagliardetto/solana-go"
)

// LeaderTerminalObservation is a detached local production event, not a chain
// inclusion result or a complete attempt history. Only accepted terminals are
// observed; intermediate retries and pending failures at shutdown may be absent.
// Its lifetime belongs to the producer, not the current identity holder.
type LeaderTerminalObservation struct {
	Slot                             uint64
	SampledAt                        time.Time
	Outcome, Reason, Terminal, Cause string
	// BankOpened gates the following context. Pre-open and other-slot window
	// failures must not be attributed to the current bank or identity holder.
	BankOpened     bool
	BankLeader     solana.PublicKey
	ParentSlot     uint64
	ParentBankhash solana.Hash
	// Costs are available only for a matching bank at a successful local
	// terminal. Failed or still-accepting banks do not have final costs here.
	CostsKnown        bool
	Costs             LeaderBankCosts
	FinalizationKnown bool
	Finalization      LeaderFinalizationObservation
}

// LeaderFinalizationObservation copies existing monotonic phase durations for
// one successful local completion. Bank phases overlap BankCommit; none of
// these durations establish network delivery or whole-slot latency.
type LeaderFinalizationObservation struct {
	SlotEnding, Freeze, EntryDrain, RewardCertificates           time.Duration
	BuildBlock, BankCommit, FooterBroadcast, EndingTickBroadcast time.Duration
	BankPhasesKnown                                              bool
	Bank                                                         replay.LeaderFinalizationTimings
	WindowCompletionKnown                                        bool
	WindowCompletion                                             LeaderWindowCompletionObservation
}

// LeaderWindowCompletionObservation preserves the existing production-clock
// intervals at final shred submission, before the local replay handoff.
// DeadlineMargin may be negative; submission is not peer delivery.
type LeaderWindowCompletionObservation struct {
	WindowStartSlot                                           uint64
	ReadyAt, NetworkCompletedAt                               time.Time
	SlotDuration, ReadyToCompletion, FinalizationToCompletion time.Duration
	DeadlineMargin                                            time.Duration
}

func (l *LeaderLoop) windowCompletionObservationLocked(slot uint64, finalizationStartedAt, networkCompletedAt time.Time) (LeaderWindowCompletionObservation, bool) {
	w := l.productionWindow
	if !w.active || w.readyAt.IsZero() || finalizationStartedAt.IsZero() || networkCompletedAt.IsZero() ||
		w.startSlot%alpenglow.LeaderWindowSlots != 0 ||
		slot < w.startSlot || slot > w.endSlot || slot-w.startSlot >= alpenglow.LeaderWindowSlots ||
		l.slotDuration <= 0 || l.slotDuration > time.Duration(math.MaxInt64/alpenglow.LeaderWindowSlots) ||
		finalizationStartedAt.Before(w.readyAt) || networkCompletedAt.Before(finalizationStartedAt) {
		return LeaderWindowCompletionObservation{}, false
	}
	elapsed := networkCompletedAt.Sub(w.readyAt)
	finalization := networkCompletedAt.Sub(finalizationStartedAt)
	margin := time.Duration(slot-w.startSlot+1)*l.slotDuration - elapsed
	if elapsed == time.Duration(math.MaxInt64) || finalization > elapsed {
		return LeaderWindowCompletionObservation{}, false
	}
	return LeaderWindowCompletionObservation{WindowStartSlot: w.startSlot, ReadyAt: w.readyAt, NetworkCompletedAt: networkCompletedAt,
		SlotDuration: l.slotDuration, ReadyToCompletion: elapsed, FinalizationToCompletion: finalization, DeadlineMargin: margin}, true
}

func (l *LeaderLoop) observeLeaderTerminalLocked(slot uint64, outcome, reason, terminal, cause string, finalization *LeaderFinalizationObservation) {
	if l.observeTerminal == nil {
		return
	}
	// Do not use l.now: that clock participates in production cutoff decisions.
	o := LeaderTerminalObservation{Slot: slot, SampledAt: time.Now(),
		Outcome: outcome, Reason: reason, Terminal: terminal, Cause: cause}
	if l.activeBank != nil && l.activeSlot == slot && l.activeBank.Slot() == slot {
		o.BankOpened = true
		o.BankLeader = l.activeBank.Leader()
		o.ParentSlot, o.ParentBankhash = l.parentCtx.ParentSlot, l.parentCtx.ParentBankhash
		if outcome == leaderOutcomeBroadcast {
			o.Costs, o.CostsKnown = l.activeBank.closedCostObservation()
			if o.CostsKnown && finalization != nil {
				o.Finalization, o.FinalizationKnown = *finalization, true
			}
		}
	}
	// No bank pointer, free-form detail or wrapped error escapes this callback.
	l.observeTerminal(o)
}
