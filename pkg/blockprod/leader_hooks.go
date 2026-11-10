package blockprod

import "time"

// LeaderHooks observes the lifecycle of locally produced leader slots.
//
// Every slot passed to SlotStarted later reaches SlotEnding, SlotAborted, or
// both. SlotEnding means the bank is about to freeze for finalization;
// SlotAborted means the slot was abandoned, either before SlotEnding (the
// bank is closed and its block discarded) or after it (finalization or
// broadcast failed). A slot that fails to open never reaches any hook.
//
// The LeaderLoop calls hooks synchronously while holding its lock. They must
// return promptly and must not call back into the LeaderLoop.
type LeaderHooks interface {
	// SlotStarted runs before the bank is published to TPU sources.
	SlotStarted(LeaderSlotInfo)
	// SlotEnding runs just before the bank freezes. Forging after the freeze
	// reports ForgeDroppedNoLeader.
	SlotEnding(slot uint64)
	// SlotAborted reports one of the leaderReason values.
	SlotAborted(slot uint64, reason string)
}

// SlotHolder is implemented by LeaderHooks that can keep a slot open past its
// production deadline while they still have work for it. The protocol
// deadline ends a hold regardless.
type SlotHolder interface {
	// HoldSlot reports whether slot must stay open. It runs under the leader
	// loop's lock and must return promptly.
	HoldSlot(slot uint64) bool
	// Released is signaled when a held slot may end, so the loop need not wait
	// for its next poll.
	Released() <-chan struct{}
}

// DeadlineSlotHolder observes the finite drain deadline announced with a slot.
// After that deadline no new work may be admitted or dispatched. The leader
// closes bank admission then waits for scratch readers before finalization;
// transactions already committed to the bank are retained. Older SlotHolder
// implementations retain their protocol-deadline hold behavior.
type DeadlineSlotHolder interface {
	SlotHolder
	DrainStatus(slot uint64) SlotDrainStatus
}

// SlotDrainStatus is a concurrency-safe snapshot for completion diagnostics.
type SlotDrainStatus struct {
	Outstanding      int64
	QueuedEvents     int
	PipelineInFlight bool
}

// LeaderSlotInfo describes a leader slot whose working bank is about to be
// published.
type LeaderSlotInfo struct {
	Slot       uint64
	ParentSlot uint64
	Bank       *WorkingBank
	// WindowStartSlot and WindowEndSlot bound the ParentReady leader window
	// that contains Slot. Both are zero outside a ParentReady window.
	WindowStartSlot uint64
	WindowEndSlot   uint64
	// Start is when the bank opened. End is the local production cutoff, at
	// which the leader freezes the bank; ProtocolDeadline is the protocol
	// deadline for the complete block. End and ProtocolDeadline are zero
	// outside a ParentReady window, where the slot ends with the live clock.
	Start            time.Time
	End              time.Time
	ProtocolDeadline time.Time
	// DrainDeadline bounds the grace for already flowing work after End. It
	// consumes at most a quarter of the completion reserve, capped at 5ms.
	// DeadlineSlotHolder implementations stop admission/dispatch by this time.
	DrainDeadline time.Time
}

func (l *LeaderLoop) leaderSlotInfoLocked(slot, parentSlot uint64, bank *WorkingBank) LeaderSlotInfo {
	info := LeaderSlotInfo{
		Slot:             slot,
		ParentSlot:       parentSlot,
		Bank:             bank,
		Start:            l.now(),
		End:              l.productionWindowDeadlineLocked(slot),
		ProtocolDeadline: l.productionWindowProtocolDeadlineLocked(slot),
		DrainDeadline:    l.productionWindowDrainDeadlineLocked(slot),
	}
	if l.productionWindow.active && slot >= l.productionWindow.startSlot {
		info.WindowStartSlot = l.productionWindow.startSlot
		info.WindowEndSlot = l.productionWindow.endSlot
	}
	return info
}
