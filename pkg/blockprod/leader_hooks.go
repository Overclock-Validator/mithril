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
}

func (l *LeaderLoop) leaderSlotInfoLocked(slot, parentSlot uint64, bank *WorkingBank) LeaderSlotInfo {
	info := LeaderSlotInfo{
		Slot:             slot,
		ParentSlot:       parentSlot,
		Bank:             bank,
		Start:            l.now(),
		End:              l.productionWindowDeadlineLocked(slot),
		ProtocolDeadline: l.productionWindowProtocolDeadlineLocked(slot),
	}
	if l.productionWindow.active && slot >= l.productionWindow.startSlot {
		info.WindowStartSlot = l.productionWindow.startSlot
		info.WindowEndSlot = l.productionWindow.endSlot
	}
	return info
}
