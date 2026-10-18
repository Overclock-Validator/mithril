package blockprod

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type hookEvent struct {
	kind   string
	slot   uint64
	reason string
	info   LeaderSlotInfo
	// published and accepting snapshot the controller and bank when the hook ran.
	published *WorkingBank
	accepting bool
}

type recordingHooks struct {
	controller *Controller
	bank       func() *WorkingBank
	events     []hookEvent
}

func (h *recordingHooks) snapshot(e hookEvent) hookEvent {
	if h.controller != nil {
		e.published = h.controller.WorkingBank()
	}
	if h.bank != nil {
		if bank := h.bank(); bank != nil {
			bank.mu.Lock()
			e.accepting = bank.accepting
			bank.mu.Unlock()
		}
	}
	return e
}

func (h *recordingHooks) SlotStarted(info LeaderSlotInfo) {
	h.events = append(h.events, h.snapshot(hookEvent{kind: "started", slot: info.Slot, info: info}))
}

func (h *recordingHooks) SlotEnding(slot uint64) {
	h.events = append(h.events, h.snapshot(hookEvent{kind: "ending", slot: slot}))
}

func (h *recordingHooks) SlotAborted(slot uint64, reason string) {
	h.events = append(h.events, h.snapshot(hookEvent{kind: "aborted", slot: slot, reason: reason}))
}

func (h *recordingHooks) kinds() []string {
	out := make([]string, 0, len(h.events))
	for _, e := range h.events {
		out = append(out, e.kind)
	}
	return out
}

func TestLeaderHooksSlotStartedBeforeBankIsPublished(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)
	const startSlot = uint64(212)
	const slot = startSlot + 1
	ready := time.Unix(1_700_000_000, 0)
	now := ready.Add(250 * time.Millisecond)
	global.SetReplayFrontier(startSlot)
	global.SetAlpenglowBlockID(startSlot, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(startSlot, solana.Hash{2})
	parent := coherentTestParentContext(startSlot, solana.Hash{3})
	controller := NewController()
	hooks := &recordingHooks{controller: controller}
	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:    controller,
		Identity:      txfixture.PayerPrivateKey(),
		Broadcaster:   &captureBroadcaster{},
		ParentContext: func(uint64) ParentContext { return parent },
		Now:           func() time.Time { return now },
		Hooks:         hooks,
		GateTPU:       true,
	})
	loop.productionWindow = leaderProductionWindow{
		active: true, startSlot: startSlot, endSlot: startSlot + 3, nextSlot: slot, readyAt: ready,
	}

	require.NoError(t, loop.startSlotLocked(slot))

	require.Equal(t, []string{"started"}, hooks.kinds())
	started := hooks.events[0]
	assert.Nil(t, started.published, "SlotStarted must run before TPU sources can obtain the bank")
	info := started.info
	require.NotNil(t, info.Bank)
	assert.Same(t, loop.activeBank, info.Bank)
	assert.Same(t, info.Bank, controller.WorkingBank())
	assert.Equal(t, slot, info.Slot)
	assert.Equal(t, startSlot, info.ParentSlot)
	assert.Equal(t, startSlot, info.WindowStartSlot)
	assert.Equal(t, startSlot+3, info.WindowEndSlot)
	assert.Equal(t, now, info.Start)
	assert.Equal(t, ready.Add(400*time.Millisecond), info.ProtocolDeadline)
	assert.Equal(t, ready.Add(400*time.Millisecond-leaderBlockCompletionReserve), info.End)
	assert.False(t, info.Bank.TPUAllowed(), "GateTPU opens the bank with TPU admission closed")
	info.Bank.AllowTPU()
	assert.True(t, info.Bank.TPUAllowed())
}

func TestLeaderGateTPUIsIgnoredWithoutHooks(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)
	const slot = uint64(98)
	global.SetReplayFrontier(slot - 1)
	global.SetAlpenglowBlockID(slot-1, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(slot-1, solana.Hash{2})
	parent := coherentTestParentContext(slot-1, solana.Hash{3})
	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity:      txfixture.PayerPrivateKey(),
		Broadcaster:   &captureBroadcaster{},
		ParentContext: func(uint64) ParentContext { return parent },
		GateTPU:       true,
	})

	require.NoError(t, loop.startSlotLocked(slot))
	require.True(t, loop.activeBank.TPUAllowed(), "without hooks nothing could open a gated bank")
	loop.releaseActiveSlotLocked()
}

func TestLeaderHooksSkipSlotsThatFailToOpen(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)
	const slot = uint64(102)
	global.SetReplayFrontier(slot - 1)
	global.SetAlpenglowBlockID(slot-1, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(slot-1, solana.Hash{2})
	parent := coherentTestParentContext(slot-1, solana.Hash{3})
	reads := 0
	hooks := &recordingHooks{}
	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: &captureBroadcaster{},
		ParentContext: func(uint64) ParentContext {
			reads++
			current := parent
			if reads > 1 {
				current.ReplayGeneration++
			}
			return current
		},
		Hooks: hooks,
	})

	require.ErrorIs(t, loop.startSlotLocked(slot), errParentNotReady)
	require.Nil(t, loop.activeBank)
	require.Empty(t, hooks.events)
}

// newFinishingLeaderLoop returns a loop with an active, published slot that is
// ready to finalize. commit replaces replay.CommitLeaderSlot.
func newFinishingLeaderLoop(t *testing.T, slot uint64, hooks *recordingHooks, commit func(*ParentContext, replay.CommitLeaderInput) error) (*LeaderLoop, *WorkingBank, *captureBroadcaster, *bool) {
	t.Helper()
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)
	t.Cleanup(func() { replay.TakeLocalLeaderCommit(slot) })
	parentSlot := slot - 1
	global.SetReplayFrontier(parentSlot)
	global.SetAlpenglowBlockID(parentSlot, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(parentSlot, solana.Hash{2})
	parent := coherentTestParentContext(parentSlot, solana.Hash{3})
	parent.PrevFeeGovernor = &sealevel.FeeRateGovernor{
		PrevLamportsPerSignature: 5000,
		LamportsPerSignature:     5000,
	}
	broadcaster := &captureBroadcaster{}
	identity := txfixture.PayerPrivateKey()
	session := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
		Leader:                  identity,
		Slot:                    slot,
		ParentSlot:              parentSlot,
		ParentBlockID:           parent.ParentBlockID,
		ParentChainedMerkleRoot: parent.ParentChainedMerkleRoot,
		Broadcaster:             broadcaster,
	})
	slotCtx := &sealevel.SlotCtx{
		Slot:            slot,
		ParentSlot:      parentSlot,
		Features:        features.NewFeaturesDefault(),
		FeeRateGovernor: parent.PrevFeeGovernor,
	}
	sink := NewShredSink(session)
	bank := NewWorkingBank(BankConfig{
		SlotCtx: slotCtx,
		Slot:    slot,
		Leader:  identity.PublicKey(),
		Sink:    sink,
	})
	controller := NewController()
	controller.SetWorkingBank(bank)
	hooks.controller = controller
	hooks.bank = func() *WorkingBank { return bank }
	delivered := false
	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:    controller,
		Identity:      identity,
		AccountsDb:    &accountsdb.AccountsDb{},
		EpochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 1_000},
		ParentContext: func(uint64) ParentContext { return parent },
		OnBlock:       func(*block.Block) { delivered = true },
		Hooks:         hooks,
	})
	loop.activeSlot = slot
	loop.activeBank = bank
	loop.activeSess = session
	loop.activeSink = sink
	loop.parentCtx = parent
	loop.activeParentID = parent.ParentBlockID
	loop.activeParentChainedRoot = parent.ParentChainedMerkleRoot
	loop.activeParentGeneration = parent.ReplayGeneration
	loop.commitLeaderSlot = func(in replay.CommitLeaderInput) (*sealevel.SlotCtx, error) {
		in.SlotCtx.FinalBankhash = make([]byte, 32)
		return in.SlotCtx, commit(&parent, in)
	}
	return loop, bank, broadcaster, &delivered
}

func TestLeaderHooksSlotEndingRunsBeforeFreeze(t *testing.T) {
	const slot = uint64(106)
	hooks := &recordingHooks{}
	loop, bank, broadcaster, delivered := newFinishingLeaderLoop(t, slot, hooks,
		func(*ParentContext, replay.CommitLeaderInput) error { return nil })

	loop.finishActiveSlotLocked()

	require.True(t, *delivered, "the slot must complete for this test to cover the success path")
	assert.Greater(t, broadcaster.count(), 0)
	require.Equal(t, []string{"ending"}, hooks.kinds(), "a completed slot is never reported as aborted")
	ending := hooks.events[0]
	assert.Equal(t, slot, ending.slot)
	assert.Same(t, bank, ending.published, "SlotEnding runs before the bank is detached")
	assert.True(t, ending.accepting, "SlotEnding runs before the bank freezes")
	assert.Nil(t, loop.activeBank)
}

func TestLeaderHooksSlotAbortedAfterEndingWhenFinalizationFails(t *testing.T) {
	const slot = uint64(110)
	hooks := &recordingHooks{}
	loop, _, broadcaster, delivered := newFinishingLeaderLoop(t, slot, hooks,
		func(parent *ParentContext, _ replay.CommitLeaderInput) error {
			parent.ReplayGeneration++
			return nil
		})

	loop.finishActiveSlotLocked()

	require.False(t, *delivered)
	assert.Zero(t, broadcaster.count())
	require.Equal(t, []string{"ending", "aborted"}, hooks.kinds())
	aborted := hooks.events[1]
	assert.Equal(t, slot, aborted.slot)
	assert.Equal(t, leaderReasonParentChangedBeforeFreeze, aborted.reason)
	assert.Nil(t, loop.activeBank)
}

func TestLeaderHooksSlotAbortedWithoutEndingWhenParentChanges(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)

	const startSlot = uint64(212)
	const activeSlot = startSlot + 1
	self := txfixture.PayerPubkey()
	controller := NewController()
	bank := NewWorkingBank(BankConfig{Slot: activeSlot})
	controller.SetWorkingBank(bank)
	global.SetReplayFrontier(startSlot)
	hooks := &recordingHooks{controller: controller, bank: func() *WorkingBank { return bank }}

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  controller,
		Identity:    txfixture.PayerPrivateKey(),
		CurrentSlot: func() uint64 { return activeSlot },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			if slot == startSlot {
				return self, true
			}
			return solana.PublicKey{}, false
		},
		ProductionParent: func(slot uint64) alpenglow.BlockProductionParent {
			if slot == activeSlot {
				return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentMissedWindow}
			}
			return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentNotReady}
		},
		ParentContext: func(uint64) ParentContext { return ParentContext{} },
		Hooks:         hooks,
	})
	loop.productionWindow = leaderProductionWindow{
		active:    true,
		startSlot: startSlot,
		endSlot:   startSlot + alpenglow.LeaderWindowSlots - 1,
		nextSlot:  activeSlot,
	}
	loop.activeSlot = activeSlot
	loop.activeBank = bank
	loop.markLeaderSlotFinished(startSlot)

	loop.tick()

	require.Equal(t, []string{"aborted"}, hooks.kinds())
	aborted := hooks.events[0]
	assert.Equal(t, activeSlot, aborted.slot)
	assert.Equal(t, leaderReasonParentReadyMissedWindow, aborted.reason)
	assert.Same(t, bank, aborted.published, "SlotAborted runs before the bank is detached")
	assert.Nil(t, controller.WorkingBank())
	assert.Nil(t, loop.activeBank)
}

// holdingHooks keeps the active slot open while hold is set.
type holdingHooks struct {
	*recordingHooks
	hold     bool
	released chan struct{}
}

func (h *holdingHooks) HoldSlot(uint64) bool      { return h.hold }
func (h *holdingHooks) Released() <-chan struct{} { return h.released }

func TestLeaderHoldsTheSlotPastItsDeadlineUntilReleased(t *testing.T) {
	holding := &holdingHooks{recordingHooks: &recordingHooks{}}
	require.Same(t, holding, NewLeaderLoop(LeaderLoopConfig{Hooks: holding}).holder)

	ready := time.Unix(1_700_000_000, 0)
	for _, tc := range []struct {
		name string
		slot uint64
		// at is past the production deadline (125ms after ParentReady for the
		// window's first slot); the protocol deadline is at 200ms.
		at       time.Duration
		release  bool
		finishes bool
	}{
		{"held past the production deadline", 120, 150 * time.Millisecond, false, false},
		{"released", 121, 150 * time.Millisecond, true, true},
		{"the protocol deadline ends a hold", 122, 200 * time.Millisecond, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hooks := &recordingHooks{}
			loop, _, _, delivered := newFinishingLeaderLoop(t, tc.slot, hooks,
				func(*ParentContext, replay.CommitLeaderInput) error { return nil })
			holder := &holdingHooks{recordingHooks: hooks, hold: true}
			loop.holder = holder
			now := ready.Add(tc.at)
			loop.now = func() time.Time { return now }
			loop.currentSlot = func() uint64 { return tc.slot }
			loop.leaderForSlot = func(uint64) (solana.PublicKey, bool) { return solana.PublicKey{}, false }
			loop.productionWindow = leaderProductionWindow{
				active: true, startSlot: tc.slot, endSlot: tc.slot + 3, nextSlot: tc.slot, readyAt: ready,
			}
			if tc.release {
				holder.hold = false
			}

			loop.tick()
			if !tc.finishes {
				assert.NotNil(t, loop.activeBank)
				assert.Empty(t, hooks.kinds())
				return
			}
			require.True(t, *delivered)
			assert.Equal(t, []string{"ending"}, hooks.kinds())
			assert.Nil(t, loop.activeBank)
		})
	}
}
