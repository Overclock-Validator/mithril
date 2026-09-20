package blockprod

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type callbackBroadcaster struct {
	onBroadcast func()
	count       int
}

func (b *callbackBroadcaster) Broadcast(_ [][]byte) error {
	b.count++
	if b.onBroadcast != nil {
		b.onBroadcast()
	}
	return nil
}

func TestLeaderWindowRequiresVerifiedParentReady(t *testing.T) {
	const leaderSlot = uint64(212)
	global.SetReplayFrontier(leaderSlot - 1)
	loop := NewLeaderLoop(LeaderLoopConfig{
		ProductionParent: func(uint64) alpenglow.BlockProductionParent {
			return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentNotReady}
		},
	})

	ready, err := loop.leaderSlotReplayReady(leaderSlot)
	assert.False(t, ready)
	require.ErrorIs(t, err, errParentNotReady)
}

func TestLeaderWindowAcceptsVerifiedOlderParentReady(t *testing.T) {
	const leaderSlot = uint64(212)
	selected := alpenglow.BlockID{Slot: 208, Hash: solana.Hash{3}}
	global.SetReplayFrontier(selected.Slot)
	global.SetAlpenglowBlockID(selected.Slot, selected.Hash)
	global.SetAlpenglowChainedMerkleRoot(selected.Slot, solana.Hash{4})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  NewController(),
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: &captureBroadcaster{},
		CurrentSlot: func() uint64 { return leaderSlot },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			return txfixture.PayerPubkey(), slot == leaderSlot
		},
		ProductionParent: func(uint64) alpenglow.BlockProductionParent {
			return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentReady, Parent: selected, ReadyAt: time.Now()}
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(selected.Slot, solana.Hash{5})
		},
		ParentBlockID: global.AlpenglowBlockID,
	})

	ready, err := loop.leaderSlotReplayReady(leaderSlot)
	assert.True(t, ready)
	assert.NoError(t, err)

	loop.tick()
	require.NotNil(t, loop.activeBank)
	loop.tick()
	assert.NotNil(t, loop.activeBank, "older verified ParentReady parent must remain canonical while the window opens")
}

func TestLeaderWindowUsesParentReadyDeadlineInsteadOfLiveSlot(t *testing.T) {
	const leaderSlot = uint64(212)
	selected := alpenglow.BlockID{Slot: 208, Hash: solana.Hash{3}}
	readyAt := time.Unix(1_700_000_000, 0)
	now := readyAt
	wallSlot := leaderSlot
	global.SetReplayFrontier(selected.Slot)
	global.SetAlpenglowBlockID(selected.Slot, selected.Hash)
	global.SetAlpenglowChainedMerkleRoot(selected.Slot, solana.Hash{4})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  NewController(),
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: &captureBroadcaster{},
		CurrentSlot: func() uint64 { return wallSlot },
		Now:         func() time.Time { return now },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			return txfixture.PayerPubkey(), slot >= leaderSlot && slot < leaderSlot+alpenglow.LeaderWindowSlots
		},
		ProductionParent: func(uint64) alpenglow.BlockProductionParent {
			return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentReady, Parent: selected, ReadyAt: readyAt}
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(selected.Slot, solana.Hash{5})
		},
		ParentBlockID: global.AlpenglowBlockID,
	})

	loop.tick()
	require.NotNil(t, loop.activeBank)

	// Authenticated traffic can jump the live-slot estimate across the window.
	// The block still owns its ParentReady-derived time budget.
	wallSlot = leaderSlot + 3
	now = readyAt.Add(100 * time.Millisecond)
	loop.tick()
	assert.NotNil(t, loop.activeBank)
	assert.Equal(t, leaderSlot, loop.activeSlot)
}

func TestLeaderWindowStartsFromParentReadyBeforeLiveClockBoundary(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)

	const nextStart = uint64(212)
	const currentStart = nextStart - alpenglow.LeaderWindowSlots
	selected := alpenglow.BlockID{Slot: currentStart, Hash: solana.Hash{3}}
	readyAt := time.Unix(1_700_000_000, 0)
	now := readyAt
	parentReady := false
	self := txfixture.PayerPubkey()
	other := solana.NewWallet().PublicKey()
	global.SetReplayFrontier(selected.Slot)
	global.SetAlpenglowBlockID(selected.Slot, selected.Hash)
	global.SetAlpenglowChainedMerkleRoot(selected.Slot, solana.Hash{4})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  NewController(),
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: &captureBroadcaster{},
		CurrentSlot: func() uint64 { return currentStart },
		Now:         func() time.Time { return now },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			switch {
			case slot >= currentStart && slot < nextStart:
				return other, true
			case slot >= nextStart && slot < nextStart+alpenglow.LeaderWindowSlots:
				return self, true
			default:
				return solana.PublicKey{}, false
			}
		},
		ProductionParent: func(slot uint64) alpenglow.BlockProductionParent {
			if slot != nextStart || !parentReady {
				return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentNotReady}
			}
			return alpenglow.BlockProductionParent{
				Kind:    alpenglow.BlockProductionParentReady,
				Parent:  selected,
				ReadyAt: readyAt,
			}
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(selected.Slot, solana.Hash{5})
		},
	})

	loop.tick()
	require.Nil(t, loop.activeBank)
	require.False(t, loop.productionWindow.active)
	require.NotContains(t, loop.pendingFailures, nextStart)

	// Verified ParentReady is authoritative throughout the immediately preceding
	// window, not only when its coarse live-slot estimate reaches the final slot.
	parentReady = true
	now = readyAt.Add(5 * time.Millisecond)
	loop.tick()

	require.NotNil(t, loop.activeBank)
	require.Equal(t, nextStart, loop.activeSlot)
	require.True(t, loop.productionWindow.active)
	require.Equal(t, nextStart, loop.productionWindow.startSlot)
	require.Equal(t, nextStart, loop.productionWindow.nextSlot)
	require.Equal(t, readyAt, loop.productionWindow.readyAt)
}

func TestLeaderWindowEarlyParentReadyWaitsForReplay(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)

	const nextStart = uint64(212)
	selected := alpenglow.BlockID{Slot: nextStart - alpenglow.LeaderWindowSlots, Hash: solana.Hash{3}}
	readyAt := time.Unix(1_700_000_000, 0)
	now := readyAt
	self := txfixture.PayerPubkey()
	other := solana.NewWallet().PublicKey()
	global.SetReplayFrontier(selected.Slot - 1)
	global.SetAlpenglowBlockID(selected.Slot, selected.Hash)
	global.SetAlpenglowChainedMerkleRoot(selected.Slot, solana.Hash{4})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  NewController(),
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: &captureBroadcaster{},
		CurrentSlot: func() uint64 { return nextStart - 1 },
		Now:         func() time.Time { return now },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			if slot == nextStart-alpenglow.LeaderWindowSlots {
				return other, true
			}
			if slot >= nextStart && slot < nextStart+alpenglow.LeaderWindowSlots {
				return self, true
			}
			return solana.PublicKey{}, false
		},
		ProductionParent: func(slot uint64) alpenglow.BlockProductionParent {
			if slot != nextStart {
				return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentNotReady}
			}
			return alpenglow.BlockProductionParent{
				Kind:    alpenglow.BlockProductionParentReady,
				Parent:  selected,
				ReadyAt: readyAt,
			}
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(selected.Slot, solana.Hash{5})
		},
	})

	loop.tick()
	require.Nil(t, loop.activeBank)
	require.True(t, loop.productionWindow.active)
	require.Contains(t, loop.pendingFailures, nextStart)
	require.Equal(t, leaderReasonReplayNotReady, loop.pendingFailures[nextStart].reason)

	global.SetReplayFrontier(selected.Slot)
	now = readyAt.Add(10 * time.Millisecond)
	loop.tick()
	require.NotNil(t, loop.activeBank)
	require.Equal(t, nextStart, loop.activeSlot)
	require.NotContains(t, loop.pendingFailures, nextStart)
}

func TestLeaderWindowRefreshesCorrectedParentReadyWhileWaitingForReplay(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)

	const nextStart = uint64(212)
	parentA := alpenglow.BlockID{Slot: nextStart - alpenglow.LeaderWindowSlots, Hash: solana.Hash{3}}
	parentB := alpenglow.BlockID{Slot: parentA.Slot, Hash: solana.Hash{6}}
	readyAtA := time.Unix(1_700_000_000, 0)
	readyAtB := readyAtA.Add(100 * time.Millisecond)
	now := readyAtA
	parent := alpenglow.BlockProductionParent{
		Kind:    alpenglow.BlockProductionParentReady,
		Parent:  parentA,
		ReadyAt: readyAtA,
	}
	self := txfixture.PayerPubkey()
	other := solana.NewWallet().PublicKey()
	global.SetReplayFrontier(parentA.Slot - 1)
	global.SetAlpenglowBlockID(parentA.Slot, parentA.Hash)
	global.SetAlpenglowChainedMerkleRoot(parentA.Slot, solana.Hash{4})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  NewController(),
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: &captureBroadcaster{},
		CurrentSlot: func() uint64 { return nextStart - 1 },
		Now:         func() time.Time { return now },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			if slot == nextStart-alpenglow.LeaderWindowSlots {
				return other, true
			}
			if slot >= nextStart && slot < nextStart+alpenglow.LeaderWindowSlots {
				return self, true
			}
			return solana.PublicKey{}, false
		},
		ProductionParent: func(slot uint64) alpenglow.BlockProductionParent {
			if slot != nextStart {
				return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentNotReady}
			}
			return parent
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(parent.Parent.Slot, solana.Hash{5})
		},
	})
	defer func() {
		loop.mu.Lock()
		defer loop.mu.Unlock()
		loop.abortActiveSlotLocked()
	}()

	loop.tick()
	require.Nil(t, loop.activeBank)
	require.True(t, loop.productionWindow.active)
	require.Equal(t, parentA, loop.productionWindow.parent)
	require.Equal(t, readyAtA, loop.productionWindow.readyAt)
	require.Equal(t, leaderReasonReplayNotReady, loop.pendingFailures[nextStart].reason)

	// Invalidating A selects B, but the window keeps its original one-shot timer.
	// Even a provider that reports a newer timestamp for B must not extend the
	// protocol budget.
	parent = alpenglow.BlockProductionParent{
		Kind:    alpenglow.BlockProductionParentReady,
		Parent:  parentB,
		ReadyAt: readyAtB,
	}
	global.SetAlpenglowBlockID(parentB.Slot, parentB.Hash)
	global.SetReplayFrontier(parentB.Slot)
	now = readyAtA.Add(110 * time.Millisecond)
	loop.tick()

	require.NotNil(t, loop.activeBank)
	require.Equal(t, nextStart, loop.activeSlot)
	require.Equal(t, parentB, loop.productionWindow.parent)
	require.Equal(t, readyAtA, loop.productionWindow.readyAt)
	require.Equal(t, parentB.Hash, loop.parentCtx.ParentBlockID)
	require.NotContains(t, loop.pendingFailures, nextStart)
}

func TestLeaderWindowDoesNotExtendTimerWithoutParentChange(t *testing.T) {
	const leaderSlot = uint64(212)
	parent := alpenglow.BlockID{Slot: leaderSlot - alpenglow.LeaderWindowSlots, Hash: solana.Hash{3}}
	readyAt := time.Unix(1_700_000_000, 0)
	now := readyAt.Add(130 * time.Millisecond)

	loop := NewLeaderLoop(LeaderLoopConfig{
		CurrentSlot: func() uint64 { return leaderSlot - 1 },
		Now:         func() time.Time { return now },
		ProductionParent: func(uint64) alpenglow.BlockProductionParent {
			return alpenglow.BlockProductionParent{
				Kind:    alpenglow.BlockProductionParentReady,
				Parent:  parent,
				ReadyAt: readyAt.Add(100 * time.Millisecond),
			}
		},
	})
	loop.productionWindow = leaderProductionWindow{
		active:    true,
		startSlot: leaderSlot,
		endSlot:   leaderSlot + alpenglow.LeaderWindowSlots - 1,
		nextSlot:  leaderSlot,
		parent:    parent,
		readyAt:   readyAt,
	}

	// A later notification for the same selected parent is not a sad-leader
	// handover and must not buy the producer another 125ms.
	target, ok := loop.productionWindowTargetLocked(leaderSlot - 1)
	require.False(t, ok)
	require.Zero(t, target)
	require.False(t, loop.productionWindow.active)
	for slot := leaderSlot; slot < leaderSlot+alpenglow.LeaderWindowSlots; slot++ {
		require.Truef(t, loop.isLeaderSlotFinished(slot), "slot %d was not closed with its original expired timer", slot)
	}
}

func TestLeaderWindowEarlyParentReadyHonorsExactDeadline(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)

	const nextStart = uint64(212)
	selected := alpenglow.BlockID{Slot: nextStart - alpenglow.LeaderWindowSlots, Hash: solana.Hash{3}}
	readyAt := time.Unix(1_700_000_000, 0)
	now := readyAt.Add(AlpenglowSlotDuration - leaderBlockCompletionReserve)
	self := txfixture.PayerPubkey()
	other := solana.NewWallet().PublicKey()
	global.SetReplayFrontier(selected.Slot)
	global.SetAlpenglowBlockID(selected.Slot, selected.Hash)
	global.SetAlpenglowChainedMerkleRoot(selected.Slot, solana.Hash{4})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity:    txfixture.PayerPrivateKey(),
		CurrentSlot: func() uint64 { return nextStart - 1 },
		Now:         func() time.Time { return now },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			if slot == nextStart-alpenglow.LeaderWindowSlots {
				return other, true
			}
			if slot >= nextStart && slot < nextStart+alpenglow.LeaderWindowSlots {
				return self, true
			}
			return solana.PublicKey{}, false
		},
		ProductionParent: func(slot uint64) alpenglow.BlockProductionParent {
			if slot != nextStart {
				return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentNotReady}
			}
			return alpenglow.BlockProductionParent{
				Kind:    alpenglow.BlockProductionParentReady,
				Parent:  selected,
				ReadyAt: readyAt,
			}
		},
	})

	loop.tick()
	require.Nil(t, loop.activeBank)
	require.False(t, loop.productionWindow.active)
	for slot := nextStart; slot < nextStart+alpenglow.LeaderWindowSlots; slot++ {
		require.Truef(t, loop.isLeaderSlotFinished(slot), "slot %d was not closed with its expired window", slot)
	}
}

func TestLeaderWindowRechecksCutoffAfterReplayGate(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)

	const leaderSlot = uint64(212)
	selected := alpenglow.BlockID{Slot: leaderSlot - alpenglow.LeaderWindowSlots, Hash: solana.Hash{3}}
	readyAt := time.Unix(1_700_000_000, 0)
	cutoff := readyAt.Add(AlpenglowSlotDuration - leaderBlockCompletionReserve)
	now := cutoff.Add(-time.Nanosecond)
	parentCalls := 0
	parentContextCalls := 0
	broadcaster := &callbackBroadcaster{}
	self := txfixture.PayerPubkey()
	other := solana.NewWallet().PublicKey()
	global.SetReplayFrontier(selected.Slot)
	global.SetAlpenglowBlockID(selected.Slot, selected.Hash)
	global.SetAlpenglowChainedMerkleRoot(selected.Slot, solana.Hash{4})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  NewController(),
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: broadcaster,
		CurrentSlot: func() uint64 { return leaderSlot - 1 },
		Now:         func() time.Time { return now },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			if slot == leaderSlot-alpenglow.LeaderWindowSlots {
				return other, true
			}
			if slot >= leaderSlot && slot < leaderSlot+alpenglow.LeaderWindowSlots {
				return self, true
			}
			return solana.PublicKey{}, false
		},
		ProductionParent: func(slot uint64) alpenglow.BlockProductionParent {
			if slot != leaderSlot {
				return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentNotReady}
			}
			parentCalls++
			if parentCalls == 3 {
				now = cutoff
			}
			return alpenglow.BlockProductionParent{
				Kind:    alpenglow.BlockProductionParentReady,
				Parent:  selected,
				ReadyAt: readyAt,
			}
		},
		ParentContext: func(uint64) ParentContext {
			parentContextCalls++
			return coherentTestParentContext(selected.Slot, solana.Hash{5})
		},
	})

	loop.tick()
	require.Equal(t, cutoff, now)
	require.Equal(t, 3, parentCalls)
	require.Zero(t, parentContextCalls, "cutoff must be rechecked before allocating parent context")
	require.Zero(t, broadcaster.count)
	require.Nil(t, loop.activeBank)
	require.Nil(t, loop.controller.WorkingBank())
	require.False(t, loop.productionWindow.active)
	for slot := leaderSlot; slot < leaderSlot+alpenglow.LeaderWindowSlots; slot++ {
		require.Truef(t, loop.isLeaderSlotFinished(slot), "slot %d was not closed with its expired window", slot)
	}
}

func TestLeaderWindowRejectsCutoffCrossedDuringLocalSetup(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)

	const leaderSlot = uint64(212)
	selected := alpenglow.BlockID{Slot: leaderSlot - alpenglow.LeaderWindowSlots, Hash: solana.Hash{3}}
	readyAt := time.Unix(1_700_000_000, 0)
	cutoff := readyAt.Add(AlpenglowSlotDuration - leaderBlockCompletionReserve)
	now := cutoff.Add(-time.Nanosecond)
	parentContextCalls := 0
	broadcaster := &callbackBroadcaster{}
	controller := NewController()
	self := txfixture.PayerPubkey()
	other := solana.NewWallet().PublicKey()
	global.SetReplayFrontier(selected.Slot)
	global.SetAlpenglowBlockID(selected.Slot, selected.Hash)
	global.SetAlpenglowChainedMerkleRoot(selected.Slot, solana.Hash{4})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  controller,
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: broadcaster,
		CurrentSlot: func() uint64 { return leaderSlot - 1 },
		Now:         func() time.Time { return now },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			if slot == leaderSlot-alpenglow.LeaderWindowSlots {
				return other, true
			}
			if slot >= leaderSlot && slot < leaderSlot+alpenglow.LeaderWindowSlots {
				return self, true
			}
			return solana.PublicKey{}, false
		},
		ProductionParent: func(uint64) alpenglow.BlockProductionParent {
			return alpenglow.BlockProductionParent{
				Kind:    alpenglow.BlockProductionParentReady,
				Parent:  selected,
				ReadyAt: readyAt,
			}
		},
		ParentContext: func(uint64) ParentContext {
			parentContextCalls++
			if parentContextCalls == 2 {
				now = cutoff
			}
			return coherentTestParentContext(selected.Slot, solana.Hash{5})
		},
	})

	loop.tick()
	require.Equal(t, cutoff, now)
	require.Equal(t, 2, parentContextCalls)
	require.Zero(t, broadcaster.count, "a bank that crossed the cutoff during setup must not broadcast a header")
	require.Nil(t, loop.activeBank)
	require.Nil(t, controller.WorkingBank())
	require.False(t, loop.productionWindow.active)
	for slot := leaderSlot; slot < leaderSlot+alpenglow.LeaderWindowSlots; slot++ {
		require.Truef(t, loop.isLeaderSlotFinished(slot), "slot %d was not closed with its expired window", slot)
	}
}

func TestLeaderWindowRejectsParentChangeDuringLocalSetup(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)

	const leaderSlot = uint64(212)
	parentA := alpenglow.BlockID{Slot: leaderSlot - alpenglow.LeaderWindowSlots, Hash: solana.Hash{3}}
	parentB := alpenglow.BlockID{Slot: parentA.Slot, Hash: solana.Hash{6}}
	readyAt := time.Unix(1_700_000_000, 0)
	now := readyAt.Add(10 * time.Millisecond)
	parent := parentA
	contextParent := parentA
	parentContextCalls := 0
	controller := NewController()
	broadcaster := &callbackBroadcaster{}
	self := txfixture.PayerPubkey()
	other := solana.NewWallet().PublicKey()
	global.SetReplayFrontier(parentA.Slot)
	global.SetAlpenglowBlockID(parentA.Slot, parentA.Hash)
	global.SetAlpenglowChainedMerkleRoot(parentA.Slot, solana.Hash{4})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  controller,
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: broadcaster,
		CurrentSlot: func() uint64 { return leaderSlot - 1 },
		Now:         func() time.Time { return now },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			if slot == leaderSlot-alpenglow.LeaderWindowSlots {
				return other, true
			}
			if slot >= leaderSlot && slot < leaderSlot+alpenglow.LeaderWindowSlots {
				return self, true
			}
			return solana.PublicKey{}, false
		},
		ProductionParent: func(uint64) alpenglow.BlockProductionParent {
			return alpenglow.BlockProductionParent{
				Kind:    alpenglow.BlockProductionParentReady,
				Parent:  parent,
				ReadyAt: readyAt,
			}
		},
		ParentContext: func(uint64) ParentContext {
			parentContextCalls++
			snapshot := coherentTestParentContext(contextParent.Slot, solana.Hash{5})
			if parentContextCalls == 2 {
				parent = parentB
			}
			return snapshot
		},
	})
	defer func() {
		loop.mu.Lock()
		defer loop.mu.Unlock()
		loop.abortActiveSlotLocked()
	}()

	loop.tick()
	require.Equal(t, 2, parentContextCalls)
	require.Zero(t, broadcaster.count, "a changed ParentReady must stop the stale header")
	require.Nil(t, loop.activeBank)
	require.Nil(t, controller.WorkingBank())
	require.True(t, loop.productionWindow.active)
	require.Equal(t, leaderReasonParentChangedBeforeFreeze, loop.pendingFailures[leaderSlot].reason)

	global.SetAlpenglowBlockID(parentB.Slot, parentB.Hash)
	contextParent = parentB
	loop.tick()

	require.NotNil(t, loop.activeBank)
	require.Equal(t, parentB, loop.productionWindow.parent)
	require.Equal(t, readyAt, loop.productionWindow.readyAt)
	require.NotNil(t, controller.WorkingBank())
}

func TestLeaderWindowRejectsCutoffCrossedDuringHeaderBroadcast(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)

	const leaderSlot = uint64(212)
	selected := alpenglow.BlockID{Slot: leaderSlot - alpenglow.LeaderWindowSlots, Hash: solana.Hash{3}}
	readyAt := time.Unix(1_700_000_000, 0)
	cutoff := readyAt.Add(AlpenglowSlotDuration - leaderBlockCompletionReserve)
	now := cutoff.Add(-time.Nanosecond)
	controller := NewController()
	broadcaster := &callbackBroadcaster{onBroadcast: func() { now = cutoff }}
	self := txfixture.PayerPubkey()
	other := solana.NewWallet().PublicKey()
	global.SetReplayFrontier(selected.Slot)
	global.SetAlpenglowBlockID(selected.Slot, selected.Hash)
	global.SetAlpenglowChainedMerkleRoot(selected.Slot, solana.Hash{4})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  controller,
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: broadcaster,
		CurrentSlot: func() uint64 { return leaderSlot - 1 },
		Now:         func() time.Time { return now },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			if slot == leaderSlot-alpenglow.LeaderWindowSlots {
				return other, true
			}
			if slot >= leaderSlot && slot < leaderSlot+alpenglow.LeaderWindowSlots {
				return self, true
			}
			return solana.PublicKey{}, false
		},
		ProductionParent: func(uint64) alpenglow.BlockProductionParent {
			return alpenglow.BlockProductionParent{
				Kind:    alpenglow.BlockProductionParentReady,
				Parent:  selected,
				ReadyAt: readyAt,
			}
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(selected.Slot, solana.Hash{5})
		},
	})

	loop.tick()
	require.Equal(t, cutoff, now)
	require.Positive(t, broadcaster.count, "the synchronous header send should be the operation that crosses the cutoff")
	require.Nil(t, loop.activeBank)
	require.Nil(t, controller.WorkingBank(), "a late header must never expose a TPU working bank")
	require.False(t, loop.productionWindow.active)
	for slot := leaderSlot; slot < leaderSlot+alpenglow.LeaderWindowSlots; slot++ {
		require.Truef(t, loop.isLeaderSlotFinished(slot), "slot %d was not closed with its expired window", slot)
	}
}

func TestLeaderWindowRejectsParentChangeDuringHeaderBroadcast(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)
	spool, err := turbine.OpenShredSpool(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(spool.Close)

	const leaderSlot = uint64(212)
	parentA := alpenglow.BlockID{Slot: leaderSlot - alpenglow.LeaderWindowSlots, Hash: solana.Hash{3}}
	parentB := alpenglow.BlockID{Slot: parentA.Slot, Hash: solana.Hash{6}}
	readyAt := time.Unix(1_700_000_000, 0)
	now := readyAt.Add(10 * time.Millisecond)
	parent := parentA
	contextParent := parentA
	controller := NewController()
	broadcaster := &callbackBroadcaster{onBroadcast: func() { parent = parentB }}
	self := txfixture.PayerPubkey()
	other := solana.NewWallet().PublicKey()
	global.SetReplayFrontier(parentA.Slot)
	global.SetAlpenglowBlockID(parentA.Slot, parentA.Hash)
	global.SetAlpenglowChainedMerkleRoot(parentA.Slot, solana.Hash{4})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  controller,
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: broadcaster,
		ShredSpool:  spool,
		CurrentSlot: func() uint64 { return leaderSlot - 1 },
		Now:         func() time.Time { return now },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			if slot == leaderSlot-alpenglow.LeaderWindowSlots {
				return other, true
			}
			if slot >= leaderSlot && slot < leaderSlot+alpenglow.LeaderWindowSlots {
				return self, true
			}
			return solana.PublicKey{}, false
		},
		ProductionParent: func(uint64) alpenglow.BlockProductionParent {
			return alpenglow.BlockProductionParent{
				Kind:    alpenglow.BlockProductionParentReady,
				Parent:  parent,
				ReadyAt: readyAt,
			}
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(contextParent.Slot, solana.Hash{5})
		},
	})
	defer func() {
		loop.mu.Lock()
		defer loop.mu.Unlock()
		loop.abortActiveSlotLocked()
	}()

	loop.tick()
	require.Positive(t, broadcaster.count, "header broadcast must trigger the parent switch")
	require.Nil(t, loop.activeBank)
	require.Nil(t, controller.WorkingBank(), "an obsolete-parent bank must never reach TPU")
	require.True(t, loop.productionWindow.active)
	require.Equal(t, parentA, loop.productionWindow.parent)
	require.Equal(t, leaderReasonParentChangedBeforeFreeze, loop.pendingFailures[leaderSlot].reason)

	global.SetAlpenglowBlockID(parentB.Slot, parentB.Hash)
	contextParent = parentB
	loop.tick()

	require.NotNil(t, loop.activeBank)
	require.Equal(t, parentB, loop.productionWindow.parent)
	require.Equal(t, readyAt, loop.productionWindow.readyAt)
	require.Equal(t, parentB.Hash, loop.parentCtx.ParentBlockID)
	require.NotNil(t, controller.WorkingBank())
	requireCachedHeaderParent(t, spool, leaderSlot, self, parentB.Hash)
}

func requireCachedHeaderParent(t *testing.T, spool *turbine.ShredSpool, slot uint64, leader solana.PublicKey, want solana.Hash) {
	t.Helper()
	packets, err := spool.ReadSlot(slot)
	require.NoError(t, err)
	var data []*turbine.Shred
	for _, packet := range packets {
		shred, err := turbine.ParseShred(packet)
		require.NoError(t, err)
		require.NoError(t, shred.VerifySignature(leader))
		if shred.Type == turbine.ShredTypeData {
			data = append(data, shred)
		}
	}
	components, err := turbine.DecodeComponentsFromDataShreds(data)
	require.NoError(t, err)
	require.Len(t, components, 1)
	require.NotNil(t, components[0].Marker)
	require.NotNil(t, components[0].Marker.Header)
	require.Equal(t, want, components[0].Marker.Header.ParentBlockID,
		"repair cache retained the abandoned parent's header after the same-slot retry")
}

func TestLeaderWindowLookaheadIsBoundedToNextWindow(t *testing.T) {
	const futureStart = uint64(212)
	var parentQueries []uint64
	self := txfixture.PayerPubkey()
	other := solana.NewWallet().PublicKey()
	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity:    txfixture.PayerPrivateKey(),
		CurrentSlot: func() uint64 { return futureStart - alpenglow.LeaderWindowSlots - 1 },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			if slot == futureStart {
				return self, true
			}
			return other, true
		},
		ProductionParent: func(slot uint64) alpenglow.BlockProductionParent {
			parentQueries = append(parentQueries, slot)
			return alpenglow.BlockProductionParent{
				Kind:    alpenglow.BlockProductionParentReady,
				Parent:  alpenglow.BlockID{Slot: slot - 1, Hash: solana.Hash{1}},
				ReadyAt: time.Now(),
			}
		},
	})

	loop.tick()
	require.Nil(t, loop.activeBank)
	require.Empty(t, parentQueries)
	require.False(t, loop.productionWindow.active)
}

func TestLeaderWindowRecheckStartsAdjacentReadyWindowWithoutPollDelay(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)

	const currentStart = uint64(212)
	const nextStart = currentStart + alpenglow.LeaderWindowSlots
	selected := alpenglow.BlockID{Slot: currentStart - alpenglow.LeaderWindowSlots, Hash: solana.Hash{3}}
	readyAt := time.Unix(1_700_000_000, 0)
	now := readyAt.Add(10 * time.Millisecond)
	currentParentCalls := 0
	controller := NewController()
	self := txfixture.PayerPubkey()
	global.SetReplayFrontier(selected.Slot)
	global.SetAlpenglowBlockID(selected.Slot, selected.Hash)
	global.SetAlpenglowChainedMerkleRoot(selected.Slot, solana.Hash{4})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  controller,
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: &captureBroadcaster{},
		CurrentSlot: func() uint64 { return currentStart },
		Now:         func() time.Time { return now },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			if slot >= currentStart && slot < nextStart+alpenglow.LeaderWindowSlots {
				return self, true
			}
			return solana.PublicKey{}, false
		},
		ProductionParent: func(slot uint64) alpenglow.BlockProductionParent {
			if slot == currentStart {
				currentParentCalls++
				if currentParentCalls >= 3 {
					return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentMissedWindow}
				}
			}
			return alpenglow.BlockProductionParent{
				Kind:    alpenglow.BlockProductionParentReady,
				Parent:  selected,
				ReadyAt: readyAt,
			}
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(selected.Slot, solana.Hash{5})
		},
	})
	defer func() {
		loop.mu.Lock()
		defer loop.mu.Unlock()
		loop.abortActiveSlotLocked()
	}()

	loop.tick()

	require.GreaterOrEqual(t, currentParentCalls, 3)
	for slot := currentStart; slot < nextStart; slot++ {
		require.Truef(t, loop.isLeaderSlotFinished(slot), "stale slot %d was not closed", slot)
	}
	require.NotNil(t, loop.activeBank)
	require.Equal(t, nextStart, loop.activeSlot)
	require.Equal(t, nextStart, loop.productionWindow.startSlot)
	require.NotNil(t, controller.WorkingBank())
}

func TestLeaderWindowReservesObservedFinalizationAndFanoutBudget(t *testing.T) {
	const leaderSlot = uint64(212)
	readyAt := time.Unix(1_700_000_000, 0)
	loop := NewLeaderLoop(LeaderLoopConfig{SlotDuration: AlpenglowSlotDuration})
	loop.productionWindow = leaderProductionWindow{
		active:    true,
		startSlot: leaderSlot,
		endSlot:   leaderSlot + alpenglow.LeaderWindowSlots - 1,
		nextSlot:  leaderSlot,
		readyAt:   readyAt,
	}

	// The incident trace's slowest successful finalize+footer path consumed
	// about 55ms after the freeze cutoff. The old 25ms reserve completed 30ms
	// late; the new cutoff leaves another 20ms for scheduler/fanout jitter.
	const observedWorstFinalization = 55 * time.Millisecond
	for offset := uint64(0); offset < alpenglow.LeaderWindowSlots; offset++ {
		slot := leaderSlot + offset
		protocolDeadline := loop.productionWindowProtocolDeadlineLocked(slot)
		freezeAt := loop.productionWindowDeadlineLocked(slot)
		require.Equal(t, readyAt.Add(time.Duration(offset+1)*AlpenglowSlotDuration), protocolDeadline)
		require.Equal(t, leaderBlockCompletionReserve, protocolDeadline.Sub(freezeAt))
		require.Equal(t, 20*time.Millisecond, protocolDeadline.Sub(freezeAt.Add(observedWorstFinalization)))
	}
}

func TestLeaderProductionTimingStopsAtNetworkCompletion(t *testing.T) {
	const leaderSlot = uint64(212)
	readyAt := time.Unix(1_700_000_000, 0)
	loop := NewLeaderLoop(LeaderLoopConfig{SlotDuration: AlpenglowSlotDuration})
	loop.productionWindow = leaderProductionWindow{
		active:    true,
		startSlot: leaderSlot,
		endSlot:   leaderSlot + alpenglow.LeaderWindowSlots - 1,
		nextSlot:  leaderSlot,
		readyAt:   readyAt,
	}

	finalizationStartedAt := readyAt.Add(125 * time.Millisecond)
	networkCompletedAt := readyAt.Add(190 * time.Millisecond)
	require.Equal(t,
		" window_elapsed_ms=190 finalization_ms=65 deadline_margin_ms=10",
		loop.productionTimingDetailLocked(leaderSlot, finalizationStartedAt, networkCompletedAt))

	// A blocked local handoff after the final shred must not be charged to the
	// protocol deadline or finalization path.
	localHandoffCompletedAt := networkCompletedAt.Add(40 * time.Millisecond)
	require.Equal(t,
		" window_elapsed_ms=230 finalization_ms=105 deadline_margin_ms=-30",
		loop.productionTimingDetailLocked(leaderSlot, finalizationStartedAt, localHandoffCompletedAt),
		"this is the contaminated value that recording after OnBlock would report")
}

func TestLeaderProductionTimingRequiresActiveWindow(t *testing.T) {
	loop := NewLeaderLoop(LeaderLoopConfig{SlotDuration: AlpenglowSlotDuration})
	now := time.Unix(1_700_000_000, 0)
	require.Empty(t, loop.productionTimingDetailLocked(212, now, now))
}

func TestLeaderWindowStartsNextSlotDespiteLiveSlotJump(t *testing.T) {
	const leaderSlot = uint64(212)
	readyAt := time.Unix(1_700_000_000, 0)
	now := readyAt.Add(250 * time.Millisecond)
	selected := alpenglow.BlockID{Slot: 208, Hash: solana.Hash{3}}
	global.SetReplayFrontier(leaderSlot)
	global.SetAlpenglowBlockID(leaderSlot, solana.Hash{6})
	global.SetAlpenglowChainedMerkleRoot(leaderSlot, solana.Hash{7})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  NewController(),
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: &captureBroadcaster{},
		CurrentSlot: func() uint64 { return leaderSlot + 3 },
		Now:         func() time.Time { return now },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			return txfixture.PayerPubkey(), slot >= leaderSlot && slot < leaderSlot+alpenglow.LeaderWindowSlots
		},
		ProductionParent: func(uint64) alpenglow.BlockProductionParent {
			return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentReady, Parent: selected, ReadyAt: readyAt}
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(leaderSlot, solana.Hash{8})
		},
		ParentBlockID: global.AlpenglowBlockID,
	})
	loop.productionWindow = leaderProductionWindow{
		active:    true,
		startSlot: leaderSlot,
		endSlot:   leaderSlot + alpenglow.LeaderWindowSlots - 1,
		nextSlot:  leaderSlot + 1,
		readyAt:   readyAt,
	}
	loop.markLeaderSlotFinished(leaderSlot)

	loop.tick()
	require.NotNil(t, loop.activeBank)
	assert.Equal(t, leaderSlot+1, loop.activeSlot)
}

func TestLeaderWindowRejectsParentReadyForkMismatch(t *testing.T) {
	const leaderSlot = uint64(216)
	selected := alpenglow.BlockID{Slot: 212, Hash: solana.Hash{3}}
	global.SetReplayFrontier(leaderSlot - 1)
	global.SetAlpenglowBlockID(selected.Slot, solana.Hash{4})

	loop := NewLeaderLoop(LeaderLoopConfig{
		ProductionParent: func(uint64) alpenglow.BlockProductionParent {
			return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentReady, Parent: selected}
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(selected.Slot, solana.Hash{5})
		},
		ParentBlockID: global.AlpenglowBlockID,
	})

	err := loop.startSlotLocked(leaderSlot)
	require.ErrorIs(t, err, errParentNotReady)
	assert.Nil(t, loop.activeBank)
}

func TestLeaderSlotReplayReadyOtherLeaderParent(t *testing.T) {
	self := txfixture.PayerPubkey()
	other := solana.NewWallet().PublicKey()

	loop := &LeaderLoop{
		identity: txfixture.PayerPrivateKey(),
		leaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			switch slot {
			case 44:
				return other, true
			case 45:
				return self, true
			default:
				return solana.PublicKey{}, false
			}
		},
	}

	global.SetReplayFrontier(43)
	ready, err := loop.leaderSlotReplayReady(45)
	assert.False(t, ready)
	require.ErrorIs(t, err, errParentNotReady)

	global.SetReplayFrontier(44)
	global.SetAlpenglowBlockID(44, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(44, solana.Hash{2})
	ready, err = loop.leaderSlotReplayReady(45)
	assert.True(t, ready)
	assert.NoError(t, err)
}

func TestLeaderSlotReplayReadyOwnParentRequiresReplay(t *testing.T) {
	self := txfixture.PayerPubkey()

	loop := &LeaderLoop{
		identity:            txfixture.PayerPrivateKey(),
		finishedLeaderSlots: map[uint64]struct{}{42: {}},
		leaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			if slot == 42 || slot == 43 {
				return self, true
			}
			return solana.PublicKey{}, false
		},
	}

	global.SetAlpenglowBlockID(42, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(42, solana.Hash{2})
	global.SetReplayFrontier(40)
	ready, err := loop.leaderSlotReplayReady(43)
	assert.False(t, ready)
	require.ErrorIs(t, err, errParentNotReady)

	global.SetReplayFrontier(42)
	ready, err = loop.leaderSlotReplayReady(43)
	assert.True(t, ready)
	assert.NoError(t, err)
}

func TestLeaderLoopDoesNotAttemptLeaderUntilOtherParentReplayed(t *testing.T) {
	self := txfixture.PayerPubkey()
	other := solana.NewWallet().PublicKey()
	var wallSlot uint64 = 45

	global.SetReplayFrontier(43)
	global.SetAlpenglowBlockID(43, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(43, solana.Hash{2})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  NewController(),
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: &captureBroadcaster{},
		CurrentSlot: func() uint64 { return wallSlot },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			switch slot {
			case 44:
				return other, true
			case 45:
				return self, true
			default:
				return solana.PublicKey{}, false
			}
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(44, solana.Hash{2})
		},
		ParentBlockID: func(parentSlot uint64) (solana.Hash, bool) {
			return global.AlpenglowBlockID(parentSlot)
		},
	})

	loop.tick()
	assert.Nil(t, loop.activeBank)

	global.SetReplayFrontier(44)
	global.SetAlpenglowBlockID(44, solana.Hash{3})
	global.SetAlpenglowChainedMerkleRoot(44, solana.Hash{4})
	loop.tick()
	assert.NotNil(t, loop.activeBank)
}

func TestLeaderLoopNeverBackfillsMissedLeaderSlot(t *testing.T) {
	self := txfixture.PayerPubkey()
	global.SetReplayFrontier(44)
	global.SetAlpenglowBlockID(44, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(44, solana.Hash{2})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  NewController(),
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: &captureBroadcaster{},
		CurrentSlot: func() uint64 { return 50 },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			return self, slot == 45
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(44, solana.Hash{3})
		},
		ParentBlockID: global.AlpenglowBlockID,
	})

	loop.tick()
	assert.Nil(t, loop.activeBank)
	assert.False(t, loop.isLeaderSlotFinished(45))
}

func TestLeaderLoopRecordsExpiredLocalLeaderGateFailure(t *testing.T) {
	self := txfixture.PayerPubkey()
	var wallSlot uint64 = 45
	global.SetReplayFrontier(43)

	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity:    txfixture.PayerPrivateKey(),
		CurrentSlot: func() uint64 { return wallSlot },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			return self, slot == 45
		},
	})

	loop.tick()
	require.Contains(t, loop.pendingFailures, uint64(45))
	assert.Equal(t, leaderReasonReplayNotReady, loop.pendingFailures[45].reason)
	assert.False(t, loop.isLeaderSlotFinished(45))

	wallSlot = 46
	loop.tick()
	assert.True(t, loop.isLeaderSlotFinished(45))
	assert.NotContains(t, loop.pendingFailures, uint64(45))
}

func TestLeaderAfterSkippedSlotsUsesActualReplayedParent(t *testing.T) {
	self := txfixture.PayerPubkey()
	const leaderSlot = uint64(43)
	const actualParent = uint64(40)
	global.SetReplayFrontier(leaderSlot - 1) // slots 41 and 42 were resolved skipped
	global.SetAlpenglowBlockID(actualParent, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(actualParent, solana.Hash{2})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  NewController(),
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: &captureBroadcaster{},
		CurrentSlot: func() uint64 { return leaderSlot },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			return self, slot == leaderSlot
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(actualParent, solana.Hash{3})
		},
		ParentBlockID: func(parentSlot uint64) (solana.Hash, bool) {
			return global.AlpenglowBlockID(parentSlot)
		},
	})

	loop.tick()
	require.NotNil(t, loop.activeBank)
	require.Equal(t, actualParent, loop.parentCtx.ParentSlot)
	require.Equal(t, actualParent, loop.activeBank.SlotCtx().ParentSlot)
}

func TestLeaderRestartsWhenReplayParentChangesBeforeFreeze(t *testing.T) {
	spool, err := turbine.OpenShredSpool(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(spool.Close)
	self := txfixture.PayerPubkey()
	const leaderSlot = uint64(52)
	global.SetReplayFrontier(leaderSlot - 1)
	global.SetAlpenglowBlockID(leaderSlot-1, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(leaderSlot-1, solana.Hash{2})
	parentHash := solana.Hash{3}

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  NewController(),
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: &captureBroadcaster{},
		ShredSpool:  spool,
		CurrentSlot: func() uint64 { return leaderSlot },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			return self, slot == leaderSlot
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(leaderSlot-1, parentHash)
		},
		ParentBlockID: func(parentSlot uint64) (solana.Hash, bool) {
			return global.AlpenglowBlockID(parentSlot)
		},
	})

	loop.tick()
	require.NotNil(t, loop.activeBank)
	t.Cleanup(loop.abortActiveSlotLocked)
	requireCachedHeaderParent(t, spool, leaderSlot, self, solana.Hash{1})
	parentHash = solana.Hash{9}
	global.SetAlpenglowBlockID(leaderSlot-1, solana.Hash{9})
	loop.tick()
	require.NotNil(t, loop.activeBank)
	require.Equal(t, parentHash, loop.parentCtx.ParentBankhash)
	require.False(t, loop.isLeaderSlotFinished(leaderSlot))
	requireCachedHeaderParent(t, spool, leaderSlot, self, solana.Hash{9})
}

func TestLeaderAbortsWhenOwnSlotAlreadyResolved(t *testing.T) {
	self := txfixture.PayerPubkey()
	const leaderSlot = uint64(62)
	global.SetReplayFrontier(leaderSlot - 1)
	global.SetAlpenglowBlockID(leaderSlot-1, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(leaderSlot-1, solana.Hash{2})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  NewController(),
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: &captureBroadcaster{},
		CurrentSlot: func() uint64 { return leaderSlot },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			return self, slot == leaderSlot
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(leaderSlot-1, solana.Hash{3})
		},
		ParentBlockID: global.AlpenglowBlockID,
	})

	loop.tick()
	require.NotNil(t, loop.activeBank)
	global.SetReplayFrontier(leaderSlot) // e.g. a certified skip won the slot
	loop.tick()
	assert.Nil(t, loop.activeBank)
	assert.False(t, loop.isLeaderSlotFinished(leaderSlot))
}
