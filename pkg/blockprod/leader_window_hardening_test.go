package blockprod

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestFailProductionWindowAtMaxUint64DoesNotWrap(t *testing.T) {
	const maxUint64 = ^uint64(0)
	const startSlot = maxUint64 - 3

	loop := NewLeaderLoop(LeaderLoopConfig{})
	loop.productionWindow = leaderProductionWindow{
		active:    true,
		startSlot: startSlot,
		endSlot:   maxUint64,
		nextSlot:  maxUint64 - 1,
	}

	loop.failProductionWindowLocked(maxUint64-1, leaderReasonReplayNotReady, "test failure")

	require.False(t, loop.productionWindow.active)
	require.Len(t, loop.finishedLeaderSlots, 2)
	require.True(t, loop.isLeaderSlotFinishedLocked(maxUint64-1))
	require.True(t, loop.isLeaderSlotFinishedLocked(maxUint64))
	require.False(t, loop.isLeaderSlotFinishedLocked(0))
}

func TestAdvanceProductionWindowAtMaxUint64DoesNotWrap(t *testing.T) {
	const maxUint64 = ^uint64(0)
	const startSlot = maxUint64 - 3

	loop := NewLeaderLoop(LeaderLoopConfig{})
	loop.productionWindow = leaderProductionWindow{
		active:    true,
		startSlot: startSlot,
		endSlot:   maxUint64,
		nextSlot:  maxUint64 - 1,
	}

	loop.advanceProductionWindowLocked(maxUint64 - 1)
	require.True(t, loop.productionWindow.active)
	require.Equal(t, maxUint64, loop.productionWindow.nextSlot)

	loop.advanceProductionWindowLocked(maxUint64)
	require.Equal(t, leaderProductionWindow{}, loop.productionWindow)
}

func TestExpiredScannerParksAtMaxUint64CurrentWindow(t *testing.T) {
	const maxUint64 = ^uint64(0)
	const startSlot = maxUint64 - 3

	self := txfixture.PayerPubkey()
	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity: identityHolder(txfixture.PayerPrivateKey()),
		LeaderForSlot: func(uint64) (solana.PublicKey, bool) {
			return self, true
		},
		ProductionParent: func(uint64) alpenglow.BlockProductionParent {
			return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentNotReady}
		},
	})
	loop.missedScanStarted = true
	loop.missedScanNext = startSlot

	loop.reportExpiredLeaderSlotsLocked(maxUint64)

	require.Equal(t, startSlot, loop.missedScanNext)
	require.Empty(t, loop.finishedLeaderSlots)
}

func TestIntrawindowMissedWindowClosesRemainderBeforeBankStarts(t *testing.T) {
	const startSlot = uint64(212)
	const targetSlot = startSlot + 1
	now := time.Unix(1_700_000_000, 0)
	self := txfixture.PayerPubkey()

	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity: identityHolder(txfixture.PayerPrivateKey()),
		Now:      func() time.Time { return now },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			if slot == startSlot {
				return self, true
			}
			return solana.PublicKey{}, false
		},
		ProductionParent: func(slot uint64) alpenglow.BlockProductionParent {
			if slot == targetSlot {
				return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentMissedWindow}
			}
			return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentNotReady}
		},
	})
	loop.productionWindow = leaderProductionWindow{
		active:    true,
		startSlot: startSlot,
		endSlot:   startSlot + alpenglow.LeaderWindowSlots - 1,
		nextSlot:  targetSlot,
		readyAt:   now,
	}
	loop.markLeaderSlotFinished(startSlot)

	target, ok := loop.productionWindowTargetLocked(targetSlot)

	require.False(t, ok)
	require.Zero(t, target)
	require.False(t, loop.productionWindow.active)
	for slot := startSlot; slot < startSlot+alpenglow.LeaderWindowSlots; slot++ {
		require.Truef(t, loop.isLeaderSlotFinishedLocked(slot), "slot %d was not closed after ParentReady moved beyond it", slot)
	}
}

func TestActiveIntrawindowMissedWindowAbortsAndClosesRemainder(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)

	const startSlot = uint64(212)
	const activeSlot = startSlot + 1
	self := txfixture.PayerPubkey()
	controller := NewController()
	bank := NewWorkingBank(BankConfig{Slot: activeSlot})
	controller.SetWorkingBank(bank)
	global.SetReplayFrontier(startSlot)

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  controller,
		Identity:    identityHolder(txfixture.PayerPrivateKey()),
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

	require.Nil(t, loop.activeBank)
	require.Zero(t, loop.activeSlot)
	require.Nil(t, controller.WorkingBank())
	require.False(t, bank.accepting)
	require.False(t, loop.productionWindow.active)
	for slot := startSlot; slot < startSlot+alpenglow.LeaderWindowSlots; slot++ {
		require.Truef(t, loop.isLeaderSlotFinishedLocked(slot), "slot %d was not closed after ParentReady moved beyond the active bank", slot)
	}
}

func TestExpiredProductionWindowExposesDeadlineTerminalAndGateCause(t *testing.T) {
	const slot = uint64(212)
	readyAt := time.Unix(1_700_000_000, 0)
	loop := NewLeaderLoop(LeaderLoopConfig{SlotDuration: AlpenglowSlotDuration})
	loop.productionWindow = leaderProductionWindow{
		active:    true,
		startSlot: slot,
		endSlot:   slot,
		nextSlot:  slot,
		readyAt:   readyAt,
	}
	loop.pendingFailures[slot] = leaderSlotFailure{
		reason: leaderReasonReplayNotReady,
		detail: "replay gate did not open",
	}

	terminalLabels := map[string]string{
		"outcome":  leaderOutcomeMissed,
		"terminal": leaderTerminalDeadlineElapsed,
		"cause":    leaderReasonReplayNotReady,
	}
	legacyLabels := map[string]string{
		"outcome": leaderOutcomeMissed,
		"reason":  leaderReasonReplayNotReady,
	}
	terminalBefore := prometheusCounterValue(t, statsd.BlockProductionLeaderSlotTerminals.String(), terminalLabels)
	legacyBefore := prometheusCounterValue(t, statsd.BlockProductionLeaderSlots.String(), legacyLabels)

	loop.expireProductionWindowLocked(slot, loop.productionWindowDeadlineLocked(slot), "test cutoff")

	require.Equal(t, terminalBefore+1,
		prometheusCounterValue(t, statsd.BlockProductionLeaderSlotTerminals.String(), terminalLabels))
	require.Equal(t, legacyBefore+1,
		prometheusCounterValue(t, statsd.BlockProductionLeaderSlots.String(), legacyLabels))
}

func prometheusCounterValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if len(metric.GetLabel()) != len(labels) {
				continue
			}
			matches := true
			for _, pair := range metric.GetLabel() {
				if labels[pair.GetName()] != pair.GetValue() {
					matches = false
					break
				}
			}
			if matches {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}
