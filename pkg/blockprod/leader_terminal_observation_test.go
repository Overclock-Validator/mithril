package blockprod

import (
	"errors"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestLeaderTerminalObservationGuardAndClock(t *testing.T) {
	var got []LeaderTerminalObservation
	loop := NewLeaderLoop(LeaderLoopConfig{
		Now: func() time.Time { t.Fatal("observation called the production clock"); return time.Time{} },
		LeaderForSlot: func(uint64) (solana.PublicKey, bool) {
			t.Fatal("observation requested a schedule identity")
			return solana.PublicKey{}, false
		},
		ObserveTerminal: func(o LeaderTerminalObservation) { got = append(got, o) },
	})
	before := time.Now()
	loop.recordLeaderSlotOutcomeLocked(0, leaderOutcomeMissed, "", "not forwarded")
	loop.recordLeaderSlotOutcomeLocked(12, leaderOutcomeMissed, "", "not forwarded")
	loop.recordLeaderSlotOutcomeLocked(12, leaderOutcomeBroadcast, leaderReasonComplete, "duplicate")
	after := time.Now()
	require.Len(t, got, 1)
	o := got[0]
	require.Equal(t, uint64(12), o.Slot)
	require.Equal(t, leaderOutcomeMissed, o.Outcome)
	require.Equal(t, leaderReasonParentContextNotReady, o.Reason)
	require.Equal(t, o.Reason, o.Terminal)
	require.Equal(t, o.Reason, o.Cause)
	require.False(t, o.SampledAt.Before(before))
	require.False(t, o.SampledAt.After(after))
	require.False(t, o.BankOpened)
	require.Zero(t, o.BankLeader)
	require.Zero(t, o.ParentSlot)
	require.Zero(t, o.ParentBankhash)
	require.True(t, loop.isLeaderSlotFinishedLocked(12))
}

func TestLeaderTerminalObservationWindowBankAttribution(t *testing.T) {
	var got []LeaderTerminalObservation
	loop := NewLeaderLoop(LeaderLoopConfig{ObserveTerminal: func(o LeaderTerminalObservation) { got = append(got, o) }})
	bank := NewWorkingBank(BankConfig{Slot: 212, Leader: solana.PublicKey{7}})
	loop.activeSlot, loop.activeBank = 212, bank
	loop.parentCtx = ParentContext{ParentSlot: 211, ParentBankhash: solana.Hash{8}}
	loop.productionWindow = leaderProductionWindow{active: true, startSlot: 212, endSlot: 215, nextSlot: 212}
	loop.failProductionWindowWithTerminalLocked(212, leaderTerminalDeadlineElapsed, leaderReasonReplayNotReady, "not forwarded")
	require.Len(t, got, 4)
	for i, o := range got {
		require.Equal(t, uint64(212+i), o.Slot)
		require.Equal(t, leaderOutcomeMissed, o.Outcome)
		require.Equal(t, leaderReasonReplayNotReady, o.Cause)
		if i == 0 {
			require.True(t, o.BankOpened)
			require.Equal(t, solana.PublicKey{7}, o.BankLeader)
			require.Equal(t, uint64(211), o.ParentSlot)
			require.Equal(t, solana.Hash{8}, o.ParentBankhash)
			require.Equal(t, leaderTerminalDeadlineElapsed, o.Terminal)
		} else {
			require.False(t, o.BankOpened)
			require.Zero(t, o.BankLeader)
			require.Zero(t, o.ParentSlot)
			require.Zero(t, o.ParentBankhash)
			require.Equal(t, leaderReasonPreviousSlotMissed, o.Reason)
			require.Equal(t, leaderReasonPreviousSlotMissed, o.Terminal)
		}
	}
	require.False(t, loop.productionWindow.active)
	loop.parentCtx.ParentBankhash = solana.Hash{9}
	loop.activeBank, loop.activeSlot = nil, 0
	require.Equal(t, solana.Hash{8}, got[0].ParentBankhash, "retained event is detached from producer state")
}

func TestLeaderTerminalObservationPendingFailureWithoutIdentity(t *testing.T) {
	var got []LeaderTerminalObservation
	loop := NewLeaderLoop(LeaderLoopConfig{ObserveTerminal: func(o LeaderTerminalObservation) { got = append(got, o) }})
	// A pending failure has no recorded identity. Terminalizing it must not read
	// the current holder (nil here), which may have changed since the attempt.
	loop.missedScanStarted, loop.missedScanNext = true, 212
	loop.pendingFailures[212] = leaderSlotFailure{reason: leaderReasonReplayNotReady, detail: "not forwarded"}
	loop.reportExpiredLeaderSlotsLocked(213)
	require.Len(t, got, 1)
	require.Equal(t, leaderReasonLiveClockAdvanced, got[0].Terminal)
	require.Equal(t, leaderReasonReplayNotReady, got[0].Cause)
	require.False(t, got[0].BankOpened)
	require.Zero(t, got[0].BankLeader)
}

func TestLeaderTerminalObservationRejectsMismatchedBank(t *testing.T) {
	for _, slots := range [][2]uint64{{211, 212}, {212, 211}} {
		var got LeaderTerminalObservation
		loop := NewLeaderLoop(LeaderLoopConfig{ObserveTerminal: func(o LeaderTerminalObservation) { got = o }})
		loop.activeSlot = slots[0]
		loop.activeBank = NewWorkingBank(BankConfig{Slot: slots[1], Leader: solana.PublicKey{7}})
		loop.parentCtx = ParentContext{ParentSlot: 210, ParentBankhash: solana.Hash{8}}
		loop.recordLeaderSlotOutcomeLocked(212, leaderOutcomeMissed, leaderReasonReplayNotReady, "not forwarded")
		require.Equal(t, uint64(212), got.Slot)
		require.False(t, got.BankOpened)
		require.Zero(t, got.BankLeader)
		require.Zero(t, got.ParentSlot)
		require.Zero(t, got.ParentBankhash)
	}
}

func TestLeaderTerminalObservationFinishingPath(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "broadcast", true: "failed_commit"}[fail], func(t *testing.T) {
			loop, bank, broadcaster, delivered := newFinishingLeaderLoop(t, 106, &recordingHooks{}, func(_ *ParentContext, _ replay.CommitLeaderInput) error {
				if fail {
					return errors.New("test commit failure")
				}
				return nil
			})
			var got []LeaderTerminalObservation
			loop.observeTerminal = func(o LeaderTerminalObservation) {
				require.True(t, loop.isLeaderSlotFinishedLocked(o.Slot), "terminal is accepted before notification")
				got = append(got, o)
			}
			loop.finishActiveSlotLocked()
			require.Len(t, got, 1)
			require.Equal(t, uint64(106), got[0].Slot)
			require.True(t, got[0].BankOpened)
			require.Equal(t, bank.Leader(), got[0].BankLeader)
			if fail {
				require.Equal(t, leaderOutcomeMissed, got[0].Outcome)
				require.Equal(t, leaderReasonFreezeFailed, got[0].Cause)
				require.False(t, got[0].CostsKnown)
				require.Zero(t, got[0].Costs)
				require.False(t, got[0].FinalizationKnown)
				require.Zero(t, got[0].Finalization)
				require.False(t, *delivered)
			} else {
				require.Equal(t, leaderOutcomeBroadcast, got[0].Outcome)
				require.Equal(t, leaderReasonComplete, got[0].Cause)
				require.True(t, got[0].CostsKnown)
				require.True(t, got[0].FinalizationKnown)
				require.False(t, got[0].Finalization.BankPhasesKnown, "injected commit did not produce native inner phases")
				for _, elapsed := range []time.Duration{got[0].Finalization.SlotEnding, got[0].Finalization.Freeze, got[0].Finalization.EntryDrain,
					got[0].Finalization.RewardCertificates, got[0].Finalization.BuildBlock, got[0].Finalization.BankCommit,
					got[0].Finalization.FooterBroadcast, got[0].Finalization.EndingTickBroadcast} {
					require.GreaterOrEqual(t, elapsed, time.Duration(0))
				}
				require.Equal(t, bank.CostTracker().Limits(), got[0].Costs.Limits)
				require.True(t, *delivered)
				require.Positive(t, broadcaster.count())
			}
		})
	}
}

func TestLeaderTerminalFinalizationAttributionAndValueCopy(t *testing.T) {
	for _, state := range []string{"matching", "accepting", "missing", "wrong_bank", "wrong_slot", "failed"} {
		t.Run(state, func(t *testing.T) {
			var got LeaderTerminalObservation
			loop := NewLeaderLoop(LeaderLoopConfig{Now: func() time.Time { t.Fatal("diagnostic used protocol clock"); return time.Time{} },
				ObserveTerminal: func(o LeaderTerminalObservation) { got = o }})
			bank := NewWorkingBank(BankConfig{Slot: 12, Leader: solana.PublicKey{7}})
			if state != "accepting" {
				bank.Freeze()
			}
			loop.activeSlot, loop.activeBank = 12, bank
			outcome := leaderOutcomeBroadcast
			switch state {
			case "missing":
				loop.activeBank = nil
			case "wrong_bank":
				loop.activeBank = NewWorkingBank(BankConfig{Slot: 13})
			case "wrong_slot":
				loop.activeSlot = 13
			case "failed":
				outcome = leaderOutcomeMissed
			}
			raw := LeaderFinalizationObservation{SlotEnding: 1, Freeze: 2, EntryDrain: 3, RewardCertificates: 4,
				BuildBlock: 5, BankCommit: 6, FooterBroadcast: 7, EndingTickBroadcast: 8, BankPhasesKnown: true,
				Bank: replay.LeaderFinalizationTimings{PrepareSysvars: 9, BankHash: 10}}
			want := raw
			loop.observeLeaderTerminalLocked(12, outcome, leaderReasonComplete, leaderReasonComplete, leaderReasonComplete, &raw)
			raw.Freeze, raw.Bank.BankHash = 100, 100
			require.Equal(t, state == "matching", got.FinalizationKnown)
			if got.FinalizationKnown {
				require.Equal(t, want, got.Finalization)
			} else {
				require.Zero(t, got.Finalization)
			}
		})
	}
}

func TestLeaderTerminalObservationPreservesFinishingEffects(t *testing.T) {
	type effects struct {
		broadcasts, clockCalls int
		delivered              bool
		hooks                  []string
		legacy, terminal       float64
	}
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "broadcast", true: "failed_commit"}[fail], func(t *testing.T) {
			var observed [2]effects
			for i, enabled := range []bool{false, true} {
				t.Run(map[bool]string{false: "off", true: "on"}[enabled], func(t *testing.T) {
					hooks := &recordingHooks{}
					loop, _, broadcaster, delivered := newFinishingLeaderLoop(t, 106, hooks, func(_ *ParentContext, _ replay.CommitLeaderInput) error {
						if fail {
							return errors.New("test commit failure")
						}
						return nil
					})
					calls, notifications := 0, 0
					now := time.Now()
					loop.now = func() time.Time { calls++; return now }
					if !fail {
						loop.productionWindow = leaderProductionWindow{active: true, startSlot: 104, endSlot: 107, nextSlot: 106, readyAt: now.Add(-100 * time.Millisecond)}
					}
					if enabled {
						loop.observeTerminal = func(o LeaderTerminalObservation) {
							notifications++
							if !fail {
								require.True(t, o.FinalizationKnown)
								require.True(t, o.Finalization.WindowCompletionKnown)
							}
						}
					}
					outcome, reason := leaderOutcomeBroadcast, leaderReasonComplete
					if fail {
						outcome, reason = leaderOutcomeMissed, leaderReasonFreezeFailed
					}
					legacy := map[string]string{"outcome": outcome, "reason": reason}
					terminal := map[string]string{"outcome": outcome, "terminal": reason, "cause": reason}
					legacyBefore := prometheusCounterValue(t, statsd.BlockProductionLeaderSlots.String(), legacy)
					terminalBefore := prometheusCounterValue(t, statsd.BlockProductionLeaderSlotTerminals.String(), terminal)
					loop.finishActiveSlotLocked()
					observed[i] = effects{
						broadcasts: broadcaster.count(), clockCalls: calls, delivered: *delivered, hooks: hooks.kinds(),
						legacy:   prometheusCounterValue(t, statsd.BlockProductionLeaderSlots.String(), legacy) - legacyBefore,
						terminal: prometheusCounterValue(t, statsd.BlockProductionLeaderSlotTerminals.String(), terminal) - terminalBefore,
					}
					wantNotifications := 0
					if enabled {
						wantNotifications = 1
					}
					require.Equal(t, wantNotifications, notifications)
					require.Equal(t, float64(1), observed[i].legacy)
					require.Equal(t, float64(1), observed[i].terminal)
				})
			}
			require.Equal(t, observed[0], observed[1], "observation must not change production effects or clock calls")
		})
	}
}
