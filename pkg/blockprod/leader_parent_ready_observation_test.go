package blockprod

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestLeaderParentReadyObservationActivationAndHandover(t *testing.T) {
	const start = uint64(212)
	ready := time.Unix(1_700_000_000, 0)
	now := ready.Add(10 * time.Millisecond)
	parent := alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentReady,
		Parent: alpenglow.BlockID{Slot: 208, Hash: solana.Hash{3}}, ReadyAt: ready}
	var got []LeaderParentReadyObservation
	var loop *LeaderLoop
	loop = NewLeaderLoop(LeaderLoopConfig{
		Identity: identityHolder(txfixture.PayerPrivateKey()), Now: func() time.Time { return now },
		LeaderForSlot:    func(slot uint64) (solana.PublicKey, bool) { return txfixture.PayerPubkey(), slot == start },
		ProductionParent: func(uint64) alpenglow.BlockProductionParent { return parent },
		ObserveParentReady: func(o LeaderParentReadyObservation) {
			if loop.mu.TryLock() {
				loop.mu.Unlock()
				t.Fatal("notification escaped the leader lock")
			}
			got = append(got, o)
			// The callback owns its value, not a mutable production window.
			o.ParentBlockID[0] = 99
		},
	})
	selectWindow := func() {
		loop.mu.Lock()
		defer loop.mu.Unlock()
		target, ok := loop.productionWindowTargetLocked(start)
		require.True(t, ok)
		require.Equal(t, start, target)
	}
	selectWindow()
	require.Equal(t, []LeaderParentReadyObservation{{WindowStartSlot: start, ParentSlot: 208,
		ParentBlockID: solana.Hash{3}, ReadyAt: ready, ObservedAt: now, Activation: "initial"}}, got)
	require.Nil(t, loop.activeBank, "selection does not imply an opened bank")

	// Unchanged selection does not notify again or adopt a renewed timer.
	parent.ReadyAt = ready.Add(100 * time.Millisecond)
	now = ready.Add(20 * time.Millisecond)
	selectWindow()
	require.Len(t, got, 1)
	// No holder or schedule lookup is needed to report a corrected parent of
	// this already selected window; a later identity must not relabel it.
	loop.identity = nil
	loop.leaderForSlot = func(uint64) (solana.PublicKey, bool) {
		t.Fatal("handover re-read schedule")
		return solana.PublicKey{}, false
	}
	parent.Parent.Hash = solana.Hash{6}
	now = ready.Add(30 * time.Millisecond)
	selectWindow()
	require.Len(t, got, 2)
	require.Equal(t, LeaderParentReadyObservation{WindowStartSlot: start, ParentSlot: 208,
		ParentBlockID: solana.Hash{6}, ReadyAt: ready, ObservedAt: now, Activation: "handover"}, got[1])
	require.Equal(t, ready, loop.productionWindow.readyAt)
	require.Equal(t, solana.Hash{6}, loop.productionWindow.parent.Hash)
	loop.productionWindow = leaderProductionWindow{}
	parent.Parent.Hash = solana.Hash{9}
	require.Equal(t, solana.Hash{3}, got[0].ParentBlockID)
	require.Equal(t, solana.Hash{6}, got[1].ParentBlockID)
}

func TestLeaderParentReadyObservationRefusals(t *testing.T) {
	for _, scenario := range []string{"not_ready", "missed", "zero_hash", "wrong_parent", "no_timer", "other_leader"} {
		t.Run(scenario, func(t *testing.T) {
			ready := time.Unix(1_700_000_000, 0)
			parent := alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentReady,
				Parent: alpenglow.BlockID{Slot: 208, Hash: solana.Hash{3}}, ReadyAt: ready}
			leader := txfixture.PayerPubkey()
			switch scenario {
			case "not_ready":
				parent.Kind = alpenglow.BlockProductionParentNotReady
			case "missed":
				parent.Kind = alpenglow.BlockProductionParentMissedWindow
			case "zero_hash":
				parent.Parent.Hash = solana.Hash{}
			case "wrong_parent":
				parent.Parent.Slot = 212
			case "no_timer":
				parent.ReadyAt = time.Time{}
			case "other_leader":
				leader = solana.PublicKey{9}
			}
			loop := NewLeaderLoop(LeaderLoopConfig{
				Identity: identityHolder(txfixture.PayerPrivateKey()), Now: func() time.Time { return ready },
				LeaderForSlot:      func(slot uint64) (solana.PublicKey, bool) { return leader, slot == 212 },
				ProductionParent:   func(uint64) alpenglow.BlockProductionParent { return parent },
				ObserveParentReady: func(LeaderParentReadyObservation) { t.Fatal("invalid or unselected parent was observed") },
			})
			loop.mu.Lock()
			defer loop.mu.Unlock()
			_, ok := loop.productionWindowTargetLocked(212)
			require.False(t, ok)
		})
	}
}

func TestLeaderParentReadyObservationPreservesEffectsAndLateEvents(t *testing.T) {
	type effects struct {
		clock, schedule, parent int
		finished                map[uint64]struct{}
		window                  leaderProductionWindow
		initial, handover       float64
	}
	for _, handover := range []bool{false, true} {
		t.Run(map[bool]string{false: "late_initial", true: "late_handover"}[handover], func(t *testing.T) {
			var seen [2]effects
			for i, enabled := range []bool{false, true} {
				ready := time.Unix(1_700_000_000, 0)
				now := ready.Add(130 * time.Millisecond)
				if handover {
					now = ready
				}
				parent := alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentReady,
					Parent: alpenglow.BlockID{Slot: 208, Hash: solana.Hash{3}}, ReadyAt: ready}
				var got []LeaderParentReadyObservation
				cfg := LeaderLoopConfig{Identity: identityHolder(txfixture.PayerPrivateKey()),
					Now: func() time.Time { seen[i].clock++; return now },
					LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
						seen[i].schedule++
						return txfixture.PayerPubkey(), slot == 212
					},
					ProductionParent: func(uint64) alpenglow.BlockProductionParent { seen[i].parent++; return parent },
				}
				if enabled {
					cfg.ObserveParentReady = func(o LeaderParentReadyObservation) { got = append(got, o) }
				}
				loop := NewLeaderLoop(cfg)
				initialStatus := "late"
				if handover {
					initialStatus = "on_time"
				}
				initialLabels := map[string]string{"activation": "initial", "status": initialStatus}
				handoverLabels := map[string]string{"activation": "handover", "status": "late"}
				initialBefore := prometheusCounterValue(t, statsd.BlockProductionParentReady.String(), initialLabels)
				handoverBefore := prometheusCounterValue(t, statsd.BlockProductionParentReady.String(), handoverLabels)
				loop.mu.Lock()
				_, ok := loop.productionWindowTargetLocked(212)
				if handover {
					require.True(t, ok)
					parent.Parent.Hash = solana.Hash{6}
					parent.ReadyAt = ready.Add(100 * time.Millisecond)
					now = ready.Add(130 * time.Millisecond)
					_, ok = loop.productionWindowTargetLocked(212)
				}
				loop.mu.Unlock()
				require.False(t, ok)
				seen[i].finished, seen[i].window = loop.finishedLeaderSlots, loop.productionWindow
				seen[i].initial = prometheusCounterValue(t, statsd.BlockProductionParentReady.String(), initialLabels) - initialBefore
				seen[i].handover = prometheusCounterValue(t, statsd.BlockProductionParentReady.String(), handoverLabels) - handoverBefore
				require.Len(t, seen[i].finished, 4)
				require.False(t, seen[i].window.active)
				require.Equal(t, float64(1), seen[i].initial)
				if enabled {
					count := 1
					if handover {
						count = 2
					}
					require.Len(t, got, count)
					require.Equal(t, ready, got[count-1].ReadyAt)
					require.Equal(t, now, got[count-1].ObservedAt)
				} else {
					require.Empty(t, got)
				}
			}
			require.Equal(t, seen[0], seen[1], "observer changed clock calls, source calls, metrics or window outcome")
		})
	}
}

func TestLeaderParentReadyObservationDisabled(t *testing.T) {
	loop := NewLeaderLoop(LeaderLoopConfig{Now: func() time.Time { t.Fatal("observer called protocol clock"); return time.Time{} }})
	require.Zero(t, testing.AllocsPerRun(1000, func() {
		loop.observeParentReadyLocked(alpenglow.BlockProductionParent{}, "initial", time.Time{})
	}))
}
