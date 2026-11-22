package blockprod

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestLeaderCostObservationRejectsLateScratchCommit(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	p, prepared := env.Bank.PrepareBundle([]*solana.Transaction{mustSignedTransfer(t, 1)}, true)
	require.Equal(t, ForgeAccepted, prepared.Result)
	executed := env.Bank.ExecuteBundle(p)
	env.Bank.Freeze()
	before, ok := env.Bank.closedCostObservation()
	require.True(t, ok)
	require.Zero(t, before.BlockCost)
	require.Zero(t, before.Transactions)
	require.Zero(t, before.EntryBytes)
	require.Equal(t, ForgeDroppedNoLeader, env.Bank.CommitBundle(executed).Result)
	after, ok := env.Bank.closedCostObservation()
	require.True(t, ok)
	require.Equal(t, before, after, "unaccepted scratch execution is not bank cost")
}

func TestSelectedLeaderEconomicsConfigIndependentOfTerminalObserver(t *testing.T) {
	for _, cfg := range []LeaderLoopConfig{{}, {ObserveTerminal: func(LeaderTerminalObservation) {}}} {
		require.False(t, NewLeaderLoop(cfg).captureSelectedSlotFacts)
		cfg.CaptureSelectedSlotFacts = true
		require.True(t, NewLeaderLoop(cfg).captureSelectedSlotFacts)
	}
}

type selectedEconomicsBroadcaster struct {
	captureBroadcaster
	calls  int
	failAt int
}

func (b *selectedEconomicsBroadcaster) Broadcast(packets [][]byte) error {
	b.calls++
	if b.calls == b.failAt {
		return errors.New("selected-economics test send failure")
	}
	return b.captureBroadcaster.Broadcast(packets)
}

func TestSelectedLeaderEconomicsFinishingEffectsAndFailures(t *testing.T) {
	// All cases exercise the real finishing, freeze and broadcast path, with
	// injected commit results. They do not prove CommitLeaderSlot fee accuracy.
	for _, state := range []string{"success", "commit_failure", "parent_changed", "footer_failure", "ending_failure", "hash_mismatch", "count_mismatch"} {
		t.Run(state, func(t *testing.T) {
			type effects struct {
				calls, packets, clockCalls int
				delivered                  bool
				hooks                      []string
			}
			var baseline effects
			for _, enabled := range []bool{false, true} {
				t.Run(map[bool]string{false: "off", true: "on"}[enabled], func(t *testing.T) {
					const slot = uint64(106)
					var produced *block.Block
					hooks := &recordingHooks{}
					loop, bank, _, delivered := newFinishingLeaderLoop(t, slot, hooks, func(parent *ParentContext, in replay.CommitLeaderInput) error {
						produced = in.Block
						before, requested := produced.SelectedEconomics()
						require.Equal(t, enabled, requested, "capture must be requested before commit, independently of terminal callbacks")
						require.Zero(t, before)
						produced.RecordSelectedFees(10, 2, 12)
						in.SlotCtx.FinalBankhash[0] = 5
						produced.ExpectedBankhash, produced.HasExpectedBankhash = [32]byte{5}, true
						switch state {
						case "commit_failure":
							return errors.New("selected-economics test commit failure")
						case "parent_changed":
							parent.ReplayGeneration++
						case "hash_mismatch":
							produced.ExpectedBankhash = [32]byte{8}
						case "count_mismatch":
							produced.Transactions = append(produced.Transactions, nil)
						}
						return nil
					})
					loop.captureSelectedSlotFacts = enabled
					broadcaster := &selectedEconomicsBroadcaster{}
					if state == "footer_failure" {
						broadcaster.failAt = 1
					} else if state == "ending_failure" {
						broadcaster.failAt = 2
					}
					// The empty fixture has no entry sends; replace only its final
					// broadcast session so each footer/ending failure is real.
					loop.activeSess = turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
						Leader: txfixture.PayerPrivateKey(), Slot: slot, ParentSlot: slot - 1,
						ParentBlockID: loop.activeParentID, ParentChainedMerkleRoot: loop.activeParentChainedRoot,
						Broadcaster: broadcaster,
					})
					clockCalls := 0
					loop.now = func() time.Time { clockCalls++; return time.Unix(1_700_000_000, 0) }
					loop.finishActiveSlotLocked()
					require.NotNil(t, produced)
					protocolSuccess := state == "success" || state == "hash_mismatch" || state == "count_mismatch"
					require.Equal(t, protocolSuccess, *delivered, "telemetry refusal must not change handoff")
					require.Nil(t, loop.activeBank)
					got, requested := produced.SelectedEconomicsFor(slot, produced.AlpenglowBlockID, [32]byte{5}, uint64(len(produced.Transactions)))
					require.Equal(t, enabled, requested)
					if enabled && state == "success" {
						require.Equal(t, block.SelectedEconomics{FeesKnown: true, Fees: block.SelectedFeeFacts{ExecutionLamports: 10, PriorityLamports: 2, TotalLamports: 12},
							NativeCostKnown: true, NativeCost: block.SelectedNativeCost{BlockCostLimit: bank.CostTracker().Limits().BlockCost}}, got)
						require.True(t, produced.HasAlpenglowBlockID)
						require.NotZero(t, produced.AlpenglowBlockID)
					} else {
						require.Zero(t, got, "failed, mismatched or disabled capture must remain unknown")
					}
					if !protocolSuccess {
						_, registered := replay.TakeLocalLeaderCommit(slot)
						require.False(t, registered, "failed finalization must not publish an adoption")
					}
					actual := effects{broadcaster.calls, broadcaster.count(), clockCalls, *delivered, hooks.kinds()}
					if !enabled {
						baseline = actual
					} else {
						require.Equal(t, baseline, actual, "capture must preserve protocol calls, fake clock calls, hooks and sends")
					}
				})
			}
		})
	}
}

func TestSelectedLeaderEconomicsSealedBeforeBlockedHandoff(t *testing.T) {
	const slot = uint64(106)
	// The existing seam injects commit; the real final sends, identity assignment
	// and handoff still run. A blocked consumer must already see sealed values.
	loop, bank, broadcaster, _ := newFinishingLeaderLoop(t, slot, &recordingHooks{}, func(_ *ParentContext, in replay.CommitLeaderInput) error {
		in.SlotCtx.FinalBankhash[0] = 5
		in.Block.ExpectedBankhash, in.Block.HasExpectedBankhash = [32]byte{5}, true
		in.Block.RecordSelectedFees(10, 2, 12)
		return nil
	})
	loop.captureSelectedSlotFacts = true
	handed := make(chan *block.Block, 1)
	release, done := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	loop.onBlock = func(b *block.Block) { handed <- b; <-release }
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("finishing owner did not join after handoff release")
		}
	})
	go func() { defer close(done); loop.finishActiveSlotLocked() }()
	var produced *block.Block
	select {
	case produced = <-handed:
	case <-time.After(2 * time.Second):
		t.Fatal("finishing owner never reached handoff")
	}
	require.Positive(t, broadcaster.count(), "network sends precede handoff")
	got, enabled := produced.SelectedEconomicsFor(slot, produced.AlpenglowBlockID, [32]byte{5}, 0)
	require.True(t, enabled)
	require.True(t, got.FeesKnown)
	require.Equal(t, uint64(12), got.Fees.TotalLamports)
	require.True(t, got.NativeCostKnown)
	require.Equal(t, block.SelectedNativeCost{BlockCostLimit: bank.CostTracker().Limits().BlockCost}, got.NativeCost)
	releaseOnce.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("finishing owner did not return")
	}
	after, _ := produced.SelectedEconomicsFor(slot, produced.AlpenglowBlockID, [32]byte{5}, 0)
	require.Equal(t, got, after, "release must not replace the detached facts")
}
