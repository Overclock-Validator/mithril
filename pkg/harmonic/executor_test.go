package harmonic

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBlockEngine records what the executor asks of the block engine.
type fakeBlockEngine struct {
	mu      sync.Mutex
	healthy bool
	calls   []string
	notices []uint64
}

func (f *fakeBlockEngine) Healthy() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.healthy
}

func (f *fakeBlockEngine) NotifyLeaderSlot(slot uint64, _ time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "notice")
	f.notices = append(f.notices, slot)
}

// fakePipeline records the current slot and shares the call log.
type fakePipeline struct {
	be      *fakeBlockEngine
	current []uint64
}

func (f *fakePipeline) SetCurrentSlot(slot uint64) {
	f.be.mu.Lock()
	defer f.be.mu.Unlock()
	f.be.calls = append(f.be.calls, "current")
	f.current = append(f.current, slot)
}

type executorHarness struct {
	exec     *Executor
	be       *fakeBlockEngine
	pipeline *fakePipeline
	events   chan BlockEvent
	reports  chan SlotReport
}

func startExecutor(t *testing.T, healthy bool, crank CrankBuilder) *executorHarness {
	t.Helper()
	be := &fakeBlockEngine{healthy: healthy}
	h := &executorHarness{be: be, pipeline: &fakePipeline{be: be}, events: make(chan BlockEvent), reports: make(chan SlotReport, 4)}
	h.exec = NewExecutor(ExecutorConfig{
		BlockEngine: be,
		Pipeline:    h.pipeline,
		Events:      h.events,
		Crank:       crank,
		OnReport:    func(r SlotReport) { h.reports <- r },
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.exec.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return h
}

func gatedBank(t *testing.T, limits costmodel.Limits) *blockprod.TestEnv {
	t.Helper()
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{TPUGated: true, Limits: limits})
	t.Cleanup(env.Close)
	return env
}

// startSlot opens a slot whose recording window is length long.
func (h *executorHarness) startSlot(slot uint64, env *blockprod.TestEnv, length time.Duration) {
	now := time.Now()
	h.exec.SlotStarted(blockprod.LeaderSlotInfo{Slot: slot, Bank: env.Bank, Start: now, End: now.Add(length)})
}

func (h *executorHarness) send(t *testing.T, e BlockEvent) {
	t.Helper()
	select {
	case h.events <- e:
	case <-time.After(2 * time.Second):
		t.Fatal("executor not reading events")
	}
}

// sync waits until the executor has handled everything sent so far.
func (h *executorHarness) sync(t *testing.T) {
	t.Helper()
	h.send(t, StreamLostEvent{Slot: 1 << 63})
}

func (h *executorHarness) end(t *testing.T, slot uint64) SlotReport {
	t.Helper()
	h.exec.SlotEnding(slot)
	select {
	case r := <-h.reports:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("no slot report")
		return SlotReport{}
	}
}

func transfer(t *testing.T, lamports uint64) *solana.Transaction {
	t.Helper()
	tx, err := solana.NewTransaction(
		[]solana.Instruction{system.NewTransferInstruction(lamports, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build()},
		txfixture.TestBlockhash(), solana.TransactionPayer(txfixture.PayerPubkey()))
	require.NoError(t, err)
	key := txfixture.PayerPrivateKey()
	_, err = tx.Sign(func(solana.PublicKey) *solana.PrivateKey { return &key })
	require.NoError(t, err)
	return tx
}

func blockCost(units uint64) costmodel.Limits {
	limits := costmodel.DefaultLimits()
	limits.BlockCost = units
	return limits
}

func bundleEvent(slot, seq uint64, revertProtected bool, txs ...*solana.Transaction) BundleEvent {
	return BundleEvent{Slot: slot, Seq: seq, Arrival: time.Now(), RevertProtected: revertProtected, Transactions: txs}
}

func TestExecutorFallsBackAtOnceWhenHarmonicIsUnavailable(t *testing.T) {
	h := startExecutor(t, false, nil)
	env := gatedBank(t, costmodel.Limits{})
	h.startSlot(10, env, 400*time.Millisecond)
	assert.True(t, env.Bank.TPUAllowed(), "the TPU opens before the bank is published")
	assert.Empty(t, h.be.notices, "an unhealthy session is not notified")

	report := h.end(t, 10)
	assert.Equal(t, ModeFallback, report.Mode)
	assert.Equal(t, FlagUnavailable, report.Flags)

	noDeadline := startExecutor(t, true, nil)
	env2 := gatedBank(t, costmodel.Limits{})
	noDeadline.exec.SlotStarted(blockprod.LeaderSlotInfo{Slot: 11, Bank: env2.Bank, Start: time.Now()})
	assert.True(t, env2.Bank.TPUAllowed(), "a slot with no deadline cannot run a block")
}

func TestExecutorRunsTheBlockInOrderWithTheTPUClosed(t *testing.T) {
	h := startExecutor(t, true, nil)
	env := gatedBank(t, costmodel.Limits{})
	h.startSlot(10, env, 400*time.Millisecond)
	assert.Equal(t, []string{"current", "notice"}, h.be.calls, "the slot is current before it is announced")
	assert.Equal(t, []uint64{10}, h.pipeline.current)
	assert.Equal(t, []uint64{10}, h.be.notices)

	a, b, c := transfer(t, 1), transfer(t, 2), transfer(t, 3)
	h.send(t, bundleEvent(10, 1, true, a, b))
	h.send(t, bundleEvent(10, 2, false, c))
	h.sync(t)
	assert.Equal(t, []*solana.Transaction{a, b, c}, env.Bank.ForgedTransactions())
	assert.False(t, env.Bank.TPUAllowed(), "the TPU stays closed during a Harmonic block")

	report := h.end(t, 10)
	assert.Equal(t, ModeHarmonic, report.Mode)
	assert.Equal(t, uint64(2), report.Executed)
	assert.Zero(t, report.Flags)
	assert.Equal(t, []uint64{10, 0}, h.pipeline.current, "the slot stops being current when it ends")
}

func TestExecutorFallsBackWhenNoBlockArrivesByTheThreshold(t *testing.T) {
	h := startExecutor(t, true, nil)
	env := gatedBank(t, costmodel.Limits{})
	h.startSlot(10, env, 100*time.Millisecond)
	require.Eventually(t, env.Bank.TPUAllowed, time.Second, time.Millisecond, "the threshold timer opens the TPU")

	h.send(t, bundleEvent(10, 1, false, transfer(t, 1)))
	h.sync(t)
	assert.Empty(t, env.Bank.ForgedTransactions(), "a bundle after the switch is never executed")
	report := h.end(t, 10)
	assert.Equal(t, ModeFallback, report.Mode)
	assert.Equal(t, FlagTimeout, report.Flags)
	assert.Equal(t, uint64(1), report.Rejected)
}

func TestExecutorStopTriggers(t *testing.T) {
	overdraw := func(t *testing.T) *solana.Transaction { return transfer(t, 20_000_000_000) }
	cases := map[string]struct {
		events func(t *testing.T) []BlockEvent
		limits costmodel.Limits
		flag   EndFlags
	}{
		"revert": {
			events: func(t *testing.T) []BlockEvent {
				return []BlockEvent{bundleEvent(10, 1, false, transfer(t, 1)), bundleEvent(10, 2, true, transfer(t, 2), overdraw(t))}
			},
			flag: FlagReverted,
		},
		"invalid bundle": {
			events: func(t *testing.T) []BlockEvent {
				return []BlockEvent{bundleEvent(10, 1, false, transfer(t, 1)), InvalidBundleEvent{Slot: 10, Seq: 2, Reason: errors.New("bad")}}
			},
			flag: FlagFailed,
		},
		"sequence gap": {
			events: func(t *testing.T) []BlockEvent {
				return []BlockEvent{bundleEvent(10, 1, false, transfer(t, 1)), bundleEvent(10, 3, false, transfer(t, 2))}
			},
			flag: FlagFailed,
		},
		"stream lost": {
			events: func(t *testing.T) []BlockEvent {
				return []BlockEvent{bundleEvent(10, 1, false, transfer(t, 1)), StreamLostEvent{Slot: 10}}
			},
			flag: FlagFailed,
		},
		"does not fit": {
			events: func(t *testing.T) []BlockEvent {
				return []BlockEvent{bundleEvent(10, 1, false, transfer(t, 1)), bundleEvent(10, 2, true, transfer(t, 2), transfer(t, 3))}
			},
			limits: blockCost(4_000),
			flag:   FlagFailed,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := startExecutor(t, true, nil)
			env := gatedBank(t, tc.limits)
			h.startSlot(10, env, time.Hour)
			for _, e := range tc.events(t) {
				h.send(t, e)
			}
			later := transfer(t, 4)
			h.send(t, bundleEvent(10, 99, false, later))
			h.sync(t)
			assert.True(t, env.Bank.TPUAllowed(), "a stop before the cutoff falls back")
			assert.Len(t, env.Bank.ForgedTransactions(), 1, "only the first bundle landed")
			report := h.end(t, 10)
			assert.Equal(t, ModeFallback, report.Mode)
			assert.Equal(t, tc.flag, report.Flags)
			assert.Equal(t, uint64(1), report.Executed)
		})
	}
}

func TestExecutorCutoffAndVoteTail(t *testing.T) {
	t.Run("a bundle after the cutoff stops the block without falling back", func(t *testing.T) {
		h := startExecutor(t, true, nil)
		env := gatedBank(t, costmodel.Limits{})
		h.startSlot(10, env, 80*time.Millisecond)
		h.send(t, bundleEvent(10, 1, false, transfer(t, 1)))
		time.Sleep(100 * time.Millisecond)
		h.send(t, bundleEvent(10, 2, false, transfer(t, 2)))
		h.sync(t)
		assert.Len(t, env.Bank.ForgedTransactions(), 1)
		assert.False(t, env.Bank.TPUAllowed(), "past the cutoff the block is simply over")
		report := h.end(t, 10)
		assert.Equal(t, ModeHarmonic, report.Mode)
		assert.Equal(t, FlagTimeout, report.Flags)
	})
	t.Run("a revert in the vote tail lands nothing and the block continues", func(t *testing.T) {
		h := startExecutor(t, true, nil)
		env := gatedBank(t, costmodel.Limits{})
		now := time.Now()
		h.exec.SlotStarted(blockprod.LeaderSlotInfo{Slot: 10, Bank: env.Bank, Start: now, End: now.Add(time.Second)})
		h.send(t, bundleEvent(10, 1, false, transfer(t, 1)))
		h.sync(t)
		h.exec.now = func() time.Time { return now.Add(time.Second - 10*time.Millisecond) }
		h.send(t, bundleEvent(10, 2, true, transfer(t, 2), transfer(t, 20_000_000_000)))
		h.send(t, bundleEvent(10, 3, false, transfer(t, 3)))
		h.sync(t)
		assert.Len(t, env.Bank.ForgedTransactions(), 2)
		report := h.end(t, 10)
		assert.Equal(t, ModeHarmonic, report.Mode)
		assert.Zero(t, report.Flags)
		assert.Equal(t, uint64(1), report.Reverted)
	})
}

func TestExecutorCrankRunsFirstAndItsFailureFallsBack(t *testing.T) {
	crank := transfer(t, 9)
	h := startExecutor(t, true, func(*blockprod.WorkingBank) ([]*solana.Transaction, error) {
		return []*solana.Transaction{crank}, nil
	})
	env := gatedBank(t, costmodel.Limits{})
	h.startSlot(10, env, time.Hour)
	first := transfer(t, 1)
	h.send(t, bundleEvent(10, 1, false, first))
	h.send(t, bundleEvent(10, 2, false, transfer(t, 2)))
	h.sync(t)
	forged := env.Bank.ForgedTransactions()
	require.Len(t, forged, 3)
	assert.Equal(t, []*solana.Transaction{crank, first}, forged[:2], "the crank lands before the first block bundle, once")

	failing := startExecutor(t, true, func(*blockprod.WorkingBank) ([]*solana.Transaction, error) {
		return nil, errors.New("tip accounts unreadable")
	})
	env2 := gatedBank(t, costmodel.Limits{})
	failing.startSlot(10, env2, time.Hour)
	failing.send(t, bundleEvent(10, 1, false, transfer(t, 1)))
	failing.sync(t)
	assert.Empty(t, env2.Bank.ForgedTransactions())
	assert.True(t, env2.Bank.TPUAllowed())
	report := failing.end(t, 10)
	assert.Equal(t, ModeFallback, report.Mode)
	assert.Equal(t, FlagFailed, report.Flags)
}

func TestExecutorSlotsAreIndependent(t *testing.T) {
	h := startExecutor(t, true, nil)
	env10 := gatedBank(t, costmodel.Limits{})
	h.startSlot(10, env10, time.Hour)
	h.send(t, bundleEvent(10, 1, false, transfer(t, 1)))
	h.send(t, InvalidBundleEvent{Slot: 10, Seq: 2, Reason: errors.New("bad")})
	h.sync(t)
	r10 := h.end(t, 10)
	assert.Equal(t, FlagFailed, r10.Flags)

	env11 := gatedBank(t, costmodel.Limits{})
	h.startSlot(11, env11, time.Hour)
	h.send(t, bundleEvent(10, 3, false, transfer(t, 2)))
	h.send(t, bundleEvent(11, 1, false, transfer(t, 3)))
	h.sync(t)
	assert.Len(t, env11.Bank.ForgedTransactions(), 1, "the new slot starts clean at sequence 1")
	assert.False(t, env11.Bank.TPUAllowed())

	h.exec.SlotAborted(11, "parent_changed_before_freeze")
	select {
	case r := <-h.reports:
		assert.True(t, r.Aborted)
		assert.Equal(t, "parent_changed_before_freeze", r.Reason)
		assert.Equal(t, FlagFailed, r.Flags)
	case <-time.After(2 * time.Second):
		t.Fatal("no report for the aborted slot")
	}
	assert.Equal(t, []uint64{10, 0, 11, 0}, h.pipeline.current)
}
