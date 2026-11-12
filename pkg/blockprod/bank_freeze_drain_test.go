package blockprod

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type blockedExecutionAccounts struct {
	accounts.Accounts
	once            sync.Once
	entered, resume chan struct{}
}

func (a *blockedExecutionAccounts) GetAccount(key *[32]byte) (*accounts.Account, error) {
	if *key == [32]byte(txfixture.PayerPubkey()) {
		a.once.Do(func() { close(a.entered); <-a.resume })
	}
	return a.Accounts.GetAccount(key)
}

func TestFreezeQuiescesActualScratchExecution(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	first := mustSignedTransfer(t, 1)
	require.Equal(t, ForgeAccepted, forgeParallel(t, env.Bank, false, first).Result)
	payer, dest := lamportsOf(t, env, txfixture.PayerPubkey()), lamportsOf(t, env, txfixture.DestPubkey())
	prepared, result := env.Bank.PrepareBundle([]*solana.Transaction{mustSignedTransfer(t, 2), mustSignedTransfer(t, 3)}, true)
	require.Equal(t, ForgeAccepted, result.Result)
	blocked := &blockedExecutionAccounts{Accounts: env.SlotCtx.Accounts, entered: make(chan struct{}), resume: make(chan struct{})}
	env.SlotCtx.Accounts = blocked
	var once sync.Once
	unblock := func() { once.Do(func() { close(blocked.resume) }) }
	defer unblock()
	executed := make(chan *ExecutedBundle, 1)
	go func() { executed <- env.Bank.ExecuteBundle(prepared) }()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("scratch execution did not reach the blocked account read")
	}
	frozen := make(chan struct{})
	go func() { env.Bank.Freeze(); close(frozen) }()
	require.Eventually(t, func() bool {
		env.Bank.mu.Lock()
		defer env.Bank.mu.Unlock()
		return !env.Bank.accepting
	}, time.Second, time.Millisecond)
	select {
	case <-frozen:
		t.Fatal("Freeze returned while scratch execution still reads bank state")
	default:
	}
	// No new execution can extend the wait after admission has closed.
	rejected := env.Bank.ExecuteBundle(prepared)
	require.Equal(t, ForgeDroppedNoLeader, env.Bank.CommitBundle(rejected).Result)
	unblock()
	var scratch *ExecutedBundle
	select {
	case scratch = <-executed:
	case <-time.After(time.Second):
		t.Fatal("scratch execution did not finish")
	}
	select {
	case <-frozen:
	case <-time.After(time.Second):
		t.Fatal("Freeze did not finish after execution quiesced")
	}
	// These are representative fields bank finalization updates after Freeze.
	// With -race, this also verifies that no execution still reads the context.
	env.SlotCtx.NumSignatures++
	env.SlotCtx.Blockhash = solana.Hash{9}
	env.SlotCtx.Epoch++
	require.Equal(t, ForgeDroppedNoLeader, env.Bank.CommitBundle(scratch).Result)
	require.Equal(t, []*solana.Transaction{first}, env.Bank.ForgedTransactions())
	require.Equal(t, payer, lamportsOf(t, env, txfixture.PayerPubkey()))
	require.Equal(t, dest, lamportsOf(t, env, txfixture.DestPubkey()))
}

// Simulate a bundle whose account read outlasts both the scheduler drain and
// protocol deadline. Its result is disposable, but its borrowed bank reads
// still must finish before finalization or footer publication can begin.
func TestLeaderDrainDeadlineDoesNotBypassScratchQuiescence(t *testing.T) {
	const slot = uint64(120)
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	env.Bank.slot, env.SlotCtx.Slot, env.SlotCtx.ParentSlot = slot, slot, slot-1
	payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())
	destBefore := lamportsOf(t, env, txfixture.DestPubkey())
	txs := make([]*solana.Transaction, 5)
	for i := range txs {
		txs[i] = mustSignedTransfer(t, uint64(i+1))
	}
	prepared, result := env.Bank.PrepareBundle(txs, true)
	require.Equal(t, ForgeAccepted, result.Result)
	blocked := &blockedExecutionAccounts{Accounts: env.SlotCtx.Accounts, entered: make(chan struct{}), resume: make(chan struct{})}
	env.SlotCtx.Accounts = blocked
	var release sync.Once
	unblock := func() { release.Do(func() { close(blocked.resume) }) }
	defer unblock()
	executed := make(chan *ExecutedBundle, 1)
	go func() { executed <- env.Bank.ExecuteBundle(prepared) }()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("scratch bundle did not reach the blocking account read")
	}
	committed := make(chan struct{})
	hooks := &recordingHooks{}
	loop, _, broadcaster, delivered := newFinishingLeaderLoop(t, slot, hooks,
		func(*ParentContext, replay.CommitLeaderInput) error {
			close(committed)
			return nil
		})
	loop.activeBank = env.Bank
	env.Bank.sink = loop.activeSink
	loop.controller.SetWorkingBank(env.Bank)
	hooks.bank = func() *WorkingBank { return env.Bank }
	loop.holder = &deadlineHoldingHooks{&holdingHooks{recordingHooks: hooks, hold: true}}
	loop.currentSlot = func() uint64 { return slot }
	loop.leaderForSlot = func(uint64) (solana.PublicKey, bool) { return solana.PublicKey{}, false }
	readyAt := time.Unix(1_700_000_000, 0)
	loop.productionWindow = leaderProductionWindow{active: true, startSlot: slot, endSlot: slot + 3, nextSlot: slot, readyAt: readyAt}
	var now atomic.Int64
	now.Store(loop.productionWindowDrainDeadlineLocked(slot).UnixNano())
	loop.now = func() time.Time { return time.Unix(0, now.Load()) }
	beforeWait := leaderHistogram(t, statsd.BlockProductionExecutionQuiescence.String())
	finished := make(chan struct{})
	go func() { loop.tick(); close(finished) }()
	require.Eventually(t, func() bool {
		env.Bank.mu.Lock()
		defer env.Bank.mu.Unlock()
		return !env.Bank.accepting
	}, time.Second, time.Millisecond)
	// Advancing the protocol clock must not permit a timeout to abandon readers.
	now.Store(readyAt.Add(250 * time.Millisecond).UnixNano())
	select {
	case <-committed:
		t.Fatal("finalizer ran while the scratch bundle still borrowed bank state")
	case <-time.After(15 * time.Millisecond):
	}
	require.Zero(t, broadcaster.count(), "footer must wait for execution quiescence")
	unblock()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("leader did not finish after scratch execution released its bank reads")
	}
	var scratch *ExecutedBundle
	select {
	case scratch = <-executed:
	case <-time.After(time.Second):
		t.Fatal("scratch execution did not return")
	}
	require.True(t, *delivered)
	require.Greater(t, broadcaster.count(), 0)
	require.Equal(t, ForgeDroppedNoLeader, env.Bank.CommitBundle(scratch).Result)
	require.Equal(t, payerBefore, lamportsOf(t, env, txfixture.PayerPubkey()))
	require.Equal(t, destBefore, lamportsOf(t, env, txfixture.DestPubkey()))
	afterWait := leaderHistogram(t, statsd.BlockProductionExecutionQuiescence.String())
	require.Equal(t, beforeWait.GetSampleCount()+1, afterWait.GetSampleCount())
	// Do not assert a wall-time lower bound: observing admission close does
	// not guarantee Freeze's goroutine has started its wait timer yet.
}
