package replay

import (
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// funnelWorld: transactions 0 (A), 1 (B) and 2 (C), each with its own payer
// and writing account Q, so the plan is the chain 0 -> 1 -> 2. B is the
// producer whose early execution finished; C the dependent.
type funnelWorld struct {
	w       *workAhead
	f       *workAheadFunnel
	overlay *accounts.OverlayAccounts
	q       solana.PublicKey
	payers  []solana.PublicKey
}

func newFunnelWorld(t *testing.T) *funnelWorld {
	savedOn, savedCompletion := workAheadFunnelOn, workAheadCompletion
	workAheadFunnelOn, workAheadCompletion = true, false
	t.Cleanup(func() { workAheadFunnelOn, workAheadCompletion = savedOn, savedCompletion })
	q := testPk(50)
	payers := []solana.PublicKey{testPk(10), testPk(11), testPk(12)}
	program := testPk(60)
	parent := accounts.NewMemAccountsWithLen(8)
	for _, k := range append([]solana.PublicKey{q}, payers...) {
		require.NoError(t, parent.SetAccountWithoutLock(k, &accounts.Account{Key: k, Lamports: 1_000_000, Owner: [32]byte(addresses.SystemProgramAddr)}))
	}
	overlay := accounts.NewOverlayAccounts(parent)
	txs := make([]*solana.Transaction, 3)
	for i := range txs {
		txs[i] = &solana.Transaction{
			Signatures: []solana.Signature{{byte(i + 1)}},
			Message: solana.Message{
				Header:       solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
				AccountKeys:  []solana.PublicKey{payers[i], q, program},
				Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 2, Accounts: []uint16{1}}},
			},
		}
	}
	w := &workAhead{
		queue:   make(chan int, 8),
		slotCtx: &sealevel.SlotCtx{Accounts: overlay, Features: features.NewFeaturesDefault()},
		overlay: overlay, txs: txs, execute: []bool{true, true, true},
		entries: make([]atomic.Pointer[workAheadEntry], 3),
		access:  make([]atomic.Pointer[plannerTransactionAccounts], 3),
		outcome: make([]atomic.Uint32, 3),
		failFee: make([]atomic.Uint64, 3),
		durNs:   make([]atomic.Int64, 3),
	}
	f := &workAheadFunnel{w: w, ch: make(chan int, 3), done: make(chan struct{}), hints: make([]atomic.Pointer[funnelHint], 3)}
	f.build(&dependencyPlan{offsets: []uint32{0, 1, 2, 2}, dependents: []uint32{1, 2}, inDegree: []uint32{0, 1, 1}})
	return &funnelWorld{w: w, f: f, overlay: overlay, q: q, payers: payers}
}

// finishB installs B's finished early result: failed, or succeeded writing q
// with the given lamports.
func (fw *funnelWorld) finishB(t *testing.T, failed bool, qLamports uint64) {
	e := &workAheadEntry{state: workAheadFinished, finished: make(chan struct{})}
	e.keys, e.versions, e.base = workAheadReadSet(fw.w.slotCtx, fw.overlay, fw.w.txs[1])
	if failed {
		e.output.ProcessingResult.TransactionError = &TransactionError{}
	} else {
		out := []*accounts.Account{
			{Key: fw.payers[1], Lamports: 995_000, Owner: [32]byte(addresses.SystemProgramAddr)},
			{Key: fw.q, Lamports: qLamports, Owner: [32]byte(addresses.SystemProgramAddr)},
		}
		e.output.ExecCtx = &sealevel.ExecutionCtx{TransactionContext: &sealevel.TransactionCtx{Accounts: *sealevel.NewTransactionAccountsFromRefs(out, []bool{false, false})}}
	}
	fw.w.entries[1].Store(e)
}

func TestWorkAheadFunnel(t *testing.T) {
	t.Run("a failed producer newly lets the dependent start", func(t *testing.T) {
		fw := newFunnelWorld(t)
		fw.w.outcome[0].Store(1) // A committed
		fw.finishB(t, true, 0)
		fw.f.examine(1)
		require.EqualValues(t, 1, fw.f.stats.hints.Load())
		// nothing changed by the turn: an execution started then validates
		fw.f.atTurn(2)
		h := fw.f.hints[2].Load()
		require.True(t, h.versionValid && h.valueValid && !h.attempted)
	})
	t.Run("an older uncommitted writer hides behind the producer", func(t *testing.T) {
		fw := newFunnelWorld(t)
		fw.finishB(t, true, 0) // A (writes Q) has not committed
		fw.f.examine(1)
		require.EqualValues(t, 1, fw.f.stats.olderWriter.Load())
		require.Zero(t, fw.f.stats.hints.Load())
	})
	t.Run("a successful producer that left Q unchanged", func(t *testing.T) {
		fw := newFunnelWorld(t)
		fw.w.outcome[0].Store(1)
		fw.finishB(t, false, 1_000_000)
		fw.f.examine(1)
		require.EqualValues(t, 1, fw.f.stats.hints.Load())
		// B commits Q with the same value as a new object: versions differ,
		// values do not (the value check's stability rule decides adoption)
		key := [32]byte(fw.q)
		require.NoError(t, fw.overlay.SetAccount(&key, &accounts.Account{Key: fw.q, Lamports: 1_000_000, Owner: [32]byte(addresses.SystemProgramAddr)}))
		fw.f.atTurn(2)
		h := fw.f.hints[2].Load()
		require.False(t, h.versionValid)
		require.True(t, h.valueValid)
	})
	t.Run("a successful producer that changed Q", func(t *testing.T) {
		fw := newFunnelWorld(t)
		fw.w.outcome[0].Store(1)
		fw.finishB(t, false, 2_000_000)
		fw.f.examine(1)
		require.EqualValues(t, 1, fw.f.stats.changed.Load())
		require.Zero(t, fw.f.stats.hints.Load())
	})
	t.Run("the dependent uses the producer's payer", func(t *testing.T) {
		fw := newFunnelWorld(t)
		fw.w.outcome[0].Store(1)
		fw.w.txs[2].Message.AccountKeys[1] = fw.payers[1] // C writes B's payer instead of Q
		fw.w.access[2].Store(nil)
		fw.f.build(&dependencyPlan{offsets: []uint32{0, 1, 2, 2}, dependents: []uint32{1, 2}, inDegree: []uint32{0, 1, 1}})
		fw.finishB(t, true, 0)
		fw.f.examine(1)
		require.EqualValues(t, 1, fw.f.stats.payerOverlap.Load())
	})
	t.Run("a dependent already attempted is not a new opportunity", func(t *testing.T) {
		fw := newFunnelWorld(t)
		fw.w.outcome[0].Store(1)
		fw.w.entries[2].Store(&workAheadEntry{state: workAheadQueued, finished: make(chan struct{})})
		fw.finishB(t, true, 0)
		fw.f.examine(1)
		require.EqualValues(t, 1, fw.f.stats.attempted.Load())
		require.Zero(t, fw.f.stats.hints.Load())
	})
	t.Run("Q changed after the hint", func(t *testing.T) {
		fw := newFunnelWorld(t)
		fw.w.outcome[0].Store(1)
		fw.finishB(t, true, 0)
		fw.f.examine(1)
		key := [32]byte(fw.q)
		require.NoError(t, fw.overlay.SetAccount(&key, &accounts.Account{Key: fw.q, Lamports: 7, Owner: [32]byte(addresses.SystemProgramAddr)}))
		fw.f.atTurn(2)
		h := fw.f.hints[2].Load()
		require.False(t, h.versionValid || h.valueValid)
	})
	t.Run("completion admission queues the dependent", func(t *testing.T) {
		fw := newFunnelWorld(t)
		workAheadCompletion = true
		fw.w.outcome[0].Store(1)
		fw.finishB(t, true, 0)
		fw.f.examine(1)
		require.EqualValues(t, 1, fw.f.stats.admitted.Load())
		e := fw.w.entries[2].Load()
		require.Equal(t, workAheadQueued, e.state)
		require.Zero(t, e.nUnfinished, "nothing else is overtaken")
		require.Equal(t, 2, <-fw.w.queue)
		// a second finish never queues it twice
		fw.f.hints[2].Store(nil)
		fw.f.examine(1)
		require.EqualValues(t, 1, fw.f.stats.admitted.Load())
	})
	t.Run("an uncommitted writer of another account of the dependent", func(t *testing.T) {
		fw := newFunnelWorld(t)
		// A writes C's payer instead of Q, and has not committed: it is not one
		// of C's prerequisites here (no edge in this plan), but C would read
		// the payer A leaves behind
		fw.w.txs[0].Message.AccountKeys[1] = fw.payers[2]
		fw.w.access[0].Store(nil)
		fw.f.build(&dependencyPlan{offsets: []uint32{0, 1, 2, 2}, dependents: []uint32{1, 2}, inDegree: []uint32{0, 1, 1}})
		fw.finishB(t, true, 0)
		fw.f.examine(1)
		require.EqualValues(t, 1, fw.f.stats.olderWriter.Load())
		require.Zero(t, fw.f.stats.hints.Load())
	})
	t.Run("sends after close are dropped", func(t *testing.T) {
		fw := newFunnelWorld(t)
		fw.f.close()
		fw.f.finished(1) // must not panic
		fw.f.close()
	})
}
