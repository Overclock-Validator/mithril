package harmonic

import (
	"math/rand/v2"
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

// newKeys returns n fresh keys, funded with lamports in each env given.
func newKeys(t *testing.T, n int, lamports uint64, envs ...*blockprod.TestEnv) []solana.PrivateKey {
	t.Helper()
	keys := make([]solana.PrivateKey, n)
	for i := range keys {
		key, err := solana.NewRandomPrivateKey()
		require.NoError(t, err)
		keys[i] = key
		for _, env := range envs {
			require.NoError(t, env.Fund(key.PublicKey(), lamports))
		}
	}
	return keys
}

// transfers is a transaction of transfers, each from[i] to to[i], paid by
// from[0] and signed by every sender.
func transfers(t *testing.T, from []solana.PrivateKey, to []solana.PublicKey, lamports uint64) *solana.Transaction {
	t.Helper()
	ixs := make([]solana.Instruction, len(from))
	for i := range from {
		ixs[i] = system.NewTransferInstruction(lamports, from[i].PublicKey(), to[i]).Build()
	}
	tx, err := solana.NewTransaction(ixs, txfixture.TestBlockhash(), solana.TransactionPayer(from[0].PublicKey()))
	require.NoError(t, err)
	_, err = tx.Sign(func(pub solana.PublicKey) *solana.PrivateKey {
		for i := range from {
			if from[i].PublicKey() == pub {
				return &from[i]
			}
		}
		return nil
	})
	require.NoError(t, err)
	return tx
}

func pay(t *testing.T, from solana.PrivateKey, to solana.PublicKey, lamports uint64) *solana.Transaction {
	return transfers(t, []solana.PrivateKey{from}, []solana.PublicKey{to}, lamports)
}

func TestExecutorRunsDisjointBundlesInParallel(t *testing.T) {
	entered := make(chan uint64, 8)
	release := make(chan struct{})
	// A fifth worker is free for bundle 5, so only its dependency holds it.
	h := startExecutorWith(t, ExecutorConfig{Workers: 5}, true, func(seq uint64) {
		entered <- seq
		<-release
	})
	env := gatedBank(t, costmodel.Limits{})
	payers := newKeys(t, 4, 1_000_000_000, env)
	h.startSlot(10, env, time.Hour)
	for i, payer := range payers {
		h.send(t, bundleEvent(10, uint64(i+1), false, pay(t, payer, solana.NewWallet().PublicKey(), 1_000_000)))
	}
	// Conflicts with bundle 1, so it waits for it.
	h.send(t, bundleEvent(10, 5, false, pay(t, payers[0], solana.NewWallet().PublicKey(), 1_000_000)))

	got := map[uint64]bool{}
	for range 4 {
		select {
		case seq := <-entered:
			got[seq] = true
		case <-time.After(2 * time.Second):
			t.Fatal("disjoint bundles did not execute at once")
		}
	}
	assert.Equal(t, map[uint64]bool{1: true, 2: true, 3: true, 4: true}, got)
	select {
	case seq := <-entered:
		t.Fatalf("bundle %d started before the bundle it conflicts with finished", seq)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	h.sync(t)
	assert.Len(t, env.Bank.ForgedTransactions(), 5)
	report := h.end(t, 10)
	assert.Equal(t, uint64(5), report.Executed)
	assert.Equal(t, 4, report.MaxParallel)
	assert.Zero(t, report.Flags)
}

func TestExecutorKeepsConflictingBundlesInStreamOrder(t *testing.T) {
	// The first link is held until every bundle has arrived, so later links
	// would run ahead of it if dependencies were not kept.
	release := make(chan struct{})
	h := startExecutorWith(t, ExecutorConfig{Workers: 8}, true, func(seq uint64) {
		if seq == 1 {
			<-release
		}
	})
	env := gatedBank(t, costmodel.Limits{})
	funder := newKeys(t, 1, 1_000_000_000, env)[0]
	// Each link starts unfunded and pays its fee from the transfer before it,
	// so a link that ran early would revert and stop the block.
	chain := newKeys(t, 4, 0)
	others := newKeys(t, 4, 1_000_000_000, env)
	h.startSlot(10, env, time.Hour)
	seq := uint64(0)
	next := func() uint64 { seq++; return seq }
	var ordered []*solana.Transaction
	link := pay(t, funder, chain[0].PublicKey(), 100_000_000)
	ordered = append(ordered, link)
	h.send(t, bundleEvent(10, next(), true, link))
	for i := 1; i < len(chain); i++ {
		h.send(t, bundleEvent(10, next(), false, pay(t, others[i], solana.NewWallet().PublicKey(), 1_000)))
		link := pay(t, chain[i-1], chain[i].PublicKey(), uint64(100_000_000-i*1_000_000))
		ordered = append(ordered, link)
		h.send(t, bundleEvent(10, next(), true, link))
	}
	close(release)
	h.sync(t)
	report := h.end(t, 10)
	assert.Zero(t, report.Reverted)
	assert.Equal(t, seq, report.Executed)

	position := map[*solana.Transaction]int{}
	for i, tx := range env.Bank.ForgedTransactions() {
		position[tx] = i
	}
	for i := 1; i < len(ordered); i++ {
		assert.Less(t, position[ordered[i-1]], position[ordered[i]], "link %d lands after link %d", i, i-1)
	}
}

func TestExecutorRevertKeepsInFlightBundlesAndOpensTheTPUAfterThem(t *testing.T) {
	release := make(chan struct{})
	h := startExecutorWith(t, ExecutorConfig{Workers: 2}, true, func(seq uint64) {
		if seq == 1 {
			<-release
		}
	})
	env := gatedBank(t, costmodel.Limits{})
	keys := newKeys(t, 2, 1_000_000_000, env)
	h.startSlot(10, env, time.Hour)
	inFlight := pay(t, keys[0], solana.NewWallet().PublicKey(), 1_000)
	h.send(t, bundleEvent(10, 1, false, inFlight))
	h.send(t, bundleEvent(10, 2, true, pay(t, keys[1], solana.NewWallet().PublicKey(), 5_000_000_000)))
	h.send(t, bundleEvent(10, 3, false, pay(t, keys[1], solana.NewWallet().PublicKey(), 1_000)))
	require.Eventually(t, func() bool { return h.exec.pending.Load() == 1 }, 2*time.Second, time.Millisecond,
		"the revert stops the block and drops what has not started")
	assert.False(t, env.Bank.TPUAllowed(), "the TPU waits for the bundle still executing")

	close(release)
	h.sync(t)
	assert.True(t, env.Bank.TPUAllowed())
	assert.Equal(t, []*solana.Transaction{inFlight}, env.Bank.ForgedTransactions(), "a disjoint bundle already executing lands")
	report := h.end(t, 10)
	assert.Equal(t, ModeFallback, report.Mode)
	assert.Equal(t, FlagReverted, report.Flags)
	assert.Equal(t, uint64(1), report.Executed)
	assert.Equal(t, uint64(1), report.Reverted)
	assert.Equal(t, uint64(1), report.Rejected)
}

// TestExecutorParallelMatchesSerial runs one random block of transfers among
// a few accounts with one worker and with eight. Committed state must match,
// and any two bundles that share a written account land in stream order.
func TestExecutorParallelMatchesSerial(t *testing.T) {
	serialEnv, parallelEnv := gatedBank(t, costmodel.Limits{}), gatedBank(t, costmodel.Limits{})
	keys := newKeys(t, 6, 1_000_000_000, serialEnv, parallelEnv)
	rng := rand.New(rand.NewPCG(1, 2))
	var bundles []BundleEvent
	for seq := uint64(1); seq <= 120; seq++ {
		perm := rng.Perm(len(keys))
		a, b, c := keys[perm[0]], keys[perm[1]], keys[perm[2]]
		amount := uint64(1 + rng.IntN(1_000))
		if rng.IntN(3) == 0 {
			tx := transfers(t, []solana.PrivateKey{a, b}, []solana.PublicKey{b.PublicKey(), c.PublicKey()}, amount)
			bundles = append(bundles, bundleEvent(10, seq, true, tx))
		} else {
			bundles = append(bundles, bundleEvent(10, seq, false, pay(t, a, b.PublicKey(), amount)))
		}
	}
	run := func(env *blockprod.TestEnv, workers int) SlotReport {
		h := startExecutorWith(t, ExecutorConfig{Workers: workers}, true, nil)
		h.startSlot(10, env, time.Hour)
		for _, e := range bundles {
			h.send(t, e)
		}
		h.sync(t)
		return h.end(t, 10)
	}
	serial, parallel := run(serialEnv, 1), run(parallelEnv, 8)
	require.Equal(t, uint64(len(bundles)), serial.Executed)
	require.Equal(t, uint64(len(bundles)), parallel.Executed)
	for _, key := range keys {
		assert.Equal(t, balance(t, serialEnv, key.PublicKey()), balance(t, parallelEnv, key.PublicKey()))
	}

	position := map[solana.Signature]int{}
	for i, tx := range parallelEnv.Bank.ForgedTransactions() {
		position[tx.Signatures[0]] = i
	}
	writes := func(e BundleEvent) map[solana.PublicKey]bool {
		w := map[solana.PublicKey]bool{}
		for _, key := range e.Transactions[0].Message.AccountKeys {
			if ok, _ := e.Transactions[0].Message.IsWritable(key); ok {
				w[key] = true
			}
		}
		return w
	}
	for i := range bundles {
		wi := writes(bundles[i])
		for j := i + 1; j < len(bundles); j++ {
			for key := range writes(bundles[j]) {
				if wi[key] {
					require.Less(t, position[bundles[i].Transactions[0].Signatures[0]], position[bundles[j].Transactions[0].Signatures[0]],
						"bundles %d and %d share %s", i+1, j+1, key)
					break
				}
			}
		}
	}
	t.Logf("max parallel: %d", parallel.MaxParallel)
}

func balance(t *testing.T, env *blockprod.TestEnv, key solana.PublicKey) uint64 {
	t.Helper()
	acct, err := env.SlotCtx.GetAccount(key)
	require.NoError(t, err)
	return acct.Lamports
}
