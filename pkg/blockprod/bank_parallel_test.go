package blockprod

import (
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fundedPayer returns a new key funded with lamports in env.
func fundedPayer(t *testing.T, env *TestEnv, lamports uint64) solana.PrivateKey {
	t.Helper()
	key, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	require.NoError(t, env.Fund(key.PublicKey(), lamports))
	return key
}

// paidTransfer is a transfer that payer signs and pays for.
func paidTransfer(t *testing.T, payer solana.PrivateKey, to solana.PublicKey, lamports uint64) *solana.Transaction {
	t.Helper()
	tx, err := solana.NewTransaction([]solana.Instruction{transferIx(lamports, payer.PublicKey(), to)},
		txfixture.TestBlockhash(), solana.TransactionPayer(payer.PublicKey()))
	require.NoError(t, err)
	_, err = tx.Sign(func(solana.PublicKey) *solana.PrivateKey { return &payer })
	require.NoError(t, err)
	return tx
}

func forgeParallel(t *testing.T, bank *WorkingBank, revertProtected bool, txs ...*solana.Transaction) BundleResult {
	t.Helper()
	p, prepared := bank.PrepareBundle(txs, revertProtected)
	if prepared.Result != ForgeAccepted {
		return prepared
	}
	return bank.CommitBundle(bank.ExecuteBundle(p))
}

func TestParallelForgeMatchesForgeBundle(t *testing.T) {
	funded, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	fund := signedBundleTx(t, nil, transferIx(2_000_000, txfixture.PayerPubkey(), funded.PublicKey()))
	spend := signedBundleTx(t, []solana.PrivateKey{funded}, transferIx(500_000, funded.PublicKey(), txfixture.DestPubkey()))

	serial := NewTestEnv(TestEnvConfig{})
	defer serial.Close()
	require.Equal(t, ForgeAccepted, serial.Bank.ForgeBundle(BundleInput{Transactions: []*solana.Transaction{fund, spend}, AdmitOnActual: true}).Result)
	parallel := NewTestEnv(TestEnvConfig{})
	defer parallel.Close()
	require.Equal(t, ForgeAccepted, forgeParallel(t, parallel.Bank, true, fund, spend).Result)

	for _, key := range []solana.PublicKey{txfixture.PayerPubkey(), funded.PublicKey(), txfixture.DestPubkey()} {
		assert.Equal(t, lamportsOf(t, serial, key), lamportsOf(t, parallel, key), "%s", key)
	}
	assert.Equal(t, serial.Bank.TxFeeAccumulator(), parallel.Bank.TxFeeAccumulator())
	assert.Equal(t, serial.Bank.NumSignatures(), parallel.Bank.NumSignatures())
	assert.Equal(t, serial.Bank.ForgedTransactions(), parallel.Bank.ForgedTransactions())
	assert.Equal(t, serial.Bank.CostTracker().BlockCost(), parallel.Bank.CostTracker().BlockCost())
	assert.Equal(t, serial.Bank.EntryBytes(), parallel.Bank.EntryBytes())

	assert.Equal(t, ForgeDroppedAlreadyProcessed, forgeParallel(t, parallel.Bank, true, fund).Result, "a committed transaction is recorded")
}

func TestParallelForgeRevertProtectedFailureLandsNothing(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())
	destBefore := lamportsOf(t, env, txfixture.DestPubkey())
	ok := signedBundleTx(t, nil, transferIx(1, txfixture.PayerPubkey(), txfixture.DestPubkey()))
	fails := signedBundleTx(t, nil, transferIx(20_000_000_000, txfixture.PayerPubkey(), txfixture.DestPubkey()))
	got := forgeParallel(t, env.Bank, true, ok, fails)
	assert.Equal(t, ForgeDroppedExecution, got.Result)
	assert.Equal(t, 1, got.Index)
	requireNothingLanded(t, env, payerBefore, destBefore)
}

func TestParallelForgeStandaloneFailureLandsAndPays(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())
	destBefore := lamportsOf(t, env, txfixture.DestPubkey())
	fails := signedBundleTx(t, nil, transferIx(20_000_000_000, txfixture.PayerPubkey(), txfixture.DestPubkey()))
	require.Equal(t, ForgeAccepted, forgeParallel(t, env.Bank, false, fails).Result)
	assert.Equal(t, destBefore, lamportsOf(t, env, txfixture.DestPubkey()))
	assert.Equal(t, payerBefore-5_000, lamportsOf(t, env, txfixture.PayerPubkey()), "the failure is recorded and pays its fee")
	assert.Equal(t, []*solana.Transaction{fails}, env.Bank.ForgedTransactions())

	_, prepared := env.Bank.PrepareBundle([]*solana.Transaction{fails, fails}, false)
	assert.Equal(t, ForgeDroppedParse, prepared.Result, "a standalone bundle is one transaction")
}

func TestParallelForgeExecutesDisjointBundlesConcurrently(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	const n = 8
	payers := make([]solana.PrivateKey, n)
	dests := make([]solana.PublicKey, n)
	prepared := make([]*PreparedBundle, n)
	for i := range payers {
		payers[i] = fundedPayer(t, env, 1_000_000_000)
		dests[i] = solana.NewWallet().PublicKey()
		p, r := env.Bank.PrepareBundle([]*solana.Transaction{paidTransfer(t, payers[i], dests[i], uint64(1_000_000+i))}, true)
		require.Equal(t, ForgeAccepted, r.Result)
		prepared[i] = p
	}
	executed := make([]*ExecutedBundle, n)
	var wg sync.WaitGroup
	for i := range prepared {
		wg.Add(1)
		go func() {
			defer wg.Done()
			executed[i] = env.Bank.ExecuteBundle(prepared[i])
		}()
	}
	wg.Wait()
	// Disjoint bundles commit in any order.
	for i := n - 1; i >= 0; i-- {
		require.Equal(t, ForgeAccepted, env.Bank.CommitBundle(executed[i]).Result)
	}
	for i := range payers {
		assert.Equal(t, uint64(1_000_000+i), lamportsOf(t, env, dests[i]))
		assert.Equal(t, uint64(1_000_000_000-1_000_000-i-5_000), lamportsOf(t, env, payers[i].PublicKey()))
	}
	assert.Len(t, env.Bank.ForgedTransactions(), n)
}

func TestParallelForgeAccountSet(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	payer := fundedPayer(t, env, 1_000_000_000)
	other := solana.NewWallet().PublicKey()
	p, r := env.Bank.PrepareBundle([]*solana.Transaction{
		paidTransfer(t, payer, other, 1),
		signedBundleTx(t, nil, transferIx(1, txfixture.PayerPubkey(), txfixture.DestPubkey())),
	}, true)
	require.Equal(t, ForgeAccepted, r.Result)
	got := map[solana.PublicKey]bool{}
	p.Accounts(func(key solana.PublicKey, writable bool) {
		_, dup := got[key]
		require.False(t, dup, "each account once")
		got[key] = writable
	})
	assert.Equal(t, map[solana.PublicKey]bool{
		payer.PublicKey():       true,
		other:                   true,
		txfixture.PayerPubkey(): true,
		txfixture.DestPubkey():  true,
		solana.SystemProgramID:  false,
	}, got)

	// Loaded addresses are part of the set, with their lookup's lock.
	withLookupSlotHashes(t, env)
	addLookupTable(t, env, ActiveLookupTable(lookupTestTableKey, txfixture.DestPubkey()))
	v0, _ := lookupTransfer(t, 1)
	p, r = env.Bank.PrepareBundle([]*solana.Transaction{v0}, false)
	require.Equal(t, ForgeAccepted, r.Result)
	got = map[solana.PublicKey]bool{}
	p.Accounts(func(key solana.PublicKey, writable bool) { got[key] = writable })
	assert.Equal(t, map[solana.PublicKey]bool{
		txfixture.PayerPubkey(): true,
		solana.SystemProgramID:  false,
		txfixture.DestPubkey():  true,
	}, got)
}

func TestParallelForgeCommitChecks(t *testing.T) {
	t.Run("a frozen bank lands nothing", func(t *testing.T) {
		env := NewTestEnv(TestEnvConfig{})
		defer env.Close()
		payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())
		destBefore := lamportsOf(t, env, txfixture.DestPubkey())
		p, _ := env.Bank.PrepareBundle([]*solana.Transaction{mustSignedTransfer(t, 1)}, true)
		executed := env.Bank.ExecuteBundle(p)
		env.Bank.Freeze()
		assert.Equal(t, ForgeDroppedNoLeader, env.Bank.CommitBundle(executed).Result)
		requireNothingLanded(t, env, payerBefore, destBefore)

		p, _ = env.Bank.PrepareBundle([]*solana.Transaction{mustSignedTransfer(t, 2)}, true)
		assert.Equal(t, ForgeDroppedNoLeader, env.Bank.CommitBundle(env.Bank.ExecuteBundle(p)).Result)
	})
	t.Run("block limits are checked against actual usage at commit", func(t *testing.T) {
		probe := NewTestEnv(TestEnvConfig{})
		defer probe.Close()
		require.Equal(t, ForgeAccepted, forgeParallel(t, probe.Bank, true, mustSignedTransfer(t, 1)).Result)
		limits := costmodel.DefaultLimits()
		limits.BlockCost = probe.Bank.CostTracker().BlockCost() + 1
		env := NewTestEnv(TestEnvConfig{Limits: limits})
		defer env.Close()
		// Both execute before either commits; only the first fits.
		first, _ := env.Bank.PrepareBundle([]*solana.Transaction{mustSignedTransfer(t, 1)}, true)
		second, _ := env.Bank.PrepareBundle([]*solana.Transaction{mustSignedTransfer(t, 2)}, true)
		firstExec, secondExec := env.Bank.ExecuteBundle(first), env.Bank.ExecuteBundle(second)
		require.Equal(t, ForgeAccepted, env.Bank.CommitBundle(firstExec).Result)
		got := env.Bank.CommitBundle(secondExec)
		assert.Equal(t, ForgeDroppedCost, got.Result)
		assert.Equal(t, costmodel.ExceedBlockCost, got.Reason)
		assert.Len(t, env.Bank.ForgedTransactions(), 1)
	})
}
