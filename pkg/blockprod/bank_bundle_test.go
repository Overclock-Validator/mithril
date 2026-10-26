package blockprod

import (
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func signedBundleTx(t *testing.T, signers []solana.PrivateKey, instructions ...solana.Instruction) *solana.Transaction {
	t.Helper()
	tx, err := solana.NewTransaction(instructions, txfixture.TestBlockhash(), solana.TransactionPayer(txfixture.PayerPubkey()))
	require.NoError(t, err)
	keys := append([]solana.PrivateKey{txfixture.PayerPrivateKey()}, signers...)
	_, err = tx.Sign(func(pub solana.PublicKey) *solana.PrivateKey {
		for i := range keys {
			if keys[i].PublicKey() == pub {
				return &keys[i]
			}
		}
		return nil
	})
	require.NoError(t, err)
	return tx
}

func transferIx(lamports uint64, from, to solana.PublicKey) solana.Instruction {
	return system.NewTransferInstruction(lamports, from, to).Build()
}

// requireNothingLanded checks a refused bundle left no trace in the bank.
func requireNothingLanded(t *testing.T, env *TestEnv, payerBefore, destBefore uint64) {
	t.Helper()
	assert.Equal(t, payerBefore, lamportsOf(t, env, txfixture.PayerPubkey()))
	assert.Equal(t, destBefore, lamportsOf(t, env, txfixture.DestPubkey()))
	assert.Empty(t, env.Bank.ForgedTransactions())
	assert.Empty(t, env.Bank.seenMessages)
	assert.Zero(t, env.Bank.TxFeeAccumulator().TotalFees)
	assert.Zero(t, env.Bank.NumSignatures())
	assert.Zero(t, env.Bank.CostTracker().BlockCost())
	assert.Zero(t, env.Bank.EntryBuilder().PendingCount())
	assert.Empty(t, env.SlotCtx.ModifiedAccts)
}

func TestForgeBundleLaterTransactionsSeeEarlierWrites(t *testing.T) {
	funded, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	fund := signedBundleTx(t, nil, transferIx(2_000_000, txfixture.PayerPubkey(), funded.PublicKey()))
	spend := signedBundleTx(t, []solana.PrivateKey{funded}, transferIx(500_000, funded.PublicKey(), txfixture.DestPubkey()))

	alone := NewTestEnv(TestEnvConfig{})
	defer alone.Close()
	destAlone := lamportsOf(t, alone, txfixture.DestPubkey())
	alone.Bank.ForgeTransaction(spend, 0)
	require.Equal(t, destAlone, lamportsOf(t, alone, txfixture.DestPubkey()), "alone, the second transaction fails: it depends on the first")

	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())
	destBefore := lamportsOf(t, env, txfixture.DestPubkey())

	got := env.Bank.ForgeBundle(BundleInput{Transactions: []*solana.Transaction{fund, spend}})
	require.Equal(t, ForgeAccepted, got.Result, "%+v", got)
	assert.Equal(t, -1, got.Index)
	assert.Equal(t, uint64(1_500_000), lamportsOf(t, env, funded.PublicKey()))
	assert.Equal(t, destBefore+500_000, lamportsOf(t, env, txfixture.DestPubkey()))
	// One signature on the first transaction, two on the second.
	assert.Equal(t, payerBefore-2_000_000-15_000, lamportsOf(t, env, txfixture.PayerPubkey()))
	assert.Equal(t, uint64(15_000), env.Bank.TxFeeAccumulator().TotalFees)
	assert.Equal(t, uint64(3), env.Bank.NumSignatures())
	assert.Equal(t, []*solana.Transaction{fund, spend}, env.Bank.ForgedTransactions())
	assert.Equal(t, 2, env.Bank.EntryBuilder().PendingCount())
	assert.NotZero(t, env.Bank.CostTracker().BlockCost())

	again := env.Bank.ForgeBundle(BundleInput{Transactions: []*solana.Transaction{fund}})
	assert.Equal(t, ForgeDroppedAlreadyProcessed, again.Result, "landed transactions are recorded")
}

func TestForgeBundleFailureLandsNothing(t *testing.T) {
	first := mustSignBankTestTransaction(t, transferIx(1, txfixture.PayerPubkey(), txfixture.DestPubkey()))
	cases := map[string]*solana.Transaction{
		"instruction error": mustSignBankTestTransaction(t, transferIx(20_000_000_000, txfixture.PayerPubkey(), txfixture.DestPubkey())),
		"fees-only load failure": mustSignBankTestTransaction(t,
			computebudget.NewSetLoadedAccountsDataSizeLimitInstruction(1).Build(),
			transferIx(2, txfixture.PayerPubkey(), txfixture.DestPubkey())),
		"rent-state failure": mustSignBankTestTransaction(t, transferIx(1, txfixture.PayerPubkey(), solana.PublicKey{0xEE})),
	}
	for name, failing := range cases {
		t.Run(name, func(t *testing.T) {
			env := NewTestEnv(TestEnvConfig{})
			defer env.Close()
			payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())
			destBefore := lamportsOf(t, env, txfixture.DestPubkey())

			got := env.Bank.ForgeBundle(BundleInput{Transactions: []*solana.Transaction{first, failing}})
			require.Equal(t, ForgeDroppedExecution, got.Result)
			assert.Equal(t, 1, got.Index)
			assert.Error(t, got.Err)
			requireNothingLanded(t, env, payerBefore, destBefore)

			// The first transaction was never recorded, so it can still land.
			result, _ := env.Bank.ForgeTransaction(first, 0)
			assert.Equal(t, ForgeAccepted, result)
		})
	}
}

func TestForgeBundleRefusesProcessedAndDuplicateTransactions(t *testing.T) {
	a := mustSignBankTestTransaction(t, transferIx(1, txfixture.PayerPubkey(), txfixture.DestPubkey()))
	b := mustSignBankTestTransaction(t, transferIx(2, txfixture.PayerPubkey(), txfixture.DestPubkey()))

	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())
	destBefore := lamportsOf(t, env, txfixture.DestPubkey())
	got := env.Bank.ForgeBundle(BundleInput{Transactions: []*solana.Transaction{a, b, a}})
	require.Equal(t, ForgeDroppedAlreadyProcessed, got.Result)
	assert.Equal(t, 2, got.Index)
	requireNothingLanded(t, env, payerBefore, destBefore)

	result, _ := env.Bank.ForgeTransaction(a, 0)
	require.Equal(t, ForgeAccepted, result)
	got = env.Bank.ForgeBundle(BundleInput{Transactions: []*solana.Transaction{b, a}})
	require.Equal(t, ForgeDroppedAlreadyProcessed, got.Result)
	assert.Equal(t, 1, got.Index)
	assert.Equal(t, []*solana.Transaction{a}, env.Bank.ForgedTransactions())

	assert.Equal(t, ForgeDroppedParse, env.Bank.ForgeBundle(BundleInput{}).Result)
	env.Bank.Close()
	assert.Equal(t, ForgeDroppedNoLeader, env.Bank.ForgeBundle(BundleInput{Transactions: []*solana.Transaction{b}}).Result)
}

func TestForgeBundleCostAdmission(t *testing.T) {
	actual := standaloneTransferUsage(t)
	bundle := func(t *testing.T) []*solana.Transaction {
		return []*solana.Transaction{mustSignedTransfer(t, 1), mustSignedTransfer(t, 2)}
	}
	withBlockCost := func(blockCost uint64) *TestEnv {
		limits := costmodel.DefaultLimits()
		limits.BlockCost = blockCost
		return NewTestEnv(TestEnvConfig{Limits: limits})
	}

	t.Run("requested units are checked before execution", func(t *testing.T) {
		env := withBlockCost(2 * actual)
		defer env.Close()
		payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())
		destBefore := lamportsOf(t, env, txfixture.DestPubkey())
		got := env.Bank.ForgeBundle(BundleInput{Transactions: bundle(t)})
		require.Equal(t, ForgeDroppedCost, got.Result)
		assert.Equal(t, costmodel.ExceedBlockCost, got.Reason)
		requireNothingLanded(t, env, payerBefore, destBefore)
	})
	t.Run("actual usage that fits is admitted", func(t *testing.T) {
		env := withBlockCost(2 * actual)
		defer env.Close()
		got := env.Bank.ForgeBundle(BundleInput{Transactions: bundle(t), AdmitOnActual: true})
		require.Equal(t, ForgeAccepted, got.Result, "%+v", got)
		assert.Equal(t, 2*actual, env.Bank.CostTracker().BlockCost())
	})
	t.Run("actual usage that does not fit lands nothing", func(t *testing.T) {
		env := withBlockCost(2*actual - 1)
		defer env.Close()
		payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())
		destBefore := lamportsOf(t, env, txfixture.DestPubkey())
		got := env.Bank.ForgeBundle(BundleInput{Transactions: bundle(t), AdmitOnActual: true})
		require.Equal(t, ForgeDroppedCost, got.Result)
		assert.Equal(t, costmodel.ExceedBlockCost, got.Reason)
		requireNothingLanded(t, env, payerBefore, destBefore)
	})
}

func TestForgeBundleEntryBytes(t *testing.T) {
	txs := []*solana.Transaction{mustSignedTransfer(t, 1), mustSignedTransfer(t, 2), mustSignedTransfer(t, 3)}
	size, err := serializedTransactionSize(txs[0])
	require.NoError(t, err)

	t.Run("a bundle past the slot budget is refused before execution", func(t *testing.T) {
		limits := costmodel.DefaultLimits()
		limits.MaxEntryBytes = uint64(entryBatchOverheadBytes + 3*size - 1)
		env := NewTestEnv(TestEnvConfig{Limits: limits})
		defer env.Close()
		payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())
		destBefore := lamportsOf(t, env, txfixture.DestPubkey())
		got := env.Bank.ForgeBundle(BundleInput{Transactions: txs})
		require.Equal(t, ForgeDroppedCost, got.Result)
		assert.Equal(t, costmodel.ExceedBatchBytes, got.Reason)
		requireNothingLanded(t, env, payerBefore, destBefore)

		limits.MaxEntryBytes++
		exact := NewTestEnv(TestEnvConfig{Limits: limits})
		defer exact.Close()
		assert.Equal(t, ForgeAccepted, exact.Bank.ForgeBundle(BundleInput{Transactions: txs}).Result, "exactly at the budget fits")
	})

	t.Run("batches split mid-bundle keep the bundle in order", func(t *testing.T) {
		sink := &captureSink{}
		limits := costmodel.DefaultLimits()
		limits.MaxBatchBytes = uint64(entryBatchOverheadBytes + size + size/2)
		env := NewTestEnv(TestEnvConfig{Limits: limits, Sink: sink})
		defer env.Close()
		require.Equal(t, ForgeAccepted, env.Bank.ForgeBundle(BundleInput{Transactions: txs}).Result)
		env.Bank.FlushEntries()
		var entered []solana.Signature
		for _, batch := range sink.batches {
			for _, entry := range batch {
				for _, tx := range entry.Txns {
					entered = append(entered, tx.Signatures[0])
				}
			}
		}
		require.Len(t, sink.batches, 3, "each transaction filled its own batch")
		assert.Equal(t, []solana.Signature{txs[0].Signatures[0], txs[1].Signatures[0], txs[2].Signatures[0]}, entered)
	})
}

func TestForgeBundleMatchesReplay(t *testing.T) {
	funded, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	txs := []*solana.Transaction{
		signedBundleTx(t, nil, transferIx(2_000_000, txfixture.PayerPubkey(), funded.PublicKey())),
		signedBundleTx(t, []solana.PrivateKey{funded}, transferIx(500_000, funded.PublicKey(), txfixture.DestPubkey())),
		mustSignedTransfer(t, 7),
	}
	leader := NewTestEnv(TestEnvConfig{})
	defer leader.Close()
	require.Equal(t, ForgeAccepted, leader.Bank.ForgeBundle(BundleInput{Transactions: txs}).Result)

	validator := NewTestEnv(TestEnvConfig{})
	defer validator.Close()
	var totalFee uint64
	for _, tx := range txs {
		feeInfo, _, err := replay.ProcessTransaction(validator.SlotCtx, &sync.WaitGroup{}, tx, nil, nil, nil, false)
		require.NoError(t, err)
		require.NotNil(t, feeInfo)
		totalFee += feeInfo.TotalFee
	}
	assert.Equal(t, totalFee, leader.Bank.TxFeeAccumulator().TotalFees)
	for _, key := range []solana.PublicKey{txfixture.PayerPubkey(), txfixture.DestPubkey(), funded.PublicKey()} {
		assert.Equal(t, lamportsOf(t, validator, key), lamportsOf(t, leader, key), "%s", key)
	}
}

func TestEntryBuilderWouldExceedSlotNMatchesAppends(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for trial := range 500 {
		limits := costmodel.DefaultLimits()
		limits.MaxBatchBytes = uint64(300 + rng.IntN(2000))
		prefix := make([]int, rng.IntN(4))
		sizes := make([]int, 1+rng.IntN(5))
		for i := range prefix {
			prefix[i] = 100 + rng.IntN(600)
		}
		for i := range sizes {
			sizes[i] = 100 + rng.IntN(600)
		}
		builder := NewEntryBuilder(limits, solana.Hash{})
		for _, size := range prefix {
			builder.appendSerialized(solana.Transaction{}, size, size)
		}
		probe := *builder
		probe.pendingTxns = append([]solana.Transaction(nil), builder.pendingTxns...)
		for _, size := range sizes {
			probe.appendSerialized(solana.Transaction{}, size, size)
		}
		after := probe.SlotBytes()

		builder.limits.MaxEntryBytes = uint64(after)
		require.Falsef(t, builder.wouldExceedSlotN(sizes), "trial %d: exactly at the budget", trial)
		builder.limits.MaxEntryBytes = uint64(after - 1)
		require.Truef(t, builder.wouldExceedSlotN(sizes), "trial %d: one byte over", trial)
	}
}

func TestScratchSlotCtxLeavesBaseUnchanged(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	scratch := sealevel.NewScratchSlotCtx(env.SlotCtx)
	assert.True(t, scratch.DeferVoteCachePublication)
	assert.True(t, scratch.TrackProgramCacheAdds)
	assert.Same(t, env.SlotCtx.Features, scratch.Features)
	assert.Same(t, env.SlotCtx.BankSysvars(), scratch.BankSysvars())

	payer, err := scratch.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err, "reads fall through to the bank")
	before := payer.Lamports
	payer.Lamports = 1
	require.NoError(t, scratch.SetAccount(txfixture.PayerPubkey(), payer))
	got, err := scratch.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	assert.Equal(t, uint64(1), got.Lamports)
	assert.Equal(t, before, lamportsOf(t, env, txfixture.PayerPubkey()), "writes stay in the scratch view")
}

// installNonce puts an initialized nonce account, with the payer as its
// authority, in the bank and returns its durable nonce.
func installNonce(t *testing.T, env *TestEnv, key solana.PublicKey) solana.Hash {
	t.Helper()
	durableNonce := solana.Hash{0xAA}
	nonceState := sealevel.NonceStateVersions{
		Type: sealevel.NonceVersionCurrent,
		Current: sealevel.NonceData{
			IsInitialized: true,
			Authority:     txfixture.PayerPubkey(),
			DurableNonce:  durableNonce,
			FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
		},
	}
	data, err := nonceState.Marshal()
	require.NoError(t, err)
	require.NoError(t, env.SlotCtx.SetAccount(key, &accounts.Account{
		Key: key, Lamports: 1_500_000, Owner: addresses.SystemProgramAddr, Data: data, RentEpoch: math.MaxUint64,
	}))
	return durableNonce
}

func nonceTx(t *testing.T, nonce solana.PublicKey, durableNonce solana.Hash, instructions ...solana.Instruction) *solana.Transaction {
	t.Helper()
	all := append([]solana.Instruction{
		system.NewAdvanceNonceAccountInstruction(nonce, solana.SysVarRecentBlockHashesPubkey, txfixture.PayerPubkey()).Build(),
	}, instructions...)
	tx, err := solana.NewTransaction(all, durableNonce, solana.TransactionPayer(txfixture.PayerPubkey()))
	require.NoError(t, err)
	payerKey := txfixture.PayerPrivateKey()
	_, err = tx.Sign(func(solana.PublicKey) *solana.PrivateKey { return &payerKey })
	require.NoError(t, err)
	return tx
}

func TestForgeBundleFailureNeitherChargesFeesNorAdvancesNonce(t *testing.T) {
	nonceKey := solana.PublicKey{0xD9}
	// A one-byte loaded-data limit makes this a fees-only failure: outside a
	// bundle it lands, charging the fee and advancing the nonce.
	failing := func(t *testing.T, durableNonce solana.Hash) *solana.Transaction {
		return nonceTx(t, nonceKey, durableNonce,
			computebudget.NewSetLoadedAccountsDataSizeLimitInstruction(1).Build(),
			transferIx(1, txfixture.PayerPubkey(), txfixture.DestPubkey()))
	}

	control := NewTestEnv(TestEnvConfig{})
	defer control.Close()
	controlNonce := installNonce(t, control, nonceKey)
	nonceBefore, err := control.SlotCtx.GetAccount(nonceKey)
	require.NoError(t, err)
	payerBefore := lamportsOf(t, control, txfixture.PayerPubkey())
	result, _ := control.Bank.ForgeTransaction(failing(t, controlNonce), 0)
	require.Equal(t, ForgeAccepted, result, "the control lands as a fees-only failure")
	nonceAfter, err := control.SlotCtx.GetAccount(nonceKey)
	require.NoError(t, err)
	require.NotEqual(t, nonceBefore.Data, nonceAfter.Data, "outside a bundle the nonce advances")
	require.Equal(t, payerBefore-5_000, lamportsOf(t, control, txfixture.PayerPubkey()), "outside a bundle the fee is charged")

	for name, tc := range map[string]struct {
		bundle    func(t *testing.T, durableNonce solana.Hash) []*solana.Transaction
		failingAt int
	}{
		"alone": {func(t *testing.T, durableNonce solana.Hash) []*solana.Transaction {
			return []*solana.Transaction{failing(t, durableNonce)}
		}, 0},
		"after a successful transaction": {func(t *testing.T, durableNonce solana.Hash) []*solana.Transaction {
			return []*solana.Transaction{mustSignedTransfer(t, 3), failing(t, durableNonce)}
		}, 1},
		// The durable-nonce transaction succeeds and advances the nonce in the
		// scratch view; the bundle then fails, so that advance is discarded.
		"successful, before a failing transaction": {func(t *testing.T, durableNonce solana.Hash) []*solana.Transaction {
			return []*solana.Transaction{
				nonceTx(t, nonceKey, durableNonce, transferIx(4, txfixture.PayerPubkey(), txfixture.DestPubkey())),
				mustSignBankTestTransaction(t, transferIx(20_000_000_000, txfixture.PayerPubkey(), txfixture.DestPubkey())),
			}
		}, 1},
	} {
		t.Run(name, func(t *testing.T) {
			env := NewTestEnv(TestEnvConfig{})
			defer env.Close()
			durableNonce := installNonce(t, env, nonceKey)
			nonceBefore, err := env.SlotCtx.GetAccount(nonceKey)
			require.NoError(t, err)
			payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())
			destBefore := lamportsOf(t, env, txfixture.DestPubkey())

			got := env.Bank.ForgeBundle(BundleInput{Transactions: tc.bundle(t, durableNonce)})
			require.Equal(t, ForgeDroppedExecution, got.Result, "%+v", got)
			require.Equal(t, tc.failingAt, got.Index, "%v", got.Err)
			nonceAfter, err := env.SlotCtx.GetAccount(nonceKey)
			require.NoError(t, err)
			assert.Equal(t, nonceBefore.Data, nonceAfter.Data, "the nonce is not advanced")
			assert.Equal(t, nonceBefore.Lamports, nonceAfter.Lamports)
			requireNothingLanded(t, env, payerBefore, destBefore)
		})
	}
}

func TestWorkingBankReadAccountSeesSlotWritesThenParentState(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	parentOnly := solana.PublicKey{0x51}
	env.SlotCtx.AccountsDb = &accountsdb.AccountsDb{}
	env.SlotCtx.UnrootedRead = parentStateAccounts{parentOnly: {Key: parentOnly, Lamports: 7}}

	acct, ok := env.Bank.ReadAccount(txfixture.PayerPubkey())
	require.True(t, ok, "written in the slot")
	assert.Equal(t, uint64(10_000_000_000), acct.Lamports)
	acct, ok = env.Bank.ReadAccount(parentOnly)
	require.True(t, ok, "falls back to parent state")
	assert.Equal(t, uint64(7), acct.Lamports)
	_, ok = env.Bank.ReadAccount(solana.PublicKey{0x52})
	assert.False(t, ok, "missing everywhere")
	require.NoError(t, env.SlotCtx.SetAccount(parentOnly, &accounts.Account{Key: parentOnly}))
	_, ok = env.Bank.ReadAccount(parentOnly)
	assert.False(t, ok, "an account the slot emptied does not exist")
	assert.Equal(t, env.SlotCtx.Epoch, env.Bank.Epoch())
	assert.Equal(t, txfixture.TestBlockhash(), env.Bank.LastBlockhash())
}

type parentStateAccounts map[solana.PublicKey]*accounts.Account

func (p parentStateAccounts) GetAccount(_ uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if acct, ok := p[pubkey]; ok {
		return acct.Clone(), nil
	}
	return nil, errors.New("not found")
}
