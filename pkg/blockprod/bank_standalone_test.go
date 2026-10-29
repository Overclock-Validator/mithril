package blockprod

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requireExecutionError(t *testing.T, env *TestEnv, tx *solana.Transaction, want replay.TransactionErrorType) {
	t.Helper()
	preview := replay.LoadAndExecuteTransaction(replay.LoadAndExecuteTransactionInput{
		SlotCtx:     env.SlotCtx,
		Transaction: tx,
		LeanResult:  true,
	})
	require.NotNil(t, preview.ProcessingResult.TransactionError)
	require.Equal(t, want, preview.ProcessingResult.TransactionError.ErrorType)
}

func requireNothingForged(t *testing.T, env *TestEnv, payerBefore uint64) {
	t.Helper()
	assert.Equal(t, payerBefore, lamportsOf(t, env, txfixture.PayerPubkey()))
	assert.Empty(t, env.Bank.ForgedTransactions())
	assert.Empty(t, env.Bank.seenMessages)
	assert.Zero(t, env.Bank.TxFeeAccumulator().TotalFees)
	assert.Zero(t, env.Bank.CostTracker().BlockCost())
	assert.Zero(t, env.Bank.EntryBuilder().PendingCount())
}

func TestStandaloneDropsUnprocessableTransactions(t *testing.T) {
	t.Run("unknown blockhash", func(t *testing.T) {
		env := NewTestEnv(TestEnvConfig{})
		defer env.Close()
		tx, err := solana.NewTransaction(
			[]solana.Instruction{system.NewTransferInstruction(1, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build()},
			solana.Hash{0x42},
			solana.TransactionPayer(txfixture.PayerPubkey()),
		)
		require.NoError(t, err)
		payerKey := txfixture.PayerPrivateKey()
		_, err = tx.Sign(func(solana.PublicKey) *solana.PrivateKey { return &payerKey })
		require.NoError(t, err)
		requireExecutionError(t, env, tx, replay.TransactionErrorBlockhashNotFound)
		payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())

		result, _ := env.Bank.ForgeStandaloneTransaction(tx, 0)
		require.Equal(t, ForgeDroppedExecution, result)
		requireNothingForged(t, env, payerBefore)
	})
	t.Run("fee payer no-op", func(t *testing.T) {
		env := NewTestEnv(TestEnvConfig{})
		defer env.Close()
		env.SlotCtx.Features.EnableFeature(features.RelaxFeePayerConstraint, 0)
		bank := NewWorkingBank(BankConfig{SlotCtx: env.SlotCtx, Slot: env.SlotCtx.Slot,
			TransactionStatuses: replay.NewTransactionStatusCache().View()})
		env.Bank = bank
		setPayerLamports(t, env, 1)
		tx := mustSignedTransfer(t, 1)

		result, _ := bank.ForgeStandaloneTransaction(tx, 0)
		require.Equal(t, ForgeDroppedExecution, result)
		requireNothingForged(t, env, 1)
	})
}

// standaloneTransferUsage forges a transfer on an unlimited bank and returns
// the block cost it records, which is its actual usage.
func standaloneTransferUsage(t *testing.T) uint64 {
	t.Helper()
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	result, _ := env.Bank.ForgeStandaloneTransaction(mustSignedTransfer(t, 1), 0)
	require.Equal(t, ForgeAccepted, result)
	return env.Bank.CostTracker().BlockCost()
}

func TestStandaloneAdmitsOnActualUsage(t *testing.T) {
	actual := standaloneTransferUsage(t)
	requested, err := costmodel.EstimateTransactionCost(mustSignedTransfer(t, 1), features.NewFeaturesDefault())
	require.NoError(t, err)
	require.Greater(t, requested.Sum(), 10*actual, "the transfer requests far more than it uses")

	limits := costmodel.DefaultLimits()
	limits.BlockCost = actual

	tpu := NewTestEnv(TestEnvConfig{Limits: limits})
	defer tpu.Close()
	payerBefore := lamportsOf(t, tpu, txfixture.PayerPubkey())
	result, reason := tpu.Bank.ForgeTransaction(mustSignedTransfer(t, 1), 0)
	require.Equal(t, ForgeDroppedCost, result, "TPU forging admits on requested units")
	require.Equal(t, costmodel.ExceedBlockCost, reason)
	requireNothingForged(t, tpu, payerBefore)

	env := NewTestEnv(TestEnvConfig{Limits: limits})
	defer env.Close()
	result, reason = env.Bank.ForgeStandaloneTransaction(mustSignedTransfer(t, 1), 0)
	require.Equal(t, ForgeAccepted, result, "actual usage exactly at the limit fits")
	require.Equal(t, costmodel.ExceedNone, reason)
	assert.Equal(t, actual, env.Bank.CostTracker().BlockCost())
	assert.Len(t, env.Bank.ForgedTransactions(), 1)
}

func TestStandaloneRefusesWhenActualUsageDoesNotFit(t *testing.T) {
	actual := standaloneTransferUsage(t)
	limits := costmodel.DefaultLimits()
	limits.BlockCost = actual - 1
	env := NewTestEnv(TestEnvConfig{Limits: limits})
	defer env.Close()
	payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())
	destBefore := lamportsOf(t, env, txfixture.DestPubkey())
	tx := mustSignedTransfer(t, 1)

	result, reason := env.Bank.ForgeStandaloneTransaction(tx, 0)
	require.Equal(t, ForgeDroppedCost, result)
	require.Equal(t, costmodel.ExceedBlockCost, reason)
	requireNothingForged(t, env, payerBefore)
	assert.Equal(t, destBefore, lamportsOf(t, env, txfixture.DestPubkey()), "an executed but refused transaction publishes nothing")
	assert.Empty(t, env.SlotCtx.ModifiedAccts)
}
