package blockprod

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func TestPreparedBankPreservesOutcomes(t *testing.T) {
	for _, kind := range []string{"success", "instruction_failure", "fees_only", "payer_changed", "expired", "duplicate", "foreign_message"} {
		t.Run(kind, func(t *testing.T) {
			reference := NewTestEnv(TestEnvConfig{})
			defer reference.Close()
			candidate := NewTestEnv(TestEnvConfig{})
			defer candidate.Close()
			tx := mustSignedTransfer(t, 7)
			if kind == "instruction_failure" {
				tx.Message.Instructions[0].Data[0] = 0xff
			}
			if kind == "fees_only" {
				tx = mustSignBankTestTransaction(t,
					computebudget.NewSetLoadedAccountsDataSizeLimitInstruction(1).Build(),
					system.NewTransferInstruction(1, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build())
			}
			if kind == "expired" {
				tx.Message.RecentBlockhash = solana.Hash{99}
			}
			prepared := replay.NewTransactionPreparer(candidate.SlotCtx.Features.Clone()).Prepare(tx)
			require.NotNil(t, prepared)
			estimate, err := costmodel.EstimateTransactionCost(tx, candidate.SlotCtx.Features)
			require.NoError(t, err)
			require.Equal(t, estimate, prepared.Cost())
			if kind == "payer_changed" {
				// Preparation succeeded while the payer was funded. Admission must
				// still reject after another transaction spends that balance.
				setPayerLamports(t, reference, 1)
				setPayerLamports(t, candidate, 1)
			}
			if kind == "foreign_message" {
				tx = mustSignedTransfer(t, 19)
			}
			wire, err := tx.MarshalBinary()
			require.NoError(t, err)
			for i := 0; i < 2; i++ {
				a, ar := reference.Bank.ForgeTransaction(tx, len(wire))
				b, br := candidate.Bank.ForgePreparedTransaction(tx, len(wire), prepared)
				require.Equal(t, a, b)
				require.Equal(t, ar, br)
				require.Equal(t, reference.Bank.CostTracker().BlockCost(), candidate.Bank.CostTracker().BlockCost())
				require.Equal(t, reference.Bank.TxFeeAccumulator(), candidate.Bank.TxFeeAccumulator())
				for _, key := range []solana.PublicKey{txfixture.PayerPubkey(), txfixture.DestPubkey()} {
					ra, err := reference.SlotCtx.GetAccount(key)
					require.NoError(t, err)
					ca, err := candidate.SlotCtx.GetAccount(key)
					require.NoError(t, err)
					require.Equal(t, ra, ca)
				}
				if kind != "duplicate" {
					break
				}
			}
			after, err := tx.MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, wire, after)
		})
	}
}

func TestPreparedBankRejectsStaleFeatures(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	future := env.SlotCtx.Features.Clone()
	future.EnableFeature(features.EnableTxV1, 0)
	tx := mustSignedTransfer(t, 1)
	_, err := tx.Message.SetVersion(solana.MessageVersionV1)
	require.NoError(t, err)
	prepared := replay.NewTransactionPreparer(future).Prepare(tx)
	require.NotNil(t, prepared)
	require.False(t, env.Bank.preparer.Matches(prepared, tx, env.SlotCtx.Features))
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)
	result, _ := env.Bank.ForgePreparedTransaction(tx, len(wire), prepared)
	require.Equal(t, ForgeDroppedExecution, result)
	require.Empty(t, env.Bank.ForgedTransactions())
	require.Zero(t, env.Bank.TxFeeAccumulator().TotalFees)
}

func TestPreparedLeaderStillRejectsFeePayerNoOp(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	// Establish the complete feature snapshot before constructing the bank.
	env.SlotCtx.Features.EnableFeature(features.RelaxFeePayerConstraint, 0)
	bank := NewWorkingBank(BankConfig{SlotCtx: env.SlotCtx, Slot: env.SlotCtx.Slot,
		TransactionStatuses: replay.NewTransactionStatusCache().View()})
	tx := mustSignedTransfer(t, 1)
	prepared := bank.preparer.Prepare(tx)
	require.NotNil(t, prepared)
	setPayerLamports(t, env, 1)
	preview := bank.preparer.LoadAndExecute(replay.LoadAndExecuteTransactionInput{
		SlotCtx: env.SlotCtx, Transaction: tx, LeanResult: true}, prepared)
	require.True(t, preview.ProcessedAsNoOp)
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)
	result, _ := bank.ForgePreparedTransaction(tx, len(wire), prepared)
	require.Equal(t, ForgeDroppedExecution, result)
	require.Empty(t, bank.ForgedTransactions())
	require.Zero(t, bank.TxFeeAccumulator().TotalFees)
}

func TestPreparedExecutionRechecksStateWithoutMutatingPreparation(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	tx := mustSignedTransfer(t, 1)
	p := replay.NewTransactionPreparer(env.SlotCtx.Features)
	prepared := p.Prepare(tx)
	require.NotNil(t, prepared)
	input := replay.LoadAndExecuteTransactionInput{SlotCtx: env.SlotCtx, Transaction: tx, LeanResult: true}
	for _, balance := range []uint64{10_000_000, 1, 20_000_000} {
		setPayerLamports(t, env, balance)
		got := p.LoadAndExecute(input, prepared)
		want := replay.LoadAndExecuteTransaction(input)
		require.Equal(t, want.ProcessingResult, got.ProcessingResult)
		require.Equal(t, want.FeeInfo, got.FeeInfo)
		require.Equal(t, want.LoadedAccountsDataSize, got.LoadedAccountsDataSize)
		if want.ExecCtx != nil {
			require.Equal(t, want.ExecCtx.TransactionContext.Accounts.Accounts, got.ExecCtx.TransactionContext.Accounts.Accounts)
			require.Equal(t, want.ExecCtx.ComputeMeter.Used(), got.ExecCtx.ComputeMeter.Used())
		}
	}
	// Rent-boundary checks still use the bank's current payer state.
	rent := sealevel.NewDefaultRentSysvar()
	setPayerLamports(t, env, rent.MinimumBalance(0)+4999)
	require.Error(t, p.PayerCanFund(env.SlotCtx, tx, prepared))
}
