package replay

import (
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/stretchr/testify/require"
)

func TestWorkerTransactionTimingsPreservePureExecutionCounts(t *testing.T) {
	env := newGroupExecutionEnv(t, 2, 10_000_000)
	defer env.cleanup()
	txs := transferTransactions(t, 2, 0)
	require.NoError(t, env.exec.loadTransactionAccounts(&b.Block{Slot: 42, Transactions: txs}))
	input := LoadAndExecuteTransactionInput{SlotCtx: env.exec.slotCtx, Transaction: txs[0], LeanResult: true}
	previous := metrics.GlobalBlockReplay
	defer func() { metrics.GlobalBlockReplay = previous }()
	metrics.GlobalBlockReplay = metrics.BlockReplay{}
	want := LoadAndExecuteTransaction(input)
	control := metrics.GlobalBlockReplay
	metrics.GlobalBlockReplay = metrics.BlockReplay{}
	var recorder metrics.TransactionTimings
	input.TimingRecorder = &recorder
	got := LoadAndExecuteTransaction(input)
	require.Nil(t, got.ExecCtx.TimingRecorder, "a retained result cannot retain its worker's recorder")
	require.Zero(t, metrics.GlobalBlockReplay.AccountsFromTx.Count)
	recorder.Merge()
	require.Equal(t, control.AccountsFromTx.Count, metrics.GlobalBlockReplay.AccountsFromTx.Count)
	require.Equal(t, control.InstructionsAndAccountMetasFromTx.Count, metrics.GlobalBlockReplay.InstructionsAndAccountMetasFromTx.Count)
	require.Equal(t, control.IxLoop.Count, metrics.GlobalBlockReplay.IxLoop.Count)
	require.Equal(t, control.GetNextIxCtx.Count, metrics.GlobalBlockReplay.GetNextIxCtx.Count)
	require.Equal(t, control.ExecIxNativeProgramSystem.Count, metrics.GlobalBlockReplay.ExecIxNativeProgramSystem.Count)
	require.Equal(t, want.ProcessingResult, got.ProcessingResult)
	require.Equal(t, want.ExecCtx.TransactionContext.Accounts.Accounts, got.ExecCtx.TransactionContext.Accounts.Accounts)
}

func TestWorkerTransactionTimingsGroupParity(t *testing.T) {
	previousEnabled, previousDispatch := replayLocalTimingsEnabled, directDispatch
	previousMetrics := metrics.GlobalBlockReplay
	defer func() {
		replayLocalTimingsEnabled, directDispatch = previousEnabled, previousDispatch
		metrics.GlobalBlockReplay = previousMetrics
	}()
	// Independent snapshots check publication, fees, CU, and account ownership;
	// phase counts check that merging neither drops nor duplicates records.
	counts := func() []uint64 {
		m := &metrics.GlobalBlockReplay
		return []uint64{m.AccountsFromTx.Count, m.InstructionsAndAccountMetasFromTx.Count,
			m.ComputeBudgetExecutionInstructions.Count, m.CalcAndDeductFees.Count,
			m.PreTxRentStates.Count, m.PostTxRentStates.Count, m.IxLoop.Count,
			m.ExecIxNativeProgramSystem.Count, m.TxUpdateAccounts.Count,
			m.TxFailedUpdateAccounts.Count, m.TxFailedPayerPublication.Count,
			m.TxPublishTouchedAccountState.Count, m.PreBalanceDivergenceCheck.Count,
			m.PostBalanceDivergenceCheck.Count}
	}
	for _, dispatch := range []bool{false, true} {
		directDispatch = dispatch
		for _, workers := range []int{1, 4} {
			for _, failure := range []bool{false, true} {
				txs := transferTransactions(t, 12, 0)
				if failure {
					txs[4].Message.Instructions[0].Data[0] = 255
				}
				replayLocalTimingsEnabled = false
				metrics.GlobalBlockReplay = metrics.BlockReplay{}
				want := runGroups(t, workers, 10_000_000, txs, []int{3, 8})
				wantCounts := counts()
				replayLocalTimingsEnabled = true
				metrics.GlobalBlockReplay = metrics.BlockReplay{}
				got := runGroups(t, workers, 10_000_000, txs, []int{3, 8})
				require.Equal(t, want, got)
				require.Equal(t, wantCounts, counts(), "dispatch=%v workers=%d failure=%v", dispatch, workers, failure)
			}
		}
	}
}
