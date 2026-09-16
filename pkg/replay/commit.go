package replay

import (
	"fmt"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

func applySuccessfulTransactionState(slotCtx *sealevel.SlotCtx, execCtx *sealevel.ExecutionCtx, executionResult *TransactionExecutionResult) error {
	if execCtx == nil {
		return fmt.Errorf("missing execution context")
	}
	if executionResult == nil && !accountsDeltaHashRemoved(slotCtx) {
		return fmt.Errorf("missing execution result while accounts delta hash is enabled")
	}

	// The touched-account counters stay exact for every replayed transaction;
	// the publication timers follow the transaction's sampling decision
	// (execCtx.SkipTimingMetrics) and are scaled like the other Tx-level timers.
	recordCounters := slotCtx != nil && slotCtx.Replay
	recordTimers := recordCounters && !execCtx.SkipTimingMetrics
	if executionResult != nil && !accountsDeltaHashRemoved(slotCtx) {
		writableStart := metrics.StartTiming(recordTimers)
		slotCtx.RecordWritableAccts(executionResult.WritableAccounts)
		metrics.GlobalBlockReplay.TxPublishRecordWritableAcct.AddSampledTimingSince(writableStart, execCtx.TimingSampleShift)
	}

	touchedStart := metrics.StartTiming(recordTimers)
	stats := handleModifiedAccounts(slotCtx, execCtx)
	metrics.GlobalBlockReplay.TxPublishTouchedAccountState.AddSampledTimingSince(touchedStart, execCtx.TimingSampleShift)
	if recordCounters {
		atomic.AddUint64(&metrics.GlobalBlockReplay.TxPublicationTouchedAccounts, stats.touchedAccounts)
		atomic.AddUint64(&metrics.GlobalBlockReplay.TxPublicationTouchedAccountBytes, stats.touchedAccountBytes)
	}

	stakeVoteStart := metrics.StartTiming(recordTimers)
	if executionResult == nil {
		recordStakeAndVoteAccountsFromMetas(slotCtx, execCtx)
	} else {
		recordStakeAndVoteAccounts(slotCtx, execCtx, executionResult.WritableAccountSet)
	}
	metrics.GlobalBlockReplay.TxPublishStakeVoteBookkeeping.AddSampledTimingSince(stakeVoteStart, execCtx.TimingSampleShift)
	return nil
}

// ApplySuccessfulTransaction commits account updates from a successful
// LoadAndExecuteTransaction result into slotCtx. Divergence checks are
// skipped; callers must only pass outputs with no TransactionError.
func ApplySuccessfulTransaction(slotCtx *sealevel.SlotCtx, output LoadAndExecuteTransactionOutput) error {
	if output.ProcessingResult.TransactionError != nil {
		return fmt.Errorf("cannot apply failed transaction: %s", output.ProcessingResult.TransactionError.ErrorType.String())
	}
	return applySuccessfulTransactionState(slotCtx, output.ExecCtx, output.ExecutionResult)
}

// ApplyFeesOnlyTransaction commits the fee-payer and durable-nonce rollback
// state for an account-load failure that Agave treats as processable. The
// transaction itself remains failed, but must be recorded in the block.
func ApplyFeesOnlyTransaction(slotCtx *sealevel.SlotCtx, tx *solana.Transaction, output LoadAndExecuteTransactionOutput) (*fees.TxFeeInfo, error) {
	txErr := output.ProcessingResult.TransactionError
	if txErr == nil {
		return nil, fmt.Errorf("fees-only transaction has no transaction error")
	}
	switch txErr.ErrorType {
	case TransactionErrorMaxLoadedAccountsDataSizeExceeded,
		TransactionErrorInvalidProgramForExecution,
		TransactionErrorProgramAccountNotFound:
	default:
		return nil, fmt.Errorf("transaction error %s is not a processable fees-only load failure", txErr.ErrorType.String())
	}
	if slotCtx == nil || tx == nil || output.ComputeBudgetLimits == nil || output.FeeInfo == nil {
		return nil, fmt.Errorf("fees-only transaction is missing required processing state")
	}

	// The load error is the transaction's recorded status, not a failure to
	// publish its fee/nonce effects, so do not return it as an apply error.
	return handleFailedTx(slotCtx, tx, output.Instrs, output.ComputeBudgetLimits, nil, nil)
}
