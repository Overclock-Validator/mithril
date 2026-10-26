package replay

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/rent"
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

	recordMetrics := slotCtx != nil && slotCtx.Replay
	if executionResult != nil && !accountsDeltaHashRemoved(slotCtx) {
		var writableStart time.Time
		if recordMetrics {
			writableStart = time.Now()
		}
		slotCtx.RecordWritableAccts(executionResult.WritableAccounts)
		if recordMetrics {
			metrics.GlobalBlockReplay.TxPublishRecordWritableAcct.AddTimingSince(writableStart)
		}
	}

	var touchedStart time.Time
	if recordMetrics {
		touchedStart = time.Now()
	}
	stats := handleModifiedAccounts(slotCtx, execCtx)
	if recordMetrics {
		metrics.GlobalBlockReplay.TxPublishTouchedAccountState.AddTimingSince(touchedStart)
		atomic.AddUint64(&metrics.GlobalBlockReplay.TxPublicationTouchedAccounts, stats.touchedAccounts)
		atomic.AddUint64(&metrics.GlobalBlockReplay.TxPublicationTouchedAccountBytes, stats.touchedAccountBytes)
	}

	var stakeVoteStart time.Time
	if recordMetrics {
		stakeVoteStart = time.Now()
	}
	if executionResult == nil {
		recordStakeAndVoteAccountsFromMetas(slotCtx, execCtx)
	} else {
		recordStakeAndVoteAccounts(slotCtx, execCtx, executionResult.WritableAccountSet)
	}
	if recordMetrics {
		metrics.GlobalBlockReplay.TxPublishStakeVoteBookkeeping.AddTimingSince(stakeVoteStart)
	}
	return nil
}

// ApplySuccessfulTransaction commits account updates from a successful
// LoadAndExecuteTransaction result into slotCtx. Divergence checks are
// skipped; callers must only pass outputs with no TransactionError.
func ApplySuccessfulTransaction(slotCtx *sealevel.SlotCtx, output LoadAndExecuteTransactionOutput) error {
	if err := checkSuccessfulTransaction(slotCtx, output); err != nil {
		return err
	}
	return applySuccessfulTransactionState(slotCtx, output.ExecCtx, output.ExecutionResult)
}

// ApplySuccessfulTransactions commits successful results in order. It checks
// every result before applying any, so either all of them land or none do.
func ApplySuccessfulTransactions(slotCtx *sealevel.SlotCtx, outputs []LoadAndExecuteTransactionOutput) error {
	for idx, output := range outputs {
		if err := checkSuccessfulTransaction(slotCtx, output); err != nil {
			return fmt.Errorf("transaction %d: %w", idx, err)
		}
	}
	for idx, output := range outputs {
		// The checks above cover every applySuccessfulTransactionState error.
		if err := applySuccessfulTransactionState(slotCtx, output.ExecCtx, output.ExecutionResult); err != nil {
			panic(fmt.Sprintf("transaction %d failed to apply after its checks passed: %v", idx, err))
		}
	}
	return nil
}

func checkSuccessfulTransaction(slotCtx *sealevel.SlotCtx, output LoadAndExecuteTransactionOutput) error {
	if output.ProcessingResult.TransactionError != nil {
		return fmt.Errorf("cannot apply failed transaction: %s", output.ProcessingResult.TransactionError.ErrorType.String())
	}
	if output.ExecCtx == nil {
		return fmt.Errorf("missing execution context")
	}
	if output.ExecutionResult == nil && !accountsDeltaHashRemoved(slotCtx) {
		return fmt.Errorf("missing execution result while accounts delta hash is enabled")
	}
	return nil
}

// includableFailure reports whether a failed transaction was processed, so a
// leader may record it in the block as failed with only its fee charged: it
// failed loading accounts after its fee payer was validated (fees-only), or
// during execution, including the post-execution rent-state check. The same
// error types raised before execution are not includable, nor are no-ops.
func includableFailure(output LoadAndExecuteTransactionOutput) bool {
	txErr := output.ProcessingResult.TransactionError
	if txErr == nil || output.ProcessedAsNoOp {
		return false
	}
	switch txErr.ErrorType {
	case TransactionErrorMaxLoadedAccountsDataSizeExceeded,
		TransactionErrorInvalidProgramForExecution,
		TransactionErrorProgramAccountNotFound:
		return true
	case TransactionErrorInstructionError:
		// Compute-budget instructions are parsed before execution, which is
		// the only stage that creates an execution context.
		return output.ExecCtx != nil
	case TransactionErrorInsufficientFundsForRent:
		// The fee-payer rent check before execution reports a plain error; only
		// the post-execution check reports a rent-state transition.
		var transition *rent.RentStateTransitionError
		return errors.As(txErr.InstructionError, &transition)
	}
	return false
}

// ApplyFailedTransaction commits the fee-payer and durable-nonce rollback
// state of a failed transaction that includableFailure accepts. The
// transaction itself remains failed, but must be recorded in the block. Any
// other failure commits nothing.
func ApplyFailedTransaction(slotCtx *sealevel.SlotCtx, tx *solana.Transaction, output LoadAndExecuteTransactionOutput) (*fees.TxFeeInfo, error) {
	txErr := output.ProcessingResult.TransactionError
	if txErr == nil {
		return nil, fmt.Errorf("failed transaction has no transaction error")
	}
	if !includableFailure(output) {
		return nil, fmt.Errorf("transaction error %s is not an includable failure", txErr.ErrorType.String())
	}
	if slotCtx == nil || tx == nil || output.ComputeBudgetLimits == nil || output.FeeInfo == nil {
		return nil, fmt.Errorf("failed transaction is missing required processing state")
	}

	// The failure is the transaction's recorded status, not a failure to
	// publish its fee/nonce effects, so do not return it as an apply error.
	return handleFailedTx(slotCtx, tx, output.Instrs, output.ComputeBudgetLimits, nil, nil)
}
