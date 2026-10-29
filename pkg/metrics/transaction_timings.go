package metrics

import (
	"sync/atomic"
	"time"
)

// TransactionPhase selects an existing JSON timing field; the report schema is unchanged.
type TransactionPhase uint8

const (
	TransactionInstructionsAndAccountMetasFromTx TransactionPhase = iota
	TransactionComputeBudgetExecutionInstructions
	TransactionAccountsFromTx
	TransactionCalcAndDeductFees
	TransactionReadRentSysvar
	TransactionPreTxRentStates
	TransactionFixupInstructionsSysvarAccount
	TransactionIxLoop
	TransactionPostTxRentStates
	TransactionTxFailedUpdateAccounts
	TransactionTxFailedPublicationPreparation
	TransactionTxFailedPayerPublication
	TransactionTxFailedNoncePublication
	TransactionPreBalanceDivergenceCheck
	TransactionPostBalanceDivergenceCheck
	TransactionTxUpdateAccounts
	TransactionTxPublishRecordWritableAcct
	TransactionTxPublishTouchedAccountState
	TransactionTxPublishStakeVoteBookkeeping
	TransactionGetNextIxCtx
	TransactionNextIxCtxConfigure
	TransactionIxPush
	TransactionIxPop
	TransactionExecIxResolveNativeProgram
	TransactionExecIxNativeProgramSystem
	TransactionExecIxNativeProgramStake
	TransactionExecIxNativeProgramVote
	TransactionExecIxNativeProgramComputeBudget
	TransactionExecIxNativeProgramBpfLoader2
	TransactionExecIxNativeProgramBpfLoaderDeprecated
	TransactionExecIxNativeProgramBpfLoaderUpgradeable
	TransactionExecIxNativeProgramZkElgamalProof
	TransactionExecIxNativeProgramEd25519Precompile
	TransactionExecIxNativeProgramSecp256kPrecompile
	transactionPhaseCount
)

// TransactionTimings belongs to one worker. Merge only after the worker and
// its transaction calls have returned. Padding separates adjacent workers.
type TransactionTimings struct {
	values [transactionPhaseCount]Timing
	_      [64]byte
}

var transactionTimingTargets = [transactionPhaseCount]*Timing{
	&GlobalBlockReplay.InstructionsAndAccountMetasFromTx,
	&GlobalBlockReplay.ComputeBudgetExecutionInstructions,
	&GlobalBlockReplay.AccountsFromTx,
	&GlobalBlockReplay.CalcAndDeductFees,
	&GlobalBlockReplay.ReadRentSysvar,
	&GlobalBlockReplay.PreTxRentStates,
	&GlobalBlockReplay.FixupInstructionsSysvarAccount,
	&GlobalBlockReplay.IxLoop,
	&GlobalBlockReplay.PostTxRentStates,
	&GlobalBlockReplay.TxFailedUpdateAccounts,
	&GlobalBlockReplay.TxFailedPublicationPreparation,
	&GlobalBlockReplay.TxFailedPayerPublication,
	&GlobalBlockReplay.TxFailedNoncePublication,
	&GlobalBlockReplay.PreBalanceDivergenceCheck,
	&GlobalBlockReplay.PostBalanceDivergenceCheck,
	&GlobalBlockReplay.TxUpdateAccounts,
	&GlobalBlockReplay.TxPublishRecordWritableAcct,
	&GlobalBlockReplay.TxPublishTouchedAccountState,
	&GlobalBlockReplay.TxPublishStakeVoteBookkeeping,
	&GlobalBlockReplay.GetNextIxCtx,
	&GlobalBlockReplay.NextIxCtxConfigure,
	&GlobalBlockReplay.IxPush,
	&GlobalBlockReplay.IxPop,
	&GlobalBlockReplay.ExecIxResolveNativeProgram,
	&GlobalBlockReplay.ExecIxNativeProgramSystem,
	&GlobalBlockReplay.ExecIxNativeProgramStake,
	&GlobalBlockReplay.ExecIxNativeProgramVote,
	&GlobalBlockReplay.ExecIxNativeProgramComputeBudget,
	&GlobalBlockReplay.ExecIxNativeProgramBpfLoader2,
	&GlobalBlockReplay.ExecIxNativeProgramBpfLoaderDeprecated,
	&GlobalBlockReplay.ExecIxNativeProgramBpfLoaderUpgradeable,
	&GlobalBlockReplay.ExecIxNativeProgramZkElgamalProof,
	&GlobalBlockReplay.ExecIxNativeProgramEd25519Precompile,
	&GlobalBlockReplay.ExecIxNativeProgramSecp256kPrecompile,
}

// A nil recorder preserves the shared atomic path for independent callers.
func (t *TransactionTimings) AddTimingSince(phase TransactionPhase, start time.Time) {
	if start.IsZero() {
		return
	}
	d := time.Since(start)
	if t == nil {
		transactionTimingTargets[phase].AddTiming(d)
		return
	}
	t.values[phase].Count++
	t.values[phase].SumNanoseconds += uint64(d.Nanoseconds())
}

// Merge keeps the existing global metrics and populations, batching the atomic
// updates once per nonempty phase per worker instead of once per transaction.
func (t *TransactionTimings) Merge() {
	if t == nil {
		return
	}
	for phase, value := range t.values {
		if value.Count == 0 {
			continue
		}
		target := transactionTimingTargets[phase]
		atomic.AddUint64(&target.Count, value.Count)
		atomic.AddUint64(&target.SumNanoseconds, value.SumNanoseconds)
		t.values[phase] = Timing{}
	}
}
