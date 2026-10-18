package blockprod

import (
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// BundleInput is a revert-protected bundle: its transactions land in order,
// in this bank, or none of them do.
type BundleInput struct {
	Transactions []*solana.Transaction
	// Reservation is entry-byte budget reserved by PrepareScheduleN, if any.
	// It is consumed whatever the result.
	Reservation EntryReservation
	// AdmitOnActual checks block limits after execution against actual usage,
	// as for Harmonic block bundles, instead of before it against requested
	// units.
	AdmitOnActual bool
}

// BundleResult reports a bundle forge. Unless Result is ForgeAccepted,
// nothing from the bundle landed: no state change, fee, nonce advance, cost or
// entry.
type BundleResult struct {
	Result ForgeResult
	Reason costmodel.ExceedReason
	// Index is the transaction that stopped the bundle, or -1.
	Index int
	// Err describes why that transaction failed, when it was executed.
	Err error
}

// ForgeBundle forges a bundle all or nothing. Transactions execute in order
// against a scratch view of the bank, each seeing the writes of the ones
// before it, and are only applied once all have succeeded. Any failure,
// including a fees-only failure, discards the whole bundle.
func (b *WorkingBank) ForgeBundle(in BundleInput) BundleResult {
	txs := in.Transactions
	if len(txs) == 0 {
		b.RebateSchedule(in.Reservation)
		return BundleResult{Result: ForgeDroppedParse, Index: -1}
	}
	candidates := make([]forgeCandidate, len(txs))
	seen := make(map[[32]byte]struct{}, len(txs))
	for idx, tx := range txs {
		c, result := b.prepareForge(tx, 0, nil)
		if result != ForgeAccepted {
			b.RebateSchedule(in.Reservation)
			return BundleResult{Result: result, Index: idx}
		}
		if _, dup := seen[c.messageHash]; dup {
			b.RebateSchedule(in.Reservation)
			return BundleResult{Result: ForgeDroppedAlreadyProcessed, Index: idx}
		}
		seen[c.messageHash] = struct{}{}
		candidates[idx] = c
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// The lock is held from here through append, so admission is exact.
	b.entries.release(in.Reservation)
	if !b.accepting {
		return BundleResult{Result: ForgeDroppedNoLeader, Index: -1}
	}
	requested := make([]costmodel.TransactionCost, len(candidates))
	sizes := make([]int, len(candidates))
	for idx, c := range candidates {
		if result := b.statusLocked(c); result != ForgeAccepted {
			return BundleResult{Result: result, Index: idx}
		}
		requested[idx] = c.cost
		sizes[idx] = c.serializedSize
	}
	if !in.AdmitOnActual {
		if reason := b.costs.WouldExceedAll(requested); reason != costmodel.ExceedNone {
			return BundleResult{Result: ForgeDroppedCost, Reason: reason, Index: -1}
		}
	}
	if b.entries.wouldOverflowBatch(sizes[0]) {
		b.flushEntriesLocked()
	}
	if b.entries.wouldExceedSlotN(sizes) {
		return BundleResult{Result: ForgeDroppedCost, Reason: costmodel.ExceedBatchBytes, Index: -1}
	}

	scratch := sealevel.NewScratchSlotCtx(b.slotCtx)
	committed := false
	defer func() {
		if !committed {
			b.evictProgramCacheAdds(scratch)
		}
	}()
	outputs := make([]replay.LoadAndExecuteTransactionOutput, len(txs))
	actual := make([]costmodel.TransactionCost, len(txs))
	for idx, c := range candidates {
		if err := b.preparer.PayerCanFund(scratch, c.execTx, nil); err != nil {
			return BundleResult{Result: ForgeDroppedExecution, Index: idx, Err: err}
		}
		// Every output must outlive the next execution, so none may use the
		// bank's per-transaction arena.
		output := b.preparer.LoadAndExecute(replay.LoadAndExecuteTransactionInput{
			SlotCtx:           scratch,
			Transaction:       c.execTx,
			LeanResult:        true,
			SkipTimingMetrics: true,
		}, nil)
		if txErr := output.ProcessingResult.TransactionError; txErr != nil {
			return BundleResult{Result: ForgeDroppedExecution, Index: idx, Err: transactionErr(txErr)}
		}
		if output.ExecCtx == nil {
			return BundleResult{Result: ForgeDroppedExecution, Index: idx, Err: errors.New("missing execution context")}
		}
		txAccounts := &output.ExecCtx.TransactionContext.Accounts
		if err := accounts.SetTransactionAccounts(scratch.Accounts, txAccounts.Accounts, txAccounts.Touched); err != nil {
			return BundleResult{Result: ForgeDroppedExecution, Index: idx, Err: err}
		}
		execCU, loadedCost := actualExecutionUsage(output)
		actual[idx] = candidates[idx].cost.WithActualUsage(execCU, loadedCost)
		outputs[idx] = output
	}
	if in.AdmitOnActual {
		if reason := b.costs.WouldExceedAll(actual); reason != costmodel.ExceedNone {
			return BundleResult{Result: ForgeDroppedCost, Reason: reason, Index: -1}
		}
	}
	if err := replay.ApplySuccessfulTransactions(b.slotCtx, outputs); err != nil {
		return BundleResult{Result: ForgeDroppedExecution, Index: -1, Err: err}
	}
	committed = true

	if b.seenMessages == nil {
		b.seenMessages = make(map[[32]byte]struct{})
	}
	for idx, tx := range txs {
		b.seenMessages[candidates[idx].messageHash] = struct{}{}
		if outputs[idx].FeeInfo != nil {
			b.txFees.Add(outputs[idx].FeeInfo)
		}
		b.forgedTxs = append(b.forgedTxs, tx)
		b.numSigs += uint64(tx.Message.Header.NumRequiredSignatures)
		// Equivalent to recording the requested cost and rebating it.
		b.costs.Record(actual[idx])
		if flushed, batchBytes, didFlush := b.entries.appendSerialized(*tx, sizes[idx], sizes[idx]); didFlush {
			b.entryHash = b.entries.CurrentEntryHash()
			b.sink.OnEntryBatch(flushed, batchBytes)
		}
	}
	return BundleResult{Result: ForgeAccepted, Index: -1}
}

// evictProgramCacheAdds drops programs a discarded scratch execution loaded
// into the shared program cache. Eviction only forces a reload from account
// data, so it is always safe.
func (b *WorkingBank) evictProgramCacheAdds(scratch *sealevel.SlotCtx) {
	for _, key := range scratch.TakeProgramCacheAdds() {
		if scratch.AccountsDb != nil {
			scratch.AccountsDb.RemoveProgramFromCache(key)
		}
	}
}

func transactionErr(txErr *replay.TransactionError) error {
	if txErr.InstructionError != nil {
		return fmt.Errorf("%s: %w", txErr.ErrorType, txErr.InstructionError)
	}
	return errors.New(txErr.ErrorType.String())
}
