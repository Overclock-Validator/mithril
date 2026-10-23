package blockprod

import (
	"errors"
	"fmt"

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

// ForgeBundle forges a bundle all or nothing, under the bank lock throughout.
// Transactions execute in order against a scratch view of the bank, each
// seeing the writes of the ones before it, and are only applied once all have
// succeeded. Any failure, including a fees-only failure, discards the whole
// bundle.
func (b *WorkingBank) ForgeBundle(in BundleInput) BundleResult {
	p, prepared := b.PrepareBundle(in.Transactions, true)
	if prepared.Result != ForgeAccepted {
		b.RebateSchedule(in.Reservation)
		return prepared
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// The lock is held from here through append, so admission is exact.
	b.entries.release(in.Reservation)
	if !b.accepting {
		return BundleResult{Result: ForgeDroppedNoLeader, Index: -1}
	}
	requested := make([]costmodel.TransactionCost, len(p.candidates))
	sizes := make([]int, len(p.candidates))
	for idx, c := range p.candidates {
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
			evictProgramCacheAdds(scratch)
		}
	}()
	outputs, actual, result := b.executeBundleOn(scratch, p.candidates)
	if result.Result != ForgeAccepted {
		return result
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
	b.recordForgedLocked(in.Transactions, p.candidates, outputs, actual)
	return BundleResult{Result: ForgeAccepted, Index: -1}
}

// evictProgramCacheAdds drops programs a discarded scratch execution loaded
// into the shared program cache. Eviction only forces a reload from account
// data, so it is always safe.
func evictProgramCacheAdds(scratch *sealevel.SlotCtx) {
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
