package replay

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// Footer shutoff: a footer bank hash mismatch at a slot that ran early
// executions turns work-ahead off for the rest of the process and is returned
// as SpeculationFooterMismatch, which the node's recovery loop re-replays from
// the rooted checkpoint without speculation. Footer verification precedes
// every store of the bank, so nothing it rejected was committed. If the plain
// replay mismatches too, the divergence was not speculation's and the node
// halts as before.
//
// workAheadTripped: a footer mismatch implicated speculation; never cleared.
var workAheadTripped atomic.Bool

// Slots whose blocks ran early executions, for the footer shutoff.
var workAheadRanSlots struct {
	mu    sync.Mutex
	slots map[uint64]struct{}
}

const workAheadRanSlotsKeep = 4096

func markWorkAheadRan(slot uint64) {
	workAheadRanSlots.mu.Lock()
	defer workAheadRanSlots.mu.Unlock()
	if workAheadRanSlots.slots == nil {
		workAheadRanSlots.slots = make(map[uint64]struct{})
	}
	if len(workAheadRanSlots.slots) >= workAheadRanSlotsKeep {
		for s := range workAheadRanSlots.slots {
			if s+workAheadRanSlotsKeep < slot {
				delete(workAheadRanSlots.slots, s)
			}
		}
	}
	workAheadRanSlots.slots[slot] = struct{}{}
}

func workAheadRanAt(slot uint64) bool {
	workAheadRanSlots.mu.Lock()
	defer workAheadRanSlots.mu.Unlock()
	_, ok := workAheadRanSlots.slots[slot]
	return ok
}

// SpeculationFooterMismatch is a footer bank hash mismatch at a slot whose
// block ran early executions. Work-ahead is already off; the slot must be
// re-replayed from the rooted checkpoint.
type SpeculationFooterMismatch struct {
	Slot uint64
	Err  error
}

func (e *SpeculationFooterMismatch) Error() string {
	return fmt.Sprintf("footer bank hash mismatch at slot %d with work-ahead early executions (work-ahead now off): %v", e.Slot, e.Err)
}

func (e *SpeculationFooterMismatch) Unwrap() error { return e.Err }

// workAheadFooterShutoff turns a footer bank hash mismatch at a slot that ran
// early executions into a SpeculationFooterMismatch, switching work-ahead off.
// Any other error is returned unchanged.
func workAheadFooterShutoff(err error) error {
	var mismatch *FooterBankhashMismatch
	if err == nil || !errors.As(err, &mismatch) || !workAheadRanAt(mismatch.Slot) {
		return err
	}
	if workAheadTripped.CompareAndSwap(false, true) {
		// the re-replay of this slot runs without early executions, so a
		// mismatch there is not attributed to speculation
		workAheadRanSlots.mu.Lock()
		workAheadRanSlots.slots = nil
		workAheadRanSlots.mu.Unlock()
		mlog.Log.Errorf("WORK-AHEAD SHUTOFF: footer bank hash mismatch at slot %d, a block with early executions; work-ahead is off for the rest of this process and the slot re-replays from the rooted checkpoint: %v", mismatch.Slot, err)
	}
	return &SpeculationFooterMismatch{Slot: mismatch.Slot, Err: err}
}

// MITHRIL_WORK_AHEAD_INJECT_FAULT_SLOT (bench only, exercises the footer
// shutoff): the first successful early result adopted at that slot has one
// written non-payer account's lamports raised by one, so the bank hash
// mismatches the footer. The account is a private copy made by the write.
var (
	workAheadInjectSlot = uint64(specShadowEnvInt("MITHRIL_WORK_AHEAD_INJECT_FAULT_SLOT", 0))
	workAheadInjected   atomic.Bool
)

func (w *workAhead) injectFault(out *LoadAndExecuteTransactionOutput) {
	if workAheadInjectSlot == 0 || w.slot != workAheadInjectSlot || workAheadInjected.Load() ||
		out.ProcessingResult.TransactionError != nil || out.ExecCtx == nil || out.ExecCtx.TransactionContext == nil {
		return
	}
	txAccts := &out.ExecCtx.TransactionContext.Accounts
	for i, acct := range txAccts.Accounts {
		if i == 0 || acct == nil || i >= len(txAccts.Touched) || !txAccts.Touched[i] {
			continue
		}
		if workAheadInjected.CompareAndSwap(false, true) {
			acct.Lamports++
			mlog.Log.Warnf("work-ahead: injected fault at slot %d: account %s lamports +1 in an adopted early result", w.slot, acct.Key)
		}
		return
	}
}

// MITHRIL_WORK_AHEAD_INJECT_DELAY_US (bench only, pressure tests): an early
// execution that relies on predicted failures holds its worker this much
// longer after executing, emulating long private attempts. It changes no
// input, output or result; only worker occupancy.
var workAheadInjectDelayUs = specShadowEnvInt("MITHRIL_WORK_AHEAD_INJECT_DELAY_US", 0)

func workAheadInjectDelay(assumed int) {
	if workAheadInjectDelayUs > 0 && assumed > 0 {
		time.Sleep(time.Duration(workAheadInjectDelayUs) * time.Microsecond)
	}
}
