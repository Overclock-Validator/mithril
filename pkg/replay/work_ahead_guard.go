package replay

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/base58"
	b "github.com/Overclock-Validator/mithril/pkg/block"
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
	return fmt.Sprintf("footer bank hash check failed at slot %d with work-ahead early executions (work-ahead now off): %v", e.Slot, e.Err)
}

func (e *SpeculationFooterMismatch) Unwrap() error { return e.Err }

// workAheadUncheckedBlock: a block ran early executions but carries no footer
// bank hash, so nothing would check what they produced.
type workAheadUncheckedBlock struct {
	Slot      uint64
	Blockhash [32]byte // which block it was: nothing else identifies a block without a footer hash
	Computed  []byte   // the bank hash computed with early executions
}

func (e *workAheadUncheckedBlock) Error() string {
	return fmt.Sprintf("slot %d ran work-ahead early executions but its block has no footer bank hash to check them against", e.Slot)
}

// workAheadRequireFooterHash refuses a block that ran early executions unless
// it has a footer bank hash equal to the computed one. The hash arrives with
// the block's last shreds, after early executions may have run, so this cannot
// be decided before execution; it is decided here, next to footer verification
// and before any store of the bank. It compares the hashes itself rather than
// rely on that verification having run: whichever mode replays the block, an
// early execution is never accepted on a hash that was not compared. The
// refusal goes through the footer shutoff: work-ahead off, and the slot
// replayed without it.
func workAheadRequireFooterHash(block *b.Block, computed []byte) error {
	if block == nil || !workAheadRanAt(block.Slot) {
		return nil
	}
	if !block.HasExpectedBankhash {
		return &workAheadUncheckedBlock{Slot: block.Slot, Blockhash: block.Blockhash, Computed: append([]byte(nil), computed...)}
	}
	if !bytes.Equal(block.ExpectedBankhash[:], computed) {
		return &FooterBankhashMismatch{Slot: block.Slot, Footer: block.ExpectedBankhash, Computed: append([]byte(nil), computed...)}
	}
	return nil
}

// workAheadSuspect is the attempt that tripped the shutoff: the slot, the bank
// hash computed with early executions, and the footer hash it was compared
// with, if the block had one. The replay of that slot without early executions
// is judged against it (workAheadReplayVerdict).
var workAheadSuspect struct {
	mu        sync.Mutex
	set       bool
	slot      uint64
	computed  []byte
	footer    [32]byte
	hasFooter bool
	blockhash [32]byte // of a block without a footer hash
}

// WorkAheadVerdict says what the replay without early executions showed about
// a slot whose first attempt, with them, failed its footer check.
type WorkAheadVerdict string

const (
	// The plain replay computed a different bank hash, and that one matches
	// the footer: the early executions produced a wrong result.
	WorkAheadVerdictSpeculationFault WorkAheadVerdict = "speculation fault confirmed"
	// The plain replay computed the same bank hash, which still does not match
	// the footer: this node's execution of the block differs from the
	// leader's whether or not work-ahead runs.
	WorkAheadVerdictNotSpeculation WorkAheadVerdict = "not caused by speculation"
	// The plain replay computed a third bank hash, matching neither the first
	// attempt nor the footer, or a different block was replayed at the slot.
	WorkAheadVerdictInconclusive WorkAheadVerdict = "inconclusive"
	// No footer hash: both attempts computed the same bank hash.
	WorkAheadVerdictConsistentUnchecked WorkAheadVerdict = "attempts agree, no footer hash to check"
	// No footer hash: the two attempts computed different bank hashes.
	WorkAheadVerdictDifferUnchecked WorkAheadVerdict = "attempts differ, no footer hash to check"
)

// bankHashString prints a bank hash as the rest of the node does, and anything
// that is not 32 bytes (nothing computed) in hex.
func bankHashString(h []byte) string {
	if len(h) == 32 {
		return base58.Encode(h)
	}
	return fmt.Sprintf("0x%x", h)
}

// workAheadLastVerdict is the most recent verdict, for tests and diagnostics.
var workAheadLastVerdict atomic.Pointer[WorkAheadVerdict]

// workAheadReplayVerdict is called for every block once its bank hash is
// computed and its footer checked, with the footer check's error (nil when the
// hashes matched or the block has no footer hash). For the replay of the slot
// that tripped the shutoff it compares this attempt, made without early
// executions, with the one that was made with them, logs which of the two
// explanations holds, and returns footerErr with that explanation attached so
// the error a halting node prints carries it. For every other block it returns
// footerErr unchanged.
func workAheadReplayVerdict(block *b.Block, computed []byte, footerErr error) error {
	if block == nil || !workAheadTripped.Load() {
		return footerErr
	}
	workAheadSuspect.mu.Lock()
	if !workAheadSuspect.set || workAheadSuspect.slot != block.Slot {
		workAheadSuspect.mu.Unlock()
		return footerErr
	}
	first := workAheadSuspect.computed
	firstFooter, firstHasFooter := workAheadSuspect.footer, workAheadSuspect.hasFooter
	firstBlockhash := workAheadSuspect.blockhash
	workAheadSuspect.set = false
	workAheadSuspect.computed = nil
	workAheadSuspect.mu.Unlock()

	same := bytes.Equal(first, computed)
	// The same block: the same footer hash, or, for two blocks without one,
	// the same blockhash (the last entry hash, which fixes the entries).
	sameBlock := firstHasFooter == block.HasExpectedBankhash
	if sameBlock && firstHasFooter {
		sameBlock = firstFooter == block.ExpectedBankhash
	} else if sameBlock {
		sameBlock = firstBlockhash == block.Blockhash
	}
	footer := "none"
	if block.HasExpectedBankhash {
		footer = base58.Encode(block.ExpectedBankhash[:])
	}
	with, without := bankHashString(first), bankHashString(computed)
	var verdict WorkAheadVerdict
	var explain string
	switch {
	case !sameBlock:
		verdict = WorkAheadVerdictInconclusive
		explain = "the block replayed at this slot is not the one the first attempt executed (its footer bank hash, or without one its blockhash, differs), so the two attempts cannot be compared"
	case !firstHasFooter && same:
		verdict = WorkAheadVerdictConsistentUnchecked
		explain = "the block has no footer bank hash; both attempts computed the same bank hash, so nothing points at speculation, and nothing confirms the hash either"
	case !firstHasFooter:
		verdict = WorkAheadVerdictDifferUnchecked
		explain = "the block has no footer bank hash, and the two attempts computed different bank hashes: the early executions changed the result. This is a speculation bug; work-ahead stays off"
	case footerErr == nil && !same:
		verdict = WorkAheadVerdictSpeculationFault
		explain = "the replay without early executions matches the footer and the first attempt did not: an early execution produced a wrong result. This is a speculation bug; work-ahead stays off and the node continues on the correct state"
	case footerErr != nil && same:
		verdict = WorkAheadVerdictNotSpeculation
		explain = "the replay without early executions computed the same bank hash as the first attempt, and it still does not match the footer: this node's execution of the block differs from the leader's whether or not work-ahead runs. Speculation is not the cause"
	default:
		verdict = WorkAheadVerdictInconclusive
		explain = "the replay without early executions computed a bank hash that matches neither the first attempt nor the footer, so the result of this block is not reproducible on this node; speculation may or may not be involved"
	}
	workAheadLastVerdict.Store(&verdict)
	mlog.Log.Errorf("WORK-AHEAD VERDICT for slot %d: %s. Bank hash with early executions %s, without %s, footer %s. %s.", block.Slot, verdict, with, without, footer, explain)
	if footerErr == nil {
		return nil
	}
	return fmt.Errorf("%w [work-ahead verdict: %s; with early executions %s, without %s]", footerErr, verdict, with, without)
}

// workAheadFooterShutoff turns a footer bank hash mismatch at a slot that ran
// early executions, or such a slot's block having no footer bank hash at all,
// into a SpeculationFooterMismatch, switching work-ahead off. Any other error
// is returned unchanged.
func workAheadFooterShutoff(err error) error {
	var mismatch *FooterBankhashMismatch
	var unchecked *workAheadUncheckedBlock
	var slot uint64
	switch {
	case err == nil:
		return nil
	case errors.As(err, &mismatch):
		slot = mismatch.Slot
	case errors.As(err, &unchecked):
		slot = unchecked.Slot
	default:
		return err
	}
	if !workAheadRanAt(slot) {
		return err
	}
	if workAheadTripped.CompareAndSwap(false, true) {
		// the re-replay of this slot runs without early executions, so a
		// mismatch there is not attributed to speculation
		workAheadRanSlots.mu.Lock()
		workAheadRanSlots.slots = nil
		workAheadRanSlots.mu.Unlock()
		// remember this attempt, so that replay can say what it showed
		workAheadSuspect.mu.Lock()
		workAheadSuspect.set, workAheadSuspect.slot = true, slot
		if mismatch != nil {
			workAheadSuspect.computed = append([]byte(nil), mismatch.Computed...)
			workAheadSuspect.footer, workAheadSuspect.hasFooter = mismatch.Footer, true
		} else {
			workAheadSuspect.computed = append([]byte(nil), unchecked.Computed...)
			workAheadSuspect.footer, workAheadSuspect.hasFooter = [32]byte{}, false
			workAheadSuspect.blockhash = unchecked.Blockhash
		}
		workAheadSuspect.mu.Unlock()
		mlog.Log.Errorf("WORK-AHEAD SHUTOFF: footer bank hash check failed or missing at slot %d, a block with early executions; work-ahead is off for the rest of this process and the slot re-replays from the rooted checkpoint: %v", slot, err)
	}
	return &SpeculationFooterMismatch{Slot: slot, Err: err}
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
