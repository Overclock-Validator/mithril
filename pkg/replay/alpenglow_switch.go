package replay

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

// voteStakeDirtySlot is the highest slot whose transaction execution mutated the
// GLOBAL vote or stake caches (see recordStakeAndVoteAccounts). The in-loop
// unwind rolls back account state (the WorkingSet undo journal) but NOT those
// process-global caches, so it is only safe when the unwound suffix did not
// touch them. When it did, the switch falls back to the rooted-checkpoint
// re-replay, which reconstructs every cache from the resumed state. Reset per
// ReplayBlocks run; a stale value only ever causes an extra (safe) fallback.
var voteStakeDirtySlot atomic.Uint64

// markVoteStakeDirty records that slot's execution mutated a global vote/stake
// cache (monotonic max; safe under parallel transaction execution).
func markVoteStakeDirty(slot uint64) {
	for {
		cur := voteStakeDirtySlot.Load()
		if slot <= cur {
			return
		}
		if voteStakeDirtySlot.CompareAndSwap(cur, slot) {
			return
		}
	}
}

// resetVoteStakeDirty clears the watermark at the start of a replay run.
func resetVoteStakeDirty() { voteStakeDirtySlot.Store(0) }

// parentSwitchNeedsStateUnwind distinguishes an already-executed divergence
// from a fork discovered ahead of the executed bank, including a certificate
// selecting a previously skipped parent. The latter only needs a block-source
// branch replacement; replay's account state is still on the common ancestor.
func parentSwitchNeedsStateUnwind(switchSlot, executedAnchor uint64) bool {
	return switchSlot <= executedAnchor
}

// Execute-on-receipt consumes blocks and provisional skips before the chain's
// decisive outcome is known. Later certificates or newly discovered ancestry
// can select a different sibling, skip an executed block, or require a block
// previously consumed as a skip. The sweep walks the consumed-but-unrooted
// window when the chain-decision version, replay tip, or rooted frontier changes
// and reports the first contradiction.
//
// Replay rewinds the source and, if the divergence includes executed blocks,
// unwinds the in-RAM account-state suffix. If the retained state cannot support
// that unwind, node-level recovery re-replays from the rooted checkpoint.

// CertifiedSwitch reports a consumed suffix contradicted either by a
// decisive chain decision or by an exact parent-linked speculative branch. The
// historical name is retained because node recovery treats both as the same
// unwind/replay operation.
type CertifiedSwitch struct {
	Slot         uint64
	Executed     solana.Hash // zero when the local slot was treated as skipped
	Certified    solana.Hash // zero for a finalized skip or a parent-linked switch
	Skip         bool        // finalized skip contradicts a locally executed block
	ParentLinked bool        // speculative child links to an older emitted ancestor
	ParentSlot   uint64
	ParentID     solana.Hash
	ChildSlot    uint64
	ChildID      solana.Hash
}

func (e *CertifiedSwitch) Error() string {
	if e.ParentLinked {
		return fmt.Sprintf("alpenglow speculative switch: block %s at slot %d links to ancestor %s at slot %d; discarding executed suffix from slot %d",
			e.ChildID, e.ChildSlot, e.ParentID, e.ParentSlot, e.Slot)
	}
	if e.Skip {
		return fmt.Sprintf("alpenglow switch: slot %d executed locally but is certificate-skipped", e.Slot)
	}
	if e.Executed.IsZero() {
		return fmt.Sprintf("alpenglow switch: slot %d was treated as skipped locally but certificates name block %s", e.Slot, e.Certified)
	}
	return fmt.Sprintf("alpenglow switch: slot %d executed block %s but certificates name %s", e.Slot, e.Executed, e.Certified)
}

// alpenglowSwitchSweeper rate-gates the sweep on decision-version, replay-tip,
// and rooted-frontier changes. Both newly decisive ancestry and a queued skip
// consumed after its overriding certificate must trigger correction.
type alpenglowSwitchSweeper struct {
	query            consensusengine.AlpenglowChainQuery
	decisionChanges  <-chan struct{}
	lastDecisionSeen uint64
	lastReplayTip    uint64
	lastRooted       uint64
}

func newAlpenglowSwitchSweeper(engine consensusengine.Engine) *alpenglowSwitchSweeper {
	q, ok := engine.(consensusengine.AlpenglowChainQuery)
	if !ok {
		return nil
	}
	s := &alpenglowSwitchSweeper{query: q}
	if notifier, ok := engine.(consensusengine.AlpenglowChainDecisionNotifier); ok {
		// Obtain the notification channel before any sweep reads the version.
		// A change between that read and the source wait then remains pending.
		s.decisionChanges = notifier.ChainDecisionChanges()
	}
	return s
}

// peek tests for a switch without consuming the sweep's version/frontier gate.
// Streaming admission must leave a detected switch for the replay loop to apply.
func (s *alpenglowSwitchSweeper) peek(executed map[uint64]solana.Hash, lastRooted, tip uint64) *CertifiedSwitch {
	if s == nil {
		return nil
	}
	snapshot := *s
	return snapshot.sweep(executed, lastRooted, tip)
}

// sweep walks consumed block/skip outcomes in (lastRooted, tip] and returns
// the first contradiction with a decisive chain decision. tip includes trailing
// skips even when the executed bank remains at an earlier slot.
func (s *alpenglowSwitchSweeper) sweep(executed map[uint64]solana.Hash, lastRooted, tip uint64) *CertifiedSwitch {
	if s == nil || len(executed) == 0 || tip <= lastRooted {
		return nil
	}
	version := s.query.ChainDecisionVersion()
	if version == s.lastDecisionSeen && tip == s.lastReplayTip && lastRooted == s.lastRooted {
		return nil
	}
	s.lastDecisionSeen = version
	s.lastReplayTip = tip
	s.lastRooted = lastRooted

	for slot := lastRooted + 1; slot <= tip; slot++ {
		executedID, ran := executed[slot]
		if !ran {
			continue
		}
		if _, finalizedSkip := s.query.FinalizedSkipAt(slot); finalizedSkip {
			if !executedID.IsZero() {
				return &CertifiedSwitch{Slot: slot, Executed: executedID, Skip: true}
			}
			continue
		}
		if s.query.SkipCertifiedAt(slot) {
			// A skip certificate permits a future child to omit this slot, but it
			// does not invalidate an already replayed block as that child's parent.
			// Agave retains the bank in BankForks and lets exact ancestry/finality
			// select the branch. Mithril's parent-link switch performs that selection.
			continue
		}
		if certified, _, ok := s.query.CertifiedBlockAt(slot); ok {
			if solana.Hash(certified.Hash) != executedID {
				return &CertifiedSwitch{Slot: slot, Executed: executedID, Certified: solana.Hash(certified.Hash)}
			}
		}
	}
	return nil
}

const alpenglowSwitchPollInterval = 250 * time.Millisecond

// waitForAlpenglowReplayInput keeps certificate correction live while ancestry
// checks deliberately hold the next block. In particular, a finalized child
// can select a parent whose slot replay already consumed as a skip. Waiting
// for that child before checking certificates would deadlock both sides.
// Decision notifications wake the existing sweep immediately; polling remains
// a fallback for sources without notifications. The channel must be obtained
// before the first sweep and must not be drained after checking the version.
// A nil sweep preserves the ordinary blocking wait for other replay modes.
func waitForAlpenglowReplayInput(
	ctx context.Context,
	next func(context.Context, <-chan struct{}) (*b.Block, *blockstream.AlpenglowParentSwitch, bool),
	sweep func() *CertifiedSwitch,
	decisionChanges <-chan struct{},
	pollInterval time.Duration,
) (*b.Block, *blockstream.AlpenglowParentSwitch, *CertifiedSwitch) {
	adapted := func(ctx context.Context, decisionChanges <-chan struct{}, _ <-chan turbine.StreamEvent, _ <-chan time.Time) blockstream.ReplayInput {
		block, parentSwitch, decisionChanged := next(ctx, decisionChanges)
		return blockstream.ReplayInput{Block: block, ParentSwitch: parentSwitch, DecisionChanged: decisionChanged}
	}
	return waitForReplayInput(ctx, adapted, sweep, decisionChanges, pollInterval, nil)
}

// replayStreamer is the streaming executor as the wait loop drives it: the
// feed it listens to, its poll timer (nil while idle), and the two handlers,
// which execute transaction groups on the replay goroutine.
type replayStreamer interface {
	events() <-chan turbine.StreamEvent
	tick() <-chan time.Time
	handleEvent(turbine.StreamEvent)
	handleTick()
}

// replayInputSource is BlockSource.NextReplayInput.
type replayInputSource func(ctx context.Context, decisionChanges <-chan struct{}, streamEvents <-chan turbine.StreamEvent, streamTick <-chan time.Time) blockstream.ReplayInput

// waitForReplayInput is waitForAlpenglowReplayInput with the streaming
// executor folded into the same wait: feed wake-ups and poll ticks are
// handled here, on the replay goroutine, and never end the wait, so the loop
// body only ever sees a block, a switch, or an ended wait exactly as before.
// streamer may be nil (no streaming); sweep may be nil (no certificate
// correction), in which case decision notifications stay disabled and the
// wait blocks without polling, as it always did.
func waitForReplayInput(
	ctx context.Context,
	next replayInputSource,
	sweep func() *CertifiedSwitch,
	decisionChanges <-chan struct{},
	pollInterval time.Duration,
	streamer replayStreamer,
) (*b.Block, *blockstream.AlpenglowParentSwitch, *CertifiedSwitch) {
	var streamEvents <-chan turbine.StreamEvent
	if streamer != nil {
		// A slot may have become eligible while the previous block executed
		// (its header arrived mid-execution); open it before blocking.
		streamer.handleTick()
		streamEvents = streamer.events()
	}
	if sweep == nil {
		decisionChanges = nil
		if streamer == nil {
			in := next(ctx, nil, nil, nil)
			return in.Block, in.ParentSwitch, nil
		}
	}
	for {
		if ctx.Err() != nil {
			return nil, nil, nil
		}
		if sweep != nil {
			if sw := sweep(); sw != nil {
				return nil, nil, sw
			}
		}
		waitCtx, cancel := ctx, func() {}
		if sweep != nil {
			waitCtx, cancel = context.WithTimeout(ctx, pollInterval)
		}
		var tick <-chan time.Time
		if streamer != nil {
			tick = streamer.tick()
		}
		in := next(waitCtx, decisionChanges, streamEvents, tick)
		timedOut := sweep != nil && waitCtx.Err() == context.DeadlineExceeded
		cancel()
		switch {
		case in.Block != nil || in.ParentSwitch != nil:
			return in.Block, in.ParentSwitch, nil
		case in.StreamEvent != nil:
			streamer.handleEvent(*in.StreamEvent)
		case in.StreamTick:
			streamer.handleTick()
		case in.DecisionChanged || timedOut:
			// Re-sweep, then wait again.
		default:
			return nil, nil, nil
		}
	}
}

// tryInLoopUnwind attempts the in-RAM fork switch: evict the wrong suffix
// from the working set and rebuild replay's resume state from the retained
// parent context. Returns nil (caller falls back to the rooted-checkpoint
// re-replay) when the switch cannot be handled safely in-loop:
//   - the unwind span crosses an epoch boundary (epoch-scoped caches would
//     hold post-boundary state)
//   - the slot is inside the partitioned-rewards distribution window
//     (re-execution would double-apply distribution bookkeeping)
//   - the parent slot's context is no longer retained in RAM
//
// Fallback reasons reported by tryInLoopUnwind; surfaced in the fork-switch
// instrumentation (100-slot summary + logs) so operators can see WHY switches
// fell back to the rooted-checkpoint re-replay — the signal that decides
// whether the in-RAM engine suffices or a branch-aware state engine is needed.
const (
	unwindFallbackNilTail        = "nil-tail"
	unwindFallbackCrossEpoch     = "cross-epoch"
	unwindFallbackRewardsWindow  = "rewards-window"
	unwindFallbackVoteStakeDirty = "vote-stake-dirty"
	unwindFallbackMissingContext = "missing-context"
	unwindFallbackMissingSysvars = "missing-bank-sysvars"
	unwindFallbackSysvarSlot     = "bank-sysvar-slot-mismatch"
	unwindFallbackContextRebuild = "context-rebuild"
)

// tryInLoopUnwind attempts the in-RAM fork switch. On success it returns the
// rebuilt resume state, the exact immutable sysvar snapshot of its parent bank,
// and "". Otherwise it returns nil values and the guard reason that forced the
// rooted-checkpoint fallback.
func tryInLoopUnwind(
	sw *CertifiedSwitch,
	tail *unrootedTail,
	mithrilState *state.MithrilState,
	epochSchedule *sealevel.SysvarEpochSchedule,
	currentEpoch uint64,
	partitionedRewardsInfo *rewards.PartitionedRewardDistributionInfo,
) (*ResumeState, *sealevel.BankSysvars, string) {
	if tail == nil || sw.Slot == 0 {
		return nil, nil, unwindFallbackNilTail
	}
	if epochSchedule.GetEpoch(sw.Slot-1) != currentEpoch || epochSchedule.GetEpoch(sw.Slot) != currentEpoch {
		return nil, nil, unwindFallbackCrossEpoch
	}
	if partitionedRewardsInfo != nil {
		// Even after the last partition is distributed, the in-memory spool and
		// completed-distribution bookkeeping describe the abandoned suffix. They
		// cannot be reconstructed safely without re-running the epoch boundary.
		return nil, nil, unwindFallbackRewardsWindow
	}
	// Vote/stake cache safety, BOTH directions. The unwind cannot roll the
	// global vote/stake caches back (a write in the UNWOUND suffix >= sw.Slot
	// would leave them describing the wrong sibling), and the resume path's
	// cache reload reads durable AccountsDB, which cannot see writes in the
	// RETAINED suffix (rooted < slot < sw.Slot) — a reload would regress the
	// cache below live account state. Either way the caches and account state
	// disagree, and the vote cache feeds the timestamp oracle -> clock ->
	// bankhash. So: any vote/stake write anywhere ABOVE the rooted watermark
	// forces the rooted-checkpoint fallback, where reload-from-durable is
	// exact by construction. Vote-program writes are rare in Alpenglow blocks
	// (vote transactions are off-chain), so the fast path still dominates.
	if voteStakeDirtySlot.Load() > mithrilState.LastRootedSlot {
		return nil, nil, unwindFallbackVoteStakeDirty
	}

	ctx, bankSysvars := tail.unwind(sw.Slot)
	if ctx == nil && sw.Slot-1 == mithrilState.LastRootedSlot && mithrilState.LastRootedContext != nil {
		// The durable boundary carries a persisted ResumeContext, but deliberately
		// no in-memory BankSysvars pointer. Re-entering through the normal rooted
		// recovery path rebuilds the complete snapshot; using process globals here
		// could resurrect a sysvar generation from the discarded suffix.
		return nil, nil, unwindFallbackMissingSysvars
	}
	if ctx == nil {
		return nil, nil, unwindFallbackMissingContext
	}
	if bankSysvars == nil {
		return nil, nil, unwindFallbackMissingSysvars
	}
	if bankSysvars.Slot() != ctx.Slot {
		mlog.Log.Warnf("alpenglow switch: retained bank sysvars at slot %d do not match resume context slot %d", bankSysvars.Slot(), ctx.Slot)
		return nil, nil, unwindFallbackSysvarSlot
	}
	rs, err := ResumeStateFromRootedContext(ctx, nil)
	if err != nil {
		mlog.Log.Warnf("alpenglow switch: cannot rebuild resume state from retained context at slot %d: %v", ctx.Slot, err)
		return nil, nil, unwindFallbackContextRebuild
	}
	return rs, bankSysvars, ""
}
