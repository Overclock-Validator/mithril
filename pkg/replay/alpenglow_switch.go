package replay

import (
	"fmt"
	"sync/atomic"

	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
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

// Execute-on-receipt runs blocks the moment they're assembled, which means a
// certificate can land AFTER the slot already executed and name a different
// outcome: a sibling block we lost the shred race on, or a skip over a block
// we ran. The switch sweep walks the executed-but-unfolded window whenever
// new certificates arrive and reports the FIRST contradiction between an
// executed identity and a decisive certificate.
//
// Until the WorkingSet unwind engine lands, the contradiction surfaces as a
// typed error handled by the node-level recovery loop (re-replay from the
// rooted checkpoint; repair re-fetches the certified version via the
// block-id hints). The in-loop unwind replaces that coarse path.

// CertifiedSwitch reports an executed slot contradicted by a decisive
// certificate.
type CertifiedSwitch struct {
	Slot      uint64
	Executed  solana.Hash // zero when Skip
	Certified solana.Hash // zero when Skip
	Skip      bool        // a skip cert contradicts an executed block
}

func (e *CertifiedSwitch) Error() string {
	if e.Skip {
		return fmt.Sprintf("alpenglow switch: slot %d executed locally but is certificate-skipped", e.Slot)
	}
	return fmt.Sprintf("alpenglow switch: slot %d executed block %s but certificates name %s", e.Slot, e.Executed, e.Certified)
}

// alpenglowSwitchSweeper rate-gates the sweep on decision-version changes (cert
// arrivals AND replay-derived decisiveness — parent links, finalized ancestry,
// indirect skips, conflicts), so a contradiction that arises without a new
// certificate is not skipped.
type alpenglowSwitchSweeper struct {
	query            consensusengine.AlpenglowChainQuery
	lastDecisionSeen uint64
}

func newAlpenglowSwitchSweeper(engine consensusengine.Engine) *alpenglowSwitchSweeper {
	q, ok := engine.(consensusengine.AlpenglowChainQuery)
	if !ok {
		return nil
	}
	return &alpenglowSwitchSweeper{query: q}
}

// sweep walks executed identities in (lastRooted, tip] and returns the first
// contradiction with a decisive certificate. Cheap: no-ops unless the
// tracker accepted new certificates since the last sweep.
func (s *alpenglowSwitchSweeper) sweep(executed map[uint64]solana.Hash, lastRooted, tip uint64) *CertifiedSwitch {
	if s == nil || len(executed) == 0 || tip <= lastRooted {
		return nil
	}
	version := s.query.ChainDecisionVersion()
	if version == s.lastDecisionSeen {
		return nil
	}
	s.lastDecisionSeen = version

	for slot := lastRooted + 1; slot <= tip; slot++ {
		executedID, ran := executed[slot]
		if !ran {
			continue
		}
		if s.query.SkipCertifiedAt(slot) {
			return &CertifiedSwitch{Slot: slot, Skip: true}
		}
		if certified, _, ok := s.query.CertifiedBlockAt(slot); ok {
			if solana.Hash(certified.Hash) != executedID {
				return &CertifiedSwitch{Slot: slot, Executed: executedID, Certified: solana.Hash(certified.Hash)}
			}
		}
	}
	return nil
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
	unwindFallbackContextRebuild = "context-rebuild"
)

// tryInLoopUnwind attempts the in-RAM fork switch. On success it returns the
// rebuilt resume state and "". Otherwise it returns nil and the guard reason
// that forced the rooted-checkpoint fallback.
func tryInLoopUnwind(
	sw *CertifiedSwitch,
	tail *unrootedTail,
	mithrilState *state.MithrilState,
	epochSchedule *sealevel.SysvarEpochSchedule,
	currentEpoch uint64,
	partitionedRewardsInfo *rewards.PartitionedRewardDistributionInfo,
) (*ResumeState, string) {
	if tail == nil || sw.Slot == 0 {
		return nil, unwindFallbackNilTail
	}
	if epochSchedule.GetEpoch(sw.Slot-1) != currentEpoch || epochSchedule.GetEpoch(sw.Slot) != currentEpoch {
		return nil, unwindFallbackCrossEpoch
	}
	if partitionedRewardsInfo != nil && partitionedRewardsInfo.NumRewardPartitionsRemaining > 0 {
		return nil, unwindFallbackRewardsWindow
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
		return nil, unwindFallbackVoteStakeDirty
	}

	ctx := tail.unwind(sw.Slot)
	if ctx == nil && sw.Slot-1 == mithrilState.LastRootedSlot && mithrilState.LastRootedContext != nil {
		// Parent is exactly the durable fold boundary: its context lives in
		// the state file rather than the tail.
		ctx = mithrilState.LastRootedContext
	}
	if ctx == nil {
		return nil, unwindFallbackMissingContext
	}
	rs, err := ResumeStateFromRootedContext(ctx, nil)
	if err != nil {
		mlog.Log.Warnf("alpenglow switch: cannot rebuild resume state from retained context at slot %d: %v", ctx.Slot, err)
		return nil, unwindFallbackContextRebuild
	}
	return rs, ""
}
