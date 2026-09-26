package blockstream

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

const (
	alpenglowRewindWaitWarnAfter = 5 * time.Second
	alpenglowRewindWaitLogEvery  = 30 * time.Second
)

// alpenglowRewindDiagnostic is owned by BlockSource.reorderMu. It observes a
// completed rewind without changing repair, scheduling, or replay decisions.
// Each emission captures generation before releasing reorderMu for its send;
// an old send cannot satisfy a replacement rewind, even for the same identity.
type alpenglowRewindDiagnostic struct {
	generationID uint64
	slot         uint64
	certified    solana.Hash
	started      time.Time
	lastLogged   time.Time
}

type alpenglowRewindWaitNotice struct {
	slot      uint64
	certified solana.Hash
	elapsed   time.Duration
}

func (d *alpenglowRewindDiagnostic) begin(slot uint64, certified solana.Hash, now time.Time) {
	d.generationID++
	d.slot = slot
	d.certified = certified
	d.started = now
	d.lastLogged = time.Time{}
}

func (d *alpenglowRewindDiagnostic) clear() {
	d.slot = 0
	d.certified = solana.Hash{}
	d.started = time.Time{}
	d.lastLogged = time.Time{}
}

func (d *alpenglowRewindDiagnostic) generation() uint64 {
	return d.generationID
}

// superseded retires a pinned identity after a different decisive outcome is
// accepted for its slot. A zero hash denotes a certified skip. An unpinned
// rewind still awaits the requested slot's delivery regardless of its identity.
func (d *alpenglowRewindDiagnostic) superseded(slot uint64, certified solana.Hash) {
	if d.slot == slot && d.certified != (solana.Hash{}) && d.certified != certified {
		d.clear()
	}
}

// served observes only a successful ordered stream send. An emission/replay
// frontier can already be above slot because of the discarded branch or a
// trailing skip consumed before rewind; neither is evidence of this outcome.
func (d *alpenglowRewindDiagnostic) served(generation uint64, blk *b.Block) {
	if d.slot == 0 || generation != d.generationID || blk == nil || blk.Slot != d.slot {
		return
	}
	if d.certified != (solana.Hash{}) && (blk.IsSkipped || !blk.HasAlpenglowBlockID || solana.Hash(blk.AlpenglowBlockID) != d.certified) {
		return
	}
	d.clear()
}

func (d *alpenglowRewindDiagnostic) takeNotice(now time.Time) (alpenglowRewindWaitNotice, bool) {
	if d.slot == 0 || now.Sub(d.started) < alpenglowRewindWaitWarnAfter {
		return alpenglowRewindWaitNotice{}, false
	}
	if !d.lastLogged.IsZero() && now.Sub(d.lastLogged) < alpenglowRewindWaitLogEvery {
		return alpenglowRewindWaitNotice{}, false
	}
	d.lastLogged = now
	return alpenglowRewindWaitNotice{slot: d.slot, certified: d.certified, elapsed: now.Sub(d.started)}, true
}

// Called by the existing scheduler tick. Read source state under reorderMu and
// recheck consensus only when a warning is due. Diagnostics never inspect
// receiver state or initiate another repair/RPC request.
func (bs *BlockSource) maybeLogAlpenglowRewindWait(now time.Time) {
	bs.reorderMu.Lock()
	if bs.stopped.Load() {
		bs.alpenglowRewindWait.clear()
		bs.reorderMu.Unlock()
		return
	}
	notice, due := bs.alpenglowRewindWait.takeNotice(now)
	if due && bs.alpenglowDecisionSource != nil {
		// A provisional skip may have advanced the source past the requested
		// slot before a later certificate supersedes its pinned identity. The
		// emitter then checks newer slots, so revalidate this old request only
		// when a warning is due. This callback is also used under reorderMu by
		// the emitter and does not fetch blocks or initiate repair.
		if decision, ok := bs.alpenglowDecisionSource(notice.slot - 1); ok && decision.Slot == notice.slot {
			switch decision.Kind {
			case alpenglow.ChainDecisionKindBlock:
				bs.alpenglowRewindWait.superseded(notice.slot, decision.Block.Hash)
			case alpenglow.ChainDecisionKindSkip:
				bs.alpenglowRewindWait.superseded(notice.slot, solana.Hash{})
			}
			due = bs.alpenglowRewindWait.slot != 0
		}
	}
	nextToSend, lastRealEmitted, buffered := bs.nextSlotToSend, bs.lastEmittedBlockSlot, len(bs.reorderBuffer)
	bs.reorderMu.Unlock()
	if !due {
		return
	}
	mlog.Log.Warnf("ALPENGLOW certified rewind awaiting source delivery: required_slot=%d certified=%s elapsed=%s next_to_send=%d last_real_emitted=%d replay_frontier=%d buffered_blocks=%d live_edge=%d stream_connected=%t repair_pending=%t repair_from=%d repair_until=%d",
		notice.slot, notice.certified, notice.elapsed.Round(time.Millisecond), nextToSend, lastRealEmitted,
		bs.lastExecutedSlot.Load(), buffered, bs.liveLastStreamSlot.Load(), bs.liveStreamConnected.Load(),
		bs.repairCatchupPending.Load(), bs.repairCatchupFrom.Load(), bs.repairCatchupUntil.Load())
}
