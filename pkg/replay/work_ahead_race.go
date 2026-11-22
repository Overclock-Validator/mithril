package replay

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

// MITHRIL_WORK_AHEAD_RACE=1: a committing worker that reaches a transaction
// whose valid early execution is still running executes the transaction
// itself, as a private run against committed state, instead of only waiting.
// The first to finish is used: the private run is the normal execution at the
// turn (every prerequisite has committed and no dependent is released), and
// the early one is adopted under the usual checks. The loser's result is
// dropped: the early execution ends on its own worker as an abandoned run,
// and the private run stops at its next cancellation poll.
//
// This bounds what a slow early execution can cost a transaction's commit to
// about MITHRIL_WORK_AHEAD_RACE_AFTER_US plus a normal execution. The
// committing worker would otherwise idle while it waits, so the private run
// occupies no extra worker.
var workAheadRace = os.Getenv("MITHRIL_WORK_AHEAD_RACE") == "1"

// MITHRIL_WORK_AHEAD_RACE_AFTER_US: the private run starts only if the early
// execution is still running this long after the turn. Most valid early
// executions end within it (median wait about 200 us on mainnet), so most
// private runs, which would lose and only take CPU, never start. 0 starts at
// once.
var workAheadRaceAfter = time.Duration(specShadowEnvInt("MITHRIL_WORK_AHEAD_RACE_AFTER_US", 400)) * time.Microsecond

// workAheadAwait reports whether done closes within d.
func workAheadAwait(done <-chan struct{}, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

type workAheadRaceStats struct {
	won, lost atomic.Int64
	// wonNs: from the turn to the private run's end, when it won; lagNs: from
	// the early execution's end to the private run's stop, when it lost
	wonNs, lagNs atomic.Int64
}

func (w *workAhead) racing() bool {
	return workAheadRace && !workAheadVerify && !txCaptureActive()
}

// race executes idx at its turn while its valid early execution e runs;
// cancel is set by ran when e ends. The private run uses an arena from the
// early-execution pool when one is free. It returns the private result and
// its release if that finished first, after abandoning e; nil if e ended first
// or the private run produced nothing usable, and the caller continues with e
// as usual.
func (w *workAhead) race(idx int, e *workAheadEntry, cancel *atomic.Bool) (*LoadAndExecuteTransactionOutput, func()) {
	turn := time.Now()
	var raceArena *arena.Arena[sealevel.BorrowedAccount]
	select {
	case raceArena = <-workAheadArenas:
	default:
	}
	release := func() {
		if raceArena != nil {
			workAheadArenas <- raceArena
			raceArena = nil
		}
	}
	input := transactionExecutionInput(w.slotCtx, w.txs[idx], w.txMeta(idx), raceArena, false, false)
	input.Speculative = true
	input.SpeculativeCancel = cancel
	out, panicked, _ := workAheadExecute(input)
	e.mu.Lock()
	defer e.mu.Unlock()
	// cancel was set under e.mu, so an unset cancel means e has not ended.
	if cancel.Load() || panicked || !workAheadUsable(&out) {
		w.stats.race.lost.Add(1)
		if !e.ended.IsZero() {
			w.stats.race.lagNs.Add(int64(time.Since(e.ended)))
		}
		release()
		return nil, nil
	}
	e.abandoned = true
	w.stats.race.won.Add(1)
	w.stats.race.wonNs.Add(int64(time.Since(turn)))
	return &out, release
}
