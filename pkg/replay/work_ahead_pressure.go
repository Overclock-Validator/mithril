package replay

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// MITHRIL_WORK_AHEAD_PRESSURE=1 (diagnostic, log only; direct dispatch): per
// block, how much ready work waited for a worker while early executions held
// workers, and how long early executions kept the block open after its last
// commit. No scheduling decision reads these.
//
//   - pressured: ready transactions queued (not handed straight to the
//     committing worker) when more were queued than workers were idle and at
//     least one early execution was running; pressure-us is their total wait
//     until a worker took them, max-wait-us the longest. enqueued and
//     ready-wait-us are the same for every queued transaction, for reference.
//     These are observations of possible obstruction, not a counterfactual
//     cost: a pressured transaction might have waited without work-ahead too.
//   - peak-running: most early executions running at once.
//   - drain-us: from the last commit until every worker returned (early
//     executions still running hold the block open); busy-at-drain: early
//     executions running at the last commit.
//   - claim-wait-us: committing workers' time waiting on a valid early
//     execution still running (a second worker held by one transaction).
var workAheadPressureLog = os.Getenv("MITHRIL_WORK_AHEAD_PRESSURE") == "1"

type workAheadPressure struct {
	base time.Time
	// queuedAt[tx]: ns since base when tx was queued, plus one, negated when
	// queued under pressure; 0 when not queued
	queuedAt                           []atomic.Int64
	idle                               atomic.Int32
	peakRunning                        atomic.Int64
	pressured, pressureNs, maxWaitNs   atomic.Int64
	enqueued, readyWaitNs              atomic.Int64
	lastCommitNs, drainNs, busyAtDrain atomic.Int64
}

// begin sizes the per-transaction state for a plan of n transactions.
func (p *workAheadPressure) begin(n int) {
	if p == nil {
		return
	}
	p.base = time.Now()
	p.queuedAt = make([]atomic.Int64, n)
}

func (p *workAheadPressure) since() int64 { return int64(time.Since(p.base)) }

// started records an early execution starting, running being the count with it.
func (p *workAheadPressure) started(running int64) {
	if p == nil {
		return
	}
	for {
		peak := p.peakRunning.Load()
		if running <= peak || p.peakRunning.CompareAndSwap(peak, running) {
			return
		}
	}
}

func (p *workAheadPressure) idleBegin() {
	if p != nil {
		p.idle.Add(1)
	}
}

func (p *workAheadPressure) idleEnd() {
	if p != nil {
		p.idle.Add(-1)
	}
}

// queued records tx entering the ready queue, queueLen transactions now
// queued (tx included) and running early executions in flight.
func (p *workAheadPressure) queued(tx, queueLen int, running int64) {
	if p == nil {
		return
	}
	at := p.since() + 1
	if running > 0 && int32(queueLen) > p.idle.Load() {
		at = -at
	}
	p.queuedAt[tx].Store(at)
}

// taken records a worker taking tx from the ready queue.
func (p *workAheadPressure) taken(tx int) {
	if p == nil {
		return
	}
	at := p.queuedAt[tx].Swap(0)
	if at == 0 {
		return
	}
	pressured := at < 0
	if pressured {
		at = -at
	}
	wait := p.since() - (at - 1)
	p.enqueued.Add(1)
	p.readyWaitNs.Add(wait)
	if !pressured {
		return
	}
	p.pressured.Add(1)
	p.pressureNs.Add(wait)
	for {
		longest := p.maxWaitNs.Load()
		if wait <= longest || p.maxWaitNs.CompareAndSwap(longest, wait) {
			return
		}
	}
}

// lastCommit records the block's last commit, running early executions in flight.
func (p *workAheadPressure) lastCommit(running int64) {
	if p == nil {
		return
	}
	p.lastCommitNs.Store(p.since())
	p.busyAtDrain.Store(running)
}

// drained records every worker having returned.
func (p *workAheadPressure) drained() {
	if p == nil {
		return
	}
	if last := p.lastCommitNs.Load(); last > 0 {
		p.drainNs.Store(p.since() - last)
	}
}

func (p *workAheadPressure) log(slot uint64, uncovered, claimWaitNs int64) {
	if p == nil {
		return
	}
	mlog.Log.Infof("work-ahead pressure slot %d: pressured %d pressure-us %d max-wait-us %d enqueued %d ready-wait-us %d peak-running %d drain-us %d busy-at-drain %d claim-wait-us %d uncovered %d",
		slot, p.pressured.Load(), p.pressureNs.Load()/1000, p.maxWaitNs.Load()/1000, p.enqueued.Load(), p.readyWaitNs.Load()/1000,
		p.peakRunning.Load(), p.drainNs.Load()/1000, p.busyAtDrain.Load(), claimWaitNs/1000, uncovered)
}
