package harmonic

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

const (
	// thresholdFraction of the slot's recording window passes before a slot
	// with no block bundle falls back to local scheduling.
	thresholdFraction = 0.5
)

// SlotMode is a leader slot's admission mode.
type SlotMode int

const (
	ModeUndecided SlotMode = iota
	ModeHarmonic
	ModeFallback
	ModeDone
)

func (m SlotMode) String() string {
	switch m {
	case ModeUndecided:
		return "undecided"
	case ModeHarmonic:
		return "harmonic"
	case ModeFallback:
		return "fallback"
	default:
		return "done"
	}
}

// EndFlags record why a slot's block stopped, for reporting.
type EndFlags uint8

const (
	FlagTimeout EndFlags = 1 << iota
	FlagFailed
	FlagReverted
	// FlagUnavailable: Harmonic was not possible when the slot started.
	FlagUnavailable
)

func (f EndFlags) String() string {
	var names []string
	for flag, name := range map[EndFlags]string{FlagTimeout: "timeout", FlagFailed: "failed", FlagReverted: "reverted", FlagUnavailable: "unavailable"} {
		if f&flag != 0 {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ",")
}

// CrankResult is what a slot's tip crank did.
type CrankResult int

const (
	// CrankNotRun: no block bundle reached execution, or there is no crank.
	CrankNotRun CrankResult = iota
	CrankNotNeeded
	CrankLanded
	CrankFailed
)

func (c CrankResult) String() string {
	switch c {
	case CrankNotNeeded:
		return "not_needed"
	case CrankLanded:
		return "landed"
	case CrankFailed:
		return "failed"
	default:
		return "not_run"
	}
}

// SlotReport summarizes one leader slot.
type SlotReport struct {
	Slot uint64
	// Mode is the last mode before the slot ended: harmonic, fallback, or
	// undecided if no block bundle arrived and the threshold never passed.
	Mode  SlotMode
	Flags EndFlags
	// Executed counts bundles that landed; Reverted, the ones that executed
	// and landed nothing; Rejected, bundles refused without executing.
	Executed, Reverted, Rejected uint64
	Crank                        CrankResult
	// FirstArrival is how long after the bank opened the slot's first block
	// bundle arrived, admitted or not, if Arrived.
	FirstArrival time.Duration
	Arrived      bool
	// Aborted is set when the leader abandoned the slot, with its reason.
	Aborted bool
	Reason  string
}

// BlockEngineControl is the part of the block engine session the executor
// uses.
type BlockEngineControl interface {
	Healthy() bool
	NotifyLeaderSlot(slot uint64, end time.Time)
}

// SlotTracker is the block pipeline's side of the executor. It is told the
// current block slot, and admits only that slot's messages.
type SlotTracker interface {
	SetCurrentSlot(slot uint64)
	// InFlight reports whether a message is being split into bundles whose
	// events are not all sent yet.
	InFlight() bool
}

// CrankBuilder returns the transactions that must land before a slot's first
// block bundle, or none when the tip state needs no change.
type CrankBuilder func(bank *blockprod.WorkingBank) ([]*solana.Transaction, error)

// ExecutorConfig wires the executor.
type ExecutorConfig struct {
	BlockEngine BlockEngineControl
	Pipeline    SlotTracker
	Events      <-chan BlockEvent
	// Crank is optional; nil means no crank is ever needed.
	Crank CrankBuilder
	// OnReport, if set, receives each slot's report.
	OnReport func(SlotReport)
}

type hookMessage struct {
	started *slotState
	ended   uint64
	abort   bool
	reason  string
	// threshold reports that slot's threshold passed.
	threshold uint64
	// unhealthy reports the block engine session went down.
	unhealthy bool
}

type slotState struct {
	slot           uint64
	bank           *blockprod.WorkingBank
	start, end     time.Time
	threshold      time.Time
	mode           SlotMode
	stopped        bool
	nextSeq        uint64
	crankDone      bool
	report         SlotReport
	thresholdTimer *time.Timer
}

// Executor runs each leader slot's Harmonic block: it announces the slot,
// executes the block's bundles in stream order, and falls back to local
// scheduling when the block does not arrive or stops. It is the leader loop's
// LeaderHooks.
type Executor struct {
	cfg ExecutorConfig
	now func() time.Time

	hooksMu sync.Mutex
	hooks   []hookMessage
	wake    chan struct{}

	// running is the slot whose block is executing, or 0. The leader loop
	// holds that slot open while block bundles remain.
	running  atomic.Uint64
	released chan struct{}
}

var (
	_ blockprod.LeaderHooks = (*Executor)(nil)
	_ blockprod.SlotHolder  = (*Executor)(nil)
)

func NewExecutor(cfg ExecutorConfig) *Executor {
	return &Executor{cfg: cfg, now: time.Now, wake: make(chan struct{}, 1), released: make(chan struct{}, 1)}
}

// HoldSlot keeps a running block's slot open while any block bundle received
// for it has yet to execute: the slot ends once it is past its end and the
// buffer is empty. It runs on the leader loop's goroutine. A bundle already
// executing when it returns false still lands, since freezing the bank waits
// for it.
func (x *Executor) HoldSlot(slot uint64) bool {
	return x.running.Load() == slot && (len(x.cfg.Events) > 0 || x.cfg.Pipeline.InFlight())
}

// Released is signaled when the running block's slot may end.
func (x *Executor) Released() <-chan struct{} {
	return x.released
}

func (x *Executor) signalRelease() {
	select {
	case x.released <- struct{}{}:
	default:
	}
}

// endHold stops holding st's slot open.
func (x *Executor) endHold(st *slotState) {
	if x.running.CompareAndSwap(st.slot, 0) {
		x.signalRelease()
	}
}

// releaseIfDrained lets the leader loop end a running block's slot as soon as
// it is past its end and every block bundle received has executed.
func (x *Executor) releaseIfDrained(st *slotState) {
	if st != nil && x.running.Load() == st.slot && !x.now().Before(st.end) && !x.HoldSlot(st.slot) {
		x.signalRelease()
	}
}

// SlotStarted runs under the leader loop's lock, so it only does what cannot
// wait and queues the rest. The current slot is set before the notice is
// queued, because the block engine streams the slot only after the notice.
func (x *Executor) SlotStarted(info blockprod.LeaderSlotInfo) {
	st := &slotState{
		slot:    info.Slot,
		bank:    info.Bank,
		start:   info.Start,
		end:     info.End,
		nextSeq: 1,
		report:  SlotReport{Slot: info.Slot},
	}
	if info.End.IsZero() || !x.cfg.BlockEngine.Healthy() {
		// Without a deadline or a healthy session no block can arrive; the
		// TPU opens before the bank is published.
		st.mode = ModeFallback
		st.report.Flags |= FlagUnavailable
		info.Bank.AllowTPU()
	} else {
		st.threshold = info.Start.Add(time.Duration(float64(info.End.Sub(info.Start)) * thresholdFraction))
		x.cfg.Pipeline.SetCurrentSlot(info.Slot)
		x.cfg.BlockEngine.NotifyLeaderSlot(info.Slot, info.End)
	}
	x.post(hookMessage{started: st})
}

// SlotEnding runs under the leader loop's lock just before the bank freezes.
func (x *Executor) SlotEnding(slot uint64) {
	x.cfg.Pipeline.SetCurrentSlot(0)
	x.post(hookMessage{ended: slot})
}

// SlotAborted runs under the leader loop's lock when the slot is abandoned.
func (x *Executor) SlotAborted(slot uint64, reason string) {
	x.cfg.Pipeline.SetCurrentSlot(0)
	x.post(hookMessage{ended: slot, abort: true, reason: reason})
}

// BlockEngineHealthChanged reports the block engine session's health. A
// session that goes down before the slot's block starts takes the block with
// it, since it reconnects only after a backoff, so the slot falls back at once
// rather than at the threshold. A block already running ends with its stream.
func (x *Executor) BlockEngineHealthChanged(healthy bool) {
	if !healthy {
		x.post(hookMessage{unhealthy: true})
	}
}

func (x *Executor) post(msg hookMessage) {
	x.hooksMu.Lock()
	x.hooks = append(x.hooks, msg)
	x.hooksMu.Unlock()
	select {
	case x.wake <- struct{}{}:
	default:
	}
}

// Run processes slots and block events until ctx ends.
func (x *Executor) Run(ctx context.Context) {
	var st *slotState
	for {
		select {
		case <-ctx.Done():
			return
		case <-x.wake:
			st = x.drainHooks(st)
		case e := <-x.cfg.Events:
			// A slot's start is always queued before its first event, since
			// the block engine streams only after the notice.
			st = x.drainHooks(st)
			x.handleEvent(st, e)
			x.releaseIfDrained(st)
		}
	}
}

func (x *Executor) drainHooks(st *slotState) *slotState {
	x.hooksMu.Lock()
	hooks := x.hooks
	x.hooks = nil
	x.hooksMu.Unlock()
	for _, msg := range hooks {
		if msg.threshold != 0 {
			if st != nil && st.slot == msg.threshold && st.mode == ModeUndecided {
				x.fallback(st, FlagTimeout)
			}
			continue
		}
		if msg.unhealthy {
			if st != nil && st.mode == ModeUndecided {
				x.stop(st, FlagFailed)
			}
			continue
		}
		if msg.started != nil {
			if st != nil && st.mode != ModeDone {
				x.finish(st, "", false)
			}
			st = msg.started
			x.armThreshold(st)
			continue
		}
		if st != nil && st.slot == msg.ended && st.mode != ModeDone {
			if msg.abort {
				st.report.Flags |= FlagFailed
			}
			x.finish(st, msg.reason, msg.abort)
		}
	}
	return st
}

func (x *Executor) armThreshold(st *slotState) {
	if st.mode != ModeUndecided {
		return
	}
	slot := st.slot
	st.thresholdTimer = time.AfterFunc(time.Until(st.threshold), func() {
		x.post(hookMessage{threshold: slot})
	})
}

func (x *Executor) finish(st *slotState, reason string, aborted bool) {
	if st.thresholdTimer != nil {
		st.thresholdTimer.Stop()
	}
	x.endHold(st)
	st.report.Mode = st.mode
	st.report.Aborted = aborted
	st.report.Reason = reason
	st.mode = ModeDone
	r := st.report
	mlog.Log.Infof("Harmonic slot %d: mode=%s flags=%s executed=%d reverted=%d rejected=%d crank=%s aborted=%t%s",
		r.Slot, r.Mode, r.Flags, r.Executed, r.Reverted, r.Rejected, r.Crank, r.Aborted, reasonSuffix(r.Reason))
	if x.cfg.OnReport != nil {
		x.cfg.OnReport(r)
	}
}

func reasonSuffix(reason string) string {
	if reason == "" {
		return ""
	}
	return " reason=" + reason
}

func (x *Executor) handleEvent(st *slotState, e BlockEvent) {
	switch e := e.(type) {
	case StreamLostEvent:
		if st != nil && st.slot == e.Slot {
			x.stop(st, FlagFailed)
		}
	case InvalidBundleEvent:
		x.noteArrival(st, e.Slot, e.Arrival)
		if !x.admits(st, e.Slot, e.Seq) {
			return
		}
		x.stop(st, FlagFailed)
	case BundleEvent:
		x.noteArrival(st, e.Slot, e.Arrival)
		if !x.admits(st, e.Slot, e.Seq) {
			return
		}
		x.executeBundle(st, e)
	}
}

// noteArrival records when the slot's first block bundle arrived.
func (x *Executor) noteArrival(st *slotState, slot uint64, arrival time.Time) {
	if st == nil || st.slot != slot || st.report.Arrived || arrival.IsZero() {
		return
	}
	st.report.Arrived = true
	st.report.FirstArrival = max(arrival.Sub(st.start), 0)
}

// admits checks a bundle event against the slot state and consumes its
// sequence number. A gap in the sequence means block data was lost.
func (x *Executor) admits(st *slotState, slot, seq uint64) bool {
	if st == nil || st.slot != slot {
		return false
	}
	if (st.mode != ModeUndecided && st.mode != ModeHarmonic) || st.stopped {
		st.report.Rejected++
		return false
	}
	if seq != st.nextSeq {
		st.report.Rejected++
		x.stop(st, FlagFailed)
		return false
	}
	st.nextSeq++
	return true
}

func (x *Executor) executeBundle(st *slotState, e BundleEvent) {
	if st.mode == ModeUndecided && !x.now().Before(st.threshold) {
		st.report.Rejected++
		x.fallback(st, FlagTimeout)
		return
	}
	if st.mode == ModeUndecided {
		if !x.runCrank(st) {
			st.report.Rejected++
			return
		}
		st.mode = ModeHarmonic
		x.running.Store(st.slot)
	}

	var result blockprod.ForgeResult
	if e.RevertProtected {
		result = st.bank.ForgeBundle(blockprod.BundleInput{Transactions: e.Transactions, AdmitOnActual: true}).Result
	} else {
		result, _ = st.bank.ForgeStandaloneTransaction(e.Transactions[0], 0)
	}
	switch result {
	case blockprod.ForgeAccepted:
		st.report.Executed++
	case blockprod.ForgeDroppedNoLeader:
		// The bank froze: the slot is over.
		st.report.Rejected++
		st.stopped = true
		x.endHold(st)
	case blockprod.ForgeDroppedCost:
		// Execution is serial, so nothing in flight can free room.
		st.report.Rejected++
		x.stop(st, FlagFailed)
	default:
		st.report.Reverted++
		x.stop(st, FlagReverted)
	}
}

// runCrank lands the slot's tip crank before its first block bundle. A crank
// that fails aborts the block.
func (x *Executor) runCrank(st *slotState) bool {
	if st.crankDone || x.cfg.Crank == nil {
		st.crankDone = true
		return true
	}
	txs, err := x.cfg.Crank(st.bank)
	if err == nil && len(txs) > 0 {
		if r := st.bank.ForgeBundle(blockprod.BundleInput{Transactions: txs}); r.Result != blockprod.ForgeAccepted {
			err = fmt.Errorf("crank bundle: %s", r.Result)
		}
	}
	if err != nil {
		mlog.Log.Infof("Harmonic slot %d: tip crank failed: %v", st.slot, err)
		st.report.Crank = CrankFailed
		x.stop(st, FlagFailed)
		return false
	}
	st.report.Crank = CrankNotNeeded
	if len(txs) > 0 {
		st.report.Crank = CrankLanded
	}
	st.crankDone = true
	return true
}

// stop ends block admission for the slot. Before the slot's end it falls back
// to local scheduling; after it, the block is simply over.
func (x *Executor) stop(st *slotState, flag EndFlags) {
	st.stopped = true
	st.report.Flags |= flag
	x.endHold(st)
	if st.mode == ModeUndecided || (st.mode == ModeHarmonic && x.now().Before(st.end)) {
		x.fallback(st, 0)
	}
}

func (x *Executor) fallback(st *slotState, flag EndFlags) {
	st.mode = ModeFallback
	st.stopped = true
	st.report.Flags |= flag
	x.endHold(st)
	st.bank.AllowTPU()
}
