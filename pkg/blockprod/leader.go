package blockprod

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

const (
	// AlpenglowSlotDuration is the community cluster's target slot duration.
	// It also bounds the footer nanosecond clock and ParentReady-derived block
	// deadlines; keeping those clocks identical prevents late/missed slots.
	AlpenglowSlotDuration = 200 * time.Millisecond
	// Leave enough time for the Go finalizer, footer shredding, and synchronous
	// UDP fanout to complete before Votor's per-slot deadline. A two-hour live
	// trace measured 29-55ms from the old 25ms cutoff through completion: 328 of
	// 340 blocks finished after their protocol deadline and later leaders
	// discarded 52 of 85 locally produced windows. Keep 20ms above that
	// observed worst case for scheduler and network jitter.
	leaderBlockCompletionReserve = 75 * time.Millisecond
)

var errParentNotReady = errors.New("parent not ready")
var errProductionStartCutoffElapsed = errors.New("production start cutoff elapsed")
var errEpochTransitionProductionUnsupported = errors.New("leader production across an epoch transition is not supported")
var errEpochRewardsProductionUnsupported = errors.New("leader production during partitioned epoch rewards is not supported")

const (
	leaderOutcomeBroadcast = "broadcast"
	leaderOutcomeMissed    = "missed"

	leaderReasonComplete                  = "complete"
	leaderReasonLiveClockAdvanced         = "live_clock_advanced"
	leaderReasonAlreadyResolved           = "already_resolved"
	leaderReasonReplayNotReady            = "replay_not_ready"
	leaderReasonParentReadyNotReady       = "parent_ready_not_ready"
	leaderReasonParentReadyMissedWindow   = "parent_ready_missed_window"
	leaderReasonParentReadyInvalid        = "parent_ready_invalid"
	leaderReasonParentBlockIDMismatch     = "parent_block_id_mismatch"
	leaderReasonParentBlockIDMissing      = "parent_block_id_missing"
	leaderReasonParentChainedRootMissing  = "parent_chained_root_missing"
	leaderReasonParentContextNotReady     = "parent_context_not_ready"
	leaderReasonParentChangedBeforeFreeze = "parent_changed_before_freeze"
	leaderReasonEpochTransition           = "epoch_transition"
	leaderReasonEpochRewards              = "epoch_rewards"
	leaderReasonSlotContextFailed         = "slot_context_failed"
	leaderReasonSysvarPrepareFailed       = "sysvar_prepare_failed"
	leaderReasonHeaderBroadcastFailed     = "header_broadcast_failed"
	leaderReasonEntryBroadcastFailed      = "entry_broadcast_failed"
	leaderReasonFreezeFailed              = "freeze_failed"
	leaderReasonFinalizationUnavailable   = "finalization_unavailable"
	leaderReasonFooterBroadcastFailed     = "footer_broadcast_failed"
	leaderReasonEndingTickBroadcastFailed = "ending_tick_broadcast_failed"
	leaderReasonWindowDeadlineElapsed     = "window_deadline_elapsed"
	leaderReasonPreviousSlotMissed        = "previous_slot_missed"

	// A terminal describes how an outcome became final. The cause preserves
	// the earlier gate failure that may have prevented production from starting.
	leaderTerminalDeadlineElapsed = "deadline_elapsed"
)

type leaderSlotFailure struct {
	reason string
	detail string
}

// LeaderLoop activates forge when this validator is the scheduled leader.
// RewardCertBuilder produces skip/notar reward certificates for block footers.
type RewardCertBuilder interface {
	BuildForLeaderSlot(slot uint64) rewardcerts.RewardCertificates
}

type LeaderLoop struct {
	controller       *Controller
	identity         solana.PrivateKey
	accountsDb       *accountsdb.AccountsDb
	broadcaster      turbine.PacketBroadcaster
	shredVersion     uint16
	userAgent        []byte
	rewardCerts      RewardCertBuilder
	epochSchedule    *sealevel.SysvarEpochSchedule
	alpenglowClock   bool
	parentContext    func(uint64) ParentContext
	productionParent func(uint64) alpenglow.BlockProductionParent
	onBlock          func(*b.Block)
	commitLeaderSlot func(replay.CommitLeaderInput) (*sealevel.SlotCtx, error)

	currentSlot   func() uint64
	leaderForSlot func(uint64) (solana.PublicKey, bool)

	pollInterval    time.Duration
	slotDuration    time.Duration
	now             func() time.Time
	tickStartedAt   time.Time
	tickDeliveryLag time.Duration

	mu                      sync.Mutex
	activeSlot              uint64
	activeBank              *WorkingBank
	activeSess              *turbine.BroadcastSession
	activeSink              *ShredSink
	parentCtx               ParentContext
	activeParentID          solana.Hash
	activeParentChainedRoot solana.Hash
	activeParentGeneration  uint64
	finishedLeaderSlots     map[uint64]struct{}
	pendingFailures         map[uint64]leaderSlotFailure
	missedScanNext          uint64
	missedScanStarted       bool
	productionWindow        leaderProductionWindow
}

type leaderProductionWindow struct {
	active    bool
	startSlot uint64
	endSlot   uint64
	nextSlot  uint64
	parent    alpenglow.BlockID
	readyAt   time.Time
}

type LeaderLoopConfig struct {
	Controller       *Controller
	Identity         solana.PrivateKey
	AccountsDb       *accountsdb.AccountsDb
	Broadcaster      turbine.PacketBroadcaster
	ShredVersion     uint16
	UserAgent        []byte
	EpochSchedule    *sealevel.SysvarEpochSchedule
	AlpenglowClock   bool
	ParentContext    func(uint64) ParentContext
	ProductionParent func(slot uint64) alpenglow.BlockProductionParent
	OnBlock          func(*b.Block)
	CurrentSlot      func() uint64
	LeaderForSlot    func(uint64) (solana.PublicKey, bool)
	// ParentBlockID is retained for source compatibility and ignored. Parent
	// identity is supplied atomically by ParentContext.
	ParentBlockID func(parentSlot uint64) (solana.Hash, bool)
	RewardCerts   RewardCertBuilder
	PollInterval  time.Duration
	SlotDuration  time.Duration
	Now           func() time.Time
}

func NewLeaderLoop(cfg LeaderLoopConfig) *LeaderLoop {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Millisecond
	}
	if cfg.UserAgent == nil {
		cfg.UserAgent = []byte("mithril")
	}
	if cfg.SlotDuration <= 0 {
		cfg.SlotDuration = AlpenglowSlotDuration
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &LeaderLoop{
		controller:          cfg.Controller,
		identity:            cfg.Identity,
		accountsDb:          cfg.AccountsDb,
		broadcaster:         cfg.Broadcaster,
		shredVersion:        cfg.ShredVersion,
		userAgent:           cfg.UserAgent,
		epochSchedule:       cfg.EpochSchedule,
		alpenglowClock:      cfg.AlpenglowClock,
		parentContext:       cfg.ParentContext,
		productionParent:    cfg.ProductionParent,
		onBlock:             cfg.OnBlock,
		currentSlot:         cfg.CurrentSlot,
		leaderForSlot:       cfg.LeaderForSlot,
		rewardCerts:         cfg.RewardCerts,
		pollInterval:        cfg.PollInterval,
		slotDuration:        cfg.SlotDuration,
		now:                 cfg.Now,
		finishedLeaderSlots: make(map[uint64]struct{}),
		pendingFailures:     make(map[uint64]leaderSlotFailure),
	}
}

func (l *LeaderLoop) Run(stop <-chan struct{}) {
	ticker := time.NewTicker(l.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			l.finishActiveSlot()
			return
		case scheduledAt := <-ticker.C:
			l.tickScheduled(scheduledAt)
		}
	}
}

func (l *LeaderLoop) tick() {
	l.tickScheduled(l.now())
}

func (l *LeaderLoop) tickScheduled(scheduledAt time.Time) {
	if l.currentSlot == nil || l.leaderForSlot == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.tickStartedAt = l.now()
	l.tickDeliveryLag = 0
	if !scheduledAt.IsZero() {
		l.tickDeliveryLag = l.tickStartedAt.Sub(scheduledAt)
		if l.tickDeliveryLag < 0 {
			l.tickDeliveryLag = 0
		}
	}

	for {
		wallSlot := l.currentSlot()
		now := l.now()

		if l.activeBank != nil {
			if canonical, reason := l.activeParentStillCanonicalLocked(); !canonical {
				l.rememberLeaderSlotFailureLocked(l.activeSlot, reason, activeLeaderFailureDetail(reason))
				l.abortActiveSlotLocked()
				if reason == leaderReasonAlreadyResolved || reason == leaderReasonParentReadyMissedWindow {
					l.failProductionWindowLocked(l.productionWindow.nextSlot, reason, activeLeaderFailureDetail(reason))
				}
				// The no-bank path refreshes a corrected ParentReady parent.
				continue
			}
			now = l.now()
			if l.productionWindow.active {
				if now.Before(l.productionWindowDeadlineLocked(l.activeSlot)) {
					return
				}
			} else if wallSlot <= l.activeSlot {
				return
			}
			l.finishActiveSlotLocked()
			continue
		}

		l.reportExpiredLeaderSlotsLocked(wallSlot)

		// The Votor path below may continue an observed four-slot window even when
		// the coarse live-slot estimate jumps; its exact ParentReady deadline is the
		// authority. The legacy path still targets only the current wall slot and
		// never backfills historical production.
		targetSlot := wallSlot
		if l.productionParent != nil {
			var ok bool
			targetSlot, ok = l.productionWindowTargetLocked(wallSlot)
			if !ok {
				return
			}
		}
		leader, ok := l.leaderForSlot(targetSlot)
		if !ok || leader != l.identity.PublicKey() {
			return
		}
		if l.isLeaderSlotFinishedLocked(targetSlot) {
			return
		}

		if ready, err := l.leaderSlotReplayReady(targetSlot); !ready {
			detail := "leader replay gate was not ready"
			if err != nil {
				detail = err.Error()
			}
			l.rememberLeaderSlotFailureLocked(targetSlot, leaderFailureReason(err), detail)
			return
		}

		// Replay and consensus lookups can consume the final milliseconds of the
		// production budget. Refresh ParentReady and enforce its cutoff again at
		// the last boundary before allocating a working bank.
		if l.productionParent != nil {
			rechecked, ok := l.productionWindowTargetLocked(wallSlot)
			if !ok {
				return
			}
			if rechecked != targetSlot {
				if l.isLeaderSlotFinishedLocked(targetSlot) {
					continue
				}
				return
			}
		}

		startedAt := l.now()
		err := l.startSlotLocked(targetSlot)
		startDuration := l.now().Sub(startedAt)
		if startDuration < 0 {
			startDuration = 0
		}
		result := "success"
		if err != nil {
			result = "error"
		}
		_ = statsd.Duration(statsd.BlockProductionStartAttempt, startDuration, []string{result})
		if err != nil {
			detail := fmt.Sprintf("%s; start_slot_ms=%d", err.Error(), startDuration.Milliseconds())
			if errors.Is(err, errProductionStartCutoffElapsed) {
				now := l.now()
				l.expireProductionWindowLocked(targetSlot, now, detail)
				continue
			}
			if errors.Is(err, errEpochTransitionProductionUnsupported) || errors.Is(err, errEpochRewardsProductionUnsupported) {
				l.failProductionWindowLocked(targetSlot, leaderFailureReason(err), detail)
				continue
			}
			l.rememberLeaderSlotFailureLocked(targetSlot, leaderFailureReason(err), detail)
			return
		}
		delete(l.pendingFailures, targetSlot)
		openedAt := l.now()
		l.recordTickTimingLocked(openedAt, "opened")
		mlog.Log.InfofPrecise("ALPENGLOW block production: opened local leader slot=%d parent_slot=%d replay_frontier=%d live_slot=%d start_slot_ms=%d%s",
			targetSlot, l.parentCtx.ParentSlot, global.ReplayFrontier(), wallSlot,
			startDuration.Milliseconds(), l.productionStartTimingDetailLocked(targetSlot, openedAt))

		return
	}
}

func (l *LeaderLoop) productionWindowTargetLocked(wallSlot uint64) (uint64, bool) {
	if l.productionWindow.active {
		target := l.productionWindow.nextSlot
		if target > l.productionWindow.endSlot {
			l.productionWindow = leaderProductionWindow{}
			return l.productionWindowTargetLocked(wallSlot)
		}
		parentUsable := true
		parent := l.productionParent(target)
		if parent.Kind == alpenglow.BlockProductionParentMissedWindow {
			l.failProductionWindowLocked(target, leaderReasonParentReadyMissedWindow,
				"Votor certified a later slot before local production started")
			return l.productionWindowTargetLocked(wallSlot)
		}
		if target == l.productionWindow.startSlot {
			switch {
			case parent.Kind == alpenglow.BlockProductionParentReady &&
				!parent.Parent.IsZero() && parent.Parent.HasHash() &&
				parent.Parent.Slot < l.productionWindow.startSlot && !parent.ReadyAt.IsZero():
				if parent.Parent != l.productionWindow.parent {
					l.productionWindow.parent = parent.Parent
					// Votor's slot timer is one-shot. A corrected parent changes
					// identity without buying the window a fresh production budget.
					parent.ReadyAt = l.productionWindow.readyAt
					l.recordParentReadyActivationLocked(parent, "handover", wallSlot, l.now())
				}
			case parent.Kind == alpenglow.BlockProductionParentReady:
				parentUsable = false
				detail := "verified ParentReady is invalid while the first slot waits to start"
				if parent.ReadyAt.IsZero() {
					detail = "verified ParentReady has no live timer origin"
				}
				l.rememberLeaderSlotFailureLocked(target, leaderReasonParentReadyInvalid, detail)
			default:
				parentUsable = false
				l.rememberLeaderSlotFailureLocked(target, leaderReasonParentReadyNotReady,
					"verified ParentReady became unavailable before the first slot started")
			}
		}
		now := l.now()
		if !now.Before(l.productionWindowDeadlineLocked(target)) {
			l.expireProductionWindowLocked(target, now, "")
			return l.productionWindowTargetLocked(wallSlot)
		}
		if !parentUsable {
			return 0, false
		}
		return target, true
	}

	startSlot, parent, ok := l.inactiveProductionWindowCandidateLocked(wallSlot)
	if !ok {
		return 0, false
	}
	// Candidate selection and consensus-pool lookup can contend with certificate
	// admission. Refresh the monotonic clock before applying the 125ms cutoff.
	now := l.now()
	switch parent.Kind {
	case alpenglow.BlockProductionParentReady:
		if parent.Parent.IsZero() || !parent.Parent.HasHash() || parent.Parent.Slot >= startSlot {
			l.rememberLeaderSlotFailureLocked(startSlot, leaderReasonParentReadyInvalid,
				"verified ParentReady contains an invalid production parent")
			return 0, false
		}
		readyAt := parent.ReadyAt
		if readyAt.IsZero() {
			l.rememberLeaderSlotFailureLocked(startSlot, leaderReasonParentReadyNotReady, "verified ParentReady has no live timer origin")
			return 0, false
		}
		l.productionWindow = leaderProductionWindow{
			active:    true,
			startSlot: startSlot,
			endSlot:   startSlot + alpenglow.LeaderWindowSlots - 1,
			nextSlot:  startSlot,
			parent:    parent.Parent,
			readyAt:   readyAt,
		}
		onTime := l.recordParentReadyActivationLocked(parent, "initial", wallSlot, now)
		if !onTime {
			l.expireProductionWindowLocked(startSlot, now, "ParentReady arrived too late to complete the first block")
			return l.productionWindowTargetLocked(wallSlot)
		}
		return startSlot, true
	case alpenglow.BlockProductionParentMissedWindow:
		l.productionWindow = leaderProductionWindow{
			active:    true,
			startSlot: startSlot,
			endSlot:   startSlot + alpenglow.LeaderWindowSlots - 1,
			nextSlot:  startSlot,
		}
		l.failProductionWindowLocked(startSlot, leaderReasonParentReadyMissedWindow, "Votor certified a later window before local production started")
		return l.productionWindowTargetLocked(wallSlot)
	default:
		l.rememberLeaderSlotFailureLocked(startSlot, leaderReasonParentReadyNotReady, "waiting for verified ParentReady")
		return 0, false
	}
}

func (l *LeaderLoop) inactiveProductionWindowCandidateLocked(wallSlot uint64) (uint64, alpenglow.BlockProductionParent, bool) {
	if l.leaderForSlot == nil || l.productionParent == nil {
		return 0, alpenglow.BlockProductionParent{}, false
	}
	current := wallSlot - wallSlot%alpenglow.LeaderWindowSlots
	currentLeader, ok := l.leaderForSlot(current)
	if !ok {
		return 0, alpenglow.BlockProductionParent{}, false
	}
	self := l.identity.PublicKey()
	if currentLeader == self {
		if !l.isLeaderSlotFinishedLocked(current) {
			return current, l.productionParent(current), true
		}
		if !l.allProductionWindowSlotsFinishedLocked(current) {
			return 0, alpenglow.BlockProductionParent{}, false
		}
	}

	// ParentReady is the production clock authority, but the coarse live clock
	// can still report the certified parent's slot for another 200ms. Inspect
	// only the immediately following window; never scan arbitrary future slots.
	if current > ^uint64(0)-alpenglow.LeaderWindowSlots {
		return 0, alpenglow.BlockProductionParent{}, false
	}
	next := current + alpenglow.LeaderWindowSlots
	nextLeader, ok := l.leaderForSlot(next)
	if !ok || nextLeader != self || l.isLeaderSlotFinishedLocked(next) {
		return 0, alpenglow.BlockProductionParent{}, false
	}
	parent := l.productionParent(next)
	if parent.Kind != alpenglow.BlockProductionParentReady || parent.ReadyAt.IsZero() {
		return 0, alpenglow.BlockProductionParent{}, false
	}
	return next, parent, true
}

func (l *LeaderLoop) allProductionWindowSlotsFinishedLocked(start uint64) bool {
	if start > ^uint64(0)-(alpenglow.LeaderWindowSlots-1) {
		return false
	}
	for offset := uint64(0); offset < alpenglow.LeaderWindowSlots; offset++ {
		if !l.isLeaderSlotFinishedLocked(start + offset) {
			return false
		}
	}
	return true
}

func (l *LeaderLoop) recordParentReadyActivationLocked(parent alpenglow.BlockProductionParent, activation string, wallSlot uint64, now time.Time) bool {
	readyAge := now.Sub(parent.ReadyAt)
	if readyAge >= 0 {
		_ = statsd.Duration(statsd.BlockProductionParentReadyAge, readyAge, []string{activation})
	}
	status := "on_time"
	if !now.Before(l.productionWindowDeadlineLocked(l.productionWindow.startSlot)) {
		status = "late"
	}
	_ = statsd.Count(statsd.BlockProductionParentReady, 1, []string{activation, status})
	mlog.Log.InfofPrecise("ALPENGLOW block production: parent-ready activation=%s parent=%s replay_frontier=%d live_slot=%d%s",
		activation, parent.Parent.String(), global.ReplayFrontier(), wallSlot,
		l.productionStartTimingDetailLocked(l.productionWindow.startSlot, now))
	return status == "on_time"
}

func (l *LeaderLoop) tickWorkElapsedLocked(now time.Time) time.Duration {
	if l.tickStartedAt.IsZero() {
		return 0
	}
	elapsed := now.Sub(l.tickStartedAt)
	if elapsed < 0 {
		return 0
	}
	return elapsed
}

func (l *LeaderLoop) recordTickTimingLocked(now time.Time, outcome string) {
	_ = statsd.Duration(statsd.BlockProductionStartDecisionTickDeliveryLag, l.tickDeliveryLag, []string{outcome})
	_ = statsd.Duration(statsd.BlockProductionStartDecisionTickWork, l.tickWorkElapsedLocked(now), []string{outcome})
}

func (l *LeaderLoop) productionStartTimingDetailLocked(slot uint64, now time.Time) string {
	if !l.productionWindow.active || slot < l.productionWindow.startSlot {
		return ""
	}
	deadline := l.productionWindowDeadlineLocked(slot)
	lateness := now.Sub(deadline)
	if lateness < 0 {
		lateness = 0
	}
	return fmt.Sprintf(" window_start=%d slot_index=%d ready_age_ms=%d start_margin_ms=%d cutoff_lateness_ms=%d tick_delivery_lag_ms=%d tick_work_ms=%d",
		l.productionWindow.startSlot,
		slot-l.productionWindow.startSlot,
		now.Sub(l.productionWindow.readyAt).Milliseconds(),
		deadline.Sub(now).Milliseconds(),
		lateness.Milliseconds(),
		l.tickDeliveryLag.Milliseconds(),
		l.tickWorkElapsedLocked(now).Milliseconds())
}

func (l *LeaderLoop) recordStartCutoffLatenessLocked(slot uint64, now time.Time) {
	lateness := now.Sub(l.productionWindowDeadlineLocked(slot))
	if lateness < 0 {
		return
	}
	phase := "intra_window"
	if slot == l.productionWindow.startSlot {
		phase = "first_slot"
	}
	_ = statsd.Duration(statsd.BlockProductionStartCutoffLate, lateness, []string{phase})
}

func (l *LeaderLoop) expireProductionWindowLocked(slot uint64, now time.Time, context string) {
	timingDetail := l.productionStartTimingDetailLocked(slot, now)
	l.recordStartCutoffLatenessLocked(slot, now)
	l.recordTickTimingLocked(now, "cutoff_elapsed")

	failure, attempted := l.pendingFailures[slot]
	if !attempted {
		failure.reason = leaderReasonWindowDeadlineElapsed
	}
	appendDetail := func(detail string) {
		if detail == "" {
			return
		}
		if failure.detail != "" {
			failure.detail += "; "
		}
		failure.detail += detail
	}
	appendDetail(context)
	appendDetail("ParentReady-based block deadline elapsed before production started" + timingDetail)
	l.failProductionWindowWithTerminalLocked(slot, leaderTerminalDeadlineElapsed, failure.reason, failure.detail)
}

func (l *LeaderLoop) productionStartCutoffErrorLocked(slot uint64) error {
	if !l.productionWindow.active || slot < l.productionWindow.startSlot || slot > l.productionWindow.endSlot {
		return nil
	}
	deadline := l.productionWindowDeadlineLocked(slot)
	if deadline.IsZero() || l.now().Before(deadline) {
		return nil
	}
	return fmt.Errorf("%w for slot %d", errProductionStartCutoffElapsed, slot)
}

func (l *LeaderLoop) productionWindowDeadlineLocked(slot uint64) time.Time {
	deadline := l.productionWindowProtocolDeadlineLocked(slot)
	if deadline.IsZero() {
		return time.Time{}
	}
	reserve := leaderBlockCompletionReserve
	if reserve >= l.slotDuration {
		reserve = l.slotDuration / 4
	}
	return deadline.Add(-reserve)
}

func (l *LeaderLoop) productionWindowProtocolDeadlineLocked(slot uint64) time.Time {
	if !l.productionWindow.active || slot < l.productionWindow.startSlot {
		return time.Time{}
	}
	index := slot - l.productionWindow.startSlot + 1
	return l.productionWindow.readyAt.Add(time.Duration(index) * l.slotDuration)
}

// productionTimingDetailLocked measures protocol completion at the point the
// last shred has been handed to the network. Local replay handoff happens after
// this timestamp and must not make an on-time block look late.
func (l *LeaderLoop) productionTimingDetailLocked(slot uint64, finalizationStartedAt, networkCompletedAt time.Time) string {
	if !l.productionWindow.active || slot < l.productionWindow.startSlot {
		return ""
	}
	protocolDeadline := l.productionWindowProtocolDeadlineLocked(slot)
	return fmt.Sprintf(" window_elapsed_ms=%d finalization_ms=%d deadline_margin_ms=%d",
		networkCompletedAt.Sub(l.productionWindow.readyAt).Milliseconds(),
		networkCompletedAt.Sub(finalizationStartedAt).Milliseconds(),
		protocolDeadline.Sub(networkCompletedAt).Milliseconds())
}

func (l *LeaderLoop) failProductionWindowLocked(slot uint64, reason, detail string) {
	l.failProductionWindowWithTerminalLocked(slot, reason, reason, detail)
}

func (l *LeaderLoop) failProductionWindowWithTerminalLocked(slot uint64, terminal, cause, detail string) {
	if !l.productionWindow.active {
		l.recordLeaderSlotOutcomeWithTerminalLocked(slot, leaderOutcomeMissed, cause, terminal, cause, detail)
		return
	}
	end := l.productionWindow.endSlot
	l.recordLeaderSlotOutcomeWithTerminalLocked(slot, leaderOutcomeMissed, cause, terminal, cause, detail)
	for remaining := slot; remaining < end; {
		remaining++
		l.recordLeaderSlotOutcomeWithTerminalLocked(remaining, leaderOutcomeMissed, leaderReasonPreviousSlotMissed,
			leaderReasonPreviousSlotMissed, cause,
			fmt.Sprintf("cannot chain local slot %d after missed slot %d", remaining, slot))
	}
	l.productionWindow = leaderProductionWindow{}
}

func activeLeaderFailureDetail(reason string) string {
	switch reason {
	case leaderReasonAlreadyResolved:
		return "ordered replay resolved the local leader slot before its block froze"
	case leaderReasonReplayNotReady:
		return "ordered replay no longer covers the selected production parent"
	case leaderReasonParentReadyNotReady:
		return "verified ParentReady became unavailable before the block froze"
	case leaderReasonParentReadyMissedWindow:
		return "Votor moved ParentReady beyond the local leader window before the block froze"
	default:
		return "selected replay parent changed before the block froze"
	}
}

func sameReplayParentSnapshot(want, got ParentContext) bool {
	if want.ReplayGeneration == 0 || got.ReplayGeneration != want.ReplayGeneration {
		return false
	}
	if got.ParentSlot != want.ParentSlot || got.ParentBankhash != want.ParentBankhash {
		return false
	}
	if got.HasParentBlockID != want.HasParentBlockID ||
		(got.HasParentBlockID && got.ParentBlockID != want.ParentBlockID) {
		return false
	}
	return got.HasParentChainedMerkleRoot == want.HasParentChainedMerkleRoot &&
		(!got.HasParentChainedMerkleRoot || got.ParentChainedMerkleRoot == want.ParentChainedMerkleRoot)
}

func (l *LeaderLoop) activeParentStillCanonicalLocked() (bool, string) {
	if l.activeBank == nil || l.parentContext == nil || l.activeSlot == 0 {
		return true, ""
	}
	frontier := global.ReplayFrontier()
	// A certified skip (or any other accepted outcome) may resolve our slot
	// while we are still producing it. Stop before emitting a footer in that
	// case; the ordered replay outcome is authoritative.
	if frontier >= l.activeSlot {
		return false, leaderReasonAlreadyResolved
	}

	requiredFrontier := l.activeSlot - 1
	if l.productionWindow.active && l.productionParent != nil {
		parent := l.productionParent(l.activeSlot)
		if parent.Kind == alpenglow.BlockProductionParentMissedWindow {
			// Agave abandons the working bank once highest ParentReady moves beyond
			// the bank slot: the cluster has already selected a later outcome.
			return false, leaderReasonParentReadyMissedWindow
		}
		if l.activeSlot == l.productionWindow.startSlot {
			switch parent.Kind {
			case alpenglow.BlockProductionParentReady:
				if parent.Parent.Slot != l.parentCtx.ParentSlot || parent.Parent.Hash != l.activeParentID {
					return false, leaderReasonParentChangedBeforeFreeze
				}
				requiredFrontier = parent.Parent.Slot
			default:
				return false, leaderReasonParentReadyNotReady
			}
		}
	}
	if frontier < requiredFrontier {
		return false, leaderReasonReplayNotReady
	}
	if l.activeParentGeneration == 0 || l.parentCtx.ReplayGeneration != l.activeParentGeneration {
		return false, leaderReasonParentChangedBeforeFreeze
	}
	current := l.parentContext(l.activeSlot)
	if !sameReplayParentSnapshot(l.parentCtx, current) {
		return false, leaderReasonParentChangedBeforeFreeze
	}
	return true, ""
}

func (l *LeaderLoop) abortActiveSlotLocked() {
	if l.controller != nil {
		l.controller.ClearWorkingBank()
	}
	if l.activeBank != nil {
		l.activeBank.Close()
	}
	if l.activeSink != nil {
		l.activeSink.Discard()
	}
	l.activeBank = nil
	l.activeSess = nil
	l.activeSink = nil
	l.activeSlot = 0
	l.activeParentID = solana.Hash{}
	l.activeParentChainedRoot = solana.Hash{}
	l.activeParentGeneration = 0
}

func leaderFailureReason(err error) string {
	if err == nil {
		return leaderReasonLiveClockAdvanced
	}
	if errors.Is(err, errProductionStartCutoffElapsed) {
		return leaderReasonWindowDeadlineElapsed
	}
	if errors.Is(err, errEpochTransitionProductionUnsupported) {
		return leaderReasonEpochTransition
	}
	if errors.Is(err, errEpochRewardsProductionUnsupported) {
		return leaderReasonEpochRewards
	}
	detail := err.Error()
	switch {
	case strings.Contains(detail, "already resolved leader slot"):
		return leaderReasonAlreadyResolved
	case strings.Contains(detail, "ParentReady parent changed while"):
		return leaderReasonParentChangedBeforeFreeze
	case strings.Contains(detail, "ParentReady arrived after"):
		return leaderReasonParentReadyMissedWindow
	case strings.Contains(detail, "no verified ParentReady"):
		return leaderReasonParentReadyNotReady
	case strings.Contains(detail, "invalid ParentReady"):
		return leaderReasonParentReadyInvalid
	case strings.Contains(detail, "does not match ParentReady block id"):
		return leaderReasonParentBlockIDMismatch
	case strings.Contains(detail, "block id missing"), strings.Contains(detail, "no parent block id lookup"):
		return leaderReasonParentBlockIDMissing
	case strings.Contains(detail, "chained merkle root missing"):
		return leaderReasonParentChainedRootMissing
	case strings.Contains(detail, "parent bankhash missing"), strings.Contains(detail, "replay parent slot"):
		return leaderReasonParentContextNotReady
	case strings.Contains(detail, "ordered replay"):
		return leaderReasonReplayNotReady
	case strings.Contains(detail, "new leader slot ctx"):
		return leaderReasonSlotContextFailed
	case strings.Contains(detail, "prepare leader sysvars"):
		return leaderReasonSysvarPrepareFailed
	case strings.Contains(detail, "broadcast header"):
		return leaderReasonHeaderBroadcastFailed
	default:
		return leaderReasonParentContextNotReady
	}
}

func (l *LeaderLoop) rememberLeaderSlotFailureLocked(slot uint64, reason, detail string) {
	if slot == 0 || l.isLeaderSlotFinishedLocked(slot) {
		return
	}
	if l.pendingFailures == nil {
		l.pendingFailures = make(map[uint64]leaderSlotFailure)
	}
	l.pendingFailures[slot] = leaderSlotFailure{reason: reason, detail: detail}
}

// reportExpiredLeaderSlotsLocked records one terminal outcome for every local
// leader slot observed while the loop was running. The first sample establishes
// the live watermark so a process restart does not report historical slots it
// never had an opportunity to produce.
func (l *LeaderLoop) reportExpiredLeaderSlotsLocked(wallSlot uint64) {
	if !l.missedScanStarted {
		l.missedScanStarted = true
		l.missedScanNext = wallSlot
		return
	}
	if wallSlot < l.missedScanNext {
		l.missedScanNext = wallSlot
		return
	}
	for l.missedScanNext < wallSlot {
		slot := l.missedScanNext
		// A ParentReady-timed window owns all four of its outcomes even when the
		// network live-slot estimate jumps ahead. Leave the scanner parked at the
		// window until production either completes it or the next window proves it
		// expired.
		if l.productionParent != nil {
			currentWindowStart := wallSlot - wallSlot%alpenglow.LeaderWindowSlots
			if slot >= currentWindowStart && slot-currentWindowStart < alpenglow.LeaderWindowSlots {
				return
			}
			if l.productionWindow.active && slot >= l.productionWindow.startSlot && slot <= l.productionWindow.endSlot {
				return
			}
		}
		l.missedScanNext++
		if l.isLeaderSlotFinishedLocked(slot) {
			delete(l.pendingFailures, slot)
			continue
		}
		failure, attempted := l.pendingFailures[slot]
		if !attempted {
			leader, ok := l.leaderForSlot(slot)
			if !ok || leader != l.identity.PublicKey() {
				continue
			}
			failure = leaderSlotFailure{
				reason: leaderReasonLiveClockAdvanced,
				detail: "live clock advanced before block production started",
			}
		}
		l.recordLeaderSlotOutcomeWithTerminalLocked(slot, leaderOutcomeMissed, failure.reason,
			leaderReasonLiveClockAdvanced, failure.reason, failure.detail)
	}
}

func (l *LeaderLoop) recordLeaderSlotOutcomeLocked(slot uint64, outcome, reason, detail string) {
	l.recordLeaderSlotOutcomeWithTerminalLocked(slot, outcome, reason, reason, reason, detail)
}

func (l *LeaderLoop) recordLeaderSlotOutcomeWithTerminalLocked(slot uint64, outcome, reason, terminal, cause, detail string) {
	if slot == 0 || l.isLeaderSlotFinishedLocked(slot) {
		return
	}
	if reason == "" {
		reason = leaderReasonParentContextNotReady
	}
	if terminal == "" {
		terminal = reason
	}
	if cause == "" {
		cause = reason
	}
	_ = statsd.Count(statsd.BlockProductionLeaderSlots, 1, []string{outcome, reason})
	_ = statsd.Count(statsd.BlockProductionLeaderSlotTerminals, 1, []string{outcome, terminal, cause})
	liveSlot := uint64(0)
	if l.currentSlot != nil {
		liveSlot = l.currentSlot()
	}
	if outcome == leaderOutcomeBroadcast {
		mlog.Log.Infof("ALPENGLOW block production: broadcast local leader slot=%d terminal=%s cause=%s reason=%s replay_frontier=%d live_slot=%d %s", slot, terminal, cause, reason, global.ReplayFrontier(), liveSlot, detail)
	} else {
		mlog.Log.Warnf("ALPENGLOW block production: missed local leader slot=%d terminal=%s cause=%s reason=%s replay_frontier=%d live_slot=%d detail=%s", slot, terminal, cause, reason, global.ReplayFrontier(), liveSlot, detail)
	}
	l.markLeaderSlotFinished(slot)
	delete(l.pendingFailures, slot)
}

func (l *LeaderLoop) isLeaderSlotFinishedLocked(slot uint64) bool {
	_, ok := l.finishedLeaderSlots[slot]
	return ok
}

func (l *LeaderLoop) isLeaderSlotFinished(slot uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.isLeaderSlotFinishedLocked(slot)
}

func (l *LeaderLoop) markLeaderSlotFinished(slot uint64) {
	if slot == 0 {
		return
	}
	if l.finishedLeaderSlots == nil {
		l.finishedLeaderSlots = make(map[uint64]struct{})
	}
	l.finishedLeaderSlots[slot] = struct{}{}
	for finished := range l.finishedLeaderSlots {
		if finished < slot && slot-finished > 256 {
			delete(l.finishedLeaderSlots, finished)
		}
	}
}

func (l *LeaderLoop) finishActiveSlot() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.finishActiveSlotLocked()
}

// clampProducerTimeNanos clamps the leader's wall-clock footer timestamp into the
// Alpenglow nanosecond-clock bounds derived from the parent bank, matching Agave's
// block_creation_loop::skew_block_producer_time_nanos. Without this, a leader that
// just caught up stamps real "now", which is typically >2*slot_duration past the
// parent clock and is rejected by the network as NanosecondClockOutOfBounds.
func (l *LeaderLoop) clampProducerTimeNanos(slot uint64, nowNanos int64) uint64 {
	if !l.alpenglowClock || l.accountsDb == nil || l.activeBank == nil {
		return uint64(nowNanos)
	}
	slotCtx := l.activeBank.SlotCtx()
	if slotCtx == nil {
		return uint64(nowNanos)
	}
	parentSlot := slotCtx.ParentSlot
	parentNanos, ok := replay.ReadNanosecondClockFromSlotCtx(slotCtx)
	if !ok {
		return uint64(nowNanos)
	}
	var elapsed uint64
	if slot > parentSlot {
		elapsed = (slot - parentSlot) * uint64(l.slotDuration.Nanoseconds())
	}
	clamped := replay.SkewBlockProducerTimeNanos(parentNanos, nowNanos, elapsed)
	if clamped < 0 {
		clamped = 0
	}
	return uint64(clamped)
}

func (l *LeaderLoop) finishActiveSlotLocked() {
	if l.activeBank == nil {
		return
	}
	finalizationStartedAt := l.now()
	slot := l.activeSlot
	if canonical, reason := l.activeParentStillCanonicalLocked(); !canonical {
		l.failProductionWindowLocked(slot, reason, activeLeaderFailureDetail(reason))
		l.abortActiveSlotLocked()
		return
	}

	// Detach first so no new TPU packet can obtain this bank. Freeze then waits
	// for any packet that already did, closing the footer/entry race.
	if l.controller != nil {
		l.controller.ClearWorkingBank()
	}
	l.activeBank.Freeze()
	if l.activeSink == nil {
		l.failProductionWindowLocked(slot, leaderReasonFinalizationUnavailable, "entry broadcast sink is unavailable")
		l.abortActiveSlotLocked()
		return
	}
	l.activeSink.Wait()
	if err := l.activeSink.Err(); err != nil {
		l.failProductionWindowLocked(slot, leaderReasonEntryBroadcastFailed, err.Error())
		l.abortActiveSlotLocked()
		return
	}

	pohTip := l.activeBank.EntryHash()
	tickHash := turbine.AlpentickHash(pohTip)
	producerTimeNanos := l.clampProducerTimeNanos(slot, time.Now().UnixNano())
	footerTimestamp := int64(producerTimeNanos / 1_000_000_000)

	var footerRewards rewardcerts.RewardCertificates
	if l.rewardCerts != nil {
		footerRewards = l.rewardCerts.BuildForLeaderSlot(slot)
	}

	if l.accountsDb == nil || l.activeSess == nil {
		l.failProductionWindowLocked(slot, leaderReasonFinalizationUnavailable, "accounts DB or broadcast session is unavailable")
		l.abortActiveSlotLocked()
		return
	}
	parentLastBlockhash := l.parentCtx.ParentLastBlockhash
	if parentLastBlockhash == (solana.Hash{}) {
		parentLastBlockhash = l.parentCtx.ParentLastEntryHash
	}

	producedBlock := BuildLeaderBlock(LeaderBlockInput{
		Bank:                l.activeBank,
		ParentSlot:          l.parentCtx.ParentSlot,
		ParentBankhash:      l.parentCtx.ParentBankhash,
		ParentLastBlockhash: parentLastBlockhash,
		ParentBlockHeight:   l.parentCtx.ParentBlockHeight,
		PrevNumSigs:         l.parentCtx.PrevNumSigs,
		PrevFeeGovernor:     l.parentCtx.PrevFeeGovernor,
		EntryBlockhash:      tickHash,
		TxFeeAccumulator:    l.activeBank.TxFeeAccumulator(),
	})
	producedBlock.SkipRewardCert = append([]byte(nil), footerRewards.Skip...)
	producedBlock.NotarRewardCert = append([]byte(nil), footerRewards.Notar...)
	producedBlock.FooterProducerTimeNanos = producerTimeNanos
	producedBlock.HasAlpenglowFooter = true
	producedBlock.AlpenglowShredVersion = l.shredVersion
	if slot > 0 {
		producedBlock.AlpenglowParentBlockID = l.activeParentID
		producedBlock.HasAlpenglowParentBlockID = true
	}

	commitLeaderSlot := replay.CommitLeaderSlot
	if l.commitLeaderSlot != nil {
		commitLeaderSlot = l.commitLeaderSlot
	}
	if _, err := commitLeaderSlot(replay.CommitLeaderInput{
		AcctsDb:                 l.accountsDb,
		SlotCtx:                 l.activeBank.SlotCtx(),
		Block:                   producedBlock,
		TxFeeAccumulator:        l.activeBank.TxFeeAccumulator(),
		AlpenglowClock:          l.alpenglowClock,
		AlpenglowShredVersion:   l.shredVersion,
		FooterTimestamp:         footerTimestamp,
		FooterProducerTimeNanos: producerTimeNanos,
	}); err != nil {
		l.failProductionWindowLocked(slot, leaderReasonFreezeFailed, err.Error())
		l.abortActiveSlotLocked()
		return
	}

	if canonical, reason := l.activeParentStillCanonicalLocked(); !canonical {
		l.failProductionWindowLocked(slot, reason, activeLeaderFailureDetail(reason))
		l.abortActiveSlotLocked()
		return
	}
	footerBankhash := solana.HashFromBytes(l.activeBank.SlotCtx().FinalBankhash)
	if err := l.activeSess.BroadcastFooter(footerBankhash, producerTimeNanos, footerRewards.Skip, footerRewards.Notar); err != nil {
		l.failProductionWindowLocked(slot, leaderReasonFooterBroadcastFailed, err.Error())
		l.abortActiveSlotLocked()
		return
	}
	// Ending tick is the last-in-slot shred. Mid-slot entry batches are only
	// emitted when they fill a FEC set; Freeze already flushed any leftover.
	if err := l.activeSess.BroadcastEndingTickLast(tickHash); err != nil {
		l.failProductionWindowLocked(slot, leaderReasonEndingTickBroadcastFailed, err.Error())
		l.abortActiveSlotLocked()
		return
	}
	// BroadcastEndingTickLast synchronously hands the final shred batch to the
	// packet broadcaster. Capture protocol timing now, before local bookkeeping
	// and OnBlock backpressure can delay the leader loop.
	networkCompletedAt := l.now()
	timingDetail := l.productionTimingDetailLocked(slot, finalizationStartedAt, networkCompletedAt)
	if slot > 0 {
		parentSlot := producedBlock.ParentSlot
		blockID := l.activeSess.BlockID(parentSlot, l.activeParentID)
		producedBlock.AlpenglowBlockID = blockID
		producedBlock.HasAlpenglowBlockID = true
		global.SetAlpenglowBlockID(slot, blockID)
	}
	if chained := l.activeSess.ChainedMerkleRoot(); chained != (solana.Hash{}) {
		producedBlock.AlpenglowLastChainedRoot = chained
		producedBlock.HasAlpenglowLastChainedRoot = true
		global.SetAlpenglowChainedMerkleRoot(slot, chained)
	}
	replay.RegisterLocalLeaderCommit(l.activeBank.SlotCtx())
	if l.onBlock != nil {
		handoffStartedAt := l.now()
		l.onBlock(producedBlock)
		if timingDetail != "" {
			timingDetail += fmt.Sprintf(" local_handoff_ms=%d", l.now().Sub(handoffStartedAt).Milliseconds())
		}
	}
	l.recordLeaderSlotOutcomeLocked(slot, leaderOutcomeBroadcast, leaderReasonComplete,
		fmt.Sprintf("block=%s parent_slot=%d txns=%d%s", solana.Hash(producedBlock.AlpenglowBlockID).String(), producedBlock.ParentSlot, len(producedBlock.Transactions), timingDetail))
	l.advanceProductionWindowLocked(slot)
	l.abortActiveSlotLocked()
}

func (l *LeaderLoop) advanceProductionWindowLocked(slot uint64) {
	if !l.productionWindow.active || l.productionWindow.nextSlot != slot {
		return
	}
	if slot == l.productionWindow.endSlot {
		l.productionWindow = leaderProductionWindow{}
		return
	}
	l.productionWindow.nextSlot = slot + 1
}

func (l *LeaderLoop) revalidateProductionParentForStartLocked(slot uint64, selectedParent alpenglow.BlockID) error {
	if l.productionParent == nil || slot == 0 || slot%alpenglow.LeaderWindowSlots != 0 {
		return nil
	}
	parent := l.productionParent(slot)
	switch parent.Kind {
	case alpenglow.BlockProductionParentReady:
		if parent.Parent.IsZero() || !parent.Parent.HasHash() || parent.Parent.Slot >= slot ||
			parent.ReadyAt.IsZero() {
			return fmt.Errorf("%w: invalid ParentReady value while opening leader window %d", errParentNotReady, slot)
		}
		if parent.Parent != selectedParent {
			return fmt.Errorf("%w: ParentReady parent changed while opening leader slot %d", errParentNotReady, slot)
		}
		return nil
	case alpenglow.BlockProductionParentMissedWindow:
		return fmt.Errorf("%w: ParentReady arrived after leader window %d", errParentNotReady, slot)
	default:
		return fmt.Errorf("%w: no verified ParentReady for leader window %d", errParentNotReady, slot)
	}
}

func (l *LeaderLoop) startSlotLocked(slot uint64) error {
	selectedParent, parentReadyRequired, err := l.resolveProductionParent(slot)
	if err != nil {
		return err
	}
	if parentReadyRequired && l.productionWindow.active && slot == l.productionWindow.startSlot && selectedParent != l.productionWindow.parent {
		return fmt.Errorf("%w: ParentReady parent changed while opening leader slot %d", errParentNotReady, slot)
	}
	if err := l.productionStartCutoffErrorLocked(slot); err != nil {
		return err
	}
	parentCtx := ParentContext{}
	if l.parentContext != nil {
		parentCtx = l.parentContext(slot)
	}
	epochSchedule := l.epochSchedule
	if parentCtx.BankSysvars != nil {
		if bankEpochSchedule, ok := parentCtx.BankSysvars.EpochSchedule(); ok {
			epochSchedule = &bankEpochSchedule
		} else {
			return fmt.Errorf("%w: EpochSchedule missing for replay parent slot %d", errParentNotReady, parentCtx.ParentSlot)
		}
	}
	parentSlot := parentCtx.ParentSlot
	if slot == 0 {
		parentSlot = 0
	}
	if ready, err := l.leaderSlotReplayReady(slot); !ready {
		return err
	}
	if parentReadyRequired && parentCtx.ParentSlot != selectedParent.Slot {
		return fmt.Errorf("%w: replay parent slot %d does not match ParentReady slot %d", errParentNotReady, parentCtx.ParentSlot, selectedParent.Slot)
	}
	if slot > 0 && parentCtx.ParentBankhash == (solana.Hash{}) {
		return fmt.Errorf("%w: parent bankhash missing for slot %d", errParentNotReady, parentSlot)
	}
	if slot > 0 && epochSchedule != nil && epochSchedule.GetEpoch(parentSlot) != epochSchedule.GetEpoch(slot) {
		return fmt.Errorf("%w: parent block is slot %d", errEpochTransitionProductionUnsupported, parentSlot)
	}
	if parentCtx.EpochRewardsActive {
		return errEpochRewardsProductionUnsupported
	}
	if slot > 0 && (parentCtx.TransactionStatuses == nil || !parentCtx.TransactionStatuses.CoverageComplete()) {
		return fmt.Errorf("%w: complete transaction-status view missing for replay parent slot %d", errParentNotReady, parentSlot)
	}
	if slot > 0 && parentCtx.PrevFeeGovernor == nil {
		return fmt.Errorf("%w: parent fee rate governor missing for replay parent slot %d", errParentNotReady, parentSlot)
	}
	if slot > 0 && l.accountsDb != nil && parentCtx.BankSysvars == nil {
		return fmt.Errorf("%w: bank sysvar snapshot missing for replay parent slot %d", errParentNotReady, parentSlot)
	}
	if slot > 0 && l.accountsDb != nil {
		if err := parentCtx.BankSysvars.ValidateForExecution(); err != nil {
			return fmt.Errorf("%w: invalid bank sysvar snapshot for replay parent slot %d: %v", errParentNotReady, parentSlot, err)
		}
	}
	if slot > 0 && parentCtx.ReplayGeneration == 0 {
		return fmt.Errorf("%w: replay generation missing for parent slot %d", errParentNotReady, parentSlot)
	}
	if slot > 0 && !parentCtx.HasParentBlockID {
		return fmt.Errorf("%w: alpenglow block id missing for parent slot %d", errParentNotReady, parentSlot)
	}
	if slot > 0 && !parentCtx.HasParentChainedMerkleRoot {
		return fmt.Errorf("%w: chained merkle root missing for parent slot %d", errParentNotReady, parentSlot)
	}

	parentID := parentCtx.ParentBlockID
	if parentReadyRequired && parentID != selectedParent.Hash {
		return fmt.Errorf("%w: replay parent block id %s does not match ParentReady block id %s", errParentNotReady, parentID, selectedParent.Hash)
	}

	parentChainedRoot := parentCtx.ParentChainedMerkleRoot

	session := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
		Leader:                  l.identity,
		Slot:                    slot,
		ParentSlot:              parentSlot,
		ParentBlockID:           parentID,
		ParentChainedMerkleRoot: parentChainedRoot,
		Version:                 l.shredVersion,
		Broadcaster:             l.broadcaster,
		UserAgent:               l.userAgent,
	})
	slotCtx, err := NewLeaderSlotCtx(slot, parentSlot, l.accountsDb, parentCtx, epochSchedule)
	if err != nil {
		return fmt.Errorf("new leader slot ctx: %w", err)
	}
	if l.accountsDb != nil && epochSchedule != nil {
		prepBlock := &b.Block{
			Slot:           slot,
			ParentSlot:     parentSlot,
			Epoch:          epochSchedule.GetEpoch(slot),
			ParentBankhash: parentCtx.ParentBankhash,
		}
		if err := replay.PrepareLeaderSlotSysvars(slotCtx, prepBlock, l.alpenglowClock); err != nil {
			return fmt.Errorf("prepare leader sysvars: %w", err)
		}
	}
	startEntryHash := parentCtx.ParentLastBlockhash
	if startEntryHash == (solana.Hash{}) {
		startEntryHash = parentCtx.ParentLastEntryHash
	}
	if startEntryHash == (solana.Hash{}) {
		startEntryHash = parentCtx.ParentBankhash
	}
	sink := NewShredSink(session)
	bank := NewWorkingBank(BankConfig{
		SlotCtx:             slotCtx,
		Slot:                slot,
		Leader:              l.identity.PublicKey(),
		Limits:              costmodel.LimitsForFeatures(slotCtx.Features),
		EntryHash:           startEntryHash,
		Sink:                sink,
		TransactionStatuses: parentCtx.TransactionStatuses,
	})
	if slot > 0 && !sameReplayParentSnapshot(parentCtx, l.parentContext(slot)) {
		bank.Close()
		return fmt.Errorf("%w: replay parent changed while opening leader slot %d", errParentNotReady, slot)
	}
	if parentReadyRequired {
		if err := l.revalidateProductionParentForStartLocked(slot, selectedParent); err != nil {
			bank.Close()
			return err
		}
	}
	if err := l.productionStartCutoffErrorLocked(slot); err != nil {
		bank.Close()
		return err
	}
	// Publish the working bank only after all local preparation and the header
	// broadcast succeed; failures before this point cannot admit TPU traffic.
	if err := session.BroadcastHeader(parentID); err != nil {
		bank.Close()
		return fmt.Errorf("broadcast header parent_block_id=%s: %w", parentID, err)
	}
	if slot > 0 && !sameReplayParentSnapshot(parentCtx, l.parentContext(slot)) {
		bank.Close()
		return fmt.Errorf("%w: replay parent changed while broadcasting leader header for slot %d", errParentNotReady, slot)
	}
	if parentReadyRequired {
		if err := l.revalidateProductionParentForStartLocked(slot, selectedParent); err != nil {
			bank.Close()
			return err
		}
	}
	if err := l.productionStartCutoffErrorLocked(slot); err != nil {
		bank.Close()
		return err
	}
	l.parentCtx = parentCtx
	l.activeSlot = slot
	l.activeBank = bank
	l.activeSess = session
	l.activeSink = sink
	l.activeParentID = parentID
	l.activeParentChainedRoot = parentChainedRoot
	l.activeParentGeneration = parentCtx.ReplayGeneration
	if l.controller != nil {
		l.controller.SetWorkingBank(bank)
	}
	return nil
}
