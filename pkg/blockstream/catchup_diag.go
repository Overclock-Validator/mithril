// catchup_diag: Catchup diagnostics: peer-quality lines, peer service tables, and stall dumps.
package blockstream

import (
	"fmt"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

// repairPeerQualityLine renders the one-clause peer-quality summary ("" when
// nothing tracked yet): responder count, timely/late shares, median latency,
// score distribution. Compact by design — it rides existing summary and
// heartbeat lines rather than adding new ones.
func repairPeerQualityLine(receiver *turbine.UDPReceiver) string {
	agg, ok := receiver.RepairPeerQuality()
	if !ok || agg.Tracked == 0 || agg.Responding == 0 {
		return ""
	}
	return fmt.Sprintf("peers %d/%d (timely %d%%, late %d%%, lat p50 %dms, score p50 %.2f p90 %.2f)",
		agg.Responding, agg.Tracked, agg.TimelyPct, agg.LatePct,
		agg.MedianLatencyMillis, agg.ScoreP50, agg.ScoreP90)
}

// RepairPeerQualityLine exposes the peer-quality clause for the replay
// summary block; empty when no turbine receiver is active.
func (bs *BlockSource) RepairPeerQualityLine() string {
	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver == nil {
		return ""
	}
	return repairPeerQualityLine(receiver)
}

// logRepairPeerTable writes the busiest repair peers' service records to the
// repair-peers file log — which peers actually answer, how fast, and what
// their rolling quality score is. File-only: the table is diagnostic bulk.
func logRepairPeerTable(receiver *turbine.UDPReceiver, context string) {
	report := receiver.RepairPeerReport(20)
	if len(report) == 0 {
		return
	}
	mlog.NamedFilef("repair-peers", "— %s: top %d peers by requests sent —", context, len(report))
	for _, p := range report {
		last := "never"
		if p.SecondsSinceHeard >= 0 {
			last = fmt.Sprintf("%ds ago", p.SecondsSinceHeard)
		}
		mlog.NamedFilef("repair-peers", "%-22s sent %6d | timely %6d | late %5d | timeout %6d | avg %5dms | score %.2f | heard %s",
			p.Addr, p.Sent, p.Timely, p.Late, p.Timeouts, p.AvgResponseMillis, p.Score, last)
	}
}

// catchupDiagSuffix appends the live repair/hydration/verification picture to
// a catchup.log line — the counters that turn "waiting on slot X" into
// "waiting on slot X because responses stalled / are arriving late / the
// window isn't filling". Counters are cumulative; read progressions down the
// file. Empty when no turbine receiver is active.
func (bs *BlockSource) catchupDiagSuffix() string {
	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver == nil {
		return ""
	}
	st := receiver.Stats()
	latest, full := receiver.ShredEdges()
	return fmt.Sprintf(" | edge shred=%d full=%d | repair: req %d, resp %d (late %d), timeout %d, outstanding %d, timeout_ms %d (avg %d), peers %d/%d | hydrated %d (disk-complete %d) | sigverify ed25519 %d cached %d | active_slots %d",
		latest, full,
		st.Repair.Requests, st.Repair.Responses, st.Repair.LateResponses, st.Repair.Timeouts,
		st.Repair.Outstanding, st.Repair.TimeoutMillis, st.Repair.AvgResponseMillis,
		st.Repair.RespondingPeers, st.Repair.Peers,
		st.HydratedSlots, st.HydratedFromDisk,
		st.SigVerifies, st.SigVerifyCached,
		st.ActiveSlots)
}

// collectStallDiagnostics gathers comprehensive state for stall analysis
func (bs *BlockSource) collectStallDiagnostics() StallDiagnostics {
	diag := StallDiagnostics{}

	// Waiting slot context
	bs.reorderMu.Lock()
	diag.WaitingSlot = bs.nextSlotToSend
	diag.ReorderBufLen = len(bs.reorderBuffer)
	diag.SkippedSlotsLen = len(bs.skippedSlots)
	bs.reorderMu.Unlock()

	diag.LastExecutedSlot = bs.lastExecutedSlot.Load()
	diag.ConfirmedTip = bs.confirmedTip.Load()
	if diag.ConfirmedTip > diag.LastExecutedSlot {
		diag.Gap = diag.ConfirmedTip - diag.LastExecutedSlot
	}
	if bs.isNearTip.Load() {
		diag.Mode = "near-tip"
	} else {
		diag.Mode = "catchup"
	}
	diag.LastProgressTs = time.Unix(bs.lastProgress.Load(), 0)
	diag.StallElapsed = time.Since(diag.LastProgressTs)

	// Slot state snapshot
	bs.slotStateMu.Lock()
	diag.InflightCount = 0
	for _, state := range bs.slotState {
		if state == slotInflight {
			diag.InflightCount++
		}
	}
	// Check waiting slot state
	if state, exists := bs.slotState[diag.WaitingSlot]; exists {
		switch state {
		case slotInflight:
			diag.WaitingSlotState = "inflight"
		case slotDone:
			diag.WaitingSlotState = "done"
		case slotPending:
			diag.WaitingSlotState = "pending"
		}
	} else {
		diag.WaitingSlotState = "missing"
	}
	bs.slotStateMu.Unlock()

	bs.retryMu.Lock()
	diag.RetryQueueLen = len(bs.retrySlots)
	bs.retryMu.Unlock()

	diag.WorkQueueLen = len(bs.workQueue)
	diag.MaxBufferedSlot = bs.stats.MaxBufferedSlot.Load()

	// Waiting slot error info
	bs.waitingSlotErrorsMu.Lock()
	if info, exists := bs.waitingSlotErrors[diag.WaitingSlot]; exists {
		diag.WaitingSlotErrors = &slotErrorInfo{
			slot:           info.slot,
			retryCount:     info.retryCount,
			firstSeenAt:    info.firstSeenAt,
			lastSeenAt:     info.lastSeenAt,
			lastError:      info.lastError,
			lastErrorClass: info.lastErrorClass,
			lastRpcIdx:     info.lastRpcIdx,
			lastLatencyMs:  info.lastLatencyMs,
		}
	}
	bs.waitingSlotErrorsMu.Unlock()

	// RPC health snapshot
	diag.ActiveRpcIdx = bs.activeRpcIdx.Load()
	if diag.ActiveRpcIdx >= 0 && int(diag.ActiveRpcIdx) < len(bs.rpcClients) {
		diag.ActiveRpcURL = bs.rpcClients[diag.ActiveRpcIdx].EndpointForDisplay()
	}
	diag.FailoverCount = bs.failoverCount.Load()
	diag.LastFailoverTime = time.Unix(bs.lastFailoverTime.Load(), 0)
	diag.IsOnPrimary = bs.isOnPrimary()

	// Error counts
	diag.ErrSlotNotAvail = bs.stats.ErrSlotNotAvail.Load()
	diag.ErrRateLimited = bs.stats.ErrRateLimited.Load()
	diag.ErrBeyondTip = bs.stats.ErrBeyondTip.Load()
	diag.ErrHistory = bs.stats.ErrHistory.Load()
	diag.ErrTransient = bs.stats.ErrTransient.Load()
	diag.ErrOther = bs.stats.ErrOther.Load()

	// Worker pool stats
	diag.WorkersTotal = bs.maxInflight
	diag.RateLimitRPS = float64(bs.rateLimiter.Limit())

	return diag
}

// logStallDiagnostics logs comprehensive stall diagnostic information
func (bs *BlockSource) logStallDiagnostics(prefix string) {
	diag := bs.collectStallDiagnostics()

	mlog.Log.Errorf("=== %s ===", prefix)

	// 1) Waiting slot context
	mlog.Log.Errorf("Waiting slot context:")
	mlog.Log.Errorf("  waiting_slot=%d last_executed=%d confirmed_tip=%d gap=%d",
		diag.WaitingSlot, diag.LastExecutedSlot, diag.ConfirmedTip, diag.Gap)
	mlog.Log.Errorf("  mode=%s stall_elapsed=%v last_progress=%s",
		diag.Mode, diag.StallElapsed.Round(time.Second), diag.LastProgressTs.Format("15:04:05"))

	// 2) Slot state snapshot
	mlog.Log.Errorf("Slot state snapshot:")
	mlog.Log.Errorf("  inflight=%d retry_queue=%d work_queue=%d",
		diag.InflightCount, diag.RetryQueueLen, diag.WorkQueueLen)
	mlog.Log.Errorf("  reorder_buf=%d skipped_slots=%d max_buffered=%d",
		diag.ReorderBufLen, diag.SkippedSlotsLen, diag.MaxBufferedSlot)
	mlog.Log.Errorf("  waiting_slot_state=%s", diag.WaitingSlotState)

	// 3) Waiting slot error info
	if diag.WaitingSlotErrors != nil {
		e := diag.WaitingSlotErrors
		mlog.Log.Errorf("Waiting slot %d error history:", e.slot)
		mlog.Log.Errorf("  retry_count=%d first_seen=%s last_seen=%s",
			e.retryCount, e.firstSeenAt.Format("15:04:05"), e.lastSeenAt.Format("15:04:05"))
		mlog.Log.Errorf("  last_error_class=%s last_rpc_idx=%d last_latency_ms=%d",
			e.lastErrorClass, e.lastRpcIdx, e.lastLatencyMs)
		if e.lastError != "" {
			mlog.Log.Errorf("  last_error: %s", e.lastError)
		}
	} else {
		mlog.Log.Errorf("Waiting slot %d: no error history (never attempted or cleared)", diag.WaitingSlot)
	}

	// 4) RPC health snapshot
	mlog.Log.Errorf("RPC health:")
	mlog.Log.Errorf("  active_rpc_idx=%d active_url=%s is_primary=%v",
		diag.ActiveRpcIdx, diag.ActiveRpcURL, diag.IsOnPrimary)
	mlog.Log.Errorf("  failover_count=%d last_failover=%s",
		diag.FailoverCount, diag.LastFailoverTime.Format("15:04:05"))
	mlog.Log.Errorf("  errors: slot_not_avail=%d rate_limited=%d beyond_tip=%d history_unavailable=%d transient=%d other=%d",
		diag.ErrSlotNotAvail, diag.ErrRateLimited, diag.ErrBeyondTip, diag.ErrHistory, diag.ErrTransient, diag.ErrOther)

	// 5) Worker pool stats
	mlog.Log.Errorf("Worker pool: workers=%d rate_limit_rps=%.1f",
		diag.WorkersTotal, diag.RateLimitRPS)

	mlog.Log.Errorf("=== End %s ===", prefix)
}
