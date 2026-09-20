// repair_catchup: Repair-first catchup: the continuous monitor and drive that fill resume/regression gaps via turbine repair (shreds-only has no other catchup).
package blockstream

import (
	"context"
	"fmt"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

// repairCatchupActive reports whether the repair-first catchup window is
// armed (gap slots fill via turbine repair; RPC stays off them).
func (bs *BlockSource) repairCatchupActive() bool {
	return bs.repairCatchupFrom.Load() != 0
}

// repairCatchupAcceptingLiveBlocks covers both sides of the short handoff
// between updateMode and the resident repair monitor.  updateMode first
// clears near-tip and marks catchup pending; the monitor publishes the active
// range a few ticks later.  Live blocks assembled in that interval must stay
// admitted, otherwise the assembler keeps a completed-slot marker for a block
// the block source discarded and replay has to repair it all over again.
func (bs *BlockSource) repairCatchupAcceptingLiveBlocks() bool {
	return bs.repairCatchupPending.Load() || bs.repairCatchupActive()
}

// repairCatchupResumeFrontier is the slot replay resumes at: the durable /
// snapshot frontier seeded into the block source at construction (startSlot =
// manifest.Bank.Slot+1 on a fresh bootstrap, i.e. the incremental snapshot
// slot; or GetResumeSlot() on a restart). This — NOT lastExecutedSlot, which
// is 0 until replay executes its first block — is the baseline for the catchup
// gap and the pending RPC gate. During the pending window RPC is suppressed
// over the gap, so nextSlotToSend cannot advance past startSlot; reading the
// immutable startSlot avoids taking reorderMu on the fetch hot path.
func (bs *BlockSource) repairCatchupResumeFrontier() uint64 {
	return bs.startSlot
}

// repairCatchupGateSlot is the first slot RPC must not fetch while repair
// catchup is pending or active. While pending it is the resume frontier; once
// active it is the armed gap start.
func (bs *BlockSource) repairCatchupGateSlot() uint64 {
	if from := bs.repairCatchupFrom.Load(); from != 0 {
		return from
	}
	return bs.repairCatchupResumeFrontier()
}

func (bs *BlockSource) deactivateRepairCatchup(receiver *turbine.UDPReceiver) {
	bs.repairCatchupFrom.Store(0)
	bs.repairCatchupUntil.Store(0)
	if receiver != nil {
		receiver.SetRetentionFloor(0)
		receiver.SetHydrationWindow(0, 0)
	}
}

// catchupRescueCovers reports whether slot is inside the active catchup
// stall-rescue window.
func (bs *BlockSource) catchupRescueCovers(slot uint64) bool {
	from := bs.rescueFrom.Load()
	return from != 0 && slot >= from && slot <= bs.rescueUntil.Load()
}

// maybeRescueStalledCatchupSlot detects head-of-line blocking during RPC
// catchup — the emitter waiting on ONE slot while the buffer grows (typical
// cause: a 50k+-txn block the RPC takes tens of seconds to serialize) — and,
// when turbine is connected, arms a small rescue window: the assembler is
// pushed to repair-fetch the waiting slot and its immediate successors, and
// blocks assembled inside the window are delivered to the emitter directly.
// Rescued blocks carry Alpenglow block ids (which RPC blocks cannot), and a
// rescued block that fails the parent-connect check simply stays buffered
// with RPC still racing — strictly additive.
func (bs *BlockSource) maybeRescueStalledCatchupSlot() {
	if bs.isNearTip.Load() || bs.repairCatchupActive() || bs.repairCatchupPending.Load() {
		return
	}
	if bs.liveHandoffSlot.Load() != 0 || !bs.usesLiveShredStream() {
		return
	}
	bs.reorderMu.Lock()
	waiting := bs.nextSlotToSend
	buffered := len(bs.reorderBuffer)
	waitingMissing := waiting != 0 && bs.reorderBuffer[waiting] == nil && !bs.skippedSlots[waiting]
	bs.reorderMu.Unlock()
	if !waitingMissing || buffered < reorderGapWarnThreshold {
		return
	}

	now := time.Now()
	if bs.rescueWaitingSlot.Swap(waiting) != waiting {
		bs.rescueWaitingSince.Store(now.Unix())
		return
	}
	if now.Sub(time.Unix(bs.rescueWaitingSince.Load(), 0)) < catchupRescueAfterStall {
		return
	}

	until := waiting + catchupRescueWindowSlots - 1
	bs.rescueUntil.Store(until)
	bs.rescueFrom.Store(waiting)
	bs.prioritizeTurbineRepairRange(waiting, until)
	if bs.lastRescueLogSlot.Swap(waiting) != waiting {
		mlog.Log.Infof("catchup stall rescue: emitter blocked on slot %d for >%s (RPC slow — likely a large block); pulling slots %d..%d via turbine repair instead",
			waiting, catchupRescueAfterStall, waiting, until)
	}
}

// runRepairCatchup keeps catchup on turbine repair whenever the gap allows.
// It is a CONTINUOUS monitor, not a one-shot decision: at startup it measures
// the gap from the resume frontier to the live tip and arms when within
// block.repair_catchup_max_gap_slots; when the gap is too large it falls back
// to RPC catchup but KEEPS WATCHING — the moment RPC catchup shrinks the gap
// under the threshold, the remainder hands over to turbine repair and RPC
// serves only the trailing verifier. Turbine + repair are the native block
// path; RPC block fetch exists ONLY for the too-far-behind case (the same
// threshold, re-evaluated live inside the drive) — never on a stall timer.
// With RPC fallback enabled, a fresh boot within the threshold waits for
// turbine to wake (shreds received, repair peers known) — shreds
// are coming, so the node waits for shreds; if the gap outgrows the
// threshold while waiting, the far-behind rule releases the hold to RPC.
// The only RPC bridge left is the no-tip-signal edge (RPC tip unknown AND
// no shreds — nothing to measure the gap against).
func (bs *BlockSource) runRepairCatchup(ctx context.Context, receiver *turbine.UDPReceiver) {
	// The construction-time RPC hold persists until repair arms or is ruled
	// out (gap too large, no tip signal). It is deliberately NOT released
	// just because turbine is still waking up: a fresh boot within the
	// repair threshold waits for its repair path instead of starting an RPC catchup
	// it would abandon.
	first := true
	releaseHold := func() {
		if first {
			first = false
			bs.repairCatchupPending.Store(false)
		}
	}
	defer bs.repairCatchupPending.Store(false)
	var lastNotReadyLog time.Time

	for {
		if bs.stopped.Load() || ctx.Err() != nil {
			return
		}

		// Frontier: the resume gate slot on the first pass (replay has not
		// executed anything yet), the live emission frontier afterwards.
		var from uint64
		if first {
			from = bs.repairCatchupResumeFrontier()
			if from == 0 {
				mlog.Log.Warnf("repair catchup: no resume frontier (replaying from genesis); using RPC catchup")
				return
			}
		} else {
			bs.reorderMu.Lock()
			from = bs.nextSlotToSend
			bs.reorderMu.Unlock()
			if from == 0 {
				from = bs.lastExecutedSlot.Load() + 1
			}
		}

		// Tip signal: turbine's shred edge or the RPC confirmed tip,
		// whichever is ahead. On the first pass, wait a bounded window for
		// any signal before releasing the RPC hold.
		edge := bs.repairCatchupEdge(receiver)
		if edge == 0 && first {
			deadline := time.Now().Add(repairCatchupDecisionTimeout)
			for edge == 0 && time.Now().Before(deadline) && !bs.stopped.Load() && ctx.Err() == nil {
				if !bs.sleepOrStop(ctx, repairCatchupPollInterval) {
					return
				}
				edge = bs.repairCatchupEdge(receiver)
			}
			if edge == 0 {
				if bs.rpcFallbackEnabled {
					mlog.Log.Warnf("repair catchup: no live-tip signal within %s; RPC catchup proceeds (repair re-arms when a signal appears and the gap is within threshold)", repairCatchupDecisionTimeout)
				} else {
					mlog.Log.Warnf("repair catchup: no live-tip signal within %s; waiting for shreds (RPC block fetch is disabled)", repairCatchupDecisionTimeout)
				}
			}
		}
		if edge == 0 || edge < from {
			releaseHold()
			if !bs.sleepOrStop(ctx, 2*time.Second) {
				return
			}
			continue
		}

		gap := edge - from + 1
		// Shreds-only mode ignores the gap threshold entirely: repair is the
		// only catchup there is, whatever the distance.
		gapOK := !bs.rpcFallbackEnabled ||
			(bs.repairCatchupMaxGapSlots > 0 && gap <= bs.repairCatchupMaxGapSlots)
		eligible := bs.repairCatchupEligible(gapOK)

		// Shreds-only startup must be able to request its first shred. Live
		// traffic may belong to an epoch whose leader schedule is not loaded
		// until replay catches up. Keep the existing live-traffic gate when
		// repair is competing with RPC fallback.
		turbineReady := false
		var shredEdge uint64
		repairPeers := 0
		if eligible {
			shredEdge, _ = receiver.ShredEdges()
			repairPeers = receiver.Stats().Repair.Peers
			turbineReady = repairPeers > 0 && (shredEdge > 0 || !bs.rpcFallbackEnabled)
		}

		if !eligible || !turbineReady {
			if first && !gapOK {
				mlog.Log.Infof("repair catchup: gap %d slots (resume %d, tip %d) exceeds block.repair_catchup_max_gap_slots=%d; using RPC catchup — turbine repair takes over once the gap closes to within threshold", gap, from, edge, bs.repairCatchupMaxGapSlots)
			}
			if eligible && !turbineReady {
				if time.Since(lastNotReadyLog) >= 30*time.Second {
					lastNotReadyLog = time.Now()
					switch {
					case !bs.rpcFallbackEnabled:
						// A shreds-only boot needs a repair peer, even before
						// any live shred is accepted.
						stats := receiver.Stats()
						mlog.Log.Infof("catchup: WAITING for repair peers (received shreds: %v, peers: %d) — RPC block fetch is disabled (block.rpc_fallback=false); gap %d slots | receiver: packets %d, missing_leader %d, sig_err %d, parse_err %d",
							shredEdge > 0, repairPeers, gap, stats.Packets, stats.MissingLeaders, stats.SignatureErrors, stats.ParseErrors)
					case first:
						mlog.Log.Infof("catchup: gap %d slots is within the repair threshold (%d) — holding RPC block fetch and WAITING for turbine shreds (received: %v, repair peers: %d); RPC engages only if the gap outgrows the threshold", gap, bs.repairCatchupMaxGapSlots, shredEdge > 0, repairPeers)
					default:
						mlog.Log.Infof("repair catchup: gap %d slots is within threshold but turbine is not ready yet (shreds received: %v, repair peers: %d); RPC catchup continues — repair takes over when turbine wakes", gap, shredEdge > 0, repairPeers)
					}
				}
				// Within-threshold with turbine still waking: the RPC hold is
				// NOT released — shreds are the native path, so a fresh boot
				// waits for the native path instead of opening an RPC catchup it would
				// abandon. The far-behind rule stands watch: if the gap
				// outgrows the threshold while waiting, eligibility flips and
				// the branch below releases the hold to RPC.
				if !bs.sleepOrStop(ctx, 2*time.Second) {
					return
				}
				continue
			}
			releaseHold()
			if !bs.sleepOrStop(ctx, 2*time.Second) {
				return
			}
			continue
		}

		bs.repairCatchupFrom.Store(from)
		bs.repairCatchupUntil.Store(edge)
		receiver.SetRetentionFloor(from)
		releaseHold()
		mlog.Log.Infof("repair catchup: filling slots %d..%d (%d slots) via turbine repair", from, edge, gap)

		if done := bs.driveRepairCatchup(ctx, receiver, from, edge); done {
			if bs.rpcFallbackEnabled {
				return // near-tip machinery owns the stream; RPC covers any regression
			}
			// Shreds-only: STAY RESIDENT. If replay later falls behind again
			// (a monster-block streak, a promotion pause), this monitor is
			// the only catchup that exists — updateMode reopens the intake
			// gates and the next tick here re-arms the drive from the live
			// emission frontier. While near-tip holds, the loop idles at
			// poll cadence via the eligibility check. The completed handoff
			// is cleared so re-arming eligibility is not blocked by a stale
			// boot-time marker.
			bs.liveHandoffSlot.Store(0)
			bs.repairCatchupPending.Store(false)
			continue
		}
		// Stalled and fell back to RPC: loop back to monitoring (cooldown
		// gates the next arming).
	}
}

func (bs *BlockSource) clearSlotStateForLiveRefetch(slot uint64) {
	bs.slotStateMu.Lock()
	delete(bs.slotState, slot)
	delete(bs.inflightStart, slot)
	bs.slotStateMu.Unlock()
	bs.clearSlotErrors(slot)
}

// repairCatchupEligible reports whether the resident repair monitor may own
// the current gap. An RPC-enabled source waits for an existing handoff to be
// torn down before changing catchup owners. A shreds-only source has no such
// owner change: its near-tip handoff is the live delivery path that repair
// must keep using while it fills a replay regression. Treating that handoff
// as an exclusion deadlocks the transition — updateMode re-arms repair, but
// the monitor idles forever even when the waiting slot is complete on disk.
func (bs *BlockSource) repairCatchupEligible(gapOK bool) bool {
	return gapOK &&
		(!bs.rpcFallbackEnabled || bs.liveHandoffSlot.Load() == 0) &&
		!bs.isNearTip.Load() &&
		bs.liveForceRPCUntil.Load() == 0 &&
		time.Now().Unix() >= bs.repairCatchupCooldownUntil.Load()
}

// repairCatchupEdge returns the best live-tip signal available: turbine's
// observed shred edge or the RPC confirmed tip, whichever is ahead.
func (bs *BlockSource) repairCatchupEdge(receiver *turbine.UDPReceiver) uint64 {
	edge, _ := receiver.ShredEdges()
	if tip := bs.confirmedTip.Load(); tip > edge {
		edge = tip
	}
	return edge
}

// sleepOrStop sleeps for d; false means the source is stopping.
func (bs *BlockSource) sleepOrStop(ctx context.Context, d time.Duration) bool {
	select {
	case <-bs.stopChan:
		return false
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// driveRepairCatchup slides the priority-repair window ahead of replay until
// replay reaches the (advancing) edge and near-tip mode engages (returns
// true). Turbine repair OWNS the gap: there is no timer that hands catchup to
// RPC. A stalled head is warned about loudly — with the repair
// request/response counters that say WHY ("no peers", "requests unanswered",
// "responses that never complete a block" all read differently) — but the
// node stays on repair. The only ways out (returns false; the monitor keeps
// watching and re-arms once the gap closes back under the threshold):
//   - The far-behind rule, re-evaluated continuously: replay has fallen more
//     than block.repair_catchup_max_gap_slots behind the live edge — the
//     operator's own "REALLY far behind" number, the one reason RPC is for.
//   - Stream loss (ctx cancellation; handleLiveShredStreamClosed cleans up).
//
// After a far-behind fallback, a PRODUCTIVE attempt (repair delivered blocks,
// replay just could not keep pace) re-arms on the short cooldown; a BARREN
// attempt (not one shred-path block produced) re-arms on the long one, so
// wholesale RPC can genuinely close the gap before repair is retried instead
// of oscillating at the threshold boundary.
func (bs *BlockSource) driveRepairCatchup(ctx context.Context, receiver *turbine.UDPReceiver, from, edge uint64) bool {
	ticker := time.NewTicker(repairCatchupPollInterval)
	defer ticker.Stop()
	statsAtArm := receiver.Stats()
	lastWaiting := uint64(0)
	lastProgress := time.Now()
	headShreds := -1           // distinct data shreds seen for the current head (-1 = none yet)
	headResets := 0            // shred-state resets performed on the current head
	headGrowthAt := time.Now() // last time the head's shred count increased
	var headCompletedAt time.Time
	headCompletedSlot := uint64(0)
	sawWindowFill := false
	lastResponses := statsAtArm.Repair.Responses + statsAtArm.Repair.LateResponses
	var lastStallWarn time.Time
	var lastStagedWarn time.Time
	lastStatusLog := time.Now()
	lastPeerTableLog := time.Now()
	hbPrev := statsAtArm
	hbPrevAt := time.Now()
	// QoS-throttle detector state: the observed serve-repair ban signature is
	// "requests keep flowing but responses freeze" — a sharp collapse in the
	// ANSWER rate (timely + late) while we are still pushing hard. Low timely%
	// alone is NOT the signal (a resume-gap catchup legitimately sees ~95%
	// timeouts when only a few peers still retain old shreds; a latency spike
	// shifts timely to late); the signal is the DROP from a previously-healthy
	// answer rate. prevAnswerRate carries last interval's (timely+late)/s to
	// spot that collapse; qosThrottleIntervals counts trips.
	prevAnswerRate := 0.0
	qosThrottleIntervals := 0
	previousRepairGap := uint64(0)
	havePreviousRepairGap := false
	// AIMD rate controller: active only when the operator has NOT pinned
	// block.repair_max_requests_per_second — an explicit knob is a decision,
	// the default is a starting point. Steps the ceiling up while the peer
	// set demonstrably absorbs it, snaps back to the default on degradation.
	autoRate := 0
	if bs.repairMaxRequestsPerSecond == 0 {
		autoRate = turbine.DefaultRepairRequestsPerSecond
	}

	for {
		select {
		case <-bs.stopChan:
			return true
		case <-ctx.Done():
			// Stream died mid-catchup; handleLiveShredStreamClosed cleans up.
			return true
		case <-ticker.C:
		}

		bs.reorderMu.Lock()
		waiting := bs.nextSlotToSend
		bs.reorderMu.Unlock()
		if waiting == 0 {
			waiting = from
		}

		if waiting > edge {
			// Hand over to the normal near-tip machinery only once it is
			// actually engaged — dropping the catchup flag earlier would
			// close the decode/ingest gates for the second or two the tip
			// hysteresis needs, and live blocks arriving in that window
			// would be lost to completed-slot markers. Until then, chase
			// the advancing edge under the catchup gates.
			if bs.isNearTip.Load() {
				bs.deactivateRepairCatchup(receiver)
				mlog.Log.Infof("repair catchup complete: replay reached slot %d and near-tip mode is active", waiting-1)
				return true
			}
			if latest, _ := receiver.ShredEdges(); latest > edge {
				edge = latest
			}
			continue
		}

		// A certificate-skipped head has NO shreds to repair — apply the skip
		// and advance. The oracle is evidence-based only (explicit skip certs
		// or skips derived from finalized ancestry), never bare parent-link
		// inference; assembly-time footer-cert ingestion is what feeds it
		// during catchup.
		if bs.alpenglowSkipCertifiedFn != nil && bs.alpenglowSkipCertifiedFn(waiting) {
			if bs.applyCertifiedSkipToCatchupHead(waiting) {
				mlog.Log.Infof("repair catchup: slot %d is certificate-skipped — emitting the skip and advancing (no block exists to repair)", waiting)
			}
			continue
		}

		winEnd := waiting + repairCatchupWindowSlots - 1
		if winEnd > edge {
			winEnd = edge
		}
		windowBlocks := bs.turbineCatchupBlocksInWindow(waiting, winEnd)
		if windowBlocks > 0 {
			sawWindowFill = true
		}

		// Feed the global 5-minute stall watchdog while repair is
		// demonstrably exchanging data: a head-of-line block on one slot is
		// a wait (the configured shreds-only behavior), not node death. Late
		// answers count too — a node living on slower-than-timeout responses
		// is making real progress, not dying. If even repair traffic ceases,
		// the watchdog fires as before.
		if repairNow := receiver.Stats().Repair; repairNow.Responses+repairNow.LateResponses != lastResponses {
			lastResponses = repairNow.Responses + repairNow.LateResponses
			bs.lastProgress.Store(time.Now().Unix())
		}

		// The far-behind rule, re-evaluated live against the ADVANCING edge:
		// the one and only condition that hands catchup back to RPC. In
		// shreds-only mode there is nothing to hand it to — repair keeps
		// going at any distance.
		if liveEdge := bs.repairCatchupEdge(receiver); bs.rpcFallbackEnabled && liveEdge > waiting && liveEdge-waiting > bs.repairCatchupMaxGapSlots {
			repair := receiver.Stats().Repair
			bs.deactivateRepairCatchup(receiver)
			bs.forceRPCForLiveGap(waiting, 0, 0, 0)
			repairAtArm := statsAtArm.Repair
			cooldown := repairCatchupReArmCooldown
			note := ""
			if !sawWindowFill {
				cooldown = repairCatchupBarrenCooldown
				note = "; repair produced nothing this attempt, so the retry waits longer"
			}
			bs.repairCatchupCooldownUntil.Store(time.Now().Add(cooldown).Unix())
			mlog.Log.Warnf("repair catchup: replay fell %d slots behind the live edge (block.repair_catchup_max_gap_slots=%d) — handing catchup to RPC%s | repair since arming: requests +%d, responses +%d, timeouts +%d, peers %d (responding %d) (repair re-arms once the gap is back under threshold, after %s)",
				liveEdge-waiting, bs.repairCatchupMaxGapSlots, note,
				repair.Requests-repairAtArm.Requests, repair.Responses-repairAtArm.Responses,
				repair.Timeouts-repairAtArm.Timeouts, repair.Peers, repair.RespondingPeers, cooldown)
			return false
		}

		if waiting != lastWaiting {
			lastWaiting = waiting
			lastProgress = time.Now()
			headShreds = -1
			headResets = 0
			headGrowthAt = time.Now()
		} else {
			if obs, ok := receiver.ShredObservation(waiting); ok {
				if obs.DataShreds > headShreds {
					headGrowthAt = time.Now()
				}
				headShreds = obs.DataShreds
			}
			stalled := time.Since(lastProgress)
			headStalled := time.Since(headGrowthAt)

			// Stuck-head self-heal is deliberately evidence-based. A partial
			// slot with no assembly errors is not poisoned merely because its
			// last few data shreds are slow or temporarily unavailable. Purging
			// that state discards thousands of verified shreds and makes a
			// scarcity stall start over from zero. Only recorded assembly/decode
			// failures can trigger a purge; a COMPLETED head and a head whose
			// shred count is still growing remain protected as before.
			errCount, lastErr := receiver.SlotAssemblyErrors(waiting)
			headDetail, haveHeadDetail := receiver.HeadShredDetail(waiting)
			resetForAssemblyFailure, completeDecodeFailure := shouldResetRepairHead(
				errCount, headDetail, haveHeadDetail, stalled, headStalled, headResets)
			if resetForAssemblyFailure &&
				!receiver.SlotCompleted(waiting) {
				if lastErr == "" {
					lastErr = "none"
				}
				headResets++
				detail := headShredDetailString(receiver, waiting)
				heldShreds := max(headShreds, 0)
				stalledFor := headStalled.Round(time.Second)
				receiver.ResetSlotAndDiscardSpool(waiting)
				// The next tick observes the newly empty state. Do not compare it
				// with the pre-purge count or reuse the old growth deadline: doing
				// so consumed both bounded reset attempts back-to-back in one live
				// incident before a single repair response could arrive.
				headShreds = -1
				headGrowthAt = time.Now()
				reason := fmt.Sprintf("%d shreds held with no growth for %s", heldShreds, stalledFor)
				if completeDecodeFailure {
					reason = "the complete contiguous shred set failed deterministic block decoding"
				}
				mlog.Log.Warnf("repair catchup: purging shred state and spool for stuck head slot %d (attempt %d/%d) — %s (assembly errors: %d, latest: %s) | %s; re-repairing from scratch",
					waiting, headResets, repairCatchupMaxHeadResets, reason, errCount, lastErr, detail)
			}

			// A stalled head stays on repair — loudly. The counters make the
			// failure mode readable while the node keeps asking peers.
			// A completed-slot marker on the waiting head means the block
			// ASSEMBLED. Three sub-cases, and only one is stale:
			//   staged   -> assembled-but-blocked on handoff arming; resetting
			//               would rebuild the same block forever — log WHY it
			//               is not emitting instead
			//   buffered -> the emit loop owns it; nothing to do
			//   neither  -> the block was genuinely lost (dropped after
			//               assembly); clear the marker so repair can re-fetch
			if !receiver.SlotCompleted(waiting) {
				headCompletedSlot = 0
			} else if headCompletedSlot != waiting {
				// First sighting: give an in-flight handoff drain a moment —
				// the block legitimately sits in the result queue (neither
				// staged nor buffered) for a tick or two. Classify only if
				// the marker persists.
				headCompletedSlot = waiting
				headCompletedAt = time.Now()
			} else if time.Since(headCompletedAt) >= time.Second {
				staged := bs.stagedLiveBlock(waiting)
				pendingDelivery := receiver.BlockPendingDelivery(waiting) || bs.liveDeliveryInFlight(waiting)
				bs.reorderMu.Lock()
				buffered := bs.reorderBuffer[waiting] != nil
				emittedAnchor := bs.lastEmittedBlockSlot
				bs.reorderMu.Unlock()
				switch {
				case pendingDelivery:
					// The receiver/ordered-emitter pipeline owns it. Under a
					// repair burst these bounded queues can legitimately hold a
					// completed block for several seconds; resetting here creates
					// a duplicate re-repair and turns queueing into a false stall.
				case staged != nil:
					if time.Since(lastStagedWarn) >= 30*time.Second {
						lastStagedWarn = time.Now()
						anchor := emittedAnchor
						if anchor == 0 {
							if lastExecuted := bs.lastExecutedSlot.Load(); lastExecuted != 0 {
								anchor = lastExecuted
							} else if bs.startSlot > 0 {
								anchor = bs.startSlot - 1
							}
						}
						mlog.Log.Warnf("repair catchup: head slot %d is ASSEMBLED and staged but not emitting — staged parent %d, runway anchor %d, handoff_slot %d, alpenglow_block_id %v; waiting on handoff arming",
							waiting, staged.SourceParentSlot, anchor, bs.liveHandoffSlot.Load(), staged.HasAlpenglowBlockID)
					}
				case buffered:
					// Emit loop owns it.
				default:
					// Reset BOTH halves of delivery state. Resetting only the
					// receiver clears its completed marker, but slotDone in the
					// ordered emitter then classifies every reassembled block as
					// a duplicate and drops it, recreating the marker forever.
					bs.clearSlotStateForLiveRefetch(waiting)
					receiver.ResetSlot(waiting)
					mlog.Log.Warnf("repair catchup: head slot %d carried a completed-slot marker but the block is in neither staging nor the buffer (lost after assembly) — cleared; repair re-fetches the slot", waiting)
				}
			}

			if stalled >= repairCatchupStallWarnEvery && time.Since(lastStallWarn) >= repairCatchupStallWarnEvery {
				lastStallWarn = time.Now()
				stats := receiver.Stats()
				repair := stats.Repair
				behind := uint64(0)
				if liveEdge := bs.repairCatchupEdge(receiver); liveEdge > waiting {
					behind = liveEdge - waiting
				}
				rpcNote := fmt.Sprintf("RPC takes over only if replay falls %d slots behind the edge; currently %d", bs.repairCatchupMaxGapSlots, behind)
				if !bs.rpcFallbackEnabled {
					rpcNote = fmt.Sprintf("RPC block fetch is disabled; currently %d behind the edge", behind)
				}
				errCount, lastErr := receiver.SlotAssemblyErrors(waiting)
				if lastErr == "" {
					lastErr = "none"
				}
				hint := ""
				if stalled > 10*time.Minute {
					if bs.rpcFallbackEnabled {
						hint = " | HINT: if peers no longer retain shreds this old (responding count near zero), restart with --bootstrap new-snapshot or allow the configured RPC fallback"
					} else {
						hint = " | HINT: if peers no longer retain shreds this old (responding count near zero), restart with --bootstrap new-snapshot"
					}
				}
				mlog.Log.Warnf("repair catchup: no progress at slot %d for %s — staying on turbine repair (%s) | head shreds held %d (assembly errors %d, latest: %s) | %s | window blocks %d | repair since arming: requests +%d, responses +%d (late +%d), timeouts +%d, timeout %dms (avg resp %dms), peers %d (responding %d) | receiver drops since arming: ignored_old +%d, missing_leader +%d, sig_err +%d, parse_err +%d, non_canonical +%d (last: slot %d) | evicted_slots +%d, active_slots %d | repair pings +%d, pongs +%d, send_errors +%d%s",
					waiting, stalled.Round(time.Second), rpcNote,
					max(headShreds, 0), errCount, lastErr, headShredDetailString(receiver, waiting), windowBlocks,
					repair.Requests-statsAtArm.Repair.Requests, repair.Responses-statsAtArm.Repair.Responses,
					repair.LateResponses-statsAtArm.Repair.LateResponses,
					repair.Timeouts-statsAtArm.Repair.Timeouts, repair.TimeoutMillis, repair.AvgResponseMillis,
					repair.Peers, repair.RespondingPeers,
					stats.IgnoredOldShreds-statsAtArm.IgnoredOldShreds, stats.MissingLeaders-statsAtArm.MissingLeaders,
					stats.SignatureErrors-statsAtArm.SignatureErrors, stats.ParseErrors-statsAtArm.ParseErrors,
					stats.NonCanonicalBlockIDs-statsAtArm.NonCanonicalBlockIDs, stats.LastNonCanonicalSlot,
					stats.EvictedSlots-statsAtArm.EvictedSlots, stats.ActiveSlots,
					repair.Pings-statsAtArm.Repair.Pings, repair.Pongs-statsAtArm.Repair.Pongs,
					repair.Errors-statsAtArm.Repair.Errors, hint)
			}
		}

		// Slide the window: release retention behind replay, keep repair
		// pressure on the slots immediately ahead of it, and keep the disk
		// spool hydrating a prefetch window AHEAD of the head so emission
		// never waits on a disk load.
		receiver.SetRetentionFloor(waiting)
		receiver.PrioritizeRepairRange(waiting, winEnd)
		receiver.SetHydrationWindow(waiting, waiting+repairCatchupLiveDeliverWindow)

		// Post-handoff, far-future live blocks stage instead of flooding
		// the reorder buffer; hand the ones replay is approaching to the
		// emitter now.
		if bs.liveHandoffSlot.Load() != 0 {
			bs.drainStagedCatchupBlocks(waiting, waiting+repairCatchupLiveDeliverWindow)
		}

		// Repair-health heartbeat to the FILE log only: whether requests go
		// out, whether responses come back, and whether the window is
		// actually filling — the questions a stalled-catchup postmortem asks.
		// Interval rates (this heartbeat vs the previous one) sit next to
		// the cumulative deltas: rates answer "how is it going NOW".
		if time.Since(lastStatusLog) >= 15*time.Second {
			lastStatusLog = time.Now()
			hb := receiver.Stats()
			repair := hb.Repair
			interval := time.Since(hbPrevAt).Seconds()
			if interval <= 0 {
				interval = 1
			}
			dReq := repair.Requests - hbPrev.Repair.Requests
			dResp := repair.Responses - hbPrev.Repair.Responses
			dLate := repair.LateResponses - hbPrev.Repair.LateResponses
			dTmo := repair.Timeouts - hbPrev.Repair.Timeouts
			// USEFUL is total distinct repair throughput: shreds accepted into
			// in-RAM assembly PLUS those written to the catchup spool (disk-bound
			// far slots). During deep catchup most repair lands on disk, so the
			// spool term is what keeps this from reading ~0 while repair carries
			// the whole gap.
			dUseful := (hb.UsefulRepairShreds - hbPrev.UsefulRepairShreds) +
				(hb.SpooledRepairShreds - hbPrev.SpooledRepairShreds)
			timelyPct := uint64(100)
			if resolved := dResp + dTmo; resolved > 0 {
				timelyPct = 100 * dResp / resolved
			}
			// QoS-throttle detection: sending hard, WAS being served, and the
			// ANSWER rate (timely + late) just collapsed. Using total answers,
			// not timely-only, is deliberate: a latency spike shifts responses
			// from timely to late without a real throttle, so timely-only would
			// false-fire — but total answers only collapses when peers actually
			// stop responding. See qosThrottleSuspected / the repairQoS* thresholds.
			sendRate := float64(dReq) / interval
			respRate := float64(dResp) / interval // timely, for the heartbeat display
			answerRate := float64(dResp+dLate) / interval
			qosSuspect := qosThrottleSuspected(sendRate, prevAnswerRate, answerRate)
			if qosSuspect {
				qosThrottleIntervals++
			}
			headMissing := int64(-1)
			if d, ok := receiver.HeadShredDetail(waiting); ok && d.HaveLast {
				headMissing = int64(d.LastIndex) + 1 - int64(d.DataShreds)
			}
			liveEdge := bs.repairCatchupEdge(receiver)
			currentRepairGap := uint64(0)
			if liveEdge > waiting {
				currentRepairGap = liveEdge - waiting
			}
			gapGrowing := havePreviousRepairGap && currentRepairGap > previousRepairGap+repairAutoGapGrowthSlots
			// A growing gap is a repair-pressure signal only when replay is
			// actually running out of assembled runway. If the head is complete
			// and blocks are buffered ahead, execution is the limiter and a larger
			// repair window would only grow UDP/spool pressure.
			repairStarved := headMissing > 0 || windowBlocks == 0
			mlog.NamedFilef("catchup", "repair catchup status: head %d (in-flight %d, missing %d), edge %d, gap %d, window blocks %d | interval: send %.0f/s, resp %.0f/s, USEFUL %.0f shreds/s, timely %d%% | since arming: requests +%d, responses +%d (late +%d), timeouts +%d, timeout %dms (avg resp %dms), peers %d (responding %d) | hydration: slots +%d (complete from disk +%d) | sigverify: ed25519 +%d, cached +%d | qos: throttle-suspect intervals %d",
				waiting, receiver.RepairOutstandingForSlot(waiting), headMissing, liveEdge, currentRepairGap, windowBlocks,
				sendRate, respRate, float64(dUseful)/interval, timelyPct,
				repair.Requests-statsAtArm.Repair.Requests, repair.Responses-statsAtArm.Repair.Responses,
				repair.LateResponses-statsAtArm.Repair.LateResponses,
				repair.Timeouts-statsAtArm.Repair.Timeouts, repair.TimeoutMillis, repair.AvgResponseMillis,
				repair.Peers, repair.RespondingPeers,
				hb.HydratedSlots-statsAtArm.HydratedSlots, hb.HydratedFromDisk-statsAtArm.HydratedFromDisk,
				hb.SigVerifies-statsAtArm.SigVerifies, hb.SigVerifyCached-statsAtArm.SigVerifyCached, qosThrottleIntervals)

			// The throttle event itself goes to the terminal (not just the file):
			// it is the actionable signal that the aggressive rate has tripped a
			// serve-repair ban, and it tells the operator exactly which knob to turn.
			if qosSuspect {
				mlog.Log.Warnf("repair catchup: POSSIBLE serve-repair QoS THROTTLE at slot %d — sending %.0f/s but answers (timely+late) collapsed %.0f/s -> %.0f/s (timely %d%%, responding %d/%d, avg resp %dms). Peers may be rate-banning us; the auto rate controller is backing off — pin a lower block.repair_max_requests_per_second if this persists.",
					waiting, sendRate, prevAnswerRate, answerRate, timelyPct, repair.RespondingPeers, repair.Peers, repair.AvgResponseMillis)
			}

			// AIMD rate control (active only when the operator has NOT pinned the
			// rate). Additive increase toward repairAutoRateMax while the peer set
			// cleanly absorbs the current rate; MULTIPLICATIVE decrease on a
			// throttle signal (the QoS detector firing, or timely collapsing) so
			// it actually recovers instead of only warning. The backoff floor is
			// intentionally below the starting rate: otherwise a throttle at the
			// default ceiling can only log and never reduce load.
			if autoRate > 0 {
				utilized := float64(dReq)/interval >= 0.6*float64(autoRate)
				// A FRACTION of the live peers answering — works on a small
				// validator set, unlike the old fixed >=80 gate that never fired
				// on a ~62-peer cluster (so the rate never moved).
				healthyPeers := repair.Peers > 0 && repair.RespondingPeers*10 >= repair.Peers*8
				switch {
				case qosSuspect || timelyPct < 60:
					if autoRate > repairAutoRateMin {
						next := autoRate / 2
						if next < repairAutoRateMin {
							next = repairAutoRateMin
						}
						if qosSuspect {
							mlog.NamedFilef("catchup", "repair rate: throttle-suspect — backing off %d/s -> %d/s", autoRate, next)
						} else {
							mlog.NamedFilef("catchup", "repair rate: timely fell to %d%% — backing off %d/s -> %d/s", timelyPct, autoRate, next)
						}
						autoRate = next
						receiver.SetRepairRequestRate(autoRate)
					}
				case shouldIncreaseRepairRate(timelyPct, healthyPeers, utilized, gapGrowing, repairStarved) && autoRate < repairAutoRateMax:
					autoRate += repairAutoRateStep
					if autoRate > repairAutoRateMax {
						autoRate = repairAutoRateMax
					}
					receiver.SetRepairRequestRate(autoRate)
					reason := "request ceiling utilized"
					if gapGrowing && !utilized {
						reason = fmt.Sprintf("replay gap grew %d -> %d slots while admission was below the send ceiling", previousRepairGap, currentRepairGap)
					}
					mlog.NamedFilef("catchup", "repair rate: stepping up to %d/s (%s; timely %d%%, responding %d/%d) — backs off automatically on a throttle signal", autoRate, reason, timelyPct, repair.RespondingPeers, repair.Peers)
				}
			}
			previousRepairGap = currentRepairGap
			havePreviousRepairGap = true
			prevAnswerRate = answerRate
			hbPrev = hb
			hbPrevAt = time.Now()
		}
		// Peer service table to its own file on a slower cadence: which
		// peers actually answer, how fast, and their rolling quality score
		// — the ground truth behind responder-ranked selection. The compact
		// aggregate goes to catchup.log alongside it.
		if time.Since(lastPeerTableLog) >= 60*time.Second {
			lastPeerTableLog = time.Now()
			logRepairPeerTable(receiver, fmt.Sprintf("catchup head %d", waiting))
			if quality := repairPeerQualityLine(receiver); quality != "" {
				mlog.NamedFilef("catchup", "peer quality: %s", quality)
			}
		}
	}
}

// shouldResetRepairHead distinguishes corrupt assembly state from an ordinary
// incomplete slot. Time without growth is only meaningful after the assembler
// itself recorded an error; otherwise Turbine repair must retain every verified
// shred and keep requesting the holes.
func shouldResetRepairHead(errCount int, detail turbine.HeadShredDetail, haveDetail bool, stalled, headStalled time.Duration, headResets int) (reset, completeDecodeFailure bool) {
	if errCount <= 0 || headResets >= repairCatchupMaxHeadResets {
		return false, false
	}
	completeDecodeFailure = haveDetail && detail.HaveLast &&
		detail.ContiguousThrough == int64(detail.LastIndex) &&
		detail.DataShreds == int(detail.LastIndex)+1
	if completeDecodeFailure && stalled >= repairCatchupDecodeErrorResetAfter {
		return true, true
	}
	genericPoisonTimeout := stalled > time.Duration(headResets+1)*repairCatchupHeadResetAfter &&
		headStalled >= repairCatchupHeadGrowthGrace
	return genericPoisonTimeout, false
}

// headShredDetailString renders the stuck-head completion picture: how far
// the contiguous prefix reaches, whether the block-end shred registered, and
// the flags of the highest held shred. This is the line that separates
// "missing tail" from "end flag never registered" from "gappy holdings" —
// the three remaining explanations for a head that holds ~a whole block yet
// never completes.
func headShredDetailString(receiver *turbine.UDPReceiver, slot uint64) string {
	d, ok := receiver.HeadShredDetail(slot)
	if !ok {
		return "head detail: no live shred state"
	}
	lastDesc := "?"
	if d.HaveLast {
		lastDesc = fmt.Sprintf("%d", d.LastIndex)
	}
	return fmt.Sprintf("head detail: max_idx %d, contig_through %d, have_last %v (last_idx %s), top_flags 0x%02x recovered=%v",
		d.MaxIndex, d.ContiguousThrough, d.HaveLast, lastDesc, d.TopFlags, d.TopRecovered)
}

// turbineCatchupBlocksInWindow counts shred-path blocks the drive's priority
// window has produced that are visible but not yet emitted — staged runway
// blocks (pre-handoff blocks live in the staging buffer, NOT the reorder
// buffer) and reorder-buffered live-stream blocks alike. It distinguishes a
// productive drive from a barren one (cooldown choice on far-behind
// fallback) and feeds the stall warnings and health heartbeat. RPC-fetched
// blocks (pre-arm inflight leftovers) never count.
func (bs *BlockSource) turbineCatchupBlocksInWindow(from, to uint64) int {
	count := 0
	bs.liveStagingMu.Lock()
	for slot := range bs.liveStagingBuffer {
		if slot >= from && slot <= to {
			count++
		}
	}
	bs.liveStagingMu.Unlock()
	bs.reorderMu.Lock()
	for slot, blk := range bs.reorderBuffer {
		if blk != nil && blk.FromLiveStream && slot >= from && slot <= to {
			count++
		}
	}
	bs.reorderMu.Unlock()
	return count
}
