package blockstream

import (
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

func newRepairCatchupTestSource(maxGap uint64) *BlockSource {
	return NewBlockSource(&BlockSourceOpts{
		SourceType:               BlockSourceTurbine,
		TurbineBindAddr:          "127.0.0.1:0",
		RepairCatchupMaxGapSlots: maxGap,
		StartSlot:                1_000,
	})
}

// Construction arms the pending flag so the RPC scheduler can never race the
// gap before the shred-edge decision resolves.
func TestRepairCatchupPendingArmsAtConstruction(t *testing.T) {
	bs := newRepairCatchupTestSource(1024)
	if !bs.repairCatchupPending.Load() {
		t.Fatalf("turbine source with a threshold must arm the pending flag")
	}

	if disabled := newRepairCatchupTestSource(0); disabled.repairCatchupPending.Load() {
		t.Fatalf("threshold 0 must not arm repair catchup")
	}

	rpcOnly := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceRpc, RepairCatchupMaxGapSlots: 1024, StartSlot: 1_000})
	if rpcOnly.repairCatchupPending.Load() {
		t.Fatalf("non-turbine sources must not arm repair catchup")
	}
}

// The gap baseline is the RESUME FRONTIER (startSlot = snapshot/durable slot),
// not lastExecutedSlot — which is 0 on a fresh bootstrap because replay has
// not executed its first block. This is the regression guard for that bug: a
// snapshot-bootstrap node (lastExecutedSlot == 0) must still gate RPC from the
// snapshot slot, not from slot 1.
func TestRepairCatchupBaselineIsResumeFrontierNotLastExecuted(t *testing.T) {
	const snapshotSlot = 5_000
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:               BlockSourceTurbine,
		TurbineBindAddr:          "127.0.0.1:0",
		RepairCatchupMaxGapSlots: 1024,
		StartSlot:                snapshotSlot, // replay resumes here; nothing executed yet
	})
	if bs.lastExecutedSlot.Load() != 0 {
		t.Fatalf("premise: lastExecutedSlot is 0 before the first block executes")
	}
	if got := bs.repairCatchupResumeFrontier(); got != snapshotSlot {
		t.Fatalf("resume frontier = %d, want the snapshot slot %d (not lastExecutedSlot+1)", got, snapshotSlot)
	}
	// Pending gate must sit at the snapshot slot, so RPC is held off the gap...
	if bs.shouldUseRPCForSlot(snapshotSlot) || bs.shouldUseRPCForSlot(snapshotSlot+500) {
		t.Fatalf("pending: RPC must hold off from the snapshot slot, not from slot 1")
	}
	// ...but NOT the entire history below it (the lastExecutedSlot+1=1 bug
	// would have suppressed everything).
	if !bs.shouldUseRPCForSlot(snapshotSlot - 1) {
		t.Fatalf("slots below the resume frontier keep normal RPC behavior")
	}
}

// The gap belongs to turbine repair: RPC is suppressed at/above the gate while
// pending and while active, and resumes when both clear (fallback).
func TestRepairCatchupSuppressesRPCOverGap(t *testing.T) {
	const resume = 1_000 // = StartSlot in the test helper
	bs := newRepairCatchupTestSource(1024)

	// Pending: gate tracks the resume frontier (startSlot).
	if bs.shouldUseRPCForSlot(resume) {
		t.Fatalf("pending: RPC must hold off the resume frontier")
	}
	if bs.shouldUseRPCForSlot(resume + 500) {
		t.Fatalf("pending: RPC must hold off the whole prospective gap")
	}
	if !bs.shouldUseRPCForSlot(resume - 1) {
		t.Fatalf("slots below the resume frontier keep normal RPC behavior")
	}

	// Active: gate is the armed gap start.
	bs.repairCatchupPending.Store(false)
	bs.repairCatchupFrom.Store(2_001)
	bs.repairCatchupUntil.Store(2_800)
	if bs.shouldUseRPCForSlot(2_001) || bs.shouldUseRPCForSlot(2_800) {
		t.Fatalf("active: RPC must not fetch the armed gap")
	}
	if !bs.shouldUseRPCForSlot(2_000) {
		t.Fatalf("slots below the gap keep normal RPC behavior")
	}

	// Fallback: everything clears, RPC owns catchup again.
	bs.deactivateRepairCatchup(nil)
	if !bs.shouldUseRPCForSlot(2_001) {
		t.Fatalf("after fallback, RPC catchup must resume")
	}
}

// Shreds-only mode (block.rpc_fallback=false, the shipped default): RPC never
// fetches blocks on a live-shred source — any slot, any distance, any state —
// and the live-gap recovery path must not arm the RPC force flags that would
// close the shred decode gates waiting for an RPC resume that cannot happen.
// Non-shred sources are unaffected: they ARE the RPC path. The repair monitor
// arms even with the gap threshold at 0, because repair is the only catchup
// there is.
func TestShredsOnlyModeGatesAllRPCBlockFetch(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:               BlockSourceTurbine,
		TurbineBindAddr:          "127.0.0.1:0",
		RepairCatchupMaxGapSlots: 1024,
		StartSlot:                1_000,
		DisableRPCBlockFetch:     true,
	})
	bs.repairCatchupPending.Store(false)
	for _, slot := range []uint64{1, 999, 1_000, 5_000, 900_000} {
		if bs.shouldUseRPCForSlot(slot) {
			t.Fatalf("shreds-only: slot %d must never be RPC-fetchable", slot)
		}
	}

	bs.forceRPCForLiveGap(1_500, 0, 0, 0)
	if bs.liveForceRPCUntil.Load() != 0 || bs.liveCooldownUntil.Load() != 0 {
		t.Fatalf("shreds-only: live-gap recovery must not arm RPC force flags")
	}

	rpcSrc := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceRpc, StartSlot: 1_000, DisableRPCBlockFetch: true})
	if !rpcSrc.shouldUseRPCForSlot(10) {
		t.Fatalf("source=rpc must keep fetching blocks regardless of rpc_fallback")
	}

	// Lightbringer is a live shred stream WITHOUT repair machinery: it needs
	// RPC for old/evicted shreds, so shreds-only mode must not gate it.
	lb := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceLightbringer, LightbringerEndpoint: "localhost:1", StartSlot: 1_000, DisableRPCBlockFetch: true})
	if !lb.rpcBlockFetchAllowed() {
		t.Fatalf("source=lightbringer must keep RPC block fetch despite rpc_fallback=false")
	}
	if !lb.shouldUseRPCForSlot(2_000) {
		t.Fatalf("source=lightbringer catchup slots must stay RPC-fetchable")
	}

	noThreshold := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceTurbine,
		TurbineBindAddr:      "127.0.0.1:0",
		StartSlot:            1_000,
		DisableRPCBlockFetch: true,
	})
	if !noThreshold.repairCatchupPending.Load() {
		t.Fatalf("shreds-only must arm the repair monitor even with threshold 0")
	}
}

// A shreds-only node keeps its Turbine handoff active at the live edge. If
// replay later slips across the catchup hysteresis, that SAME handoff must
// coexist with the repair drive: there is no RPC owner to hand the gap to.
// The observed failure had slot 3228975 complete in the disk spool, but the
// old handoff marker made the resident monitor idle forever after re-arming.
func TestShredsOnlyRepairRearmsAcrossActiveHandoff(t *testing.T) {
	const (
		lastExecuted = uint64(3_228_974)
		confirmedTip = uint64(3_229_039) // gap 65: crosses the 64-slot threshold
		handoffSlot  = uint64(3_228_738)
	)
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceTurbine,
		TurbineBindAddr:      "127.0.0.1:0",
		StartSlot:            lastExecuted + 1,
		DisableRPCBlockFetch: true,
	})
	bs.repairCatchupPending.Store(false)
	bs.isNearTip.Store(true)
	bs.lastExecutedSlot.Store(lastExecuted)
	bs.confirmedTip.Store(confirmedTip)
	bs.liveHandoffSlot.Store(handoffSlot)
	bs.liveStreamActive.Store(true)
	bs.liveStreamStarted.Store(true) // The simulated active handoff already owns its stream.

	bs.updateMode()

	if bs.isNearTip.Load() {
		t.Fatalf("gap regression must leave near-tip mode")
	}
	if !bs.repairCatchupPending.Load() {
		t.Fatalf("gap regression must re-arm repair catchup")
	}
	if !bs.repairCatchupEligible(true) {
		t.Fatalf("shreds-only repair must activate while its Turbine handoff remains active")
	}
	if got := bs.liveHandoffSlot.Load(); got != handoffSlot {
		t.Fatalf("re-arm must preserve the live delivery handoff: got %d, want %d", got, handoffSlot)
	}

	// RPC fallback retains the old ownership barrier: forceRPCForCatchup is
	// responsible for dismantling the handoff before repair may take over.
	bs.rpcFallbackEnabled = true
	if bs.repairCatchupEligible(true) {
		t.Fatalf("RPC-enabled repair must still wait for an active handoff to be dismantled")
	}
}

// A near-tip regression is a two-phase transition: updateMode publishes the
// pending bit immediately, then the resident repair monitor publishes the
// active range.  The old intake gates recognized only the second phase.  A
// block assembled during the pending interval was therefore dropped while
// the assembler retained its completed-slot marker, producing the live
// "lost after assembly" loop and forcing a full re-repair of the head slot.
func TestPendingRepairCatchupKeepsActiveHandoffBlocks(t *testing.T) {
	const (
		handoff = uint64(1_900)
		waiting = uint64(2_001)
	)
	bs := newRepairCatchupTestSource(1024)
	bs.isNearTip.Store(false)
	bs.repairCatchupPending.Store(true)
	bs.repairCatchupFrom.Store(0)
	bs.liveHandoffSlot.Store(handoff)
	bs.liveStreamActive.Store(true)
	bs.nextSlotToSend = waiting

	if !bs.shouldDecodeLiveSlot(waiting) {
		t.Fatalf("pending catchup must keep post-handoff decode admission open")
	}

	head := &b.Block{Slot: waiting, FromLiveStream: true, SourceParentSlot: waiting - 1}
	if !bs.ingestLiveShredBlock(head) {
		t.Fatalf("pending catchup must accept the assembled head block")
	}
	select {
	case res := <-bs.resultQueue:
		if res.slot != waiting || res.block != head {
			t.Fatalf("pending catchup delivered %+v, want head slot %d", res, waiting)
		}
		bs.finishLiveDelivery(res.slot) // simulate emitOrderedBlocks intake
	default:
		t.Fatalf("pending catchup dropped the assembled head block")
	}

	farSlot := waiting + repairCatchupLiveDeliverWindow + 1
	far := &b.Block{Slot: farSlot, FromLiveStream: true, SourceParentSlot: farSlot - 1}
	if !bs.ingestLiveShredBlock(far) {
		t.Fatalf("pending catchup must accept a far-edge block")
	}
	if got := bs.stagedLiveBlock(farSlot); got != far {
		t.Fatalf("pending catchup far-edge block was not staged: got %p, want %p", got, far)
	}
	select {
	case res := <-bs.resultQueue:
		t.Fatalf("far-edge block bypassed bounded staging during pending catchup: slot %d", res.slot)
	default:
	}
}

func TestLostLiveBlockRefetchClearsEmitterDoneState(t *testing.T) {
	bs := newRepairCatchupTestSource(1024)
	const slot = uint64(2_001)
	bs.slotState[slot] = slotDone
	bs.inflightStart[slot] = time.Now()

	bs.clearSlotStateForLiveRefetch(slot)

	if _, ok := bs.slotState[slot]; ok {
		t.Fatal("slotDone survived live-block refetch reset and would drop the replacement as a duplicate")
	}
	if _, ok := bs.inflightStart[slot]; ok {
		t.Fatal("inflight timestamp survived live-block refetch reset")
	}
}

func TestRepairCatchupDoesNotPurgeSlowErrorFreeHead(t *testing.T) {
	// Live slot 3256396 held 8189/8192 verified data shreds with no assembly
	// errors. The old time-only heuristic purged it, throwing away the whole
	// valid set; the second fetch eventually assembled the exact same slot.
	detail := turbine.HeadShredDetail{
		DataShreds:        8189,
		MaxIndex:          8191,
		ContiguousThrough: 7905,
		HaveLast:          true,
		LastIndex:         8191,
	}
	reset, decodeFailure := shouldResetRepairHead(
		0, detail, true, 10*time.Minute, 5*time.Minute, 0)
	if reset || decodeFailure {
		t.Fatalf("an error-free partial head must retain verified shreds: reset=%v decode_failure=%v", reset, decodeFailure)
	}
}

func TestRepairCatchupPurgesOnlyErrorBackedPoison(t *testing.T) {
	partial := turbine.HeadShredDetail{
		DataShreds:        79,
		MaxIndex:          79,
		ContiguousThrough: 63,
		HaveLast:          false,
	}
	if reset, decodeFailure := shouldResetRepairHead(
		3, partial, true, 2*repairCatchupHeadResetAfter, repairCatchupHeadGrowthGrace, 0); !reset || decodeFailure {
		t.Fatalf("stalled head with recorded assembly errors must reset: reset=%v decode_failure=%v", reset, decodeFailure)
	}

	complete := turbine.HeadShredDetail{
		DataShreds:        80,
		MaxIndex:          79,
		ContiguousThrough: 79,
		HaveLast:          true,
		LastIndex:         79,
	}
	if reset, decodeFailure := shouldResetRepairHead(
		1, complete, true, repairCatchupDecodeErrorResetAfter, 0, 0); !reset || !decodeFailure {
		t.Fatalf("complete set with deterministic decode error must reset quickly: reset=%v decode_failure=%v", reset, decodeFailure)
	}

	if reset, _ := shouldResetRepairHead(
		1, partial, true, 10*time.Minute, 10*time.Minute, repairCatchupMaxHeadResets); reset {
		t.Fatal("error-backed resets must remain bounded")
	}
}

// The FIRST handoff of a fresh boot: nothing has emitted or executed yet, so
// the runway anchor falls back to startSlot-1 (the resume block). Without
// the fallback the handoff can never arm — no block parent-links to slot 0 —
// and a shreds-only catchup deadlocks AT BOOT with the head fully assembled
// and staged, since the first emission can only come from this very handoff.
// Root cause behind four consecutive live catchup stalls.
func TestHandoffArmsOnFreshResumeAnchor(t *testing.T) {
	bs := newRepairCatchupTestSource(1024) // StartSlot 1_000
	bs.repairCatchupPending.Store(false)
	bs.repairCatchupFrom.Store(1_000)
	bs.repairCatchupUntil.Store(1_800)

	head := &b.Block{Slot: 1_000, SourceParentSlot: 999, FromLiveStream: true}
	bs.bufferLiveStreamBlock(head)
	// A staged block BEYOND a gap (1001 missing): the payload must include
	// it anyway — the reorder buffer handles out-of-order, holes are the
	// repair drive's job. Handing over only the connected runway threw away
	// a whole prewarm spool at the first gap (observed live: first buffered
	// block 252 slots ahead of replay while ~190 collected blocks were
	// dropped for re-repair).
	beyondGap := &b.Block{Slot: 1_002, SourceParentSlot: 1_001, FromLiveStream: true}
	bs.bufferLiveStreamBlock(beyondGap)

	bs.maybePrepareLiveHandoff()

	if got := bs.liveHandoffSlot.Load(); got != 1_000 {
		t.Fatalf("handoff must arm at the resume head on a fresh boot (anchor = startSlot-1); handoff_slot=%d", got)
	}
	gotSlots := map[uint64]bool{}
	for {
		select {
		case res := <-bs.resultQueue:
			if res.block == nil {
				t.Fatalf("handoff payload must be blocks, got %+v", res)
			}
			gotSlots[res.slot] = true
			continue
		default:
		}
		break
	}
	if !gotSlots[1_000] || !gotSlots[1_002] {
		t.Fatalf("armed handoff must enqueue ALL staged blocks at/above the handoff slot (gaps included), got %v", gotSlots)
	}
}

// A certificate-skipped catchup head must be EMITTED as a skip, not merely
// crossed by the handoff runway: pre-handoff nothing feeds the results loop,
// so the drive injects a synthetic certified-skip result that the standard
// emission machinery consumes. This is the half of skipped-gap handling the
// runway test alone did not prove (observed live: an on-chain-skipped head
// slot with zero shreds stalled catchup for minutes).
func TestApplyCertifiedSkipToCatchupHead(t *testing.T) {
	bs := newRepairCatchupTestSource(1024)
	bs.reorderMu.Lock()
	bs.nextSlotToSend = 2_001
	bs.reorderMu.Unlock()

	if !bs.applyCertifiedSkipToCatchupHead(2_001) {
		t.Fatalf("skip must apply to the waiting head")
	}
	bs.reorderMu.Lock()
	certified := bs.alpenglowCertifiedSkips[2_001]
	bs.reorderMu.Unlock()
	if !certified {
		t.Fatalf("head must carry the CERTIFIED skip marker (post-handoff logic trusts it)")
	}
	select {
	case res := <-bs.resultQueue:
		if res.slot != 2_001 || !res.skipped || res.rpcIdx != -1 {
			t.Fatalf("expected a synthetic certified-skip result for the head, got %+v", res)
		}
	default:
		t.Fatalf("the results loop must be nudged — pre-handoff nothing else feeds it")
	}

	// Idempotent: a second application (skip already recorded) is a no-op.
	bs.reorderMu.Lock()
	bs.skippedSlots[2_001] = true
	bs.reorderMu.Unlock()
	if bs.applyCertifiedSkipToCatchupHead(2_001) {
		t.Fatalf("an already-recorded skip must not re-apply")
	}

	// Only the WAITING slot qualifies: skips beyond the head stay owned by
	// the ordinary decision machinery.
	if bs.applyCertifiedSkipToCatchupHead(2_010) {
		t.Fatalf("non-head slots must not be force-skipped by the drive")
	}
}

// No timer-based RPC exception exists inside the repair gate: while the
// drive is active, the ENTIRE gap — head included — is repair-owned. RPC
// takes catchup back only via the far-behind rule (drive-internal) or
// deactivation. This is the "repair owns the gap, no RPC creep" contract.
func TestRepairCatchupGateHasNoRPCException(t *testing.T) {
	bs := newRepairCatchupTestSource(1024)
	bs.repairCatchupPending.Store(false)
	bs.repairCatchupFrom.Store(2_001)
	bs.repairCatchupUntil.Store(2_800)

	for _, slot := range []uint64{2_001, 2_050, 2_800, 3_500} {
		if bs.shouldUseRPCForSlot(slot) {
			t.Fatalf("active drive: slot %d at/above the gate must stay repair-owned", slot)
		}
	}
	if !bs.shouldUseRPCForSlot(2_000) {
		t.Fatalf("slots below the gap keep normal RPC behavior")
	}

	bs.deactivateRepairCatchup(nil)
	if !bs.shouldUseRPCForSlot(2_050) {
		t.Fatalf("after deactivation, RPC catchup must resume")
	}
}

// During catchup the handoff/decode gates bypass near-tip: gap blocks decode,
// buffer, and arm the handoff at the gap start even though replay is far
// behind the live edge.
func TestRepairCatchupBypassesNearTipGates(t *testing.T) {
	bs := newRepairCatchupTestSource(1024)
	bs.lastExecutedSlot.Store(2_000)
	bs.repairCatchupPending.Store(false)
	bs.repairCatchupFrom.Store(2_001)
	bs.repairCatchupUntil.Store(2_800)

	if bs.isNearTip.Load() {
		t.Fatalf("test premise: far from tip")
	}
	if !bs.shouldDecodeLiveSlot(2_050) {
		t.Fatalf("gap blocks must decode during repair catchup despite !nearTip")
	}
	if bs.shouldDecodeLiveSlot(1_500) {
		t.Fatalf("slots below the gap must not decode")
	}

	// Post-handoff: decode continues for slots >= handoff without nearTip.
	bs.liveHandoffSlot.Store(2_001)
	if !bs.shouldDecodeLiveSlot(2_100) {
		t.Fatalf("post-handoff decode must bypass nearTip during repair catchup")
	}

	// prepare path: nearTip + replay-gap guards bypassed while active.
	bs.liveHandoffSlot.Store(0)
	if _, _, prepared := bs.prepareLiveHandoff(2_001, 2_000); prepared {
		// Not prepared without buffered runway blocks — but it must get PAST
		// the nearTip/replay-gap guards and fail only on the empty runway.
		t.Fatalf("cannot prepare with an empty buffer")
	}

	// Deactivated: the same call is rejected by the nearTip guard again
	// (identical empty-runway input, different guard outcome is unobservable —
	// so assert via shouldDecode, which flips with the flag).
	bs.deactivateRepairCatchup(nil)
	if bs.shouldDecodeLiveSlot(2_050) {
		t.Fatalf("after deactivation, decode gating must revert to near-tip rules")
	}
}

// Catchup stall rescue: when RPC catchup is head-of-line blocked, slots in
// the armed rescue window decode and deliver straight to the emitter — but
// ONLY inside the window, only pre-handoff, only outside near-tip.
func TestCatchupStallRescueGates(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:      BlockSourceTurbine,
		TurbineBindAddr: "127.0.0.1:0",
		StartSlot:       1_000,
	})

	// Inactive: normal catchup gating (slot far from tip is dropped).
	if bs.shouldDecodeLiveSlot(1_005) {
		t.Fatalf("no rescue armed: catchup slots must not decode")
	}

	// Arm the window [1005, 1020].
	bs.rescueUntil.Store(1_020)
	bs.rescueFrom.Store(1_005)

	if !bs.catchupRescueCovers(1_005) || !bs.catchupRescueCovers(1_020) {
		t.Fatalf("window bounds must be covered")
	}
	if bs.catchupRescueCovers(1_004) || bs.catchupRescueCovers(1_021) {
		t.Fatalf("outside the window must not be covered")
	}
	if !bs.shouldDecodeLiveSlot(1_010) {
		t.Fatalf("rescue-window slot must decode during catchup")
	}
	if bs.shouldDecodeLiveSlot(1_030) {
		t.Fatalf("slots beyond the window keep normal catchup gating")
	}

	// Delivery: an assembled block inside the window goes straight to the
	// emitter's result queue (rpcIdx -1 marks it live-sourced).
	blk := &b.Block{Slot: 1_006, FromLiveStream: true, SourceParentSlot: 1_005}
	if !bs.ingestLiveShredBlock(blk) {
		t.Fatalf("ingest must accept the rescued block")
	}
	select {
	case res := <-bs.resultQueue:
		if res.slot != 1_006 || res.rpcIdx != -1 || res.block != blk {
			t.Fatalf("rescued block must be delivered as a live result, got %+v", res)
		}
		if !bs.liveDeliveryInFlight(res.slot) {
			t.Fatalf("queued live result must remain explicitly owned before emitter intake")
		}
		bs.finishLiveDelivery(res.slot) // simulate emitOrderedBlocks intake
		if bs.liveDeliveryInFlight(res.slot) {
			t.Fatalf("emitter intake must release queued-delivery ownership")
		}
	default:
		t.Fatalf("rescued block must be in the result queue, not the staging buffer")
	}

	// Outside the window: staged in the runway buffer as before, not delivered.
	other := &b.Block{Slot: 1_030, FromLiveStream: true, SourceParentSlot: 1_029}
	// (1_030 fails shouldDecode in catchup, so the receiver would drop it
	// before ingest; simulate the near-tip staging path being off by calling
	// ingest directly — it must buffer, not deliver.)
	if !bs.ingestLiveShredBlock(other) {
		t.Fatalf("ingest must not error on non-rescue blocks")
	}
	select {
	case res := <-bs.resultQueue:
		t.Fatalf("non-rescue block must not be delivered directly, got slot %d", res.slot)
	default:
	}
}
