package blockstream

import (
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func waitForBlockSourceCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for block source condition")
}

// A rooted Alpenglow fork recovery starts a replacement BlockSource in the
// same process and reuses the configured TVU address. The abandoned source
// must synchronously release that socket first; otherwise every replacement
// receiver fails with "bind: address already in use" and replay stalls without
// ever attempting its first slot.
func TestStopReleasesTurbineSocketForForkReplay(t *testing.T) {
	seed, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("reserve turbine address: %v", err)
	}
	addr := seed.LocalAddr().String()
	if err := seed.Close(); err != nil {
		t.Fatalf("release reserved turbine address: %v", err)
	}

	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:      BlockSourceTurbine,
		TurbineBindAddr: addr,
		StartSlot:       100,
		EndSlot:         200,
	})
	bs.liveStreamWg.Add(1)
	go bs.runTurbineStream()
	waitForBlockSourceCondition(t, bs.liveStreamConnected.Load)

	// Model Stop racing with the scheduler's ordinary Start shutdown.
	bs.Stop()
	bs.Stop()
	done := make(chan struct{})
	go func() {
		bs.liveStreamWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("turbine stream did not stop and release ownership")
	}

	replacement, err := net.ListenUDP("udp", mustResolveUDPAddr(t, addr))
	if err != nil {
		t.Fatalf("replacement fork-replay receiver could not bind %s: %v", addr, err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatalf("close replacement turbine socket: %v", err)
	}
}

func TestStopUnblocksEmitterWhenForkReplayStopsConsuming(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceRpc,
		StartSlot:  100,
		EndSlot:    200,
	})
	for i := 0; i < cap(bs.streamChan); i++ {
		bs.streamChan <- &b.Block{Slot: uint64(i + 1)}
	}

	emitterDone := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(emitterDone)
	}()
	bs.resultQueue <- fetchResult{slot: 100, block: &b.Block{Slot: 100}}
	waitForBlockSourceCondition(t, func() bool {
		bs.reorderMu.Lock()
		defer bs.reorderMu.Unlock()
		return bs.lastEmittedBlockSlot == 100
	})

	// Replay has stopped reading streamChan while the emitter is trying to
	// publish one more block. Shutdown must still join instead of deadlocking.
	bs.Stop()
	select {
	case <-emitterDone:
	case <-time.After(time.Second):
		t.Fatal("ordered emitter stayed blocked after fork-replay shutdown")
	}
}

func mustResolveUDPAddr(t *testing.T, addr string) *net.UDPAddr {
	t.Helper()
	resolved, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatalf("resolve UDP address %q: %v", addr, err)
	}
	return resolved
}

func TestInjectLocalBlockReplacesBufferedNetworkBlock(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceRpc,
		StartSlot:  10,
		EndSlot:    12,
	})
	emitterDone := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(emitterDone)
	}()

	network := &b.Block{Slot: 11}
	bs.resultQueue <- fetchResult{slot: 11, block: network}
	waitForBlockSourceCondition(t, func() bool {
		bs.reorderMu.Lock()
		defer bs.reorderMu.Unlock()
		return bs.reorderBuffer[11] == network
	})

	local := &b.Block{Slot: 11}
	if !bs.InjectLocalBlock(local) {
		t.Fatal("local block injection failed")
	}
	waitForBlockSourceCondition(t, func() bool {
		bs.reorderMu.Lock()
		defer bs.reorderMu.Unlock()
		return bs.reorderBuffer[11] == local
	})

	bs.resultQueue <- fetchResult{slot: 10, block: &b.Block{Slot: 10}}
	if got := <-bs.streamChan; got.Slot != 10 {
		t.Fatalf("first emitted slot = %d, want 10", got.Slot)
	}
	if got := <-bs.streamChan; got != local || !got.FromLocalProduction {
		t.Fatalf("second emitted block = %#v, want locally produced block", got)
	}

	close(bs.resultQueue)
	<-emitterDone
}

func TestInjectLocalBlockReplacesProvisionalSkip(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceRpc,
		StartSlot:  20,
		EndSlot:    22,
	})
	emitterDone := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(emitterDone)
	}()

	bs.resultQueue <- fetchResult{slot: 21, skipped: true}
	waitForBlockSourceCondition(t, func() bool {
		bs.reorderMu.Lock()
		defer bs.reorderMu.Unlock()
		return bs.skippedSlots[21]
	})

	local := &b.Block{Slot: 21}
	if !bs.InjectLocalBlock(local) {
		t.Fatal("local block injection failed")
	}
	waitForBlockSourceCondition(t, func() bool {
		bs.reorderMu.Lock()
		defer bs.reorderMu.Unlock()
		return bs.reorderBuffer[21] == local && !bs.skippedSlots[21]
	})

	bs.resultQueue <- fetchResult{slot: 20, block: &b.Block{Slot: 20}}
	if got := <-bs.streamChan; got.Slot != 20 {
		t.Fatalf("first emitted slot = %d, want 20", got.Slot)
	}
	if got := <-bs.streamChan; got != local || got.IsSkipped {
		t.Fatalf("second emitted block = %#v, want local non-skipped block", got)
	}

	close(bs.resultQueue)
	<-emitterDone
}

func TestLightbringerBlockConnectsLocked(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceLightbringer,
		StartSlot:  100,
		EndSlot:    200,
	})

	bs.lastEmittedBlockSlot = 150

	if !bs.liveBlockConnectsLocked(&b.Block{Slot: 151, FromLiveStream: true, SourceParentSlot: 150}) {
		t.Fatalf("expected Lightbringer block with matching parent slot to connect")
	}
	if bs.liveBlockConnectsLocked(&b.Block{Slot: 151, FromLiveStream: true, SourceParentSlot: 149}) {
		t.Fatalf("expected Lightbringer block with mismatched parent slot to be rejected")
	}
	if bs.liveBlockConnectsLocked(&b.Block{Slot: 151, FromLiveStream: true, SourceParentSlot: 0}) {
		t.Fatalf("expected Lightbringer block without parent metadata to be rejected once an anchor exists")
	}
	if !bs.liveBlockConnectsLocked(&b.Block{Slot: 151, FromLiveStream: false}) {
		t.Fatalf("expected RPC block to pass through ancestry guard")
	}
}

func TestAlpenglowLiveBlockConnectsByExactParentID(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    100,
		EndSlot:                      200,
	})
	bs.lastEmittedBlockSlot = 150
	bs.lastEmittedAlpenglowBlockID = solana.Hash{1}
	bs.hasLastEmittedAlpenglowBlockID = true

	child := &b.Block{
		Slot:                      151,
		FromLiveStream:            true,
		SourceParentSlot:          150,
		HasAlpenglowParentBlockID: true,
		AlpenglowParentBlockID:    solana.Hash{2},
	}
	if bs.liveBlockConnectsLocked(child) {
		t.Fatal("same parent slot with a different Alpenglow parent ID connected")
	}
	child.AlpenglowParentBlockID = solana.Hash{1}
	if !bs.liveBlockConnectsLocked(child) {
		t.Fatal("exact Alpenglow parent ID did not connect")
	}
}

func TestAlpenglowCheckpointResumeSeedsSkipInferenceAnchor(t *testing.T) {
	anchorID := solana.Hash{0x31}
	childID := solana.Hash{0x36}
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    312,
		EndSlot:                      400,
		InitialAlpenglowBlockID:      anchorID,
		HasInitialAlpenglowBlockID:   true,
	})
	if bs.lastEmittedBlockSlot != 311 || !bs.hasLastEmittedAlpenglowBlockID || bs.lastEmittedAlpenglowBlockID != anchorID {
		t.Fatalf("resume anchor = slot %d id %s present=%v, want slot 311 id %s",
			bs.lastEmittedBlockSlot, bs.lastEmittedAlpenglowBlockID, bs.hasLastEmittedAlpenglowBlockID, anchorID)
	}
	bs.reorderBuffer[316] = &b.Block{
		Slot:                      316,
		FromLiveStream:            true,
		SourceParentSlot:          311,
		AlpenglowBlockID:          childID,
		HasAlpenglowBlockID:       true,
		AlpenglowParentBlockID:    anchorID,
		HasAlpenglowParentBlockID: true,
	}
	if !bs.synthesizeAlpenglowParentLinkedSkipsLocked() {
		t.Fatal("rooted resume anchor did not enable exact parent-linked skip inference")
	}
	for slot := uint64(312); slot <= 315; slot++ {
		if !bs.skippedSlots[slot] || !bs.liveSynthesizedSkips[slot] {
			t.Fatalf("slot %d was not marked as a provisional Alpenglow skip", slot)
		}
	}
}

func TestCurrentSourceSnapshotUsesTurbineSourceName(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:      BlockSourceTurbine,
		TurbineBindAddr: "127.0.0.1:0",
		StartSlot:       100,
		EndSlot:         200,
	})

	bs.isNearTip.Store(true)
	bs.liveStreamConnected.Store(true)
	bs.liveLastStreamSlot.Store(150)

	source, status, handoff := bs.currentSourceSnapshot()
	if source != "rpc" || handoff != 0 || !strings.Contains(status, "turbine connected") {
		t.Fatalf("expected pre-handoff turbine status, got source=%q status=%q handoff=%d", source, status, handoff)
	}

	bs.liveStreamActive.Store(true)
	bs.liveHandoffSlot.Store(151)

	source, status, handoff = bs.currentSourceSnapshot()
	if source != "turbine" || status != "turbine live stream" || handoff != 151 {
		t.Fatalf("expected active turbine status, got source=%q status=%q handoff=%d", source, status, handoff)
	}
}

func TestAlpenglowBlockIDHintsAreExplicitlyOptedIn(t *testing.T) {
	var blockID solana.Hash
	blockID[0] = 1

	classic := NewBlockSource(&BlockSourceOpts{
		SourceType:      BlockSourceTurbine,
		TurbineBindAddr: "127.0.0.1:0",
		StartSlot:       100,
		EndSlot:         200,
	})
	classic.SetKnownAlpenglowBlockID(101, blockID)
	if len(classic.knownAlpenglowBlockIDs) != 0 {
		t.Fatalf("expected classic turbine source to ignore Alpenglow block-id hints")
	}

	alpenglow := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    100,
		EndSlot:                      200,
	})
	alpenglow.SetKnownAlpenglowBlockID(101, blockID)
	if got := alpenglow.knownAlpenglowBlockIDs[101]; got != blockID {
		t.Fatalf("expected opted-in turbine source to retain Alpenglow block-id hint, got %v", got)
	}
}

func TestApplyAlpenglowDecisionLockedMarksCertifiedSkip(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    151,
		EndSlot:                      200,
		AlpenglowDecisionSource: func(anchorSlot uint64) (alpenglow.ChainDecision, bool) {
			if anchorSlot != 150 {
				t.Fatalf("anchorSlot = %d, want 150", anchorSlot)
			}
			return alpenglow.ChainDecision{
				Slot: 151,
				Kind: alpenglow.ChainDecisionKindSkip,
			}, true
		},
	})
	bs.isNearTip.Store(true)
	bs.liveStreamActive.Store(true)

	bs.reorderMu.Lock()
	changed := bs.applyAlpenglowDecisionLocked()
	bs.reorderMu.Unlock()

	if changed {
		t.Fatalf("expected skip decision to be emitted by normal skipped-slot branch")
	}
	if !bs.skippedSlots[151] {
		t.Fatalf("expected certified skip to mark waiting slot skipped")
	}
	if got := bs.stats.FetchSkipped.Load(); got != 1 {
		t.Fatalf("FetchSkipped = %d, want 1", got)
	}
}

// A certified skip applies even OUTSIDE active near-tip Turbine (RPC catchup /
// pre-handoff), so a certified-skipped slot is not re-run after block-source
// recreation on the post-switch re-replay.
func TestApplyAlpenglowCertifiedSkipModeIndependent(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    151,
		EndSlot:                      200,
		AlpenglowDecisionSource: func(anchorSlot uint64) (alpenglow.ChainDecision, bool) {
			return alpenglow.ChainDecision{Slot: 151, Kind: alpenglow.ChainDecisionKindSkip}, true
		},
	})
	// NOT near-tip and NOT lightbringer-active: the pre-handoff / RPC-catchup
	// case that the near-tip gate would otherwise skip.
	bs.isNearTip.Store(false)
	bs.liveStreamActive.Store(false)

	bs.reorderMu.Lock()
	changed := bs.applyAlpenglowDecisionLocked()
	skipped := bs.skippedSlots[151]
	certSkip := bs.alpenglowCertifiedSkips[151]
	bs.reorderMu.Unlock()

	if changed {
		t.Fatal("skip marking returns false; the frontier advances via the normal skip path")
	}
	if !skipped || !certSkip {
		t.Fatalf("certified skip must be marked outside near-tip: skipped=%v cert=%v", skipped, certSkip)
	}
	if got := bs.stats.FetchSkipped.Load(); got != 1 {
		t.Fatalf("FetchSkipped = %d, want 1", got)
	}
}

func TestApplyAlpenglowDecisiveBlockReauthorizesOutsideNearTip(t *testing.T) {
	blockID := solana.Hash{0xA1}
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    151,
		EndSlot:                      200,
		AlpenglowDecisionSource: func(anchorSlot uint64) (alpenglow.ChainDecision, bool) {
			return alpenglow.ChainDecision{
				Slot: 151, Kind: alpenglow.ChainDecisionKindBlock,
				Block: alpenglow.BlockID{Slot: 151, Hash: blockID},
			}, true
		},
	})
	block := &b.Block{Slot: 151, HasAlpenglowBlockID: true, AlpenglowBlockID: blockID}
	bs.rejectAlpenglowBlockID(151, blockID)
	if !bs.isRejectedAlpenglowBlock(block) {
		t.Fatal("test premise: decisive block was not soft-tombstoned")
	}
	// Catchup/pre-handoff: candidate steering is inactive, but consensus
	// authority still has to revive this identity for later repair.
	bs.isNearTip.Store(false)
	bs.liveStreamActive.Store(false)

	bs.reorderMu.Lock()
	changed := bs.applyAlpenglowDecisionLocked()
	bs.reorderMu.Unlock()
	if changed {
		t.Fatal("mode-independent reauthorization must not claim a buffered change")
	}
	if bs.isRejectedAlpenglowBlock(block) {
		t.Fatal("decisive block remained tombstoned outside near-tip mode")
	}
	if got := bs.knownAlpenglowBlockIDs[151]; got != blockID {
		t.Fatalf("known Alpenglow block id = %s, want %s", got, blockID)
	}
}

func TestApplyAlpenglowDecisionLockedLeavesMatchingCertifiedBlock(t *testing.T) {
	blockID := solana.Hash{1}
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    151,
		EndSlot:                      200,
		AlpenglowDecisionSource: func(anchorSlot uint64) (alpenglow.ChainDecision, bool) {
			return alpenglow.ChainDecision{
				Slot:  151,
				Kind:  alpenglow.ChainDecisionKindBlock,
				Block: alpenglow.BlockID{Slot: 151, Hash: blockID},
			}, true
		},
	})
	bs.isNearTip.Store(true)
	bs.liveStreamActive.Store(true)
	matching := &b.Block{
		Slot:                151,
		FromLiveStream:      true,
		HasAlpenglowBlockID: true,
		AlpenglowBlockID:    [32]byte(blockID),
	}
	bs.reorderBuffer[151] = matching
	bs.rejectAlpenglowBlockID(151, blockID)
	if !bs.isRejectedAlpenglowBlock(matching) {
		t.Fatal("test premise: matching decisive block was not speculatively tombstoned")
	}

	bs.reorderMu.Lock()
	changed := bs.applyAlpenglowDecisionLocked()
	bs.reorderMu.Unlock()

	if changed {
		t.Fatalf("expected matching certified block to stay in normal emission path")
	}
	if bs.reorderBuffer[151] == nil {
		t.Fatalf("expected matching block to remain buffered")
	}
	if bs.isRejectedAlpenglowBlock(matching) {
		t.Fatal("decisive block decision did not clear its speculative tombstone")
	}
	if got := bs.knownAlpenglowBlockIDs[151]; got != blockID {
		t.Fatalf("known Alpenglow block id = %s, want %s", got, blockID)
	}
}

func TestApplyAlpenglowDecisionLockedDiscardsMismatchedCertifiedBlock(t *testing.T) {
	wantBlockID := solana.Hash{1}
	gotBlockID := solana.Hash{2}
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    151,
		EndSlot:                      200,
		AlpenglowDecisionSource: func(anchorSlot uint64) (alpenglow.ChainDecision, bool) {
			return alpenglow.ChainDecision{
				Slot:  151,
				Kind:  alpenglow.ChainDecisionKindBlock,
				Block: alpenglow.BlockID{Slot: 151, Hash: wantBlockID},
			}, true
		},
	})
	bs.isNearTip.Store(true)
	bs.liveStreamActive.Store(true)
	bs.reorderBuffer[151] = &b.Block{
		Slot:                151,
		FromLiveStream:      true,
		HasAlpenglowBlockID: true,
		AlpenglowBlockID:    [32]byte(gotBlockID),
	}
	bs.slotState[151] = slotDone

	bs.reorderMu.Lock()
	changed := bs.applyAlpenglowDecisionLocked()
	bs.reorderMu.Unlock()

	if !changed {
		t.Fatalf("expected mismatched certified block to advance the emission loop")
	}
	if bs.reorderBuffer[151] != nil {
		t.Fatalf("expected mismatched block to be discarded")
	}
	if _, exists := bs.slotState[151]; exists {
		t.Fatalf("expected slot state to be cleared so another source can retry")
	}
	if got := bs.knownAlpenglowBlockIDs[151]; got != wantBlockID {
		t.Fatalf("known Alpenglow block id = %s, want %s", got, wantBlockID)
	}
	if got := bs.liveRepairSlot.Load(); got != 0 {
		t.Fatalf("expected certified mismatch to avoid RPC repair, got repair slot %d", got)
	}
	if len(bs.retrySlots) != 0 {
		t.Fatalf("expected certified mismatch to avoid enqueueing an RPC retry, got %+v", bs.retrySlots)
	}
}

// Restart adjudication: after a finality-mismatch halt the node re-runs replay with
// the SAME engine — a FRESH block source wired to the surviving tracker's decisions
// must discard the wrong version and steer repair to the certified block.
func TestRestartAdjudicationConvergesViaRetainedDecision(t *testing.T) {
	certified := solana.Hash{0xAA}
	tracker := alpenglow.NewChainTracker()
	if _, err := tracker.ObserveCertificate(alpenglow.Certificate{
		Type: alpenglow.CertificateNotarize, Slot: 151, BlockHash: certified,
		SignatureVerified: true, StakeVerified: true,
	}); err != nil {
		t.Fatalf("observe cert: %v", err)
	}

	// "Restart": a brand-new block source, only the tracker survives.
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    151,
		EndSlot:                      200,
		AlpenglowDecisionSource:      tracker.NextDecision,
	})
	bs.isNearTip.Store(true)
	bs.liveStreamActive.Store(true)
	bs.reorderBuffer[151] = &b.Block{
		Slot:                151,
		FromLiveStream:      true,
		HasAlpenglowBlockID: true,
		AlpenglowBlockID:    [32]byte{0xBB}, // the wrong version that caused the halt
	}
	bs.slotState[151] = slotDone

	bs.reorderMu.Lock()
	changed := bs.applyAlpenglowDecisionLocked()
	bs.reorderMu.Unlock()

	if !changed {
		t.Fatal("retained decision must adjudicate the waiting slot after restart")
	}
	if bs.reorderBuffer[151] != nil {
		t.Fatal("wrong version must be discarded on restart")
	}
	if got := bs.knownAlpenglowBlockIDs[151]; got != certified {
		t.Fatalf("repair must target the certified block: got %s want %s", got, certified)
	}
}

// A conflict decision (equivocation) must fail closed: drop the buffered candidate
// so no fork is emitted, and record a fatal stop reason so replay halts.
func TestApplyAlpenglowDecisionLockedHaltsOnConflict(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    151,
		EndSlot:                      200,
		AlpenglowDecisionSource: func(anchorSlot uint64) (alpenglow.ChainDecision, bool) {
			return alpenglow.ChainDecision{
				Slot:   151,
				Kind:   alpenglow.ChainDecisionKindConflict,
				Reason: "two certified blocks",
			}, true
		},
	})
	bs.isNearTip.Store(true)
	bs.liveStreamActive.Store(true)
	bs.reorderBuffer[151] = &b.Block{Slot: 151, HasAlpenglowBlockID: true}

	bs.reorderMu.Lock()
	bs.applyAlpenglowDecisionLocked()
	_, stillBuffered := bs.reorderBuffer[151]
	bs.reorderMu.Unlock()

	if stillBuffered {
		t.Fatal("conflicted slot's block must be dropped, not emitted")
	}
	if bs.stopReasonEnum() != blockSourceStopReasonAlpenglowConflict {
		t.Fatalf("stop reason = %d, want AlpenglowConflict", bs.stopReasonEnum())
	}
	if !strings.Contains(bs.StopReason(), "conflict") {
		t.Fatalf("StopReason() = %q, want a conflict/halt message", bs.StopReason())
	}
}

func TestForceRPCForLightbringerParentMismatchClearsBufferedState(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.liveHandoffSlot.Store(101)
	bs.liveStreamActive.Store(true)

	bs.reorderBuffer[101] = &b.Block{Slot: 101, FromLiveStream: true, SourceParentSlot: 99}
	bs.reorderBuffer[102] = &b.Block{Slot: 102, FromLiveStream: true, SourceParentSlot: 101}
	bs.reorderBuffer[103] = &b.Block{Slot: 103, FromLiveStream: false}
	bs.slotState[101] = slotDone
	bs.slotState[102] = slotDone
	bs.slotState[103] = slotDone
	bs.liveStagingBuffer[104] = &b.Block{Slot: 104, FromLiveStream: true, SourceParentSlot: 102}
	bs.liveStagingOrder = []uint64{104}

	bs.forceRPCForLiveParentMismatch(101, 99, 100)

	if got := bs.liveForceRPCUntil.Load(); got != 101 {
		t.Fatalf("expected RPC to be forced until slot 101, got %d", got)
	}
	if got := bs.liveCooldownUntil.Load(); got != 101+liveRecoverySlots {
		t.Fatalf("expected cooldown boundary to match configured recovery window, got %d", got)
	}
	if got := bs.liveHandoffSlot.Load(); got != 0 {
		t.Fatalf("expected handoff slot to be cleared, got %d", got)
	}
	if bs.liveStreamActive.Load() {
		t.Fatalf("expected Lightbringer to be marked inactive after parent mismatch")
	}
	if !bs.liveNeedRPCResume.Load() {
		t.Fatalf("expected RPC resume flag to be raised after parent mismatch")
	}
	if _, exists := bs.reorderBuffer[101]; exists {
		t.Fatalf("expected mismatched Lightbringer slot 101 to be dropped from reorder buffer")
	}
	if _, exists := bs.reorderBuffer[102]; exists {
		t.Fatalf("expected prefetched Lightbringer slot 102 to be dropped from reorder buffer")
	}
	if _, exists := bs.reorderBuffer[103]; !exists {
		t.Fatalf("expected RPC slot 103 to remain buffered")
	}
	if _, exists := bs.slotState[101]; exists {
		t.Fatalf("expected slot state for 101 to be cleared")
	}
	if _, exists := bs.slotState[102]; exists {
		t.Fatalf("expected slot state for 102 to be cleared")
	}
	if _, exists := bs.slotState[103]; !exists {
		t.Fatalf("expected slot state for non-Lightbringer slot 103 to remain")
	}
	if len(bs.liveStagingBuffer) != 0 || len(bs.liveStagingOrder) != 0 {
		t.Fatalf("expected prefetched Lightbringer buffer to be cleared")
	}
}

func TestHandleLiveShredStreamClosedForcesRPCAndInvalidatesBufferedRunway(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:      BlockSourceTurbine,
		TurbineBindAddr: "127.0.0.1:0",
		StartSlot:       100,
		EndSlot:         200,
	})

	bs.isNearTip.Store(true)
	bs.liveStreamConnected.Store(true)
	bs.liveHandoffSlot.Store(121)
	bs.liveStreamActive.Store(true)
	bs.nextSlotToSend = 122
	bs.lastEmittedBlockSlot = 121
	bs.reorderBuffer[123] = &b.Block{Slot: 123, FromLiveStream: true, SourceParentSlot: 122}
	bs.reorderBuffer[124] = &b.Block{Slot: 124, FromLiveStream: true, SourceParentSlot: 123}
	bs.reorderBuffer[125] = &b.Block{Slot: 125, FromLiveStream: false}
	bs.slotState[123] = slotDone
	bs.slotState[124] = slotDone
	bs.slotState[125] = slotDone
	bs.liveStagingBuffer[126] = &b.Block{Slot: 126, FromLiveStream: true, SourceParentSlot: 125}
	bs.liveStagingOrder = []uint64{126}
	oldGeneration := bs.liveResultGeneration.Load()

	bs.handleLiveShredStreamClosed("test reconnect")

	if got := bs.liveResultGeneration.Load(); got != oldGeneration+1 {
		t.Fatalf("expected live stream generation to advance, got %d want %d", got, oldGeneration+1)
	}
	if got := bs.liveForceRPCUntil.Load(); got != 122 {
		t.Fatalf("expected RPC to be forced from waiting slot 122, got %d", got)
	}
	if got := bs.liveHandoffSlot.Load(); got != 0 {
		t.Fatalf("expected handoff slot to be cleared, got %d", got)
	}
	if bs.liveStreamActive.Load() {
		t.Fatalf("expected turbine to be marked inactive after stream close")
	}
	if !bs.liveNeedRPCResume.Load() {
		t.Fatalf("expected RPC resume flag after stream close")
	}
	if _, exists := bs.reorderBuffer[123]; exists {
		t.Fatalf("expected stale turbine slot 123 to be removed from reorder buffer")
	}
	if _, exists := bs.reorderBuffer[124]; exists {
		t.Fatalf("expected stale turbine slot 124 to be removed from reorder buffer")
	}
	if _, exists := bs.reorderBuffer[125]; !exists {
		t.Fatalf("expected buffered RPC slot 125 to remain")
	}
	if _, exists := bs.slotState[123]; exists {
		t.Fatalf("expected stale turbine slot state 123 to be cleared")
	}
	if _, exists := bs.slotState[124]; exists {
		t.Fatalf("expected stale turbine slot state 124 to be cleared")
	}
	if _, exists := bs.slotState[125]; !exists {
		t.Fatalf("expected RPC slot state 125 to remain")
	}
	if len(bs.liveStagingBuffer) != 0 || len(bs.liveStagingOrder) != 0 {
		t.Fatalf("expected prefetched turbine buffer to be cleared")
	}
}

func TestPrepareLightbringerHandoffAllowsSkippedGapFromParentSlot(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.isNearTip.Store(true)
	bs.lastEmittedBlockSlot = 150
	for slot := uint64(152); slot <= 158; slot++ {
		parentSlot := slot - 1
		if slot == 152 {
			parentSlot = 150
		}
		bs.liveStagingBuffer[slot] = &b.Block{Slot: slot, FromLiveStream: true, SourceParentSlot: parentSlot}
		bs.liveStagingOrder = append(bs.liveStagingOrder, slot)
	}

	blocks, handoffSlot, prepared := bs.prepareLiveHandoff(151, 150)
	if !prepared {
		t.Fatalf("expected handoff to prepare across a skipped gap")
	}
	if handoffSlot != 151 {
		t.Fatalf("expected handoff slot 151, got %d", handoffSlot)
	}
	if got := bs.liveHandoffSlot.Load(); got != 151 {
		t.Fatalf("expected stored handoff slot 151, got %d", got)
	}
	if len(blocks) != 7 {
		t.Fatalf("expected buffered Lightbringer runway 152-158 to be retained, got %+v", blocks)
	}
	saw := make(map[uint64]bool, len(blocks))
	for _, blk := range blocks {
		saw[blk.Slot] = true
	}
	for slot := uint64(152); slot <= 158; slot++ {
		if !saw[slot] {
			t.Fatalf("expected buffered Lightbringer slot %d to be retained, got %+v", slot, blocks)
		}
	}
}

func TestPrepareLightbringerHandoffRequiresMinimumRunway(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.isNearTip.Store(true)
	bs.lastEmittedBlockSlot = 150
	for slot := uint64(151); slot <= 152; slot++ {
		parentSlot := slot - 1
		if slot == 151 {
			parentSlot = 150
		}
		bs.liveStagingBuffer[slot] = &b.Block{Slot: slot, FromLiveStream: true, SourceParentSlot: parentSlot}
		bs.liveStagingOrder = append(bs.liveStagingOrder, slot)
	}

	if blocks, handoffSlot, prepared := bs.prepareLiveHandoff(151, 150); prepared || handoffSlot != 0 || len(blocks) != 0 {
		t.Fatalf("expected handoff to stay unarmed without the minimum Lightbringer runway, got prepared=%v handoff=%d blocks=%+v",
			prepared, handoffSlot, blocks)
	}
	if got := bs.liveHandoffSlot.Load(); got != 0 {
		t.Fatalf("expected stored handoff slot to remain unset without enough runway, got %d", got)
	}
}

func TestPrepareTurbineHandoffAllowsLiveEdgeRunwayAtTipWithoutConsensusBuffering(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:      BlockSourceTurbine,
		TurbineBindAddr: "127.0.0.1:8001",
		StartSlot:       100,
		EndSlot:         200,
	})

	bs.isNearTip.Store(true)
	bs.liveStreamConnected.Store(true)
	bs.lastExecutedSlot.Store(150)
	bs.confirmedTip.Store(151)
	bs.liveLastStreamSlot.Store(151)
	bs.lastEmittedBlockSlot = 150
	bs.liveStagingBuffer[151] = &b.Block{Slot: 151, FromLiveStream: true, SourceParentSlot: 150}
	bs.liveStagingOrder = append(bs.liveStagingOrder, 151)

	reason := bs.liveHandoffWaitReason(151, 150)
	if !strings.Contains(reason, "handoff-ready runway buffered through slot 151") {
		t.Fatalf("expected live-edge turbine runway to be handoff-ready, got %q", reason)
	}

	blocks, handoffSlot, prepared := bs.prepareLiveHandoff(151, 150)
	if !prepared {
		t.Fatalf("expected turbine handoff to prepare at the live edge")
	}
	if handoffSlot != 151 {
		t.Fatalf("expected handoff slot 151, got %d", handoffSlot)
	}
	if len(blocks) != 1 || blocks[0].Slot != 151 {
		t.Fatalf("expected single live-edge turbine block to be enqueued, got %+v", blocks)
	}
}

func TestPrepareLightbringerHandoffKeepsMinimumRunwayWhenLightbringerLagsTip(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.isNearTip.Store(true)
	bs.liveStreamConnected.Store(true)
	bs.lastExecutedSlot.Store(150)
	bs.confirmedTip.Store(157)
	bs.liveLastStreamSlot.Store(151)
	bs.lastEmittedBlockSlot = 150
	bs.liveStagingBuffer[151] = &b.Block{Slot: 151, FromLiveStream: true, SourceParentSlot: 150}
	bs.liveStagingOrder = append(bs.liveStagingOrder, 151)

	blocks, handoffSlot, prepared := bs.prepareLiveHandoff(151, 150)
	if prepared || handoffSlot != 0 || len(blocks) != 0 {
		t.Fatalf("expected stale Lightbringer stream to require the full handoff runway, got prepared=%v handoff=%d blocks=%+v",
			prepared, handoffSlot, blocks)
	}
}

func TestPrepareLightbringerHandoffRequiresRunwayThroughConfiguredBoundary(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.isNearTip.Store(true)
	bs.lastEmittedBlockSlot = 150
	for slot := uint64(151); slot <= 157; slot++ {
		parentSlot := slot - 1
		if slot == 151 {
			parentSlot = 150
		}
		bs.liveStagingBuffer[slot] = &b.Block{Slot: slot, FromLiveStream: true, SourceParentSlot: parentSlot}
		bs.liveStagingOrder = append(bs.liveStagingOrder, slot)
	}

	if blocks, handoffSlot, prepared := bs.prepareLiveHandoff(151, 150); prepared || handoffSlot != 0 || len(blocks) != 0 {
		t.Fatalf("expected handoff to stay unarmed when connected runway only covers through slot 157, got prepared=%v handoff=%d blocks=%+v",
			prepared, handoffSlot, blocks)
	}
}

func TestPrepareLightbringerHandoffRequiresConnectedRunway(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.isNearTip.Store(true)
	bs.lastEmittedBlockSlot = 150
	bs.liveStagingBuffer[151] = &b.Block{Slot: 151, FromLiveStream: true, SourceParentSlot: 150}
	bs.liveStagingBuffer[158] = &b.Block{Slot: 158, FromLiveStream: true, SourceParentSlot: 157}
	bs.liveStagingOrder = []uint64{151, 158}

	if blocks, handoffSlot, prepared := bs.prepareLiveHandoff(151, 150); prepared || handoffSlot != 0 || len(blocks) != 0 {
		t.Fatalf("expected sparse Lightbringer buffer to stay unarmed without a connected runway, got prepared=%v handoff=%d blocks=%+v",
			prepared, handoffSlot, blocks)
	}
}

func TestPrepareLightbringerHandoffPurgesRPCOwnedStateAtBoundary(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.isNearTip.Store(true)
	bs.lastEmittedBlockSlot = 150
	for slot := uint64(151); slot <= 158; slot++ {
		parentSlot := slot - 1
		if slot == 151 {
			parentSlot = 150
		}
		bs.liveStagingBuffer[slot] = &b.Block{Slot: slot, FromLiveStream: true, SourceParentSlot: parentSlot}
		bs.liveStagingOrder = append(bs.liveStagingOrder, slot)
	}

	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLiveStream: false}
	bs.reorderBuffer[152] = &b.Block{Slot: 152, FromLiveStream: false}
	bs.skippedSlots[153] = true
	bs.slotState[151] = slotInflight
	bs.slotState[152] = slotDone
	bs.retrySlots = []uint64{149, 151, 152}

	blocks, handoffSlot, prepared := bs.prepareLiveHandoff(151, 150)
	if !prepared {
		t.Fatalf("expected handoff to prepare")
	}
	if handoffSlot != 151 {
		t.Fatalf("expected handoff slot 151, got %d", handoffSlot)
	}
	if len(blocks) != 8 {
		t.Fatalf("expected buffered Lightbringer handoff runway 151-158, got %+v", blocks)
	}
	saw := make(map[uint64]bool, len(blocks))
	for _, blk := range blocks {
		if !blk.FromLiveStream {
			t.Fatalf("expected only Lightbringer blocks in handoff runway, got %+v", blocks)
		}
		saw[blk.Slot] = true
	}
	for slot := uint64(151); slot <= 158; slot++ {
		if !saw[slot] {
			t.Fatalf("expected buffered Lightbringer slot %d in handoff runway, got %+v", slot, blocks)
		}
	}
	if _, exists := bs.reorderBuffer[151]; exists {
		t.Fatalf("expected RPC buffered slot 151 to be purged at handoff")
	}
	if _, exists := bs.reorderBuffer[152]; exists {
		t.Fatalf("expected RPC buffered slot 152 to be purged at handoff")
	}
	if bs.skippedSlots[153] {
		t.Fatalf("expected RPC-owned skipped marker at slot 153 to be purged at handoff")
	}
	if _, exists := bs.slotState[151]; exists {
		t.Fatalf("expected slot state for 151 to be purged at handoff")
	}
	if _, exists := bs.slotState[152]; exists {
		t.Fatalf("expected slot state for 152 to be purged at handoff")
	}
	if len(bs.retrySlots) != 1 || bs.retrySlots[0] != 149 {
		t.Fatalf("expected retries at or beyond handoff to be purged, got %+v", bs.retrySlots)
	}
}

func TestMaybePrepareLightbringerHandoffDefersWhenStreamTipShowsReplayGapTooLarge(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.isNearTip.Store(true)
	bs.liveStreamConnected.Store(true)
	bs.lastExecutedSlot.Store(101)
	bs.confirmedTip.Store(117)
	bs.liveLastStreamSlot.Store(118)
	bs.lastEmittedBlockSlot = 110
	bs.nextSlotToSend = 111
	for slot := uint64(111); slot <= 118; slot++ {
		parentSlot := slot - 1
		if slot == 111 {
			parentSlot = 110
		}
		bs.liveStagingBuffer[slot] = &b.Block{Slot: slot, FromLiveStream: true, SourceParentSlot: parentSlot}
		bs.liveStagingOrder = append(bs.liveStagingOrder, slot)
	}

	bs.maybePrepareLiveHandoff()

	if got := bs.liveHandoffSlot.Load(); got != 0 {
		t.Fatalf("expected handoff to stay unarmed while replay gap exceeds handoff threshold, got %d", got)
	}
	if queued := len(bs.resultQueue); queued != 0 {
		t.Fatalf("expected no Lightbringer blocks to be enqueued before handoff, got %d", queued)
	}
}

func TestMaybePrepareLightbringerHandoffArmsWhenReplayGapHasHeadroom(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.isNearTip.Store(true)
	bs.liveStreamConnected.Store(true)
	bs.lastExecutedSlot.Store(102)
	bs.confirmedTip.Store(117)
	bs.liveLastStreamSlot.Store(118)
	bs.lastEmittedBlockSlot = 110
	bs.nextSlotToSend = 111
	for slot := uint64(111); slot <= 118; slot++ {
		parentSlot := slot - 1
		if slot == 111 {
			parentSlot = 110
		}
		bs.liveStagingBuffer[slot] = &b.Block{Slot: slot, FromLiveStream: true, SourceParentSlot: parentSlot}
		bs.liveStagingOrder = append(bs.liveStagingOrder, slot)
	}

	bs.maybePrepareLiveHandoff()

	if got := bs.liveHandoffSlot.Load(); got != 111 {
		t.Fatalf("expected handoff to arm at slot 111 once replay gap has headroom, got %d", got)
	}
	if queued := len(bs.resultQueue); queued != 8 {
		t.Fatalf("expected the 8-slot Lightbringer runway to be enqueued, got %d", queued)
	}
}

func TestShouldDecodeLightbringerSlotStagesBeforeNearTipWithinCatchupWindow(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              300,
	})

	bs.isNearTip.Store(false)
	bs.liveStreamConnected.Store(true)
	bs.lastExecutedSlot.Store(100)
	bs.confirmedTip.Store(164)
	bs.liveLastStreamSlot.Store(164)
	bs.nextSlotToSend = 110

	if !bs.shouldDecodeLiveSlot(120) {
		t.Fatalf("expected Lightbringer slot within catchup staging window to be decoded")
	}
	if bs.shouldDecodeLiveSlot(109) {
		t.Fatalf("expected slot behind the emission frontier to stay unstaged")
	}
}

func TestShouldDecodeLightbringerSlotDoesNotStageWhenReplayGapTooLarge(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              300,
	})

	bs.isNearTip.Store(false)
	bs.liveStreamConnected.Store(true)
	bs.lastExecutedSlot.Store(100)
	bs.confirmedTip.Store(165)
	bs.liveLastStreamSlot.Store(165)
	bs.nextSlotToSend = 101

	if bs.shouldDecodeLiveSlot(120) {
		t.Fatalf("expected Lightbringer staging to wait until replay is inside the catchup staging window")
	}
}

func TestShouldPreferIncomingLightbringerBlockLockedPrefersConnectedSameSlotBlock(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.lastEmittedBlockSlot = 150
	disconnected := &b.Block{Slot: 151, FromLiveStream: true, SourceParentSlot: 149}
	connected := &b.Block{Slot: 151, FromLiveStream: true, SourceParentSlot: 150}

	bs.reorderMu.Lock()
	preferIncoming := bs.shouldPreferIncomingLiveBlockLocked(disconnected, connected)
	bs.reorderMu.Unlock()

	if !preferIncoming {
		t.Fatalf("expected same-slot Lightbringer block that matches the anchor to replace the disconnected buffered fork")
	}
}

func TestSynthesizeAlpenglowParentLinkedSkipsLocked(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    151,
		EndSlot:                      200,
	})
	anchorID := solana.Hash{1}
	childID := solana.Hash{2}
	bs.lastEmittedBlockSlot = 150
	bs.lastEmittedAlpenglowBlockID = anchorID
	bs.hasLastEmittedAlpenglowBlockID = true
	bs.reorderBuffer[155] = &b.Block{
		Slot:                      155,
		FromLiveStream:            true,
		SourceParentSlot:          150,
		AlpenglowBlockID:          childID,
		HasAlpenglowBlockID:       true,
		AlpenglowParentBlockID:    anchorID,
		HasAlpenglowParentBlockID: true,
	}

	bs.reorderMu.Lock()
	changed := bs.synthesizeAlpenglowParentLinkedSkipsLocked()
	bs.reorderMu.Unlock()
	if !changed {
		t.Fatal("expected exact Alpenglow parent link to resolve the absent slot run")
	}
	for slot := uint64(151); slot < 155; slot++ {
		if !bs.skippedSlots[slot] || !bs.liveSynthesizedSkips[slot] {
			t.Fatalf("slot %d was not marked as a provisional live skip", slot)
		}
	}
}

func TestSynthesizeAlpenglowParentLinkedSkipsRejectsWeakEvidence(t *testing.T) {
	newSource := func() *BlockSource {
		bs := NewBlockSource(&BlockSourceOpts{
			SourceType:                   BlockSourceTurbine,
			TurbineBindAddr:              "127.0.0.1:0",
			TurbineAlpenglowBlockIDHints: true,
			StartSlot:                    151,
			EndSlot:                      200,
		})
		bs.lastEmittedBlockSlot = 150
		bs.lastEmittedAlpenglowBlockID = solana.Hash{1}
		bs.hasLastEmittedAlpenglowBlockID = true
		return bs
	}

	t.Run("parent slot alone", func(t *testing.T) {
		bs := newSource()
		bs.reorderBuffer[155] = &b.Block{
			Slot:                      155,
			FromLiveStream:            true,
			SourceParentSlot:          150,
			AlpenglowBlockID:          solana.Hash{2},
			HasAlpenglowBlockID:       true,
			AlpenglowParentBlockID:    solana.Hash{9},
			HasAlpenglowParentBlockID: true,
		}
		if bs.synthesizeAlpenglowParentLinkedSkipsLocked() {
			t.Fatal("parent slot with a different parent block ID must not synthesize skips")
		}
	})

	t.Run("observed candidate in gap", func(t *testing.T) {
		bs := newSource()
		bs.reorderBuffer[153] = &b.Block{Slot: 153, FromLiveStream: true}
		bs.reorderBuffer[155] = &b.Block{
			Slot:                      155,
			FromLiveStream:            true,
			SourceParentSlot:          150,
			AlpenglowBlockID:          solana.Hash{2},
			HasAlpenglowBlockID:       true,
			AlpenglowParentBlockID:    solana.Hash{1},
			HasAlpenglowParentBlockID: true,
		}
		if bs.synthesizeAlpenglowParentLinkedSkipsLocked() {
			t.Fatal("a later reconnecting branch must not skip over an observed candidate")
		}
	})
}

func TestRewindForAlpenglowSwitchRestoresLastRealAnchorAcrossSkips(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    151,
		EndSlot:                      200,
	})
	anchorID := solana.Hash{1}
	childID := solana.Hash{2}
	bs.emittedAlpenglowBlockIDs[150] = anchorID
	bs.emittedAlpenglowBlockIDs[155] = childID
	bs.emittedAlpenglowBlockIDOrder = []uint64{150, 155}
	bs.lastEmittedBlockSlot = 155
	bs.lastEmittedAlpenglowBlockID = childID
	bs.hasLastEmittedAlpenglowBlockID = true
	bs.nextSlotToSend = 156

	bs.RewindForAlpenglowSwitch(153, solana.Hash{})
	if bs.lastEmittedBlockSlot != 150 || !bs.hasLastEmittedAlpenglowBlockID || bs.lastEmittedAlpenglowBlockID != anchorID {
		t.Fatalf("rewind anchor = slot %d id %s present=%v, want slot 150 id %s",
			bs.lastEmittedBlockSlot, bs.lastEmittedAlpenglowBlockID, bs.hasLastEmittedAlpenglowBlockID, anchorID)
	}
	if _, exists := bs.emittedAlpenglowBlockIDs[155]; exists {
		t.Fatal("rewind retained a discarded descendant block ID")
	}
}

func TestAlpenglowParentLinkedForkRewindsAndEmitsAlternateBranch(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    344,
		EndSlot:                      400,
	})
	parentID := solana.Hash{0x43}
	wrong344 := solana.Hash{0x44}
	wrong345 := solana.Hash{0x45}
	childID := solana.Hash{0x48}
	child := &b.Block{
		Slot:                      348,
		FromLiveStream:            true,
		SourceParentSlot:          343,
		AlpenglowBlockID:          childID,
		HasAlpenglowBlockID:       true,
		AlpenglowParentBlockID:    parentID,
		HasAlpenglowParentBlockID: true,
	}

	bs.emittedAlpenglowBlockIDs[343] = parentID
	bs.emittedAlpenglowBlockIDs[344] = wrong344
	bs.emittedAlpenglowBlockIDs[345] = wrong345
	bs.emittedAlpenglowBlockIDOrder = []uint64{343, 344, 345}
	bs.lastEmittedBlockSlot = 345
	bs.lastEmittedAlpenglowBlockID = wrong345
	bs.hasLastEmittedAlpenglowBlockID = true
	// Replay is blocked on absent slot 346; the alternate child is buffered
	// later at 348 and links behind the already-emitted 344-345 suffix.
	bs.nextSlotToSend = 346
	bs.reorderBuffer[348] = child
	bs.slotState[348] = slotDone

	bs.reorderMu.Lock()
	if !bs.queueBufferedAlpenglowParentSwitchLocked() {
		bs.reorderMu.Unlock()
		t.Fatal("buffered exact child-to-emitted-ancestor link did not queue a switch across absent head slots")
	}
	bs.reorderMu.Unlock()

	var event AlpenglowParentSwitch
	select {
	case event = <-bs.alpenglowParentSwitchCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for parent-linked switch event")
	}
	if event.SwitchSlot != 344 || event.ParentSlot != 343 || event.ChildSlot != 348 || event.ParentID != parentID || event.ChildID != childID {
		t.Fatalf("unexpected switch event: %+v", event)
	}
	// Model speculative emissions already queued for replay when the alternate
	// child exposed the fork. The source rewind must purge them.
	bs.streamChan <- &b.Block{Slot: 346, IsSkipped: true, FromLiveStream: true}

	done := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(done)
	}()
	if !bs.RewindForAlpenglowParentSwitch(event) {
		t.Fatal("source rejected the pending parent-linked switch")
	}

	for slot := uint64(344); slot <= 347; slot++ {
		select {
		case got := <-bs.streamChan:
			if got == nil || got.Slot != slot || !got.IsSkipped || !got.FromLiveStream {
				t.Fatalf("slot %d emission = %+v, want provisional live skip", slot, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for replayed skip slot %d", slot)
		}
	}
	select {
	case got := <-bs.streamChan:
		if got != child {
			t.Fatalf("alternate child emission = %+v, want original child %+v", got, child)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for alternate child emission")
	}

	close(bs.resultQueue)
	<-done
	if bs.lastEmittedBlockSlot != 348 || bs.lastEmittedAlpenglowBlockID != childID {
		t.Fatalf("emission anchor = slot %d id %s, want slot 348 id %s", bs.lastEmittedBlockSlot, bs.lastEmittedAlpenglowBlockID, childID)
	}
	if _, exists := bs.emittedAlpenglowBlockIDs[344]; exists {
		t.Fatal("discarded fork identity at slot 344 survived the rewind")
	}
	lateWrong := &b.Block{
		Slot:                      344,
		FromLiveStream:            true,
		SourceParentSlot:          343,
		AlpenglowBlockID:          wrong344,
		HasAlpenglowBlockID:       true,
		AlpenglowParentBlockID:    parentID,
		HasAlpenglowParentBlockID: true,
	}
	if !bs.isRejectedAlpenglowBlock(lateWrong) {
		t.Fatal("discarded block identity was not tombstoned")
	}
	bs.reorderMu.Lock()
	queuedReverse := bs.queueAlpenglowParentSwitchLocked(lateWrong)
	bs.reorderMu.Unlock()
	if queuedReverse {
		t.Fatal("delayed discarded block reversed the accepted parent-linked switch")
	}
	unseenWrong := *lateWrong
	unseenWrong.Slot = 347
	unseenWrong.SourceParentSlot = 343
	unseenWrong.AlpenglowBlockID = solana.Hash{0x47}
	if !bs.isRejectedAlpenglowBlock(&unseenWrong) {
		t.Fatal("previously unseen block inside selected skip range was not rejected")
	}

	// A later certificate naming the old identity is authoritative and must be
	// able to override the speculative tombstone.
	bs.SetKnownAlpenglowBlockID(344, wrong344)
	if bs.isRejectedAlpenglowBlock(lateWrong) {
		t.Fatal("certificate-derived block-ID hint did not clear speculative tombstone")
	}
	for slot := uint64(10_000); slot < 10_000+maxKnownAlpenglowBlockIDs; slot++ {
		bs.SetKnownAlpenglowBlockID(slot, solana.Hash{byte(slot), byte(slot >> 8)})
	}
	bs.rejectedAlpenglowMu.RLock()
	_, retainedOldAuthority := bs.authoritativeAlpenglowBlockIDs[344]
	authorityCount := len(bs.authoritativeAlpenglowBlockIDs)
	bs.rejectedAlpenglowMu.RUnlock()
	if retainedOldAuthority || authorityCount > maxKnownAlpenglowBlockIDs {
		t.Fatalf("authoritative tombstone overrides were not bounded with block-ID hints: retained_old=%t count=%d", retainedOldAuthority, authorityCount)
	}
}

func TestRejectAlpenglowParentSwitchRetainsRootedBranch(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    344,
		EndSlot:                      400,
	})
	parentID := solana.Hash{0x43}
	rootedChildID := solana.Hash{0x48}
	lateSibling := &b.Block{
		Slot:                      350,
		FromLiveStream:            true,
		SourceParentSlot:          343,
		AlpenglowBlockID:          rootedChildID,
		HasAlpenglowBlockID:       true,
		AlpenglowParentBlockID:    parentID,
		HasAlpenglowParentBlockID: true,
	}
	bs.emittedAlpenglowBlockIDs[343] = parentID
	bs.emittedAlpenglowBlockIDs[348] = solana.Hash{0x99}
	bs.lastEmittedBlockSlot = 348
	bs.lastEmittedAlpenglowBlockID = solana.Hash{0x99}
	bs.hasLastEmittedAlpenglowBlockID = true
	bs.nextSlotToSend = 349
	bs.reorderBuffer[350] = lateSibling

	bs.reorderMu.Lock()
	if !bs.queueAlpenglowParentSwitchLocked(lateSibling) {
		bs.reorderMu.Unlock()
		t.Fatal("test premise: late sibling did not queue a parent switch")
	}
	event := *bs.pendingAlpenglowParentSwitch
	bs.reorderMu.Unlock()

	if !bs.RejectAlpenglowParentSwitch(event) {
		t.Fatal("rooted branch retention rejected the pending event")
	}
	if bs.pendingAlpenglowParentSwitch != nil {
		t.Fatal("rooted branch retention left the switch pending")
	}
	if bs.reorderBuffer[event.ChildSlot] != nil {
		t.Fatal("late sibling remained buffered after rejection")
	}
	if bs.nextSlotToSend != 349 || bs.lastEmittedBlockSlot != 348 {
		t.Fatalf("rooted branch frontier changed: next=%d emitted=%d", bs.nextSlotToSend, bs.lastEmittedBlockSlot)
	}
	if !bs.isRejectedAlpenglowBlock(lateSibling) {
		t.Fatal("late sibling identity was not tombstoned")
	}
	bs.reorderMu.Lock()
	reversed := bs.queueAlpenglowParentSwitchLocked(lateSibling)
	bs.reorderMu.Unlock()
	if reversed {
		t.Fatal("tombstoned late sibling queued the same reverse switch again")
	}
}

func TestLateParentLinkedChildCannotReplaceProgressedEmittedBranchWithoutDecision(t *testing.T) {
	parentID := solana.Hash{0x45}
	canonicalID := solana.Hash{0x48}
	lateID := solana.Hash{0x46}
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    346,
		EndSlot:                      400,
	})
	bs.emittedAlpenglowBlockIDs[345] = parentID
	bs.emittedAlpenglowBlockIDs[348] = canonicalID
	bs.emittedAlpenglowBlockIDOrder = []uint64{345, 348}
	bs.lastEmittedBlockSlot = 348
	bs.lastEmittedAlpenglowBlockID = canonicalID
	bs.hasLastEmittedAlpenglowBlockID = true
	bs.nextSlotToSend = 349
	late := &b.Block{
		Slot:                      346,
		FromLiveStream:            true,
		SourceParentSlot:          345,
		AlpenglowBlockID:          lateID,
		HasAlpenglowBlockID:       true,
		AlpenglowParentBlockID:    parentID,
		HasAlpenglowParentBlockID: true,
	}

	bs.reorderMu.Lock()
	queued := bs.queueAlpenglowParentSwitchLocked(late)
	bs.reorderMu.Unlock()
	if queued || bs.pendingAlpenglowParentSwitch != nil {
		t.Fatal("late unknown child replaced a branch already emitted past its slot")
	}
	if got := bs.emittedAlpenglowBlockIDs[348]; got != canonicalID {
		t.Fatalf("progressed branch identity changed: got %s want %s", got, canonicalID)
	}
	if !bs.isRejectedAlpenglowBlock(late) {
		t.Fatal("late unknown child was not soft-tombstoned")
	}
	canonical := &b.Block{Slot: 348, HasAlpenglowBlockID: true, AlpenglowBlockID: canonicalID}
	if bs.isRejectedAlpenglowBlock(canonical) {
		t.Fatal("retained progressed branch was tombstoned")
	}

	// Consensus authority, unlike arrival order, may select the late branch.
	bs.SetKnownAlpenglowBlockID(late.Slot, lateID)
	bs.alpenglowDecisionSource = func(anchor uint64) (alpenglow.ChainDecision, bool) {
		if anchor != late.Slot-1 {
			return alpenglow.ChainDecision{}, false
		}
		return alpenglow.ChainDecision{
			Slot: late.Slot, Kind: alpenglow.ChainDecisionKindBlock,
			Block: alpenglow.BlockID{Slot: late.Slot, Hash: lateID},
		}, true
	}
	bs.reorderMu.Lock()
	queued = bs.queueAlpenglowParentSwitchLocked(late)
	bs.reorderMu.Unlock()
	if !queued || bs.pendingAlpenglowParentSwitch == nil {
		t.Fatal("decisive consensus block did not override the late-child guard")
	}
}

func TestAlpenglowParentLinkedForkRequiresExactIDAndAlpenglowMode(t *testing.T) {
	newSource := func(alpenglowMode bool) *BlockSource {
		bs := NewBlockSource(&BlockSourceOpts{
			SourceType:                   BlockSourceTurbine,
			TurbineBindAddr:              "127.0.0.1:0",
			TurbineAlpenglowBlockIDHints: alpenglowMode,
			StartSlot:                    151,
			EndSlot:                      200,
		})
		bs.nextSlotToSend = 155
		bs.lastEmittedBlockSlot = 154
		bs.emittedAlpenglowBlockIDs[150] = solana.Hash{1}
		return bs
	}
	child := &b.Block{
		Slot:                      155,
		FromLiveStream:            true,
		SourceParentSlot:          150,
		AlpenglowBlockID:          solana.Hash{2},
		HasAlpenglowBlockID:       true,
		AlpenglowParentBlockID:    solana.Hash{9}, // does not match emitted ancestor
		HasAlpenglowParentBlockID: true,
	}
	if bs := newSource(true); bs.queueAlpenglowParentSwitchLocked(child) {
		t.Fatal("slot-only ancestry mismatch queued a speculative switch")
	}
	child.AlpenglowParentBlockID = solana.Hash{1}
	if bs := newSource(false); bs.queueAlpenglowParentSwitchLocked(child) {
		t.Fatal("classic turbine mode queued an Alpenglow speculative switch")
	}
}

func TestAlpenglowParentLinkedForkCannotOverrideCertifiedSkip(t *testing.T) {
	parentID := solana.Hash{0x43}
	childID := solana.Hash{0x44}
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    344,
		EndSlot:                      400,
		AlpenglowDecisionSource: func(anchor uint64) (alpenglow.ChainDecision, bool) {
			if anchor == 343 {
				return alpenglow.ChainDecision{Slot: 344, Kind: alpenglow.ChainDecisionKindSkip}, true
			}
			return alpenglow.ChainDecision{}, false
		},
	})
	bs.emittedAlpenglowBlockIDs[343] = parentID
	bs.emittedAlpenglowBlockIDs[348] = solana.Hash{0x48}
	bs.lastEmittedBlockSlot = 348
	bs.lastEmittedAlpenglowBlockID = solana.Hash{0x48}
	bs.hasLastEmittedAlpenglowBlockID = true
	child := &b.Block{
		Slot:                      344,
		FromLiveStream:            true,
		SourceParentSlot:          343,
		AlpenglowBlockID:          childID,
		HasAlpenglowBlockID:       true,
		AlpenglowParentBlockID:    parentID,
		HasAlpenglowParentBlockID: true,
	}

	bs.reorderMu.Lock()
	queued := bs.queueAlpenglowParentSwitchLocked(child)
	bs.reorderMu.Unlock()
	if queued || bs.pendingAlpenglowParentSwitch != nil {
		t.Fatal("a child at a certified-skipped slot queued a speculative switch")
	}
	if !bs.isRejectedAlpenglowBlock(child) {
		t.Fatal("the consensus-rejected child identity was not tombstoned")
	}
}

func TestAlpenglowParentLinkedForkOutsideFiniteRangeIsIgnored(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    100,
		EndSlot:                      120,
	})
	parentID := solana.Hash{1}
	bs.lastEmittedBlockSlot = 150
	bs.emittedAlpenglowBlockIDs[125] = parentID
	child := &b.Block{
		Slot:                      140,
		FromLiveStream:            true,
		SourceParentSlot:          125,
		AlpenglowBlockID:          solana.Hash{2},
		HasAlpenglowBlockID:       true,
		AlpenglowParentBlockID:    parentID,
		HasAlpenglowParentBlockID: true,
	}
	bs.reorderMu.Lock()
	queued := bs.queueAlpenglowParentSwitchLocked(child)
	bs.reorderMu.Unlock()
	if queued {
		t.Fatal("fork outside finite replay range queued a shutdown-racing control event")
	}
}

func TestWaitingLightbringerParentMismatchLockedDetectsDisconnectedBufferedSlot(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.lastEmittedBlockSlot = 150
	bs.nextSlotToSend = 151
	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLiveStream: true, SourceParentSlot: 149}

	bs.reorderMu.Lock()
	waitingSlot, observedParent, expectedParent, mismatch := bs.waitingLiveParentMismatchLocked()
	bs.reorderMu.Unlock()

	if !mismatch {
		t.Fatalf("expected disconnected waiting Lightbringer block to be recognized as a parent mismatch")
	}
	if waitingSlot != 151 || observedParent != 149 || expectedParent != 150 {
		t.Fatalf("expected mismatch details slot=151 observed_parent=149 expected_parent=150, got slot=%d observed=%d expected=%d",
			waitingSlot, observedParent, expectedParent)
	}
}

func TestShouldDiscardSkippedSlotAfterHandoffDropsRPCSkipMarker(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.isNearTip.Store(true)
	bs.liveHandoffSlot.Store(151)
	bs.skippedSlots[151] = true

	if !bs.shouldDiscardSkippedSlotAfterHandoff(151) {
		t.Fatalf("expected provisional RPC skip marker at slot 151 to be discarded after Lightbringer handoff")
	}
}

func TestShouldDiscardSkippedSlotAfterHandoffKeepsAlpenglowCertifiedSkip(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    151,
		EndSlot:                      200,
		AlpenglowDecisionSource: func(anchorSlot uint64) (alpenglow.ChainDecision, bool) {
			return alpenglow.ChainDecision{
				Slot:   151,
				Kind:   alpenglow.ChainDecisionKindSkip,
				Reason: "skip certificate",
			}, true
		},
	})
	bs.isNearTip.Store(true)
	bs.liveStreamActive.Store(true)
	bs.liveHandoffSlot.Store(151)

	bs.reorderMu.Lock()
	bs.applyAlpenglowDecisionLocked()
	bs.reorderMu.Unlock()

	if !bs.skippedSlots[151] {
		t.Fatalf("expected certified skip to mark slot 151 skipped")
	}
	if bs.shouldDiscardSkippedSlotAfterHandoff(151) {
		t.Fatalf("expected Alpenglow-certified skip marker at slot 151 to survive Turbine handoff")
	}
}

func TestEmitOrderedBlocksMarksAlpenglowCertifiedSkipAsLiveStreamObservation(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    151,
		EndSlot:                      200,
		AlpenglowDecisionSource: func(anchorSlot uint64) (alpenglow.ChainDecision, bool) {
			if anchorSlot != 150 {
				return alpenglow.ChainDecision{}, false
			}
			return alpenglow.ChainDecision{
				Slot:   151,
				Kind:   alpenglow.ChainDecisionKindSkip,
				Reason: "skip certificate",
			}, true
		},
	})
	bs.isNearTip.Store(true)
	bs.liveStreamActive.Store(true)
	bs.liveHandoffSlot.Store(151)

	done := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(done)
	}()

	bs.resultQueue <- fetchResult{
		slot:  152,
		block: &b.Block{Slot: 152, FromLiveStream: true, SourceParentSlot: 151},
	}
	close(bs.resultQueue)
	<-done

	skip := bs.NextBlock()
	if skip == nil || !skip.IsSkipped || skip.Slot != 151 {
		t.Fatalf("expected certified skip for slot 151, got %+v", skip)
	}
	if !skip.FromLiveStream {
		t.Fatalf("expected certified skip to be marked as live-stream sourced")
	}
}

func TestInspectLaterLightbringerBlocksLockedFindsConnectedDescendant(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.lastEmittedBlockSlot = 150
	bs.reorderBuffer[152] = &b.Block{Slot: 152, FromLiveStream: true, SourceParentSlot: 151}
	bs.reorderBuffer[154] = &b.Block{Slot: 154, FromLiveStream: false}
	bs.reorderBuffer[155] = &b.Block{Slot: 155, FromLiveStream: true, SourceParentSlot: 150}
	bs.reorderBuffer[156] = &b.Block{Slot: 156, FromLiveStream: true, SourceParentSlot: 155}

	firstBufferedSlot, firstBufferedParentSlot, bufferedCount, firstConnectedSlot, firstConnectedParentSlot, foundConnected := bs.inspectLaterLiveBlocksLocked(151)
	if firstBufferedSlot != 152 || firstBufferedParentSlot != 151 {
		t.Fatalf("expected earliest later Lightbringer block to be 152(parent=151), got slot=%d parent=%d", firstBufferedSlot, firstBufferedParentSlot)
	}
	if bufferedCount != 3 {
		t.Fatalf("expected three later Lightbringer blocks to be counted, got %d", bufferedCount)
	}
	if !foundConnected {
		t.Fatalf("expected a connected descendant to the current anchor to be found")
	}
	if firstConnectedSlot != 155 || firstConnectedParentSlot != 150 {
		t.Fatalf("expected first connected descendant to be 155(parent=150), got slot=%d parent=%d", firstConnectedSlot, firstConnectedParentSlot)
	}
}

func TestHandleDetectedLightbringerGapWaitsForStreamWhenLightbringerActive(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.liveStreamActive.Store(true)
	bs.liveHandoffSlot.Store(120)

	bs.handleDetectedLiveGap(125, 126, 125, 4)

	if got := bs.liveForceRPCUntil.Load(); got != 0 {
		t.Fatalf("expected active Lightbringer gap to avoid forcing RPC, got %d", got)
	}
	if got := bs.liveCooldownUntil.Load(); got != 0 {
		t.Fatalf("expected active Lightbringer gap to avoid setting cooldown, got %d", got)
	}
	if got := bs.liveHandoffSlot.Load(); got != 120 {
		t.Fatalf("expected active Lightbringer gap to preserve handoff slot, got %d", got)
	}
	if !bs.liveStreamActive.Load() {
		t.Fatalf("expected Lightbringer to remain active")
	}
	if got := bs.liveRepairSlot.Load(); got != 0 {
		t.Fatalf("expected active Lightbringer gap to avoid scheduling RPC repair, got %d", got)
	}
	if len(bs.retrySlots) != 0 {
		t.Fatalf("expected active Lightbringer gap to avoid queueing an RPC retry, got %+v", bs.retrySlots)
	}
}

func TestRepairLightbringerGapReconnectsForMissingAncestorRange(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.liveStreamConnected.Store(true)
	bs.liveStreamActive.Store(true)
	bs.isNearTip.Store(true)
	bs.nextSlotToSend = 120
	bs.lastEmittedBlockSlot = 119
	bs.liveGapSinceUnix.Store(time.Now().Add(-(lightbringerDeepGapReconnect + time.Second)).UnixNano())

	reconnected := false
	bs.setLiveStreamCancel(func() {
		reconnected = true
	})

	bs.repairLiveStreamGap(120, 122, 121, reorderGapWarnThreshold)

	if !reconnected {
		t.Fatalf("expected reconnect to be requested for a missing Lightbringer ancestor range")
	}
	if got := bs.lightbringerGapReconnectSlot.Load(); got != 120 {
		t.Fatalf("expected reconnect slot to be recorded as 120, got %d", got)
	}
}

func TestDetectLightbringerGapWaitsForConfiguredFallbackDelay(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.liveStreamActive.Store(true)
	bs.nextSlotToSend = 120
	bs.reorderBuffer[121] = &b.Block{Slot: 121, FromLiveStream: true, SourceParentSlot: 120}
	bs.reorderBuffer[122] = &b.Block{Slot: 122, FromLiveStream: true, SourceParentSlot: 121}
	bs.lightbringerGapSlot.Store(120)
	bs.liveGapSinceUnix.Store(time.Now().Add(-(liveGapFallbackWait / 2)).UnixNano())

	waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, shouldFallback := bs.detectLiveGapLocked()
	if waitingSlot != 120 || firstBufferedSlot != 121 || firstBufferedParentSlot != 120 || bufferedCount != 2 || shouldFallback {
		t.Fatalf("expected Lightbringer gap detection to report gap while staying patient before fallback delay expires, got waiting=%d first=%d parent=%d buffered=%d fallback=%v",
			waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, shouldFallback)
	}
}

func TestSetLastExecutedSlotClearsRecoveryWindowImmediatelyWhenDisabled(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	waitingSlot := uint64(125)
	bs.liveForceRPCUntil.Store(waitingSlot)
	bs.liveCooldownUntil.Store(waitingSlot + liveRecoverySlots)

	bs.SetLastExecutedSlot(waitingSlot)

	if got := bs.liveForceRPCUntil.Load(); got != 0 {
		t.Fatalf("expected forced RPC boundary to clear at slot %d, got %d", waitingSlot, got)
	}
	if got := bs.liveCooldownUntil.Load(); got != 0 {
		t.Fatalf("expected disabled recovery window to clear immediately at slot %d, got %d", waitingSlot, got)
	}
}

func TestSetLastExecutedSlotAdvancesDeferredLightbringerFrontier(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.nextSlotToSend = 151
	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLiveStream: true}
	bs.reorderBuffer[152] = &b.Block{Slot: 152, FromLiveStream: true}
	bs.reorderBuffer[154] = &b.Block{Slot: 154, FromLiveStream: true}
	bs.skippedSlots[153] = true
	bs.slotState[151] = slotDone
	bs.slotState[152] = slotDone
	bs.slotState[154] = slotInflight
	bs.retrySlots = []uint64{150, 151, 154}

	bs.SetLastExecutedSlot(153)

	if got := bs.nextSlotToSend; got != 154 {
		t.Fatalf("expected resolved frontier to advance to slot 154, got %d", got)
	}
	if _, exists := bs.reorderBuffer[151]; exists {
		t.Fatalf("expected resolved buffered slot 151 to be pruned")
	}
	if _, exists := bs.reorderBuffer[152]; exists {
		t.Fatalf("expected resolved buffered slot 152 to be pruned")
	}
	if _, exists := bs.reorderBuffer[154]; !exists {
		t.Fatalf("expected unresolved buffered slot 154 to remain")
	}
	if bs.skippedSlots[153] {
		t.Fatalf("expected resolved skipped slot 153 to be pruned")
	}
	if _, exists := bs.slotState[151]; exists {
		t.Fatalf("expected resolved slot state 151 to be pruned")
	}
	if _, exists := bs.slotState[152]; exists {
		t.Fatalf("expected resolved slot state 152 to be pruned")
	}
	if _, exists := bs.slotState[154]; !exists {
		t.Fatalf("expected unresolved slot state 154 to remain")
	}
	if len(bs.retrySlots) != 1 || bs.retrySlots[0] != 154 {
		t.Fatalf("expected only unresolved retries to remain, got %+v", bs.retrySlots)
	}
}

func TestForceRPCForCatchupRewindsConsensusManagedFrontier(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.liveStreamActive.Store(true)
	bs.lastExecutedSlot.Store(120)
	bs.nextSlotToSend = 150
	bs.reorderBuffer[121] = &b.Block{Slot: 121, FromLiveStream: true}
	bs.reorderBuffer[149] = &b.Block{Slot: 149, FromLiveStream: true}
	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLiveStream: false}
	bs.slotState[121] = slotDone
	bs.slotState[149] = slotDone
	bs.slotState[151] = slotInflight
	bs.retrySlots = []uint64{119, 121, 149, 151}

	bs.forceRPCForCatchup(64)

	if got := bs.nextSlotToSend; got != 121 {
		t.Fatalf("expected RPC catchup frontier to rewind to replay's next slot 121, got %d", got)
	}
	if bs.liveStreamActive.Load() {
		t.Fatalf("expected Lightbringer to be marked inactive")
	}
	if !bs.liveNeedRPCResume.Load() {
		t.Fatalf("expected scheduler to be told to resume RPC from the rewound frontier")
	}
	if _, exists := bs.reorderBuffer[121]; exists {
		t.Fatalf("expected Lightbringer slot 121 to be dropped for RPC refetch")
	}
	if _, exists := bs.reorderBuffer[149]; exists {
		t.Fatalf("expected Lightbringer slot 149 to be dropped for RPC refetch")
	}
	if _, exists := bs.reorderBuffer[151]; !exists {
		t.Fatalf("expected RPC buffered slot 151 to remain")
	}
	if _, exists := bs.slotState[121]; exists {
		t.Fatalf("expected slot state 121 to be cleared")
	}
	if _, exists := bs.slotState[149]; exists {
		t.Fatalf("expected slot state 149 to be cleared")
	}
	if _, exists := bs.slotState[151]; exists {
		t.Fatalf("expected consensus-managed catchup to clear future RPC slot state for rescheduling")
	}
	if len(bs.retrySlots) != 1 || bs.retrySlots[0] != 119 {
		t.Fatalf("expected only retries before the replay frontier to remain, got %+v", bs.retrySlots)
	}
}

func TestForceRPCFallbackRewindsConsensusManagedTurbineFrontier(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:      BlockSourceTurbine,
		TurbineBindAddr: "127.0.0.1:8001",
		StartSlot:       100,
		EndSlot:         200,
	})

	bs.liveStreamActive.Store(true)
	bs.liveHandoffSlot.Store(121)
	bs.lastExecutedSlot.Store(120)
	bs.confirmedTip.Store(180)
	bs.nextSlotToSend = 150
	bs.reorderBuffer[121] = &b.Block{Slot: 121, FromLiveStream: true}
	bs.reorderBuffer[149] = &b.Block{Slot: 149, FromLiveStream: true}
	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLiveStream: false}
	bs.slotState[121] = slotDone
	bs.slotState[149] = slotDone
	bs.slotState[151] = slotInflight
	bs.retrySlots = []uint64{119, 121, 149, 151}

	bs.ForceRPCFallback("consensus_depth_exceeded")

	if got := bs.nextSlotToSend; got != 121 {
		t.Fatalf("expected RPC fallback frontier to rewind to replay's next slot 121, got %d", got)
	}
	if bs.liveStreamActive.Load() {
		t.Fatalf("expected turbine to be marked inactive")
	}
	if got := bs.liveHandoffSlot.Load(); got != 0 {
		t.Fatalf("expected turbine handoff to be cleared, got %d", got)
	}
	if !bs.liveNeedRPCResume.Load() {
		t.Fatalf("expected scheduler to resume RPC from the rewound frontier")
	}
	if _, exists := bs.reorderBuffer[121]; exists {
		t.Fatalf("expected turbine slot 121 to be dropped for RPC refetch")
	}
	if _, exists := bs.reorderBuffer[149]; exists {
		t.Fatalf("expected turbine slot 149 to be dropped for RPC refetch")
	}
	if _, exists := bs.reorderBuffer[151]; !exists {
		t.Fatalf("expected buffered RPC slot 151 to remain")
	}
	if _, exists := bs.slotState[151]; exists {
		t.Fatalf("expected future slot state to be cleared for RPC rescheduling")
	}
	if len(bs.retrySlots) != 1 || bs.retrySlots[0] != 119 {
		t.Fatalf("expected only retries before the replay frontier to remain, got %+v", bs.retrySlots)
	}
}

func TestForceRPCFallbackRewindsActiveTurbineFrontierToReplayProgress(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:      BlockSourceTurbine,
		TurbineBindAddr: "127.0.0.1:8001",
		StartSlot:       100,
		EndSlot:         200,
	})

	bs.liveStreamActive.Store(true)
	bs.liveHandoffSlot.Store(121)
	bs.lastExecutedSlot.Store(120)
	bs.confirmedTip.Store(180)
	bs.nextSlotToSend = 150
	bs.reorderBuffer[121] = &b.Block{Slot: 121, FromLiveStream: true}
	bs.reorderBuffer[149] = &b.Block{Slot: 149, FromLiveStream: true}
	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLiveStream: false}
	bs.slotState[121] = slotDone
	bs.slotState[149] = slotDone
	bs.slotState[151] = slotInflight
	bs.retrySlots = []uint64{119, 121, 149, 151}

	bs.forceRPCForCatchup(64)

	if got := bs.nextSlotToSend; got != 121 {
		t.Fatalf("expected active turbine fallback to rewind to replay's next slot 121, got %d", got)
	}
	if bs.liveStreamActive.Load() {
		t.Fatalf("expected turbine to be marked inactive")
	}
	if !bs.liveNeedRPCResume.Load() {
		t.Fatalf("expected scheduler to resume RPC from the rewound frontier")
	}
	if _, exists := bs.reorderBuffer[121]; exists {
		t.Fatalf("expected queued turbine slot 121 to be dropped for RPC refetch")
	}
	if _, exists := bs.reorderBuffer[149]; exists {
		t.Fatalf("expected queued turbine slot 149 to be dropped for RPC refetch")
	}
	if _, exists := bs.reorderBuffer[151]; !exists {
		t.Fatalf("expected buffered RPC slot 151 to remain")
	}
	if _, exists := bs.slotState[151]; exists {
		t.Fatalf("expected future slot state to be cleared for RPC rescheduling")
	}
	if len(bs.retrySlots) != 1 || bs.retrySlots[0] != 119 {
		t.Fatalf("expected only retries before the replay frontier to remain, got %+v", bs.retrySlots)
	}
}

func TestForceRPCForCatchupKeepsPendingHandoffEmissionFrontier(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.liveHandoffSlot.Store(121)
	bs.lastExecutedSlot.Store(120)
	bs.nextSlotToSend = 150
	bs.reorderBuffer[150] = &b.Block{Slot: 150, FromLiveStream: true}
	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLiveStream: false}
	bs.slotState[150] = slotDone
	bs.slotState[151] = slotInflight

	bs.forceRPCForCatchup(64)

	if got := bs.nextSlotToSend; got != 150 {
		t.Fatalf("expected pending handoff fallback to keep emitted RPC frontier 150, got %d", got)
	}
	if got := bs.liveHandoffSlot.Load(); got != 0 {
		t.Fatalf("expected pending handoff to be cleared, got %d", got)
	}
	if !bs.liveNeedRPCResume.Load() {
		t.Fatalf("expected scheduler to resume RPC from the current emission frontier")
	}
	if _, exists := bs.reorderBuffer[150]; exists {
		t.Fatalf("expected pending Lightbringer slot 150 to be dropped")
	}
	if _, exists := bs.reorderBuffer[151]; !exists {
		t.Fatalf("expected buffered RPC slot 151 to remain")
	}
}

func TestEmitOrderedBlocksDropsStaleLiveStreamGeneration(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:      BlockSourceTurbine,
		TurbineBindAddr: "127.0.0.1:0",
		StartSlot:       100,
		EndSlot:         200,
	})

	bs.isNearTip.Store(true)
	bs.liveStreamActive.Store(true)
	bs.liveHandoffSlot.Store(100)
	staleGeneration := bs.liveResultGeneration.Load()
	bs.invalidateLiveStreamResults()

	done := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(done)
	}()

	bs.resultQueue <- fetchResult{
		slot:                 100,
		block:                &b.Block{Slot: 100, FromLiveStream: true, SourceParentSlot: 99},
		liveStreamGeneration: staleGeneration,
	}
	close(bs.resultQueue)
	<-done

	if len(bs.streamChan) != 0 {
		t.Fatalf("expected stale turbine result to be dropped without emission")
	}
	if _, exists := bs.reorderBuffer[100]; exists {
		t.Fatalf("expected stale turbine result not to enter reorder buffer")
	}
	if got := bs.nextSlotToSend; got != 100 {
		t.Fatalf("expected stale turbine result to leave emission frontier at 100, got %d", got)
	}
}

func TestEmitOrderedBlocksDropsResultsBehindEmissionFrontier(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceRpc,
		StartSlot:  100,
		EndSlot:    200,
	})

	bs.nextSlotToSend = 105
	bs.slotState[103] = slotInflight
	bs.inflightStart[103] = time.Now()

	done := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(done)
	}()

	bs.resultQueue <- fetchResult{
		slot:  103,
		block: &b.Block{Slot: 103},
	}
	close(bs.resultQueue)
	<-done

	if len(bs.streamChan) != 0 {
		t.Fatalf("expected stale result to be dropped without emission")
	}
	if _, exists := bs.reorderBuffer[103]; exists {
		t.Fatalf("expected stale result not to enter reorder buffer")
	}
	if _, exists := bs.slotState[103]; exists {
		t.Fatalf("expected stale slot state to be cleared")
	}
}

func TestIsLightbringerReconnectCancelRecognizesGrpcCanceledStatus(t *testing.T) {
	err := status.Error(codes.Canceled, "context canceled")
	if !isLiveStreamReconnectCancel(err) {
		t.Fatalf("expected gRPC canceled status to be treated as a reconnect cancel")
	}
}

func TestDetectLightbringerGapResetsReconnectLatchForNewWaitingSlot(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.liveStreamActive.Store(true)
	bs.reorderBuffer[121] = &b.Block{Slot: 121, FromLiveStream: true, SourceParentSlot: 120}
	bs.reorderBuffer[122] = &b.Block{Slot: 122, FromLiveStream: true, SourceParentSlot: 121}
	bs.nextSlotToSend = 120
	bs.lightbringerGapSlot.Store(120)
	bs.liveGapSinceUnix.Store(time.Now().Add(-6 * time.Second).UnixNano())
	bs.liveGapLastLogUnix.Store(time.Now().Add(-3 * time.Second).Unix())
	bs.lightbringerGapReconnectSlot.Store(120)

	delete(bs.reorderBuffer, 121)
	delete(bs.reorderBuffer, 122)
	bs.reorderBuffer[126] = &b.Block{Slot: 126, FromLiveStream: true, SourceParentSlot: 125}
	bs.reorderBuffer[127] = &b.Block{Slot: 127, FromLiveStream: true, SourceParentSlot: 126}
	bs.nextSlotToSend = 125

	waitingSlot, _, _, _, shouldFallback := bs.detectLiveGapLocked()
	if waitingSlot != 125 || shouldFallback {
		t.Fatalf("expected first observation of a new gap to arm tracking only while reporting the waiting slot, got waitingSlot=%d shouldFallback=%v", waitingSlot, shouldFallback)
	}
	if got := bs.lightbringerGapSlot.Load(); got != 125 {
		t.Fatalf("expected new gap slot 125 to be tracked, got %d", got)
	}
	if got := bs.lightbringerGapReconnectSlot.Load(); got != 0 {
		t.Fatalf("expected reconnect latch to reset for new gap, got %d", got)
	}
	if got := bs.liveGapLastLogUnix.Load(); got != 0 {
		t.Fatalf("expected gap log throttle to reset for new gap, got %d", got)
	}
}

func TestHandleDetectedLightbringerGapForcesRPCBeforeLightbringerIsActive(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.liveHandoffSlot.Store(120)

	bs.handleDetectedLiveGap(125, 126, 125, 4)

	if got := bs.liveForceRPCUntil.Load(); got != 125 {
		t.Fatalf("expected pending Lightbringer gap to force RPC until slot 125, got %d", got)
	}
	if got := bs.liveCooldownUntil.Load(); got != 125+liveRecoverySlots {
		t.Fatalf("expected pending Lightbringer gap to set cooldown boundary from the configured recovery window, got %d", got)
	}
	if got := bs.liveHandoffSlot.Load(); got != 0 {
		t.Fatalf("expected pending handoff to be cleared after forcing RPC, got %d", got)
	}
}

func TestShouldProbeAbsentConfirmationRequiresDepth(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceRpc,
		StartSlot:  100,
		EndSlot:    200,
	})

	slot := uint64(150)
	bs.confirmedTip.Store(slot + rpcSkipConfirmDepth - 1)
	if bs.shouldProbeAbsentConfirmation(slot) {
		t.Fatalf("expected absent confirmation probe to stay disabled before the slot is far enough behind tip")
	}

	bs.confirmedTip.Store(slot + rpcSkipConfirmDepth)
	if !bs.shouldProbeAbsentConfirmation(slot) {
		t.Fatalf("expected absent confirmation probe once the slot is safely behind confirmed tip")
	}
}

func TestShouldFinalizeSkippedSlotRequiresConfirmedAbsenceProbe(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceRpc,
		StartSlot:  100,
		EndSlot:    200,
	})

	slot := uint64(150)
	bs.confirmedTip.Store(slot + rpcSkipConfirmDepth)
	bs.waitingSlotErrors[slot] = &slotErrorInfo{
		slot:           slot,
		retryCount:     99,
		firstSeenAt:    time.Now().Add(-time.Hour),
		lastSeenAt:     time.Now(),
		lastErrorClass: "skipped",
	}

	if bs.shouldFinalizeSkippedSlot(slot, false) {
		t.Fatalf("expected skipped slot to remain provisional without a confirmed absence probe")
	}
	if !bs.shouldFinalizeSkippedSlot(slot, true) {
		t.Fatalf("expected skipped slot to finalize once absence is explicitly confirmed")
	}
}

func TestShouldFinalizeSkippedSlotAcceptsConfirmedSlotNotAvailable(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceRpc,
		StartSlot:  100,
		EndSlot:    200,
	})

	slot := uint64(150)
	bs.confirmedTip.Store(slot + rpcSkipConfirmDepth)
	bs.waitingSlotErrors[slot] = &slotErrorInfo{
		slot:           slot,
		retryCount:     3,
		firstSeenAt:    time.Now().Add(-time.Minute),
		lastSeenAt:     time.Now(),
		lastErrorClass: "slot_not_available",
	}

	if !bs.shouldFinalizeSkippedSlot(slot, true) {
		t.Fatalf("expected confirmed slot-not-available observation to finalize as skipped")
	}
}

func TestRescueStaleWaitingSlotRequeuesHungSlot(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceRpc,
		StartSlot:  100,
		EndSlot:    200,
	})

	slot := uint64(150)
	bs.slotState[slot] = slotInflight
	bs.inflightStart[slot] = time.Now().Add(-staleWaitingSlotRetry - time.Second)

	if !bs.rescueStaleWaitingSlot(slot, staleWaitingSlotRetry) {
		t.Fatalf("expected stale waiting slot to be rescued")
	}
	if _, exists := bs.slotState[slot]; exists {
		t.Fatalf("expected rescued slot state to be cleared")
	}
	if _, exists := bs.inflightStart[slot]; exists {
		t.Fatalf("expected rescued inflight timestamp to be cleared")
	}

	retries := bs.getRetrySlots()
	if len(retries) != 1 || retries[0] != slot {
		t.Fatalf("expected rescued slot %d to be requeued, got %v", slot, retries)
	}
}

func TestStopReasonDistinguishesFiniteCompletionFromUnexpectedLiveEnd(t *testing.T) {
	finite := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceRpc,
		StartSlot:  100,
		EndSlot:    200,
	})
	finite.setStopReason(blockSourceStopReasonCompleted, 200)
	if !finite.Completed() {
		t.Fatalf("expected finite block source to report completion")
	}
	if got := finite.StopReason(); !strings.Contains(got, "completed finite replay") {
		t.Fatalf("expected finite completion reason, got %q", got)
	}

	live := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceLightbringer,
		StartSlot:  100,
		EndSlot:    uint64(math.MaxUint64),
	})
	live.setStopReason(blockSourceStopReasonUnexpectedLiveEnd, 150)
	if live.Completed() {
		t.Fatalf("expected unexpected live stop to not report completion")
	}
	if got := live.StopReason(); !strings.Contains(got, "unexpectedly in live mode") {
		t.Fatalf("expected unexpected live stop reason, got %q", got)
	}
}

func TestSchedulerExitsOnExternallySetTerminalStopReason(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceRpc,
		StartSlot:  100,
		EndSlot:    200,
	})
	defer bs.Stop()

	// Candidate validation and consensus callbacks run outside the scheduler.
	// Once either records a terminal reason, Start must enter its ordinary
	// teardown path instead of waiting for the unrelated stall timeout.
	bs.setStopReason(blockSourceStopReasonInvalidAlpenglowCertificate, 123)
	done := make(chan struct{})
	go func() {
		bs.scheduler()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler ignored externally recorded terminal stop reason")
	}
}
