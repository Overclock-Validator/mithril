package blockstream

import (
	"crypto/ed25519"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLightbringerBlockConnectsLocked(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceLightbringer,
		StartSlot:  100,
		EndSlot:    200,
	})

	bs.lastEmittedBlockSlot = 150

	if !bs.lightbringerBlockConnectsLocked(&b.Block{Slot: 151, FromLightbringer: true, SourceParentSlot: 150}) {
		t.Fatalf("expected Lightbringer block with matching parent slot to connect")
	}
	if bs.lightbringerBlockConnectsLocked(&b.Block{Slot: 151, FromLightbringer: true, SourceParentSlot: 149}) {
		t.Fatalf("expected Lightbringer block with mismatched parent slot to be rejected")
	}
	if bs.lightbringerBlockConnectsLocked(&b.Block{Slot: 151, FromLightbringer: true, SourceParentSlot: 0}) {
		t.Fatalf("expected Lightbringer block without parent metadata to be rejected once an anchor exists")
	}
	if !bs.lightbringerBlockConnectsLocked(&b.Block{Slot: 151, FromLightbringer: false}) {
		t.Fatalf("expected RPC block to pass through ancestry guard")
	}
}

func TestWaitingLightbringerParentIdentityMismatchLocked(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    100,
		EndSlot:                      200,
	})
	executedID := solana.Hash{1}
	observedID := solana.Hash{2}
	bs.lastEmittedBlockSlot = 150
	bs.lastEmittedBlockID = executedID
	bs.hasLastEmittedBlockID = true
	bs.nextSlotToSend = 151
	bs.reorderBuffer[151] = &b.Block{
		Slot:                      151,
		FromLightbringer:          true,
		SourceParentSlot:          150,
		HasAlpenglowParentBlockID: true,
		AlpenglowParentBlockID:    observedID,
	}

	waiting, parent, observed, executed, mismatch := bs.waitingLightbringerParentIdentityMismatchLocked()
	if !mismatch || waiting != 151 || parent != 150 || observed != observedID || executed != executedID {
		t.Fatalf("unexpected identity mismatch: waiting=%d parent=%d observed=%s executed=%s mismatch=%v",
			waiting, parent, observed, executed, mismatch)
	}
	if bs.lightbringerBlockConnectsLocked(bs.reorderBuffer[151]) {
		t.Fatal("same-slot child with a different parent block ID must not connect")
	}
}

func TestNextBlockSerializesParentIdentityControlAfterEmittedBlocks(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		TurbineRepairOnly:            true,
		StartSlot:                    100,
		EndSlot:                      200,
	})
	executedID := solana.Hash{1}
	selectedID := solana.Hash{2}
	bs.streamChan <- &b.Block{Slot: 150, HasAlpenglowBlockID: true, AlpenglowBlockID: executedID}
	bs.controlPending = true
	bs.controlChan <- blockSourceControl{
		kind:             blockSourceControlParentIdentityMismatch,
		waitingSlot:      151,
		parentSlot:       150,
		observedParentID: selectedID,
		executedParentID: executedID,
	}

	called := false
	bs.SetParentIdentityMismatchHandler(func(waitingSlot, parentSlot uint64, observedParentID, executedParentID solana.Hash) bool {
		called = true
		bs.RewindAlpenglowFork(149, solana.Hash{9}, alpenglow.BlockID{Slot: parentSlot, Hash: observedParentID})
		bs.streamChan <- &b.Block{Slot: parentSlot, HasAlpenglowBlockID: true, AlpenglowBlockID: observedParentID}
		return true
	})

	first := bs.NextBlock()
	if first == nil || first.Slot != 150 || solana.Hash(first.AlpenglowBlockID) != executedID {
		t.Fatalf("expected already-emitted block before control, got %+v", first)
	}
	if called {
		t.Fatal("fork control ran before replay consumed the preceding block")
	}

	replacement := bs.NextBlock()
	if !called {
		t.Fatal("expected fork control to run on the NextBlock caller")
	}
	if replacement == nil || replacement.Slot != 150 || solana.Hash(replacement.AlpenglowBlockID) != selectedID {
		t.Fatalf("expected replacement branch block after rewind, got %+v", replacement)
	}
}

func TestNextBlockFailsClosedWhenParentIdentitySwitchUnavailable(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:      BlockSourceTurbine,
		TurbineBindAddr: "127.0.0.1:0",
		StartSlot:       100,
		EndSlot:         200,
	})
	bs.controlPending = true
	bs.controlChan <- blockSourceControl{
		kind:             blockSourceControlParentIdentityMismatch,
		waitingSlot:      151,
		parentSlot:       150,
		observedParentID: solana.Hash{2},
		executedParentID: solana.Hash{1},
	}

	if block := bs.NextBlock(); block != nil {
		t.Fatalf("expected unavailable fork switch to stop delivery, got %+v", block)
	}
	if err := bs.Err(); err == nil {
		t.Fatal("expected an explicit safety error")
	}
	if got := bs.StopReason(); !strings.Contains(got, "fork switch unavailable") {
		t.Fatalf("unexpected stop reason: %s", got)
	}
}

func TestRewindAlpenglowForkClearsSpeculativeDeliverySuffix(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		TurbineRepairOnly:            true,
		StartSlot:                    100,
		EndSlot:                      200,
	})
	anchorID := solana.Hash{3}
	targetID := solana.Hash{4}
	bs.lastEmittedBlockSlot = 105
	bs.lastEmittedBlockID = solana.Hash{9}
	bs.hasLastEmittedBlockID = true
	bs.nextSlotToSend = 106
	bs.reorderBuffer[106] = &b.Block{Slot: 106}
	bs.skippedSlots[107] = true
	bs.slotState[106] = slotDone

	bs.RewindAlpenglowFork(104, anchorID, alpenglow.BlockID{Slot: 106, Hash: targetID})

	if bs.nextSlotToSend != 105 || bs.lastEmittedBlockSlot != 104 || bs.lastEmittedBlockID != anchorID {
		t.Fatalf("unexpected rewound frontier: next=%d last=%d id=%s", bs.nextSlotToSend, bs.lastEmittedBlockSlot, bs.lastEmittedBlockID)
	}
	if len(bs.reorderBuffer) != 0 || len(bs.skippedSlots) != 0 || len(bs.slotState) != 0 {
		t.Fatalf("speculative delivery suffix survived rewind: buffered=%d skipped=%d states=%d",
			len(bs.reorderBuffer), len(bs.skippedSlots), len(bs.slotState))
	}
	if got := bs.knownAlpenglowBlockIDs[106]; got != targetID {
		t.Fatalf("target block id = %s, want %s", got, targetID)
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
	bs.lightbringerConnected.Store(true)
	bs.lightbringerLastStreamSlot.Store(150)

	source, status, handoff := bs.currentSourceSnapshot()
	if source != "rpc" || handoff != 0 || !strings.Contains(status, "turbine connected") {
		t.Fatalf("expected pre-handoff turbine status, got source=%q status=%q handoff=%d", source, status, handoff)
	}

	bs.lightbringerActive.Store(true)
	bs.lightbringerHandoffSlot.Store(151)

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
	bs.lightbringerActive.Store(true)

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
	bs.lightbringerActive.Store(true)
	bs.reorderBuffer[151] = &b.Block{
		Slot:                151,
		FromLightbringer:    true,
		HasAlpenglowBlockID: true,
		AlpenglowBlockID:    [32]byte(blockID),
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
	bs.lightbringerActive.Store(true)
	bs.reorderBuffer[151] = &b.Block{
		Slot:                151,
		FromLightbringer:    true,
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
	if got := bs.lightbringerRepairSlot.Load(); got != 0 {
		t.Fatalf("expected certified mismatch to avoid RPC repair, got repair slot %d", got)
	}
	if len(bs.retrySlots) != 0 {
		t.Fatalf("expected certified mismatch to avoid enqueueing an RPC retry, got %+v", bs.retrySlots)
	}
}

func TestForceRPCForLightbringerParentMismatchClearsBufferedState(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
	})

	bs.lightbringerHandoffSlot.Store(101)
	bs.lightbringerActive.Store(true)

	bs.reorderBuffer[101] = &b.Block{Slot: 101, FromLightbringer: true, SourceParentSlot: 99}
	bs.reorderBuffer[102] = &b.Block{Slot: 102, FromLightbringer: true, SourceParentSlot: 101}
	bs.reorderBuffer[103] = &b.Block{Slot: 103, FromLightbringer: false}
	bs.slotState[101] = slotDone
	bs.slotState[102] = slotDone
	bs.slotState[103] = slotDone
	bs.lightbringerBuffer[104] = &b.Block{Slot: 104, FromLightbringer: true, SourceParentSlot: 102}
	bs.lightbringerBufferOrder = []uint64{104}

	bs.forceRPCForLightbringerParentMismatch(101, 99, 100)

	if got := bs.lightbringerForceRPCUntil.Load(); got != 101 {
		t.Fatalf("expected RPC to be forced until slot 101, got %d", got)
	}
	if got := bs.lightbringerCooldownUntil.Load(); got != 101+lightbringerRecoverySlots {
		t.Fatalf("expected cooldown boundary to match configured recovery window, got %d", got)
	}
	if got := bs.lightbringerHandoffSlot.Load(); got != 0 {
		t.Fatalf("expected handoff slot to be cleared, got %d", got)
	}
	if bs.lightbringerActive.Load() {
		t.Fatalf("expected Lightbringer to be marked inactive after parent mismatch")
	}
	if !bs.lightbringerNeedRPCResume.Load() {
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
	if len(bs.lightbringerBuffer) != 0 || len(bs.lightbringerBufferOrder) != 0 {
		t.Fatalf("expected prefetched Lightbringer buffer to be cleared")
	}
}

func TestRollbackEmissionFrontierMarksSkippedRange(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		TurbineRepairOnly:            true,
		StartSlot:                    100,
		EndSlot:                      200,
	})

	bs.lastEmittedBlockSlot = 6378242
	bs.nextSlotToSend = 6378244
	bs.reorderBuffer[6378244] = &b.Block{
		Slot:             6378244,
		FromLightbringer: true,
		SourceParentSlot: 6378241,
	}
	bs.slotState[6378244] = slotDone

	bs.RollbackEmissionFrontier(6378241, 6378243)

	if bs.lastEmittedBlockSlot != 6378241 {
		t.Fatalf("lastEmittedBlockSlot = %d, want 6378241", bs.lastEmittedBlockSlot)
	}
	if bs.nextSlotToSend != 6378244 {
		t.Fatalf("nextSlotToSend = %d, want 6378244", bs.nextSlotToSend)
	}
	if !bs.skippedSlots[6378242] || !bs.skippedSlots[6378243] {
		t.Fatalf("expected slots 6378242 and 6378243 to be marked skipped")
	}
	if _, exists := bs.reorderBuffer[6378244]; !exists {
		t.Fatalf("expected waiting block 6378244 to remain buffered")
	}
	if got := bs.lastExecutedSlot.Load(); got != 6378241 {
		t.Fatalf("lastExecutedSlot = %d, want 6378241", got)
	}
}

func TestNextBlockParentMismatchRepairOnlyUsesHandler(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		TurbineRepairOnly:            true,
		StartSlot:                    100,
		EndSlot:                      200,
	})

	bs.lastEmittedBlockSlot = 6378242
	bs.nextSlotToSend = 6378244
	bs.reorderBuffer[6378244] = &b.Block{
		Slot:             6378244,
		FromLightbringer: true,
		SourceParentSlot: 6378241,
	}
	bs.controlPending = true
	bs.controlChan <- blockSourceControl{
		kind:               blockSourceControlParentMismatch,
		waitingSlot:        6378244,
		observedParentSlot: 6378241,
		expectedParentSlot: 6378242,
	}

	handled := false
	bs.SetParentMismatchHandler(func(waiting, observed, expected uint64) bool {
		if waiting != 6378244 || observed != 6378241 || expected != 6378242 {
			t.Fatalf("unexpected mismatch args: waiting=%d observed=%d expected=%d", waiting, observed, expected)
		}
		bs.RollbackEmissionFrontier(observed, waiting-1)
		handled = true
		bs.streamChan <- bs.reorderBuffer[waiting]
		return true
	})

	replacement := bs.NextBlock()

	if !handled {
		t.Fatalf("expected parent mismatch handler to run")
	}
	if replacement == nil || replacement.Slot != 6378244 {
		t.Fatalf("expected replacement block 6378244 after rollback, got %+v", replacement)
	}
	if _, exists := bs.reorderBuffer[6378244]; !exists {
		t.Fatalf("expected handler rollback to keep waiting block 6378244")
	}
	if !bs.skippedSlots[6378242] {
		t.Fatalf("expected slot 6378242 to be marked skipped")
	}
}

func TestForceRPCForLightbringerParentMismatchRepairOnlyDiscardsWaitingBlock(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		TurbineRepairOnly:            true,
		StartSlot:                    100,
		EndSlot:                      200,
	})

	bs.lastEmittedBlockSlot = 6378242
	bs.nextSlotToSend = 6378244
	bs.reorderBuffer[6378244] = &b.Block{
		Slot:             6378244,
		FromLightbringer: true,
		SourceParentSlot: 6378241,
	}
	bs.slotState[6378244] = slotDone

	bs.forceRPCForLightbringerParentMismatch(6378244, 6378241, 6378242)

	if _, exists := bs.reorderBuffer[6378244]; exists {
		t.Fatalf("expected disconnected repair-only block to be dropped from reorder buffer")
	}
	if _, exists := bs.slotState[6378244]; exists {
		t.Fatalf("expected slot state for disconnected block to be cleared")
	}
	if got := bs.lightbringerForceRPCUntil.Load(); got != 0 {
		t.Fatalf("expected repair-only parent mismatch to avoid RPC fallback, got force_rpc_until=%d", got)
	}
}

func localLeaderBlockSourceTest(t *testing.T, slot uint64, hasCommit bool) *BlockSource {
	t.Helper()
	_, identity, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	leader := solana.PublicKey(identity.Public().(ed25519.PublicKey))
	return NewBlockSource(&BlockSourceOpts{
		SourceType:        BlockSourceTurbine,
		TurbineBindAddr:   "127.0.0.1:0",
		TurbineRepairOnly: true,
		StartSlot:         100,
		EndSlot:           200,
		GossipIdentity:    identity,
		LeaderForSlot: func(s uint64) (solana.PublicKey, bool) {
			if s == slot {
				return leader, true
			}
			return solana.PublicKey{}, false
		},
		HasLocalLeaderCommit: func(s uint64) bool {
			return hasCommit && s == slot
		},
	})
}

func TestSynthesizeLocalLeaderSkipClearsBufferedTurbineBlock(t *testing.T) {
	const slot = uint64(105)
	bs := localLeaderBlockSourceTest(t, slot, true)
	bs.lastEmittedBlockSlot = slot - 1
	bs.nextSlotToSend = slot
	bs.reorderBuffer[slot] = &b.Block{
		Slot:             slot,
		FromLightbringer: true,
		SourceParentSlot: slot - 1,
	}
	bs.slotState[slot] = slotDone

	bs.reorderMu.Lock()
	if !bs.synthesizeLocalLeaderSkipLocked() {
		t.Fatal("expected local leader skip synthesis")
	}
	bs.reorderMu.Unlock()

	if _, exists := bs.reorderBuffer[slot]; exists {
		t.Fatal("expected buffered turbine block to be cleared")
	}
	if !bs.skippedSlots[slot] || !bs.lightbringerSynthesizedSkips[slot] {
		t.Fatal("expected local leader skip markers")
	}
	if _, exists := bs.slotState[slot]; exists {
		t.Fatal("expected slot state to be cleared after releasing turbine slot")
	}
}

func TestIngestLiveShredBlockDefersOwnLeaderSlot(t *testing.T) {
	const slot = uint64(105)
	bs := localLeaderBlockSourceTest(t, slot, false)
	bs.nextSlotToSend = slot
	bs.lightbringerHandoffSlot.Store(slot)
	bs.isNearTip.Store(true)
	bs.lightbringerActive.Store(true)

	blk := &b.Block{Slot: slot, FromLightbringer: true, SourceParentSlot: slot - 1}
	if !bs.ingestLiveShredBlock(blk) {
		t.Fatal("expected ingest to succeed without blocking")
	}
	select {
	case <-bs.resultQueue:
		t.Fatal("expected own-leader turbine block to be dropped before result queue")
	default:
	}
}

func TestNotifyLocalLeaderCommitWakesSkipSynthesis(t *testing.T) {
	const slot = uint64(105)
	bs := localLeaderBlockSourceTest(t, slot, true)
	bs.nextSlotToSend = slot
	bs.lastEmittedBlockSlot = slot - 1

	done := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(done)
	}()

	bs.NotifyLocalLeaderCommit(slot)
	close(bs.resultQueue)
	<-done

	skip := bs.NextBlock()
	if skip == nil || !skip.IsSkipped || skip.Slot != slot {
		t.Fatalf("expected local leader skip for slot %d, got %+v", slot, skip)
	}
	if !skip.FromLightbringer {
		t.Fatal("expected local leader skip to be marked as live-stream sourced")
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
	bs.lightbringerConnected.Store(true)
	bs.lightbringerHandoffSlot.Store(121)
	bs.lightbringerActive.Store(true)
	bs.nextSlotToSend = 122
	bs.lastEmittedBlockSlot = 121
	bs.reorderBuffer[123] = &b.Block{Slot: 123, FromLightbringer: true, SourceParentSlot: 122}
	bs.reorderBuffer[124] = &b.Block{Slot: 124, FromLightbringer: true, SourceParentSlot: 123}
	bs.reorderBuffer[125] = &b.Block{Slot: 125, FromLightbringer: false}
	bs.slotState[123] = slotDone
	bs.slotState[124] = slotDone
	bs.slotState[125] = slotDone
	bs.lightbringerBuffer[126] = &b.Block{Slot: 126, FromLightbringer: true, SourceParentSlot: 125}
	bs.lightbringerBufferOrder = []uint64{126}
	oldGeneration := bs.lightbringerResultGeneration.Load()

	bs.handleLiveShredStreamClosed("test reconnect")

	if got := bs.lightbringerResultGeneration.Load(); got != oldGeneration+1 {
		t.Fatalf("expected live stream generation to advance, got %d want %d", got, oldGeneration+1)
	}
	if got := bs.lightbringerForceRPCUntil.Load(); got != 122 {
		t.Fatalf("expected RPC to be forced from waiting slot 122, got %d", got)
	}
	if got := bs.lightbringerHandoffSlot.Load(); got != 0 {
		t.Fatalf("expected handoff slot to be cleared, got %d", got)
	}
	if bs.lightbringerActive.Load() {
		t.Fatalf("expected turbine to be marked inactive after stream close")
	}
	if !bs.lightbringerNeedRPCResume.Load() {
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
	if len(bs.lightbringerBuffer) != 0 || len(bs.lightbringerBufferOrder) != 0 {
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
		bs.lightbringerBuffer[slot] = &b.Block{Slot: slot, FromLightbringer: true, SourceParentSlot: parentSlot}
		bs.lightbringerBufferOrder = append(bs.lightbringerBufferOrder, slot)
	}

	blocks, handoffSlot, prepared := bs.prepareLightbringerHandoff(151, 150)
	if !prepared {
		t.Fatalf("expected handoff to prepare across a skipped gap")
	}
	if handoffSlot != 151 {
		t.Fatalf("expected handoff slot 151, got %d", handoffSlot)
	}
	if got := bs.lightbringerHandoffSlot.Load(); got != 151 {
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
		bs.lightbringerBuffer[slot] = &b.Block{Slot: slot, FromLightbringer: true, SourceParentSlot: parentSlot}
		bs.lightbringerBufferOrder = append(bs.lightbringerBufferOrder, slot)
	}

	if blocks, handoffSlot, prepared := bs.prepareLightbringerHandoff(151, 150); prepared || handoffSlot != 0 || len(blocks) != 0 {
		t.Fatalf("expected handoff to stay unarmed without the minimum Lightbringer runway, got prepared=%v handoff=%d blocks=%+v",
			prepared, handoffSlot, blocks)
	}
	if got := bs.lightbringerHandoffSlot.Load(); got != 0 {
		t.Fatalf("expected stored handoff slot to remain unset without enough runway, got %d", got)
	}
}

func TestPrepareLightbringerHandoffAllowsLiveEdgeRunwayAtTip(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceLightbringer,
		LightbringerEndpoint:         "127.0.0.1:50051",
		StartSlot:                    100,
		EndSlot:                      200,
		ConsensusManagedLightbringer: true,
	})

	bs.isNearTip.Store(true)
	bs.lightbringerConnected.Store(true)
	bs.lastExecutedSlot.Store(150)
	bs.confirmedTip.Store(151)
	bs.lightbringerLastStreamSlot.Store(151)
	bs.lastEmittedBlockSlot = 150
	bs.lightbringerBuffer[151] = &b.Block{Slot: 151, FromLightbringer: true, SourceParentSlot: 150}
	bs.lightbringerBufferOrder = append(bs.lightbringerBufferOrder, 151)

	reason := bs.lightbringerHandoffWaitReason(151, 150)
	if !strings.Contains(reason, "handoff-ready runway buffered through slot 151") {
		t.Fatalf("expected live-edge runway to be handoff-ready, got %q", reason)
	}

	blocks, handoffSlot, prepared := bs.prepareLightbringerHandoff(151, 150)
	if !prepared {
		t.Fatalf("expected consensus-managed handoff to prepare at the live edge")
	}
	if handoffSlot != 151 {
		t.Fatalf("expected handoff slot 151, got %d", handoffSlot)
	}
	if len(blocks) != 1 || blocks[0].Slot != 151 {
		t.Fatalf("expected single live-edge Lightbringer block to be enqueued, got %+v", blocks)
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
	bs.lightbringerConnected.Store(true)
	bs.lastExecutedSlot.Store(150)
	bs.confirmedTip.Store(151)
	bs.lightbringerLastStreamSlot.Store(151)
	bs.lastEmittedBlockSlot = 150
	bs.lightbringerBuffer[151] = &b.Block{Slot: 151, FromLightbringer: true, SourceParentSlot: 150}
	bs.lightbringerBufferOrder = append(bs.lightbringerBufferOrder, 151)

	reason := bs.lightbringerHandoffWaitReason(151, 150)
	if !strings.Contains(reason, "handoff-ready runway buffered through slot 151") {
		t.Fatalf("expected live-edge turbine runway to be handoff-ready, got %q", reason)
	}

	blocks, handoffSlot, prepared := bs.prepareLightbringerHandoff(151, 150)
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

func TestTurbineHandoffMaxReplayGapUsesFullNearTipWindow(t *testing.T) {
	turbine := NewBlockSource(&BlockSourceOpts{
		SourceType:       BlockSourceTurbine,
		TurbineBindAddr:  "127.0.0.1:8001",
		StartSlot:        100,
		EndSlot:          200,
		NearTipThreshold: 32,
		CatchupThreshold: 64,
	})
	if got := turbine.lightbringerHandoffMaxReplayGap(); got != 32 {
		t.Fatalf("expected turbine handoff gap 32, got %d", got)
	}

	lb := NewBlockSource(&BlockSourceOpts{
		SourceType:           BlockSourceLightbringer,
		LightbringerEndpoint: "127.0.0.1:50051",
		StartSlot:            100,
		EndSlot:              200,
		NearTipThreshold:     32,
		CatchupThreshold:     64,
	})
	if got := lb.lightbringerHandoffMaxReplayGap(); got != 16 {
		t.Fatalf("expected lightbringer handoff gap 16, got %d", got)
	}
}

func TestPrepareTurbineHandoffAllowsNearTipReplayGapWithinThreshold(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:       BlockSourceTurbine,
		TurbineBindAddr:  "127.0.0.1:8001",
		StartSlot:        100,
		EndSlot:          5000,
		NearTipThreshold: 32,
		CatchupThreshold: 64,
	})

	bs.isNearTip.Store(true)
	bs.lightbringerConnected.Store(true)
	bs.lastExecutedSlot.Store(3732316)
	bs.confirmedTip.Store(3732339)
	bs.lightbringerLastStreamSlot.Store(3732339)
	bs.nextSlotToSend = 3732317
	bs.lastEmittedBlockSlot = 3732316

	for slot := uint64(3732317); slot <= 3732324; slot++ {
		parentSlot := slot - 1
		bs.lightbringerBuffer[slot] = &b.Block{Slot: slot, FromLightbringer: true, SourceParentSlot: parentSlot}
		bs.lightbringerBufferOrder = append(bs.lightbringerBufferOrder, slot)
	}

	ok, gap, maxGap, _, _ := bs.lightbringerHandoffReplayGapOK()
	if !ok || gap != 23 || maxGap != 32 {
		t.Fatalf("expected replay gap ok with gap=23 max=32, got ok=%v gap=%d max=%d", ok, gap, maxGap)
	}

	blocks, handoffSlot, prepared := bs.prepareLightbringerHandoff(3732317, 3732316)
	if !prepared || handoffSlot != 3732317 || len(blocks) != 8 {
		t.Fatalf("expected turbine handoff with gap=23, got prepared=%v handoff=%d blocks=%d", prepared, handoffSlot, len(blocks))
	}
}

func TestMaybeKickTurbineRepairWhenIdlePrioritizesWaitingFrontier(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:8001",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    100,
		EndSlot:                      5000,
	})

	bs.isNearTip.Store(true)
	bs.lightbringerConnected.Store(true)
	bs.lastExecutedSlot.Store(3732316)
	bs.confirmedTip.Store(3732339)
	bs.nextSlotToSend = 3732317

	receiver := turbine.NewUDPReceiver("127.0.0.1:0")
	bs.attachAlpenglowBlockIDHintsToReceiver(receiver)

	bs.maybeKickTurbineRepairWhenIdle()
	if got := receiver.Stats().PriorityRepairSlots; got == 0 {
		t.Fatalf("expected turbine repair to be prioritized for idle connected receiver, got priority_slots=%d", got)
	}
}

func TestPrepareLightbringerHandoffKeepsMinimumRunwayWhenLightbringerLagsTip(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceLightbringer,
		LightbringerEndpoint:         "127.0.0.1:50051",
		StartSlot:                    100,
		EndSlot:                      200,
		ConsensusManagedLightbringer: true,
	})

	bs.isNearTip.Store(true)
	bs.lightbringerConnected.Store(true)
	bs.lastExecutedSlot.Store(150)
	bs.confirmedTip.Store(157)
	bs.lightbringerLastStreamSlot.Store(151)
	bs.lastEmittedBlockSlot = 150
	bs.lightbringerBuffer[151] = &b.Block{Slot: 151, FromLightbringer: true, SourceParentSlot: 150}
	bs.lightbringerBufferOrder = append(bs.lightbringerBufferOrder, 151)

	blocks, handoffSlot, prepared := bs.prepareLightbringerHandoff(151, 150)
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
		bs.lightbringerBuffer[slot] = &b.Block{Slot: slot, FromLightbringer: true, SourceParentSlot: parentSlot}
		bs.lightbringerBufferOrder = append(bs.lightbringerBufferOrder, slot)
	}

	if blocks, handoffSlot, prepared := bs.prepareLightbringerHandoff(151, 150); prepared || handoffSlot != 0 || len(blocks) != 0 {
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
	bs.lightbringerBuffer[151] = &b.Block{Slot: 151, FromLightbringer: true, SourceParentSlot: 150}
	bs.lightbringerBuffer[158] = &b.Block{Slot: 158, FromLightbringer: true, SourceParentSlot: 157}
	bs.lightbringerBufferOrder = []uint64{151, 158}

	if blocks, handoffSlot, prepared := bs.prepareLightbringerHandoff(151, 150); prepared || handoffSlot != 0 || len(blocks) != 0 {
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
		bs.lightbringerBuffer[slot] = &b.Block{Slot: slot, FromLightbringer: true, SourceParentSlot: parentSlot}
		bs.lightbringerBufferOrder = append(bs.lightbringerBufferOrder, slot)
	}

	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLightbringer: false}
	bs.reorderBuffer[152] = &b.Block{Slot: 152, FromLightbringer: false}
	bs.skippedSlots[153] = true
	bs.slotState[151] = slotInflight
	bs.slotState[152] = slotDone
	bs.retrySlots = []uint64{149, 151, 152}

	blocks, handoffSlot, prepared := bs.prepareLightbringerHandoff(151, 150)
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
		if !blk.FromLightbringer {
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
	bs.lightbringerConnected.Store(true)
	bs.lastExecutedSlot.Store(101)
	bs.confirmedTip.Store(117)
	bs.lightbringerLastStreamSlot.Store(118)
	bs.lastEmittedBlockSlot = 110
	bs.nextSlotToSend = 111
	for slot := uint64(111); slot <= 118; slot++ {
		parentSlot := slot - 1
		if slot == 111 {
			parentSlot = 110
		}
		bs.lightbringerBuffer[slot] = &b.Block{Slot: slot, FromLightbringer: true, SourceParentSlot: parentSlot}
		bs.lightbringerBufferOrder = append(bs.lightbringerBufferOrder, slot)
	}

	bs.maybePrepareLightbringerHandoff()

	if got := bs.lightbringerHandoffSlot.Load(); got != 0 {
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
	bs.lightbringerConnected.Store(true)
	bs.lastExecutedSlot.Store(102)
	bs.confirmedTip.Store(117)
	bs.lightbringerLastStreamSlot.Store(118)
	bs.lastEmittedBlockSlot = 110
	bs.nextSlotToSend = 111
	for slot := uint64(111); slot <= 118; slot++ {
		parentSlot := slot - 1
		if slot == 111 {
			parentSlot = 110
		}
		bs.lightbringerBuffer[slot] = &b.Block{Slot: slot, FromLightbringer: true, SourceParentSlot: parentSlot}
		bs.lightbringerBufferOrder = append(bs.lightbringerBufferOrder, slot)
	}

	bs.maybePrepareLightbringerHandoff()

	if got := bs.lightbringerHandoffSlot.Load(); got != 111 {
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
	bs.lightbringerConnected.Store(true)
	bs.lastExecutedSlot.Store(100)
	bs.confirmedTip.Store(164)
	bs.lightbringerLastStreamSlot.Store(164)
	bs.nextSlotToSend = 110

	if !bs.shouldDecodeLightbringerSlot(120) {
		t.Fatalf("expected Lightbringer slot within catchup staging window to be decoded")
	}
	if bs.shouldDecodeLightbringerSlot(109) {
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
	bs.lightbringerConnected.Store(true)
	bs.lastExecutedSlot.Store(100)
	bs.confirmedTip.Store(165)
	bs.lightbringerLastStreamSlot.Store(165)
	bs.nextSlotToSend = 101

	if bs.shouldDecodeLightbringerSlot(120) {
		t.Fatalf("expected Lightbringer staging to wait until replay is inside the catchup staging window")
	}
}

func TestUpdateModeDefersCatchupWhileConsensusManagedLightbringerIsLive(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceLightbringer,
		LightbringerEndpoint:         "127.0.0.1:50051",
		StartSlot:                    100,
		EndSlot:                      300,
		ConsensusManagedLightbringer: true,
	})

	bs.lightbringerStarted.Store(true)
	bs.isNearTip.Store(true)
	bs.lightbringerActive.Store(true)
	bs.lightbringerConnected.Store(true)
	bs.lightbringerHandoffSlot.Store(101)
	bs.lastExecutedSlot.Store(100)
	bs.confirmedTip.Store(165)
	bs.lightbringerLastStreamSlot.Store(164)
	bs.lightbringerLastRecvUnix.Store(time.Now().Unix())
	bs.lastProgress.Store(time.Now().Unix())
	bs.nextSlotToSend = 101

	bs.updateMode()

	if !bs.isNearTip.Load() {
		t.Fatalf("expected near-tip mode to remain active while Lightbringer observations are fresh")
	}
	if !bs.lightbringerActive.Load() {
		t.Fatalf("expected Lightbringer to stay active during consensus buffering")
	}
	if bs.lightbringerNeedRPCResume.Load() {
		t.Fatalf("expected RPC resume flag to stay clear while deferring catchup")
	}
}

func TestUpdateModeFallsBackWhenConsensusManagedLightbringerReplayGapExceedsGrace(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceLightbringer,
		LightbringerEndpoint:         "127.0.0.1:50051",
		StartSlot:                    100,
		EndSlot:                      300,
		ConsensusManagedLightbringer: true,
	})

	bs.lightbringerStarted.Store(true)
	bs.isNearTip.Store(true)
	bs.lightbringerActive.Store(true)
	bs.lightbringerConnected.Store(true)
	bs.lightbringerHandoffSlot.Store(101)
	bs.lastExecutedSlot.Store(100)
	bs.confirmedTip.Store(229)
	bs.lightbringerLastStreamSlot.Store(229)
	bs.lightbringerLastRecvUnix.Store(time.Now().Unix())
	bs.lastProgress.Store(time.Now().Unix())
	bs.nextSlotToSend = 150

	bs.updateMode()

	if bs.isNearTip.Load() {
		t.Fatalf("expected near-tip mode to fall back once replay gap exceeds consensus buffering grace")
	}
	if bs.lightbringerActive.Load() {
		t.Fatalf("expected Lightbringer to be marked inactive after fallback")
	}
	if !bs.lightbringerNeedRPCResume.Load() {
		t.Fatalf("expected RPC resume flag to be raised after fallback")
	}
	if got := bs.nextSlotToSend; got != 101 {
		t.Fatalf("expected consensus-managed fallback to rewind emission frontier to replay next slot 101, got %d", got)
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
	disconnected := &b.Block{Slot: 151, FromLightbringer: true, SourceParentSlot: 149}
	connected := &b.Block{Slot: 151, FromLightbringer: true, SourceParentSlot: 150}

	bs.reorderMu.Lock()
	preferIncoming := bs.shouldPreferIncomingLightbringerBlockLocked(disconnected, connected)
	bs.reorderMu.Unlock()

	if !preferIncoming {
		t.Fatalf("expected same-slot Lightbringer block that matches the anchor to replace the disconnected buffered fork")
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
	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLightbringer: true, SourceParentSlot: 149}

	bs.reorderMu.Lock()
	waitingSlot, observedParent, expectedParent, mismatch := bs.waitingLightbringerParentMismatchLocked()
	bs.reorderMu.Unlock()

	if !mismatch {
		t.Fatalf("expected disconnected waiting Lightbringer block to be recognized as a parent mismatch")
	}
	if waitingSlot != 151 || observedParent != 149 || expectedParent != 150 {
		t.Fatalf("expected mismatch details slot=151 observed_parent=149 expected_parent=150, got slot=%d observed=%d expected=%d",
			waitingSlot, observedParent, expectedParent)
	}
}

func TestWaitingLightbringerParentMismatchLockedDefersWhenConsensusManaged(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceLightbringer,
		LightbringerEndpoint:         "127.0.0.1:50051",
		StartSlot:                    100,
		EndSlot:                      200,
		ConsensusManagedLightbringer: true,
	})

	bs.lightbringerActive.Store(true)
	bs.lastEmittedBlockSlot = 150
	bs.nextSlotToSend = 151
	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLightbringer: true, SourceParentSlot: 149}

	bs.reorderMu.Lock()
	waitingSlot, observedParent, expectedParent, mismatch := bs.waitingLightbringerParentMismatchLocked()
	bs.reorderMu.Unlock()

	if mismatch || waitingSlot != 0 || observedParent != 0 || expectedParent != 0 {
		t.Fatalf("expected consensus-managed Lightbringer to defer parent mismatch handling, got mismatch=%v slot=%d observed=%d expected=%d",
			mismatch, waitingSlot, observedParent, expectedParent)
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
	bs.lightbringerHandoffSlot.Store(151)
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
	bs.lightbringerActive.Store(true)
	bs.lightbringerHandoffSlot.Store(151)

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
	bs.lightbringerActive.Store(true)
	bs.lightbringerHandoffSlot.Store(151)

	done := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(done)
	}()

	bs.resultQueue <- fetchResult{
		slot:  152,
		block: &b.Block{Slot: 152, FromLightbringer: true, SourceParentSlot: 151},
	}
	close(bs.resultQueue)
	<-done

	skip := bs.NextBlock()
	if skip == nil || !skip.IsSkipped || skip.Slot != 151 {
		t.Fatalf("expected certified skip for slot 151, got %+v", skip)
	}
	if !skip.FromLightbringer {
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
	bs.reorderBuffer[152] = &b.Block{Slot: 152, FromLightbringer: true, SourceParentSlot: 151}
	bs.reorderBuffer[154] = &b.Block{Slot: 154, FromLightbringer: false}
	bs.reorderBuffer[155] = &b.Block{Slot: 155, FromLightbringer: true, SourceParentSlot: 150}
	bs.reorderBuffer[156] = &b.Block{Slot: 156, FromLightbringer: true, SourceParentSlot: 155}

	firstBufferedSlot, firstBufferedParentSlot, bufferedCount, firstConnectedSlot, firstConnectedParentSlot, foundConnected := bs.inspectLaterLightbringerBlocksLocked(151)
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

	bs.lightbringerActive.Store(true)
	bs.lightbringerHandoffSlot.Store(120)

	bs.handleDetectedLightbringerGap(125, 126, 125, 4)

	if got := bs.lightbringerForceRPCUntil.Load(); got != 0 {
		t.Fatalf("expected active Lightbringer gap to avoid forcing RPC, got %d", got)
	}
	if got := bs.lightbringerCooldownUntil.Load(); got != 0 {
		t.Fatalf("expected active Lightbringer gap to avoid setting cooldown, got %d", got)
	}
	if got := bs.lightbringerHandoffSlot.Load(); got != 120 {
		t.Fatalf("expected active Lightbringer gap to preserve handoff slot, got %d", got)
	}
	if !bs.lightbringerActive.Load() {
		t.Fatalf("expected Lightbringer to remain active")
	}
	if got := bs.lightbringerRepairSlot.Load(); got != 0 {
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

	bs.lightbringerConnected.Store(true)
	bs.lightbringerActive.Store(true)
	bs.isNearTip.Store(true)
	bs.nextSlotToSend = 120
	bs.lastEmittedBlockSlot = 119
	bs.lightbringerGapSinceUnix.Store(time.Now().Add(-(lightbringerDeepGapReconnect + time.Second)).UnixNano())

	reconnected := false
	bs.setLightbringerCancel(func() {
		reconnected = true
	})

	bs.repairLightbringerGap(120, 122, 121, reorderGapWarnThreshold)

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

	bs.lightbringerActive.Store(true)
	bs.nextSlotToSend = 120
	bs.reorderBuffer[121] = &b.Block{Slot: 121, FromLightbringer: true, SourceParentSlot: 120}
	bs.reorderBuffer[122] = &b.Block{Slot: 122, FromLightbringer: true, SourceParentSlot: 121}
	bs.lightbringerGapSlot.Store(120)
	bs.lightbringerGapSinceUnix.Store(time.Now().Add(-(lightbringerGapFallbackWait / 2)).UnixNano())

	waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, shouldFallback := bs.detectLightbringerGapLocked()
	if waitingSlot != 0 || firstBufferedSlot != 0 || firstBufferedParentSlot != 0 || bufferedCount != 0 || shouldFallback {
		t.Fatalf("expected Lightbringer gap detection to stay silent before fallback delay expires, got waiting=%d first=%d parent=%d buffered=%d fallback=%v",
			waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, shouldFallback)
	}
	if got := bs.lightbringerGapSlot.Load(); got != 120 {
		t.Fatalf("expected gap slot 120 to remain tracked, got %d", got)
	}
}

func TestDetectLightbringerGapDefersWhenConsensusManaged(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceLightbringer,
		LightbringerEndpoint:         "127.0.0.1:50051",
		StartSlot:                    100,
		EndSlot:                      200,
		ConsensusManagedLightbringer: true,
	})

	bs.lightbringerActive.Store(true)
	bs.nextSlotToSend = 120
	bs.reorderBuffer[121] = &b.Block{Slot: 121, FromLightbringer: true, SourceParentSlot: 120}
	bs.reorderBuffer[122] = &b.Block{Slot: 122, FromLightbringer: true, SourceParentSlot: 121}
	bs.lightbringerGapSlot.Store(120)
	bs.lightbringerGapSinceUnix.Store(time.Now().Add(-time.Minute).UnixNano())

	waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, shouldFallback := bs.detectLightbringerGapLocked()
	if waitingSlot != 0 || firstBufferedSlot != 0 || firstBufferedParentSlot != 0 || bufferedCount != 0 || shouldFallback {
		t.Fatalf("expected consensus-managed Lightbringer to defer gap fallback, got waiting=%d first=%d parent=%d buffered=%d fallback=%v",
			waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, shouldFallback)
	}
	if got := bs.lightbringerGapSlot.Load(); got != 0 {
		t.Fatalf("expected deferred gap tracking to clear the active gap watch, got slot %d", got)
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
	bs.lightbringerForceRPCUntil.Store(waitingSlot)
	bs.lightbringerCooldownUntil.Store(waitingSlot + lightbringerRecoverySlots)

	bs.SetLastExecutedSlot(waitingSlot)

	if got := bs.lightbringerForceRPCUntil.Load(); got != 0 {
		t.Fatalf("expected forced RPC boundary to clear at slot %d, got %d", waitingSlot, got)
	}
	if got := bs.lightbringerCooldownUntil.Load(); got != 0 {
		t.Fatalf("expected disabled recovery window to clear immediately at slot %d, got %d", waitingSlot, got)
	}
}

func TestSetLastExecutedSlotAdvancesDeferredLightbringerFrontier(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceLightbringer,
		LightbringerEndpoint:         "127.0.0.1:50051",
		StartSlot:                    100,
		EndSlot:                      200,
		ConsensusManagedLightbringer: true,
	})

	bs.nextSlotToSend = 151
	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLightbringer: true}
	bs.reorderBuffer[152] = &b.Block{Slot: 152, FromLightbringer: true}
	bs.reorderBuffer[154] = &b.Block{Slot: 154, FromLightbringer: true}
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
		SourceType:                   BlockSourceLightbringer,
		LightbringerEndpoint:         "127.0.0.1:50051",
		StartSlot:                    100,
		EndSlot:                      200,
		ConsensusManagedLightbringer: true,
	})

	bs.lightbringerActive.Store(true)
	bs.lastExecutedSlot.Store(120)
	bs.nextSlotToSend = 150
	bs.reorderBuffer[121] = &b.Block{Slot: 121, FromLightbringer: true}
	bs.reorderBuffer[149] = &b.Block{Slot: 149, FromLightbringer: true}
	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLightbringer: false}
	bs.slotState[121] = slotDone
	bs.slotState[149] = slotDone
	bs.slotState[151] = slotInflight
	bs.retrySlots = []uint64{119, 121, 149, 151}

	bs.forceRPCForCatchup(64)

	if got := bs.nextSlotToSend; got != 121 {
		t.Fatalf("expected RPC catchup frontier to rewind to replay's next slot 121, got %d", got)
	}
	if bs.lightbringerActive.Load() {
		t.Fatalf("expected Lightbringer to be marked inactive")
	}
	if !bs.lightbringerNeedRPCResume.Load() {
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
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:8001",
		StartSlot:                    100,
		EndSlot:                      200,
		ConsensusManagedLightbringer: true,
	})

	bs.lightbringerActive.Store(true)
	bs.lightbringerHandoffSlot.Store(121)
	bs.lastExecutedSlot.Store(120)
	bs.confirmedTip.Store(180)
	bs.nextSlotToSend = 150
	bs.reorderBuffer[121] = &b.Block{Slot: 121, FromLightbringer: true}
	bs.reorderBuffer[149] = &b.Block{Slot: 149, FromLightbringer: true}
	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLightbringer: false}
	bs.slotState[121] = slotDone
	bs.slotState[149] = slotDone
	bs.slotState[151] = slotInflight
	bs.retrySlots = []uint64{119, 121, 149, 151}

	bs.ForceRPCFallback("consensus_depth_exceeded")

	if got := bs.nextSlotToSend; got != 121 {
		t.Fatalf("expected RPC fallback frontier to rewind to replay's next slot 121, got %d", got)
	}
	if bs.lightbringerActive.Load() {
		t.Fatalf("expected turbine to be marked inactive")
	}
	if got := bs.lightbringerHandoffSlot.Load(); got != 0 {
		t.Fatalf("expected turbine handoff to be cleared, got %d", got)
	}
	if !bs.lightbringerNeedRPCResume.Load() {
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
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:8001",
		StartSlot:                    100,
		EndSlot:                      200,
		ConsensusManagedLightbringer: true,
	})

	bs.lightbringerActive.Store(true)
	bs.lightbringerHandoffSlot.Store(121)
	bs.lastExecutedSlot.Store(120)
	bs.confirmedTip.Store(180)
	bs.nextSlotToSend = 150
	bs.reorderBuffer[121] = &b.Block{Slot: 121, FromLightbringer: true}
	bs.reorderBuffer[149] = &b.Block{Slot: 149, FromLightbringer: true}
	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLightbringer: false}
	bs.slotState[121] = slotDone
	bs.slotState[149] = slotDone
	bs.slotState[151] = slotInflight
	bs.retrySlots = []uint64{119, 121, 149, 151}

	bs.forceRPCForCatchup(64)

	if got := bs.nextSlotToSend; got != 121 {
		t.Fatalf("expected active turbine fallback to rewind to replay's next slot 121, got %d", got)
	}
	if bs.lightbringerActive.Load() {
		t.Fatalf("expected turbine to be marked inactive")
	}
	if !bs.lightbringerNeedRPCResume.Load() {
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
		SourceType:                   BlockSourceLightbringer,
		LightbringerEndpoint:         "127.0.0.1:50051",
		StartSlot:                    100,
		EndSlot:                      200,
		ConsensusManagedLightbringer: true,
	})

	bs.lightbringerHandoffSlot.Store(121)
	bs.lastExecutedSlot.Store(120)
	bs.nextSlotToSend = 150
	bs.reorderBuffer[150] = &b.Block{Slot: 150, FromLightbringer: true}
	bs.reorderBuffer[151] = &b.Block{Slot: 151, FromLightbringer: false}
	bs.slotState[150] = slotDone
	bs.slotState[151] = slotInflight

	bs.forceRPCForCatchup(64)

	if got := bs.nextSlotToSend; got != 150 {
		t.Fatalf("expected pending handoff fallback to keep emitted RPC frontier 150, got %d", got)
	}
	if got := bs.lightbringerHandoffSlot.Load(); got != 0 {
		t.Fatalf("expected pending handoff to be cleared, got %d", got)
	}
	if !bs.lightbringerNeedRPCResume.Load() {
		t.Fatalf("expected scheduler to resume RPC from the current emission frontier")
	}
	if _, exists := bs.reorderBuffer[150]; exists {
		t.Fatalf("expected pending Lightbringer slot 150 to be dropped")
	}
	if _, exists := bs.reorderBuffer[151]; !exists {
		t.Fatalf("expected buffered RPC slot 151 to remain")
	}
}

func TestEmitOrderedBlocksDirectlyStreamsConsensusManagedLightbringerObservations(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceLightbringer,
		LightbringerEndpoint:         "127.0.0.1:50051",
		StartSlot:                    100,
		EndSlot:                      200,
		ConsensusManagedLightbringer: true,
	})

	bs.isNearTip.Store(true)
	bs.lightbringerActive.Store(true)
	bs.nextSlotToSend = 101

	done := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(done)
	}()

	bs.resultQueue <- fetchResult{
		slot:  105,
		block: &b.Block{Slot: 105, FromLightbringer: true, SourceParentSlot: 104},
	}
	close(bs.resultQueue)

	blk := bs.NextBlock()
	<-done

	if blk == nil || blk.Slot != 105 || !blk.FromLightbringer {
		t.Fatalf("expected direct Lightbringer observation for slot 105, got %+v", blk)
	}
	if _, exists := bs.reorderBuffer[105]; exists {
		t.Fatalf("expected direct observation to bypass the reorder buffer")
	}
	if got := bs.nextSlotToSend; got != 101 {
		t.Fatalf("expected direct observation to leave nextSlotToSend at 101 until replay resolves it, got %d", got)
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
	bs.lightbringerActive.Store(true)
	bs.lightbringerHandoffSlot.Store(100)
	staleGeneration := bs.lightbringerResultGeneration.Load()
	bs.invalidateLightbringerResults()

	done := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(done)
	}()

	bs.resultQueue <- fetchResult{
		slot:                 100,
		block:                &b.Block{Slot: 100, FromLightbringer: true, SourceParentSlot: 99},
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
	if !isLightbringerReconnectCancel(err) {
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

	bs.lightbringerActive.Store(true)
	bs.reorderBuffer[121] = &b.Block{Slot: 121, FromLightbringer: true, SourceParentSlot: 120}
	bs.reorderBuffer[122] = &b.Block{Slot: 122, FromLightbringer: true, SourceParentSlot: 121}
	bs.nextSlotToSend = 120
	bs.lightbringerGapSlot.Store(120)
	bs.lightbringerGapSinceUnix.Store(time.Now().Add(-6 * time.Second).UnixNano())
	bs.lightbringerGapLastLogUnix.Store(time.Now().Add(-3 * time.Second).Unix())
	bs.lightbringerGapReconnectSlot.Store(120)

	delete(bs.reorderBuffer, 121)
	delete(bs.reorderBuffer, 122)
	bs.reorderBuffer[126] = &b.Block{Slot: 126, FromLightbringer: true, SourceParentSlot: 125}
	bs.reorderBuffer[127] = &b.Block{Slot: 127, FromLightbringer: true, SourceParentSlot: 126}
	bs.nextSlotToSend = 125

	waitingSlot, _, _, _, shouldFallback := bs.detectLightbringerGapLocked()
	if waitingSlot != 0 || shouldFallback {
		t.Fatalf("expected first observation of a new gap to arm tracking without triggering fallback yet, got waitingSlot=%d shouldFallback=%v", waitingSlot, shouldFallback)
	}
	if got := bs.lightbringerGapSlot.Load(); got != 125 {
		t.Fatalf("expected new gap slot 125 to be tracked, got %d", got)
	}
	if got := bs.lightbringerGapReconnectSlot.Load(); got != 0 {
		t.Fatalf("expected reconnect latch to reset for new gap, got %d", got)
	}
	if got := bs.lightbringerGapLastLogUnix.Load(); got != 0 {
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

	bs.lightbringerHandoffSlot.Store(120)

	bs.handleDetectedLightbringerGap(125, 126, 125, 4)

	if got := bs.lightbringerForceRPCUntil.Load(); got != 125 {
		t.Fatalf("expected pending Lightbringer gap to force RPC until slot 125, got %d", got)
	}
	if got := bs.lightbringerCooldownUntil.Load(); got != 125+lightbringerRecoverySlots {
		t.Fatalf("expected pending Lightbringer gap to set cooldown boundary from the configured recovery window, got %d", got)
	}
	if got := bs.lightbringerHandoffSlot.Load(); got != 0 {
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

func TestTurbineRepairOnlyDisablesRPCBlockFetch(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:        BlockSourceTurbine,
		TurbineBindAddr:   "127.0.0.1:8001",
		TurbineRepairOnly: true,
		StartSlot:         100,
		EndSlot:           200,
	})
	bs.nextSlotToSend = 120
	bs.isNearTip.Store(false)

	bs.forceRPCForCatchup(128)
	if bs.lightbringerNeedRPCResume.Load() {
		t.Fatalf("expected repair-only mode to ignore RPC catchup")
	}
	if bs.shouldUseRPCForSlot(120) {
		t.Fatalf("expected repair-only mode to disable RPC for waiting slot")
	}

	bs.lightbringerForceRPCUntil.Store(120)
	if bs.shouldUseRPCForSlot(120) {
		t.Fatalf("expected repair-only mode to ignore forced RPC state")
	}

	bs.isNearTip.Store(true)
	source, status, _ := bs.currentSourceSnapshot()
	if source != "turbine" || !strings.Contains(status, "repair-only") {
		t.Fatalf("expected repair-only source status, got source=%q status=%q", source, status)
	}
}

func TestFrontierRepairEndSlot(t *testing.T) {
	if end := frontierRepairEndSlot(100, 0); end != 164 {
		t.Fatalf("default end = %d, want 164", end)
	}
	if end := frontierRepairEndSlot(100, 103); end != 103 {
		t.Fatalf("gap parent end = %d, want 103", end)
	}
	if end := frontierRepairEndSlot(100, 200); end != 164 {
		t.Fatalf("distant gap parent end = %d, want capped 164", end)
	}
}
