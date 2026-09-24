package blockstream

import (
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

func observeSkipRecoveryCertificate(t *testing.T, tracker *alpenglow.ChainTracker, cert alpenglow.Certificate) {
	t.Helper()
	cert.SignatureVerified = true
	if _, err := tracker.ObserveCertificate(cert); err != nil {
		t.Fatalf("observe %s certificate at slot %d: %v", cert.Type, cert.Slot, err)
	}
}

func finalizeSkipRecoveryChild(t *testing.T, tracker *alpenglow.ChainTracker, child, parent alpenglow.BlockID) {
	t.Helper()
	tracker.ObserveReplayBlock(alpenglow.ReplayBlockObservation{
		Block: child, ParentSlot: parent.Slot, ParentHash: parent.Hash,
	})
	observeSkipRecoveryCertificate(t, tracker, alpenglow.Certificate{
		Type: alpenglow.CertificateFinalizeFast, Slot: child.Slot, BlockHash: child.Hash,
	})
	if err := tracker.ObserveFinalized(child, alpenglow.CertificateFinalizeFast); err != nil {
		t.Fatalf("finalize child: %v", err)
	}
	if tracker.SkipCertifiedAt(parent.Slot) {
		t.Fatal("finalized child's exact parent must override the parent's skip certificate")
	}
}

// A skip certificate can legally coexist with a fallback parent that a later
// finalized child selects. A queued skip must be retracted even during repair
// catchup, where near-tip candidate steering is inactive.
func TestAlpenglowFinalizedParentClearsQueuedSkipDuringCatchup(t *testing.T) {
	tracker := alpenglow.NewChainTracker()
	parent := alpenglow.BlockID{Slot: 151, Hash: solana.Hash{1}}
	child := alpenglow.BlockID{Slot: 154, Hash: solana.Hash{4}}
	observeSkipRecoveryCertificate(t, tracker, alpenglow.Certificate{
		Type: alpenglow.CertificateNotarizeFallback, Slot: parent.Slot, BlockHash: parent.Hash,
	})
	observeSkipRecoveryCertificate(t, tracker, alpenglow.Certificate{
		Type: alpenglow.CertificateSkip, Slot: parent.Slot,
	})
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		DisableRPCBlockFetch:         true,
		StartSlot:                    parent.Slot,
		EndSlot:                      200,
		AlpenglowDecisionSource:      tracker.NextDecision,
	})
	bs.isNearTip.Store(false)
	bs.liveStreamActive.Store(true)
	bs.reorderMu.Lock()
	bs.alpenglowRewindWait.begin(parent.Slot, parent.Hash, time.Now())
	bs.applyAlpenglowDecisionLocked()
	marked := bs.skippedSlots[parent.Slot] && bs.alpenglowCertifiedSkips[parent.Slot]
	waitingAfterSkip := bs.alpenglowRewindWait.slot
	bs.reorderMu.Unlock()
	if !marked {
		t.Fatal("initial skip certificate did not queue a skip")
	}
	if waitingAfterSkip != 0 {
		t.Fatal("superseding skip decision left the old block delivery diagnostic active")
	}

	finalizeSkipRecoveryChild(t, tracker, child, parent)
	bs.reorderMu.Lock()
	bs.alpenglowRewindWait.begin(parent.Slot, solana.Hash{99}, time.Now())
	bs.applyAlpenglowDecisionLocked()
	stillSkipped := bs.skippedSlots[parent.Slot] || bs.alpenglowCertifiedSkips[parent.Slot] || bs.liveSynthesizedSkips[parent.Slot]
	waitingAfterBlock := bs.alpenglowRewindWait.slot
	bs.reorderMu.Unlock()
	if stillSkipped {
		t.Fatal("decisive parent block left a queued skip outside near-tip mode")
	}
	if waitingAfterBlock != 0 {
		t.Fatal("superseding block decision left the old identity delivery diagnostic active")
	}
	bs.slotStateMu.Lock()
	_, stillDone := bs.slotState[parent.Slot]
	bs.slotStateMu.Unlock()
	if stillDone {
		t.Fatal("retracted skip left slot done, preventing parent repair")
	}
}

// Mirrors the live stall at 2795460 -> 2795461 -> 2795464: the source already
// emitted three skips when the finalized child reveals that the first skipped
// slot is its real parent. Rewinding must admit repaired blocks below both the
// handoff boundary and the repair catchup boundary, then resume ordered replay.
func TestAlpenglowSkippedParentRewindResumesRepairCatchup(t *testing.T) {
	tracker := alpenglow.NewChainTracker()
	anchor := alpenglow.BlockID{Slot: 150, Hash: solana.Hash{10}}
	parent := alpenglow.BlockID{Slot: 151, Hash: solana.Hash{11}}
	child := alpenglow.BlockID{Slot: 154, Hash: solana.Hash{14}}
	observeSkipRecoveryCertificate(t, tracker, alpenglow.Certificate{
		Type: alpenglow.CertificateNotarizeFallback, Slot: parent.Slot, BlockHash: parent.Hash,
	})
	for slot := parent.Slot; slot < child.Slot; slot++ {
		observeSkipRecoveryCertificate(t, tracker, alpenglow.Certificate{Type: alpenglow.CertificateSkip, Slot: slot})
	}
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		DisableRPCBlockFetch:         true,
		StartSlot:                    parent.Slot,
		EndSlot:                      200,
		AlpenglowDecisionSource:      tracker.NextDecision,
		AlpenglowSkipCertified:       tracker.SkipCertifiedAt,
		AlpenglowWantedBlocks:        tracker.WantedBlocks,
		AlpenglowCandidateBlockSink:  func(obs alpenglow.ReplayBlockObservation) { tracker.ObserveReplayBlock(obs) },
	})
	bs.lastEmittedBlockSlot = anchor.Slot
	bs.lastEmittedAlpenglowBlockID = anchor.Hash
	bs.hasLastEmittedAlpenglowBlockID = true
	bs.emittedAlpenglowBlockIDs[anchor.Slot] = anchor.Hash
	bs.emittedAlpenglowBlockIDOrder = []uint64{anchor.Slot}
	bs.isNearTip.Store(false)
	bs.liveStreamActive.Store(true)
	bs.liveHandoffSlot.Store(parent.Slot)
	bs.repairCatchupFrom.Store(parent.Slot)
	bs.repairCatchupUntil.Store(190)
	bs.lastExecutedSlot.Store(anchor.Slot)

	done := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(done)
	}()
	t.Cleanup(func() {
		close(bs.resultQueue)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("ordered emitter did not stop")
		}
	})
	next := func(slot uint64, skipped bool) *b.Block {
		t.Helper()
		select {
		case blk := <-bs.streamChan:
			if blk == nil || blk.Slot != slot || blk.IsSkipped != skipped {
				t.Fatalf("emitted block = %+v, want slot %d skipped=%v", blk, slot, skipped)
			}
			return blk
		case <-time.After(time.Second):
			t.Fatalf("repair catchup did not emit slot %d skipped=%v", slot, skipped)
			return nil
		}
	}
	bs.resultQueue <- fetchResult{wakeEmitter: true}
	for slot := parent.Slot; slot < child.Slot; slot++ {
		next(slot, true)
	}
	waitForBlockSourceCondition(t, func() bool {
		bs.reorderMu.Lock()
		defer bs.reorderMu.Unlock()
		return bs.nextSlotToSend == child.Slot
	})
	// Replay has consumed the trailing skips, but that frontier does not
	// establish delivery of a subsequently selected parent after rewind.
	bs.lastExecutedSlot.Store(child.Slot - 1)

	childBlock := &b.Block{
		Slot: child.Slot, ParentSlot: parent.Slot, SourceParentSlot: parent.Slot,
		FromLiveStream: true, HasAlpenglowBlockID: true, AlpenglowBlockID: [32]byte(child.Hash),
		HasAlpenglowParentBlockID: true, AlpenglowParentBlockID: [32]byte(parent.Hash),
	}
	if !bs.ingestLiveShredBlock(childBlock) {
		t.Fatal("child ingestion unexpectedly stopped")
	}
	waitForBlockSourceCondition(t, func() bool {
		bs.reorderMu.Lock()
		defer bs.reorderMu.Unlock()
		return bs.reorderBuffer[child.Slot] == childBlock
	})
	finalizeSkipRecoveryChild(t, tracker, child, parent)

	// A catchup activation while replay is stalled begins at the held child.
	bs.liveHandoffSlot.Store(child.Slot)
	bs.repairCatchupFrom.Store(child.Slot)
	bs.RewindForAlpenglowSwitch(parent.Slot, parent.Hash)
	if got := bs.liveHandoffSlot.Load(); got != parent.Slot {
		t.Fatalf("rewound handoff boundary = %d, want repaired parent slot %d", got, parent.Slot)
	}
	if got := bs.repairCatchupFrom.Load(); got != parent.Slot {
		t.Fatalf("rewound repair catchup boundary = %d, want parent slot %d", got, parent.Slot)
	}
	bs.reorderMu.Lock()
	notice, due := bs.alpenglowRewindWait.takeNotice(bs.alpenglowRewindWait.started.Add(alpenglowRewindWaitWarnAfter))
	bs.reorderMu.Unlock()
	if !due || notice.slot != parent.Slot || notice.certified != parent.Hash {
		t.Fatalf("rewind lost the delivery wait behind consumed skips: notice=%+v due=%v", notice, due)
	}
	parentBlock := &b.Block{
		Slot: parent.Slot, ParentSlot: anchor.Slot, SourceParentSlot: anchor.Slot,
		FromLiveStream: true, HasAlpenglowBlockID: true, AlpenglowBlockID: [32]byte(parent.Hash),
		HasAlpenglowParentBlockID: true, AlpenglowParentBlockID: [32]byte(anchor.Hash),
	}
	if !bs.ingestLiveShredBlock(parentBlock) || !bs.ingestLiveShredBlock(childBlock) {
		t.Fatal("repaired parent/child ingestion unexpectedly stopped")
	}
	if got := next(parent.Slot, false); solana.Hash(got.AlpenglowBlockID) != parent.Hash {
		t.Fatal("repaired parent identity was not preserved")
	}
	waitForBlockSourceCondition(t, func() bool {
		bs.reorderMu.Lock()
		defer bs.reorderMu.Unlock()
		return bs.alpenglowRewindWait.slot == 0
	})
	for slot := parent.Slot + 1; slot < child.Slot; slot++ {
		next(slot, true)
	}
	if got := next(child.Slot, false); solana.Hash(got.AlpenglowBlockID) != child.Hash {
		t.Fatal("finalized child identity was not preserved")
	}
}

func TestAlpenglowCertificateRewindWaitsForPausedSendAndDrainsSuffix(t *testing.T) {
	anchorID := solana.Hash{20}
	discardedID := solana.Hash{23}
	selectedID := solana.Hash{21}
	anchor := invalidTestLiveBlock(150, 149, anchorID, solana.Hash{19})
	discarded := invalidTestLiveBlock(153, anchor.Slot, discardedID, anchorID)
	paused := invalidTestLiveBlock(154, discarded.Slot, solana.Hash{24}, discardedID)
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		DisableRPCBlockFetch:         true,
		StartSlot:                    paused.Slot,
		EndSlot:                      200,
	})
	bs.lastEmittedBlockSlot = discarded.Slot
	bs.lastEmittedAlpenglowBlockID = discardedID
	bs.hasLastEmittedAlpenglowBlockID = true
	bs.emittedAlpenglowBlockIDs[anchor.Slot] = anchorID
	bs.emittedAlpenglowBlockIDs[discarded.Slot] = discardedID
	bs.emittedAlpenglowBlockIDOrder = []uint64{anchor.Slot, discarded.Slot}
	bs.isNearTip.Store(true)
	bs.liveHandoffSlot.Store(151)
	bs.streamChan <- anchor
	bs.streamChan <- &b.Block{Slot: 151, IsSkipped: true, FromLiveStream: true}
	bs.streamChan <- &b.Block{Slot: 152, IsSkipped: true, FromLiveStream: true}
	bs.streamChan <- discarded

	sendPaused := make(chan struct{})
	releaseSend := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSend) }) }
	bs.beforeReplayBlockSend = func(got *b.Block) {
		if got == paused {
			close(sendPaused)
			<-releaseSend
		}
	}
	emitterDone := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(emitterDone)
	}()
	var rewindDone chan struct{}
	t.Cleanup(func() {
		release()
		if rewindDone != nil {
			waitInvalidTestDone(t, rewindDone, "certificate rewind")
		}
		close(bs.resultQueue)
		waitInvalidTestDone(t, emitterDone, "certificate rewind emitter")
	})
	bs.resultQueue <- fetchResult{slot: paused.Slot, block: paused}
	waitInvalidTestDone(t, sendPaused, "paused certificate rewind send")

	rewindDone = make(chan struct{})
	go func() {
		bs.RewindForAlpenglowSwitch(151, selectedID)
		close(rewindDone)
	}()
	waitForBlockSourceCondition(t, func() bool { return bs.alpenglowQuarantineFrom.Load() == 151 })
	select {
	case <-rewindDone:
		t.Fatal("certificate rewind completed while the old replay send was paused")
	default:
	}
	release()
	waitInvalidTestDone(t, rewindDone, "certificate rewind with in-flight send")

	bs.reorderMu.Lock()
	frontier, restoredAnchor := bs.nextSlotToSend, bs.lastEmittedBlockSlot
	_, retainedDiscarded := bs.emittedAlpenglowBlockIDs[discarded.Slot]
	_, retainedPaused := bs.emittedAlpenglowBlockIDs[paused.Slot]
	notice, due := bs.alpenglowRewindWait.takeNotice(bs.alpenglowRewindWait.started.Add(alpenglowRewindWaitWarnAfter))
	bs.reorderMu.Unlock()
	if frontier != 151 || restoredAnchor != anchor.Slot || retainedDiscarded || retainedPaused {
		t.Fatalf("old send corrupted restored frontier: next=%d anchor=%d old_block=%v paused_block=%v",
			frontier, restoredAnchor, retainedDiscarded, retainedPaused)
	}
	if !due || notice.slot != 151 || notice.certified != selectedID {
		t.Fatalf("pre-rewind send satisfied the post-rewind delivery wait: notice=%+v due=%v", notice, due)
	}
	select {
	case got := <-bs.streamChan:
		if got != anchor {
			t.Fatalf("rewind retained wrong queued block: %+v", got)
		}
	default:
		t.Fatal("rewind discarded the queued block below its boundary")
	}
	select {
	case stale := <-bs.streamChan:
		t.Fatalf("rewind left a stale skip or block queued: %+v", stale)
	default:
	}

	selected := invalidTestLiveBlock(151, anchor.Slot, selectedID, anchorID)
	if !bs.ingestLiveShredBlock(selected) {
		t.Fatal("selected replacement ingestion stopped")
	}
	select {
	case got := <-bs.streamChan:
		if got != selected {
			t.Fatalf("emitted %+v after rewind, want selected replacement", got)
		}
	case <-time.After(time.Second):
		t.Fatal("selected replacement could not emit after the paused-send rewind")
	}
	waitForBlockSourceCondition(t, func() bool {
		bs.reorderMu.Lock()
		defer bs.reorderMu.Unlock()
		return bs.alpenglowRewindWait.slot == 0
	})
}
