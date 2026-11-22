package turbine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

type addPacketResult struct {
	block *block.Block
	err   error
}

func feedFixturePrefix(t *testing.T, assembler *SlotAssembler, packets [][]byte) {
	t.Helper()
	for idx, packet := range packets {
		blk, err := assembler.AddPacket(packet)
		if err != nil {
			t.Fatalf("AddPacket(%d): %v", idx, err)
		}
		if blk != nil {
			t.Fatalf("AddPacket(%d) completed before final packet", idx)
		}
	}
}

func waitSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func TestSlotAssemblerCompletionRunsOutsideMutexAndClaimsOnce(t *testing.T) {
	packets := fixtures.DataShreds(t, "mainnet", 102815960)
	assembler := NewSlotAssembler()
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var verifyCalls atomic.Int32
	assembler.verifyTransactions = func(context.Context, *block.Block) error {
		verifyCalls.Add(1)
		startedOnce.Do(func() { close(started) })
		<-release
		return nil
	}
	callbackObserved := make(chan bool, 1)
	assembler.SetOnComplete(func(slot uint64, _, _ uint32) {
		callbackObserved <- assembler.SlotCompleted(slot)
	})

	feedFixturePrefix(t, assembler, packets[:len(packets)-1])
	result := make(chan addPacketResult, 1)
	go func() {
		blk, err := assembler.AddPacket(packets[len(packets)-1])
		result <- addPacketResult{block: blk, err: err}
	}()
	waitSignal(t, started, "blocked transaction verifier")
	assembler.mu.Lock()
	firstAt, fullAt := assembler.slots[102815960].firstShredAt, assembler.slots[102815960].fullAt
	assembler.mu.Unlock()

	edges := make(chan [2]uint64, 1)
	go func() {
		latest, full := assembler.ShredEdges()
		edges <- [2]uint64{latest, full}
	}()
	select {
	case got := <-edges:
		if got != [2]uint64{102815960, 102815960} {
			t.Fatalf("shred edges while verifier blocked = %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("assembler mutex remained held during signature verification")
	}

	duplicate := make(chan addPacketResult, 1)
	go func() {
		blk, err := assembler.AddPacket(packets[len(packets)-1])
		duplicate <- addPacketResult{block: blk, err: err}
	}()
	select {
	case got := <-duplicate:
		if got.block != nil || got.err != nil {
			t.Fatalf("duplicate in-flight completion = block %v, err %v", got.block, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("duplicate shred blocked behind completed-slot processing")
	}

	releasedAt := time.Now()
	close(release)
	var got addPacketResult
	select {
	case got = <-result:
	case <-time.After(3 * time.Second):
		t.Fatal("completion did not finish after verifier release")
	}
	if got.err != nil || got.block == nil {
		t.Fatalf("completion = block %v, err %v", got.block, got.err)
	}
	if !got.block.TransactionSignaturesVerified() {
		t.Fatal("successful parallel verification did not set trust marker")
	}
	if timings, ok := got.block.TurbineIngressTimings(); !ok {
		t.Fatal("successful completion did not carry per-slot ingress timings")
	} else if timings.TransactionSigverify <= 0 || timings.BlockDecode <= 0 || timings.TransactionParse <= 0 {
		t.Fatalf("incomplete per-slot ingress timings: %+v", timings)
	} else if timings.AssemblyFirstAcceptedAt != firstAt || timings.AssemblyFullAt != fullAt ||
		timings.ReadyAt.Before(releasedAt) || timings.ReadyAt.IsZero() || timings.FullToReady != timings.ReadyAt.Sub(fullAt) {
		t.Fatalf("completion did not preserve owner timing values: %+v", timings)
	} else if start, ok := got.block.TurbineReplayAdmissionStart(); !ok || start != timings.ReadyAt {
		t.Fatal("ready and replay-admission boundary diverged")
	}

	if got.block.ShredFullNanos == 0 || got.block.ShredFullNanos > releasedAt.UnixNano() {
		t.Fatalf("full timestamp %d was not captured before verifier release %d", got.block.ShredFullNanos, releasedAt.UnixNano())
	}
	if verifyCalls.Load() != 1 {
		t.Fatalf("verifier calls = %d, want one completion claim", verifyCalls.Load())
	}
	select {
	case completed := <-callbackObserved:
		if !completed {
			t.Fatal("callback did not observe completed marker")
		}
	case <-time.After(time.Second):
		t.Fatal("completion callback deadlocked on assembler mutex")
	}
}

func TestSlotAssemblerResetSupersedesInFlightCompletion(t *testing.T) {
	packets := fixtures.DataShreds(t, "mainnet", 102815960)
	assembler := NewSlotAssembler()
	started := make(chan struct{})
	release := make(chan struct{})
	var candidate *block.Block
	assembler.verifyTransactions = func(_ context.Context, blk *block.Block) error {
		candidate = blk
		close(started)
		<-release
		return nil
	}
	var callbacks atomic.Int32
	assembler.SetOnComplete(func(uint64, uint32, uint32) { callbacks.Add(1) })

	feedFixturePrefix(t, assembler, packets[:len(packets)-1])
	result := make(chan addPacketResult, 1)
	go func() {
		blk, err := assembler.AddPacket(packets[len(packets)-1])
		result <- addPacketResult{block: blk, err: err}
	}()
	waitSignal(t, started, "in-flight completion")

	assembler.ResetSlot(102815960)
	if blk, err := assembler.AddPacket(packets[0]); err != nil || blk != nil {
		t.Fatalf("replacement first packet = block %v, err %v", blk, err)
	}
	close(release)
	select {
	case got := <-result:
		if got.block != nil || got.err != nil {
			t.Fatalf("stale completion = block %v, err %v", got.block, got.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stale completion did not drain")
	}
	if assembler.SlotCompleted(102815960) {
		t.Fatal("stale generation marked replacement slot completed")
	}
	if _, ok := assembler.HeadShredDetail(102815960); !ok {
		t.Fatal("stale completion deleted replacement slot state")
	}
	if callbacks.Load() != 0 {
		t.Fatalf("stale completion invoked callback %d times", callbacks.Load())
	}
	if candidate == nil {
		t.Fatal("reset test did not reach decoded candidate")
	}
	if _, ok := candidate.TurbineIngressTimings(); ok {
		t.Fatal("superseded completion retained trusted timing")
	}
}

func TestSlotAssemblerVerificationFailureRemainsFailClosed(t *testing.T) {
	packets := fixtures.DataShreds(t, "mainnet", 102815960)
	assembler := NewSlotAssembler()
	verifyErr := errors.New("test invalid transaction signature")
	var candidate *block.Block
	assembler.verifyTransactions = func(_ context.Context, blk *block.Block) error { candidate = blk; return verifyErr }
	var callbacks atomic.Int32
	assembler.SetOnComplete(func(uint64, uint32, uint32) { callbacks.Add(1) })

	feedFixturePrefix(t, assembler, packets[:len(packets)-1])
	blk, err := assembler.AddPacket(packets[len(packets)-1])
	if !errors.Is(err, verifyErr) || blk != nil {
		t.Fatalf("invalid block completion = block %v, err %v", blk, err)
	}
	if assembler.SlotCompleted(102815960) {
		t.Fatal("invalid block was marked completed")
	}
	count, latest := assembler.SlotAssemblyErrors(102815960)
	if count == 0 || !strings.Contains(latest, verifyErr.Error()) {
		t.Fatalf("verification failure was not retained: count=%d latest=%q", count, latest)
	}
	if callbacks.Load() != 0 {
		t.Fatal("invalid block invoked completion callback")
	}
	if candidate == nil {
		t.Fatal("failure test did not reach decoded candidate")
	}
	if _, ok := candidate.TurbineIngressTimings(); ok {
		t.Fatal("failed signature verification retained trusted timing")
	}
}

func generatedAlpenglowDataShreds(t *testing.T) []*Shred {
	t.Helper()
	leader := testShredLeader(t)
	gen := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7, ReferenceTick: 63}
	chainedRoot := solana.Hash{5}
	var nextData, nextCode uint32
	var dataShreds []*Shred
	for _, component := range buildAlpenglowSlot(t) {
		packets, root, newData, newCode, err := gen.MakeShredsFromData(
			leader, component.payload, component.isLastInSlot,
			chainedRoot, nextData, nextCode,
		)
		if err != nil {
			t.Fatalf("make component %s: %v", component.name, err)
		}
		chainedRoot, nextData, nextCode = root, newData, newCode
		for _, packet := range packets {
			shred, err := ParseShred(packet)
			if err != nil {
				t.Fatalf("parse component %s: %v", component.name, err)
			}
			if shred.Type == ShredTypeData {
				dataShreds = append(dataShreds, shred)
			}
		}
	}
	return dataShreds
}

func TestSlotAssemblerFinalizationHonorsHintLearnedDuringVerification(t *testing.T) {
	shreds := generatedAlpenglowDataShreds(t)
	assembler := NewSlotAssembler()
	started := make(chan struct{})
	release := make(chan struct{})
	var candidate *block.Block
	assembler.verifyTransactions = func(_ context.Context, blk *block.Block) error {
		candidate = blk
		close(started)
		<-release
		return nil
	}
	for idx, shred := range shreds[:len(shreds)-1] {
		if blk, err := assembler.AddShred(shred); err != nil || blk != nil {
			t.Fatalf("AddShred(%d) = block %v, err %v", idx, blk, err)
		}
	}
	result := make(chan addPacketResult, 1)
	go func() {
		blk, err := assembler.AddShred(shreds[len(shreds)-1])
		result <- addPacketResult{block: blk, err: err}
	}()
	waitSignal(t, started, "Alpenglow verification")

	assembler.SetKnownAlpenglowBlockID(100, solana.Hash{0xff})
	close(release)
	select {
	case got := <-result:
		if got.block != nil || got.err != nil {
			t.Fatalf("late contradictory hint completion = block %v, err %v", got.block, got.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Alpenglow completion did not drain")
	}
	if assembler.SlotCompleted(100) {
		t.Fatal("block contradicting a hint learned during verification completed")
	}
	if candidate == nil {
		t.Fatal("noncanonical test did not reach decoded candidate")
	}
	if _, ok := candidate.TurbineIngressTimings(); ok {
		t.Fatal("noncanonical completion retained trusted timing")
	}
	count, slot, _, want := assembler.NonCanonicalBlockIDStats()
	if count != 1 || slot != 100 || want != (solana.Hash{0xff}) {
		t.Fatalf("noncanonical stats = count %d slot %d want %s", count, slot, want)
	}
}

func TestCompletionCarriesOriginalAlpenglowTimingOnlyAfterAcceptance(t *testing.T) {
	assembler := NewSlotAssembler()
	var work *slotCompletionWork
	for _, shred := range generatedAlpenglowDataShreds(t) {
		next, err := assembler.addShredFrom(shred, false)
		if err != nil {
			t.Fatal(err)
		}
		if next != nil {
			if work != nil {
				t.Fatal("fixture claimed more than one completion")
			}
			work = next
		}
	}
	if work == nil {
		t.Fatal("fixture did not become reconstructable")
	}
	processed := assembler.processCompletion(context.Background(), work)
	if processed.err != nil || processed.block == nil || processed.completionReadyAt.IsZero() {
		t.Fatalf("process completion: block=%v err=%v ready=%v", processed.block != nil, processed.err, processed.completionReadyAt)
	}
	if _, ok := processed.block.TurbineIngressTimings(); ok {
		t.Fatal("unaccepted decoded candidate acquired trusted timings")
	}
	blk, err := assembler.finalizeCompletion(work, processed)
	if err != nil || blk == nil || !blk.HasAlpenglowBlockID {
		t.Fatalf("accepted completion: block=%v err=%v", blk != nil, err)
	}
	timings, ok := blk.TurbineIngressTimings()
	if !ok || timings.AssemblyFirstAcceptedAt != work.state.firstShredAt || timings.AssemblyFullAt != work.state.fullAt ||
		timings.ReadyAt != processed.completionReadyAt || timings.ReadyAt.IsZero() {
		t.Fatalf("accepted completion lost original times: %+v", timings)
	}
	if timings.AssemblyFirstAcceptedAt.UnixNano() != blk.ShredFirstNanos || timings.AssemblyFullAt.UnixNano() != blk.ShredFullNanos {
		t.Fatal("existing wall-clock fields changed meaning")
	}
}

func TestCompletionAdmissionFallbackDoesNotInventReadyTime(t *testing.T) {
	assembler := NewSlotAssembler()
	state := &slotState{slot: 1, completing: true}
	assembler.slots[state.slot] = state
	blk, err := assembler.finalizeCompletion(&slotCompletionWork{state: state}, processedSlotCompletion{block: &block.Block{Slot: 1}})
	if err != nil || blk == nil {
		t.Fatalf("completion: block=%v err=%v", blk != nil, err)
	}
	timings, ok := blk.TurbineIngressTimings()
	if !ok || !timings.ReadyAt.IsZero() || !timings.AssemblyFirstAcceptedAt.IsZero() || !timings.AssemblyFullAt.IsZero() {
		t.Fatalf("unknown source timing was synthesized: %+v", timings)
	}
	if _, ok := blk.TurbineReplayAdmissionStart(); !ok {
		t.Fatal("existing replay-admission fallback was removed")
	}
}
