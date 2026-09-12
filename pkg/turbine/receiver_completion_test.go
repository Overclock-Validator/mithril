package turbine

import (
	"context"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/block"
)

func TestReceiverFinalShredReturnsBeforeCompletionWorkFinishes(t *testing.T) {
	packets := fixtures.DataShreds(t, "mainnet", 102815960)
	receiver := NewUDPReceiver("127.0.0.1:0")
	started := make(chan struct{})
	release := make(chan struct{})
	receiver.assembler.verifyTransactions = func(context.Context, *block.Block) error {
		close(started)
		<-release
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := newSlotCompletionPool(receiver.assembler, &receiver.slotResetMu, receiver.startPendingBlock, 1, 2)
	receiver.completionPool = pool
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		receiver.consumeCompletionResults(ctx, pool.results)
	}()

	for idx, packet := range packets[:len(packets)-1] {
		if !receiver.processPacket(ctx, nil, packet, nil, false) {
			t.Fatalf("processPacket(%d) stopped", idx)
		}
	}
	finalReturned := make(chan bool, 1)
	go func() {
		finalReturned <- receiver.processPacket(ctx, nil, packets[len(packets)-1], nil, false)
	}()
	select {
	case keepRunning := <-finalReturned:
		if !keepRunning {
			t.Fatal("final packet unexpectedly stopped receiver")
		}
	case <-time.After(time.Second):
		t.Fatal("live packet reader waited for completed-block verification")
	}
	waitSignal(t, started, "receiver completion worker")
	if receiver.SlotCompleted(102815960) {
		t.Fatal("slot completed before signature verification joined")
	}
	select {
	case blk := <-receiver.Blocks():
		t.Fatalf("receiver emitted block before verifier release: %v", blk)
	default:
	}

	close(release)
	pool.closeAndWait()
	select {
	case <-consumerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("completion result consumer did not join")
	}
	receiver.completionPool = nil

	select {
	case blk := <-receiver.Blocks():
		if blk == nil || blk.Slot != 102815960 || !blk.TransactionSignaturesVerified() {
			t.Fatalf("emitted block = %+v", blk)
		}
		receiver.AcknowledgeBlockDelivery(blk.Slot)
	case <-time.After(3 * time.Second):
		t.Fatal("verified block was not emitted")
	}
}
