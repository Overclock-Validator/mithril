package harmonic

import (
	"testing"
	"time"

	blockpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/block"
	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// wireTx is a distinct signed transfer for each seq.
func wireTx(seq uint64) []byte {
	return txfixture.MustSignedTransferWire(seq)
}

func blockMsg(slot uint64, txs ...*blockpb.Transaction) *blockpb.Block {
	return &blockpb.Block{Slot: slot, Transactions: txs}
}

func blockTx(data []byte, bundleID uint64) *blockpb.Transaction {
	return &blockpb.Transaction{Transaction: data, BundleId: bundleID}
}

// collect runs fn and returns every event it emitted.
func collect(t *testing.T, fn func(*BlockPipeline), txV1Enabled func() bool, current uint64) ([]BlockEvent, *BlockPipeline) {
	t.Helper()
	events := make(chan BlockEvent, 64)
	p := NewBlockPipeline(events, txV1Enabled)
	p.SetCurrentSlot(current)
	fn(p)
	close(events)
	var out []BlockEvent
	for e := range events {
		out = append(out, e)
	}
	return out, p
}

type bundleShape struct {
	seq             uint64
	revertProtected bool
	size            int
}

func shapes(t *testing.T, events []BlockEvent) []bundleShape {
	t.Helper()
	var out []bundleShape
	for _, e := range events {
		b, ok := e.(BundleEvent)
		require.Truef(t, ok, "unexpected event %#v", e)
		out = append(out, bundleShape{seq: b.Seq, revertProtected: b.RevertProtected, size: len(b.Transactions)})
	}
	return out
}

func TestBlockPipelineSegmentsPerMessage(t *testing.T) {
	arrival := time.Unix(1_800_000_000, 0)
	ids := []uint64{0, 5, 0, 0, 9, 9, 5}
	var txs []*blockpb.Transaction
	for i, id := range ids {
		txs = append(txs, blockTx(wireTx(uint64(i)), id))
	}
	events, p := collect(t, func(p *BlockPipeline) {
		p.HandleBlock(blockMsg(10, txs...), arrival)
		// A run that seems to continue into the next message is a new bundle.
		p.HandleBlock(blockMsg(10, blockTx(wireTx(20), 5), blockTx(wireTx(21), 5)), arrival)
	}, nil, 10)

	assert.Equal(t, []bundleShape{
		{1, false, 1}, {2, true, 1}, {3, false, 1}, {4, false, 1}, {5, true, 2}, {6, true, 1},
		{7, true, 2},
	}, shapes(t, events))
	first := events[0].(BundleEvent)
	assert.Equal(t, uint64(10), first.Slot)
	assert.Equal(t, arrival, first.Arrival)
	fifth := events[4].(BundleEvent)
	assert.Equal(t, solana.SignatureFromBytes(wireTx(4)[1:65]), fifth.Transactions[0].Signatures[0], "members keep stream order")
	assert.Equal(t, uint64(7), p.Stats().Bundles)
}

func TestBlockPipelineAdmitsOnlyTheCurrentSlot(t *testing.T) {
	events, p := collect(t, func(p *BlockPipeline) {
		p.HandleBlock(blockMsg(9, blockTx(wireTx(1), 0)), time.Now())
		p.HandleBlock(blockMsg(11, blockTx(wireTx(2), 0)), time.Now())
		p.HandleBlock(blockMsg(10, blockTx(wireTx(3), 0)), time.Now())
		p.SetCurrentSlot(0)
		p.HandleBlock(blockMsg(10, blockTx(wireTx(4), 0)), time.Now())
		p.SetCurrentSlot(11)
		p.HandleBlock(blockMsg(11, blockTx(wireTx(5), 0)), time.Now())
	}, nil, 10)

	require.Len(t, events, 2)
	b10, b11 := events[0].(BundleEvent), events[1].(BundleEvent)
	assert.Equal(t, uint64(10), b10.Slot)
	assert.Equal(t, uint64(1), b10.Seq, "messages for other slots consume no sequence numbers")
	assert.Equal(t, uint64(11), b11.Slot)
	assert.Equal(t, uint64(1), b11.Seq, "each slot's sequence starts at 1")
	assert.Equal(t, uint64(3), p.Stats().WrongSlot)
}

func TestBlockPipelineDiscardsALostSlotEvenAfterReconnect(t *testing.T) {
	events, p := collect(t, func(p *BlockPipeline) {
		p.HandleBlock(blockMsg(10, blockTx(wireTx(1), 0)), time.Now())
		p.StreamLost(10)
		p.HandleBlock(blockMsg(10, blockTx(wireTx(2), 0)), time.Now())
		p.SetCurrentSlot(10)
		p.HandleBlock(blockMsg(10, blockTx(wireTx(3), 0)), time.Now())
		p.SetCurrentSlot(11)
		p.HandleBlock(blockMsg(11, blockTx(wireTx(4), 0)), time.Now())
	}, nil, 10)

	require.Len(t, events, 3)
	assert.Equal(t, uint64(10), events[0].(BundleEvent).Slot)
	assert.Equal(t, StreamLostEvent{Slot: 10}, events[1])
	assert.Equal(t, uint64(11), events[2].(BundleEvent).Slot)
	assert.Equal(t, uint64(2), p.Stats().LostSlot)
}

func TestBlockPipelineDropsInvalidBundlesWhole(t *testing.T) {
	badSig := wireTx(3)
	badSig[10] ^= 0xff
	valid := func(seq uint64) *blockpb.Transaction { return blockTx(wireTx(seq), 7) }
	cases := map[string][]*blockpb.Transaction{
		"empty payload":       {valid(1), blockTx(nil, 7)},
		"oversized payload":   {valid(1), blockTx(make([]byte, packet.DataSize+1), 7)},
		"unparseable":         {valid(1), blockTx([]byte{1, 2, 3}, 7)},
		"bad signature":       {valid(1), blockTx(badSig, 7)},
		"repeated signature":  {valid(1), valid(2), valid(1)},
		"too many members":    {valid(1), valid(2), valid(3), valid(4), valid(5), valid(6)},
		"v1 while not active": {valid(1), blockTx(txfixture.MustSignedV1Wire(1, 0), 7)},
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			before := blockTx(wireTx(100), 0)
			after := blockTx(wireTx(101), 3)
			txs := append(append([]*blockpb.Transaction{before}, bad...), after)
			events, p := collect(t, func(p *BlockPipeline) { p.HandleBlock(blockMsg(10, txs...), time.Now()) }, nil, 10)

			require.Len(t, events, 3)
			assert.Equal(t, uint64(1), events[0].(BundleEvent).Seq)
			invalid, ok := events[1].(InvalidBundleEvent)
			require.Truef(t, ok, "got %#v", events[1])
			assert.Equal(t, uint64(2), invalid.Seq, "an invalid bundle keeps its place")
			assert.Error(t, invalid.Reason)
			assert.Equal(t, uint64(3), events[2].(BundleEvent).Seq, "bundles around it are unaffected")
			assert.Equal(t, uint64(1), p.Stats().InvalidBundles)
		})
	}
}

func TestBlockPipelineAcceptsWhatIngestMustNotReject(t *testing.T) {
	v1 := blockTx(txfixture.MustSignedV1Wire(1, 0), 0)
	events, _ := collect(t, func(p *BlockPipeline) {
		p.HandleBlock(blockMsg(10,
			v1,
			blockTx(wireTx(1), 4), blockTx(wireTx(2), 4), blockTx(wireTx(3), 4), blockTx(wireTx(4), 4), blockTx(wireTx(5), 4),
			// The same transaction in two bundles is for execution to refuse.
			blockTx(wireTx(1), 0),
		), time.Now())
	}, func() bool { return true }, 10)
	assert.Equal(t, []bundleShape{{1, false, 1}, {2, true, 5}, {3, false, 1}}, shapes(t, events),
		"a v1 transaction once active, a five-member bundle, and a cross-bundle repeat all pass ingest")
}

func TestBlockPipelineBlocksRatherThanDrops(t *testing.T) {
	events := make(chan BlockEvent)
	p := NewBlockPipeline(events, nil)
	p.SetCurrentSlot(10)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var txs []*blockpb.Transaction
		for i := range uint64(20) {
			txs = append(txs, blockTx(wireTx(i), 0))
		}
		p.HandleBlock(blockMsg(10, txs...), time.Now())
	}()
	for want := uint64(1); want <= 20; want++ {
		time.Sleep(time.Millisecond)
		e := <-events
		require.Equal(t, want, e.(BundleEvent).Seq)
	}
	<-done
}

func TestBlockEngineSessionFeedsThePipeline(t *testing.T) {
	h := newBlockEngineHarness(t, time.Hour)
	events := make(chan BlockEvent, 16)
	pipeline := NewBlockPipeline(events, nil)
	h.session.cfg.Blocks = pipeline
	h.start()
	eventually(t, h.session.Healthy, "healthy")

	pipeline.SetCurrentSlot(50)
	h.session.NotifyLeaderSlot(50, time.Now().Add(time.Hour))
	eventually(t, func() bool { return h.session.Stats().NoticesSent == 1 }, "notice sent")
	h.be.blocks <- blockMsg(49, blockTx(wireTx(1), 0))
	h.be.blocks <- blockMsg(50, blockTx(wireTx(2), 8), blockTx(wireTx(3), 8))
	select {
	case e := <-events:
		bundle := e.(BundleEvent)
		assert.Equal(t, uint64(50), bundle.Slot)
		assert.True(t, bundle.RevertProtected)
		assert.Len(t, bundle.Transactions, 2)
	case <-time.After(2 * time.Second):
		t.Fatal("no bundle")
	}

	h.be.endBlocks <- status.Error(codes.Unavailable, "reset mid-slot")
	select {
	case e := <-events:
		assert.Equal(t, StreamLostEvent{Slot: 50}, e)
	case <-time.After(2 * time.Second):
		t.Fatal("no stream-lost event")
	}
	eventually(t, h.session.Healthy, "reconnected")
	h.be.blocks <- blockMsg(50, blockTx(wireTx(4), 0))
	eventually(t, func() bool { return pipeline.Stats().LostSlot == 1 }, "the lost slot stays discarded after reconnecting")
	assert.Empty(t, events)
}
