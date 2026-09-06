package blockprod

import (
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"testing"
)

func BenchmarkProducerBlock50k(b *testing.B) {
	const transactions = 50000
	wires := txfixture.PrecomputeTransferPool(512)
	txns := make([]solana.Transaction, len(wires))
	for i, wire := range wires {
		tx, err := solana.TransactionFromBytes(wire)
		if err != nil {
			b.Fatal(err)
		}
		txns[i] = *tx
	}
	leader := txfixture.PayerPrivateKey()
	var lastBatches, lastPackets int
	b.ReportAllocs()
	b.ResetTimer()
	for round := 0; round < b.N; round++ {
		builder := NewEntryBuilder(costmodel.DefaultLimits(), solana.Hash{})
		sink := &benchmarkPacketBroadcaster{}
		session := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
			Leader: leader, Slot: 100, ParentSlot: 99, Version: 1, Broadcaster: sink,
		})
		if err := session.BroadcastHeader(solana.Hash{}); err != nil {
			b.Fatal(err)
		}
		batches := 0
		for i := 0; i < transactions; i++ {
			idx := i % len(txns)
			entries, _, flushed := builder.Append(txns[idx], len(wires[idx]))
			if flushed {
				if err := session.BroadcastEntryBatch(entries); err != nil {
					b.Fatal(err)
				}
				batches++
			}
		}
		if entries, _ := builder.Flush(); len(entries) > 0 {
			if err := session.BroadcastEntryBatch(entries); err != nil {
				b.Fatal(err)
			}
			batches++
		}
		if err := session.BroadcastFooter(solana.Hash{1}, 0, nil, nil); err != nil {
			b.Fatal(err)
		}
		if err := session.BroadcastEndingTickLast(builder.CurrentEntryHash()); err != nil {
			b.Fatal(err)
		}
		_ = session.BlockID(99, solana.Hash{})
		lastBatches, lastPackets = batches, sink.packets
	}
	b.StopTimer()
	b.ReportMetric(transactions, "transactions/op")
	b.ReportMetric(float64(len(wires[0])), "wire-B/tx")
	b.ReportMetric(float64(lastBatches), "entry-batches/op")
	b.ReportMetric(float64(lastPackets), "packets/op")
}
