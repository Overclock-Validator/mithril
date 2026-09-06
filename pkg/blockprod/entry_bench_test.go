package blockprod

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

// Measure the producer's batch accounting both alone and through component
// serialization, shred generation, and broadcast-session bookkeeping.
// Transaction execution, peer routing, and UDP are excluded.
func BenchmarkEntryBuilder(b *testing.B) {
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
	for _, tc := range []struct {
		name      string
		shred     bool
		broadcast bool
	}{
		{name: "append"},
		{name: "append-and-shred", shred: true},
		{name: "append-and-broadcast", broadcast: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			builder := NewEntryBuilder(costmodel.DefaultLimits(), solana.Hash{})
			shredder := turbine.Shredder{Slot: 100, ParentSlot: 99, Version: 1}
			session := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
				Leader: leader, Slot: 100, ParentSlot: 99, Version: 1,
				Broadcaster: &benchmarkPacketBroadcaster{},
			})
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				index := i % len(txns)
				entries, _, flushed := builder.Append(txns[index], len(wires[index]))
				if flushed && tc.broadcast {
					if err := session.BroadcastEntryBatch(entries); err != nil {
						b.Fatal(err)
					}
				}
				if flushed && tc.shred {
					component, err := turbine.NewEntryBatch(entries)
					if err != nil {
						b.Fatal(err)
					}
					if _, _, _, err := shredder.MakeMerkleShredsFromComponent(
						leader, component, false, solana.Hash{}, 0, 0,
					); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

type benchmarkPacketBroadcaster struct {
	packets int
}

func (b *benchmarkPacketBroadcaster) Broadcast(packets [][]byte) error {
	b.packets += len(packets)
	return nil
}
