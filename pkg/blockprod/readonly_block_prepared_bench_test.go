package blockprod

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/replay"
)

// Models a queue already statically prepared before leadership. Preparation is
// shifted out of this timed bank phase, not eliminated from validator CPU work.
func BenchmarkReadonlyPairPreparedFullBlock(b *testing.B) {
	f := makeReadonlyBlockFixture(b, readonlyBlockAccepted)
	setup := f.bank(b, nil)
	preparer := replay.NewTransactionPreparer(setup.SlotCtx.Features)
	prepared := make([]*replay.PreparedTransaction, len(f.txs))
	for i, tx := range f.txs {
		prepared[i] = preparer.Prepare(tx)
		if prepared[i] == nil {
			b.Fatal("preparation failed")
		}
	}
	setup.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		env := f.bank(b, nil)
		env.Bank.preparer = replay.NewTransactionPreparer(env.SlotCtx.Features)
		b.StartTimer()
		for j, tx := range f.txs {
			result, _ := env.Bank.ForgePreparedTransaction(tx, len(f.wires[j]), prepared[j])
			if result != ForgeAccepted {
				b.Fatalf("transaction %d: %v", j, result)
			}
		}
		env.Bank.Freeze()
		b.StopTimer()
		if env.Bank.CostTracker().BlockCost() != readonlyBlockAccepted*readonlyActualCost {
			b.Fatal("unexpected block cost")
		}
		env.Close()
		b.StartTimer()
	}
	b.ReportMetric(float64(readonlyBlockAccepted), "tx/block")
}
