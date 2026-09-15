package replay

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

// Measure the publication tail separately from preparation and ancestor checks.
// Use the identical benchmark file on both source revisions. Includes binding
// checks and index publication; excludes fixture creation, preparation and unwind.
// A warmed existing blockhash index is kept across iterations. This is an
// isolated component benchmark, not a prediction of live voting percentiles.
func BenchmarkStatusMapCriticalTail(b *testing.B) {
	for _, existing := range []bool{false, true} {
		b.Run(fmt.Sprintf("existing_%t", existing), func(b *testing.B) {
			txs := benchmarkUniqueTransactions(67520)
			parent := statusCacheTestBlock(10, txs[:33760]...)
			if !existing {
				parent.Transactions = nil
			}
			block := statusCacheTestBlock(11, txs[33760:]...)
			plan, err := planBlockTransactionExecution(block)
			if err != nil {
				b.Fatal(err)
			}
			cache := NewTransactionStatusCache()
			if err := cache.CommitBlock(parent); err != nil {
				b.Fatal(err)
			}
			durations := make([]int64, 0, b.N)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				prepared := cache.prepareTransactionStatusDelta(plan.messageIdentities)
				receipt, err := cache.validateBlockForPublication(block, plan)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				start := time.Now()
				err = cache.commitBlockWithValidation(block, plan, prepared, receipt)
				duration := time.Since(start).Nanoseconds()
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				durations = append(durations, duration)
				if err := cache.Unwind(11); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			b.StopTimer()
			sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
			b.ReportMetric(float64(durations[(len(durations)-1)/2]), "p50-ns")
			b.ReportMetric(float64(durations[(99*len(durations)+99)/100-1]), "p99-ns")
		})
	}
}
