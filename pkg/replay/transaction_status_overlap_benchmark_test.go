package replay

import (
	"fmt"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
)

// This controlled workload runs 4,096 executions of the transfer fixture while
// preparing 33,760 independent status keys. It measures scheduling/GC contention,
// not full replay: no accounts are committed, and the status fixture differs
// from the repeated transfer fixture. Run with -cpu=1,2 to compare contention
// without and with a spare execution thread. Check live replay separately.
func BenchmarkTransactionStatusExecutionOverlap(tb *testing.B) {
	for _, mode := range []string{"legacy", "sized", "overlap"} {
		tb.Run(mode, func(tb *testing.B) {
			slotCtx, cleanup := newCommitTestSlotCtx()
			defer cleanup()
			tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
			if err != nil {
				tb.Fatal(err)
			}
			cache := NewTransactionStatusCache()
			if err := cache.CommitBlock(statusCacheTestBlock(10)); err != nil {
				tb.Fatal(err)
			}
			blk := statusCacheTestBlock(11, benchmarkUniqueTransactions(33760)...)
			plan, err := planBlockTransactionExecution(blk)
			if err != nil {
				tb.Fatal(err)
			}
			var execution, commit time.Duration
			tb.ReportAllocs()
			tb.ResetTimer()
			for range tb.N {
				var p *transactionStatusPreparation
				if mode == "overlap" {
					p = cache.startStatusPreparation(plan)
				}
				start := time.Now()
				for range 4096 {
					output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{SlotCtx: slotCtx, Transaction: tx, LeanResult: true})
					if output.ProcessingResult.TransactionError != nil {
						tb.Fatal(output.ProcessingResult.TransactionError)
					}
				}
				execution += time.Since(start)
				start = time.Now()
				switch mode {
				case "legacy":
					err = cache.legacyCommitStatusForBenchmark(blk, plan)
				case "sized":
					err = cache.commitBlockWithPlan(blk, plan)
				case "overlap":
					err = cache.commitBlockWithPreparedDelta(blk, plan, p.wait())
				}
				commit += time.Since(start)
				if err != nil {
					tb.Fatal(err)
				}
				tb.StopTimer()
				if err := cache.Unwind(11); err != nil {
					tb.Fatal(err)
				}
				tb.StartTimer()
			}
			tb.StopTimer()
			tb.ReportMetric(float64(execution.Nanoseconds())/float64(tb.N), "execution-ns/op")
			tb.ReportMetric(float64(commit.Nanoseconds())/float64(tb.N), "commit-with-wait-ns/op")
		})
	}
}

func BenchmarkTransactionStatusSmallPublication(tb *testing.B) {
	for _, count := range []int{0, 1, 32} {
		for _, mode := range []string{"legacy", "prepared_total"} {
			tb.Run(fmt.Sprintf("txs_%d/%s", count, mode), func(tb *testing.B) {
				cache := NewTransactionStatusCache()
				if err := cache.CommitBlock(statusCacheTestBlock(10)); err != nil {
					tb.Fatal(err)
				}
				blk := statusCacheTestBlock(11, benchmarkUniqueTransactions(count)...)
				plan, err := planBlockTransactionExecution(blk)
				if err != nil {
					tb.Fatal(err)
				}
				tb.ReportAllocs()
				tb.ResetTimer()
				for range tb.N {
					if mode == "legacy" {
						err = cache.legacyCommitStatusForBenchmark(blk, plan)
					} else {
						err = cache.commitBlockWithPreparedDelta(blk, plan, cache.startStatusPreparation(plan).wait())
					}
					if err != nil {
						tb.Fatal(err)
					}
					// Include unwind equally in this small-work benchmark, avoiding
					// timer start/stop overhead around microsecond operations.
					if err := cache.Unwind(11); err != nil {
						tb.Fatal(err)
					}
				}
			})
		}
	}
}
