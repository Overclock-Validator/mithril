package turbine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
)

// Compare the request-class policy with the same workers, rolling job window,
// Narya backend and total work. Shared submits prefetch as ordinary completion
// requests to reproduce the old four-permit occupancy; reserved labels it as
// prefetch. This is a saturation microbenchmark, not observed live p99 or true
// replay-head scheduling. All signatures are verified and every request joined.
func BenchmarkVerifierCompletionReservation(b *testing.B) {
	flowConfigureBackend(b)
	txs := make([]*solana.Transaction, 4096)
	for i := range txs {
		txs[i] = flowGeneratedTransaction(b, 228, uint64(i))
	}
	for _, size := range []int{256, 4096} {
		for _, reserved := range []bool{false, true} {
			b.Run(fmt.Sprintf("prefetch_%d/reserved_%t", size, reserved), func(b *testing.B) {
				v := newTransactionVerifierWithBatchTarget(2, 32, 8, nil)
				defer v.closeAndWait()
				submit := v.submitTransactions
				if reserved {
					submit = v.submitPrefetchTransactions
				}
				var admission, finish, total []time.Duration
				occupied := 0
				ctx := context.WithValue(context.Background(), entryTraceContextKey{}, true)
				warm, err := v.submitTransactions(context.Background(), txs[:256])
				if err != nil {
					b.Fatal(err)
				}
				if _, err = warm.wait(); err != nil {
					b.Fatal(err)
				}
				b.ResetTimer()
				for range b.N {
					start := time.Now()
					prior := make([]*transactionVerification, 0, 4)
					for range 3 {
						r, err := submit(context.Background(), txs[:size])
						if err != nil {
							b.Fatal(err)
						}
						prior = append(prior, r)
					}
					type result struct {
						r   *transactionVerification
						err error
					}
					fourth := make(chan result, 1)
					attempting := make(chan struct{})
					go func() {
						close(attempting)
						r, err := submit(context.Background(), txs[:size])
						fourth <- result{r, err}
					}()
					<-attempting
					if !reserved {
						last := <-fourth
						if last.err != nil {
							b.Fatal(last.err)
						}
						prior = append(prior, last.r)
					}
					expectedActive := cap(v.requests)
					if reserved {
						expectedActive--
					}
					if len(v.requests) == expectedActive {
						occupied++
					}
					r, err := v.submitTransactions(ctx, txs[:32])
					if err != nil {
						b.Fatal(err)
					}
					if _, err = r.wait(); err != nil {
						b.Fatal(err)
					}
					admission = append(admission, time.Duration(r.trace.Admitted-r.trace.Submit))
					finish = append(finish, time.Duration(r.trace.Finished-r.trace.Submit))
					if reserved {
						last := <-fourth
						if last.err != nil {
							b.Fatal(last.err)
						}
						prior = append(prior, last.r)
					}
					for _, r := range prior {
						if _, err = r.wait(); err != nil {
							b.Fatal(err)
						}
					}
					total = append(total, time.Since(start))
				}
				b.StopTimer()
				b.ReportMetric(float64(occupied)/float64(b.N), "occupied_fraction")
				for _, metric := range []struct {
					name   string
					values []time.Duration
				}{
					{"completion_admit", admission}, {"completion_done", finish}, {"all_work", total},
				} {
					flowReportPercentiles(b, metric.values, metric.name)
					index := max(0, (len(metric.values)*99+99)/100-1)
					b.ReportMetric(float64(metric.values[index])/float64(time.Millisecond), metric.name+"_p99-ms")
					b.ReportMetric(float64(metric.values[len(metric.values)-1])/float64(time.Millisecond), metric.name+"_max-ms")
				}
			})
		}
	}
}
