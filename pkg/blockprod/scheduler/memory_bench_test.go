package scheduler

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
)

// Compare a selected transaction with preparation retained by the old queue
// against the worst case of the bounded cache: a cold fallback selection.
// Both cases include queue selection, reservation and real bank execution;
// signing, ingress and bank initialization stay outside the timed region.
func BenchmarkSchedulerFallback(b *testing.B) {
	const perBank = 512
	wires := txfixture.PrecomputeTransferPool(perBank)
	for _, policy := range []struct {
		name  string
		eager int
	}{
		{"eager", perBank}, {"cold_lazy", -1},
	} {
		b.Run(policy.name, func(b *testing.B) {
			var env *blockprod.TestEnv
			var s *Scheduler
			defer func() {
				if env != nil {
					env.Close()
				}
			}()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%perBank == 0 {
					b.StopTimer()
					if env != nil {
						env.Close()
					}
					env = blockprod.NewTestEnv(blockprod.TestEnvConfig{})
					s = NewWithConfig(env.Controller, Config{
						FeatureSource:           func() *features.Features { return env.SlotCtx.Features },
						MaxPreparedTransactions: policy.eager,
					})
					for _, wire := range wires {
						s.Receive(packet.Owned(wire))
					}
					b.StartTimer()
				}
				e := s.buffer.PopMax()
				if e == nil {
					b.Fatal("empty queue")
				}
				res, reason := env.Bank.PrepareSchedule(e.wireSize)
				if reason != 0 {
					b.Fatalf("reservation rejected: %v", reason)
				}
				prepared := e.prepared
				if prepared == nil {
					prepared = s.preparer.Load().Prepare(e.tx)
				}
				result, reason := env.Bank.ForgeScheduledTransaction(e.tx, e.wireSize, prepared, res)
				s.releasePrepared(e)
				if result != blockprod.ForgeAccepted {
					b.Fatalf("forge rejected: %v / %v", result, reason)
				}
			}
		})
	}
}

// Each operation is one bounded maintenance chunk against a full-size queue.
// Refill happens outside timing; all-expired exercises heap removal/token work.
func BenchmarkSchedulerCleanupChunk(b *testing.B) {
	for _, expired := range []bool{false, true} {
		name := "live"
		if expired {
			name = "expired"
		}
		b.Run(name, func(b *testing.B) {
			var buffer *Buffer
			s := New(nil)
			for i := uint64(0); i < expiredHashHistoryCapacity; i++ {
				var hash solana.Hash
				binary.LittleEndian.PutUint64(hash[:], i)
				s.cleanupHistory.remember(hash)
			}
			cursor := 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if buffer == nil || buffer.Len() < 512 {
					b.StopTimer()
					buffer = NewBuffer(MaxBufferedTxns)
					s.preparedRetained.Store(defaultMaxPreparedTransactions)
					for j := 0; j < MaxBufferedTxns; j++ {
						e := &entry{tx: &solana.Transaction{}, seq: uint64(j), reward: uint64(j*7919) % 1000}
						binary.LittleEndian.PutUint64(e.messageHash[:], uint64(j))
						hash := uint64(j % 150)
						if !expired {
							hash += 2000
						}
						binary.LittleEndian.PutUint64(e.blockhash[:], hash)
						e.preparationSlot = j < defaultMaxPreparedTransactions
						buffer.Insert(e)
					}
					cursor = 0
					b.StartTimer()
				}
				next, inspected, _ := buffer.cleanupSome(cursor, 512, func(e *entry) bool {
					if !s.cleanupHistory.expired(e.blockhash) {
						return false
					}
					s.releasePrepared(e)
					return true
				})
				cursor = next
				if inspected != 512 {
					b.Fatalf("inspected %d entries", inspected)
				}
			}
		})
	}
}
