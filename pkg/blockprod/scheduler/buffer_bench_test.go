package scheduler

import (
	"encoding/binary"
	"testing"

	"github.com/gagliardetto/solana-go"
)

// BenchmarkBufferDrain measures selection from a large, already-filled TPU
// queue. Transaction decoding and queue filling are outside the timed region.
func BenchmarkBufferDrain(b *testing.B) {
	const count = 120000
	for _, mixed := range []bool{false, true} {
		name := "equal_rewards"
		if mixed {
			name = "mixed_rewards"
		}
		b.Run(name, func(b *testing.B) {
			var buffer *Buffer
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%count == 0 {
					b.StopTimer()
					buffer = NewBuffer(count)
					for j := 0; j < count; j++ {
						e := &entry{tx: &solana.Transaction{}, seq: uint64(j), reward: 2500}
						binary.LittleEndian.PutUint64(e.messageHash[:], uint64(j))
						if mixed {
							e.reward = uint64(j*7919) % 1000
						}
						if result, _ := buffer.Insert(e); result != InsertAccepted {
							b.Fatal("queue fill failed")
						}
					}
					b.StartTimer()
				}
				if buffer.PopMax() == nil {
					b.Fatal("queue drained prematurely")
				}
			}
		})
	}
}
