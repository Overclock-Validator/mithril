package turbine

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func BenchmarkEntrySignatureRoot(b *testing.B) {
	for _, count := range []int{1, 64, 311, 512, 1024} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			sigs := make([][]byte, count)
			for i := range sigs {
				sigs[i] = make([]byte, 64)
				binary.LittleEndian.PutUint64(sigs[i], uint64(i))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = hashSignatures(sigs)
			}
		})
	}
}
