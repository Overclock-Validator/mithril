package sealevel

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// A valid executable with reserved trailing account space models the byte
// comparisons paid by large deployed program accounts, without adding VM work.
func BenchmarkLargeUpgradeableProgramRead(b *testing.B) {
	w := expandedProgramWorkloads(b)[0]
	for _, size := range []int{256 << 10, 2 << 20, 8 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			padded := w
			padded.elf = make([]byte, max(size, len(w.elf)))
			copy(padded.elf, w.elf)
			_, data := upgradeableReadFixture(b, padded)
			source := &programReadSource{}
			source.account.Store(data)
			run := upgradeableReadRunner(b, padded, false, source, nil)
			want, err := run()
			require.NoError(b, err)
			w.check(b, want)
			used := want.ComputeMeter.Used()
			_, err = run()
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			var got *ExecutionCtx
			for range b.N {
				got, err = run()
				if err != nil || got.ComputeMeter.Used() != used {
					b.Fatalf("execution/CU changed: %v", err)
				}
			}
			b.StopTimer()
			w.check(b, got)
			b.ReportMetric(float64(used), "cu/op")
		})
	}
}
