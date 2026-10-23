package sealevel

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/stretchr/testify/require"
)

// Exercise the conservative path when immutable account identity changes on
// every invocation, even though the executable bytes remain equal.
func BenchmarkUpgradeableProgramVersions(b *testing.B) {
	for _, w := range expandedProgramWorkloads(b) {
		b.Run(w.name, func(b *testing.B) {
			_, first := upgradeableReadFixture(b, w)
			versions := [2]*accounts.Account{first, first.Clone()}
			source := &programReadSource{}
			source.account.Store(first)
			run := upgradeableReadRunner(b, w, false, source, nil)
			want, err := run()
			require.NoError(b, err)
			used := want.ComputeMeter.Used()
			b.ReportAllocs()
			b.ResetTimer()
			var got *ExecutionCtx
			for i := range b.N {
				source.account.Store(versions[i%2])
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
