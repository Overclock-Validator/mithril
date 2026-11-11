package sbpf

import (
	"fmt"
	"testing"
)

// withAsmLoop enables the assembly loop with the given chunk size (0 keeps
// the default) for the duration of fn. It skips where the loop is not built.
func withAsmLoop(t *testing.T, chunk uint64, fn func()) {
	t.Helper()
	prev := useAsmLoop
	useAsmLoop = true
	defer func() { useAsmLoop = prev }()
	if !asmLoopAvailable() {
		t.Skip("assembly interpreter loop is not available on this platform")
	}
	if chunk != 0 {
		defer setAsmChunk(chunk)()
	}
	fn()
}

// The existing fast-loop suites compare Run alone against Run with the fast
// loop. With the assembly loop enabled the fast side is the assembly path, so
// they check it against the reference interpreter: generated programs at every
// budget, edge cases, invalid pcs and syscall registry precedence. Tiny chunks
// force a re-entry at every instruction boundary.
func TestAsmLoopMatchesRun(t *testing.T) {
	for _, chunk := range []uint64{0, 1, 2, 7} {
		t.Run(fmt.Sprintf("chunk=%d", chunk), func(t *testing.T) {
			withAsmLoop(t, chunk, func() {
				t.Run("budgets", TestFastLoopMatchesRunAcrossBudgets)
				t.Run("edges", TestFastLoopEdgeCases)
				t.Run("invalid-pc", TestFastLoopInvalidPC)
				t.Run("registry", TestFastLoopInvocationRegistryPrecedence)
				t.Run("synthetic", TestSyntheticProgramsRun)
			})
		})
	}
}
