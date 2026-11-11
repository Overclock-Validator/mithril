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
	t.Run("threaded", func(t *testing.T) {
		defer setAsmSharedDispatch(false)()
		testAsmLoopMatchesRun(t, []uint64{0, 1, 2, 7})
	})
	// The shared-dispatch set is an experiment: the default chunk and the
	// smallest are enough to cover it against the reference.
	t.Run("shared", func(t *testing.T) { defer setAsmSharedDispatch(true)(); testAsmLoopMatchesRun(t, []uint64{0, 1}) })
}

func testAsmLoopMatchesRun(t *testing.T, chunks []uint64) {
	for _, chunk := range chunks {
		t.Run(fmt.Sprintf("chunk=%d", chunk), func(t *testing.T) {
			withAsmLoop(t, chunk, func() {
				// The budgets suite is nearly all of this package's run time
				// under the race detector (about 150 s of 157 s with two
				// threads), and it already runs there once against the Go
				// loop. The assembly reruns of it are single-threaded
				// comparisons, so they run without the detector only.
				if !raceDetector {
					t.Run("budgets", TestFastLoopMatchesRunAcrossBudgets)
				}
				t.Run("edges", TestFastLoopEdgeCases)
				t.Run("invalid-pc", TestFastLoopInvalidPC)
				t.Run("registry", TestFastLoopInvocationRegistryPrecedence)
				t.Run("synthetic", TestSyntheticProgramsRun)
			})
		})
	}
}
