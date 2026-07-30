//go:build !sbpfprofile

package sbpf

// Profiling is compiled out unless the sbpfprofile build tag is set, so an
// ordinary node carries none of it. These stubs are trivially inlinable and
// leave no trace in the emitted code.
//
// A build tag rather than a runtime flag because the counters sit in the
// interpreter's innermost loop, which is consensus-critical: a tag makes it
// impossible for a production build to contain them by accident, and keeps the
// dispatch loop byte-identical to what ships.

const sbpfProfileEnabled = false

func profileInstruction(uint8) {}

func profileRun(int64) {}

func profileSyscall(uint32, int64) {}

func profileNow() int64 { return 0 }
