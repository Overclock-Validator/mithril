//go:build !amd64 || purego

package sbpf

// The assembly interpreter loop exists only for amd64. Elsewhere, and under
// the purego tag, runFast is the only fast loop and none of this is reached:
// asmLoopTable always returns nil.
var useAsmLoop = false

var asmChunk uint64 = 1 << 14

func (ip *Interpreter) asmLoopTable(fastFlags) *[256]uintptr { return nil }

func asmLoopAvailable() bool { return false }

func runAsmLoop(_ *Slot, _ int64, _ *[16]uint64, _ *[numFastRegions]memRegion, _ *[256]uintptr, _ *Stack, pc int64, rem uint64, _ *int64) (int64, uint64) {
	return pc, rem
}

func (ip *Interpreter) runAsmChunks(_ *[16]uint64, pc int64, due, _ uint64, _ *[256]uintptr) (int64, uint64) {
	return pc, due
}

func (ip *Interpreter) asmCalls() *int64 { return nil }

func setAsmChunk(uint64) func() { return func() {} }
