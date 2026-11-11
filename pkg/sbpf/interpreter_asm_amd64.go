//go:build amd64 && !purego

package sbpf

import (
	"os"
	"sync/atomic"
	"unsafe"
)

//go:generate go run gen_interp_amd64.go

// useAsmLoop routes the fast loop through the amd64 assembly interpreter
// (interpreter_asm_amd64.s). It is off unless MITHRIL_SBPF_ASM_LOOP=1, so one
// binary can run both arms of a comparison. The Go loops stay the reference:
// the assembly loop executes runFast's instructions with identical results and
// leaves everything else, untouched, to Run.
var useAsmLoop = os.Getenv("MITHRIL_SBPF_ASM_LOOP") == "1"

// asmChunk bounds one call into the assembly loop, in instructions. Assembly
// has no preemption points, so this bounds how long a goroutine can delay a
// stop-the-world; asmYield is the preemption point between chunks.
var asmChunk uint64 = 1 << 14

// runAsmLoop executes instructions starting at pc until rem instructions have
// completed, pc leaves the text, or it reaches an instruction it does not
// handle (which it leaves untouched). It returns the next pc and the unused
// part of rem. text must have textLen elements and table 256 handler entries.
//
//go:noescape
func runAsmLoop(text *Slot, textLen int64, r *[16]uint64, regions *[numFastRegions]memRegion, table *[256]uintptr, stack *Stack, pc int64, rem uint64) (newPC int64, newRem uint64)

// asmHandlerTable returns the handler addresses, indexed by handler ID.
func asmHandlerTable() *[numAsmHandlers]uintptr

// asmTables caches one opcode table per combination of the version gates that
// change which handler an opcode uses. Tables are immutable once stored.
var asmTables [1 << 7]atomic.Pointer[[256]uintptr]

func asmFlagsIndex(f fastFlags, gaps bool) int {
	idx := 0
	for i, on := range [...]bool{f.moveMem, f.pqr, f.swapSub, f.explicitSext, f.disableLddw, f.staticSyscalls, gaps} {
		if on {
			idx |= 1 << i
		}
	}
	return idx
}

// asmVersionTables caches the opcode table per SBPF version number and per
// stack-gap setting, so an invocation finds its table with one load. Every
// version gate is a function of that number alone.
var asmVersionTables [8][2]atomic.Pointer[[256]uintptr]

// The assembly indexes the window table by shifting, so a record is 64 bytes.
const _ = uint(64 - unsafe.Sizeof(memRegion{}))
const _ = uint(unsafe.Sizeof(memRegion{}) - 64)

// asmRegionsHaveGaps reports whether any window uses stack-frame gap
// translation. Only initRegions sets the gap fields, and only for the stack,
// so the answer cannot change while an interpreter runs: every later write to
// the table stores a record whose gap fields are zero.
func asmRegionsHaveGaps(regions *[numFastRegions]memRegion) bool {
	var g uint64
	for i := range regions {
		g |= regions[i].gapBit | regions[i].gapMask
	}
	return g != 0
}

// asmLoopTable returns the opcode table for this interpreter, whose version
// gates are f, or nil when the assembly loop is disabled or there is no text.
func (ip *Interpreter) asmLoopTable(f fastFlags) *[256]uintptr {
	if !useAsmLoop || len(ip.text) == 0 {
		return nil
	}
	gaps := 0
	if asmRegionsHaveGaps(&ip.regions) {
		gaps = 1
	}
	if v := ip.sbpfVersion.Version; v < uint32(len(asmVersionTables)) {
		if t := asmVersionTables[v][gaps].Load(); t != nil {
			return t
		}
	}
	return asmLoopTableSlow(ip.sbpfVersion.Version, f, gaps == 1)
}

//go:noinline
func asmLoopTableSlow(version uint32, f fastFlags, gaps bool) *[256]uintptr {
	slot := &asmTables[asmFlagsIndex(f, gaps)]
	t := slot.Load()
	if t == nil {
		slot.CompareAndSwap(nil, buildAsmTable(f, gaps))
		t = slot.Load()
	}
	if version < uint32(len(asmVersionTables)) {
		g := 0
		if gaps {
			g = 1
		}
		asmVersionTables[version][g].Store(t)
	}
	return t
}

// asmLoopAvailable reports whether this build has the assembly loop.
func asmLoopAvailable() bool { return true }

// buildAsmTable maps each opcode to its handler under f. It follows runFast
// case by case: an opcode runFast refuses under f (or always) maps to asmBail.
// gaps must be true if any window has gap fields set (asmRegionsHaveGaps):
// the NoGap memory handlers leave the gap arithmetic out.
func buildAsmTable(f fastFlags, gaps bool) *[256]uintptr {
	h := asmHandlerTable()
	t := new([256]uintptr)
	for i := range t {
		t[i] = h[asmBail]
	}
	set := func(op uint8, id int) { t[op] = h[id] }
	pick := func(cond bool, yes, no int) int {
		if cond {
			return yes
		}
		return no
	}

	if !f.moveMem {
		set(OpLdxb, pick(gaps, asmLdxb, asmLdxbNoGap))
		set(OpLdxh, pick(gaps, asmLdxh, asmLdxhNoGap))
		set(OpLdxw, pick(gaps, asmLdxw, asmLdxwNoGap))
		set(OpLdxdw, pick(gaps, asmLdxdw, asmLdxdwNoGap))
		set(OpStb, pick(gaps, asmStb, asmStbNoGap))
		set(OpSth, pick(gaps, asmSth, asmSthNoGap))
		set(OpStw, pick(gaps, asmStw, asmStwNoGap))
		set(OpStdw, pick(gaps, asmStdw, asmStdwNoGap))
		set(OpStxb, pick(gaps, asmStxb, asmStxbNoGap))
		set(OpStxh, pick(gaps, asmStxh, asmStxhNoGap))
		set(OpStxw, pick(gaps, asmStxw, asmStxwNoGap))
		set(OpStxdw, pick(gaps, asmStxdw, asmStxdwNoGap))
	}

	// sext32(explicit, v) zero-extends when explicit and sign-extends otherwise.
	zx := f.explicitSext
	set(OpAdd32Imm, pick(zx, asmAdd32ImmZX, asmAdd32ImmSX))
	set(OpAdd32Reg, pick(zx, asmAdd32RegZX, asmAdd32RegSX))
	if f.swapSub {
		set(OpSub32Imm, pick(zx, asmSub32ImmSwapZX, asmSub32ImmSwapSX))
		set(OpSub64Imm, asmSub64ImmSwap)
	} else {
		set(OpSub32Imm, pick(zx, asmSub32ImmZX, asmSub32ImmSX))
		set(OpSub64Imm, asmSub64Imm)
	}
	set(OpSub32Reg, pick(zx, asmSub32RegZX, asmSub32RegSX))
	// mov32 reg is the opposite: it sign-extends under explicit sign extension.
	set(OpMov32Reg, pick(f.explicitSext, asmMov32RegSX, asmMov32RegZX))

	set(OpAdd64Imm, asmAdd64Imm)
	set(OpAdd64Reg, asmAdd64Reg)
	set(OpSub64Reg, asmSub64Reg)
	set(OpMul32Imm, asmMul32Imm)
	set(OpOr32Imm, asmOr32Imm)
	set(OpOr32Reg, asmOr32Reg)
	set(OpOr64Imm, asmOr64Imm)
	set(OpOr64Reg, asmOr64Reg)
	set(OpAnd32Imm, asmAnd32Imm)
	set(OpAnd32Reg, asmAnd32Reg)
	set(OpAnd64Imm, asmAnd64Imm)
	set(OpAnd64Reg, asmAnd64Reg)
	set(OpLsh32Imm, asmLsh32Imm)
	set(OpLsh32Reg, asmLsh32Reg)
	set(OpLsh64Imm, asmLsh64Imm)
	set(OpLsh64Reg, asmLsh64Reg)
	set(OpRsh32Imm, asmRsh32Imm)
	set(OpRsh32Reg, asmRsh32Reg)
	set(OpRsh64Imm, asmRsh64Imm)
	set(OpRsh64Reg, asmRsh64Reg)
	set(OpXor32Imm, asmXor32Imm)
	set(OpXor32Reg, asmXor32Reg)
	set(OpXor64Imm, asmXor64Imm)
	set(OpXor64Reg, asmXor64Reg)
	set(OpMov32Imm, asmMov32Imm)
	set(OpMov64Imm, asmMov64Imm)
	set(OpMov64Reg, asmMov64Reg)
	set(OpArsh32Imm, asmArsh32Imm)
	set(OpArsh32Reg, asmArsh32Reg)
	set(OpArsh64Imm, asmArsh64Imm)
	set(OpArsh64Reg, asmArsh64Reg)
	if !f.disableLddw {
		set(OpLddw, asmLddw)
	}

	set(OpJa, asmJa)
	set(OpJeqImm, asmJeqImm)
	set(OpJeqReg, asmJeqReg)
	set(OpJgtImm, asmJgtImm)
	set(OpJgtReg, asmJgtReg)
	set(OpJgeImm, asmJgeImm)
	set(OpJgeReg, asmJgeReg)
	set(OpJltImm, asmJltImm)
	set(OpJltReg, asmJltReg)
	set(OpJleImm, asmJleImm)
	set(OpJleReg, asmJleReg)
	set(OpJsetImm, asmJsetImm)
	set(OpJsetReg, asmJsetReg)
	set(OpJneImm, asmJneImm)
	set(OpJneReg, asmJneReg)
	set(OpJsgtImm, asmJsgtImm)
	set(OpJsgtReg, asmJsgtReg)
	set(OpJsgeImm, asmJsgeImm)
	set(OpJsgeReg, asmJsgeReg)
	set(OpJsltImm, asmJsltImm)
	set(OpJsltReg, asmJsltReg)
	set(OpJsleImm, asmJsleImm)
	set(OpJsleReg, asmJsleReg)

	if !f.pqr {
		set(OpMul32Reg, asmMul32Reg)
		set(OpMul64Imm, asmMul64Imm)
		set(OpMul64Reg, asmMul64Reg)
		set(OpDiv32Reg, asmDiv32Reg)
		set(OpDiv64Imm, asmDiv64Imm)
		set(OpDiv64Reg, asmDiv64Reg)
		set(OpMod32Reg, asmMod32Reg)
		set(OpMod64Imm, asmMod64Imm)
		set(OpMod64Reg, asmMod64Reg)
	}
	// Legacy call immediates may name a syscall and stay on Run's path. The
	// handler computes the target as pc + imm + 1, the static-syscall rule of
	// CalculateCallImmTargetPC; f always comes from the interpreter's version.
	if f.staticSyscalls {
		set(OpCall, asmCallStatic)
	}
	set(OpExit, asmExit)
	return t
}

// asmYield is a preemption point between chunks: unlike the assembly loop,
// its prologue honours a pending preemption request.
//
//go:noinline
func asmYield() { asmYieldSink() }

//go:noinline
func asmYieldSink() {}

// runAsmChunks continues after the assembly loop used up a whole chunk with
// budget still left. Run makes the first call itself; this keeps the yield and
// the loop out of Run's hot path. It returns when the loop stops at an
// instruction it leaves to Run, or when the budget is spent.
func (ip *Interpreter) runAsmChunks(r *[16]uint64, pc int64, due, budget uint64, table *[256]uintptr) (int64, uint64) {
	text := ip.text
	for {
		asmYield()
		// A chunk of at least one instruction, so the loop always progresses.
		rem := min(budget-due, max(asmChunk, 1))
		newPC, left := runAsmLoop(&text[0], int64(len(text)), r, &ip.regions, table, &ip.stack, pc, rem)
		pc, due = newPC, due+(rem-left)
		if left != 0 || due == budget {
			return pc, due
		}
	}
}

// setAsmChunk changes the chunk size and returns a function restoring it.
func setAsmChunk(n uint64) func() {
	prev := asmChunk
	asmChunk = n
	return func() { asmChunk = prev }
}
