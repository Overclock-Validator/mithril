package sbpf

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
)

// runWithFastLoop runs p once with runFast enabled or disabled and returns
// everything observable: return value, CU, meter state, error text and PC,
// memory contents and whether the pooled buffers came back clean.
func runWithFastLoop(t *testing.T, p *Program, fast bool, budget uint64, seed int64) string {
	t.Helper()
	prev := useFastLoop
	useFastLoop = fast
	defer func() { useFastLoop = prev }()

	rng := rand.New(rand.NewSource(seed))
	input := make([]byte, 700)
	for j := range input {
		input[j] = byte(j)
	}
	var regions []InputRegion
	if rng.Intn(2) == 0 {
		regions = []InputRegion{
			{Offset: 0, HostOffset: 0, RegionSize: 100, AddressSpaceReserved: 100, Writable: true, AccountIndex: -1},
			{Offset: 100, HostOffset: 100, RegionSize: 150, AddressSpaceReserved: 300, Writable: rng.Intn(2) == 0, AccountIndex: 0},
			{Offset: 400, HostOffset: 400, RegionSize: 300, AddressSpaceReserved: 300, Writable: true, AccountIndex: -1},
		}
	}
	cm := cu.NewComputeMeter(budget)
	ip := NewInterpreter(p, &VMOpts{
		HeapMax:               4096 * (1 + rng.Intn(4)),
		Syscalls:              diffRegistry,
		ComputeMeter:          &cm,
		Input:                 input,
		InputRegions:          regions,
		DisableStackFrameGaps: rng.Intn(3) == 0,
	})
	var ret, cuUsed uint64
	var runErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				runErr = fmt.Errorf("PANIC: %v", r)
			}
		}()
		ret, cuUsed, runErr = ip.Run()
	}()
	errStr, pc := "<nil>", int64(-1)
	if runErr != nil {
		errStr = runErr.Error()
		var exc *Exception
		if errors.As(runErr, &exc) {
			pc = exc.PC
		}
	}
	h := memHash(ip.stack.mem, ip.heap, input)
	depth := len(ip.stack.shadow)
	ip.Finish()
	return fmt.Sprintf("ret=%d cu=%d remaining=%d err=%q pc=%d depth=%d mem=%x",
		ret, cuUsed, cm.Remaining(), errStr, pc, depth, h)
}

func requireSameWithAndWithoutFastLoop(t *testing.T, name string, p *Program, budget uint64, seed int64) {
	t.Helper()
	slow := runWithFastLoop(t, p, false, budget, seed)
	fast := runWithFastLoop(t, p, true, budget, seed)
	if slow != fast {
		t.Fatalf("%s budget=%d:\n run:     %s\n runFast: %s", name, budget, slow, fast)
	}
}

// Every generated program, at budgets around and inside its full cost, so CU
// exhaustion lands on every kind of instruction, including budget 0.
func TestFastLoopMatchesRunAcrossBudgets(t *testing.T) {
	rng := rand.New(rand.NewSource(424242))
	programs := 4000
	if testing.Short() {
		programs = 400
	}
	for i := 0; i < programs; i++ {
		ver := uint32(i % 4)
		p := genProgram(rng, ver)
		if p.Verify() != nil {
			continue
		}
		resolveCallTargetsIfSupported(p)
		seed := rng.Int63()
		full := runWithFastLoop(t, p, false, 1<<20, seed)
		var used uint64
		if _, err := fmt.Sscanf(full, "ret=%d cu=%d", new(uint64), &used); err != nil {
			t.Fatal(err)
		}
		budgets := []uint64{0, 1, 2, used - 1, used, used + 1, 1 << 20}
		for k := 0; k < 6; k++ {
			budgets = append(budgets, uint64(rng.Int63n(int64(used)+2)))
		}
		for _, b := range budgets {
			if b > 1<<20 {
				continue
			}
			requireSameWithAndWithoutFastLoop(t, fmt.Sprintf("program %d v%d", i, ver), p, b, seed)
		}
	}
}

func verifiedProgram(t *testing.T, text []Slot, ver uint32, funcs map[uint32]int64) *Program {
	t.Helper()
	p := mkProgram(text, ver)
	for h, pc := range funcs {
		p.Funcs[h] = pc
	}
	if err := p.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
	resolveCallTargetsIfSupported(p)
	return p
}

func TestFastLoopEdgeCases(t *testing.T) {
	for _, ver := range []uint32{sbpfver.SbpfVersionV0, sbpfver.SbpfVersionV1, sbpfver.SbpfVersionV3} {
		ver := ver
		t.Run(fmt.Sprintf("v%d", ver), func(t *testing.T) {
			check := func(name string, p *Program) {
				for _, b := range []uint64{0, 1, 3, 7, 50, 1 << 20} {
					requireSameWithAndWithoutFastLoop(t, name, p, b, 1)
				}
			}

			// Division and modulo by a zero register after fast instructions.
			if ver < sbpfver.SbpfVersionV2 {
				for _, op := range []uint8{OpDiv64Reg, OpDiv32Reg, OpMod64Reg, OpMod32Reg} {
					check(fmt.Sprintf("div0 op=%#x", op), verifiedProgram(t, []Slot{
						slot(OpMov64Imm, 1, 0, 0, 9),
						slot(OpMov64Imm, 2, 0, 0, 0),
						slot(OpAdd64Imm, 1, 0, 0, 1),
						slot(op, 1, 2, 0, 0),
						slot(OpExit, 0, 0, 0, 0),
					}, ver, nil))
				}
			}

			// Unbounded recursion: call depth exhaustion inside fast calls.
			var rec *Program
			if ver >= sbpfver.SbpfVersionV3 {
				rec = verifiedProgram(t, []Slot{
					slot(OpAdd64Imm, 1, 0, 0, 1),
					slot(OpCall, 0, 1, 0, uint32(0xfffffffe)), // pc 1 -> target 0
					slot(OpExit, 0, 0, 0, 0),
				}, ver, nil)
			} else {
				h := PCHash(0)
				rec = verifiedProgram(t, []Slot{
					slot(OpAdd64Imm, 1, 0, 0, 1),
					slot(OpCall, 0, 0, 0, h),
					slot(OpExit, 0, 0, 0, 0),
				}, ver, map[uint32]int64{h: 0})
			}
			check("recursion", rec)

			// Syscalls alternating with fast instructions (re-entry each time).
			text := []Slot{slot(OpMov64Imm, 6, 0, 0, 50)}
			for k := 0; k < 4; k++ {
				text = append(text, diffLoadImm64(1, VaddrInput+uint64(8*k), ver)...)
				text = append(text,
					slot(OpAdd64Reg, 6, 1, 0, 0),
					slot(OpCall, 0, 0, 0, hashPeek),
					slot(OpAdd64Reg, 6, 0, 0, 0))
			}
			text = append(text, slot(OpMov64Reg, 0, 6, 0, 0), slot(OpExit, 0, 0, 0, 0))
			check("syscall alternation", verifiedProgram(t, text, ver, nil))
		})
	}
}

// Unverified control flow: falling off the end and jumping past it must fail
// the same way (or panic the same way) with and without the fast loop.
func TestFastLoopInvalidPC(t *testing.T) {
	for _, ver := range []uint32{sbpfver.SbpfVersionV0, sbpfver.SbpfVersionV3} {
		for name, text := range map[string][]Slot{
			"fall off end":      {slot(OpMov64Imm, 1, 0, 0, 1), slot(OpAdd64Imm, 1, 0, 0, 1)},
			"jump past end":     {slot(OpJa, 0, 0, 5, 0), slot(OpExit, 0, 0, 0, 0)},
			"jump before start": {slot(OpMov64Imm, 1, 0, 0, 1), slot(OpJa, 0, 0, -5, 0), slot(OpExit, 0, 0, 0, 0)},
		} {
			p := mkProgram(text, ver)
			resolveCallTargetsIfSupported(p)
			for _, b := range []uint64{0, 1, 2, 100} {
				requireSameWithAndWithoutFastLoop(t, fmt.Sprintf("%s v%d", name, ver), p, b, 2)
			}
		}
	}
}

// Syscall-heavy code leaves and re-enters runFast at every syscall; this
// measures that overhead against Run alone.
func BenchmarkSyscallAlternation(b *testing.B) {
	for _, fast := range []bool{false, true} {
		b.Run(fmt.Sprintf("fast=%v", fast), func(b *testing.B) {
			prev := useFastLoop
			useFastLoop = fast
			defer func() { useFastLoop = prev }()
			text := []Slot{slot(OpMov64Imm, 6, 0, 0, 0), slot(OpMov64Imm, 7, 0, 0, 2000)}
			text = append(text, diffLoadImm64(1, VaddrInput, 0)...)
			// loop: two ALU ops and a syscall per iteration
			text = append(text,
				slot(OpAdd64Imm, 6, 0, 0, 3),
				slot(OpCall, 0, 0, 0, hashPeek),
				slot(OpSub64Imm, 7, 0, 0, 1),
				slot(OpJneImm, 7, 0, -4, 0),
				slot(OpMov64Reg, 0, 6, 0, 0),
				slot(OpExit, 0, 0, 0, 0))
			p := mkProgram(text, 0)
			if err := p.Verify(); err != nil {
				b.Fatal(err)
			}
			resolveCallTargetsIfSupported(p)
			input := make([]byte, 700)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runProgram(b, p, input, diffRegistry, 1<<40)
			}
		})
	}
}

// runFast relies on handling no jmp32/pqr-class opcode; keep this list in
// sync with its cases.
func TestRunFastCaseClasses(t *testing.T) {
	ops := []uint8{OpLdxb, OpLdxh, OpLdxw, OpLdxdw, OpStb, OpSth, OpStw, OpStdw, OpStxb, OpStxh, OpStxw, OpStxdw, OpAdd32Imm, OpAdd32Reg, OpAdd64Imm, OpAdd64Reg, OpSub32Imm, OpSub32Reg, OpSub64Imm, OpSub64Reg, OpMul32Imm, OpOr32Imm, OpOr32Reg, OpOr64Imm, OpOr64Reg, OpAnd32Imm, OpAnd32Reg, OpAnd64Imm, OpAnd64Reg, OpLsh32Imm, OpLsh32Reg, OpLsh64Imm, OpLsh64Reg, OpRsh32Imm, OpRsh32Reg, OpRsh64Imm, OpRsh64Reg, OpXor32Imm, OpXor32Reg, OpXor64Imm, OpXor64Reg, OpMov32Imm, OpMov32Reg, OpMov64Imm, OpMov64Reg, OpLddw, OpJa, OpJeqImm, OpJeqReg, OpJgtImm, OpJgtReg, OpJgeImm, OpJgeReg, OpJltImm, OpJltReg, OpJleImm, OpJleReg, OpJsetImm, OpJsetReg, OpJneImm, OpJneReg, OpJsgtImm, OpJsgtReg, OpJsgeImm, OpJsgeReg, OpJsltImm, OpJsltReg, OpJsleImm, OpJsleReg, OpMul32Reg, OpMul64Imm, OpMul64Reg, OpDiv32Reg, OpDiv64Imm, OpDiv64Reg, OpMod32Reg, OpMod64Imm, OpMod64Reg, OpArsh32Imm, OpArsh32Reg, OpArsh64Imm, OpArsh64Reg, OpCall, OpExit}
	for _, op := range ops {
		if op&0x07 == ClassPqr {
			t.Errorf("op %#x is in the jmp32/pqr class", op)
		}
	}
}
