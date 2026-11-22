//go:build amd64 && !purego

package sbpf

import (
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
)

// asmStepVM is an interpreter whose registers the test owns, so single
// instructions can be executed outside Run.
type asmStepVM struct {
	ip    *Interpreter
	input []byte
	r     [16]uint64
}

func newAsmStepVM(t *testing.T, ver uint32, disableGaps bool, seed int64) *asmStepVM {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	ro := make([]byte, 300)
	rng.Read(ro)
	p := mkProgram([]Slot{slot(OpExit, 0, 0, 0, 0)}, ver)
	p.RO = ro
	// Larger than the dirty bitmap covers, so write offsets past page 63 occur.
	input := make([]byte, fastDirtyBytes+3*4096)
	rng.Read(input)
	cm := cu.NewComputeMeter(1 << 30)
	ip := NewInterpreter(p, &VMOpts{
		HeapMax:               32 * 1024,
		Syscalls:              noSyscalls,
		ComputeMeter:          &cm,
		Input:                 input,
		DisableStackFrameGaps: disableGaps,
	})
	rng.Read(ip.stack.mem)
	rng.Read(ip.heap)
	return &asmStepVM{ip: ip, input: input}
}

// finish returns the VM's buffers to their pools. The tests overwrite dirty
// bits and shadow frames freely, so mark everything as used first.
func (vm *asmStepVM) finish() {
	vm.ip.fastCalls = nil
	for k := range vm.ip.regions {
		vm.ip.regions[k].dirty = ^uint64(0)
	}
	vm.ip.stack.shadow = vm.ip.stack.shadow[:1]
	vm.ip.stack.maxDepth = cap(vm.ip.stack.shadow)
	vm.ip.Finish()
}

func (vm *asmStepVM) buffers() [3][]byte { return [3][]byte{vm.ip.stack.mem, vm.ip.heap, vm.input} }

func (vm *asmStepVM) describe(pc int64, due uint64, panicked string) string {
	var dirty [numFastRegions]uint64
	for i := range dirty {
		dirty[i] = vm.ip.regions[i].dirty
	}
	st := &vm.ip.stack
	return fmt.Sprintf("pc=%d due=%d panic=%q r=%x dirty=%x depth=%d maxDepth=%d", pc, due, panicked, vm.r, dirty, len(st.shadow), st.maxDepth)
}

// asmSameState compares everything a step may change except guest memory.
func asmSameState(a, b *asmStepVM) bool {
	if a.r != b.r {
		return false
	}
	for i := range a.ip.regions {
		if a.ip.regions[i].dirty != b.ip.regions[i].dirty {
			return false
		}
	}
	as, bs := &a.ip.stack, &b.ip.stack
	return len(as.shadow) == len(bs.shadow) && as.maxDepth == bs.maxDepth &&
		slices.Equal(as.shadow[:cap(as.shadow)], bs.shadow[:cap(bs.shadow)])
}

// asmSameMemory compares guest memory of two VMs. With near >= 0 it only looks
// at the bytes around that offset in each buffer, which is where a store to
// that guest offset (or to its gap-adjusted stack offset) lands.
func asmSameMemory(a, b *asmStepVM, near int64) bool {
	ab, bb := a.buffers(), b.buffers()
	for i := range ab {
		if near < 0 {
			if !bytes.Equal(ab[i], bb[i]) {
				return false
			}
			continue
		}
		for _, center := range []int64{near, near - (near&int64(GapMask&0xffffffff))>>1} {
			lo, hi := max(center-64, 0), min(center+64, int64(len(ab[i])))
			if lo < hi && !bytes.Equal(ab[i][lo:hi], bb[i][lo:hi]) {
				return false
			}
		}
	}
	return true
}

// asmCatch runs one step and reports a panic as part of its outcome: the
// reference loop panics on shift counts Go rejects, and the assembly path must
// reach the same panic by handing that instruction back.
func asmCatch(step func() (int64, uint64)) (pc int64, due uint64, panicked string) {
	defer func() {
		if r := recover(); r != nil {
			panicked = fmt.Sprint(r)
		}
	}()
	pc, due = step()
	return pc, due, ""
}

// Values that sit on the boundaries the handlers distinguish.
var asmEdges = []uint64{
	0, 1, 2, 7, 8, 31, 32, 33, 63, 64, 65, 0x7f, 0x80, 0xff, 0x100, 0x7fff, 0x8000, 0xffff,
	0x7fffffff, 0x80000000, 0xffffffff, 0x100000000, 0x7fffffffffffffff, 0x8000000000000000,
	0xffffffffffffffff, 0xffffffff00000000, 0xfffffffffffffff8,
}

func asmRandValue(rng *rand.Rand) uint64 {
	switch rng.Intn(4) {
	case 0:
		return asmEdges[rng.Intn(len(asmEdges))]
	case 1:
		return asmEdges[rng.Intn(len(asmEdges))] + uint64(rng.Intn(5)) - 2
	case 2:
		return uint64(rng.Uint32())
	default:
		return rng.Uint64()
	}
}

// asmRandAddr returns a guest address near something the translation checks:
// the ends of each window's mapping, stack frame gaps, the dirty bitmap limit
// and the window boundaries themselves.
func asmRandAddr(rng *rand.Rand, vm *asmStepVM) uint64 {
	window := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 0xffff, 0xffffffff}[rng.Intn(10)] << 32
	lengths := []uint64{300, StackMax, uint64(len(vm.ip.heap)), uint64(len(vm.input)), fastDirtyBytes, StackFrameSize, 2 * StackFrameSize, 1 << 32}
	var off uint64
	switch rng.Intn(4) {
	case 0:
		off = uint64(rng.Intn(24))
	case 1:
		off = lengths[rng.Intn(len(lengths))] + uint64(rng.Intn(24)) - 12
	case 2:
		off = uint64(rng.Intn(70))*StackFrameSize + uint64(rng.Intn(24)) - 12
	default:
		off = uint64(rng.Intn(len(vm.input)))
	}
	return window + off
}

// One instruction at a time, the assembly loop must either leave exactly the
// state runFast leaves (registers, guest memory, dirty bits, shadow stack, pc
// and budget) or decline without touching anything. Every opcode byte is tried
// under every combination of version gates and at every text position, and
// every handler the table selects must have completed at least one instruction
// itself, so no handler passes merely by handing its work back to Go.
func TestAsmLoopSingleStepMatchesRunFast(t *testing.T) {
	trials, memTrials := 300, 4000
	if testing.Short() || raceDetector {
		trials, memTrials = 60, 800
	}
	handlers := asmHandlerTable()
	for cfgIndex, cfg := range []struct {
		ver         uint32
		disableGaps bool
	}{{0, false}, {0, true}, {1, false}, {3, false}} {
		for flagBits := 0; flagBits < 1<<7; flagBits++ {
			// The comparison is single-threaded and deterministic, so the race
			// detector has no races to find here. What it adds is checking of
			// the unsafe pointer use around the assembly call, at well over
			// ten times the run time. Under it, each version keeps one
			// setting of the four low gates (a different one per version)
			// and every setting of the rest, a sixteenth of the tables. All
			// of them run without the detector.
			if raceDetector && flagBits&15 != (cfgIndex*5+3)&15 {
				continue
			}
			f := fastFlags{
				moveMem:        flagBits&1 != 0,
				pqr:            flagBits&2 != 0,
				swapSub:        flagBits&4 != 0,
				explicitSext:   flagBits&8 != 0,
				disableLddw:    flagBits&16 != 0,
				staticSyscalls: flagBits&32 != 0,
				disableNeg:     flagBits&64 != 0,
			}
			if asmFlagsIndex(f, false) != flagBits || asmFlagsIndex(f, true) != flagBits|1<<7 {
				t.Fatalf("asmFlagsIndex(%+v) = %d and %d, want %d and %d", f, asmFlagsIndex(f, false), asmFlagsIndex(f, true), flagBits, flagBits|1<<7)
			}
			// runFast takes the call target rule from the interpreter's
			// version, and Run always derives staticSyscalls from that same
			// version, so only the consistent pairing can occur. The other
			// gates only select handlers and are tried in every combination.
			if ver := (sbpfver.SbpfVersion{Version: cfg.ver}); f.staticSyscalls != ver.EnableStaticSyscalls() {
				continue
			}
			rng := rand.New(rand.NewSource(int64(flagBits)*31 + int64(cfg.ver)))
			ref := newAsmStepVM(t, cfg.ver, cfg.disableGaps, 99)
			got := newAsmStepVM(t, cfg.ver, cfg.disableGaps, 99)
			// The table an interpreter in this configuration would get:
			// gap-handling memory handlers only where a window has gaps.
			gaps := asmRegionsHaveGaps(&got.ip.regions)
			if want := cfg.ver == 0 && !cfg.disableGaps; gaps != want {
				t.Fatalf("v%d disableGaps=%t: asmRegionsHaveGaps = %t, want %t", cfg.ver, cfg.disableGaps, gaps, want)
			}
			table := buildAsmTable(f, gaps)
			name := fmt.Sprintf("v%d gaps=%t %+v", cfg.ver, !cfg.disableGaps, f)
			text := make([]Slot, 12)
			callTable := make([]int64, len(text))
			for op := 0; op < 256; op++ {
				n, isMem := trials, false
				switch uint8(op) {
				case OpLdxb, OpLdxh, OpLdxw, OpLdxdw, OpStb, OpSth, OpStw, OpStdw, OpStxb, OpStxh, OpStxw, OpStxdw:
					n, isMem = memTrials, true
				case OpLd1BReg, OpLd2BReg, OpLd8BReg, OpSt1BImm, OpSt2BImm, OpSt8BImm, OpSt1BReg, OpSt2BReg, OpSt8BReg:
					// The relocated memory instructions, where these
					// encodings are not arithmetic.
					if f.pqr && f.moveMem {
						n, isMem = memTrials, true
					}
				case OpLd4BReg, OpSt4BReg:
					if f.moveMem {
						n, isMem = memTrials, true
					}
				case OpSt4BImm:
					if f.moveMem && f.disableNeg {
						n, isMem = memTrials, true
					}
				}
				completedInAsm := false
				for i := 0; i < n; i++ {
					dst, src := uint8(rng.Intn(16)), uint8(rng.Intn(16))
					off := int16(asmRandValue(rng))
					imm := uint32(asmRandValue(rng))
					if uint8(op) == OpCall && rng.Intn(2) == 0 {
						src, imm = 1, uint32(int32(rng.Intn(33)-16))
					}
					ins := slot(uint8(op), dst, src, off, imm)
					for k := range text {
						text[k] = Slot(rng.Uint64())
					}
					pc := int64(rng.Intn(len(text)))
					text[pc] = ins

					for k := range ref.r {
						ref.r[k] = asmRandValue(rng)
					}
					if isMem || rng.Intn(4) == 0 {
						ref.r[dst] = asmRandAddr(rng, ref) - uint64(int64(off))
						ref.r[src] = asmRandAddr(rng, ref) - uint64(int64(off))
					}
					got.r = ref.r
					ref.ip.text, got.ip.text = text, text
					// Resolved legacy call targets: mostly present, with
					// entries that are absent, valid, and out of the text in
					// both directions. The static versions never have a table.
					var calls []int64
					var asmCalls *int64
					if !f.staticSyscalls && rng.Intn(8) != 0 {
						calls, asmCalls = callTable, &callTable[0]
						for k := range calls {
							calls[k] = []int64{-1, -1, int64(rng.Intn(len(text))), int64(rng.Intn(len(text))), int64(len(text)), int64(len(text)) + 1, -2, 1 << 40, -1 << 63}[rng.Intn(9)]
						}
					}
					ref.ip.fastCalls, got.ip.fastCalls = calls, calls
					// Fresh dirty bits (accumulated ones would hide a wrong
					// update) and a random shadow stack, identical on both.
					for k := range ref.ip.regions {
						ref.ip.regions[k].dirty, got.ip.regions[k].dirty = 0, 0
					}
					rs, gs := &ref.ip.stack, &got.ip.stack
					depths := []int{1, 1, 2, 3, 7, cap(rs.shadow) - 1, cap(rs.shadow)}
					rs.shadow = rs.shadow[:cap(rs.shadow)]
					for k := range rs.shadow {
						rs.shadow[k] = Frame{FramePtr: rng.Uint64(), NVRegs: [4]uint64{rng.Uint64(), rng.Uint64(), rng.Uint64(), rng.Uint64()}, RetAddr: int64(rng.Intn(16)) - 2}
					}
					gs.shadow = gs.shadow[:cap(gs.shadow)]
					copy(gs.shadow, rs.shadow)
					depth := depths[rng.Intn(len(depths))]
					rs.shadow, gs.shadow = rs.shadow[:depth], gs.shadow[:depth]
					rs.maxDepth = rng.Intn(cap(rs.shadow) + 1)
					gs.maxDepth = rs.maxDepth
					// A call adjusts the frame pointer according to these two
					// stack settings. Real versions fix them, so vary them
					// here to reach every branch of the call handler.
					rs.dynamicStackFrames, rs.stackFrameGaps = rng.Intn(4) == 0, rng.Intn(2) == 0
					gs.dynamicStackFrames, gs.stackFrameGaps = rs.dynamicStackFrames, rs.stackFrameGaps

					near := int64(0)
					if isMem {
						near = int64((ref.r[dst] + uint64(int64(off))) & 0xffffffff)
					}
					wantPC, wantDue, wantPanic := asmCatch(func() (int64, uint64) { return ref.ip.runFast(&ref.r, pc, 0, 1, f) })

					before := got.r
					asmPC, left := runAsmLoop(&text[0], int64(len(text)), &got.r, &got.ip.regions, table, &got.ip.stack, pc, 1, asmCalls)
					if left != 0 {
						// The loop declined, so Run's general path executes
						// this instruction. Nothing may have changed, and the
						// loop may decline an instruction runFast completes
						// only for arithmetic shifts, whose unmasked counts
						// it leaves to Go.
						if asmPC != pc || left != 1 || got.r != before || len(gs.shadow) != depth {
							t.Fatalf("%s ins=%#016x: assembly loop declined but changed state (pc=%d left=%d)", name, uint64(ins), asmPC, left)
						}
						switch uint8(op) {
						case OpArsh32Imm, OpArsh32Reg, OpArsh64Imm, OpArsh64Reg:
						default:
							if wantDue != 0 {
								t.Fatalf("%s ins=%#016x at pc %d: runFast completed an instruction the assembly loop declined", name, uint64(ins), pc)
							}
						}
						continue
					}
					completedInAsm = true
					if asmPC != wantPC || wantDue != 1 || wantPanic != "" || !asmSameState(ref, got) || !asmSameMemory(ref, got, near) {
						t.Fatalf("%s ins=%#016x at pc %d:\n runFast: %s\n asm:     %s",
							name, uint64(ins), pc, ref.describe(wantPC, wantDue, wantPanic), got.describe(asmPC, 1, ""))
					}
				}
				if !asmSameMemory(ref, got, -1) {
					t.Fatalf("%s op %#x: guest memory differs from runFast", name, op)
				}
				if mapped := table[op] != handlers[asmBail]; mapped && !completedInAsm {
					t.Errorf("%s op %#x: handler never completed an instruction", name, op)
				}
			}
			ref.finish()
			got.finish()
		}
	}
}

// The loop must stop exactly when its budget is spent and when pc leaves the
// text, in either direction, without reading outside the text.
func TestAsmLoopBudgetAndBounds(t *testing.T) {
	table := buildAsmTable(fastFlags{}, true)
	vm := newAsmStepVM(t, 0, false, 1)
	defer vm.finish()
	text := []Slot{
		slot(OpAdd64Imm, 1, 0, 0, 1),
		slot(OpAdd64Imm, 1, 0, 0, 1),
		slot(OpJa, 0, 0, -3, 0), // back to pc 0
	}
	for _, tc := range []struct {
		pc           int64
		rem          uint64
		wantPC       int64
		wantExecuted uint64
	}{
		{0, 0, 0, 0},
		{0, 1, 1, 1},
		{0, 2, 2, 2},
		{0, 3, 0, 3},
		{0, 100, 1, 100},
		{3, 5, 3, 0},
		{-1, 5, -1, 0},
		{1 << 40, 5, 1 << 40, 0},
	} {
		var r [16]uint64
		pc, left := runAsmLoop(&text[0], int64(len(text)), &r, &vm.ip.regions, table, &vm.ip.stack, tc.pc, tc.rem, nil)
		// Two of every three instructions are adds to r1.
		wantR1 := tc.wantExecuted - tc.wantExecuted/3
		if pc != tc.wantPC || tc.rem-left != tc.wantExecuted || r[1] != wantR1 {
			t.Errorf("pc=%d rem=%d: got pc=%d executed=%d r1=%d, want pc=%d executed=%d r1=%d", tc.pc, tc.rem, pc, tc.rem-left, r[1], tc.wantPC, tc.wantExecuted, wantR1)
		}
	}
	// Jumps out of the text, forward and backward, stop before the next fetch.
	for _, off := range []int16{5, 32767, -2, -32768} {
		jump := []Slot{slot(OpJa, 0, 0, off, 0)}
		var r [16]uint64
		pc, left := runAsmLoop(&jump[0], 1, &r, &vm.ip.regions, table, &vm.ip.stack, 0, 10, nil)
		if pc != int64(off)+1 || left != 9 {
			t.Errorf("ja %d: got pc=%d left=%d", off, pc, left)
		}
	}
}

// The cache must hand every real version, with and without stack gaps, the
// table its own gates and windows select.
func TestAsmLoopVersionTables(t *testing.T) {
	prev := useAsmLoop
	useAsmLoop = true
	defer func() { useAsmLoop = prev }()
	for ver := uint32(0); ver <= sbpfver.SbpfVersionV3; ver++ {
		for _, disableGaps := range []bool{false, true} {
			p := mkProgram([]Slot{slot(OpExit, 0, 0, 0, 0)}, ver)
			cm := cu.NewComputeMeter(10)
			ip := NewInterpreter(p, &VMOpts{HeapMax: 4096, Syscalls: noSyscalls, ComputeMeter: &cm, DisableStackFrameGaps: disableGaps})
			gaps := asmRegionsHaveGaps(&ip.regions)
			if want := ver == 0 && !disableGaps; gaps != want {
				t.Fatalf("v%d disableGaps=%t: asmRegionsHaveGaps = %t, want %t", ver, disableGaps, gaps, want)
			}
			want := buildAsmTable(ip.fastFlags(), gaps)
			if noGap := buildAsmTable(ip.fastFlags(), false); gaps && *want == *noGap {
				t.Fatalf("v%d: gap and gap-free tables are identical", ver)
			}
			for range 2 { // slow path, then the cached one
				if got := ip.asmLoopTable(ip.fastFlags()); got == nil || *got != *want {
					t.Fatalf("v%d disableGaps=%t: cached table differs from the one its gates select", ver, disableGaps)
				}
			}
			ip.Finish()
		}
	}
}
