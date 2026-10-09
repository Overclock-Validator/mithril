package sbpf

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
)

// Differential test: generates deterministic pseudo-random programs and dumps
// (return value, error string, CU consumed, memory hash) per program to the
// file named by SBPF_DIFF_OUT. Running it against the baseline and the
// optimized interpreter and diffing the two files checks that observable
// behaviour is identical.

type diffSyscall struct {
	fn func(vm VM, r1, r2, r3, r4, r5 uint64) (uint64, error)
}

func (s diffSyscall) Invoke(vm VM, r1, r2, r3, r4, r5 uint64) (uint64, error) {
	return s.fn(vm, r1, r2, r3, r4, r5)
}

var (
	hashPoke   = SymbolHash("poke")   // write r3 bytes of value r2 at r1 via vm.Write
	hashPeek   = SymbolHash("peek")   // read 8 bytes at r1 -> r0
	hashCopy   = SymbolHash("copy")   // copy r3 bytes from r2 to r1 (Translate based)
	hashBurn   = SymbolHash("burn")   // consume r1 CU
	hashSetLen = SymbolHash("setlen") // SetInputRegionLength(r1, r2, r3!=0)
)

func diffRegistry(h uint32) (Syscall, bool) {
	switch h {
	case hashPoke:
		return diffSyscall{func(vm VM, r1, r2, r3, _, _ uint64) (uint64, error) {
			if err := vm.ComputeMeter().Consume(10); err != nil {
				return 0, err
			}
			if r3 > 4096 {
				r3 = 4096
			}
			buf := make([]byte, r3)
			for i := range buf {
				buf[i] = byte(r2 + uint64(i))
			}
			return 0, vm.Write(r1, buf)
		}}, true
	case hashPeek:
		return diffSyscall{func(vm VM, r1, _, _, _, _ uint64) (uint64, error) {
			if err := vm.ComputeMeter().Consume(10); err != nil {
				return 0, err
			}
			return vm.Read64(r1)
		}}, true
	case hashCopy:
		return diffSyscall{func(vm VM, r1, r2, r3, _, _ uint64) (uint64, error) {
			if err := vm.ComputeMeter().Consume(10); err != nil {
				return 0, err
			}
			if r3 > 4096 {
				r3 = 4096
			}
			src, err := vm.Translate(r2, r3, false)
			if err != nil {
				return 0, err
			}
			dst, err := vm.Translate(r1, r3, true)
			if err != nil {
				return 0, err
			}
			copy(dst, src)
			return 0, nil
		}}, true
	case hashBurn:
		return diffSyscall{func(vm VM, r1, _, _, _, _ uint64) (uint64, error) {
			return 0, vm.ComputeMeter().Consume(r1 & 0xff)
		}}, true
	case hashSetLen:
		return diffSyscall{func(vm VM, r1, r2, r3, _, _ uint64) (uint64, error) {
			if err := vm.ComputeMeter().Consume(10); err != nil {
				return 0, err
			}
			ip := vm.(*Interpreter)
			ok := ip.SetInputRegionLength(r1, r2, r3 != 0)
			if ok {
				return 1, nil
			}
			return 0, nil
		}}, true
	}
	return nil, false
}

// v2 replaces LDDW with MOV32 + HOR64. MOV32 avoids sign-extending the
// low word before ORing in the high word.
func diffLoadImm64(dst uint8, value uint64, ver uint32) []Slot {
	if ver == sbpfver.SbpfVersionV2 {
		return []Slot{slot(OpMov32Imm, dst, 0, 0, uint32(value)), slot(OpHor64Imm, dst, 0, 0, uint32(value>>32))}
	}
	return []Slot{slot(OpLddw, dst, 0, 0, uint32(value)), slot(0, 0, 0, 0, uint32(value>>32))}
}

func randSlot(rng *rand.Rand, pc, n int, ver uint32, fnPC int64) []Slot {
	reg := func() uint8 { return uint8(1 + rng.Intn(9)) } // r1..r9
	imm := func() uint32 {
		switch rng.Intn(4) {
		case 0:
			return uint32(rng.Intn(16))
		case 1:
			return uint32(int32(-rng.Intn(16)))
		case 2:
			return rng.Uint32()
		default:
			return uint32(rng.Intn(4096))
		}
	}
	alu64 := []uint8{OpAdd64Imm, OpAdd64Reg, OpSub64Imm, OpSub64Reg, OpMul64Imm, OpMul64Reg, OpDiv64Imm, OpDiv64Reg,
		OpOr64Imm, OpOr64Reg, OpAnd64Imm, OpAnd64Reg, OpLsh64Imm, OpLsh64Reg, OpRsh64Imm, OpRsh64Reg, OpMod64Imm, OpMod64Reg,
		OpXor64Imm, OpXor64Reg, OpMov64Imm, OpMov64Reg, OpArsh64Imm, OpArsh64Reg, OpNeg64,
		OpAdd32Imm, OpAdd32Reg, OpSub32Imm, OpSub32Reg, OpMul32Imm, OpMul32Reg, OpDiv32Imm, OpDiv32Reg, OpOr32Imm, OpOr32Reg,
		OpAnd32Imm, OpAnd32Reg, OpLsh32Imm, OpLsh32Reg, OpRsh32Imm, OpRsh32Reg, OpMod32Imm, OpMod32Reg, OpXor32Imm, OpXor32Reg,
		OpMov32Imm, OpMov32Reg, OpArsh32Imm, OpArsh32Reg, OpNeg32, OpLe, OpBe}
	if ver == sbpfver.SbpfVersionV2 {
		// Arithmetic and memory encodings both change in v2. Keep generated ALU
		// operations arithmetic rather than generating unintended memory accesses.
		replacements := map[uint8]uint8{
			OpMul32Imm: OpLmul32Imm, OpMul32Reg: OpLmul32Reg,
			OpMul64Imm: OpLmul64Imm, OpMul64Reg: OpLmul64Reg,
			OpDiv32Imm: OpUdiv32Imm, OpDiv32Reg: OpUdiv32Reg,
			OpDiv64Imm: OpUdiv64Imm, OpDiv64Reg: OpUdiv64Reg,
			OpMod32Imm: OpUrem32Imm, OpMod32Reg: OpUrem32Reg,
			OpMod64Imm: OpUrem64Imm, OpMod64Reg: OpUrem64Reg,
		}
		filtered := alu64[:0]
		for _, op := range alu64 {
			if op == OpNeg32 || op == OpNeg64 || op == OpLe {
				continue
			}
			if replacement, ok := replacements[op]; ok {
				op = replacement
			}
			filtered = append(filtered, op)
		}
		alu64 = filtered
	}
	jmp := []uint8{OpJeqImm, OpJeqReg, OpJgtImm, OpJgtReg, OpJgeImm, OpJgeReg, OpJltImm, OpJltReg, OpJleImm, OpJleReg,
		OpJsetImm, OpJsetReg, OpJneImm, OpJneReg, OpJsgtImm, OpJsgtReg, OpJsgeImm, OpJsgeReg, OpJsltImm, OpJsltReg, OpJsleImm, OpJsleReg}
	switch rng.Intn(10) {
	case 0, 1, 2, 3: // alu
		op := alu64[rng.Intn(len(alu64))]
		i := imm()
		if op == OpLe || op == OpBe {
			i = []uint32{16, 32, 64}[rng.Intn(3)]
		}
		if (op == OpDiv64Imm || op == OpMod64Imm || op == OpDiv32Imm || op == OpMod32Imm || op == OpUdiv32Imm || op == OpUdiv64Imm || op == OpUrem32Imm || op == OpUrem64Imm) && i == 0 {
			i = 3
		}
		switch op {
		case OpLsh32Imm, OpRsh32Imm, OpArsh32Imm:
			i = uint32(rng.Intn(32))
		case OpLsh64Imm, OpRsh64Imm, OpArsh64Imm:
			i = uint32(rng.Intn(64))
		}
		return []Slot{slot(op, reg(), reg(), 0, i)}
	case 4: // load
		ops := []uint8{OpLdxb, OpLdxh, OpLdxw, OpLdxdw}
		if ver == sbpfver.SbpfVersionV2 {
			ops = []uint8{OpLd1BReg, OpLd2BReg, OpLd4BReg, OpLd8BReg}
		}
		// base register: r10 (stack) or r5 (heap ptr) or r1 (input ptr) or random
		var base uint8
		var off int16
		switch rng.Intn(4) {
		case 0:
			base, off = 10, int16(-rng.Intn(4096))
		case 1:
			base, off = 5, int16(rng.Intn(1024))
		case 2:
			base, off = 1, int16(rng.Intn(600))
		default:
			base, off = reg(), int16(rng.Intn(65536)-32768)
		}
		return []Slot{slot(ops[rng.Intn(4)], reg(), base, off, 0)}
	case 5: // store
		ops := []uint8{OpStb, OpSth, OpStw, OpStdw, OpStxb, OpStxh, OpStxw, OpStxdw}
		if ver == sbpfver.SbpfVersionV2 {
			ops = []uint8{OpSt1BImm, OpSt2BImm, OpSt4BImm, OpSt8BImm, OpSt1BReg, OpSt2BReg, OpSt4BReg, OpSt8BReg}
		}
		var base uint8
		var off int16
		switch rng.Intn(5) {
		case 0, 1:
			base, off = 10, int16(-rng.Intn(4096))
		case 2:
			base, off = 5, int16(rng.Intn(1024))
		case 3:
			base, off = 1, int16(rng.Intn(600))
		default:
			base, off = reg(), int16(rng.Intn(65536)-32768)
		}
		return []Slot{slot(ops[rng.Intn(8)], base, reg(), off, imm())}
	case 6: // forward conditional jump (never backwards: guarantees termination)
		maxOff := n - pc - 2
		if maxOff <= 0 {
			return []Slot{slot(OpMov64Imm, reg(), 0, 0, imm())}
		}
		return []Slot{slot(jmp[rng.Intn(len(jmp))], reg(), reg(), int16(rng.Intn(min(maxOff, 8))), imm())}
	case 7: // syscall
		hs := []uint32{hashPoke, hashPeek, hashCopy, hashBurn, hashSetLen}
		return []Slot{slot(OpCall, 0, 0, 0, hs[rng.Intn(len(hs))])}
	case 8: // internal call
		if ver >= sbpfver.SbpfVersionV3 {
			return []Slot{slot(OpCall, 0, 1, 0, uint32(fnPC-int64(pc)-1))}
		}
		return []Slot{slot(OpCall, 0, 0, 0, PCHash(uint64(fnPC)))}
	default: // set up pointer registers
		switch rng.Intn(3) {
		case 0: // r5 = heap
			return diffLoadImm64(5, VaddrHeap, ver)
		case 1: // r1 = input + small
			return diffLoadImm64(1, VaddrInput+uint64(rng.Intn(64)), ver)
		default: // r9 = random 64-bit
			return diffLoadImm64(9, uint64(rng.Uint32())|uint64(rng.Intn(6))<<32, ver)
		}
	}
}

func genProgram(rng *rand.Rand, ver uint32) *Program {
	n := 8 + rng.Intn(120)
	// layout: [0, n) main body then exit; fn at fnPC: a few ALU ops + exit
	body := make([]Slot, 0, n+16)
	fnPC := int64(n + 1)
	for len(body) < n {
		body = append(body, randSlot(rng, len(body), n, ver, fnPC)...)
	}
	if len(body) > n {
		body = body[:n-1] // drop a cut lddw pair
	}
	body = append(body, slot(OpExit, 0, 0, 0, 0))
	// fix up forward jumps that would land on the second slot of an lddw
	for pc := range body {
		if body[pc].Op()&0x07 == ClassJmp && body[pc].Op() != OpCall && body[pc].Op() != OpExit {
			dst := pc + int(body[pc].Off()) + 1
			if dst < len(body) && body[dst].Op() == 0 {
				body[pc] = body[pc]&^(Slot(0xffff)<<16) | Slot(uint16(body[pc].Off()+1))<<16
			}
		}
	}
	// function
	store := uint8(OpStxdw)
	if ver == sbpfver.SbpfVersionV2 {
		store = OpSt8BReg
	}
	body = append(body,
		slot(OpAdd64Imm, 6, 0, 0, uint32(rng.Intn(100))),
		slot(OpXor64Reg, 7, 6, 0, 0),
		slot(store, 10, 7, int16(-8-rng.Intn(64)), 0),
		slot(OpExit, 0, 0, 0, 0))
	p := mkProgram(body, ver)
	if ver < sbpfver.SbpfVersionV3 {
		p.Funcs[PCHash(uint64(fnPC))] = fnPC
	}
	p.RO = make([]byte, 256)
	for i := range p.RO {
		p.RO[i] = byte(i * 7)
	}
	return p
}

// Keep this check in the ordinary suite: merely selecting v2 is insufficient
// if its programs still contain legacy memory opcodes or LDDW and never run.
func TestDifferentialV2Generator(t *testing.T) {
	rng := rand.New(rand.NewSource(12345))
	seen := make(map[uint8]bool)
	for i := 0; i < 1000; i++ {
		p := genProgram(rng, sbpfver.SbpfVersionV2)
		if err := p.Verify(); err != nil {
			continue
		}
		for _, ins := range p.Text {
			seen[ins.Op()] = true
		}
	}
	for _, op := range []uint8{OpLd1BReg, OpLd2BReg, OpLd4BReg, OpLd8BReg,
		OpSt1BImm, OpSt2BImm, OpSt4BImm, OpSt8BImm,
		OpSt1BReg, OpSt2BReg, OpSt4BReg, OpSt8BReg} {
		if !seen[op] {
			t.Errorf("no verifier-accepted v2 program contains opcode %#x", op)
		}
	}
}

func memHash(bs ...[]byte) uint64 {
	h := fnv.New64a()
	for _, b := range bs {
		h.Write(b)
	}
	return h.Sum64()
}

func TestDifferentialDump(t *testing.T) {
	out := os.Getenv("SBPF_DIFF_OUT")
	if out == "" {
		t.Skip("SBPF_DIFF_OUT not set")
	}
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	writeDifferentialDump(t, w, 100000)
}

func writeDifferentialDump(t *testing.T, w io.Writer, n int) {
	rng := rand.New(rand.NewSource(12345))
	generated, verified := 0, 0
	var verifiedByVersion [4]int
	for i := 0; i < n; i++ {
		// Equal representation of every version, independent of RNG consumption.
		ver := uint32(i % 4)
		p := genProgram(rng, ver)
		generated++
		if err := p.Verify(); err != nil {
			fmt.Fprintf(w, "%d ver=%d VERIFY_FAIL %v\n", i, ver, err)
			continue
		}
		verified++
		verifiedByVersion[ver]++
		resolveCallTargetsIfSupported(p)
		input := make([]byte, 700)
		for j := range input {
			input[j] = byte(j)
		}
		var regions []InputRegion
		useRegions := rng.Intn(2) == 0
		if useRegions {
			regions = []InputRegion{
				{Offset: 0, HostOffset: 0, RegionSize: 100, AddressSpaceReserved: 100, Writable: true, AccountIndex: -1},
				{Offset: 100, HostOffset: 100, RegionSize: 150, AddressSpaceReserved: 300, Writable: rng.Intn(2) == 0, AccountIndex: 0},
				{Offset: 400, HostOffset: 400, RegionSize: 300, AddressSpaceReserved: 300, Writable: true, AccountIndex: -1},
			}
		}
		budget := uint64(1 + rng.Intn(400))
		if rng.Intn(4) == 0 {
			budget = 100000
		}
		cm := cu.NewComputeMeter(budget)
		heapMax := 4096 * (1 + rng.Intn(4))
		ip := NewInterpreter(p, &VMOpts{
			HeapMax:               heapMax,
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
		errStr := "<nil>"
		if runErr != nil {
			errStr = runErr.Error()
		}
		h := memHash(ip.stack.mem, ip.heap, input)
		regionSizes := ""
		for _, r := range ip.inputRegions {
			regionSizes += fmt.Sprintf("%d/%v,", r.RegionSize, r.Writable)
		}
		fmt.Fprintf(w, "%d ver=%d budget=%d ret=%d cu=%d remaining=%d err=%q mem=%x regions=%s\n",
			i, ver, budget, ret, cuUsed, cm.Remaining(), errStr, h, regionSizes)
		ip.Finish()
		if os.Getenv("SBPF_CHECK_POOL_ZERO") != "" {
			// The buffers just returned to the pool must be all-zero.
			st := stackMemPool.Get().([]byte)
			hp := heapPool.Get().([]byte)
			for j, b := range st[:StackMax] {
				if b != 0 {
					t.Fatalf("program %d: pooled stack not zeroed at %d", i, j)
				}
			}
			for j, b := range hp[:cap(hp)] {
				if b != 0 {
					t.Fatalf("program %d: pooled heap not zeroed at %d", i, j)
				}
			}
			stackMemPool.Put(st)
			heapPool.Put(hp)
		}
	}
	for ver, count := range verifiedByVersion {
		if count == 0 {
			t.Errorf("no verifier-accepted programs for v%d", ver)
		}
	}
	t.Logf("generated=%d verified=%d verified_by_version=%v", generated, verified, verifiedByVersion)
}

// Golden derived from the pre-optimization e1204b32 interpreter with this
// generator (seed 12345). Do not regenerate from candidate output alone.
// Covers 1024 programs per version; dumps compare results, errors, CU and memory.
func TestDifferentialGolden(t *testing.T) {
	h := sha256.New()
	writeDifferentialDump(t, h, 4096)
	const want = "ccc16b4021e784342a7200a3d8911d362d2ab11a5ddefccf1c93880a704426b0"
	if got := fmt.Sprintf("%x", h.Sum(nil)); got != want {
		t.Fatalf("differential drift: got %s, want %s", got, want)
	}
}
