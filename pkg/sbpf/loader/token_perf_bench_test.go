package loader_test

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/loader"
)

// ---- minimal syscall set mirroring pkg/sealevel semantics (CU costs + memory behaviour) ----

const (
	cuSyscallBase   = 100
	cuMemOpBase     = 10
	cuCpiBytesPerCU = 250
)

type stats struct {
	logs    int
	memcpy  int
	memset  int
	memcmp  int
	memmove int
	bytes   uint64
}

var st stats

func memOpConsume(vm sbpf.VM, n uint64) error {
	cost := max(uint64(cuMemOpBase), n/cuCpiBytesPerCU)
	return vm.ComputeMeter().Consume(cost)
}

func memmoveImplInternal(vm sbpf.VM, dst, src, n uint64) (err error) {
	srcBuf := make([]byte, n) // same allocation pattern as sealevel/syscalls_mem.go
	err = vm.Read(src, srcBuf)
	if err != nil {
		return
	}
	err = vm.Write(dst, srcBuf)
	return
}

func isNonOverlapping(src, srcLen, dst, dstLen uint64) bool {
	if src > dst {
		return src-dst >= dstLen
	}
	return dst-src >= srcLen
}

var syscallMemcpy = sbpf.SyscallFunc3(func(vm sbpf.VM, dst, src, n uint64) (uint64, error) {
	st.memcpy++
	st.bytes += n
	if err := memOpConsume(vm, n); err != nil {
		return 0, err
	}
	if !isNonOverlapping(src, n, dst, n) {
		return 0, errors.New("overlapping")
	}
	if n == 0 {
		return 0, nil
	}
	return 0, memmoveImplInternal(vm, dst, src, n)
})

var syscallMemmove = sbpf.SyscallFunc3(func(vm sbpf.VM, dst, src, n uint64) (uint64, error) {
	st.memmove++
	st.bytes += n
	if err := memOpConsume(vm, n); err != nil {
		return 0, err
	}
	return 0, memmoveImplInternal(vm, dst, src, n)
})

var syscallMemcmp = sbpf.SyscallFunc4(func(vm sbpf.VM, a1, a2, n, res uint64) (uint64, error) {
	st.memcmp++
	st.bytes += n
	if err := memOpConsume(vm, n); err != nil {
		return 0, err
	}
	s1, err := vm.Translate(a1, n, false)
	if err != nil {
		return 0, err
	}
	s2, err := vm.Translate(a2, n, false)
	if err != nil {
		return 0, err
	}
	r := int32(0)
	for i := uint64(0); i < n; i++ {
		if s1[i] != s2[i] {
			r = int32(s1[i]) - int32(s2[i])
			break
		}
	}
	out, err := vm.Translate(res, 4, true)
	if err != nil {
		return 0, err
	}
	binary.LittleEndian.PutUint32(out, uint32(r))
	return 0, nil
})

var syscallMemset = sbpf.SyscallFunc3(func(vm sbpf.VM, dst, c, n uint64) (uint64, error) {
	st.memset++
	st.bytes += n
	if err := memOpConsume(vm, n); err != nil {
		return 0, err
	}
	mem, err := vm.Translate(dst, n, true)
	if err != nil {
		return 0, err
	}
	for i := uint64(0); i < n; i++ {
		mem[i] = byte(c)
	}
	return 0, nil
})

var syscallLog = sbpf.SyscallFunc2(func(vm sbpf.VM, ptr, strlen uint64) (uint64, error) {
	st.logs++
	if err := vm.ComputeMeter().Consume(max(uint64(cuSyscallBase), strlen)); err != nil {
		return 0, err
	}
	buf := make([]byte, strlen)
	if err := vm.Read(ptr, buf); err != nil {
		return 0, err
	}
	_ = string(buf)
	return 0, nil
})

var syscallLog64 = sbpf.SyscallFunc5(func(vm sbpf.VM, a, b, c, d, e uint64) (uint64, error) {
	st.logs++
	return 0, vm.ComputeMeter().Consume(100)
})
var syscallLogPubkey = sbpf.SyscallFunc1(func(vm sbpf.VM, a uint64) (uint64, error) {
	st.logs++
	return 0, vm.ComputeMeter().Consume(100)
})
var syscallLogCUs = sbpf.SyscallFunc0(func(vm sbpf.VM) (uint64, error) {
	return 0, vm.ComputeMeter().Consume(100)
})
var syscallAbort = sbpf.SyscallFunc0(func(vm sbpf.VM) (uint64, error) { return 0, errors.New("abort") })
var syscallPanic = sbpf.SyscallFunc4(func(vm sbpf.VM, f, l, line, col uint64) (uint64, error) {
	return 0, errors.New("panic")
})
var syscallAllocFree = sbpf.SyscallFunc2(func(vm sbpf.VM, size, free uint64) (uint64, error) {
	if free != 0 {
		return 0, nil
	}
	hs := (vm.HeapSize() + 7) &^ 7
	addr := sbpf.VaddrHeap + hs
	hs += size
	if hs > vm.HeapMax() {
		return 0, nil
	}
	vm.UpdateHeapSize(hs)
	return addr, nil
})

var registry = map[uint32]sbpf.Syscall{
	sbpf.SymbolHash("abort"):                  syscallAbort,
	sbpf.SymbolHash("sol_panic_"):             syscallPanic,
	sbpf.SymbolHash("sol_log_"):               syscallLog,
	sbpf.SymbolHash("sol_log_64_"):            syscallLog64,
	sbpf.SymbolHash("sol_log_pubkey"):         syscallLogPubkey,
	sbpf.SymbolHash("sol_log_compute_units_"): syscallLogCUs,
	sbpf.SymbolHash("sol_memcpy_"):            syscallMemcpy,
	sbpf.SymbolHash("sol_memmove_"):           syscallMemmove,
	sbpf.SymbolHash("sol_memcmp_"):            syscallMemcmp,
	sbpf.SymbolHash("sol_memset_"):            syscallMemset,
	sbpf.SymbolHash("sol_alloc_free_"):        syscallAllocFree,
}

var syscalls = sbpf.SyscallRegistry(func(h uint32) (sbpf.Syscall, bool) {
	s, ok := registry[h]
	return s, ok
})

// ---- aligned input serialization (BPF loader v2/v3 format, no direct mapping) ----

const maxPermittedDataIncrease = 10 * 1024

type acct struct {
	key, owner       [32]byte
	lamports         uint64
	data             []byte
	signer, writable bool
}

func serializeAligned(accts []acct, instrData []byte, programId [32]byte) []byte {
	out := binary.LittleEndian.AppendUint64(nil, uint64(len(accts)))
	for _, a := range accts {
		out = append(out, 0xff)
		out = append(out, b2u8(a.signer), b2u8(a.writable), 0)
		out = append(out, 0, 0, 0, 0) // original_data_len
		out = append(out, a.key[:]...)
		out = append(out, a.owner[:]...)
		out = binary.LittleEndian.AppendUint64(out, a.lamports)
		out = binary.LittleEndian.AppendUint64(out, uint64(len(a.data)))
		out = append(out, a.data...)
		pad := maxPermittedDataIncrease + ((8 - len(a.data)%8) % 8)
		out = append(out, make([]byte, pad)...)
		out = binary.LittleEndian.AppendUint64(out, ^uint64(0)) // rent epoch
	}
	out = binary.LittleEndian.AppendUint64(out, uint64(len(instrData)))
	out = append(out, instrData...)
	out = append(out, programId[:]...)
	return out
}

func b2u8(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// SPL token account layout (165 bytes)
func tokenAccount(mint, owner [32]byte, amount uint64) []byte {
	d := make([]byte, 165)
	copy(d[0:32], mint[:])
	copy(d[32:64], owner[:])
	binary.LittleEndian.PutUint64(d[64:72], amount)
	// delegate: COption none (4 bytes 0) + 32
	d[108] = 1 // state = Initialized
	// is_native COption none, delegated_amount 0, close_authority none
	return d
}

func key(b byte) [32]byte {
	var k [32]byte
	for i := range k {
		k[i] = b
	}
	return k
}

func loadTokenProgram(tb testing.TB) *sbpf.Program {
	elfBytes := fixtures.Load(tb, "sbpf", "spl-token.so")
	f := features.NewFeaturesDefault()
	l, err := loader.NewLoaderWithSyscalls(elfBytes, syscalls, false, f)
	if err != nil {
		tb.Fatal(err)
	}
	p, err := l.Load()
	if err != nil {
		tb.Fatal(err)
	}
	if err := p.Verify(); err != nil {
		tb.Fatal(err)
	}
	return p
}

func transferInput(programId [32]byte) ([]byte, []acct) {
	mint := key(0x11)
	authority := key(0x22)
	src := key(0x33)
	dst := key(0x44)
	accts := []acct{
		{key: src, owner: programId, lamports: 2039280, data: tokenAccount(mint, authority, 1_000_000), writable: true},
		{key: dst, owner: programId, lamports: 2039280, data: tokenAccount(mint, key(0x55), 5), writable: true},
		{key: authority, owner: key(0), lamports: 1_000_000_000, data: nil, signer: true},
	}
	instr := append([]byte{3}, binary.LittleEndian.AppendUint64(nil, 1000)...)
	return serializeAligned(accts, instr, programId), accts
}

func runTransfer(tb testing.TB, p *sbpf.Program, input []byte) (uint64, uint64) {
	cm := cu.NewComputeMeter(200_000)
	ip := sbpf.NewInterpreter(p, &sbpf.VMOpts{
		HeapMax:      32 * 1024,
		Syscalls:     syscalls,
		ComputeMeter: &cm,
		Input:        input,
	})
	ret, used, err := ip.Run()
	ip.Finish()
	if err != nil {
		tb.Fatal(err)
	}
	return ret, used
}

func TestTokenTransfer(t *testing.T) {
	p := loadTokenProgram(t)
	programId := key(0x99)
	input, _ := transferInput(programId)
	st = stats{}
	ret, used := runTransfer(t, p, input)
	t.Logf("ret=%d cuUsed=%d stats=%+v", ret, used, st)
	// verify balances changed in the serialized input
	// account 0 data starts at 8 + 8 + 32+32+8+8 = 96
	srcAmt := binary.LittleEndian.Uint64(input[96+64:])
	off1 := 8 + (8 + 32 + 32 + 8 + 8 + 165 + maxPermittedDataIncrease + 3 + 8)
	dstAmt := binary.LittleEndian.Uint64(input[off1+88+64:])
	t.Logf("src=%d dst=%d", srcAmt, dstAmt)
	if ret != 0 || srcAmt != 999_000 || dstAmt != 1005 {
		t.Fatalf("unexpected result ret=%d src=%d dst=%d", ret, srcAmt, dstAmt)
	}
}

func BenchmarkTokenTransfer(b *testing.B) {
	p := loadTokenProgram(b)
	programId := key(0x99)
	input, _ := transferInput(programId)
	orig := append([]byte(nil), input...)
	b.ReportAllocs()
	b.ResetTimer()
	var used uint64
	for i := 0; i < b.N; i++ {
		copy(input, orig)
		_, used = runTransfer(b, p, input)
	}
	b.StopTimer()
	b.ReportMetric(float64(used), "cu/op")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(used), "ns/cu")
}

func BenchmarkTokenLoadVerify(b *testing.B) {
	elfBytes := fixtures.Load(b, "sbpf", "spl-token.so")
	f := features.NewFeaturesDefault()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l, err := loader.NewLoaderWithSyscalls(elfBytes, syscalls, false, f)
		if err != nil {
			b.Fatal(err)
		}
		p, err := l.Load()
		if err != nil {
			b.Fatal(err)
		}
		if err := p.Verify(); err != nil {
			b.Fatal(err)
		}
	}
}

// ---- VASA layout (VirtualAddressSpaceAdjustments active, direct mapping off) ----
// Mirrors serializeParametersAligned with vasa=true, directMapping=false:
// same bytes as the aligned layout, but the input window is split into
// metadata regions and per-account data regions.

func vasaRegions(accts []acct, instrLen int) []sbpf.InputRegion {
	var regions []sbpf.InputRegion
	var regionStart, hostRegionStart uint64
	vmOff := uint64(8)
	for i, a := range accts {
		l := vmOff // host offset == vm offset in this layout
		dataLen := uint64(len(a.data))
		align := (8 - dataLen%8) % 8
		reserved := dataLen + maxPermittedDataIncrease
		dataStart := vmOff + 88
		if dataStart > regionStart {
			regions = append(regions, sbpf.InputRegion{Offset: regionStart, HostOffset: hostRegionStart,
				RegionSize: dataStart - regionStart, AddressSpaceReserved: dataStart - regionStart, Writable: true, AccountIndex: -1})
		}
		regions = append(regions, sbpf.InputRegion{Offset: dataStart, HostOffset: l + 88, RegionSize: dataLen,
			AddressSpaceReserved: reserved, Writable: a.writable, AccountIndex: i})
		hostRegionStart = l + 88 + reserved
		regionStart = dataStart + reserved
		vmOff += 88 + reserved + align + 8
	}
	end := vmOff + 8 + uint64(instrLen) + 32
	regions = append(regions, sbpf.InputRegion{Offset: regionStart, HostOffset: hostRegionStart,
		RegionSize: end - regionStart, AddressSpaceReserved: end - regionStart, Writable: true, AccountIndex: -1})
	return regions
}

func runTransferVasa(tb testing.TB, p *sbpf.Program, input []byte, regions []sbpf.InputRegion) (uint64, uint64) {
	cm := cu.NewComputeMeter(200_000)
	// regions are mutated by the VM (RegionSize on growth), so copy per run
	rc := append([]sbpf.InputRegion(nil), regions...)
	ip := sbpf.NewInterpreter(p, &sbpf.VMOpts{
		HeapMax:               32 * 1024,
		Syscalls:              syscalls,
		ComputeMeter:          &cm,
		Input:                 input,
		InputRegions:          rc,
		DisableStackFrameGaps: true,
	})
	ret, used, err := ip.Run()
	ip.Finish()
	if err != nil {
		tb.Fatal(err)
	}
	return ret, used
}

func TestTokenTransferVasa(t *testing.T) {
	p := loadTokenProgram(t)
	programId := key(0x99)
	input, accts := transferInput(programId)
	regions := vasaRegions(accts, 9)
	if regions[len(regions)-1].Offset+regions[len(regions)-1].RegionSize != uint64(len(input)) {
		t.Fatalf("region layout mismatch: %d vs %d", regions[len(regions)-1].Offset+regions[len(regions)-1].RegionSize, len(input))
	}
	ret, used := runTransferVasa(t, p, input, regions)
	srcAmt := binary.LittleEndian.Uint64(input[96+64:])
	if ret != 0 || srcAmt != 999_000 {
		t.Fatalf("unexpected ret=%d src=%d", ret, srcAmt)
	}
	t.Logf("ret=%d cu=%d regions=%d", ret, used, len(regions))
}

func BenchmarkTokenTransferVasa(b *testing.B) {
	p := loadTokenProgram(b)
	programId := key(0x99)
	input, accts := transferInput(programId)
	regions := vasaRegions(accts, 9)
	orig := append([]byte(nil), input...)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(input, orig)
		runTransferVasa(b, p, input, regions)
	}
}
