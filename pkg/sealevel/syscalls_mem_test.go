package sealevel

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	feat "github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/stretchr/testify/require"
)

// Unit tests for the rewritten memory syscalls (sol_memcpy_/memmove_/memset_/
// memcmp_). These are the primary correctness gate for the rewrite: they cover
// copy correctness across regions, memmove overlap semantics, Agave-matching
// dst-first fault ordering, input-region copy-on-write, and the memset/memcmp
// fast paths.

func newMemTestVM(t *testing.T, input []byte, regions []sbpf.InputRegion) sbpf.VM {
	t.Helper()
	features := feat.NewFeaturesDefault()
	execCtx := &ExecutionCtx{Features: *features}
	execCtx.ComputeMeter = cu.NewComputeMeter(1 << 40)
	vm := sbpf.NewInterpreter(
		&sbpf.Program{TextVA: sbpf.VaddrProgram, Funcs: map[uint32]int64{}},
		&sbpf.VMOpts{
			HeapMax:      256 * 1024,
			Input:        input,
			InputRegions: regions,
			Context:      execCtx,
			ComputeMeter: &execCtx.ComputeMeter,
		},
	)
	t.Cleanup(vm.Finish)
	return vm
}

func vmWrite(t *testing.T, vm sbpf.VM, addr uint64, data []byte) {
	t.Helper()
	m, err := vm.Translate(addr, uint64(len(data)), true)
	require.NoError(t, err)
	copy(m, data)
}

func vmRead(t *testing.T, vm sbpf.VM, addr, n uint64) []byte {
	t.Helper()
	m, err := vm.Translate(addr, n, false)
	require.NoError(t, err)
	out := make([]byte, n)
	copy(out, m)
	return out
}

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + 1)
	}
	return b
}

var memTestSizes = []uint64{0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 255, 4095, 4096, 65536}

// --- memcpy / memmove correctness ---

func TestMemcpyHeapToHeap(t *testing.T) {
	vm := newMemTestVM(t, nil, nil)
	const dst = sbpf.VaddrHeap + 0
	const src = sbpf.VaddrHeap + 0x10000 // non-overlapping for the sizes tested
	for _, n := range memTestSizes {
		p := pattern(int(n))
		vmWrite(t, vm, src, p)
		// dirty dst with a different value so we can detect the copy.
		vmWrite(t, vm, dst, bytes.Repeat([]byte{0xEE}, int(n)))
		ret, err := SyscallMemcpyImpl(vm, dst, src, n)
		require.NoError(t, err, "n=%d", n)
		require.Equal(t, uint64(0), ret)
		require.Equal(t, p, vmRead(t, vm, dst, n), "n=%d", n)
	}
}

func TestMemcpyInputToHeap(t *testing.T) {
	input := pattern(4096)
	vm := newMemTestVM(t, input, nil)
	const dst = sbpf.VaddrHeap + 0x1000
	n := uint64(1000)
	ret, err := SyscallMemcpyImpl(vm, dst, sbpf.VaddrInput, n)
	require.NoError(t, err)
	require.Equal(t, uint64(0), ret)
	require.Equal(t, input[:n], vmRead(t, vm, dst, n))
}

func TestMemcpyOverlapRejected(t *testing.T) {
	vm := newMemTestVM(t, nil, nil)
	// dst and src overlap in heap -> memcpy must reject.
	_, err := SyscallMemcpyImpl(vm, sbpf.VaddrHeap+0, sbpf.VaddrHeap+50, 100)
	require.ErrorIs(t, err, SyscallErrCopyOverlapping)
}

// TestMemmoveOverlap checks memmove produces byte-identical results to a
// reference memmove (Go copy) for overlapping forward and backward moves.
func TestMemmoveOverlap(t *testing.T) {
	cases := []struct {
		name           string
		dstOff, srcOff uint64
		n              uint64
	}{
		{"forward_overlap", 8, 0, 64},  // dst > src
		{"backward_overlap", 0, 8, 64}, // dst < src
		{"full_overlap", 0, 0, 64},     // dst == src (no-op)
		{"adjacent", 64, 0, 64},        // touching, non-overlapping
	}
	const base = sbpf.VaddrHeap + 0x2000
	region := 256
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vm := newMemTestVM(t, nil, nil)
			seed := pattern(region)
			vmWrite(t, vm, base, seed)

			// Reference: apply the same memmove to a plain copy.
			ref := make([]byte, region)
			copy(ref, seed)
			copy(ref[c.dstOff:c.dstOff+c.n], ref[c.srcOff:c.srcOff+c.n])

			_, err := SyscallMemmoveImpl(vm, base+c.dstOff, base+c.srcOff, c.n)
			require.NoError(t, err)
			require.Equal(t, ref, vmRead(t, vm, base, uint64(region)))
		})
	}
}

// --- fault ordering (Agave: dst translated first) ---

func TestMemmoveDstFirstFaultOrdering(t *testing.T) {
	vm := newMemTestVM(t, nil, nil)
	const dstBad = sbpf.VaddrHeap + 0x100000 // beyond 256KB heap
	const srcBad = sbpf.VaddrHeap + 0x180000
	const good = sbpf.VaddrHeap + 0

	// Both invalid -> the destination (Store) fault must win.
	_, err := SyscallMemmoveImpl(vm, dstBad, srcBad, 16)
	var ba sbpf.ExcBadAccess
	require.True(t, errors.As(err, &ba), "expected ExcBadAccess, got %v", err)
	require.True(t, ba.Write, "both-invalid memmove must fault on the write (dst) first")
	require.Equal(t, uint64(dstBad), ba.Addr)

	// Valid dst, invalid src -> read fault at src.
	_, err = SyscallMemmoveImpl(vm, good, srcBad, 16)
	require.True(t, errors.As(err, &ba))
	require.False(t, ba.Write, "src fault must be a read")
	require.Equal(t, uint64(srcBad), ba.Addr)
}

// --- input-region copy-on-write (dst materialized before src is read) ---

func TestMemmoveInputRegionCopyOnWrite(t *testing.T) {
	const regionLen = 64
	bufA := make([]byte, regionLen) // original backing (must stay untouched)
	bufB := make([]byte, regionLen) // CoW backing (write must land here)
	var onWriteCalled bool

	regions := []sbpf.InputRegion{{
		Offset:               0,
		RegionSize:           regionLen,
		AddressSpaceReserved: regionLen,
		Data:                 bufA,
		Writable:             false, // triggers OnWrite on a write access
		AccountIndex:         0,
		OnWrite: func(r *sbpf.InputRegion, _ uint64) error {
			onWriteCalled = true
			r.Data = bufB
			r.Writable = true
			return nil
		},
	}}
	vm := newMemTestVM(t, nil, regions)

	src := pattern(regionLen)
	const srcVA = sbpf.VaddrHeap + 0
	vmWrite(t, vm, srcVA, src)

	// memmove into the CoW input region: dst is translated (Store) first, which
	// fires OnWrite swapping the backing to bufB; then src is read; the copy
	// must land in bufB, leaving bufA untouched.
	_, err := SyscallMemmoveImpl(vm, sbpf.VaddrInput+0, srcVA, regionLen)
	require.NoError(t, err)
	require.True(t, onWriteCalled, "OnWrite should fire when writing a non-writable region")
	require.Equal(t, src, bufB, "copy must land in the post-CoW backing")
	require.Equal(t, make([]byte, regionLen), bufA, "original backing must be untouched")
}

// --- memset ---

func TestMemsetCorrectness(t *testing.T) {
	vm := newMemTestVM(t, nil, nil)
	// Place a guard byte immediately after each fill to catch overruns.
	for _, n := range memTestSizes {
		for _, c := range []byte{0x00, 0xAB, 0xFF} {
			base := uint64(sbpf.VaddrHeap + 0x3000)
			// pre-fill region + guard with a distinct value
			vmWrite(t, vm, base, bytes.Repeat([]byte{0x5A}, int(n)+1))
			ret, err := SyscallMemsetImpl(vm, base, uint64(c), n)
			require.NoError(t, err, "n=%d c=%#x", n, c)
			require.Equal(t, uint64(0), ret)
			got := vmRead(t, vm, base, n+1)
			require.Equal(t, bytes.Repeat([]byte{c}, int(n)), got[:n], "n=%d c=%#x", n, c)
			require.Equal(t, byte(0x5A), got[n], "memset overran by one byte (n=%d)", n)
		}
	}
}

// fillBytes exercised directly at the doubling threshold boundaries.
func TestFillBytesBoundaries(t *testing.T) {
	for _, n := range []int{0, 1, 31, 32, 33, 64, 1000} {
		for _, v := range []byte{0, 0x7F, 0xFF} {
			buf := bytes.Repeat([]byte{0x11}, n+1)
			fillBytes(buf[:n], v)
			require.Equal(t, bytes.Repeat([]byte{v}, n), buf[:n], "n=%d v=%#x", n, v)
			if n >= 0 {
				require.Equal(t, byte(0x11), buf[n], "fillBytes overran (n=%d)", n)
			}
		}
	}
}

// --- memcmp ---

func TestMemcmpCorrectness(t *testing.T) {
	vm := newMemTestVM(t, nil, nil)
	const a = sbpf.VaddrHeap + 0
	const b = sbpf.VaddrHeap + 0x10000
	const res = sbpf.VaddrHeap + 0x20000

	readResult := func() int32 {
		r := vmRead(t, vm, res, 4)
		return int32(uint32(r[0]) | uint32(r[1])<<8 | uint32(r[2])<<16 | uint32(r[3])<<24)
	}

	// Equal buffers of various sizes -> 0.
	for _, n := range memTestSizes {
		p := pattern(int(n))
		vmWrite(t, vm, a, p)
		vmWrite(t, vm, b, p)
		_, err := SyscallMemcmpImpl(vm, a, b, n, res)
		require.NoError(t, err, "n=%d", n)
		require.Equal(t, int32(0), readResult(), "equal buffers n=%d", n)
	}

	// Difference at a specific byte index (crossing word lanes and tail).
	for _, diffAt := range []int{0, 1, 7, 8, 9, 15, 16, 100} {
		n := uint64(128)
		p := pattern(int(n))
		q := append([]byte(nil), p...)
		q[diffAt] = p[diffAt] + 10 // b[diffAt] > a[diffAt]
		vmWrite(t, vm, a, p)
		vmWrite(t, vm, b, q)
		_, err := SyscallMemcmpImpl(vm, a, b, n, res)
		require.NoError(t, err)
		require.Equal(t, int32(p[diffAt])-int32(q[diffAt]), readResult(), "diffAt=%d (a<b)", diffAt)

		// Reverse the inequality.
		q[diffAt] = p[diffAt] - 10
		vmWrite(t, vm, b, q)
		_, err = SyscallMemcmpImpl(vm, a, b, n, res)
		require.NoError(t, err)
		require.Equal(t, int32(p[diffAt])-int32(q[diffAt]), readResult(), "diffAt=%d (a>b)", diffAt)
	}
}

// Cross-check memcmpBytes against a naive byte loop over many random-ish inputs.
func TestMemcmpBytesMatchesNaive(t *testing.T) {
	naive := func(a, b []byte, n uint64) int32 {
		for i := uint64(0); i < n; i++ {
			if a[i] != b[i] {
				return int32(a[i]) - int32(b[i])
			}
		}
		return 0
	}
	for seed := 0; seed < 200; seed++ {
		n := (seed*13 + 1) % 300
		a := make([]byte, n)
		b := make([]byte, n)
		for i := range a {
			a[i] = byte((i*7 + seed) & 0xff)
			b[i] = a[i]
		}
		// introduce 0..2 differences
		if n > 0 {
			b[(seed*3)%n] ^= 0x80
			if n > 5 {
				b[(seed*5)%n] ^= 0x01
			}
		}
		require.Equal(t, naive(a, b, uint64(n)), memcmpBytes(a, b, uint64(n)), "seed=%d n=%d", seed, n)
	}
}

func TestMemcmpFaultOrdering(t *testing.T) {
	vm := newMemTestVM(t, nil, nil)
	const good = sbpf.VaddrHeap + 0
	const bad = sbpf.VaddrHeap + 0x100000
	const resBad = sbpf.VaddrHeap + 0x180000

	vmWrite(t, vm, good, pattern(64))
	vmWrite(t, vm, good+0x1000, pattern(64))

	// Valid inputs, invalid result address -> result-addr (write) fault, which
	// only happens because the compare runs first (inputs already translated).
	_, err := SyscallMemcmpImpl(vm, good, good+0x1000, 64, resBad)
	var ba sbpf.ExcBadAccess
	require.True(t, errors.As(err, &ba))
	require.True(t, ba.Write)
	require.Equal(t, uint64(resBad), ba.Addr)

	// Invalid first input -> read fault before the result is ever translated.
	_, err = SyscallMemcmpImpl(vm, bad, good, 64, good+0x2000)
	require.True(t, errors.As(err, &ba))
	require.False(t, ba.Write)
	require.Equal(t, uint64(bad), ba.Addr)
}

// memcpy/memmove must not allocate (regression guard for the temp-buffer removal).
func TestMemcpyMemmoveZeroAlloc(t *testing.T) {
	vm := newMemTestVM(t, nil, nil)
	const dst = sbpf.VaddrHeap + 0
	const src = sbpf.VaddrHeap + 0x10000
	vmWrite(t, vm, src, pattern(4096))
	cpy := testing.AllocsPerRun(100, func() {
		_, _ = SyscallMemcpyImpl(vm, dst, src, 4096)
	})
	require.Zero(t, cpy, "memcpy must not allocate")
	mv := testing.AllocsPerRun(100, func() {
		_, _ = SyscallMemmoveImpl(vm, dst, src, 4096)
	})
	require.Zero(t, mv, "memmove must not allocate")
}
