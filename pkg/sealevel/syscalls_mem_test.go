package sealevel

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	feat "github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/stretchr/testify/require"
)

func newMemSyscallVM(t *testing.T, input []byte, regions []sbpf.InputRegion) (*sbpf.Interpreter, *ExecutionCtx) {
	t.Helper()
	features := feat.NewFeaturesDefault()
	execCtx := &ExecutionCtx{Features: *features, ComputeMeter: cu.NewComputeMeter(1_000_000)}
	vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram, Funcs: map[uint32]int64{}}, &sbpf.VMOpts{
		Input:        input,
		Context:      execCtx,
		ComputeMeter: &execCtx.ComputeMeter,
		InputRegions: regions,
	})
	t.Cleanup(vm.Finish)
	return vm, execCtx
}

func TestSyscallMemmoveOverlapping(t *testing.T) {
	for _, test := range []struct {
		name          string
		dst, src, n   uint64
		expectMemcpyE bool
	}{
		{name: "forward-overlap", dst: 4, src: 0, n: 16, expectMemcpyE: true},
		{name: "backward-overlap", dst: 0, src: 4, n: 16, expectMemcpyE: true},
		{name: "disjoint", dst: 40, src: 0, n: 16},
		{name: "adjacent", dst: 16, src: 0, n: 16},
		{name: "empty", dst: 0, src: 0, n: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := make([]byte, 64)
			for i := range input {
				input[i] = byte(i + 1)
			}
			want := append([]byte(nil), input...)
			copy(want[test.dst:test.dst+test.n], want[test.src:test.src+test.n])

			vm, _ := newMemSyscallVM(t, input, nil)
			ret, err := SyscallMemmoveImpl(vm, sbpf.VaddrInput+test.dst, sbpf.VaddrInput+test.src, test.n)
			require.NoError(t, err)
			require.Zero(t, ret)
			require.Equal(t, want, input, "memmove must have Go copy (memmove) semantics")

			// memcpy: the same result for disjoint ranges, an error for overlap.
			input2 := make([]byte, 64)
			for i := range input2 {
				input2[i] = byte(i + 1)
			}
			vm2, _ := newMemSyscallVM(t, input2, nil)
			_, err = SyscallMemcpyImpl(vm2, sbpf.VaddrInput+test.dst, sbpf.VaddrInput+test.src, test.n)
			if test.expectMemcpyE {
				require.ErrorIs(t, err, SyscallErrCopyOverlapping)
			} else {
				require.NoError(t, err)
				require.Equal(t, want, input2)
			}
		})
	}
}

func TestSyscallMemmoveBadAddressOrder(t *testing.T) {
	input := make([]byte, 32)
	vm, _ := newMemSyscallVM(t, input, nil)
	// Unreadable source is reported before an unwritable destination.
	_, err := SyscallMemmoveImpl(vm, sbpf.VaddrProgram, sbpf.VaddrInput+100, 8)
	require.Error(t, err)
	var badAccess sbpf.ExcBadAccess
	require.ErrorAs(t, err, &badAccess)
	require.False(t, badAccess.Write, "the source translation must fail first")
	// Readable source, write to a read-only region.
	_, err = SyscallMemmoveImpl(vm, sbpf.VaddrProgram, sbpf.VaddrInput, 8)
	require.Error(t, err)
	require.ErrorAs(t, err, &badAccess)
	require.True(t, badAccess.Write)
}

func TestSyscallMemmoveIntoGrowingInputRegion(t *testing.T) {
	// The destination region copy-on-writes and grows on first write; the
	// source slice taken before that must still yield the original bytes.
	original := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	var replaced []byte
	region := sbpf.InputRegion{
		Offset:               0,
		RegionSize:           uint64(len(original)),
		AddressSpaceReserved: 32,
		Writable:             false,
		AccountIndex:         0,
		Data:                 original,
		OnWrite: func(region *sbpf.InputRegion, requestedLen uint64) error {
			replaced = make([]byte, 32)
			copy(replaced, region.Data)
			region.Data = replaced
			region.RegionSize = 32
			region.Writable = true
			return nil
		},
	}
	vm, _ := newMemSyscallVM(t, nil, []sbpf.InputRegion{region})
	// Copy the first 4 bytes over bytes 4..8 within the same region: the
	// source translation sees the original buffer, the destination the clone.
	_, err := SyscallMemmoveImpl(vm, sbpf.VaddrInput+4, sbpf.VaddrInput, 4)
	require.NoError(t, err)
	require.NotNil(t, replaced, "the first write must trigger the copy-on-write hook")
	require.Equal(t, []byte{1, 2, 3, 4, 1, 2, 3, 4}, replaced[:8])
	require.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8}, original, "the shared buffer must stay untouched")
}

func TestSyscallMemcmpAndMemset(t *testing.T) {
	input := make([]byte, 128)
	for i := range input {
		input[i] = byte(i)
	}
	vm, _ := newMemSyscallVM(t, input, nil)

	// memcmp of equal and differing 32-byte keys, result written at 96.
	copy(input[32:64], input[0:32])
	_, err := SyscallMemcmpImpl(vm, sbpf.VaddrInput, sbpf.VaddrInput+32, 32, sbpf.VaddrInput+96)
	require.NoError(t, err)
	require.Equal(t, int32(0), int32(binary.LittleEndian.Uint32(input[96:])))
	input[63] = 0xff
	_, err = SyscallMemcmpImpl(vm, sbpf.VaddrInput, sbpf.VaddrInput+32, 32, sbpf.VaddrInput+96)
	require.NoError(t, err)
	require.Equal(t, int32(31)-int32(0xff), int32(binary.LittleEndian.Uint32(input[96:])))
	_, err = SyscallMemcmpImpl(vm, sbpf.VaddrInput+32, sbpf.VaddrInput, 32, sbpf.VaddrInput+96)
	require.NoError(t, err)
	require.Equal(t, int32(0xff)-int32(31), int32(binary.LittleEndian.Uint32(input[96:])))

	// memset 0xab over 33 bytes, then zero over 9 bytes.
	_, err = SyscallMemsetImpl(vm, sbpf.VaddrInput+64, 0x1ab, 33)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xab}, 33), input[64:97])
	require.Equal(t, byte(97), input[97])
	_, err = SyscallMemsetImpl(vm, sbpf.VaddrInput+70, 0, 9)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xab}, 6), input[64:70])
	require.Equal(t, make([]byte, 9), input[70:79])
	require.Equal(t, bytes.Repeat([]byte{0xab}, 18), input[79:97])
}

// The byte loop the syscalls used before memcmpResult; kept as the reference.
func referenceMemcmp(a, b []byte) int32 {
	for i := range a {
		if a[i] != b[i] {
			return int32(a[i]) - int32(b[i])
		}
	}
	return 0
}

func TestMemcmpResultMatchesByteLoop(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for iter := 0; iter < 100000; iter++ {
		n := rng.Intn(70)
		a := make([]byte, n)
		rng.Read(a)
		b := append([]byte(nil), a...)
		if n > 0 && rng.Intn(4) != 0 {
			b[rng.Intn(n)] = byte(rng.Intn(256))
			if rng.Intn(2) == 0 {
				b[rng.Intn(n)] ^= byte(1 + rng.Intn(255))
			}
		}
		if got, want := memcmpResult(a, b), referenceMemcmp(a, b); got != want {
			t.Fatalf("n=%d a=%x b=%x: got %d want %d", n, a, b, got, want)
		}
	}
	if memcmpResult([]byte{0xff}, []byte{0x00}) != 255 || memcmpResult([]byte{0x00}, []byte{0xff}) != -255 {
		t.Fatal("memcmp must return the unsigned byte difference")
	}
	if memcmpResult(nil, nil) != 0 {
		t.Fatal("empty compare must be 0")
	}
}

func TestMemsetBytes(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 7, 8, 9, 31, 32, 33, 100, 1023, 4096, 10001} {
		for _, c := range []byte{0, 1, 0x7f, 0xff} {
			mem := make([]byte, n)
			for i := range mem {
				mem[i] = byte(i)
			}
			memsetBytes(mem, c)
			if !bytes.Equal(mem, bytes.Repeat([]byte{c}, n)) {
				t.Fatalf("n=%d c=%d: %x", n, c, mem)
			}
		}
	}
}
