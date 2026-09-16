package sealevel

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

// Frozen syscall implementation from bd17683a; keep independent for differential tests.
func sha256BaselineReference(vm sbpf.VM, valsAddr, valsLen, resultsAddr uint64) (uint64, error) {
	//mlog.Log.Debugf("sha256BaselineReference")

	if valsLen > cu.CUSha256MaxSlices {
		return syscallErr(SyscallErrTooManySlices)
	}

	execCtx := executionCtx(vm)
	err := execCtx.ComputeMeter.Consume(cu.CUSha256BaseCost)
	if err != nil {
		return syscallCuErr()
	}

	hashResult, err := vm.Translate(resultsAddr, 32, true)
	if err != nil {
		return syscallErr(err)
	}

	hasher := sha256.New()
	if valsLen > 0 {
		var vals []byte

		// The data at 'valsAddr' consists of an array of 'slice references', which consists
		// of: [ptr (u64)] [size (u64)], hence 16 bytes for each of the slice references that
		// refers to an input value to hash.
		// Safety: valsLen*16 cannot overflow because of the check versus CUSha256MaxSlices above
		vals, err = vm.Translate(valsAddr, valsLen*16, false)
		if err != nil {
			return syscallErr(err)
		}

		var data []byte
		reader := bytes.NewReader(vals)

		for count := uint64(0); count < valsLen; count++ {

			var vec VectorDescrC
			err = vec.Unmarshal(reader)
			if err != nil {
				return syscallErr(err)
			}

			data, err = vm.Translate(vec.Addr, vec.Len, false)
			if err != nil {
				return syscallErr(err)
			}

			cost := max(vec.Len/2, cu.CUMemOpBaseCost)
			err = execCtx.ComputeMeter.Consume(cost)
			if err != nil {
				return syscallCuErr()
			}

			hasher.Write(data)
		}
	}
	copy(hashResult[:], hasher.Sum(nil))
	return syscallSuccess(0)
}

// Experimental bounded-buffer variant retained only for benchmark comparison.
func sha256SmallInputReference(vm sbpf.VM, valsAddr, valsLen, resultsAddr uint64) (uint64, error) {
	//mlog.Log.Debugf("sha256SmallInputReference")

	if valsLen > cu.CUSha256MaxSlices {
		return syscallErr(SyscallErrTooManySlices)
	}

	execCtx := executionCtx(vm)
	err := execCtx.ComputeMeter.Consume(cu.CUSha256BaseCost)
	if err != nil {
		return syscallCuErr()
	}

	hashResult, err := vm.Translate(resultsAddr, 32, true)
	if err != nil {
		return syscallErr(err)
	}

	hasher := sha256.New()
	// Inputs up to 55 bytes fit in one padded SHA-256 block. Buffer only
	// this bounded case; larger inputs retain streaming hashing.
	var small [55]byte
	buffered := 0
	streaming := false
	if valsLen > 0 {
		var vals []byte

		// The data at 'valsAddr' consists of an array of 'slice references', which consists
		// of: [ptr (u64)] [size (u64)], hence 16 bytes for each of the slice references that
		// refers to an input value to hash.
		// Safety: valsLen*16 cannot overflow because of the check versus CUSha256MaxSlices above
		vals, err = vm.Translate(valsAddr, valsLen*16, false)
		if err != nil {
			return syscallErr(err)
		}

		var data []byte

		for count := uint64(0); count < valsLen; count++ {

			offset := count * 16
			vec := VectorDescrC{Addr: binary.LittleEndian.Uint64(vals[offset:]), Len: binary.LittleEndian.Uint64(vals[offset+8:])}

			data, err = vm.Translate(vec.Addr, vec.Len, false)
			if err != nil {
				return syscallErr(err)
			}

			cost := max(vec.Len/2, cu.CUMemOpBaseCost)
			err = execCtx.ComputeMeter.Consume(cost)
			if err != nil {
				return syscallCuErr()
			}

			if !streaming && len(data) <= len(small)-buffered {
				buffered += copy(small[buffered:], data)
			} else {
				if !streaming {
					hasher.Write(small[:buffered])
					streaming = true
				}
				hasher.Write(data)
			}
		}
	}
	if streaming {
		hasher.Sum(hashResult[:0])
	} else {
		digest := sha256.Sum256(small[:buffered])
		copy(hashResult, digest[:])
	}
	return syscallSuccess(0)
}

type sha256Call func(sbpf.VM, uint64, uint64, uint64) (uint64, error)

func sha256Fixture(sizes []int) ([]byte, uint64, uint64, uint64) {
	mem := make([]byte, 32768)
	pos := 8192
	for i, n := range sizes {
		binary.LittleEndian.PutUint64(mem[i*16:], sbpf.VaddrInput+uint64(pos))
		binary.LittleEndian.PutUint64(mem[i*16+8:], uint64(n))
		for j := 0; j < n; j++ {
			mem[pos+j] = byte(i + j)
		}
		pos += n
	}
	return mem, sbpf.VaddrInput, uint64(len(sizes)), sbpf.VaddrInput + 4096
}

func sha256VM(mem []byte, budget uint64) (*sbpf.Interpreter, *ExecutionCtx) {
	ctx := &ExecutionCtx{ComputeMeter: cu.NewComputeMeter(budget)}
	vm := sbpf.NewInterpreter(&sbpf.Program{}, &sbpf.VMOpts{Input: mem, HeapMax: 32768, Context: ctx, ComputeMeter: &ctx.ComputeMeter})
	return vm, ctx
}

func TestSha256SyscallDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(1234))
	for i := 0; i < 700; i++ {
		sizes := make([]int, rng.Intn(8))
		for j := range sizes {
			sizes[j] = rng.Intn(80)
		}
		if i < 8 {
			sizes = [][]int{nil, {0}, {32, 4}, {55}, {56}, {32, 24}, {4096}, {0, 32, 0, 4}}[i]
		}
		mem, a, n, out := sha256Fixture(sizes)
		budget := uint64(100000)
		switch i % 11 {
		case 1:
			budget = uint64(rng.Intn(250))
		case 2:
			out = 1
		case 3:
			a = 1
		case 4:
			if n > 0 {
				binary.LittleEndian.PutUint64(mem, 1)
			}
		case 5:
			if n > 0 {
				binary.LittleEndian.PutUint64(mem[8:], ^uint64(0))
			}
		case 6:
			out = sbpf.VaddrInput + 8192 // output overlaps the input
		case 7:
			out = a // output overlaps descriptors
		case 8:
			n = cu.CUSha256MaxSlices + 1
		case 9:
			n = cu.CUSha256MaxSlices
		case 10:
			a = sbpf.VaddrInput + uint64(len(mem)-1)
		}
		var wantMem []byte
		var wantRet, wantCU uint64
		var wantErr string
		for k, fn := range []sha256Call{sha256BaselineReference, sha256SmallInputReference, SyscallSha256Impl} {
			buf := append([]byte(nil), mem...)
			vm, ctx := sha256VM(buf, budget)
			ret, err := fn(vm, a, n, out)
			remaining := ctx.ComputeMeter.Remaining()
			vm.Finish()
			if k == 0 {
				wantMem = buf
				wantRet = ret
				wantCU = remaining
				wantErr = fmt.Sprint(err)
				continue
			}
			require.Equal(t, wantRet, ret, "case %d variant %d", i, k)
			require.Equal(t, wantErr, fmt.Sprint(err), "case %d variant %d", i, k)
			require.Equal(t, wantCU, remaining, "case %d variant %d", i, k)
			require.Equal(t, wantMem, buf, "case %d variant %d", i, k)
		}
	}
}

var sha256BenchDigest [32]byte

func BenchmarkSha256Syscall(b *testing.B) {
	for _, tc := range []struct {
		name  string
		sizes []int
	}{{"empty", nil}, {"36_contiguous", []int{36}}, {"32_plus_4", []int{32, 4}}, {"55", []int{55}}, {"56", []int{56}}, {"1232", []int{1232}}, {"4096", []int{4096}}} {
		for _, variant := range []struct {
			name string
			fn   sha256Call
		}{{"baseline", sha256BaselineReference}, {"lean", SyscallSha256Impl}, {"small", sha256SmallInputReference}} {
			b.Run(tc.name+"/"+variant.name, func(b *testing.B) {
				mem, a, n, out := sha256Fixture(tc.sizes)
				vm, ctx := sha256VM(mem, ^uint64(0))
				defer vm.Finish()
				b.ReportAllocs()
				b.ResetTimer()
				for j := 0; j < b.N; j++ {
					ctx.ComputeMeter = cu.NewComputeMeter(100000)
					if _, err := variant.fn(vm, a, n, out); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
	b.Run("raw36", func(b *testing.B) {
		var data [36]byte
		b.ReportAllocs()
		for j := 0; j < b.N; j++ {
			sha256BenchDigest = sha256.Sum256(data[:])
		}
	})
}
