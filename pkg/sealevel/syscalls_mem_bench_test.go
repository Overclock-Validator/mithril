package sealevel

import (
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	feat "github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
)

// Benchmarks for the memory syscalls (sol_memcpy_/memmove_/memset_/memcmp_).
// These currently allocate a temporary buffer and copy twice (memcpy/memmove)
// and iterate byte-at-a-time (memset/memcmp); this file is the durable baseline
// for the rewrite. Capture with:
//
//	go test -bench=BenchmarkMem -benchmem -count=10 ./pkg/sealevel/

const benchMemHeap = 256 * 1024

func newMemBenchVM(b *testing.B) sbpf.VM {
	b.Helper()
	features := feat.NewFeaturesDefault()
	execCtx := &ExecutionCtx{Features: *features}
	execCtx.ComputeMeter = cu.NewComputeMeter(1 << 62)
	vm := sbpf.NewInterpreter(
		&sbpf.Program{TextVA: sbpf.VaddrProgram, Funcs: map[uint32]int64{}},
		&sbpf.VMOpts{
			HeapMax:      benchMemHeap,
			Input:        make([]byte, 128*1024),
			Context:      execCtx,
			ComputeMeter: &execCtx.ComputeMeter,
		},
	)
	b.Cleanup(vm.Finish)
	return vm
}

var benchMemSizes = []uint64{32, 256, 4096, 65536}

// heap dst/src are chosen non-overlapping for the largest size.
const (
	benchDstVA    = sbpf.VaddrHeap
	benchSrcHeap  = sbpf.VaddrHeap + 65536
	benchSrcInput = sbpf.VaddrInput
	benchCmpResVA = sbpf.VaddrHeap + 131072
)

func BenchmarkMemcpyHeap(b *testing.B) {
	vm := newMemBenchVM(b)
	for _, n := range benchMemSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := SyscallMemcpyImpl(vm, benchDstVA, benchSrcHeap, n); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMemcpyInput(b *testing.B) {
	vm := newMemBenchVM(b)
	for _, n := range benchMemSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := SyscallMemcpyImpl(vm, benchDstVA, benchSrcInput, n); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMemmoveHeap(b *testing.B) {
	vm := newMemBenchVM(b)
	for _, n := range benchMemSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := SyscallMemmoveImpl(vm, benchDstVA, benchSrcHeap, n); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMemset(b *testing.B) {
	vm := newMemBenchVM(b)
	for _, n := range benchMemSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := SyscallMemsetImpl(vm, benchDstVA, 0xAB, n); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMemcmp compares equal buffers (worst case: full scan of n bytes).
func BenchmarkMemcmp(b *testing.B) {
	vm := newMemBenchVM(b)
	for _, n := range benchMemSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := SyscallMemcmpImpl(vm, benchDstVA, benchSrcHeap, n, benchCmpResVA); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
