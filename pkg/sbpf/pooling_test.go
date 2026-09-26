package sbpf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
	"github.com/stretchr/testify/require"
)

func poolingInterpreter(heap int) *Interpreter {
	meter := cu.NewComputeMeter(100)
	return NewInterpreter(testV3Program([]Slot{testSlot(OpExit, 0, 0, 0, 0)}, nil),
		&VMOpts{HeapMax: heap, ComputeMeter: &meter})
}

func TestPooledVMIsolationAcrossNestedAndConcurrentExecutions(t *testing.T) {
	old := UsePool
	UsePool = true
	t.Cleanup(func() { UsePool = old })
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				parent := poolingInterpreter(32 * 1024)
				if !bytes.Equal(parent.heap, make([]byte, len(parent.heap))) ||
					!bytes.Equal(parent.stack.mem, make([]byte, len(parent.stack.mem))) {
					t.Error("pooled VM exposed data from an earlier execution")
				}
				// Write through the VM's translation layer, as programs and
				// syscalls do: the pool only re-zeroes memory the VM saw written.
				if err := parent.Write8(VaddrHeap, byte(worker+1)); err != nil {
					t.Error(err)
				}
				if err := parent.Write8(VaddrStack, byte(worker+1)); err != nil {
					t.Error(err)
				}
				child := poolingInterpreter(256 * 1024)
				childHeap, err := child.Translate(VaddrHeap, uint64(len(child.heap)), true)
				if err != nil {
					t.Error(err)
				}
				for j := range childHeap {
					childHeap[j] = 0xab
				}
				childStack, err := child.Translate(VaddrStack, StackMax, true)
				if err != nil {
					t.Error(err)
				}
				for j := range childStack {
					childStack[j] = 0xcd
				}
				child.Finish()
				if parent.heap[0] != byte(worker+1) || parent.stack.mem[0] != byte(worker+1) {
					t.Error("nested VM storage aliased its active parent")
				}
				parent.Finish()
			}
		}(worker)
	}
	wg.Wait()
	ip := poolingInterpreter(32 * 1024)
	defer ip.Finish()
	require.Len(t, ip.heap, 32*1024)
	_, err := ip.Translate(VaddrHeap+32*1024, 1, false)
	require.Error(t, err, "pool capacity must not widen the requested heap mapping")
}

func BenchmarkVMCreateAndFinish(b *testing.B) {
	old := UsePool
	b.Cleanup(func() { UsePool = old })
	for _, pooled := range []bool{false, true} {
		name := "fresh"
		if pooled {
			name = "pooled"
		}
		b.Run(name, func(b *testing.B) {
			UsePool = pooled
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ip := poolingInterpreter(32 * 1024)
				ip.Finish()
			}
		})
	}
}

// Exercise actual stores at and beyond the bitmap boundary. Inspect the returned
// buffer directly: sync.Pool is permitted to discard entries, so a subsequent Get
// alone would not reliably detect a missed clear.
func TestPooledHeapDirtyBitmapBoundary(t *testing.T) {
	oldUsePool, oldPool := UsePool, heapPool
	UsePool = true
	heapPool = &sync.Pool{New: func() any { return newHeap() }}
	t.Cleanup(func() { UsePool, heapPool = oldUsePool, oldPool })
	for _, size := range []int{fastDirtyBytes, fastDirtyBytes + 1, 2 * fastDirtyBytes} {
		for _, ver := range []uint32{sbpfver.SbpfVersionV0, sbpfver.SbpfVersionV2, sbpfver.SbpfVersionV3} {
			t.Run(fmt.Sprintf("%d/v%d", size, ver), func(t *testing.T) {
				offsets := []int{0, size - 8}
				if size >= fastDirtyBytes+8 {
					offsets = append(offsets, fastDirtyBytes-4, fastDirtyBytes)
				}
				var text []Slot
				op := uint8(OpStdw)
				if ver == sbpfver.SbpfVersionV2 {
					op = OpSt8BImm
				}
				for _, off := range offsets {
					text = append(text, diffLoadImm64(5, VaddrHeap+uint64(off), ver)...)
					text = append(text, slot(op, 5, 0, 0, 0x12345678))
				}
				text = append(text, slot(OpExit, 0, 0, 0, 0))
				program := mkProgram(text, ver)
				require.NoError(t, program.Verify())
				meter := cu.NewComputeMeter(100)
				ip := NewInterpreter(program, &VMOpts{HeapMax: size, ComputeMeter: &meter, Syscalls: noSyscalls})
				// Always clear test storage on failure so later cases cannot inherit dirt.
				defer func() { clear(ip.heap) }()
				require.NotNil(t, ip.fastRead(VaddrHeap+uint64(size-8), 8))
				_, _, err := ip.Run()
				require.NoError(t, err)
				require.Equal(t, uint64(0x12345678), binary.LittleEndian.Uint64(ip.heap[offsets[len(offsets)-1]:]))
				ip.Finish()
				require.True(t, bytes.Equal(make([]byte, size), ip.heap), "Finish must clear every written byte before pooling")
				require.Equal(t, size <= fastDirtyBytes, ip.regions[VaddrHeap>>32].wlen != 0)
			})
		}
	}
}
