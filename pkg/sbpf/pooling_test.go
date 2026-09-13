package sbpf

import (
	"bytes"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
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
				parent.heap[0] = byte(worker + 1)
				parent.stack.mem[0] = byte(worker + 1)
				child := poolingInterpreter(256 * 1024)
				for j := range child.heap {
					child.heap[j] = 0xab
				}
				for j := range child.stack.mem {
					child.stack.mem[j] = 0xcd
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
