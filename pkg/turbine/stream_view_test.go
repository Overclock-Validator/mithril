package turbine

import (
	"sync"
	"testing"
	"time"
)

func TestReadyStreamViewReused(t *testing.T) {
	g := NewDetachedStreamGeneration(42)
	ready := make(chan struct{})
	at := time.Now().Add(-time.Second)
	b := &prefetchedShredBatch{ready: ready, readyAt: at, start: 1, end: 2}
	close(ready)
	expected := newStreamBatch(g, b)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				got := newStreamBatch(g, b)
				if got != expected || !got.ReadyAt.Equal(at) {
					t.Error("view or readiness time changed")
				}
			}
		}()
	}
	wg.Wait()
	if n := testing.AllocsPerRun(100, func() { _ = newStreamBatch(g, b) }); n != 0 {
		t.Fatalf("cached view allocates: %v", n)
	}
}
