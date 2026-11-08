package spinlock

import (
	"sync"
	"testing"
)

func TestLocksExcludeWriters(t *testing.T) {
	var mu sync.Mutex
	var rw sync.RWMutex
	var a, b int
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20000 {
				Lock(&mu)
				a++
				mu.Unlock()
				WLock(&rw)
				b++
				rw.Unlock()
				RLock(&rw)
				_ = b
				rw.RUnlock()
			}
		}()
	}
	wg.Wait()
	if a != 8*20000 || b != 8*20000 {
		t.Fatalf("lost updates: a=%d b=%d", a, b)
	}
}

func TestFallsBackToBlockingLock(t *testing.T) {
	var rw sync.RWMutex
	rw.Lock()
	acquired := make(chan struct{})
	go func() {
		RLock(&rw)
		close(acquired)
		rw.RUnlock()
	}()
	select {
	case <-acquired:
		t.Fatal("read lock acquired while write-locked")
	default:
	}
	rw.Unlock()
	<-acquired
}
