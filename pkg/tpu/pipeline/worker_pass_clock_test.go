package pipeline

import (
	"testing"
	"time"
)

func TestWorkerPassClockIncludesOpenAndCompletedPasses(t *testing.T) {
	now := time.Now()
	started := now.Add(-10 * time.Second)
	w := workerPassClock{start: now.Add(-2 * time.Second)}
	active, ok := w.observe(started, 3)
	if !ok || active.Index != 3 || active.Active < 2*time.Second || active.Active > 3*time.Second || active.Elapsed < 10*time.Second {
		t.Fatalf("open pass: %+v, %v", active, ok)
	}
	w.end()
	finished, ok := w.observe(started, 3)
	if !ok || finished.Active < 2*time.Second || finished.Active > 3*time.Second {
		t.Fatalf("completed pass: %+v, %v", finished, ok)
	}
	w.begin()
	w.begin()
	if _, ok = w.observe(started, 3); ok {
		t.Fatal("overlapping pass must be withheld")
	}
}

func TestWorkerPassClockConcurrentObservation(t *testing.T) {
	started := time.Now()
	var clock workerPassClock
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			clock.begin()
			clock.end()
		}
	}()
	for {
		select {
		case <-done:
			if _, ok := clock.observe(started, 0); !ok {
				t.Fatal("final sample invalid")
			}
			return
		default:
			if _, ok := clock.observe(started, 0); !ok {
				t.Fatal("concurrent sample invalid")
			}
		}
	}
}
