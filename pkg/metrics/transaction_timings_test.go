package metrics

import (
	"sync"
	"testing"
	"time"
)

func TestTransactionTimingsMergeAndAtomicFallback(t *testing.T) {
	previous := GlobalBlockReplay
	defer func() { GlobalBlockReplay = previous }()
	GlobalBlockReplay = BlockReplay{}
	const workers, calls = 8, 200
	local := make([]TransactionTimings, workers)
	var wg sync.WaitGroup
	for worker := range local {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range calls {
				for phase := TransactionPhase(0); phase < transactionPhaseCount; phase++ {
					local[worker].AddTimingSince(phase, time.Now())
				}
			}
		}()
	}
	wg.Wait()
	for _, target := range transactionTimingTargets {
		if target.Count != 0 {
			t.Fatal("local records escaped before merge")
		}
	}
	for i := range local {
		local[i].Merge()
		local[i].Merge()
	}
	for _, target := range transactionTimingTargets {
		if target.Count != workers*calls {
			t.Fatalf("count = %d", target.Count)
		}
	}
	var shared *TransactionTimings
	shared.AddTimingSince(TransactionIxLoop, time.Now())
	shared.AddTimingSince(TransactionIxLoop, time.Time{})
	if GlobalBlockReplay.IxLoop.Count != workers*calls+1 {
		t.Fatal("nil/disabled recorder changed fallback counts")
	}
}
