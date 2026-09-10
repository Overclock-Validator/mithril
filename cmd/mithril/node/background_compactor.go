package node

import (
	"context"
	"sync"
	"time"
)

// backgroundCompactor owns the cancellation and join point for periodic
// AccountsDB compaction. Stop must complete before any AccountsDB teardown.
type backgroundCompactor struct {
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
}

func startBackgroundCompactor(parent context.Context, interval time.Duration, compact func(context.Context)) *backgroundCompactor {
	if parent == nil {
		panic("start background compactor: nil parent context")
	}
	if interval <= 0 {
		panic("start background compactor: non-positive interval")
	}
	if compact == nil {
		panic("start background compactor: nil compact callback")
	}

	ctx, cancel := context.WithCancel(parent)
	worker := &backgroundCompactor{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go func() {
		defer close(worker.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ctx.Err() != nil {
					return
				}
				compact(ctx)
			}
		}
	}()
	return worker
}

// Stop is idempotent. It cancels an active CompactOnceContext and waits until
// its callback has returned, making it safe to close AccountsDB afterward.
func (worker *backgroundCompactor) Stop() {
	if worker == nil {
		return
	}
	worker.stopOnce.Do(worker.cancel)
	<-worker.done
}
