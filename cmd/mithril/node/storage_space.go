package node

import (
	"context"
	"fmt"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

var storageMinFreeBytes uint64

func configureDiskBudget() error {
	value, err := memoryMiB("storage.min_free_mb", 32<<30, false)
	if err != nil {
		return err
	}
	storageMinFreeBytes = value
	return nil
}

func checkStorageSpace(paths []string, reserve uint64, available func(string) (uint64, error)) error {
	for _, path := range paths {
		if path == "" {
			continue
		}
		free, err := available(path)
		if err != nil {
			return err
		}
		if free < reserve {
			return fmt.Errorf("storage space guard: %s has %d MiB available, below %d MiB reserve; stopping replay to preserve room for vote history and recovery", path, free>>20, reserve>>20)
		}
	}
	return nil
}

// Check all writing volumes independently of fold progress (including while
// waiting for shreds). Cancellation uses the normal replay shutdown path.
func startStorageSpaceGuard(parent context.Context, check func() error, cancelRun context.CancelFunc) func() {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			if err := check(); err != nil {
				mlog.Log.Errorf("%v", err)
				cancelRun()
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() { cancel(); <-done }
}
