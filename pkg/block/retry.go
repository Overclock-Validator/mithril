package block

import (
	"context"
	"fmt"
	"time"
)

const (
	maxRetriesGetLeaderForSlot = 10
	baseBackoffMs              = 100
)

type GetLeaderFunc func(ctx context.Context) (interface{}, error)

func RetryWithExponentialBackoff(ctx context.Context, maxRetries int, fn GetLeaderFunc) (interface{}, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		result, err := fn(ctx)
		if err == nil {
			return result, nil
		}
		lastErr = err

		if attempt < maxRetries-1 {
			backoffDuration := time.Duration(attempt+1) * time.Duration(baseBackoffMs) * time.Millisecond
			select {
			case <-time.After(backoffDuration):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	return nil, fmt.Errorf("operation failed after %d attempts: %w", maxRetries, lastErr)
}
