package accountsdb

import (
	"context"
	"fmt"
	"runtime"
	"sync"
)

const batchStaticParallelMinJobs = 64

// batchReadSpan is one exact byte interval needed by a logical item. Spans
// passed to coalesceBatchReadSpans must be ordered by start offset.
type batchReadSpan struct {
	start int64
	end   int64
}

// batchReadRange is one bounded physical read covering spans[first:limit]. A
// single oversized span is returned alone so callers can use a zero-copy/direct
// fallback rather than allocating an equally large temporary coalescing buffer.
type batchReadRange struct {
	start int64
	end   int64
	first int
	limit int
}

func coalesceBatchReadSpans(spans []batchReadSpan, maxGap, maxRangeBytes int64) ([]batchReadRange, error) {
	if maxGap < 0 {
		return nil, fmt.Errorf("accountsdb: negative batch read coalesce gap %d", maxGap)
	}
	if maxRangeBytes <= 0 {
		return nil, fmt.Errorf("accountsdb: non-positive batch read range limit %d", maxRangeBytes)
	}
	if len(spans) == 0 {
		return nil, nil
	}

	ranges := make([]batchReadRange, 0, len(spans))
	for item, span := range spans {
		if span.start < 0 || span.end < span.start {
			return nil, fmt.Errorf("accountsdb: invalid batch read span %d [%d,%d)", item, span.start, span.end)
		}
		if item > 0 && span.start < spans[item-1].start {
			return nil, fmt.Errorf(
				"accountsdb: unordered batch read span %d starts at %d before %d",
				item, span.start, spans[item-1].start,
			)
		}

		if len(ranges) == 0 {
			ranges = append(ranges, batchReadRange{start: span.start, end: span.end, first: item, limit: item + 1})
			continue
		}
		current := &ranges[len(ranges)-1]
		gap := int64(0)
		if span.start > current.end {
			gap = span.start - current.end
		}
		mergedEnd := max(current.end, span.end)
		if gap <= maxGap && mergedEnd-current.start <= maxRangeBytes {
			current.end = mergedEnd
			current.limit = item + 1
			continue
		}
		ranges = append(ranges, batchReadRange{start: span.start, end: span.end, first: item, limit: item + 1})
	}
	return ranges, nil
}

// runBatchStaticRanges partitions count contiguous jobs once and gives each
// worker one fixed range. Unlike the general small-job scheduler, this puts no
// shared atomic counter in the per-key index-probe loop. The callback should
// check workerCtx periodically when a range can take appreciable time.
func runBatchStaticRanges(
	ctx context.Context,
	count int,
	work func(workerCtx context.Context, start, end int) error,
) error {
	if count == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Goroutine/context setup costs more than a handful of hot mmap probes.
	// Keep transaction- and sysvar-sized batches allocation-free while the
	// large block path below still exposes independent memory chains.
	if count < batchStaticParallelMinJobs {
		return work(ctx, 0, count)
	}

	workerCount := min(count, max(1, runtime.GOMAXPROCS(0)*2))
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	setError := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	baseRange := count / workerCount
	extra := count % workerCount
	start := 0
	wg.Add(workerCount)
	for worker := 0; worker < workerCount; worker++ {
		width := baseRange
		if worker < extra {
			width++
		}
		end := start + width
		go func(start, end int) {
			defer wg.Done()
			if err := work(workerCtx, start, end); err != nil {
				setError(err)
			}
		}(start, end)
		start = end
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}
