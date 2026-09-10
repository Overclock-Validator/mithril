package accountsdb

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestRunBatchStaticRangesInlineSmallBatch(t *testing.T) {
	calls := 0
	err := runBatchStaticRanges(context.Background(), 16, func(_ context.Context, start, end int) error {
		calls++
		if start != 0 || end != 16 {
			t.Fatalf("inline range = [%d,%d), want [0,16)", start, end)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("run inline range: %v", err)
	}
	if calls != 1 {
		t.Fatalf("inline callback calls = %d, want 1", calls)
	}
}

func TestRunBatchStaticRangesLargeBatchCoversEachJobOnce(t *testing.T) {
	const count = 10_003
	seen := make([]bool, count)
	var mu sync.Mutex
	err := runBatchStaticRanges(context.Background(), count, func(_ context.Context, start, end int) error {
		if start < 0 || start >= end || end > count {
			return errors.New("invalid static range")
		}
		mu.Lock()
		defer mu.Unlock()
		for job := start; job < end; job++ {
			if seen[job] {
				return errors.New("duplicate static-range job")
			}
			seen[job] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("run static ranges: %v", err)
	}
	for job, ok := range seen {
		if !ok {
			t.Fatalf("static ranges missed job %d", job)
		}
	}
}

func TestRunBatchStaticRangesHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runBatchStaticRanges(ctx, 100, func(context.Context, int, int) error {
		t.Fatal("canceled static batch invoked work")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled static batch error = %v, want context.Canceled", err)
	}
}

func TestCoalesceBatchReadSpansBoundsAndKeepsItemRanges(t *testing.T) {
	spans := []batchReadSpan{
		{start: 0, end: 100},
		{start: 120, end: 180},   // nearby: joins the first range
		{start: 400, end: 460},   // gap is too large
		{start: 470, end: 700},   // union would exceed the range bound
		{start: 710, end: 2_000}, // oversized spans remain a standalone fallback
	}
	ranges, err := coalesceBatchReadSpans(spans, 32, 256)
	if err != nil {
		t.Fatalf("coalesce spans: %v", err)
	}
	want := []batchReadRange{
		{start: 0, end: 180, first: 0, limit: 2},
		{start: 400, end: 460, first: 2, limit: 3},
		{start: 470, end: 700, first: 3, limit: 4},
		{start: 710, end: 2_000, first: 4, limit: 5},
	}
	if len(ranges) != len(want) {
		t.Fatalf("ranges = %+v, want %+v", ranges, want)
	}
	for i := range want {
		if ranges[i] != want[i] {
			t.Fatalf("range %d = %+v, want %+v", i, ranges[i], want[i])
		}
	}
}

func TestCoalesceBatchReadSpansOverlapsAndValidatesInput(t *testing.T) {
	ranges, err := coalesceBatchReadSpans([]batchReadSpan{
		{start: 10, end: 30},
		{start: 20, end: 40},
		{start: 40, end: 40},
	}, 0, 64)
	if err != nil {
		t.Fatalf("coalesce overlapping spans: %v", err)
	}
	if len(ranges) != 1 || ranges[0] != (batchReadRange{start: 10, end: 40, first: 0, limit: 3}) {
		t.Fatalf("overlapping ranges = %+v", ranges)
	}

	for _, tc := range []struct {
		name     string
		spans    []batchReadSpan
		maxGap   int64
		maxBytes int64
	}{
		{name: "negative start", spans: []batchReadSpan{{start: -1, end: 1}}, maxBytes: 1},
		{name: "reversed", spans: []batchReadSpan{{start: 2, end: 1}}, maxBytes: 1},
		{name: "unordered", spans: []batchReadSpan{{start: 2, end: 3}, {start: 1, end: 2}}, maxBytes: 8},
		{name: "negative gap", spans: []batchReadSpan{{start: 0, end: 1}}, maxGap: -1, maxBytes: 1},
		{name: "zero bound", spans: []batchReadSpan{{start: 0, end: 1}}, maxBytes: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := coalesceBatchReadSpans(tc.spans, tc.maxGap, tc.maxBytes); err == nil {
				t.Fatal("invalid span input unexpectedly succeeded")
			}
		})
	}
}
