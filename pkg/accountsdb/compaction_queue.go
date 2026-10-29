package accountsdb

import (
	"container/heap"
	"strings"
)

// compactCandidateQueue yields exactly the previous sorted/rotated/interleaved
// order, but only orders candidates actually requested by the scan budget.
// Heap construction is linear in directory size. No maintenance is skipped or
// delayed: cursor, retry alternation and every eighth cycle's slow priority are
// unchanged. The input is scratch storage and is partitioned in place.
type compactCandidateQueue struct {
	fresh, retry, slow   compactCandidateHeap
	retryNext, slowFirst bool
}

func newCompactCandidateQueue(candidates []compactCandidate, sparseCursor, slowCursor string, retryNext, slowFirst bool) *compactCandidateQueue {
	lo, mid, hi := 0, 0, len(candidates)
	for mid < hi {
		switch compactCandidateClass(candidates[mid]) {
		case '0':
			candidates[lo], candidates[mid] = candidates[mid], candidates[lo]
			lo++
			mid++
		case '1':
			mid++
		default:
			hi--
			candidates[mid], candidates[hi] = candidates[hi], candidates[mid]
		}
	}
	q := &compactCandidateQueue{
		fresh:     compactCandidateHeap{items: candidates[:lo], prefix: "0:", cursor: sparseCursor},
		retry:     compactCandidateHeap{items: candidates[lo:hi], prefix: "1:"},
		slow:      compactCandidateHeap{items: candidates[hi:], prefix: "2:", cursor: slowCursor},
		retryNext: retryNext, slowFirst: slowFirst,
	}
	heap.Init(&q.fresh)
	heap.Init(&q.retry)
	heap.Init(&q.slow)
	return q
}
func (q *compactCandidateQueue) next() (compactCandidate, bool) {
	if q.slowFirst && q.slow.Len() > 0 {
		return heap.Pop(&q.slow).(compactCandidate), true
	}
	if q.retry.Len() > 0 && (q.retryNext || q.fresh.Len() == 0) {
		q.retryNext = false
		return heap.Pop(&q.retry).(compactCandidate), true
	}
	if q.fresh.Len() > 0 {
		q.retryNext = true
		return heap.Pop(&q.fresh).(compactCandidate), true
	}
	if q.slow.Len() > 0 {
		return heap.Pop(&q.slow).(compactCandidate), true
	}
	return compactCandidate{}, false
}

type compactCandidateHeap struct {
	items          []compactCandidate
	prefix, cursor string
}

func (h compactCandidateHeap) Len() int      { return len(h.items) }
func (h compactCandidateHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h compactCandidateHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if h.prefix == "1:" {
		if a.scanOrder != b.scanOrder {
			return a.scanOrder < b.scanOrder
		}
	} else {
		// Compare prefix+name with the cursor without allocating concatenations.
		// A different cursor prefix places every item on the same side.
		if strings.HasPrefix(h.cursor, h.prefix) {
			afterA, afterB := a.name > h.cursor[2:], b.name > h.cursor[2:]
			if afterA != afterB {
				return afterA
			}
		}
	}
	return a.name < b.name
}
func (h *compactCandidateHeap) Push(v any) { h.items = append(h.items, v.(compactCandidate)) }
func (h *compactCandidateHeap) Pop() any {
	n := len(h.items) - 1
	v := h.items[n]
	h.items[n] = compactCandidate{}
	h.items = h.items[:n]
	return v
}
