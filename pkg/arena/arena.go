package arena

import (
	"sync/atomic"
)

// Arena is a generic arena allocator that pre-allocates a slice and provides
// goroutine-safe allocation of individual items or batches
type Arena[T any] struct {
	pool     []T
	index    uint64
	capacity uint64
	// Count only heap fallbacks; successful arena allocations keep their
	// existing fast path. Both counters reset with the arena.
	overflowElements atomic.Uint64
	overflowAllocs   atomic.Uint64
}

// New creates a new arena with the specified capacity
func New[T any](capacity uint64) *Arena[T] {
	return &Arena[T]{
		pool:     make([]T, capacity),
		index:    0,
		capacity: capacity,
	}
}

// Alloc allocates a single item from the arena.
// Returns a pointer to the item and true if allocated from arena,
// or a pointer to a heap-allocated item and false if arena is full.
func (a *Arena[T]) Alloc() (*T, bool) {
	for {
		current := atomic.LoadUint64(&a.index)
		if current >= a.capacity {
			// Fallback to heap allocation
			a.overflowElements.Add(1)
			a.overflowAllocs.Add(1)
			var item T
			return &item, false
		}
		if atomic.CompareAndSwapUint64(&a.index, current, current+1) {
			return &a.pool[current], true
		}
	}
}

// AllocN allocates n items from the arena.
// Returns a slice of the items and true if all allocated from arena,
// or a heap-allocated slice and false if arena doesn't have enough space.
func (a *Arena[T]) AllocN(n uint64) ([]T, bool) {
	if n == 0 {
		return nil, true
	}

	for {
		current := atomic.LoadUint64(&a.index)
		if n > a.capacity-current {
			// Fallback to heap allocation
			a.overflowElements.Add(n)
			a.overflowAllocs.Add(1)
			return make([]T, n), false
		}
		if atomic.CompareAndSwapUint64(&a.index, current, current+n) {
			return a.pool[current : current+n : current+n], true
		}
	}
}

// Used returns the number of elements reserved since the last reset.
func (a *Arena[T]) Used() uint64 {
	return atomic.LoadUint64(&a.index)
}

// Capacity returns the number of items the arena holds before falling back to the heap.
func (a *Arena[T]) Capacity() uint64 {
	return a.capacity
}

// Overflow returns heap-allocated element and allocation counts since the last
// reset. Read after users finish for a consistent per-bank measurement.
func (a *Arena[T]) Overflow() (elements, allocations uint64) {
	return a.overflowElements.Load(), a.overflowAllocs.Load()
}

// ResetPaced is Reset done in chunks of at most chunk items, calling pause
// between chunks, so a background caller can spread the clearing out instead
// of using all memory bandwidth at once. Same requirements as Reset.
func (a *Arena[T]) ResetPaced(chunk uint64, pause func()) {
	used := atomic.LoadUint64(&a.index)
	for start := uint64(0); start < used; start += chunk {
		end := min(start+chunk, used)
		clear(a.pool[start:end])
		if end < used {
			pause()
		}
	}
	a.index = 0
	a.overflowElements.Store(0)
	a.overflowAllocs.Store(0)
}

// Reset requires all users of the previous allocations to have finished.
func (a *Arena[T]) Reset() {
	clear(a.pool[:atomic.LoadUint64(&a.index)])
	a.index = 0
	a.overflowElements.Store(0)
	a.overflowAllocs.Store(0)
}
