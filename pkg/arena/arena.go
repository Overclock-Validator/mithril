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

// Reset requires all users of the previous allocations to have finished.
func (a *Arena[T]) Reset() {
	clear(a.pool[:atomic.LoadUint64(&a.index)])
	a.index = 0
}
