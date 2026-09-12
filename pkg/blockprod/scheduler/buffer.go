package scheduler

import (
	"container/heap"
	"sync"

	"github.com/gagliardetto/solana-go"
)

// MaxBufferedTxns is the hard cap on cross-slot buffered transactions.
const MaxBufferedTxns = 2 * 65536

// entry is one buffered, scored transaction.
type entry struct {
	tx *solana.Transaction
	// wire is an owned copy of the packet bytes. Parsed tx fields may alias it
	// (solana-go decoder slices), so it must outlive any use of tx.
	wire        []byte
	wireSize    int
	messageHash [32]byte
	blockhash   solana.Hash
	reward      uint64
	seq         uint64
	// skipGen matches Scheduler.bankGen when forge hit a slot-local reject
	// (e.g. cost limit). The entry is retained for cross-slot retry.
	skipGen uint64

	alive  bool
	maxIdx int
	minIdx int
}

type maxHeap []*entry

func (h maxHeap) Len() int { return len(h) }
func (h maxHeap) Less(i, j int) bool {
	if h[i].reward != h[j].reward {
		return h[i].reward > h[j].reward
	}
	return h[i].seq < h[j].seq // older first on ties
}
func (h maxHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].maxIdx = i
	h[j].maxIdx = j
}
func (h *maxHeap) Push(x any) {
	e := x.(*entry)
	e.maxIdx = len(*h)
	*h = append(*h, e)
}
func (h *maxHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	e.maxIdx = -1
	return e
}

type minHeap []*entry

func (h minHeap) Len() int { return len(h) }
func (h minHeap) Less(i, j int) bool {
	if h[i].reward != h[j].reward {
		return h[i].reward < h[j].reward
	}
	// Evict newer first when rewards tie so older buffered txs are retained.
	return h[i].seq > h[j].seq
}
func (h minHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].minIdx = i
	h[j].minIdx = j
}
func (h *minHeap) Push(x any) {
	e := x.(*entry)
	e.minIdx = len(*h)
	*h = append(*h, e)
}
func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	e.minIdx = -1
	return e
}

// Buffer is a capacity-limited, reward-ordered transaction heap.
type Buffer struct {
	mu       sync.Mutex
	capacity int
	alive    int
	byHash   map[[32]byte]*entry
	max      maxHeap
	min      minHeap
}

func NewBuffer(capacity int) *Buffer {
	if capacity <= 0 {
		capacity = MaxBufferedTxns
	}
	return &Buffer{
		capacity: capacity,
		byHash:   make(map[[32]byte]*entry),
	}
}

func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.alive
}

func (b *Buffer) Capacity() int {
	if b == nil || b.capacity <= 0 {
		return MaxBufferedTxns
	}
	return b.capacity
}

// InsertResult reports how an Insert attempt resolved.
type InsertResult int

const (
	InsertAccepted InsertResult = iota
	InsertDuplicate
	InsertRejectedCapacity
)

// Insert adds e when not a duplicate. At capacity, the lowest-reward entry is
// evicted if e has a strictly higher reward; otherwise e is rejected.
func (b *Buffer) Insert(e *entry) (InsertResult, *entry) {
	if e == nil || e.tx == nil {
		return InsertRejectedCapacity, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.byHash[e.messageHash]; exists {
		return InsertDuplicate, nil
	}
	var evicted *entry
	if b.alive >= b.capacity {
		min := b.peekMinAliveLocked()
		if min == nil {
			return InsertRejectedCapacity, nil
		}
		if e.reward <= min.reward {
			return InsertRejectedCapacity, nil
		}
		evicted = b.popMinAliveLocked()
	}
	b.pushAliveLocked(e)
	return InsertAccepted, evicted
}

// PopMax removes and returns the highest-reward alive entry.
func (b *Buffer) PopMax() *entry {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.popMaxAliveLocked()
}

// Cleanup removes entries for which drop returns true. Returns the drop count.
func (b *Buffer) Cleanup(drop func(*entry) bool) int {
	if drop == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	var doomed []*entry
	for _, e := range b.byHash {
		if e.alive && drop(e) {
			doomed = append(doomed, e)
		}
	}
	for _, e := range doomed {
		b.killLocked(e)
	}
	b.drainDeadLocked()
	return len(doomed)
}

func (b *Buffer) pushAliveLocked(e *entry) {
	e.alive = true
	b.byHash[e.messageHash] = e
	heap.Push(&b.max, e)
	heap.Push(&b.min, e)
	b.alive++
}

func (b *Buffer) killLocked(e *entry) {
	if e == nil || !e.alive {
		return
	}
	e.alive = false
	delete(b.byHash, e.messageHash)
	b.alive--
}

func (b *Buffer) popMaxAliveLocked() *entry {
	for b.max.Len() > 0 {
		e := heap.Pop(&b.max).(*entry)
		if !e.alive {
			continue
		}
		b.killLocked(e)
		return e
	}
	return nil
}

func (b *Buffer) peekMinAliveLocked() *entry {
	for b.min.Len() > 0 {
		if b.min[0].alive {
			return b.min[0]
		}
		heap.Pop(&b.min)
	}
	return nil
}

func (b *Buffer) popMinAliveLocked() *entry {
	for b.min.Len() > 0 {
		e := heap.Pop(&b.min).(*entry)
		if !e.alive {
			continue
		}
		b.killLocked(e)
		return e
	}
	return nil
}

func (b *Buffer) drainDeadLocked() {
	for b.max.Len() > 0 {
		if b.max[0].alive {
			break
		}
		heap.Pop(&b.max)
	}
	for b.min.Len() > 0 {
		if b.min[0].alive {
			break
		}
		heap.Pop(&b.min)
	}
}
