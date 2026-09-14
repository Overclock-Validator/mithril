package scheduler

import (
	"container/heap"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/gagliardetto/solana-go"
)

// MaxBufferedTxns is the default cap on cross-slot buffered transactions.
const MaxBufferedTxns = 2 * 65536

// entry is one buffered, scored transaction.
type entry struct {
	tx       *solana.Transaction
	prepared *replay.PreparedTransaction
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

	alive bool
	// Indexes belong to Buffer.mu. Every buffered entry appears exactly once
	// in each heap; -1 denotes absence while an entry is owned by the consumer.
	maxIndex, minIndex int
}

type maxHeap []*entry

func (h maxHeap) Len() int { return len(h) }
func (h maxHeap) Less(i, j int) bool {
	return higherPriority(h[i], h[j])
}
func (h maxHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].maxIndex, h[j].maxIndex = i, j
}
func (h *maxHeap) Push(x any) {
	e := x.(*entry)
	e.maxIndex = len(*h)
	*h = append(*h, e)
}
func (h *maxHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	e.maxIndex = -1
	old[n-1] = nil
	*h = old[:n-1]
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
	h[i].minIndex, h[j].minIndex = i, j
}
func (h *minHeap) Push(x any) {
	e := x.(*entry)
	e.minIndex = len(*h)
	*h = append(*h, e)
}
func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	e.minIndex = -1
	old[n-1] = nil
	*h = old[:n-1]
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

	dropped := 0
	for _, e := range b.byHash {
		if drop(e) {
			b.killLocked(e)
			dropped++
		}
	}
	return dropped
}

func (b *Buffer) pushAliveLocked(e *entry) {
	e.alive = true
	e.maxIndex, e.minIndex = -1, -1
	b.byHash[e.messageHash] = e
	heap.Push(&b.max, e)
	heap.Push(&b.min, e)
	b.alive++
}

func (b *Buffer) killLocked(e *entry) {
	if e == nil || !e.alive {
		return
	}
	if e.maxIndex >= 0 {
		heap.Remove(&b.max, e.maxIndex)
	}
	if e.minIndex >= 0 {
		heap.Remove(&b.min, e.minIndex)
	}
	e.alive = false
	delete(b.byHash, e.messageHash)
	b.alive--
}

func (b *Buffer) popMaxAliveLocked() *entry {
	if b.max.Len() > 0 {
		e := b.max.popEntry()
		b.killLocked(e)
		return e
	}
	return nil
}

// popEntry moves the winning child into the hole at each level. This avoids
// interface dispatch and swapping two entries at every level of a large queue.
// Ordering is identical to maxHeap.Less, including FIFO for equal rewards.
func (h *maxHeap) popEntry() *entry {
	nodes := *h
	root := nodes[0]
	root.maxIndex = -1
	last := nodes[len(nodes)-1]
	nodes[len(nodes)-1] = nil
	nodes = nodes[:len(nodes)-1]
	if len(nodes) > 0 {
		i := 0
		for {
			child := 2*i + 1
			if child >= len(nodes) {
				break
			}
			if child+1 < len(nodes) && higherPriority(nodes[child+1], nodes[child]) {
				child++
			}
			if !higherPriority(nodes[child], last) {
				break
			}
			nodes[i] = nodes[child]
			nodes[i].maxIndex = i
			i = child
		}
		nodes[i] = last
		last.maxIndex = i
	}
	*h = nodes
	return root
}

func higherPriority(a, b *entry) bool {
	if a.reward != b.reward {
		return a.reward > b.reward
	}
	return a.seq < b.seq
}

func (b *Buffer) peekMinAliveLocked() *entry {
	if b.min.Len() > 0 {
		return b.min[0]
	}
	return nil
}

func (b *Buffer) popMinAliveLocked() *entry {
	if b.min.Len() > 0 {
		e := heap.Pop(&b.min).(*entry)
		b.killLocked(e)
		return e
	}
	return nil
}
