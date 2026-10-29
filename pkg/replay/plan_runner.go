package replay

import (
	"encoding/binary"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// Direct dispatch, on unless MITHRIL_DIRECT_DISPATCH=0: the worker that
// commits a transaction releases its dependents itself instead of reporting to
// a dispatcher goroutine. Each transaction keeps an atomic count of
// uncommitted prerequisites; the committing worker decrements its dependents'
// counts after the commit, keeps one newly ready dependent to run next itself
// (its continuation), and queues any others for idle workers. This removes two
// goroutine hand-offs (worker to dispatcher, dispatcher to worker) from every
// link of a dependency chain. Dependents are still released only after the
// commit. With =0 the dispatcher goroutine schedules every transaction in
// arrival order, as before.
//
// A continuation runs without consulting the shared queue, so the order is not
// a global priority order: a worker can continue its own chain while
// higher-priority work waits in the queue for another worker. With
// MITHRIL_READY_PRIORITY=0 the continuation is the first dependent released
// and the queue is first in, first out; that is still not the dispatcher's
// order, which only MITHRIL_DIRECT_DISPATCH=0 restores.
var directDispatch = os.Getenv("MITHRIL_DIRECT_DISPATCH") != "0"

// Critical-path-first scheduling (direct dispatch), on unless
// MITHRIL_READY_PRIORITY=0: each transaction's height is the length of the
// longest chain of dependents below it. Idle workers take queued transactions
// highest first instead of in arrival order, and a committing worker's
// continuation is its highest newly ready dependent, so a transaction on the
// block's longest chain does not wait behind ready work that nothing depends
// on. Priority orders the queue and each continuation choice, not the whole
// block (see direct dispatch above).
var readyPriority = os.Getenv("MITHRIL_READY_PRIORITY") != "0"

// With MITHRIL_READY_PRIORITY_CU=1, heights weigh each transaction by its
// requested compute (requestedKCU) instead of counting transactions, so one
// expensive transaction outranks a long run of cheap ones.
var readyPriorityCU = os.Getenv("MITHRIL_READY_PRIORITY_CU") == "1"

// readyHeap orders ready transactions by height, then block order. Each push
// is paired with one token on the ready channel; a worker that receives a token
// pops the current best transaction.
type readyHeap struct {
	mu     sync.Mutex
	height []int32
	items  []int32
}

func (h *readyHeap) less(a, b int32) bool {
	if h.height[a] != h.height[b] {
		return h.height[a] > h.height[b]
	}
	return a < b
}

func (h *readyHeap) push(tx int32) {
	h.mu.Lock()
	h.items = append(h.items, tx)
	for i := len(h.items) - 1; i > 0; {
		parent := (i - 1) / 2
		if !h.less(h.items[i], h.items[parent]) {
			break
		}
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		i = parent
	}
	h.mu.Unlock()
}

func (h *readyHeap) pop() int32 {
	h.mu.Lock()
	top := h.items[0]
	last := len(h.items) - 1
	h.items[0] = h.items[last]
	h.items = h.items[:last]
	for i := 0; ; {
		best, l, r := i, 2*i+1, 2*i+2
		if l < last && h.less(h.items[l], h.items[best]) {
			best = l
		}
		if r < last && h.less(h.items[r], h.items[best]) {
			best = r
		}
		if best == i {
			break
		}
		h.items[i], h.items[best] = h.items[best], h.items[i]
		i = best
	}
	h.mu.Unlock()
	return top
}

// planHeights returns, for each transaction, its weight plus the heaviest
// chain of dependents below it (weight 1 per transaction when weight is nil).
// Dependents always come later in the block.
func planHeights(plan *dependencyPlan, weight func(tx int) int32) []int32 {
	n := len(plan.inDegree)
	height := make([]int32, n)
	for tx := n - 1; tx >= 0; tx-- {
		var best int32
		for _, dependent := range plan.dependents[plan.offsets[tx]:plan.offsets[tx+1]] {
			best = max(best, height[dependent])
		}
		w := int32(1)
		if weight != nil {
			w = weight(tx)
		}
		height[tx] = best + w
	}
	return height
}

// planSchedule chooses how runDependencyPlan orders ready work.
type planSchedule struct {
	priority bool               // highest first (readyHeap)
	weight   func(tx int) int32 // height weights; nil counts transactions
}

// scheduleFor is the plan schedule for txs under the current settings.
func scheduleFor(txs []*solana.Transaction) planSchedule {
	s := planSchedule{priority: readyPriority}
	if readyPriorityCU {
		s.weight = func(tx int) int32 { return requestedKCU(txs[tx]) }
	}
	return s
}

// requestedKCU estimates a transaction's requested compute in thousands of CU
// from its message alone: its SetComputeUnitLimit instruction, else the
// default of 200k per other instruction, capped at 1.4M. It only weighs
// scheduling, so it parses the instruction bytes directly instead of running
// the full compute-budget validation.
func requestedKCU(tx *solana.Transaction) int32 {
	keys := tx.Message.AccountKeys
	limit, others := int64(-1), int64(0)
	for _, instr := range tx.Message.Instructions {
		if int(instr.ProgramIDIndex) < len(keys) && keys[instr.ProgramIDIndex] == addresses.ComputeBudgetProgramAddr {
			if len(instr.Data) >= 5 && instr.Data[0] == sealevel.ComputeBudgetInstrTypeSetComputeUnitLimit {
				limit = int64(binary.LittleEndian.Uint32(instr.Data[1:5]))
			}
			continue
		}
		others++
	}
	if limit < 0 {
		limit = 200_000 * others
	}
	limit = min(limit, 1_400_000)
	return int32(limit/1000) + 1
}

// runDependencyPlan executes every transaction of plan on the given number of
// workers. process(worker, tx) executes and commits tx; it returns only after
// the commit, and dependents are released only then. It returns the time from
// its start until the final transaction became ready, the counterpart of the
// dispatcher's DependencyPlannerDispatch (which ends once the final wave is
// enqueued, before it executes).
func runDependencyPlan(plan *dependencyPlan, workers int, sched planSchedule, process func(worker, tx int)) time.Duration {
	n := len(plan.inDegree)
	if n == 0 {
		return 0
	}
	start := time.Now()
	var readied atomic.Int64 // transactions made ready so far
	var dispatched atomic.Int64
	markReady := func(count int64) {
		if readied.Add(count) == int64(n) {
			dispatched.Store(int64(time.Since(start)))
		}
	}
	remaining := make([]atomic.Int32, n)
	ready := make(chan int, n)
	var heap *readyHeap
	if sched.priority {
		heap = &readyHeap{height: planHeights(plan, sched.weight), items: make([]int32, 0, n)}
	}
	// enqueue makes tx available to idle workers; take turns a received value
	// into the transaction to run.
	enqueue := func(tx int) {
		if heap != nil {
			heap.push(int32(tx))
		}
		ready <- tx
	}
	take := func(received int) int {
		if heap != nil {
			return int(heap.pop())
		}
		return received
	}
	var left atomic.Int64
	left.Store(int64(n))

	for tx, degree := range plan.inDegree {
		remaining[tx].Store(int32(degree))
	}
	initial := int64(0)
	for tx, degree := range plan.inDegree {
		if degree == 0 {
			enqueue(tx)
			initial++
		}
	}
	markReady(initial)

	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := range workers {
		go func() {
			defer wg.Done()
			next := -1
			for {
				tx := next
				next = -1
				if tx < 0 {
					received, ok := <-ready
					if !ok {
						return
					}
					tx = take(received)
				}
				process(worker, tx)
				for _, dependent := range plan.dependents[plan.offsets[tx]:plan.offsets[tx+1]] {
					if remaining[dependent].Add(-1) != 0 {
						continue
					}
					markReady(1)
					switch {
					case next < 0:
						next = int(dependent)
					case heap != nil && heap.height[dependent] > heap.height[next]:
						enqueue(next)
						next = int(dependent)
					default:
						enqueue(int(dependent))
					}
				}
				if left.Add(-1) == 0 {
					close(ready)
				}
			}
		}()
	}
	wg.Wait()
	return time.Duration(dispatched.Load())
}
