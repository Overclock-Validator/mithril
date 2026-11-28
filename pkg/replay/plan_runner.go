package replay

import (
	"encoding/binary"
	"os"
	"runtime"
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

// Idle workers park, and waking one (an OS thread, possibly on a core in a
// deep sleep state) can take tens to hundreds of microseconds. With
// MITHRIL_WORKER_SPIN_US > 0, up to MITHRIL_WORKER_SPINNERS idle workers poll
// for that long before parking, so newly ready work usually finds one awake.
var (
	workerSpin     = time.Duration(specShadowEnvInt("MITHRIL_WORKER_SPIN_US", 0)) * time.Microsecond
	workerSpinners = int32(specShadowEnvInt("MITHRIL_WORKER_SPINNERS", 2))
)

// Critical-path-first scheduling (direct dispatch), on unless
// MITHRIL_READY_PRIORITY=0: each transaction's height is the length of the
// longest chain of dependents below it. Idle workers take queued transactions
// highest first instead of in arrival order, and a committing worker's
// continuation is its highest newly ready dependent, so a transaction on the
// block's longest chain does not wait behind ready work that nothing depends
// on. Priority orders the queue and each continuation choice, not the whole
// block (see direct dispatch above).
var readyPriority = os.Getenv("MITHRIL_READY_PRIORITY") != "0"

// MITHRIL_AB_PARITY=1 (diagnostic): direct dispatch, priority scheduling and
// work-ahead apply only to even slots, so one live run compares them with plain
// replay (odd slots) under the same conditions.
var abParity = os.Getenv("MITHRIL_AB_PARITY") == "1"

func schedulingOnFor(slot uint64) bool {
	return !abParity || slot%2 == 0
}

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

// With MITHRIL_READY_PRIORITY_CU=1, heights weigh each transaction by its
// requested compute (requestedKCU) instead of counting transactions, so one
// expensive transaction outranks a long run of cheap ones.
var readyPriorityCU = os.Getenv("MITHRIL_READY_PRIORITY_CU") == "1"

// planSchedule chooses how runDependencyPlan orders ready work.
type planSchedule struct {
	priority bool               // highest first (readyHeap)
	weight   func(tx int) int32 // height weights; nil counts transactions
}

// scheduleFor is the plan schedule for txs under the current settings.
func scheduleFor(txs []*solana.Transaction, slot uint64) planSchedule {
	s := planSchedule{priority: readyPriority && schedulingOnFor(slot)}
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
// the commit. onReady, when set, observes each transaction becoming ready.
// With work-ahead, idle workers run early executions and a side goroutine
// keeps the offer bookkeeping from worker events. It returns the time from
// its start until the final transaction became ready, the counterpart of the
// dispatcher's DependencyPlannerDispatch (which ends once the final wave is
// enqueued, before it executes).
func runDependencyPlan(plan *dependencyPlan, workers int, w *workAhead, sched planSchedule, onReady func(tx int), process func(worker, tx int)) time.Duration {
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
	var heights []int32
	if sched.priority || (w != nil && workAheadQueuePriority) {
		heights = planHeights(plan, sched.weight)
	}
	var heap *readyHeap
	if sched.priority {
		heap = &readyHeap{height: heights, items: make([]int32, 0, n)}
	}
	if w != nil && workAheadQueuePriority {
		w.aheadHeap = &readyHeap{height: heights, items: make([]int32, 0, n)}
	}
	var pressure *workAheadPressure
	if w != nil {
		pressure = w.pressure
	}
	pressure.begin(n)
	// enqueue makes tx available to idle workers; take turns a received value
	// into the transaction to run.
	enqueue := func(tx int) {
		if heap != nil {
			heap.push(int32(tx))
		}
		// recorded before the send, so a worker taking tx finds its queue time
		if pressure != nil {
			pressure.queued(tx, len(ready)+1, w.running.Load())
		}
		ready <- tx
	}
	take := func(received int) int {
		tx := received
		if heap != nil {
			tx = int(heap.pop())
		}
		pressure.taken(tx)
		return tx
	}
	var left atomic.Int64
	left.Store(int64(n))

	var events chan planEvent
	plannerDone := make(chan struct{})
	if w != nil {
		events = make(chan planEvent, 4*n)
		w.events = events
		go func() {
			defer close(plannerDone)
			w.plan(plan, events)
		}()
	} else {
		close(plannerDone)
	}
	emit := func(kind uint8, tx int) {
		if events != nil {
			select {
			case events <- planEvent{kind: kind, tx: int32(tx)}:
			default:
			}
		}
	}

	for tx, degree := range plan.inDegree {
		remaining[tx].Store(int32(degree))
	}
	initial := int64(0)
	for tx, degree := range plan.inDegree {
		if degree == 0 {
			if onReady != nil {
				onReady(tx)
			}
			emit(planEventDispatched, tx)
			enqueue(tx)
			initial++
		}
	}
	markReady(initial)

	var spinning atomic.Int32
	// spin polls for work for up to workerSpin before the caller blocks.
	spin := func() (idx int, ahead, ok, got bool) {
		if workerSpin <= 0 || spinning.Add(1) > workerSpinners {
			if workerSpin > 0 {
				spinning.Add(-1)
			}
			return 0, false, false, false
		}
		defer spinning.Add(-1)
		start := time.Now()
		for i := 0; ; i++ {
			if idx, ahead, ok, got = w.tryNext(ready); got {
				return idx, ahead, ok, true
			}
			if i%32 == 31 && time.Since(start) > workerSpin {
				return 0, false, false, false
			}
			runtime.Gosched()
		}
	}

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
					pressure.idleBegin()
					idx, ahead, ok, got := spin()
					if !got {
						idx, ahead, ok = w.next(ready)
					}
					pressure.idleEnd()
					if !ok {
						return
					}
					if ahead {
						w.run(idx)
						continue
					}
					tx = take(idx)
				}
				process(worker, tx)
				emit(planEventCompleted, tx)
				for _, dependent := range plan.dependents[plan.offsets[tx]:plan.offsets[tx+1]] {
					if remaining[dependent].Add(-1) != 0 {
						continue
					}
					markReady(1)
					if onReady != nil {
						onReady(int(dependent))
					}
					emit(planEventDispatched, int(dependent))
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
					if pressure != nil {
						pressure.lastCommit(w.running.Load())
					}
					close(ready)
				}
			}
		}()
	}
	wg.Wait()
	pressure.drained()
	if events != nil {
		close(events)
	}
	<-plannerDone
	return time.Duration(dispatched.Load())
}

const (
	planEventDispatched = iota
	planEventCompleted
	planEventStarted
)

type planEvent struct {
	kind uint8
	tx   int32
}

// plan keeps the dispatcher's offer bookkeeping from worker events, off the
// critical path: a transaction is offered once every prerequisite has been
// dispatched (or, with early offers, has started executing early), and again
// as they commit, while at most maxUnfinished are uncommitted. Events can lag;
// offers are only advisory, and the read-set check at the transaction's turn
// decides adoption.
func (w *workAhead) plan(plan *dependencyPlan, events <-chan planEvent) {
	w.funnel.start(plan)
	defer w.funnel.close()
	w.offerDone = make(chan struct{})
	go func() {
		defer close(w.offerDone)
		if workAheadFailDepth > 0 && workAheadPredict == "account" {
			w.buildLastWriters()
		}
		for o := range w.offers {
			w.offer(int(o.tx), o.unfinished[:o.n])
		}
	}()
	defer close(w.offers)

	n := len(plan.inDegree)
	maxUnfinished := min(workAheadMaxReaders, workAheadOfferMax)
	uncommitted := append([]uint32(nil), plan.inDegree...)
	pending := append([]uint32(nil), plan.inDegree...)
	completed := make([]bool, n)
	dispatched := make([]bool, n)
	prereqOffsets := make([]uint32, n+1)
	for tx, degree := range plan.inDegree {
		prereqOffsets[tx+1] = prereqOffsets[tx] + degree
	}
	prerequisites := make([]uint32, prereqOffsets[n])
	fill := append([]uint32(nil), prereqOffsets[:n]...)
	for tx := range n {
		for _, dependent := range plan.dependents[plan.offsets[tx]:plan.offsets[tx+1]] {
			prerequisites[fill[dependent]] = uint32(tx)
			fill[dependent]++
		}
	}
	tryOffer := func(tx uint32) {
		if int(uncommitted[tx]) > maxUnfinished || int(tx) >= len(w.execute) || !w.execute[tx] {
			return
		}
		o := workAheadOffer{tx: int32(tx)}
		for _, p := range prerequisites[prereqOffsets[tx]:prereqOffsets[tx+1]] {
			if !completed[p] {
				o.unfinished[o.n] = p
				o.n++
			}
		}
		if o.n == 0 {
			return
		}
		select {
		case w.offers <- o:
		default:
		}
	}
	markDispatched := func(tx uint32) {
		if dispatched[tx] {
			return
		}
		dispatched[tx] = true
		for _, dependent := range plan.dependents[plan.offsets[tx]:plan.offsets[tx+1]] {
			pending[dependent]--
			if pending[dependent] == 0 && uncommitted[dependent] > 0 {
				tryOffer(dependent)
			}
		}
	}
	for e := range events {
		tx := uint32(e.tx)
		switch e.kind {
		case planEventDispatched:
			markDispatched(tx)
		case planEventStarted:
			if workAheadEarlyOffer {
				markDispatched(tx)
			}
		case planEventCompleted:
			if workAheadPredict == "program" {
				w.observe(int(tx))
			}
			markDispatched(tx)
			completed[tx] = true
			for _, dependent := range plan.dependents[plan.offsets[tx]:plan.offsets[tx+1]] {
				uncommitted[dependent]--
				if uncommitted[dependent] > 0 && pending[dependent] == 0 {
					tryOffer(dependent)
				}
			}
		}
	}
}
