package turbine

import (
	"context"
	"runtime"
	"sync"
)

const slotCompletionQueueDepth = 16

type queuedSlotCompletion struct {
	work     *slotCompletionWork
	hydrated bool
	ctx      context.Context
}

// slotCompletionPool keeps the single live UDP reader out of block decode and
// Ed25519 verification. The queue is deliberately bounded: saturation becomes
// packet-reader backpressure, not unbounded retained slot memory.
type slotCompletionPool struct {
	assembler    *SlotAssembler
	resetGate    *sync.RWMutex
	onBlockReady func(uint64)
	jobs         chan queuedSlotCompletion
	results      chan slotCompletionResult
	workers      sync.WaitGroup
	stopOnce     sync.Once
}

func defaultSlotCompletionWorkers() int {
	return min(8, max(2, runtime.GOMAXPROCS(0)/4))
}

func newSlotCompletionPool(assembler *SlotAssembler, resetGate *sync.RWMutex, onBlockReady func(uint64), workerCount, queueDepth int) *slotCompletionPool {
	if workerCount < 1 {
		workerCount = 1
	}
	if queueDepth < 1 {
		queueDepth = 1
	}
	p := &slotCompletionPool{
		assembler:    assembler,
		resetGate:    resetGate,
		onBlockReady: onBlockReady,
		jobs:         make(chan queuedSlotCompletion, queueDepth),
		results:      make(chan slotCompletionResult, queueDepth+workerCount),
	}
	p.workers.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer p.workers.Done()
			for queued := range p.jobs {
				ctx := queued.ctx
				if ctx == nil {
					ctx = context.Background()
				}
				if ctx.Err() != nil {
					p.assembler.abortCompletion(queued.work)
					continue
				}
				processed := p.assembler.processCompletion(ctx, queued.work)
				if processed.canceled || ctx.Err() != nil {
					p.assembler.abortCompletion(queued.work)
					continue
				}
				if p.resetGate != nil {
					p.resetGate.RLock()
				}
				if ctx.Err() != nil {
					if p.resetGate != nil {
						p.resetGate.RUnlock()
					}
					p.assembler.abortCompletion(queued.work)
					continue
				}
				blk, err := p.assembler.finalizeCompletion(queued.work, processed)
				pending := false
				if blk != nil && err == nil && p.onBlockReady != nil {
					p.onBlockReady(blk.Slot)
					pending = true
				}
				if p.resetGate != nil {
					p.resetGate.RUnlock()
				}
				p.results <- slotCompletionResult{
					generation: StreamGeneration{slot: queued.work.state.slot, state: queued.work.state},
					block:      blk,
					err:        err,
					hydrated:   queued.hydrated,
					pending:    pending,
				}
			}
		}()
	}
	return p
}

func (p *slotCompletionPool) enqueue(ctx context.Context, work *slotCompletionWork, hydrated bool) bool {
	if p == nil || work == nil {
		return true
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case p.jobs <- queuedSlotCompletion{work: work, hydrated: hydrated, ctx: ctx}:
		return true
	case <-ctx.Done():
		p.assembler.abortCompletion(work)
		return false
	}
}

// closeAndWait is called only after every packet reader and the hydrator have
// stopped submitting. Results must be drained concurrently while this joins.
func (p *slotCompletionPool) closeAndWait() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() {
		close(p.jobs)
		p.workers.Wait()
		close(p.results)
	})
}
