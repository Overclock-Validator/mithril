package turbine

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

const (
	entryPrefetchSlots = 8
	// Covers a large block of maximum-size transactions while bounding the
	// extra retained encoded bytes across all in-flight generations.
	entryPrefetchBytes      = 64 << 20
	entryPrefetchBatchBytes = 1 << 20
)

type shredBatchRange struct{ start, end uint32 }

// A result belongs to one exact DATA_COMPLETE range in one slot generation.
// Fields are immutable after ready closes; signature readers own its decoded
// transactions until verification.done closes.
type prefetchedShredBatch struct {
	start, end    uint32
	raw           []byte
	entries       []Entry
	parent        *AlpenglowParentInfo
	footer        *BlockFooter
	marker        bool
	parseDuration time.Duration
	err           error
	ready         chan struct{}
	verification  *transactionVerification
	submittedAt   time.Time
	submitErr     error
}

type slotEntryPrefetch struct {
	pool             *entryPrefetchPool
	ctx              context.Context
	cancel           context.CancelFunc
	batches          map[uint32]*prefetchedShredBatch
	next             int
	queued, released bool
	bytes            int
}

// All scheduling and accounting use assembler.mu. The packet reader only
// advances a contiguous frontier and attempts a nonblocking, coalesced enqueue.
// Decoding and bounded verifier admission run on separate background workers.
type entryPrefetchPool struct {
	a                *SlotAssembler
	ctx              context.Context
	cancel           context.CancelFunc
	verifier         *transactionVerifier
	jobs             chan *slotState
	workers, cleanup sync.WaitGroup
	slots, bytes     int
	closed           bool
	close            sync.Once
}

func newEntryPrefetchPool(ctx context.Context, a *SlotAssembler, verifier *transactionVerifier) *entryPrefetchPool {
	ctx, cancel := context.WithCancel(ctx)
	p := &entryPrefetchPool{a: a, ctx: ctx, cancel: cancel, verifier: verifier, jobs: make(chan *slotState, entryPrefetchSlots)}
	a.mu.Lock()
	a.entryPrefetch = p
	a.mu.Unlock()
	p.workers.Add(2)
	for i := 0; i < 2; i++ {
		go p.run()
	}
	return p
}

func (a *SlotAssembler) prefetchEntriesLocked(s *slotState) {
	p := a.entryPrefetch
	if p == nil || p.closed || p.ctx.Err() != nil {
		return
	}
	// Each index is visited once, even when one batch spans many FEC sets or
	// arrives out of order. A gap never yields a speculative partial decode.
	for s.batchScan < maxDataShredsPerSlot {
		sh := s.shreds[s.batchScan]
		if sh == nil {
			break
		}
		if sh.DataComplete() {
			s.completeBatches = append(s.completeBatches, shredBatchRange{s.batchStart, s.batchScan})
			s.batchStart = s.batchScan + 1
		}
		s.batchScan++
	}
	if s.prefetch == nil && len(s.completeBatches) > 0 && p.slots < entryPrefetchSlots {
		ctx, cancel := context.WithCancel(p.ctx)
		s.prefetch = &slotEntryPrefetch{pool: p, ctx: ctx, cancel: cancel, batches: make(map[uint32]*prefetchedShredBatch)}
		p.slots++
	}
	p.enqueueLocked(s)
}

func (p *entryPrefetchPool) enqueueLocked(s *slotState) {
	f := s.prefetch
	if f == nil || f.pool != p || f.queued || f.released || s.completing || p.closed || f.next >= len(s.completeBatches) {
		return
	}
	select {
	case p.jobs <- s:
		f.queued = true
	default:
	}
}

func (p *entryPrefetchPool) run() {
	defer p.workers.Done()
	for s := range p.jobs {
		p.a.mu.Lock()
		f := s.prefetch
		if p.closed || f.released || f.ctx.Err() != nil || p.a.slots[s.slot] != s || s.completing {
			f.queued = false
			p.a.mu.Unlock()
			continue
		}
		var batch *prefetchedShredBatch
		var shreds []*Shred
		var rawSize int
		for f.next < len(s.completeBatches) {
			r := s.completeBatches[f.next]
			size := 0
			for i := r.start; i <= r.end; i++ {
				size += len(s.shreds[i].Data)
				if size > entryPrefetchBatchBytes {
					break
				}
			}
			if size > entryPrefetchBatchBytes {
				f.next++
				continue
			}
			if p.bytes+size > entryPrefetchBytes {
				break
			}
			f.next++
			p.bytes += size
			f.bytes += size
			rawSize = size
			batch = &prefetchedShredBatch{start: r.start, end: r.end, ready: make(chan struct{})}
			f.batches[r.start] = batch
			shreds = make([]*Shred, 0, r.end-r.start+1)
			for i := r.start; i <= r.end; i++ {
				shreds = append(shreds, s.shreds[i])
			}
			break
		}
		if batch == nil {
			f.queued = false
			p.a.mu.Unlock()
			continue
		}
		p.a.mu.Unlock()

		raw := make([]byte, 0, rawSize)
		for _, sh := range shreds {
			raw = append(raw, sh.Data...)
		}
		decoded := decodeClosedShredBatch(raw, batch.start, batch.end)
		ready := batch.ready
		batch.raw, batch.entries = decoded.raw, decoded.entries
		batch.parent, batch.footer, batch.marker = decoded.parent, decoded.footer, decoded.marker
		batch.parseDuration, batch.err = decoded.parseDuration, decoded.err
		if batch.err == nil && !batch.marker && f.ctx.Err() == nil {
			txs := entryBatchTransactions(batch.entries)
			if len(txs) > 0 {
				batch.submittedAt = time.Now()
				batch.verification, batch.submitErr = p.verifier.submitTransactions(f.ctx, txs)
			}
		}
		close(ready)
		p.a.mu.Lock()
		f.queued = false
		p.enqueueLocked(s)
		p.a.mu.Unlock()
	}
}

func entryBatchTransactions(entries []Entry) []*solana.Transaction {
	var txs []*solana.Transaction
	for i := range entries {
		for j := range entries[i].Txns {
			txs = append(txs, &entries[i].Txns[j])
		}
	}
	return txs
}

// Keep reservations until canceled readers have actually relinquished their
// buffers. Repeated resets cannot evade the memory or slot bounds.
func (a *SlotAssembler) releasePrefetchLocked(s *slotState) {
	if s == nil || s.prefetch == nil || s.prefetch.released {
		return
	}
	f := s.prefetch
	f.released = true
	f.cancel()
	p := f.pool
	p.cleanup.Add(1)
	go func() {
		defer p.cleanup.Done()
		for _, b := range f.batches {
			<-b.ready
			if b.verification != nil {
				b.verification.wait()
			}
		}
		p.a.mu.Lock()
		p.slots--
		p.bytes -= f.bytes
		p.a.mu.Unlock()
	}()
}

func (p *entryPrefetchPool) closeAndWait() {
	p.close.Do(func() {
		p.cancel()
		p.a.mu.Lock()
		p.closed = true
		if p.a.entryPrefetch == p {
			p.a.entryPrefetch = nil
		}
		for _, s := range p.a.slots {
			if s.prefetch != nil && s.prefetch.pool == p {
				p.a.releasePrefetchLocked(s)
			}
		}
		close(p.jobs)
		p.a.mu.Unlock()
		p.workers.Wait()
		p.cleanup.Wait()
	})
}

// Reuse only retained entry results. UpdateParent may intentionally discard an
// invalid optimistic prefix. Unprefetched transactions form one immediately
// available request, overlapping any early requests still running.
func verifyDecodedEntryBatches(ctx context.Context, blk *block.Block, batches []*prefetchedShredBatch, verifier *transactionVerifier) error {
	type pending struct {
		future *transactionVerification
		offset int
	}
	var early []pending
	var missing []*solana.Transaction
	var indices []int
	offset := 0
	for _, b := range batches {
		count := 0
		for _, e := range b.entries {
			count += len(e.Txns)
		}
		reusable := b.verification != nil
		if reusable {
			select {
			case <-b.verification.done:
				// Cancellation is not a signature verdict. A completion canceled
				// after preparation may be retried on this same slot generation.
				reusable = !errors.Is(b.verification.err, context.Canceled) && !errors.Is(b.verification.err, context.DeadlineExceeded)
			default:
			}
		}
		if reusable {
			early = append(early, pending{b.verification, offset})
		} else {
			missing = append(missing, blk.Transactions[offset:offset+count]...)
			for i := 0; i < count; i++ {
				indices = append(indices, offset+i)
			}
		}
		offset += count
	}
	var fallback *transactionVerification
	var err error
	if len(missing) > 0 {
		fallback, err = verifier.submitTransactions(ctx, missing)
	}
	firstIndex := len(blk.Transactions)
	firstErr := err
	for _, p := range early {
		i, e := p.future.waitContext(ctx)
		if e != nil && (firstErr == nil || (i >= 0 && p.offset+i < firstIndex)) {
			firstErr = e
			if i >= 0 {
				firstIndex = p.offset + i
			}
		}
	}
	if fallback != nil {
		i, e := fallback.waitContext(ctx)
		if e != nil && (firstErr == nil || (i >= 0 && indices[i] < firstIndex)) {
			firstErr = e
			if i >= 0 {
				firstIndex = indices[i]
			}
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if firstErr != nil && firstIndex < len(blk.Transactions) {
		return formatTransactionVerificationError(blk, firstIndex, firstErr)
	}
	return firstErr
}

func earlyEntryTimings(t *entryDecodeTimings, fullAt time.Time, timings *block.TurbineIngressTimings) {
	for _, b := range t.all {
		if b.ready == nil {
			continue
		}
		timings.EarlyTransactionParse += b.parseDuration
		if b.verification != nil {
			select {
			case <-b.verification.done:
				timings.EarlyTransactionSigverify += b.verification.finishedAt.Sub(b.submittedAt)
			default:
			}
		}
	}
	for _, b := range t.retained {
		if b.verification != nil {
			select {
			case <-b.verification.done:
				if b.verification.err == nil && !b.verification.finishedAt.After(fullAt) {
					for _, e := range b.entries {
						timings.EarlyVerifiedTransactions += uint64(len(e.Txns))
					}
				}
			default:
			}
		}
	}
}
