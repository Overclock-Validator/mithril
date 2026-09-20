package turbine

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
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
	start, end       uint32
	raw              []byte
	entries          []Entry
	parent           *AlpenglowParentInfo
	footer           *BlockFooter
	marker           bool
	traceDecodeStart int64
	traceDecodeEnd   int64
	parseDuration    time.Duration
	err              error
	ready            chan struct{}
	verification     *transactionVerification
	submittedAt      time.Time
	submitErr        error
}

type slotEntryPrefetch struct {
	pool             *entryPrefetchPool
	ctx              context.Context
	cancel           context.CancelFunc
	batches          map[uint32]*prefetchedShredBatch
	next             int
	queued, released bool
	queueDone        chan struct{} // closed after the queued/running token retires
	bytes            int
	budgetBlocked    bool
}

// All scheduling and accounting use assembler.mu. The packet reader only
// indexes complete data ranges and attempts a nonblocking, coalesced enqueue.
// Decoding and bounded verifier admission run on separate background workers.
type entryPrefetchPool struct {
	a                *SlotAssembler
	ctx              context.Context
	cancel           context.CancelFunc
	verifier         *transactionVerifier
	jobs             chan *slotState
	workers, cleanup sync.WaitGroup
	slots, bytes     int
	active           map[*slotState]struct{} // at most entryPrefetchSlots admitted generations
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
	if p == nil || p.closed || p.ctx.Err() != nil || s.streamCancelReason != "" {
		return
	}
	if s.batchIndex == nil && len(s.shreds) != 0 {
		s.batchIndex = newEntryBatchIndex()
		for _, sh := range s.shreds {
			s.discoverEntryBatch(sh)
		}
	}
	if s.prefetch == nil && len(s.completeBatches) > 0 && p.slots < entryPrefetchSlots {
		ctx, cancel := context.WithCancel(withEntryPipelineTrace(p.ctx, s.pipelineTrace))
		s.prefetch = &slotEntryPrefetch{pool: p, ctx: ctx, cancel: cancel, batches: make(map[uint32]*prefetchedShredBatch)}
		p.slots++
		if p.active == nil {
			p.active = make(map[*slotState]struct{})
		}
		p.active[s] = struct{}{}
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
		f.queueDone = make(chan struct{})
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
			close(f.queueDone)
			p.a.mu.Unlock()
			continue
		}
		f.budgetBlocked = false
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
				// Never publish a prefix with an unfillable hole. Completion
				// still decodes and verifies the entire valid block normally.
				s.streamCancelReason = "prefetch_batch_too_large"
				p.a.releasePrefetchLocked(s)
				break
			}
			if p.bytes+size > entryPrefetchBytes {
				f.budgetBlocked = true
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
			close(f.queueDone)
			p.a.mu.Unlock()
			continue
		}
		p.a.mu.Unlock()

		if s.pipelineTrace != nil {
			batch.traceDecodeStart = entryTraceNow()
		}
		raw := make([]byte, 0, rawSize)
		for _, sh := range shreds {
			raw = append(raw, sh.Data...)
		}
		decoded := decodeClosedShredBatch(raw, batch.start, batch.end)
		ready := batch.ready
		batch.raw, batch.entries = decoded.raw, decoded.entries
		batch.parent, batch.footer, batch.marker = decoded.parent, decoded.footer, decoded.marker
		batch.parseDuration, batch.err = decoded.parseDuration, decoded.err
		if s.pipelineTrace != nil {
			batch.traceDecodeEnd = entryTraceNow()
		}
		if batch.err == nil && !batch.marker && f.ctx.Err() == nil {
			txs := entryBatchTransactions(batch.entries)
			if len(txs) > 0 {
				batch.submittedAt = time.Now()
				batch.verification, batch.submitErr = p.verifier.submitPrefetchTransactions(f.ctx, txs)
			}
		}
		close(ready)
		p.a.mu.Lock()
		f.queued = false
		close(f.queueDone)
		if !f.released && p.a.slots[s.slot] == s {
			p.a.publishStreamBatchReadyLocked(s, batch)
		}
		p.enqueueLocked(s)
		p.a.mu.Unlock()
	}
}

func entryBatchTransactions(entries []Entry) []*solana.Transaction {
	count := 0
	for i := range entries {
		count += len(entries[i].Txns)
	}
	// Entries are already decoded: size this pointer view once instead of
	// repeatedly reallocating and copying it while preparing each component.
	txs := make([]*solana.Transaction, 0, count)
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
	reason := s.streamCancelReason
	if reason == "" {
		reason = "released"
	}
	a.publishStreamReleaseLocked(s, reason)
	p := f.pool
	queueDone := f.queueDone
	p.cleanup.Add(1)
	go func() {
		defer p.cleanup.Done()
		for _, b := range f.batches {
			<-b.ready
			if b.verification != nil {
				b.verification.wait()
			}
		}
		if queueDone != nil {
			<-queueDone
		}
		p.a.mu.Lock()
		p.slots--
		p.bytes -= f.bytes
		delete(p.active, s)
		p.retryBudgetBlockedLocked()
		p.a.mu.Unlock()
	}()
}

// Retry only admitted generations, oldest slot first, when readers release bytes.
// This avoids both waiting for another shred and scanning all retained slots.
func (p *entryPrefetchPool) retryBudgetBlockedLocked() {
	if p.closed || p.ctx.Err() != nil {
		return
	}
	waiting := make([]*slotState, 0, len(p.active))
	for s := range p.active {
		if s.prefetch.budgetBlocked && p.a.slots[s.slot] == s {
			waiting = append(waiting, s)
		}
	}
	sort.Slice(waiting, func(i, j int) bool { return waiting[i].slot < waiting[j].slot })
	for _, s := range waiting {
		p.enqueueLocked(s)
	}
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
				if s.streamCancelReason == "" {
					s.streamCancelReason = "shutdown"
				}
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
	return verifyDecodedEntryBatchesWithTimings(ctx, blk, batches, verifier, nil)
}

func verifyDecodedEntryBatchesWithTimings(ctx context.Context, blk *block.Block, batches []*prefetchedShredBatch, verifier *transactionVerifier, timings *entryDecodeTimings) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if blk == nil {
		return errors.New("verify decoded entries: nil block")
	}
	type pending struct {
		future *transactionVerification
		offset int
		count  int
	}
	var early []pending
	var missing []*solana.Transaction
	var indices []int
	offset := 0
	for _, b := range batches {
		if b == nil {
			return recoverEntryVerification(ctx, blk, batches, verifier, errors.New("nil retained entry batch"))
		}
		count := 0
		for _, e := range b.entries {
			count += len(e.Txns)
		}
		if count > len(blk.Transactions)-offset {
			return recoverEntryVerification(ctx, blk, batches, verifier, errors.New("entry transaction range exceeds final block"))
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
			early = append(early, pending{b.verification, offset, count})
		} else {
			missing = append(missing, blk.Transactions[offset:offset+count]...)
			for i := 0; i < count; i++ {
				indices = append(indices, offset+i)
			}
		}
		offset += count
	}
	if offset != len(blk.Transactions) {
		return recoverEntryVerification(ctx, blk, batches, verifier, errors.New("entry identity coverage mismatch"))
	}
	var fallback *transactionVerification
	var err error
	if len(missing) > 0 {
		fallback, err = verifier.submitTransactions(ctx, missing)
		if timings != nil {
			timings.traceFallback = fallback
		}
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
	if firstErr != nil {
		return firstErr
	}
	// Custom verification hooks do not produce trusted message identities.
	// Preserve their existing lazy preparation path (primarily test fixtures).
	if verifier.verify != nil {
		return nil
	}
	identities := make([]txverify.VerifiedMessageIdentity, len(blk.Transactions))
	for _, p := range early {
		if len(p.future.identities) != p.count || p.offset+len(p.future.identities) > len(identities) {
			return recoverEntryVerification(ctx, blk, batches, verifier, errors.New("entry identity range mismatch"))
		}
		copy(identities[p.offset:], p.future.identities)
	}
	if fallback != nil {
		if len(fallback.identities) != len(indices) {
			return recoverEntryVerification(ctx, blk, batches, verifier, errors.New("fallback identity coverage mismatch"))
		}
		for i, index := range indices {
			identities[index] = fallback.identities[i]
		}
	}
	if err := blk.CacheVerifiedTransactionMessageIdentities(identities); err != nil {
		return recoverEntryVerification(ctx, blk, batches, verifier, err)
	}
	return nil
}

// Prefetch metadata is an optimization, never a substitute for verifying the
// final block. Join old readers, then verify every final transaction afresh.
// This also repairs pointer/coverage mismatches without treating a successful
// verdict for another transaction as proof for this one. Normal signature
// failures above are still rejected directly. Failed recovery stays an error.
func recoverEntryVerification(ctx context.Context, blk *block.Block, batches []*prefetchedShredBatch, verifier *transactionVerifier, reason error) error {
	for _, batch := range batches {
		if batch != nil && batch.verification != nil {
			_, _ = batch.verification.waitContext(ctx)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	mlog.Log.Warnf("slot %d: discarded inconsistent entry verification metadata; re-verifying final transactions: %v", blk.Slot, reason)
	return verifier.verifyBlockContext(ctx, blk)
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
