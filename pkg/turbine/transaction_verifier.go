package turbine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sigverify"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

var (
	errNilTransaction            = fmt.Errorf("nil transaction")
	errTransactionVerifierClosed = fmt.Errorf("transaction verifier closed")
)

// A job is formed before admission, from transactions which are already
// available. Workers never wait for more transactions to fill a vector group.
type transactionVerifyJob struct {
	txs   []*solana.Transaction
	errs  []error
	start int
	done  chan<- *transactionVerifyJob
}

// transactionVerification owns an asynchronous request until done closes.
// Transactions submitted to it must remain immutable until wait returns.
type transactionVerification struct {
	done       chan struct{}
	cancel     context.CancelFunc
	index      int
	err        error
	finishedAt time.Time
}

func (r *transactionVerification) wait() (int, error) {
	return r.waitContext(context.Background())
}

// waitContext cancels further admission when ctx is canceled, but joins every
// admitted job before returning. The caller may then safely release or mutate
// the transaction objects, including their backing message byte slices.
func (r *transactionVerification) waitContext(ctx context.Context) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-r.done:
	case <-ctx.Done():
		r.cancel()
		<-r.done
		return -1, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	return r.index, r.err
}

type transactionVerifier struct {
	jobs        chan *transactionVerifyJob
	verify      func(*solana.Transaction) error
	workers     int
	batchTarget int
	// Each accepted request owns at most workers outstanding groups. The
	// admission semaphore also bounds asynchronous request goroutines; callers
	// apply backpressure before handing off another decoded component.
	requests chan struct{}
	request  sync.WaitGroup
	mu       sync.Mutex
	closed   bool
	stopped  chan struct{}
	close    sync.Once
	worker   sync.WaitGroup
}

func newTransactionVerifier(workers, queueDepth int, verify func(*solana.Transaction) error) *transactionVerifier {
	return newTransactionVerifierWithBatchTarget(workers, queueDepth, sigverify.BatchTarget, verify)
}

// queueDepth is a transaction budget, rounded up to whole groups. batchTarget
// counts signature lanes: multi-signature transactions stay indivisible and
// may exceed the target. Four/eight targets can be compared without changing
// the admission or cancellation policy.
func newTransactionVerifierWithBatchTarget(workers, queueDepth, batchTarget int, verify func(*solana.Transaction) error) *transactionVerifier {
	workers = max(1, workers)
	batchTarget = max(1, min(batchTarget, sigverify.BatchTarget))
	queueGroups := max(1, (queueDepth+batchTarget-1)/batchTarget)
	v := &transactionVerifier{
		jobs:        make(chan *transactionVerifyJob, queueGroups),
		verify:      verify,
		workers:     workers,
		batchTarget: batchTarget,
		requests:    make(chan struct{}, 2*workers),
		stopped:     make(chan struct{}),
	}
	v.worker.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer v.worker.Done()
			var batch txverify.BatchVerifier
			for job := range v.jobs {
				v.verifyGroup(job, &batch)
			}
		}()
	}
	return v
}

// verifyGroup releases its job even if signature verification panics. The
// request's bounded completion channel always has room for every pending job.
func (v *transactionVerifier) verifyGroup(job *transactionVerifyJob, batch *txverify.BatchVerifier) {
	defer func() { job.done <- job }()
	if v.verify != nil {
		for i, tx := range job.txs {
			if tx == nil {
				job.errs[i] = errNilTransaction
			} else {
				job.errs[i] = verifyTransactionSafely(v.verify, tx)
			}
		}
		return
	}
	verifyBatchSafely(batch, job.txs, job.errs)
}

// submitTransactions admits one immutable decoded component or complete block.
// Admission is bounded and may block: call it from a decode/completion worker,
// never the UDP reader or while holding the assembler mutex. Cancellation of
// ctx stops further groups but still joins every admitted group.
func (v *transactionVerifier) submitTransactions(ctx context.Context, txs []*solana.Transaction) (*transactionVerification, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case v.requests <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-v.stopped:
		return nil, errTransactionVerifierClosed
	}

	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		<-v.requests
		return nil, errTransactionVerifierClosed
	}
	v.request.Add(1)
	v.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	r := &transactionVerification{done: make(chan struct{}), cancel: cancel, index: -1}
	go func() {
		defer v.request.Done()
		defer func() { <-v.requests }()
		defer cancel()
		r.index, r.err = v.verifyTransactions(ctx, txs)
		r.finishedAt = time.Now()
		close(r.done)
	}()
	return r, nil
}

// verifyTransactions keeps a rolling window instead of waiting for an entire
// worker wave. A slow group cannot idle workers whose earlier groups finished.
// One caller can queue at most workers groups, so a large catch-up block cannot
// put all its transactions ahead of a newly available component.
func (v *transactionVerifier) verifyTransactions(ctx context.Context, txs []*solana.Transaction) (int, error) {
	if len(txs) == 0 {
		return -1, ctx.Err()
	}
	window := min(v.workers, len(txs))
	completed := make(chan *transactionVerifyJob, window)
	groups := make([]transactionVerifyJob, window)
	errs := make([]error, window*v.batchTarget)
	free := make([]*transactionVerifyJob, window)
	for i := range groups {
		groups[i].errs = errs[i*v.batchTarget : (i+1)*v.batchTarget]
		groups[i].done = completed
		free[i] = &groups[i]
	}

	nextIndex, active := 0, 0
	failureIndex := -1
	var failure error
	var pending *transactionVerifyJob
	ctxDone := ctx.Done()
	stopped := false
	for active > 0 || (!stopped && nextIndex < len(txs)) {
		if !stopped && ctx.Err() != nil {
			stopped = true
			ctxDone = nil
		}
		if stopped && active == 0 {
			break
		}
		if !stopped && pending == nil && nextIndex < len(txs) && len(free) > 0 {
			pending = free[len(free)-1]
			free = free[:len(free)-1]
			end := transactionVerifyGroupEnd(txs, nextIndex, v.batchTarget)
			pending.start = nextIndex
			pending.txs = txs[nextIndex:end]
			pending.errs = pending.errs[:end-nextIndex]
			clear(pending.errs)
		}
		var admission chan *transactionVerifyJob
		if !stopped && pending != nil {
			admission = v.jobs
		}
		select {
		case admission <- pending:
			nextIndex += len(pending.txs)
			active++
			pending = nil
		case job := <-completed:
			active--
			for i, err := range job.errs {
				if err != nil && (failureIndex < 0 || job.start+i < failureIndex) {
					failureIndex, failure = job.start+i, err
					stopped = true
				}
			}
			job.txs = nil
			clear(job.errs)
			free = append(free, job)
		case <-ctxDone:
			stopped = true
			ctxDone = nil
		}
	}
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	return failureIndex, failure
}

func transactionVerifyGroupEnd(txs []*solana.Transaction, start, target int) int {
	end, signatures := start, 0
	for end < len(txs) && end-start < target {
		count := 1
		if txs[end] != nil {
			count = max(1, len(txs[end].Signatures))
		}
		if end > start && signatures+count > target {
			break
		}
		signatures += count
		end++
		if signatures >= target {
			break
		}
	}
	return end
}

// verifyBatchSafely mirrors verifyTransactionSafely: a panic in the verifier
// becomes an error for every transaction in the group rather than taking down
// the process. Attributing it to all of them is deliberate — a panic gives no
// evidence about which transaction caused it, and silently passing the others
// would admit unverified transactions.
func verifyBatchSafely(batch *txverify.BatchVerifier, txs []*solana.Transaction, errs []error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			for i := range errs {
				errs[i] = fmt.Errorf("signature verifier panic: %v", recovered)
			}
		}
	}()
	batch.Verify(txs, errs)
}

func (v *transactionVerifier) closeAndWait() {
	if v == nil {
		return
	}
	v.close.Do(func() {
		v.mu.Lock()
		v.closed = true
		close(v.stopped)
		v.mu.Unlock()
		v.request.Wait()
		close(v.jobs)
		v.worker.Wait()
	})
}

func verifyTransactionSafely(verify func(*solana.Transaction) error, tx *solana.Transaction) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("signature verifier panic: %v", recovered)
		}
	}()
	return verify(tx)
}

func (v *transactionVerifier) verifyBlock(blk *block.Block) error {
	return v.verifyBlockContext(context.Background(), blk)
}

func (v *transactionVerifier) verifyBlockContext(ctx context.Context, blk *block.Block) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if blk == nil || len(blk.Transactions) == 0 {
		return nil
	}
	request, err := v.submitTransactions(ctx, blk.Transactions)
	if err != nil {
		return err
	}
	index, err := request.wait()
	if err != nil && index >= 0 {
		return formatTransactionVerificationError(blk, index, err)
	}
	return err
}

func formatTransactionVerificationError(blk *block.Block, txIdx int, err error) error {
	txSig := "<missing>"
	version := solana.MessageVersionLegacy
	if blk != nil && txIdx >= 0 && txIdx < len(blk.Transactions) && blk.Transactions[txIdx] == nil {
		return fmt.Errorf("slot %d transaction %d is nil", blk.Slot, txIdx)
	}
	if blk != nil && txIdx >= 0 && txIdx < len(blk.Transactions) && blk.Transactions[txIdx] != nil {
		tx := blk.Transactions[txIdx]
		version = tx.Message.GetVersion()
		if len(tx.Signatures) > 0 {
			txSig = tx.Signatures[0].String()
		}
	}
	return fmt.Errorf("slot %d transaction %d %s version=%d failed signature verification: %w", blk.Slot, txIdx, txSig, version, err)
}

var (
	defaultTransactionVerifierOnce sync.Once
	defaultTransactionVerifier     *transactionVerifier
)

func getDefaultTransactionVerifier() *transactionVerifier {
	defaultTransactionVerifierOnce.Do(func() {
		workers := sigverify.TransactionWorkers()
		target := sigverify.TransactionBatchTarget()
		defaultTransactionVerifier = newTransactionVerifierWithBatchTarget(workers, 2*workers*target, target, nil)
	})
	return defaultTransactionVerifier
}

func validateBlockTransactionsContext(ctx context.Context, blk *block.Block) error {
	return getDefaultTransactionVerifier().verifyBlockContext(ctx, blk)
}

func validateBlockTransactions(blk *block.Block) error {
	return validateBlockTransactionsContext(context.Background(), blk)
}
