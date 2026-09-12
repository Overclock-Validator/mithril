package turbine

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sigverify"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

var errNilTransaction = fmt.Errorf("nil transaction")

type transactionVerifyJob struct {
	tx   *solana.Transaction
	err  *error
	done *sync.WaitGroup
}

type transactionVerifier struct {
	jobs    chan transactionVerifyJob
	verify  func(*solana.Transaction) error
	workers int
	// wave is how many transactions verifyBlockContext admits at once. It is
	// workers * sigverify.BatchTarget so each worker can actually accumulate a
	// full vector group rather than being handed one transaction at a time.
	wave  int
	close sync.Once

	worker sync.WaitGroup
}

func newTransactionVerifier(workers, queueDepth int, verify func(*solana.Transaction) error) *transactionVerifier {
	if workers < 1 {
		workers = 1
	}
	wave := workers * sigverify.BatchTarget
	if queueDepth < 1 {
		queueDepth = 1
	}
	v := &transactionVerifier{
		jobs:    make(chan transactionVerifyJob, queueDepth),
		verify:  verify,
		workers: workers,
		wave:    wave,
	}
	v.worker.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer v.worker.Done()
			// Worker-local scratch reused across groups.
			var (
				group []transactionVerifyJob
				scr   verifyScratch
			)
			for job := range v.jobs {
				group = sigverify.Drain(group, job, v.jobs,
					sigverify.FairShare(len(v.jobs), v.workers, sigverify.BatchTarget))
				v.verifyGroup(group, &scr)
				// Do not keep finished jobs reachable through the scratch.
				clear(group)
			}
		}()
	}
	return v
}

// verifyScratch is one worker's reusable buffers.
type verifyScratch struct {
	txs   []*solana.Transaction
	errs  []error
	batch txverify.BatchVerifier
}

// verifyGroup verifies a drained group and releases every job in it.
//
// Releasing happens in a defer covering the whole group, so no caller can be
// left waiting on a job that was drained into a batch which then failed —
// a stranded job would hang verifyBlockContext's done.Wait() forever.
func (v *transactionVerifier) verifyGroup(group []transactionVerifyJob, scr *verifyScratch) {
	defer func() {
		for _, job := range group {
			job.done.Done()
		}
	}()

	// An injected verifier is a per-transaction function and stays that way;
	// only the default path can batch. This seam is used by tests.
	if v.verify != nil {
		for _, job := range group {
			*job.err = verifyTransactionSafely(v.verify, job.tx)
		}
		return
	}

	scr.txs = scr.txs[:0]
	for _, job := range group {
		scr.txs = append(scr.txs, job.tx)
	}
	if cap(scr.errs) < len(scr.txs) {
		scr.errs = make([]error, len(scr.txs))
	}
	scr.errs = scr.errs[:len(scr.txs)]

	verifyBatchSafely(&scr.batch, scr.txs, scr.errs)

	for i, job := range group {
		*job.err = scr.errs[i]
	}
	clear(scr.txs)
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
	// Admit one worker-wave at a time. A monster block still occupies every
	// verifier lane, but cannot park tens of thousands of jobs ahead of a newly
	// completed small block in the shared bounded queue.
	for chunkStart := 0; chunkStart < len(blk.Transactions); chunkStart += v.wave {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunkEnd := min(chunkStart+v.wave, len(blk.Transactions))
		errs := make([]error, chunkEnd-chunkStart)
		var done sync.WaitGroup
		for txIdx := chunkStart; txIdx < chunkEnd; txIdx++ {
			if err := ctx.Err(); err != nil {
				done.Wait()
				return err
			}
			tx := blk.Transactions[txIdx]
			errIdx := txIdx - chunkStart
			if tx == nil {
				errs[errIdx] = errNilTransaction
				continue
			}
			done.Add(1)
			select {
			case v.jobs <- transactionVerifyJob{tx: tx, err: &errs[errIdx], done: &done}:
			case <-ctx.Done():
				done.Done()
				done.Wait()
				return ctx.Err()
			}
		}
		done.Wait()
		if err := ctx.Err(); err != nil {
			return err
		}
		for errIdx, err := range errs {
			if err != nil {
				return formatTransactionVerificationError(blk, chunkStart+errIdx, err)
			}
		}
	}
	return nil
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

func validateBlockTransactionsContext(ctx context.Context, blk *block.Block) error {
	defaultTransactionVerifierOnce.Do(func() {
		workers := max(1, (runtime.GOMAXPROCS(0)+1)/2)
		defaultTransactionVerifier = newTransactionVerifier(workers, 2*workers*sigverify.BatchTarget, nil)
	})
	return defaultTransactionVerifier.verifyBlockContext(ctx, blk)
}

func validateBlockTransactions(blk *block.Block) error {
	return validateBlockTransactionsContext(context.Background(), blk)
}
