package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

// unrootedTailHaltCap bounds the in-RAM unrooted tail; replay halts if held slots
// exceed it rather than growing RAM unbounded (~16x normal rooting lag).
const unrootedTailHaltCap = 512

// batchCommitter durably folds a batch of rooted slots into the canonical
// store as one sequential segment (union-deduped, one fsync, atomic index
// flip). Satisfied by AccountsDb.CommitBatch.
type batchCommitter interface {
	CommitBatch(deltas []accounts.SlotDelta, throughSlot uint64, bankhashes map[uint64][32]byte, resumeCtx []byte) (accountsdb.BatchCommitResult, error)
}

// TransactionStatusCheckpointHooks deliberately split status-cache capture
// from sidecar I/O. Snapshot runs on the replay loop while its mutable cache is
// coherent; Install runs on the fold worker using only those immutable bytes.
// This makes it impossible for the async worker to traverse concurrently
// changing replay lineage. The later AccountsDB manifest remains the selector.
type TransactionStatusCheckpointHooks struct {
	Snapshot func(through uint64) ([]byte, error)
	Install  func(through uint64, payload []byte) (*state.TransactionStatusCheckpointRef, error)
	// AfterCommit is an advisory retention hook. It runs only after CommitBatch
	// has durably selected the manifest carrying selected. Its error is logged
	// and ignored: once CommitBatch succeeds, the fold must remain successful.
	AfterCommit func(selected *state.TransactionStatusCheckpointRef) error
}

// defaultFoldBatchSlots is the fold chunk size K when none is configured:
// rooted slots fold to disk K at a time (union-deduped), and a trailing
// partial chunk stays in RAM (bounded by K) until it fills or flush() runs.
const defaultFoldBatchSlots = 128

// FoldBatchSlots is the configured fold chunk size (storage.fold_batch_slots),
// set by node startup before ReplayBlocks. Zero uses the default. Larger K =
// better union dedupe and less NVMe wear, more RAM, longer crash re-execution.
var FoldBatchSlots = defaultFoldBatchSlots

// blockAccountSource is the slot-scoped read API the block loader needs; both
// AccountsDb and unrootedTail satisfy it, so the loader is mode-agnostic.
type blockAccountSource interface {
	GetAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error)
	GetAccountsBatch(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error)
}

// sharedBlockAccountSource is the read-only fast path used solely to build a
// block's immutable parent snapshot. Public reads retain their owned-copy
// semantics; implementations may return cache/WorkingSet-backed pointers here.
type sharedBlockAccountSource interface {
	GetAccountsBatchShared(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error)
}

type measuredSharedBlockAccountSource interface {
	GetAccountsBatchSharedWithStats(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, accountsdb.BatchReadStats, error)
}

type measuredBlockAccountSource interface {
	GetAccountWithStats(slot uint64, pubkey solana.PublicKey) (*accounts.Account, accountsdb.AccountReadStats, error)
}

func getAccountWithStats(source blockAccountSource, slot uint64, pubkey solana.PublicKey) (*accounts.Account, accountsdb.AccountReadStats, error) {
	if measured, ok := source.(measuredBlockAccountSource); ok {
		return measured.GetAccountWithStats(slot, pubkey)
	}
	start := time.Now()
	acct, err := source.GetAccount(slot, pubkey)
	return acct, accountsdb.AccountReadStats{
		IndexAndAppendVecReadNanoseconds: uint64(time.Since(start).Nanoseconds()),
		DurableRead:                      true,
	}, err
}

func getAccountsBatchShared(ctx context.Context, source blockAccountSource, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	out, _, err := getAccountsBatchSharedWithStats(ctx, source, slot, pks)
	return out, err
}

func getAccountsBatchSharedWithStats(ctx context.Context, source blockAccountSource, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, accountsdb.BatchReadStats, error) {
	if measured, ok := source.(measuredSharedBlockAccountSource); ok {
		return measured.GetAccountsBatchSharedWithStats(ctx, slot, pks)
	}
	if shared, ok := source.(sharedBlockAccountSource); ok {
		out, err := shared.GetAccountsBatchShared(ctx, slot, pks)
		return out, accountsdb.BatchReadStats{RequestedKeys: uint64(len(pks)), DurableKeys: uint64(len(pks))}, err
	}
	out, err := source.GetAccountsBatch(ctx, slot, pks)
	return out, accountsdb.BatchReadStats{RequestedKeys: uint64(len(pks)), DurableKeys: uint64(len(pks))}, err
}

// unrootedState is the in-RAM speculative-state engine the replay loop drives in
// rooted-durable mode: reads resolve speculative→durable, commits buffer in RAM,
// rooted slots promote to disk. Implemented by unrootedTail (linear) and forkTail
// (fork-aware, over forkCoordinator).
type unrootedState interface {
	blockAccountSource
	Add(slot uint64, delta []*accounts.Account, bankhash []byte)
	SetContext(slot uint64, ctx *state.ResumeContext, bankSysvars ...*sealevel.BankSysvars)
	promote(through uint64) (uint64, *state.ResumeContext, error)
	// flush force-folds the trailing partial chunk <= through. Epoch-boundary
	// scans use it to settle the durable AccountsDB view; graceful shutdown uses
	// it so restart re-execution is bounded by the fold batch size.
	flush(through uint64) (uint64, *state.ResumeContext, error)
	OverCap() bool
}

// unrootedTail layers an in-RAM UnrootedOverlay over the durable store: reads
// resolve overlay→durable, commits buffer until rooted slots promote out.
type unrootedTail struct {
	overlay     *accounts.WorkingSet
	durable     blockAccountSource // the canonical rooted store (for read fall-through)
	committer   batchCommitter     // durable promotion of rooted slot batches
	bankhashes  map[uint64][32]byte
	batchSlots  int    // fold chunk size K
	stakeIdxDir string // directory of stake_pubkeys.idx; pending stake entries flush here at fold time
	// contexts holds the deep-copied end-of-slot resume context per held slot,
	// retained until promotion so the context as of the last rooted slot survives for resume.
	contexts map[uint64]*state.ResumeContext
	// bankSysvars is the in-memory-only immutable sysvar snapshot paired with
	// each retained context. It is deliberately not part of ResumeContext (and
	// therefore never enters persisted JSON): the durable resume path rebuilds
	// its first snapshot from AccountsDB, while an in-loop fork unwind needs the
	// exact surviving unrooted parent rather than the process-global cache left
	// by the abandoned suffix.
	bankSysvars map[uint64]*sealevel.BankSysvars
	haltCap     int // halt replay if held slots exceed this (rooting stalled)

	transactionStatusCheckpointHooks TransactionStatusCheckpointHooks
}

func newUnrootedTail(durable blockAccountSource, committer batchCommitter, haltCap int, batchSlots int, stakeIdxDir string) *unrootedTail {
	if batchSlots <= 0 {
		batchSlots = defaultFoldBatchSlots
	}
	return &unrootedTail{
		overlay:     accounts.NewWorkingSet(),
		durable:     durable,
		committer:   committer,
		bankhashes:  make(map[uint64][32]byte),
		batchSlots:  batchSlots,
		stakeIdxDir: stakeIdxDir,
		contexts:    make(map[uint64]*state.ResumeContext),
		bankSysvars: make(map[uint64]*sealevel.BankSysvars),
		haltCap:     haltCap,
	}
}

// SetTransactionStatusCheckpointHooks installs the fold-time durability hooks.
// They must be set before promotion starts and remain immutable thereafter.
// Both async full chunks and synchronous forced partial chunks use them.
func (t *unrootedTail) SetTransactionStatusCheckpointHooks(hooks TransactionStatusCheckpointHooks) error {
	if err := validateTransactionStatusCheckpointHooks(hooks); err != nil {
		return err
	}
	t.transactionStatusCheckpointHooks = hooks
	return nil
}

// GetAccount resolves the newest unrooted write for pubkey, else the durable
// (rooted) value read at slot.
func (t *unrootedTail) GetAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if a, ok := t.overlay.Lookup([32]byte(pubkey)); ok {
		return a.Clone(), nil
	}
	a, err := t.durable.GetAccount(slot, pubkey)
	if err != nil || a == nil {
		return a, err
	}
	return a.Clone(), nil
}

func (t *unrootedTail) GetAccountWithStats(slot uint64, pubkey solana.PublicKey) (*accounts.Account, accountsdb.AccountReadStats, error) {
	var stats accountsdb.AccountReadStats
	lookupStart := time.Now()
	if a, ok := t.overlay.Lookup([32]byte(pubkey)); ok {
		stats.WorkingSetLookupNanoseconds = uint64(time.Since(lookupStart).Nanoseconds())
		stats.WorkingSetHit = true
		// Callers receive a mutable account, never the WorkingSet's retained
		// historical value. Fee and reward paths legitimately mutate values
		// returned by this method.
		cloneStart := time.Now()
		acct := a.Clone()
		stats.CloneNanoseconds = uint64(time.Since(cloneStart).Nanoseconds())
		return acct, stats, nil
	}
	stats.WorkingSetLookupNanoseconds = uint64(time.Since(lookupStart).Nanoseconds())
	a, durableStats, err := getAccountWithStats(t.durable, slot, pubkey)
	durableStats.WorkingSetLookupNanoseconds += stats.WorkingSetLookupNanoseconds
	if err != nil || a == nil {
		return a, durableStats, err
	}
	// AccountsDb may satisfy this read from one of its shared read caches.
	// Do not let a speculative caller (notably leader fee distribution) mutate
	// that cached parent in place: ordered replay must observe the same parent
	// value when it reconstructs the locally produced block.
	cloneStart := time.Now()
	acct := a.Clone()
	durableStats.CloneNanoseconds += uint64(time.Since(cloneStart).Nanoseconds())
	return acct, durableStats, nil
}

// GetAccountsBatch returns one entry per requested key, in order, preferring the
// unrooted value and falling through to a single durable batch for the misses.
func (t *unrootedTail) GetAccountsBatch(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	out, err := t.GetAccountsBatchShared(ctx, slot, pks)
	if err != nil {
		return nil, err
	}
	for i, acct := range out {
		if acct != nil {
			out[i] = acct.Clone()
		}
	}
	return out, nil
}

// GetAccountsBatchShared resolves each key through one WorkingSet read lock,
// then falls through to one durable batch for the misses. Results are borrowed
// immutable values for the block parent snapshot; execution copy-on-writes on
// first mutation.
func (t *unrootedTail) GetAccountsBatchShared(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	out, _, err := t.GetAccountsBatchSharedWithStats(ctx, slot, pks)
	return out, err
}

func (t *unrootedTail) GetAccountsBatchSharedWithStats(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, accountsdb.BatchReadStats, error) {
	stats := accountsdb.BatchReadStats{RequestedKeys: uint64(len(pks))}
	if len(pks) == 0 {
		return nil, stats, nil
	}
	out := make([]*accounts.Account, len(pks))
	lookupStart := time.Now()
	t.overlay.LookupBatch(pks, out)
	stats.WorkingSetLookupNanoseconds = uint64(time.Since(lookupStart).Nanoseconds())
	missCount := 0
	for _, acct := range out {
		if acct == nil {
			missCount++
		}
	}
	stats.WorkingSetHits = uint64(len(pks) - missCount)
	stats.DurableKeys = uint64(missCount)
	if missCount > 0 {
		misses := make([]solana.PublicKey, 0, missCount)
		missIdx := make([]int, 0, missCount)
		for i, acct := range out {
			if acct == nil {
				misses = append(misses, pks[i])
				missIdx = append(missIdx, i)
			}
		}
		loaded, durableStats, err := getAccountsBatchSharedWithStats(ctx, t.durable, slot, misses)
		if err != nil {
			return nil, stats, err
		}
		durableStats.RequestedKeys = stats.RequestedKeys
		durableStats.DurableKeys = stats.DurableKeys
		durableStats.WorkingSetHits += stats.WorkingSetHits
		durableStats.WorkingSetLookupNanoseconds += stats.WorkingSetLookupNanoseconds
		stats = durableStats
		// Durable returns one entry per requested key; guard so a contract
		// violation surfaces as an error, not an index panic.
		if len(loaded) != len(misses) {
			return nil, stats, fmt.Errorf("durable GetAccountsBatch returned %d accounts for %d keys at slot %d", len(loaded), len(misses), slot)
		}
		for j, a := range loaded {
			out[missIdx[j]] = a
		}
	}
	return out, stats, nil
}

// Add buffers a replayed slot's writes + bankhash in the overlay; it becomes
// durable only via promote(). Resume context is attached separately (SetContext).
func (t *unrootedTail) Add(slot uint64, delta []*accounts.Account, bankhash []byte) {
	t.overlay.Add(slot, delta)
	var slotBankhash [32]byte
	copy(slotBankhash[:], bankhash)
	t.bankhashes[slot] = slotBankhash
}

// SetContext attaches a held slot's end-of-slot resume context and, when
// supplied, its immutable bank-local sysvar snapshot. ctx MUST be deep-copied
// (no pointers into the global SysvarCache); both values are retained until
// promotion. The variadic snapshot preserves compatibility for test/bootstrap
// callers that only exercise durable context persistence; such entries are
// intentionally ineligible for an in-loop fork unwind.
func (t *unrootedTail) SetContext(slot uint64, ctx *state.ResumeContext, bankSysvars ...*sealevel.BankSysvars) {
	if ctx != nil {
		t.contexts[slot] = ctx
		delete(t.bankSysvars, slot)
		if len(bankSysvars) > 0 && bankSysvars[0] != nil {
			t.bankSysvars[slot] = bankSysvars[0]
		}
	}
}

// promote folds full K-slot chunks of the rooted prefix <= through to disk;
// a trailing partial chunk stays in RAM until it fills (or flush() forces it).
// Returns the highest slot now durable and its resume context (nil if none).
func (t *unrootedTail) promote(through uint64) (uint64, *state.ResumeContext, error) {
	return t.promoteChunked(through, false)
}

// flush force-folds everything <= through including the partial trailing
// chunk for epoch-boundary settlement and graceful shutdown.
func (t *unrootedTail) flush(through uint64) (uint64, *state.ResumeContext, error) {
	return t.promoteChunked(through, true)
}

func (t *unrootedTail) promoteChunked(through uint64, force bool) (uint64, *state.ResumeContext, error) {
	promotedThrough, err := promoteRootedBatched(t.overlay, through, t.bankhashes, t.contexts, t.committer, t.batchSlots, t.stakeIdxDir, force, t.transactionStatusCheckpointHooks)
	if promotedThrough == 0 {
		return 0, nil, err
	}
	ctx := t.contexts[promotedThrough] // context as of the last rooted slot (read before pruning)
	for s := range t.contexts {
		if s <= promotedThrough {
			delete(t.contexts, s)
		}
	}
	for s := range t.bankSysvars {
		if s <= promotedThrough {
			delete(t.bankSysvars, s)
		}
	}
	return promotedThrough, ctx, err
}

// ── Async promotion ─────────────────────────────────────────────────────────
//
// CommitBatch is the replay loop's only heavy synchronous stall (segment write
// + fsync + index flip, ~hundreds of ms per K-slot chunk). The async promoter
// moves it off the loop: the loop BUILDS an immutable fold job (chunk snapshot
// + marshaled context), a worker goroutine runs the durable part (stake-index
// flush + CommitBatch), and the loop APPLIES the completion on a later
// iteration (PromotePrefix + map pruning + watermark bookkeeping). All
// WorkingSet/map mutation stays on the loop thread — the worker touches only
// its job and the committer.
//
// Safety notes:
// - While a fold is in flight the chunk's overlay layers are retained (reads
//   stay correct: overlay wins over durable) and are immutable. Mutable tail
//   reads clone; the block-parent fast path only borrows values and transaction
//   execution copy-on-writes them. Add only appends new slots, and the
//   fork-switch unwind DRAINS the promoter before evicting (block.go), so
//   EvictFrom can never race the worker's reads.
// - One job in flight at a time; the next chunk builds only after apply, so
//   the alpenglow promotion gate re-checks every span it folds.
// - A completed-but-unapplied fold on exit is identical to the supported
//   "crash after commit, before state-file update" case: RecoverFoldState
//   reconciles the store frontier forward on the next start.
// - A failed fold is retried naturally: LastRootedSlot did not advance, so the
//   next iteration rebuilds the same chunk. The stake-index flush that already
//   landed is a harmless superset (second flush is a no-op).

// foldJob is an immutable snapshot of one K-slot fold chunk. Account values
// reference immutable retained WorkingSet layers; the layers are not removed
// until this job completes and applies.
type foldJob struct {
	chunk                                  []accounts.SlotDelta
	through                                uint64
	bankhashes                             map[uint64][32]byte
	ctx                                    *state.ResumeContext
	ctxJSON                                []byte
	stakeIdxDir                            string
	transactionStatusCheckpointPayload     []byte
	installTransactionStatusCheckpoint     func(through uint64, payload []byte) (*state.TransactionStatusCheckpointRef, error)
	afterTransactionStatusCheckpointCommit func(selected *state.TransactionStatusCheckpointRef) error
}

type foldResult struct {
	job *foldJob
	err error
}

// buildFoldJob snapshots the FIRST fold chunk of the rooted prefix <= through
// (loop thread). force also takes a trailing partial chunk. Returns nil when
// no chunk is ready. A missing chunk-top context is an error — a context-less
// fold manifest would be unrecoverable, so it must not commit. An optional
// hooks overrides the tail's configured hooks, primarily for focused tests.
func (t *unrootedTail) buildFoldJob(through uint64, force bool, hookOverrides ...TransactionStatusCheckpointHooks) (*foldJob, error) {
	hooks, err := resolveTransactionStatusCheckpointHooks(t.transactionStatusCheckpointHooks, hookOverrides)
	if err != nil {
		return nil, err
	}
	prefix := t.overlay.PromotionPrefix(through)
	if len(prefix) == 0 {
		return nil, nil
	}
	chunk := prefix
	if len(chunk) > t.batchSlots {
		chunk = chunk[:t.batchSlots]
	} else if len(chunk) < t.batchSlots && !force {
		return nil, nil // trailing partial chunk stays in RAM
	}
	through = chunk[len(chunk)-1].Slot

	ctx := t.contexts[through]
	if ctx == nil {
		return nil, fmt.Errorf("fold chunk through slot %d: no resume context recorded for chunk-top slot", through)
	}
	ctx = cloneResumeContextForFold(ctx)
	var checkpointPayload []byte
	if hooks.Snapshot != nil {
		checkpointPayload, err = hooks.Snapshot(through)
		if err != nil {
			return nil, fmt.Errorf("fold chunk through slot %d: snapshot transaction status checkpoint: %w", through, err)
		}
		if len(checkpointPayload) == 0 {
			return nil, fmt.Errorf("fold chunk through slot %d: transaction status checkpoint snapshot is empty", through)
		}
		// The worker owns this immutable copy. Even a future Snapshot
		// implementation that reuses a scratch buffer cannot race it.
		checkpointPayload = append([]byte(nil), checkpointPayload...)
	}
	bankhashes := make(map[uint64][32]byte, len(chunk))
	for _, sd := range chunk {
		if bh, ok := t.bankhashes[sd.Slot]; ok {
			bankhashes[sd.Slot] = bh
		}
	}
	return &foldJob{
		chunk:                                  append([]accounts.SlotDelta(nil), chunk...),
		through:                                through,
		bankhashes:                             bankhashes,
		ctx:                                    ctx,
		stakeIdxDir:                            t.stakeIdxDir,
		transactionStatusCheckpointPayload:     checkpointPayload,
		installTransactionStatusCheckpoint:     hooks.Install,
		afterTransactionStatusCheckpointCommit: hooks.AfterCommit,
	}, nil
}

// runFoldJob performs the durable half of a fold (worker-safe: no tail
// state). Stake-index entries flush (fsync'd) BEFORE the batch commit — see
// promoteRootedBatched for why that order is a correctness requirement.
func runFoldJob(committer batchCommitter, job *foldJob) error {
	if job == nil || job.ctx == nil {
		return errors.New("fold job has no resume context")
	}
	var selectedCheckpoint *state.TransactionStatusCheckpointRef
	if job.installTransactionStatusCheckpoint != nil {
		ref, err := job.installTransactionStatusCheckpoint(job.through, job.transactionStatusCheckpointPayload)
		if err != nil {
			return fmt.Errorf("fold chunk through slot %d: prepare transaction status checkpoint: %w", job.through, err)
		}
		if err := ValidateTransactionStatusCheckpointRef(ref, job.through); err != nil {
			return fmt.Errorf("fold chunk through slot %d: prepared transaction status checkpoint is invalid: %w", job.through, err)
		}
		refCopy := *ref
		job.ctx.TransactionStatusCheckpoint = &refCopy
		selectedCopy := refCopy
		selectedCheckpoint = &selectedCopy
	}
	ctxJSON, err := json.Marshal(job.ctx)
	if err != nil {
		return fmt.Errorf("fold chunk through slot %d: marshal resume context: %w", job.through, err)
	}
	job.ctxJSON = ctxJSON

	if job.stakeIdxDir != "" {
		if _, err := global.FlushPendingStakePubkeysThrough(job.stakeIdxDir, job.through); err != nil {
			return fmt.Errorf("fold chunk through slot %d: flush stake index: %w", job.through, err)
		}
	}
	if _, err := committer.CommitBatch(job.chunk, job.through, job.bankhashes, job.ctxJSON); err != nil {
		return fmt.Errorf("fold chunk through slot %d: %w", job.through, err)
	}
	if job.afterTransactionStatusCheckpointCommit != nil {
		if err := job.afterTransactionStatusCheckpointCommit(selectedCheckpoint); err != nil {
			mlog.Log.Warnf("fold chunk through slot %d: transaction status checkpoint cleanup failed after durable commit: %v", job.through, err)
		}
	}
	return nil
}

// applyFoldJob applies a completed fold on the loop thread: the overlay drops
// the now-durable prefix and the per-slot maps prune. Returns the rooted
// context (the job snapshot — identical to what the manifest carries).
func (t *unrootedTail) applyFoldJob(job *foldJob) *state.ResumeContext {
	t.overlay.PromotePrefix(job.through)
	for s := range t.bankhashes {
		if s <= job.through {
			delete(t.bankhashes, s)
		}
	}
	for s := range t.contexts {
		if s <= job.through {
			delete(t.contexts, s)
		}
	}
	for s := range t.bankSysvars {
		if s <= job.through {
			delete(t.bankSysvars, s)
		}
	}
	return job.ctx
}

// asyncPromoter runs fold jobs on a worker goroutine, one in flight at a time.
// inFlight is loop-thread-owned; jobs/results carry the handoff.
type asyncPromoter struct {
	committer batchCommitter
	jobs      chan *foldJob
	results   chan foldResult
	inFlight  bool
	done      chan struct{}
}

func newAsyncPromoter(committer batchCommitter) *asyncPromoter {
	p := &asyncPromoter{
		committer: committer,
		jobs:      make(chan *foldJob, 1),
		results:   make(chan foldResult, 1),
		done:      make(chan struct{}),
	}
	go p.run()
	return p
}

func (p *asyncPromoter) run() {
	defer close(p.done)
	for job := range p.jobs {
		start := time.Now()
		err := runFoldJob(p.committer, job)
		if err == nil {
			mlog.Log.FileOnlyf("async fold: committed %d slots through %d in %s", len(job.chunk), job.through, time.Since(start).Round(time.Millisecond))
		}
		p.results <- foldResult{job: job, err: err}
	}
}

// enqueue hands a job to the worker (loop thread; requires !inFlight).
func (p *asyncPromoter) enqueue(job *foldJob) {
	p.jobs <- job
	p.inFlight = true
}

// poll returns a completed result without blocking (nil when none / none in
// flight).
func (p *asyncPromoter) poll() *foldResult {
	if !p.inFlight {
		return nil
	}
	select {
	case res := <-p.results:
		p.inFlight = false
		return &res
	default:
		return nil
	}
}

// drain blocks until the in-flight job (if any) completes and returns it.
// Called before fork unwinds, the shutdown flush, and loop exit — anywhere
// that must not race the worker or needs the durable frontier settled.
func (p *asyncPromoter) drain() *foldResult {
	if !p.inFlight {
		return nil
	}
	res := <-p.results
	p.inFlight = false
	return &res
}

// stop drains any in-flight job and terminates the worker. The result of a
// drained-but-unapplied fold is intentionally discarded: the store is ahead
// of the state file, which RecoverFoldState reconciles on the next start.
func (p *asyncPromoter) stop() {
	p.drain()
	close(p.jobs)
	<-p.done
}

// unwind drops all held slots >= fromSlot (the execute-on-receipt fork
// switch) and returns the retained resume context and immutable bank-sysvar
// snapshot of the last surviving executed slot so the replay loop can rebuild
// execution state and re-run the certified version. Either result may be nil;
// the caller validates the pair and falls back to rooted-checkpoint re-replay.
func (t *unrootedTail) unwind(fromSlot uint64) (*state.ResumeContext, *sealevel.BankSysvars) {
	t.overlay.EvictFrom(fromSlot)
	// Branch-scoped side effect: stake pubkeys enqueued by the evicted slots
	// must never reach the durable index — drop them with the state.
	if dropped := global.DropPendingStakePubkeysFrom(fromSlot); dropped > 0 {
		mlog.Log.Infof("fork unwind: dropped %d pending stake-index entries from slots >= %d", dropped, fromSlot)
	}
	for s := range t.bankhashes {
		if s >= fromSlot {
			delete(t.bankhashes, s)
		}
	}
	var ctx *state.ResumeContext
	var ctxSlot uint64
	for s, c := range t.contexts {
		if s >= fromSlot {
			delete(t.contexts, s)
			continue
		}
		if ctx == nil || s > ctxSlot {
			ctx = c
			ctxSlot = s
		}
	}
	for s := range t.bankSysvars {
		if s >= fromSlot {
			delete(t.bankSysvars, s)
		}
	}
	// ctx is the highest retained context with slot < fromSlot: the ACTUAL
	// executed parent. It need not be numerically fromSlot-1 — when slots between
	// it and fromSlot were skipped, they retain no context (only executed held
	// slots call SetContext), so the last executed slot IS the parent bank of the
	// certified block at fromSlot. Returning nil (parent already durably folded,
	// or nothing retained) routes the caller to the rooted-checkpoint fallback.
	if ctx == nil {
		return nil, nil
	}
	return ctx, t.bankSysvars[ctxSlot]
}

// OverCap reports whether the unrooted tail has grown past the halt cap, i.e.
// rooting has stalled and we must stop replay rather than grow RAM unbounded.
func (t *unrootedTail) OverCap() bool {
	return t.haltCap > 0 && t.overlay.HeldSlots() > t.haltCap
}

// promoteRootedBatched folds held slots <= through in chunks of batchSlots.
// Each chunk = one CommitBatch (one segment + one fsync + one index flip),
// then the chunk drops from RAM. A trailing partial chunk folds only when
// force is set; otherwise it stays in RAM, so restart re-execution from the
// blockstore is bounded by the chunk size. Crash-safe: an error stops at the
// last fully durable chunk boundary (a failed chunk leaves only an orphan
// segment that recovery GCs).
// safePromoteTarget is the dual-watermark fold target: certificate finality
// clamped by the trailing-verification watermark (only when the verifier is
// required) and never at or beyond a persisted-divergence floor. Promotion is
// driven by this target exceeding the durable watermark, so verified progress
// alone can advance it even when certificate finality is momentarily flat.
func safePromoteTarget(finality uint64, verifierRequired bool, verifiedWatermark, divergenceFloor uint64) uint64 {
	target := finality
	if verifierRequired && verifiedWatermark < target {
		target = verifiedWatermark
	}
	if divergenceFloor > 0 && target >= divergenceFloor {
		target = divergenceFloor - 1
	}
	return target
}

func promoteRootedBatched(
	overlay *accounts.WorkingSet,
	through uint64,
	bankhashes map[uint64][32]byte,
	contexts map[uint64]*state.ResumeContext,
	committer batchCommitter,
	batchSlots int,
	stakeIdxDir string,
	force bool,
	hookOverrides ...TransactionStatusCheckpointHooks,
) (promotedThrough uint64, err error) {
	hooks, err := resolveTransactionStatusCheckpointHooks(TransactionStatusCheckpointHooks{}, hookOverrides)
	if err != nil {
		return 0, err
	}
	prefix := overlay.PromotionPrefix(through)
	if len(prefix) == 0 {
		return 0, nil
	}

	for start := 0; start < len(prefix); start += batchSlots {
		end := start + batchSlots
		if end > len(prefix) {
			if !force {
				break // trailing partial chunk stays in RAM
			}
			end = len(prefix)
		}
		chunk := prefix[start:end]
		chunkThrough := chunk[len(chunk)-1].Slot

		chunkBankhashes := make(map[uint64][32]byte, len(chunk))
		for _, sd := range chunk {
			if bh, ok := bankhashes[sd.Slot]; ok {
				chunkBankhashes[sd.Slot] = bh
			}
		}

		// The chunk-top resume context rides in the manifest so the durable
		// watermark + context survive a hard crash without the state file. A
		// missing or unmarshalable context would produce a manifest that
		// recovery cannot resume from — it fatals when the store outruns the
		// state file. Fail the fold here instead of committing an unrecoverable
		// batch; the caller holds the watermark and the slots stay in RAM.
		ctx := contexts[chunkThrough]
		if ctx == nil {
			err = fmt.Errorf("promote chunk through slot %d: no resume context recorded for chunk-top slot", chunkThrough)
			break
		}
		ctx = cloneResumeContextForFold(ctx)
		var selectedCheckpoint *state.TransactionStatusCheckpointRef
		if hooks.Snapshot != nil {
			payload, serr := hooks.Snapshot(chunkThrough)
			if serr != nil {
				err = fmt.Errorf("promote chunk through slot %d: snapshot transaction status checkpoint: %w", chunkThrough, serr)
				break
			}
			if len(payload) == 0 {
				err = fmt.Errorf("promote chunk through slot %d: transaction status checkpoint snapshot is empty", chunkThrough)
				break
			}
			ref, perr := hooks.Install(chunkThrough, payload)
			if perr != nil {
				err = fmt.Errorf("promote chunk through slot %d: prepare transaction status checkpoint: %w", chunkThrough, perr)
				break
			}
			if verr := ValidateTransactionStatusCheckpointRef(ref, chunkThrough); verr != nil {
				err = fmt.Errorf("promote chunk through slot %d: prepared transaction status checkpoint is invalid: %w", chunkThrough, verr)
				break
			}
			refCopy := *ref
			ctx.TransactionStatusCheckpoint = &refCopy
			selectedCopy := refCopy
			selectedCheckpoint = &selectedCopy
		}
		ctxJSON, merr := json.Marshal(ctx)
		if merr != nil {
			err = fmt.Errorf("promote chunk through slot %d: marshal resume context: %w", chunkThrough, merr)
			break
		}

		// Stake-index entries for this chunk's slots flush (fsync'd) BEFORE the
		// batch commit: if we crash between the two, the index holds a harmless
		// superset (those slots re-execute and re-enqueue; scans dedup). The
		// reverse order could leave folded slots' stake accounts missing from
		// the index — a subset — which would silently corrupt the epoch-stakes
		// scan. Entries for unfolded slots stay in RAM (branch-scoped).
		if stakeIdxDir != "" {
			if _, ferr := global.FlushPendingStakePubkeysThrough(stakeIdxDir, chunkThrough); ferr != nil {
				err = fmt.Errorf("promote chunk through slot %d: flush stake index: %w", chunkThrough, ferr)
				break
			}
		}

		if _, cerr := committer.CommitBatch(chunk, chunkThrough, chunkBankhashes, ctxJSON); cerr != nil {
			err = fmt.Errorf("promote chunk through slot %d: %w", chunkThrough, cerr)
			break
		}
		if hooks.AfterCommit != nil {
			if gcErr := hooks.AfterCommit(selectedCheckpoint); gcErr != nil {
				mlog.Log.Warnf("promote chunk through slot %d: transaction status checkpoint cleanup failed after durable commit: %v", chunkThrough, gcErr)
			}
		}
		// Publish the exact context selected by the committed manifest. The caller
		// returns this context into MithrilState after the overlay apply.
		contexts[chunkThrough] = ctx

		overlay.PromotePrefix(chunkThrough)
		for _, sd := range chunk {
			delete(bankhashes, sd.Slot)
		}
		promotedThrough = chunkThrough
	}
	return promotedThrough, err
}

func resolveTransactionStatusCheckpointHooks(configured TransactionStatusCheckpointHooks, overrides []TransactionStatusCheckpointHooks) (TransactionStatusCheckpointHooks, error) {
	if len(overrides) > 1 {
		return TransactionStatusCheckpointHooks{}, fmt.Errorf("at most one transaction status checkpoint hook set may be supplied, got %d", len(overrides))
	}
	if len(overrides) == 1 {
		configured = overrides[0]
	}
	if err := validateTransactionStatusCheckpointHooks(configured); err != nil {
		return TransactionStatusCheckpointHooks{}, err
	}
	return configured, nil
}

func validateTransactionStatusCheckpointHooks(hooks TransactionStatusCheckpointHooks) error {
	if (hooks.Snapshot == nil) != (hooks.Install == nil) {
		return errors.New("transaction status checkpoint Snapshot and Install hooks must either both be set or both be nil")
	}
	if hooks.AfterCommit != nil && hooks.Install == nil {
		return errors.New("transaction status checkpoint AfterCommit hook requires Snapshot and Install hooks")
	}
	return nil
}

func cloneResumeContextForFold(ctx *state.ResumeContext) *state.ResumeContext {
	if ctx == nil {
		return nil
	}
	clone := *ctx
	clone.RecentBlockhashes = append([]state.BlockhashEntry(nil), ctx.RecentBlockhashes...)
	clone.SlotHashes = append([]state.SlotHashEntry(nil), ctx.SlotHashes...)
	if ctx.TransactionCount != nil {
		count := *ctx.TransactionCount
		clone.TransactionCount = &count
	}
	if ctx.TransactionStatusCheckpoint != nil {
		ref := *ctx.TransactionStatusCheckpoint
		clone.TransactionStatusCheckpoint = &ref
	}
	return &clone
}
