package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
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

// unrootedState is the in-RAM speculative-state engine the replay loop drives in
// rooted-durable mode: reads resolve speculative→durable, commits buffer in RAM,
// rooted slots promote to disk. Implemented by unrootedTail (linear) and forkTail
// (fork-aware, over forkCoordinator).
type unrootedState interface {
	blockAccountSource
	Add(slot uint64, delta []*accounts.Account, bankhash []byte)
	SetContext(slot uint64, ctx *state.ResumeContext)
	promote(through uint64) (uint64, *state.ResumeContext, error)
	// flush force-folds the trailing partial chunk <= through (graceful
	// shutdown), so restart re-execution is bounded by the fold batch size.
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
	haltCap  int // halt replay if held slots exceed this (rooting stalled)
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
		haltCap:     haltCap,
	}
}

// GetAccount resolves the newest unrooted write for pubkey, else the durable
// (rooted) value read at slot.
func (t *unrootedTail) GetAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if a, ok := t.overlay.Lookup([32]byte(pubkey)); ok {
		return a, nil
	}
	return t.durable.GetAccount(slot, pubkey)
}

// GetAccountsBatch returns one entry per requested key, in order, preferring the
// unrooted value and falling through to a single durable batch for the misses.
func (t *unrootedTail) GetAccountsBatch(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	return batchOverDurable(ctx, slot, pks, t.durable, func(pk solana.PublicKey) (*accounts.Account, bool) {
		return t.overlay.Lookup([32]byte(pk))
	})
}

// batchOverDurable resolves each key via the speculative lookup, falling through to a
// single durable batch for the misses, preserving order and placeholder semantics.
func batchOverDurable(ctx context.Context, slot uint64, pks []solana.PublicKey, durable blockAccountSource, lookup func(solana.PublicKey) (*accounts.Account, bool)) ([]*accounts.Account, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	out := make([]*accounts.Account, len(pks))
	var misses []solana.PublicKey
	var missIdx []int
	for i, pk := range pks {
		if a, ok := lookup(pk); ok {
			out[i] = a
		} else {
			misses = append(misses, pk)
			missIdx = append(missIdx, i)
		}
	}
	if len(misses) > 0 {
		loaded, err := durable.GetAccountsBatch(ctx, slot, misses)
		if err != nil {
			return nil, err
		}
		// Durable returns one entry per requested key; guard so a contract
		// violation surfaces as an error, not an index panic.
		if len(loaded) != len(misses) {
			return nil, fmt.Errorf("durable GetAccountsBatch returned %d accounts for %d keys at slot %d", len(loaded), len(misses), slot)
		}
		for j, a := range loaded {
			out[missIdx[j]] = a
		}
	}
	return out, nil
}

// Add buffers a replayed slot's writes + bankhash in the overlay; it becomes
// durable only via promote(). Resume context is attached separately (SetContext).
func (t *unrootedTail) Add(slot uint64, delta []*accounts.Account, bankhash []byte) {
	t.overlay.Add(slot, delta)
	var slotBankhash [32]byte
	copy(slotBankhash[:], bankhash)
	t.bankhashes[slot] = slotBankhash
}

// SetContext attaches a held slot's end-of-slot resume context. ctx MUST be
// deep-copied (no pointers into the global SysvarCache); retained until promotion.
func (t *unrootedTail) SetContext(slot uint64, ctx *state.ResumeContext) {
	if ctx != nil {
		t.contexts[slot] = ctx
	}
}

// promote folds full K-slot chunks of the rooted prefix <= through to disk;
// a trailing partial chunk stays in RAM until it fills (or flush() forces it).
// Returns the highest slot now durable and its resume context (nil if none).
func (t *unrootedTail) promote(through uint64) (uint64, *state.ResumeContext, error) {
	return t.promoteChunked(through, false)
}

// flush force-folds everything <= through including the partial trailing
// chunk — the graceful-shutdown path, bounding restart re-execution.
func (t *unrootedTail) flush(through uint64) (uint64, *state.ResumeContext, error) {
	return t.promoteChunked(through, true)
}

func (t *unrootedTail) promoteChunked(through uint64, force bool) (uint64, *state.ResumeContext, error) {
	promotedThrough, err := promoteRootedBatched(t.overlay, through, t.bankhashes, t.contexts, t.committer, t.batchSlots, t.stakeIdxDir, force)
	if promotedThrough == 0 {
		return 0, nil, err
	}
	ctx := t.contexts[promotedThrough] // context as of the last rooted slot (read before pruning)
	for s := range t.contexts {
		if s <= promotedThrough {
			delete(t.contexts, s)
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
//   stay correct: overlay wins over durable) and are immutable — Add only
//   appends new slots, and the fork-switch unwind DRAINS the promoter before
//   evicting (block.go), so EvictFrom can never race the worker's reads.
// - One job in flight at a time; the next chunk builds only after apply, so
//   the alpenglow promotion gate re-checks every span it folds.
// - A completed-but-unapplied fold on exit is identical to the supported
//   "crash after commit, before state-file update" case: RecoverFoldState
//   reconciles the store frontier forward on the next start.
// - A failed fold is retried naturally: LastRootedSlot did not advance, so the
//   next iteration rebuilds the same chunk. The stake-index flush that already
//   landed is a harmless superset (second flush is a no-op).

// foldJob is an immutable snapshot of one K-slot fold chunk.
type foldJob struct {
	chunk       []accounts.SlotDelta
	through     uint64
	bankhashes  map[uint64][32]byte
	ctx         *state.ResumeContext
	ctxJSON     []byte
	stakeIdxDir string
}

type foldResult struct {
	job *foldJob
	err error
}

// buildFoldJob snapshots the FIRST fold chunk of the rooted prefix <= through
// (loop thread). force also takes a trailing partial chunk. Returns nil when
// no chunk is ready. A missing/unmarshalable chunk-top context is an error —
// a context-less fold manifest would be unrecoverable, so it must not commit.
func (t *unrootedTail) buildFoldJob(through uint64, force bool) (*foldJob, error) {
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
	ctxJSON, err := json.Marshal(ctx)
	if err != nil {
		return nil, fmt.Errorf("fold chunk through slot %d: marshal resume context: %w", through, err)
	}
	bankhashes := make(map[uint64][32]byte, len(chunk))
	for _, sd := range chunk {
		if bh, ok := t.bankhashes[sd.Slot]; ok {
			bankhashes[sd.Slot] = bh
		}
	}
	return &foldJob{
		chunk:       append([]accounts.SlotDelta(nil), chunk...),
		through:     through,
		bankhashes:  bankhashes,
		ctx:         ctx,
		ctxJSON:     ctxJSON,
		stakeIdxDir: t.stakeIdxDir,
	}, nil
}

// runFoldJob performs the durable half of a fold (worker-safe: no tail
// state). Stake-index entries flush (fsync'd) BEFORE the batch commit — see
// promoteRootedBatched for why that order is a correctness requirement.
func runFoldJob(committer batchCommitter, job *foldJob) error {
	if job.stakeIdxDir != "" {
		if _, err := global.FlushPendingStakePubkeysThrough(job.stakeIdxDir, job.through); err != nil {
			return fmt.Errorf("fold chunk through slot %d: flush stake index: %w", job.through, err)
		}
	}
	if _, err := committer.CommitBatch(job.chunk, job.through, job.bankhashes, job.ctxJSON); err != nil {
		return fmt.Errorf("fold chunk through slot %d: %w", job.through, err)
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
// switch) and returns the retained resume context of the last surviving slot
// so the replay loop can rebuild execution state and re-run the certified
// version. Returns nil when no context for fromSlot-1 is retained (caller
// falls back to the rooted-checkpoint re-replay).
func (t *unrootedTail) unwind(fromSlot uint64) *state.ResumeContext {
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
	for s, c := range t.contexts {
		if s >= fromSlot {
			delete(t.contexts, s)
			continue
		}
		if ctx == nil || s > ctx.Slot {
			ctx = c
		}
	}
	// ctx is the highest retained context with slot < fromSlot: the ACTUAL
	// executed parent. It need not be numerically fromSlot-1 — when slots between
	// it and fromSlot were skipped, they retain no context (only executed held
	// slots call SetContext), so the last executed slot IS the parent bank of the
	// certified block at fromSlot. Returning nil (parent already durably folded,
	// or nothing retained) routes the caller to the rooted-checkpoint fallback.
	return ctx
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
) (promotedThrough uint64, err error) {
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

		overlay.PromotePrefix(chunkThrough)
		for _, sd := range chunk {
			delete(bankhashes, sd.Slot)
		}
		promotedThrough = chunkThrough
	}
	return promotedThrough, err
}
