package replay

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

// Write-after-read work-ahead, off unless MITHRIL_WORK_AHEAD=1.
//
// A transaction whose prerequisites have all been dispatched, and whose
// unfinished prerequisites write none of its accounts (they only read accounts
// it writes), cannot observe their effects. An idle worker may execute it now,
// against committed slot state, into private state. Its result is then
// finished but not committed. The planner's completion event stays the commit:
// in the transaction's normal turn, after every prerequisite has committed, the
// worker the dispatcher hands it to checks that no account the execution could
// read was stored since it began, and publishes it through the same path as a
// fresh execution (processTransaction with the executed output). Only then are
// its dependents released. If the check fails or no execution finished, the
// transaction executes normally; a running one is waited for, since it started
// earlier.
//
// Early executions are Speculative (no shared program cache insertion; they
// allocate from the bank's parameter arena, so a discarded one only leaves
// space that the bank's arena reset reclaims). They are never attempted for
// votes, debug transactions, transactions naming a loader program (deploy,
// upgrade, close and retract are loader instructions, and a CPI target must be
// among the keys) or a durable nonce. A worker takes work-ahead only when no
// ready transaction is waiting.
//
// Failure speculation (MITHRIL_WORK_AHEAD_FAIL_DEPTH > 0): an early execution
// may also overtake up to that many unfinished prerequisites that do write its
// accounts, when each is predicted to fail and its fee payer is not one of the
// transaction's accounts. A failed transaction publishes only its fee payer (and
// an advanced durable nonce), so if the prediction holds the read set is
// unchanged at the transaction's turn and the result is adopted; otherwise the
// version check rejects it and the transaction executes normally. The default
// predictor is the last committed outcome of the prerequisite's fee payer.
// MITHRIL_WORK_AHEAD_PREDICT=account uses the outcome of the most recent
// earlier transaction that wrote one of the prerequisite's non-payer writable
// accounts (committed by the time the prerequisite is dispatched); it is more
// accurate on the traces but fires more often, and measured no faster for more
// CPU. =any speculates on every such prerequisite.
// Predictions only choose what to execute early; they never decide a result.
var (
	workAheadEnabled    = os.Getenv("MITHRIL_WORK_AHEAD") == "1"
	workAheadMaxReaders = specShadowEnvInt("MITHRIL_WORK_AHEAD_MAX_READERS", 4)
	workAheadLog        = os.Getenv("MITHRIL_WORK_AHEAD_LOG") == "1"
	workAheadArenaCount = specShadowEnvInt("MITHRIL_WORK_AHEAD_ARENAS", 16)
	workAheadCancel     = os.Getenv("MITHRIL_WORK_AHEAD_CANCEL") == "1"
	// MITHRIL_WORK_AHEAD_EARLY_INVALIDATE=1: when a prerequisite that an early
	// execution assumed would fail commits successfully, drop that execution if
	// it is still queued and abandon (and, with cancel, stop) it if running,
	// instead of finding out at its turn. Adoption still checks the read set.
	workAheadEarlyInvalidate = os.Getenv("MITHRIL_WORK_AHEAD_EARLY_INVALIDATE") == "1"
	workAheadFailDepth       = specShadowEnvInt("MITHRIL_WORK_AHEAD_FAIL_DEPTH", 0)
	// Failure predictor: "payer" (default), "account" or "any".
	workAheadPredict = os.Getenv("MITHRIL_WORK_AHEAD_PREDICT")
	// MITHRIL_WORK_AHEAD_EARLY_OFFER=1: a transaction whose early execution has
	// started counts as dispatched for its dependents' offers, so the next link
	// of a chain can also start early.
	workAheadEarlyOffer = os.Getenv("MITHRIL_WORK_AHEAD_EARLY_OFFER") == "1"
	// MITHRIL_WORK_AHEAD_PREDICT_PAYER=1: a predicted-failure prerequisite may
	// also be overtaken when its fee payer is one of the transaction's accounts.
	// A failed transaction changes its payer only by its fee, which follows from
	// its message alone, so the early execution is given the payer's balance as
	// it will be if the prerequisite fails, and that account is checked by
	// value instead of by version at the transaction's turn.
	workAheadPredictPayer = os.Getenv("MITHRIL_WORK_AHEAD_PREDICT_PAYER") == "1"
	// MITHRIL_WORK_AHEAD_VERIFY=1: never adopt; execute every transaction
	// normally and compare valid early results with the normal ones (diagnostic).
	workAheadVerify = os.Getenv("MITHRIL_WORK_AHEAD_VERIFY") == "1"
	// MITHRIL_WORK_AHEAD_QUEUE_PRIORITY=1 (direct dispatch): early-execution
	// candidates are taken highest chain first instead of in offer order.
	workAheadQueuePriority = os.Getenv("MITHRIL_WORK_AHEAD_QUEUE_PRIORITY") == "1"
	// MITHRIL_WORK_AHEAD_MIN_KCU: never execute early a transaction requesting
	// less compute (thousands of CU, requestedKCU); its early run saves little
	// and still costs an offer, an execution and a validation.
	workAheadMinKCU = int32(specShadowEnvInt("MITHRIL_WORK_AHEAD_MIN_KCU", 0))
	// MITHRIL_WORK_AHEAD_NONCE=1: durable-nonce transactions may also execute
	// early, and their failure is predicted like any other: the payer loses the
	// fee and, as the nonce rollback publishes it, a rent-exempt payer's rent
	// epoch is normalized. Their own nonce account is not predicted.
	workAheadNonce = os.Getenv("MITHRIL_WORK_AHEAD_NONCE") == "1"
	// MITHRIL_WORK_AHEAD_VALUE_CHECK=1: at the turn, a declared account whose
	// version changed still validates if its content equals what the early
	// execution read, provided no version changed while it ran (so what it read
	// is what was captured). A store that leaves an account unchanged installs a
	// new version, so this adopts results the version check alone discards.
	// Off by default, and not implied by any other setting: without it every
	// adoption rests on the version check alone.
	workAheadValueCheck = os.Getenv("MITHRIL_WORK_AHEAD_VALUE_CHECK") == "1"
	// MITHRIL_WORK_AHEAD_WASTE_BUDGET_KCU (default 48000, one block's worth of
	// compute; 0 = unlimited) is a hard bound, in compute units, on a block's
	// discarded early executions. Each launch first reserves the transaction's
	// requested compute (requestedKCU, never below its compute-unit limit, which
	// the VM enforces) and launches only if discarded work plus all outstanding
	// reservations plus its own stay within the budget; at the end of the run the
	// reservation becomes the CU actually used, charged as waste if the result is
	// discarded or never claimed. So discarded speculative compute never exceeds
	// the budget (no overshoot in CU); wall-clock time is not bounded by it.
	workAheadWasteBudgetKCU = int64(specShadowEnvInt("MITHRIL_WORK_AHEAD_WASTE_BUDGET_KCU", 48000))
	// MITHRIL_WORK_AHEAD_WASTE_CAP_KCU (default 6000; 0 = off): once a block's
	// discarded early compute reaches it, the block admits no more early
	// executions. It bounds only work already thrown away, not compute held by
	// running or finished early executions, so ordinary heavy blocks never hit
	// it (the heavy corpus wastes at most 3.6M CU in any block, 0.14M on
	// average), while transactions crafted to fail predictions cannot keep
	// workers busy with doomed work for a whole block.
	workAheadWasteCapKCU = int64(specShadowEnvInt("MITHRIL_WORK_AHEAD_WASTE_CAP_KCU", 6000))
	// MITHRIL_WORK_AHEAD_JOIN_DOOMED=1 restores waiting, at a transaction's
	// turn, for its doomed early execution to end before the normal execution
	// starts. By default the normal execution starts at once and the doomed one
	// ends on its own worker (finish still joins it before the block ends): a
	// doomed run's length is then not added to its chain, which transactions
	// crafted to fail predictions could otherwise exploit. The two executions
	// share nothing mutable: the doomed one has its own arena and private
	// account copies, inserts no programs, and its result is dropped.
	workAheadJoinDoomed = os.Getenv("MITHRIL_WORK_AHEAD_JOIN_DOOMED") == "1"
	// When positive, at most this many early executions run at once.
	workAheadMaxRunning = int64(specShadowEnvInt("MITHRIL_WORK_AHEAD_MAX_RUNNING", 0))

	workAheadArenasOnce sync.Once
	workAheadArenas     chan *arena.Arena[sealevel.BorrowedAccount]
)

const (
	workAheadIdle = iota // offered before, not admissible then; may be offered again
	workAheadQueued
	workAheadRunning
	workAheadFinished // executed, result buffered, not committed
	workAheadClaimed  // handed to the committing worker
	workAheadDropped  // never started, or discarded
)

type workAheadEntry struct {
	cancel     atomic.Bool
	mu         sync.Mutex
	state      int
	ineligible bool // never execute early (checked once)
	checked    bool
	assumed    int // predicted failures the queued offer relies on
	// viaUnchanged: the queued offer passed a prerequisite on the unchanged
	// history that the failure predictor refused (diagnostic only)
	viaUnchanged bool
	// unfinished prerequisites at the queued offer
	unfinished  [workAheadOfferMax]uint32
	nUnfinished uint8
	// predicted: accounts given to the early execution as they will be if its
	// predicted failures hold; validated by value at the turn.
	predicted map[[32]byte]*accounts.Account
	verifying bool          // verify mode: kept until compared with the normal result
	finished  chan struct{} // closed when an execution that started has ended
	keys      [][32]byte
	versions  []*accounts.Account
	// view is the early execution's only route to account state: read-only,
	// recording the version of every account read through it. Adoption requires
	// every recorded read to be unchanged and no write to have been attempted.
	view *accounts.RecordingReader
	// base: the declared accounts as captured; stable: no captured version
	// changed while the execution ran (value check).
	base   []*accounts.Account
	stable bool
	// waste budget (kCU): reservedKCU is this entry's share of the workAhead's
	// reservedKCU, the requested compute while running and the compute used
	// once finished, until the result is adopted or discarded (settle);
	// usedKCU is the compute the execution used.
	reservedKCU, usedKCU int64
	// canonical: verify mode's execution at the turn, against committed state
	canonical *LoadAndExecuteTransactionOutput
	// canonicalPanic: the verify execution panicked (reported as a mismatch)
	canonicalPanic string
	output         LoadAndExecuteTransactionOutput
	arena          *arena.Arena[sealevel.BorrowedAccount]
	panicked       bool
	// abandoned: invalid while still running; the runner drops the result.
	abandoned bool
	// uncovered: the execution loaded an account outside its read set
	uncovered bool
	// raceCancel: set when the execution ends, stopping a racing private run
	// of the same transaction at its turn (MITHRIL_WORK_AHEAD_RACE)
	raceCancel *atomic.Bool
	// began and ended bound the early execution, and queued is when the offer
	// was queued (only while tracing), for diagnostics.
	began, ended, queued time.Time
}

type workAheadStats struct {
	offered, offeredFail, executed, adopted, waited, invalid, abandoned, late, noArena, panicked atomic.Int64
	predictedPayer, verified, verifiedPredicted, verifyMismatch, verifiedNonce                   atomic.Int64
	offerNs, overBudget, valueRescued                                                            atomic.Int64
	// committing workers' time blocked on a running early execution: valid (waited) and doomed (abandoned)
	waitedNs, abandonWaitNs      atomic.Int64
	unjoined, wasteCapped        atomic.Int64 // doomed runs left running at their turn; admissions refused by the waste cap
	sealedWrite, extraReads      atomic.Int64 // results that tried to write through the sealed view; accounts read outside the captured read set
	earlyDropped, earlyAbandoned atomic.Int64
	// Diagnostic, record only: early executions whose offer was admitted only
	// because the unchanged history let it pass a prerequisite the failure
	// predictor refused (MITHRIL_WORK_AHEAD_UNCHANGED), and what became of them.
	unchangedQueued, unchangedExecuted, unchangedAdopted, unchangedInvalid atomic.Int64
	unchangedAdoptedKCU, unchangedHeadStartNs                              atomic.Int64
	uncovered                                                              atomic.Int64 // results that loaded an account outside their read set
	race                                                                   workAheadRaceStats
	// Diagnostic (MITHRIL_WORK_AHEAD_LOG): early executions by the number of
	// predicted failures they assumed, and offers turned away only by
	// MITHRIL_WORK_AHEAD_FAIL_DEPTH, by the depth they would have needed.
	byDepth [workAheadDepthBins]struct{ executed, adopted, invalid, abandoned atomic.Int64 }
	capped  [workAheadDepthBins]atomic.Int64
}

// workAheadDepthBins: assumed-failure counts 0..14, the last bin 15 or more.
const workAheadDepthBins = 16

func workAheadDepthBin(assumed int) int { return min(assumed, workAheadDepthBins-1) }

// payerOutcomes remembers whether each fee payer's last committed transaction
// failed, across blocks, in shards emptied when full; it feeds the failure
// predictor only.
var payerOutcomes [64]struct {
	mu     sync.RWMutex
	failed map[solana.PublicKey]bool
	_      [32]byte
}

const payerOutcomesShardLimit = 1 << 12

func recordPayerOutcome(payer solana.PublicKey, failed bool) {
	shard := &payerOutcomes[payer[0]%64]
	shard.mu.Lock()
	if shard.failed == nil || len(shard.failed) >= payerOutcomesShardLimit {
		shard.failed = make(map[solana.PublicKey]bool, payerOutcomesShardLimit)
	}
	shard.failed[payer] = failed
	shard.mu.Unlock()
}

func payerLastFailed(payer solana.PublicKey) bool {
	shard := &payerOutcomes[payer[0]%64]
	shard.mu.RLock()
	failed := shard.failed[payer]
	shard.mu.RUnlock()
	return failed
}

type workAhead struct {
	slotCtx *sealevel.SlotCtx
	overlay *accounts.OverlayAccounts
	txs     []*solana.Transaction
	txMetas []*rpc.TransactionMeta
	execute []bool
	access  []atomic.Pointer[plannerTransactionAccounts] // built on demand
	dbgOpts *DebugOptions
	entries []atomic.Pointer[workAheadEntry]
	queue   chan int
	// offers carries the dispatcher's offers to the offer goroutine, which
	// decides admissibility off the dispatcher's path. offerDone is made when
	// that goroutine starts and closed when it exits; it stays nil when no
	// goroutine started (an empty plan), so finish does not wait for one.
	offers    chan workAheadOffer
	offerDone chan struct{}
	started   chan int       // early executions that began, for the dispatcher
	events    chan planEvent // direct dispatch: worker events for the offer planner
	onSend    func(tx int)   // diagnostics: observes each dispatch
	// outcome[i]: 0 not committed, 1 succeeded, 2 failed. lastWriter[i]: the
	// most recent earlier transaction writing one of i's non-payer writable
	// accounts, or -1; built by the offer goroutine before its first offer.
	outcome    []atomic.Uint32
	lastWriter []int32
	// payer prediction: transactions by fee payer (built once, on demand) and
	// each transaction's failure fee plus one (0: not computed yet)
	payerTxsOnce sync.Once
	payerTxs     map[solana.PublicKey][]int32
	failFee      []atomic.Uint64
	// program predictor: program combination keys and execution times per
	// transaction (0: not known), and this block's committed failures per
	// non-payer writable account ([fails, total])
	progKey   []atomic.Uint64
	durNs     []atomic.Int64
	acctMu    sync.Mutex
	acctFails map[solana.PublicKey][2]uint32
	running   atomic.Int64
	// assumedBy[p]: transactions whose queued early execution assumed p fails
	// (written by the offer goroutine, copy on write; read by committing workers)
	assumedBy []atomic.Pointer[[]int32]
	wastedNs  atomic.Int64 // worker time of discarded early executions (diagnostics)
	// waste budget, in thousands of CU: discarded work, and the liability of
	// executions not yet settled (requested compute while running, compute
	// used once finished and not yet adopted or discarded)
	wastedKCU, reservedKCU atomic.Int64
	stats                  workAheadStats
	// aheadHeap, when set, orders queued candidates by height; each queued
	// candidate also sends one token on queue.
	aheadHeap *readyHeap
	funnel    *workAheadFunnel // completion funnel diagnostic, or nil

	slot      uint64
	ranMarked atomic.Bool // the slot is recorded as having run early executions (footer shutoff)
	pressure  *workAheadPressure
	claims    *workAheadClaims // MITHRIL_WORK_AHEAD_CLAIM_LOG
}

// newWorkAhead returns nil when work-ahead is off or cannot run for this view.
func newWorkAhead(slotCtx *sealevel.SlotCtx, view *b.Block, execute []bool, dbgOpts *DebugOptions) *workAhead {
	// A locally produced block is never executed early: the producer's own
	// execution is its bank, and nothing checks it against another footer.
	if !workAheadEnabled || workAheadTripped.Load() || slotCtx == nil || view == nil || view.FromLocalProduction || !schedulingOnFor(slotCtx.Slot) {
		return nil
	}
	overlay, ok := slotCtx.Accounts.(*accounts.OverlayAccounts)
	if !ok {
		return nil
	}
	workAheadArenasOnce.Do(func() {
		workAheadArenas = make(chan *arena.Arena[sealevel.BorrowedAccount], workAheadArenaCount)
		for range workAheadArenaCount {
			var a *arena.Arena[sealevel.BorrowedAccount]
			if len(sealevel.BorrowedAccountArenas) > 0 && sealevel.BorrowedAccountArenas[0] != nil {
				a = arena.New[sealevel.BorrowedAccount](sealevel.BorrowedAccountArenas[0].Capacity())
			}
			workAheadArenas <- a
		}
	})
	w := &workAhead{
		slotCtx:   slotCtx,
		overlay:   overlay,
		txs:       view.Transactions,
		txMetas:   view.TxMetas,
		execute:   execute,
		access:    make([]atomic.Pointer[plannerTransactionAccounts], len(view.Transactions)),
		dbgOpts:   dbgOpts,
		entries:   make([]atomic.Pointer[workAheadEntry], len(view.Transactions)),
		started:   earlyOfferChannel(len(view.Transactions)),
		outcome:   make([]atomic.Uint32, len(view.Transactions)),
		failFee:   make([]atomic.Uint64, len(view.Transactions)),
		progKey:   make([]atomic.Uint64, len(view.Transactions)),
		durNs:     make([]atomic.Int64, len(view.Transactions)),
		assumedBy: make([]atomic.Pointer[[]int32], len(view.Transactions)),
		acctFails: make(map[solana.PublicKey][2]uint32),
		offers:    make(chan workAheadOffer, 4*len(view.Transactions)),
		queue:     make(chan int, 2*len(view.Transactions)),
	}
	w.slot = slotCtx.Slot
	if workAheadPressureLog {
		w.pressure = &workAheadPressure{}
	}
	if workAheadClaimLog {
		w.claims = &workAheadClaims{}
	}
	w.funnel = newWorkAheadFunnel(w)
	return w
}

func (w *workAhead) txMeta(idx int) *rpc.TransactionMeta {
	if idx < len(w.txMetas) {
		return w.txMetas[idx]
	}
	return nil
}

const workAheadOfferMax = 8

type workAheadOffer struct {
	tx         int32
	n          uint8
	unfinished [workAheadOfferMax]uint32
}

// dispatch is dispatchDependencyPlan with offers. The dispatcher only copies
// each offer into a fixed-size record; the offer goroutine handles it.
func (w *workAhead) dispatch(plan *dependencyPlan, out chan int, done chan int) {
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
	dispatchDependencyPlanWith(plan, out, done, dispatchHooks{
		maxUnfinished: min(workAheadMaxReaders, workAheadOfferMax),
		offer: func(tx int, unfinished []uint32) {
			if len(unfinished) == 0 || tx >= len(w.execute) || !w.execute[tx] {
				return
			}
			o := workAheadOffer{tx: int32(tx), n: uint8(len(unfinished))}
			copy(o.unfinished[:], unfinished)
			select {
			case w.offers <- o:
			default:
			}
		},
		started: w.started,
		onSend:  w.onSend,
	})
}

func earlyOfferChannel(n int) chan int {
	if !workAheadEarlyOffer {
		return nil
	}
	return make(chan int, n)
}

// workAheadNone marks a transaction that reached its turn with no entry; it is
// shared and permanently dropped.
var workAheadNone = &workAheadEntry{state: workAheadDropped, finished: make(chan struct{})}

// entry returns tx's entry, creating it in state idle if absent.
func (w *workAhead) entry(tx int) *workAheadEntry {
	if e := w.entries[tx].Load(); e != nil {
		return e
	}
	e := &workAheadEntry{finished: make(chan struct{})}
	if w.entries[tx].CompareAndSwap(nil, e) {
		return e
	}
	return w.entries[tx].Load()
}

// offer runs on the offer goroutine: when the dispatcher made it, every
// prerequisite of tx had been dispatched and unfinished had not committed. It
// may arrive late; correctness never depends on admissibility, only on the
// read-set check at the transaction's turn.
func (w *workAhead) offer(tx int, unfinished []uint32) {
	start := time.Now()
	defer func() { w.stats.offerNs.Add(int64(time.Since(start))) }()
	e := w.entry(tx)
	e.mu.Lock()
	defer e.mu.Unlock()
	if !w.admissibleLocked(e, tx) {
		return
	}
	viaUnchanged := false
	predict := w.predictFailureFor
	if workAheadUnchanged {
		// the same decision, noting which predictor made it
		predict = func(prerequisite, tx int) bool {
			if w.predictFailure(prerequisite, tx) {
				return true
			}
			if w.predictUnchangedFor(prerequisite, tx) {
				viaUnchanged = true
				return true
			}
			return false
		}
	}
	ok, assumed := workAheadAdmissible(w.accessOf, w.payerOf, predict, unfinished, tx, workAheadFailDepth, workAheadPredictPayer)
	if !ok {
		if workAheadLog && workAheadFailDepth > 0 && assumed >= workAheadFailDepth {
			if ok, need := workAheadAdmissible(w.accessOf, w.payerOf, w.predictFailureFor, unfinished, tx, len(unfinished), workAheadPredictPayer); ok {
				w.stats.capped[workAheadDepthBin(need)].Add(1)
			}
		}
		return
	}
	if w.queueLocked(e, tx, unfinished, assumed) {
		e.viaUnchanged = viaUnchanged
		if viaUnchanged {
			w.stats.unchangedQueued.Add(1)
		}
	}
}

// admissibleLocked reports whether tx may still be queued for an early
// execution: idle, within the waste budget (else dropped) and eligible.
// Called with e.mu held.
func (w *workAhead) admissibleLocked(e *workAheadEntry, tx int) bool {
	if e.state != workAheadIdle {
		return false
	}
	if workAheadWasteBudgetKCU > 0 && w.wastedKCU.Load()+w.reservedKCU.Load() >= workAheadWasteBudgetKCU {
		e.state = workAheadDropped
		w.stats.overBudget.Add(1)
		return false
	}
	if w.overWasteCap() {
		e.state = workAheadDropped
		w.stats.wasteCapped.Add(1)
		return false
	}
	if !e.checked {
		e.checked = true
		e.ineligible = !workAheadEligible(w.txs[tx]) || isDebugTransaction(w.txs[tx], w.dbgOpts) ||
			(workAheadMinKCU > 0 && requestedKCU(w.txs[tx]) < workAheadMinKCU)
	}
	if e.ineligible {
		e.state = workAheadDropped
		return false
	}
	return true
}

// queueLocked queues tx for an early execution that overtakes unfinished,
// relying on assumed predicted failures. Called with e.mu held.
func (w *workAhead) queueLocked(e *workAheadEntry, tx int, unfinished []uint32, assumed int) bool {
	e.assumed = assumed
	e.nUnfinished = uint8(copy(e.unfinished[:], unfinished))
	if w.aheadHeap != nil {
		w.aheadHeap.push(int32(tx))
	}
	select {
	case w.queue <- tx:
		e.state = workAheadQueued
		if w.onSend != nil {
			e.queued = time.Now()
		}
		if workAheadEarlyInvalidate && assumed > 0 {
			for _, p := range unfinished {
				if workAheadConflicts(w.accessOf, int(p), tx) {
					old := w.assumedBy[p].Load()
					var next []int32
					if old != nil {
						next = append(next, *old...)
					}
					next = append(next, int32(tx))
					w.assumedBy[p].Store(&next)
				}
			}
		}
		return true
	default:
		return false
	}
}

// invalidateAssumers runs when prerequisite p committed successfully: every
// early execution that assumed p fails can no longer be adopted (p wrote one
// of its accounts), so a queued one is dropped before it takes a worker and a
// running one is abandoned (and asked to stop, with cancel).
func (w *workAhead) invalidateAssumers(p int) {
	list := w.assumedBy[p].Load()
	if list == nil {
		return
	}
	for _, tx := range *list {
		e := w.entries[tx].Load()
		if e == nil {
			continue
		}
		e.mu.Lock()
		switch e.state {
		case workAheadQueued:
			e.state = workAheadDropped
			w.stats.earlyDropped.Add(1)
		case workAheadRunning:
			if !e.abandoned {
				e.abandoned = true
				if workAheadCancel {
					e.cancel.Store(true)
				}
				w.stats.earlyAbandoned.Add(1)
			}
		}
		e.mu.Unlock()
	}
}

func (w *workAhead) payerOf(tx int) solana.PublicKey {
	return w.txs[tx].Message.AccountKeys[0]
}

func (w *workAhead) buildLastWriters() {
	w.lastWriter = make([]int32, len(w.txs))
	last := make(map[solana.PublicKey]int32, 4*len(w.txs))
	for tx := range w.txs {
		w.lastWriter[tx] = -1
		payer := w.payerOf(tx)
		writable := w.accessOf(tx).writable()
		for _, a := range writable {
			if a.publicKey == payer {
				continue
			}
			if lw, ok := last[a.publicKey]; ok && lw > w.lastWriter[tx] {
				w.lastWriter[tx] = lw
			}
		}
		for _, a := range writable {
			last[a.publicKey] = int32(tx)
		}
	}
}

// recordOutcome is called by the worker that committed tx, before releasing
// its dependents, so later offers see the outcome. elapsed is the normal
// execution time; an adopted transaction records its early execution's time
// instead. With direct dispatch the program predictor's tables are updated by
// the planner goroutine from these values (observe), off the critical path.
func (w *workAhead) recordOutcome(tx int, err error, elapsed time.Duration, adopted bool) {
	if w == nil || workAheadFailDepth == 0 || len(w.txs[tx].Message.AccountKeys) == 0 {
		return
	}
	failed := err != nil
	if workAheadEarlyInvalidate && !failed {
		w.invalidateAssumers(tx)
	}
	if adopted {
		if e := w.entries[tx].Load(); e != nil {
			e.mu.Lock()
			if !e.began.IsZero() && e.ended.After(e.began) {
				elapsed = e.ended.Sub(e.began)
			}
			e.mu.Unlock()
		}
	}
	w.durNs[tx].Store(int64(elapsed))
	if failed {
		w.outcome[tx].Store(2)
	} else {
		w.outcome[tx].Store(1)
	}
	switch workAheadPredict {
	case "program":
		if w.events == nil {
			w.observe(tx)
		}
	case "account", "any":
	default:
		recordPayerOutcome(w.payerOf(tx), failed)
	}
}

// accessOf returns tx's planner account view, as the plan was built from it.
// Concurrent callers may both build it; the views are identical.
func (w *workAhead) accessOf(tx int) plannerTransactionAccounts {
	if access := w.access[tx].Load(); access != nil {
		return *access
	}
	var access plannerTransactionAccounts
	if meta := w.txMeta(tx); plannerShouldUseMeta(w.txs[tx], meta) {
		access = plannerAccountsFromMeta(w.txs[tx], meta)
	} else {
		access = plannerAccountsFromMessage(&w.txs[tx].Message)
	}
	w.access[tx].Store(&access)
	return access
}

// next returns the next job for a worker: ready work first, work-ahead only
// when none is waiting. ok is false once ready work is exhausted.
func (w *workAhead) next(do <-chan int) (idx int, ahead bool, ok bool) {
	if w == nil {
		idx, ok = <-do
		return idx, false, ok
	}
	select {
	case idx, ok = <-do:
		return idx, false, ok
	default:
	}
	if workAheadMaxRunning > 0 && w.running.Load() >= workAheadMaxRunning {
		idx, ok = <-do
		return idx, false, ok
	}
	select {
	case idx, ok = <-do:
		return idx, false, ok
	case idx = <-w.queue:
		return w.takeAhead(idx), true, true
	}
}

// takeAhead turns a received queue token into the candidate to run: the
// highest queued one when the queue is ordered, else the token itself.
func (w *workAhead) takeAhead(token int) int {
	if w.aheadHeap != nil {
		return int(w.aheadHeap.pop())
	}
	return token
}

// tryNext is next without blocking: got is false when nothing is available.
func (w *workAhead) tryNext(do <-chan int) (idx int, ahead, ok, got bool) {
	select {
	case idx, ok = <-do:
		return idx, false, ok, true
	default:
	}
	if w == nil || (workAheadMaxRunning > 0 && w.running.Load() >= workAheadMaxRunning) {
		return 0, false, false, false
	}
	select {
	case idx = <-w.queue:
		return w.takeAhead(idx), true, true, true
	default:
		return 0, false, false, false
	}
}

// run executes tx early on the calling worker if it has not been claimed.
func (w *workAhead) run(idx int) {
	e := w.entries[idx].Load()
	tx := w.txs[idx]
	e.mu.Lock()
	if e.state != workAheadQueued {
		e.mu.Unlock()
		return
	}
	if w.overWasteCap() {
		e.state = workAheadDropped
		e.mu.Unlock()
		w.stats.wasteCapped.Add(1)
		return
	}
	if !w.reserve(e, tx) {
		e.state = workAheadDropped
		e.mu.Unlock()
		w.stats.overBudget.Add(1)
		return
	}
	var workerArena *arena.Arena[sealevel.BorrowedAccount]
	select {
	case workerArena = <-workAheadArenas:
	default:
		w.reservedKCU.Add(-e.reservedKCU)
		e.reservedKCU = 0
		e.state = workAheadIdle
		e.mu.Unlock()
		w.stats.noArena.Add(1)
		return
	}
	w.stats.offered.Add(1)
	if e.assumed > 0 {
		w.stats.offeredFail.Add(1)
	}
	if e.viaUnchanged {
		w.stats.unchangedExecuted.Add(1)
	}
	w.pressure.started(w.running.Add(1))
	defer w.running.Add(-1)
	if w.ranMarked.CompareAndSwap(false, true) {
		markWorkAheadRan(w.slot)
	}
	e.state = workAheadRunning
	if w.events != nil {
		select {
		case w.events <- planEvent{kind: planEventStarted, tx: int32(idx)}:
		default:
		}
	} else if w.started != nil {
		select {
		case w.started <- idx:
		default:
		}
	}
	e.began = time.Now()
	e.arena = workerArena
	e.keys, e.versions, e.base = workAheadReadSet(w.slotCtx, w.overlay, tx)
	e.view = accounts.NewRecordingReader(w.overlay)
	var overrides map[solana.PublicKey]*accounts.Account
	if workAheadPredictPayer {
		overrides = w.predictPayers(e, idx)
		if len(overrides) > 0 {
			w.stats.predictedPayer.Add(1)
		}
	}
	e.mu.Unlock()

	// The early execution runs on a sealed slot context: every account it
	// reads comes through e.view, which records the version read and refuses
	// writes, and the context's own bookkeeping is private. The block constants
	// are shared with the real context (sealevel.NewSealedSlotCtx).
	sealed := sealevel.NewSealedSlotCtx(w.slotCtx, e.view)
	captureTx := len(tx.Signatures) > 0 && txCaptureActive()
	input := transactionExecutionInput(sealed, tx, w.txMeta(idx), workerArena, false, captureTx)
	input.Speculative = true
	if workAheadCancel {
		input.SpeculativeCancel = &e.cancel
	}
	input.AccountOverrides = overrides
	output, panicked, msg := workAheadExecute(input)
	workAheadInjectDelay(e.assumed)
	if panicked {
		mlog.Log.Errorf("work-ahead execution of tx %s panicked: %s", tx.Signatures[0], msg)
		w.stats.panicked.Add(1)
	} else if !workAheadUsable(&output) {
		mlog.Log.Errorf("work-ahead execution of tx %s returned an output that cannot be published", tx.Signatures[0])
		panicked = true
		w.stats.panicked.Add(1)
	}
	w.stats.executed.Add(1)
	w.stats.byDepth[workAheadDepthBin(e.assumed)].executed.Add(1)
	stable := workAheadValueCheck && w.versionsUnchanged(e)
	w.ran(idx, e, output, panicked, stable)
}

// ran records a finished early execution: it keeps the compute used as the
// entry's liability (refunding the rest of the reservation) until claim or
// finish settles it, and drops the result if claim abandoned it meanwhile. A
// panicked execution keeps its whole reservation, since what it used is not
// known.
func (w *workAhead) ran(idx int, e *workAheadEntry, output LoadAndExecuteTransactionOutput, panicked, stable bool) {
	e.mu.Lock()
	e.stable = stable
	e.ended = time.Now()
	if e.raceCancel != nil {
		e.raceCancel.Store(true)
	}
	e.usedKCU = int64(processTransactionComputeUnits(output.ExecCtx)/1000) + 1
	held := min(e.usedKCU, e.reservedKCU)
	if panicked {
		e.usedKCU = max(e.usedKCU, e.reservedKCU)
		held = e.reservedKCU
	}
	w.reservedKCU.Add(held - e.reservedKCU)
	e.reservedKCU = held
	// Counted for every execution that ended, whether or not its result is
	// kept: an abandoned or panicked one that tried to write still tried.
	wrote := e.view != nil && e.view.Wrote()
	if wrote {
		w.stats.sealedWrite.Add(1)
		sig := "(unsigned)"
		if tx := w.txs[idx]; tx != nil && len(tx.Signatures) > 0 {
			sig = tx.Signatures[0].String()
		}
		mlog.Log.Errorf("work-ahead execution of tx %s tried to write through its sealed account view; result discarded", sig)
	}
	if e.view != nil {
		// keys read that the captured read set does not name (not a count
		// difference: a read outside it is one even if a captured key went unread)
		if extra := e.view.ReadsOutside(e.keys); extra > 0 {
			w.stats.extraReads.Add(int64(extra))
		}
	}
	if e.abandoned {
		w.settle(e, true)
		w.wastedNs.Add(int64(e.ended.Sub(e.began)))
		e.state = workAheadDropped
		if e.arena != nil {
			workAheadArenas <- e.arena
			e.arena = nil
		}
	} else {
		if wrote {
			// It tried to change account state, or to read it in a way that
			// cannot be recorded: whatever it returned is not a result.
			panicked = true
		}
		e.output, e.panicked = output, panicked
		e.uncovered = !panicked && !w.readSetCovers(e)
		e.state = workAheadFinished
		w.funnel.finished(idx)
	}
	close(e.finished)
	e.mu.Unlock()
}

// settle ends an execution's budget liability. Discarded work is charged as
// waste before the liability is released, so admission never sees less than
// the true exposure. Called with e.mu held.
func (w *workAhead) settle(e *workAheadEntry, discarded bool) {
	if discarded {
		w.wastedKCU.Add(e.usedKCU)
	}
	w.reservedKCU.Add(-e.reservedKCU)
	e.reservedKCU = 0
}

// workAheadUsable reports whether an early output has what publishing needs:
// a failed transaction its error, a successful one its execution context. A
// zero output (an execution that never produced one) has neither.
func workAheadUsable(out *LoadAndExecuteTransactionOutput) bool {
	return out.ProcessingResult.TransactionError != nil || out.ExecCtx != nil
}

// workAheadExecute runs an early execution; panicked reports a recovered
// panic separately from its message, which may be empty.
func workAheadExecute(input LoadAndExecuteTransactionInput) (out LoadAndExecuteTransactionOutput, panicked bool, msg string) {
	defer func() {
		if r := recover(); r != nil {
			out, panicked, msg = LoadAndExecuteTransactionOutput{}, true, fmt.Sprint(r)
		}
	}()
	return workAheadLoadAndExecute(input), false, ""
}

// workAheadLoadAndExecute is LoadAndExecuteTransaction; tests replace it.
var workAheadLoadAndExecute = LoadAndExecuteTransaction

// workAheadHeadStart is how long a result that ended at ended had been ready
// at its turn: zero if it ended at or after the turn.
func workAheadHeadStart(turn, ended time.Time) time.Duration {
	if turn.IsZero() || ended.IsZero() || !turn.After(ended) {
		return 0
	}
	return turn.Sub(ended)
}

// claim is called by the worker about to process idx in its normal turn, when
// every prerequisite has committed. It returns a finished execution whose read
// set is unchanged, or nil, and a release to call once processing is done.
func (w *workAhead) claim(idx int) (*LoadAndExecuteTransactionOutput, func()) {
	if w == nil {
		return nil, func() {}
	}
	w.funnel.atTurn(idx)
	// Installing the dropped sentinel where no entry exists stops late offers.
	if w.entries[idx].CompareAndSwap(nil, workAheadNone) {
		return nil, func() {}
	}
	e := w.entries[idx].Load()
	var waitedAt time.Time // the turn, if it waited on a valid running execution
	var turnAt time.Time   // the turn, for the unchanged head-start counter
	e.mu.Lock()
	switch e.state {
	case workAheadIdle:
		e.state = workAheadDropped
		e.mu.Unlock()
		return nil, func() {}
	case workAheadQueued:
		e.state = workAheadDropped
		e.mu.Unlock()
		w.stats.late.Add(1)
		return nil, func() {}
	case workAheadDropped:
		e.mu.Unlock()
		return nil, func() {}
	case workAheadFinished:
		if e.viaUnchanged {
			turnAt = time.Now()
		}
	case workAheadRunning:
		// Every prerequisite has committed, so a changed read set now is final:
		// abandon the execution rather than wait for a result that cannot be used.
		// One already abandoned (early invalidation) discards its result even if
		// the read set turned out unchanged.
		if e.abandoned || !w.readSetMatches(e, true) {
			// The doomed execution drops its result and returns its arena when it
			// ends (ran). The normal execution starts now unless joining is asked
			// for; transaction capture also joins, so one transaction's two
			// executions are never captured at once.
			if !e.abandoned {
				w.stats.abandoned.Add(1)
				w.stats.byDepth[workAheadDepthBin(e.assumed)].abandoned.Add(1)
			}
			e.abandoned = true
			if workAheadCancel {
				e.cancel.Store(true)
			}
			e.mu.Unlock()
			if !workAheadJoinDoomed && !txCaptureActive() {
				w.stats.unjoined.Add(1)
				return nil, func() {}
			}
			waitStart := time.Now()
			<-e.finished
			w.stats.abandonWaitNs.Add(int64(time.Since(waitStart)))
			return nil, func() {}
		}
		var raceCancel *atomic.Bool
		if w.racing() {
			raceCancel = new(atomic.Bool)
			e.raceCancel = raceCancel
		}
		e.mu.Unlock()
		w.stats.waited.Add(1)
		waitStart := time.Now()
		if raceCancel != nil && !workAheadAwait(e.finished, workAheadRaceAfter) {
			if out, release := w.race(idx, e, raceCancel); out != nil {
				w.stats.waitedNs.Add(int64(time.Since(waitStart)))
				return out, release
			}
		}
		<-e.finished
		w.stats.waitedNs.Add(int64(time.Since(waitStart)))
		e.mu.Lock()
		// Abandoned meanwhile: run discarded the result.
		if e.state != workAheadFinished {
			e.mu.Unlock()
			return nil, func() {}
		}
		waitedAt = waitStart
	default:
		// Claimed, or a state claim never sees: the turn is processed once.
		state := e.state
		e.mu.Unlock()
		panic(fmt.Sprintf("work-ahead: transaction %d reached its turn in state %d", idx, state))
	}
	release := func() {
		if e.arena != nil {
			workAheadArenas <- e.arena
			e.arena = nil
		}
	}
	// Only a finished, non-panicked, publishable result with an unchanged read
	// set is adopted.
	if e.panicked || !workAheadUsable(&e.output) || e.uncovered || !w.readSetUnchanged(e) {
		w.settle(e, true)
		w.wastedNs.Add(int64(e.ended.Sub(e.began)))
		e.state = workAheadDropped
		e.output = LoadAndExecuteTransactionOutput{}
		release()
		e.mu.Unlock()
		if e.viaUnchanged {
			w.stats.unchangedInvalid.Add(1)
		}
		w.stats.invalid.Add(1)
		w.stats.byDepth[workAheadDepthBin(e.assumed)].invalid.Add(1)
		return nil, func() {}
	}
	if workAheadVerify {
		// Differential oracle: execute again at the turn, against the
		// committed state the canonical execution will see, without side
		// effects; verify compares everything adoption would consume.
		e.verifying = true
		w.settle(e, false)
		e.mu.Unlock()
		input := transactionExecutionInput(w.slotCtx, w.txs[idx], w.txMeta(idx), nil, false, false)
		input.Speculative = true
		canonStart := time.Now()
		canonical, canonicalPanicked, msg := workAheadExecute(input)
		canonNs := int64(time.Since(canonStart))
		e.mu.Lock()
		if !waitedAt.IsZero() {
			w.claims.record(idx, e, waitedAt, canonNs)
		}
		e.canonical = &canonical
		if canonicalPanicked {
			e.canonicalPanic = "verify execution panicked: " + msg
		}
		e.mu.Unlock()
		return nil, func() {}
	}
	e.state = workAheadClaimed
	w.settle(e, false)
	if !waitedAt.IsZero() {
		w.claims.record(idx, e, waitedAt, -1)
	}
	output := &e.output
	// The result was produced on a sealed context; it is committed against
	// the real one.
	if output.ExecCtx != nil {
		output.ExecCtx.SlotCtx = w.slotCtx
	}
	w.injectFault(output)
	if e.viaUnchanged {
		w.stats.unchangedAdopted.Add(1)
		w.stats.unchangedAdoptedKCU.Add(e.usedKCU)
		// How long the result had been ready when its turn came, taken at
		// the turn and not after the wait or the read-set check: nothing for
		// a result its turn had to wait for.
		if !waitedAt.IsZero() {
			turnAt = waitedAt
		}
		w.stats.unchangedHeadStartNs.Add(int64(workAheadHeadStart(turnAt, e.ended)))
	}
	e.mu.Unlock()
	w.stats.adopted.Add(1)
	w.stats.byDepth[workAheadDepthBin(e.assumed)].adopted.Add(1)
	return output, func() {
		e.mu.Lock()
		e.output = LoadAndExecuteTransactionOutput{}
		release()
		e.mu.Unlock()
	}
}

// verify, in verify mode, compares a valid early result of idx with the normal
// execution that just committed (err and cu as processTransaction returned),
// before idx's dependents are released, then releases the early result.
func (w *workAhead) verify(idx int, err error, cu uint64) {
	if w == nil || !workAheadVerify {
		return
	}
	e := w.entries[idx].Load()
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.verifying {
		return
	}
	e.verifying = false
	out := e.output
	w.stats.verified.Add(1)
	if len(e.predicted) > 0 {
		w.stats.verifiedPredicted.Add(1)
	}
	tx := w.txs[idx]
	if advancesNonce(tx) {
		w.stats.verifiedNonce.Add(1)
	}
	diff := e.canonicalPanic
	if diff == "" && e.canonical != nil {
		diff = compareOutputs(&out, e.canonical)
	}
	attemptErr := out.ProcessingResult.TransactionError
	switch {
	case diff != "":
	case (attemptErr != nil) != (err != nil):
		diff = "outcome"
	case attemptErr != nil && specTxErrString(attemptErr) != err.Error():
		diff = "error"
	case processTransactionComputeUnits(out.ExecCtx) != cu:
		diff = "cu"
	case attemptErr == nil && out.ExecCtx != nil:
		for _, acct := range out.ExecCtx.TransactionContext.Accounts.Accounts {
			if acct == nil || acct.IsDummy || acct.Key == sealevel.SysvarInstructionsAddr {
				continue
			}
			committed, cerr := w.slotCtx.GetAccountShared(acct.Key)
			if cerr != nil {
				committed = nil
			}
			if !specSameAccount(acct, committed) {
				diff = fmt.Sprintf("account %s: early %s, normal %s", acct.Key, specDescribe(acct), specDescribe(committed))
				break
			}
		}
	}
	if diff != "" {
		w.stats.verifyMismatch.Add(1)
		msg := fmt.Sprintf("work-ahead VERIFY MISMATCH tx %s (predicted payer %v): %s", tx.Signatures[0], len(e.predicted) > 0, diff)
		mlog.Log.Errorf("%s", msg)
		panic(msg) // verify mode is a validation run: fail closed
	}
	e.output = LoadAndExecuteTransactionOutput{}
	e.canonical, e.canonicalPanic = nil, ""
	if e.arena != nil {
		workAheadArenas <- e.arena
		e.arena = nil
	}
}

// executionWindow reports, for diagnostics, when tx's early execution ran.
// Call only after tx was processed.
func (w *workAhead) executionWindow(tx int) (began, ended time.Time, ok bool) {
	if w == nil || w.entries[tx].Load() == nil {
		return began, ended, false
	}
	e := w.entries[tx].Load()
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.began, e.ended, !e.began.IsZero()
}

// queuedTime is when tx's offer was queued for early execution, or zero; set
// only while tracing.
func (w *workAhead) queuedTime(tx int) time.Time {
	if w == nil || w.entries[tx].Load() == nil {
		return time.Time{}
	}
	e := w.entries[tx].Load()
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.queued
}

// predictPayers returns accounts of tx that earlier, uncommitted transactions
// pay fees from, as they will be once all of those fail: the committed state
// minus every such fee. That covers the unfinished prerequisites tx overtakes
// and, in a run of one payer's transactions each speculating past the last,
// the ones before them. A failed transaction publishes its fee payer with only
// the lamports reduced (an ordinary-blockhash rollback keeps the loaded rent
// epoch; a durable-nonce rollback normalizes a rent-exempt payer's epoch, so
// the failures are applied in order). Called with e.mu held, after the read
// set capture.
func (w *workAhead) predictPayers(e *workAheadEntry, tx int) map[solana.PublicKey]*accounts.Account {
	mine := w.txs[tx].Message.AccountKeys
	overtaken := false
	for _, p := range e.unfinished[:e.nUnfinished] {
		if w.outcome[p].Load() == 0 && slices.Contains(mine, w.payerOf(int(p))) {
			overtaken = true
		}
	}
	if !overtaken {
		return nil
	}
	w.payerTxsOnce.Do(func() {
		w.payerTxs = make(map[solana.PublicKey][]int32)
		for i, t := range w.txs {
			if len(t.Message.AccountKeys) > 0 && i < len(w.execute) && w.execute[i] {
				w.payerTxs[t.Message.AccountKeys[0]] = append(w.payerTxs[t.Message.AccountKeys[0]], int32(i))
			}
		}
	})
	// The fee-payer check, the account loaders (existence, data length, owner,
	// executable, whether lamports are zero, loaded-data size), rent pre-states
	// and execution all read the predicted accounts, never slot state, so the
	// value checked at the turn is everything the execution observed of them.
	// The age check reads only tx's nonce account, so that is never predicted.
	// A prediction changes lamports and rent epoch only and must leave lamports
	// above zero.
	nonceKey, isNonce := nonceAccountOf(w.txs[tx])
	var overrides map[solana.PublicKey]*accounts.Account
	var rent sealevel.SysvarRent
	haveRent := false
	for _, key := range mine {
		var predicted *accounts.Account
		for _, j := range w.payerTxs[key] {
			if int(j) >= tx {
				break
			}
			if w.outcome[j].Load() != 0 {
				continue // committed: its effect is in the state already
			}
			fee, ok := w.failureFee(int(j))
			if !ok {
				e.predicted = nil
				return nil
			}
			if predicted == nil {
				committed, err := w.slotCtx.GetAccountShared(key)
				if err != nil || committed == nil {
					e.predicted = nil
					return nil
				}
				predicted = committed.Clone()
			}
			if predicted.Lamports <= fee || (isNonce && key == nonceKey) {
				e.predicted = nil
				return nil
			}
			// as handleFailedTx: a durable-nonce rollback keeps the rent epoch
			// normalized on the pre-fee balance; an ordinary one keeps the loaded epoch
			if advancesNonce(w.txs[j]) && !sealevel.IsRecentBlockhashTransaction(w.txs[j], w.slotCtx) {
				if !haveRent {
					rent, haveRent = fees.RentForSlot(w.slotCtx), true
				}
				if predicted.RentEpoch != math.MaxUint64 && rent.IsExempt(predicted.Lamports, uint64(len(predicted.Data))) {
					predicted.RentEpoch = math.MaxUint64
				}
			}
			predicted.Lamports -= fee
		}
		if predicted != nil {
			if overrides == nil {
				overrides = make(map[solana.PublicKey]*accounts.Account)
				e.predicted = make(map[[32]byte]*accounts.Account)
			}
			overrides[key] = predicted
			e.predicted[[32]byte(key)] = predicted
		}
	}
	return overrides
}

// failureFee is the fee transaction tx pays if it fails, as the failure path
// computes it: from its signatures and compute budget, never from execution.
// Durable-nonce transactions are predicted only with MITHRIL_WORK_AHEAD_NONCE
// (predictPayers applies their rent-epoch normalization; the nonce advance
// touches only their own nonce account).
func (w *workAhead) failureFee(tx int) (uint64, bool) {
	if w.failFee != nil {
		if cached := w.failFee[tx].Load(); cached != 0 {
			return cached - 1, true
		}
	}
	t := w.txs[tx]
	if !workAheadEligible(t) {
		return 0, false
	}
	instrs, _, _, err := instrsAndAcctMetasFromTx(t, w.slotCtx.Features)
	if err != nil {
		return 0, false
	}
	limits, err := sealevel.ComputeBudgetLimitsForTransaction(t, instrs, w.slotCtx.Features)
	if err != nil {
		return 0, false
	}
	fee := fees.CalculateTxFees(t, instrs, limits, w.slotCtx.Features).TotalFee
	if w.failFee != nil {
		w.failFee[tx].Store(fee + 1)
	}
	return fee, true
}

// readSetUnchanged checks the read set at the transaction's turn: predicted
// accounts by value, every other account by version.
func (w *workAhead) readSetUnchanged(e *workAheadEntry) bool {
	return w.readSetMatches(e, e.stable)
}

// readSetMatches is readSetUnchanged with the value check's stability given:
// claim passes true for a still-running execution, whose stability is not
// known yet, so it abandons only executions no outcome can save.
func (w *workAhead) readSetMatches(e *workAheadEntry, stable bool) bool {
	for i, key := range e.keys {
		if predicted, ok := e.predicted[key]; ok {
			committed, err := w.slotCtx.GetAccountShared(solana.PublicKey(key))
			if err != nil || !specSameInput(committed, predicted) {
				return false
			}
			continue
		}
		if w.overlay.DeltaVersion(key) != e.versions[i] {
			if !workAheadValueCheck || !stable || i >= len(e.base) || e.base[i] == nil {
				return false
			}
			committed, err := w.slotCtx.GetAccountShared(solana.PublicKey(key))
			if err != nil || !specSameInput(committed, e.base[i]) {
				return false
			}
			w.stats.valueRescued.Add(1)
		}
	}
	return w.viewUnchanged(e)
}

// viewUnchanged checks every account the early execution actually read through
// its sealed view, including any outside the captured read set: each must still
// have the version it was read at. Predicted accounts never reach the view's
// verdict (the loop above compares them by value). With the value check on, the
// captured keys are left to that loop too, which may accept a moved version by
// content; without it every recorded read must hold here, whatever the loop
// above concluded.
func (w *workAhead) viewUnchanged(e *workAheadEntry) bool {
	if e.view == nil {
		return true
	}
	return e.view.Unchanged(func(key [32]byte) bool {
		if _, ok := e.predicted[key]; ok {
			return true
		}
		if !workAheadValueCheck {
			return false
		}
		for _, k := range e.keys {
			if k == key {
				return true
			}
		}
		return false
	})
}

// readSetCovers reports whether every account the early execution loaded into
// its transaction context, executable accounts included, is in its read set,
// so that the version check covers everything it read. Today the read set is
// the declared accounts plus upgradeable program data, which is all a
// transaction can load; a runtime change that loads more fails closed here
// instead of being adopted unchecked. Checked by the early execution's worker
// when it finishes (ran), off the committing path. Called with e.mu held.
func (w *workAhead) readSetCovers(e *workAheadEntry) bool {
	if e.output.ExecCtx == nil || e.output.ExecCtx.TransactionContext == nil {
		return true // a failed result publishes only its payer and nonce, both declared
	}
	txCtx := e.output.ExecCtx.TransactionContext
	covered := func(key solana.PublicKey) bool {
		if key == sealevel.SysvarInstructionsAddr {
			return true // built from the transaction itself
		}
		for _, k := range e.keys {
			if k == [32]byte(key) {
				return true
			}
		}
		return false
	}
	for _, acct := range txCtx.Accounts.Accounts {
		if acct != nil && !covered(acct.Key) {
			w.stats.uncovered.Add(1)
			return false
		}
	}
	for i := range txCtx.ExecutableAccounts {
		if acct := txCtx.ExecutableAccounts[i].Account; acct != nil && !covered(acct.Key) {
			w.stats.uncovered.Add(1)
			return false
		}
	}
	return true
}

// versionsUnchanged reports whether every captured version other than the
// predicted accounts is still current. e.keys, e.versions and e.predicted are
// not modified once the execution started.
func (w *workAhead) versionsUnchanged(e *workAheadEntry) bool {
	for i, key := range e.keys {
		if _, ok := e.predicted[key]; ok {
			continue
		}
		if w.overlay.DeltaVersion(key) != e.versions[i] {
			return false
		}
	}
	return true
}

// finish returns any arena still held (an entry finished but never claimed,
// which only happens when the loop stopped early) and logs the block's counts.
func (w *workAhead) finish(slot uint64) {
	if w == nil {
		return
	}
	if w.offerDone != nil {
		<-w.offerDone
	}
	for i := range w.entries {
		e := w.entries[i].Load()
		if e == nil {
			continue
		}
		e.mu.Lock()
		if e.state == workAheadRunning {
			e.mu.Unlock()
			<-e.finished
			e.mu.Lock()
		}
		if e.state == workAheadFinished {
			w.settle(e, true) // executed, never claimed
		}
		if e.arena != nil {
			workAheadArenas <- e.arena
			e.arena = nil
		}
		e.mu.Unlock()
	}
	w.funnel.report(slot)
	if workAheadLog {
		s := &w.stats
		mlog.Log.Infof("work-ahead slot %d: offered %d offered-fail %d executed %d adopted %d waited %d invalid %d abandoned %d late %d no-arena %d panicked %d offer-us %d predicted-payer %d verified %d verified-predicted %d verify-mismatch %d verified-nonce %d over-budget %d wasted-kcu %d wasted-us %d value-rescued %d waited-us %d abandon-wait-us %d early-dropped %d early-abandoned %d unjoined %d waste-capped %d sealed-write %d extra-reads %d",
			slot, s.offered.Load(), s.offeredFail.Load(), s.executed.Load(), s.adopted.Load(), s.waited.Load(), s.invalid.Load(), s.abandoned.Load(), s.late.Load(), s.noArena.Load(), s.panicked.Load(), s.offerNs.Load()/1000,
			s.predictedPayer.Load(), s.verified.Load(), s.verifiedPredicted.Load(), s.verifyMismatch.Load(), s.verifiedNonce.Load(), s.overBudget.Load(), w.wastedKCU.Load(), w.wastedNs.Load()/1000, s.valueRescued.Load(), s.waitedNs.Load()/1000, s.abandonWaitNs.Load()/1000, s.earlyDropped.Load(), s.earlyAbandoned.Load(), s.unjoined.Load(), s.wasteCapped.Load(), s.sealedWrite.Load(), s.extraReads.Load())
		if workAheadUnchanged {
			h := &unchangedReasons
			mlog.Log.Infof("work-ahead unchanged slot %d: queued %d executed %d adopted %d invalid %d adopted-kcu %d head-start-us %d | asked %d predicted %d payer-shared %d no-history %d few-observed %d last-changed %d low-rate %d",
				slot, s.unchangedQueued.Load(), s.unchangedExecuted.Load(), s.unchangedAdopted.Load(), s.unchangedInvalid.Load(), s.unchangedAdoptedKCU.Load(), s.unchangedHeadStartNs.Load()/1000,
				h.asked.Swap(0), h.predicted.Swap(0), h.payerShared.Swap(0), h.noHistory.Swap(0), h.fewObserved.Swap(0), h.lastChanged.Swap(0), h.lowRate.Swap(0))
		}
		// Per assumed-failure count d: d:executed/adopted/invalid/abandoned/capped.
		var depth strings.Builder
		for d := range s.byDepth {
			b := &s.byDepth[d]
			ex, ad, in, ab, cp := b.executed.Load(), b.adopted.Load(), b.invalid.Load(), b.abandoned.Load(), s.capped[d].Load()
			if ex|ad|in|ab|cp != 0 {
				fmt.Fprintf(&depth, " %d:%d/%d/%d/%d/%d", d, ex, ad, in, ab, cp)
			}
		}
		mlog.Log.Infof("work-ahead depth slot %d:%s", slot, depth.String())
		if workAheadRace {
			r := &s.race
			mlog.Log.Infof("work-ahead race slot %d: won %d lost %d won-us %d lag-us %d", slot, r.won.Load(), r.lost.Load(), r.wonNs.Load()/1000, r.lagNs.Load()/1000)
		}
		w.pressure.log(slot, s.uncovered.Load(), s.waitedNs.Load())
	}
	if workAheadClaimLog {
		w.claims.log(slot)
	}
}

// workAheadAdmissible decides whether tx may execute while prerequisites are
// unfinished. One that writes none of tx's accounts only reads accounts tx
// writes, so it cannot affect tx. One that writes an account of tx may be
// overtaken only on a failure prediction, at most failDepth of them, and only
// if its fee payer, which its fee always changes, is not an account of tx.
// Hazards are checked per account, so a pair that conflicts both ways is never
// treated as read-only. assumed counts the predicted failures relied on.
func workAheadAdmissible(accessOf func(int) plannerTransactionAccounts, payerOf func(int) solana.PublicKey, predictFailure func(prerequisite, tx int) bool, prerequisites []uint32, tx int, failDepth int, allowPayer bool) (ok bool, assumed int) {
	own := accessOf(tx).accounts
	uses := func(key solana.PublicKey) bool {
		for _, a := range own {
			if a.publicKey == key {
				return true
			}
		}
		return false
	}
	for _, prerequisite := range prerequisites {
		conflicts := false
		for _, a := range accessOf(int(prerequisite)).writable() {
			if uses(a.publicKey) {
				conflicts = true
				break
			}
		}
		if !conflicts {
			continue
		}
		if assumed >= failDepth || (!allowPayer && uses(payerOf(int(prerequisite)))) || !predictFailure(int(prerequisite), tx) {
			return false, assumed
		}
		assumed++
	}
	return true, assumed
}

// workAheadConflicts reports whether prerequisite writes an account of tx,
// the case workAheadAdmissible may overtake only on a failure prediction.
func workAheadConflicts(accessOf func(int) plannerTransactionAccounts, prerequisite, tx int) bool {
	own := accessOf(tx).accounts
	for _, a := range accessOf(prerequisite).writable() {
		for _, b := range own {
			if a.publicKey == b.publicKey {
				return true
			}
		}
	}
	return false
}

func workAheadEligible(tx *solana.Transaction) bool {
	if tx.IsVote() || len(tx.Message.Instructions) == 0 || len(tx.Signatures) == 0 {
		return false
	}
	for _, key := range tx.Message.AccountKeys {
		switch key {
		case addresses.BpfLoaderUpgradeableAddr, addresses.BpfLoader2Addr, addresses.BpfLoaderDeprecatedAddr, addresses.LoaderV4Addr:
			return false
		}
	}
	return workAheadNonce || !advancesNonce(tx)
}

// advancesNonce reports whether tx's first instruction advances a durable nonce.
func advancesNonce(tx *solana.Transaction) bool {
	if len(tx.Message.Instructions) == 0 {
		return false
	}
	first := tx.Message.Instructions[0]
	return int(first.ProgramIDIndex) < len(tx.Message.AccountKeys) &&
		tx.Message.AccountKeys[first.ProgramIDIndex] == addresses.SystemProgramAddr &&
		len(first.Data) >= 4 && binary.LittleEndian.Uint32(first.Data) == sealevel.SystemProgramInstrTypeAdvanceNonceAccount
}

// workAheadReadSet records, before reading them, the overlay versions of every
// account an execution of tx can read: its declared accounts and the program
// data of its declared upgradeable programs (sysvars and parent-bank program
// bytes are constant for the block). base holds the declared accounts as read.
// Every store installs a new object and the delta never drops one, so equal
// versions later mean no store happened in between.
func workAheadReadSet(slotCtx *sealevel.SlotCtx, overlay *accounts.OverlayAccounts, tx *solana.Transaction) (keys [][32]byte, versions, base []*accounts.Account) {
	declared := tx.Message.AccountKeys
	keys = make([][32]byte, 0, len(declared)+2)
	versions = make([]*accounts.Account, 0, len(declared)+2)
	base = make([]*accounts.Account, len(declared))
	for i, key := range declared {
		keys = append(keys, [32]byte(key))
		versions = append(versions, overlay.DeltaVersion([32]byte(key)))
		if acct, err := slotCtx.GetAccountShared(key); err == nil {
			base[i] = acct
		}
	}
	for _, acct := range base {
		if acct == nil || solana.PublicKey(acct.Owner) != addresses.BpfLoaderUpgradeableAddr {
			continue
		}
		state, err := sealevel.UnmarshalUpgradeableLoaderState(acct.Data)
		if err == nil && state.Type == sealevel.UpgradeableLoaderStateTypeProgram {
			key := [32]byte(state.Program.ProgramDataAddress)
			keys = append(keys, key)
			versions = append(versions, overlay.DeltaVersion(key))
		}
	}
	return keys, versions, base
}

// dispatchHooks extends dispatchDependencyPlan. offer, when set, is called
// once every prerequisite of a transaction has been dispatched, and again as
// they commit, with the unfinished ones while at most maxUnfinished remain.
// started delivers transactions whose early execution began: they count as
// dispatched for their dependents' offers (they are still sent in their turn).
// onSend observes each dispatch. Completion (done) is always a commit, so
// dependents are released only for committed transactions.
type dispatchHooks struct {
	maxUnfinished int
	offer         func(tx int, unfinished []uint32)
	started       <-chan int
	onSend        func(tx int)
}

func dispatchDependencyPlanOffering(plan *dependencyPlan, out chan int, done chan int, maxUnfinished int, offer func(tx int, unfinished []uint32)) {
	dispatchDependencyPlanWith(plan, out, done, dispatchHooks{maxUnfinished: maxUnfinished, offer: offer})
}

func dispatchDependencyPlanWith(plan *dependencyPlan, out chan int, done chan int, hooks dispatchHooks) {
	n := len(plan.inDegree)
	var (
		pending, prereqOffsets, prerequisites, unfinished []uint32
		completed, dispatched                             []bool
	)
	if hooks.offer != nil {
		pending = append([]uint32(nil), plan.inDegree...)
		completed = make([]bool, n)
		dispatched = make([]bool, n)
		// prerequisites[i]: the transactions i depends on, as a reverse CSR.
		prereqOffsets = make([]uint32, n+1)
		for transaction, degree := range plan.inDegree {
			prereqOffsets[transaction+1] = prereqOffsets[transaction] + degree
		}
		prerequisites = make([]uint32, prereqOffsets[n])
		fill := append([]uint32(nil), prereqOffsets[:n]...)
		for transaction := range n {
			for _, dependentTx := range plan.dependents[plan.offsets[transaction]:plan.offsets[transaction+1]] {
				prerequisites[fill[dependentTx]] = uint32(transaction)
				fill[dependentTx]++
			}
		}
	}
	tryOffer := func(transaction uint32) {
		if int(plan.inDegree[transaction]) > hooks.maxUnfinished {
			return
		}
		unfinished = unfinished[:0]
		for _, prerequisite := range prerequisites[prereqOffsets[transaction]:prereqOffsets[transaction+1]] {
			if !completed[prerequisite] {
				unfinished = append(unfinished, prerequisite)
			}
		}
		hooks.offer(int(transaction), unfinished)
	}
	markDispatched := func(transaction uint32) {
		if dispatched[transaction] {
			return
		}
		dispatched[transaction] = true
		for _, dependentTx := range plan.dependents[plan.offsets[transaction]:plan.offsets[transaction+1]] {
			pending[dependentTx]--
			if pending[dependentTx] == 0 && plan.inDegree[dependentTx] > 0 {
				tryOffer(dependentTx)
			}
		}
	}
	sent := 0
	send := func(transaction uint32) {
		if hooks.onSend != nil {
			hooks.onSend(int(transaction))
		}
		out <- int(transaction)
		sent++
		if hooks.offer != nil {
			markDispatched(transaction)
		}
	}
	for transaction, degree := range plan.inDegree {
		if degree == 0 {
			send(uint32(transaction))
		}
	}
	for sent < n {
		select {
		case finished := <-done:
			if hooks.offer != nil {
				completed[finished] = true
			}
			start, end := plan.offsets[finished], plan.offsets[finished+1]
			for _, dependentTx := range plan.dependents[start:end] {
				plan.inDegree[dependentTx]--
				if plan.inDegree[dependentTx] == 0 {
					send(dependentTx)
				} else if hooks.offer != nil && pending[dependentTx] == 0 {
					tryOffer(dependentTx)
				}
			}
		case transaction := <-hooks.started:
			if hooks.offer != nil {
				markDispatched(uint32(transaction))
			}
		}
	}
	close(out)
}

// specSameInput is strict equality of an execution input: every field the
// runtime can observe. Unlike specSameAccount (output comparison), a
// zero-lamport account is not equal to a missing one or to a zero-lamport
// account with other owner, data or flags.
func specSameInput(x, y *accounts.Account) bool {
	if x == nil || y == nil {
		return x == nil && y == nil
	}
	return x.Key == y.Key && x.Lamports == y.Lamports && x.Owner == y.Owner && x.Executable == y.Executable &&
		x.RentEpoch == y.RentEpoch && x.IsDummy == y.IsDummy && bytes.Equal(x.Data, y.Data)
}

// overWasteCap reports whether the block's discarded early compute reached
// MITHRIL_WORK_AHEAD_WASTE_CAP_KCU.
func (w *workAhead) overWasteCap() bool {
	return workAheadWasteCapKCU > 0 && w.wastedKCU.Load() >= workAheadWasteCapKCU
}

// reserve claims the budget for launching tx's early execution: its requested
// compute, if discarded work plus outstanding reservations leave room. Called
// with e.mu held.
func (w *workAhead) reserve(e *workAheadEntry, tx *solana.Transaction) bool {
	if workAheadWasteBudgetKCU <= 0 {
		return true
	}
	need := int64(requestedKCU(tx))
	for {
		reserved := w.reservedKCU.Load()
		if w.wastedKCU.Load()+reserved+need > workAheadWasteBudgetKCU {
			return false
		}
		if w.reservedKCU.CompareAndSwap(reserved, reserved+need) {
			e.reservedKCU = need
			return true
		}
	}
}

// nonceAccountOf is the nonce account a durable-nonce transaction advances.
func nonceAccountOf(tx *solana.Transaction) (solana.PublicKey, bool) {
	if !advancesNonce(tx) {
		return solana.PublicKey{}, false
	}
	first := tx.Message.Instructions[0]
	if len(first.Accounts) == 0 || int(first.Accounts[0]) >= len(tx.Message.AccountKeys) {
		return solana.PublicKey{}, false
	}
	return tx.Message.AccountKeys[first.Accounts[0]], true
}

// compareOutputs lists the first difference between an early execution's output
// and the canonical one in anything adoption consumes: the error and its
// classification, fee info, no-op status, loaded-data accounting, compute
// budget and units, pre-balances, every transaction account (strictly) and
// whether it was written, and the stake, vote and nonce flags. Empty if equal.
// sameOptionalIndex compares two optional indexes by value.
func sameOptionalIndex(x, y *uint8) bool {
	if x == nil || y == nil {
		return x == nil && y == nil
	}
	return *x == *y
}

func compareOutputs(early, canonical *LoadAndExecuteTransactionOutput) string {
	ea, ca := early.ProcessingResult.TransactionError, canonical.ProcessingResult.TransactionError
	switch {
	case (ea == nil) != (ca == nil):
		return "outcome"
	case ea != nil && (ea.ErrorType != ca.ErrorType || !sameOptionalIndex(ea.AccountIndex, ca.AccountIndex) ||
		(ea.InstructionIndex == nil) != (ca.InstructionIndex == nil) ||
		(ea.InstructionIndex != nil && *ea.InstructionIndex != *ca.InstructionIndex) ||
		specTxErrString(ea) != specTxErrString(ca)):
		return fmt.Sprintf("error: early %s, canonical %s", specTxErrString(ea), specTxErrString(ca))
	case !reflect.DeepEqual(early.FeeInfo, canonical.FeeInfo):
		return fmt.Sprintf("fee info: early %+v, canonical %+v", early.FeeInfo, canonical.FeeInfo)
	case early.ProcessedAsNoOp != canonical.ProcessedAsNoOp:
		return "no-op classification"
	case early.LoadedAccountsDataSize != canonical.LoadedAccountsDataSize:
		return fmt.Sprintf("loaded data size: early %d, canonical %d", early.LoadedAccountsDataSize, canonical.LoadedAccountsDataSize)
	case !reflect.DeepEqual(early.ComputeBudgetLimits, canonical.ComputeBudgetLimits):
		return "compute budget limits"
	case processTransactionComputeUnits(early.ExecCtx) != processTransactionComputeUnits(canonical.ExecCtx):
		return fmt.Sprintf("cu: early %d, canonical %d", processTransactionComputeUnits(early.ExecCtx), processTransactionComputeUnits(canonical.ExecCtx))
	case !slices.Equal(early.PreBalances, canonical.PreBalances):
		return "pre-balances"
	case (early.ExecCtx == nil) != (canonical.ExecCtx == nil):
		return "execution context presence"
	}
	if early.ExecCtx == nil || early.ExecCtx.TransactionContext == nil || canonical.ExecCtx.TransactionContext == nil {
		return ""
	}
	et, ct := early.ExecCtx.TransactionContext, canonical.ExecCtx.TransactionContext
	if et.ModifiedStakeAccts != ct.ModifiedStakeAccts || et.ModifiedVoteAccts != ct.ModifiedVoteAccts || et.NonceAcctAdvanced != ct.NonceAcctAdvanced {
		return "stake, vote or nonce flags"
	}
	ea2, ca2 := et.Accounts.Accounts, ct.Accounts.Accounts
	if len(ea2) != len(ca2) || !slices.Equal(et.Accounts.Touched, ct.Accounts.Touched) {
		return "transaction accounts or written set"
	}
	for i := range ea2 {
		if !specSameInput(ea2[i], ca2[i]) {
			return fmt.Sprintf("account %d: early %s, canonical %s", i, specDescribe(ea2[i]), specDescribe(ca2[i]))
		}
	}
	return ""
}
