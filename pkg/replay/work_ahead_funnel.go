package replay

import (
	"bufio"
	"encoding/json"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// Completion funnel (MITHRIL_WORK_AHEAD_FUNNEL=1), a record-only diagnostic.
//
// Offers are made when a transaction's prerequisites are dispatched (and, with
// direct dispatch, again as they commit), never when an early execution
// finishes. A finished early result can say more than the predictor knew: the
// prerequisite failed, or succeeded without changing the accounts a dependent
// shares with it. The funnel asks, at every early finish, whether that result
// would newly let a dependent start early, and if so records the dependent's
// read set at that moment. At the dependent's turn it checks whether an early
// execution started then would have been adoptable (versions unchanged, or
// values unchanged), how long before the turn it could have started, and
// whether the dependent got an early execution anyway. Nothing is launched and
// no decision changes; the result is only counted (and, with
// MITHRIL_WORK_AHEAD_FUNNEL_OUT=FILE, written per hint as JSON lines).
//
// A hint is recorded only if every obligation of the dependent is covered:
// the producer's edge is resolved by its result; the dependent's other
// unfinished prerequisites are admissible under today's rules; and no other
// uncommitted transaction writes any of its accounts (an account the producer
// left unchanged still carries the value of the writer before it, and a
// prerequisite predicted to fail still hides the writer before it).
//
// MITHRIL_WORK_AHEAD_COMPLETION=1 acts on it: such a dependent is queued for
// an early execution at that moment (completion-driven admission). That
// changes only which early executions run and when; adoption is still decided
// by the read-set check at the dependent's turn.
var (
	workAheadFunnelOn   = os.Getenv("MITHRIL_WORK_AHEAD_FUNNEL") == "1"
	workAheadFunnelOut  = os.Getenv("MITHRIL_WORK_AHEAD_FUNNEL_OUT")
	workAheadCompletion = os.Getenv("MITHRIL_WORK_AHEAD_COMPLETION") == "1"
)

type workAheadFunnel struct {
	w    *workAhead
	ch   chan int // producers whose early execution finished
	done chan struct{}
	// mu guards ch against a send after close (sends take the read lock)
	mu      sync.RWMutex
	closed  bool
	started bool // set by start; report waits for the goroutine only then
	// built by the funnel goroutine
	prereqOffsets, prereqs []uint32
	dependents             func(tx int) []uint32
	writers                map[solana.PublicKey][]int32 // writable users of each account, in order
	hints                  []atomic.Pointer[funnelHint]
	stats                  funnelStats
}

type funnelStats struct {
	events, producerGone, candidates                                   atomic.Int64
	attempted, dropped, ineligible                                     atomic.Int64
	payerOverlap, changed, olderWriter, otherBlockers, hints, admitted atomic.Int64
	// at the dependent's turn
	versionValid, valueValid, attemptedAnyway, leadNs, earlierNs atomic.Int64
}

// funnelHint is a dependent that the producer's finished result would newly
// have let start early.
type funnelHint struct {
	producer int32
	failed   bool // the producer's early execution failed (else: left shared accounts unchanged)
	at       time.Time
	keys     [][32]byte
	versions []*accounts.Account
	base     []*accounts.Account
	// at the turn
	evaluated, versionValid, valueValid, attempted bool
	lead, earlier                                  time.Duration
}

func newWorkAheadFunnel(w *workAhead) *workAheadFunnel {
	if !workAheadFunnelOn && !workAheadCompletion {
		return nil
	}
	return &workAheadFunnel{w: w, ch: make(chan int, len(w.txs)), done: make(chan struct{}), hints: make([]atomic.Pointer[funnelHint], len(w.txs))}
}

// start runs the funnel for plan until close. It copies the in-degrees
// before returning: the dispatcher counts them down in place.
func (f *workAheadFunnel) start(plan *dependencyPlan) {
	if f == nil {
		return
	}
	snapshot := &dependencyPlan{offsets: plan.offsets, dependents: plan.dependents, inDegree: append([]uint32(nil), plan.inDegree...)}
	f.mu.Lock()
	f.started = true
	f.mu.Unlock()
	go func() {
		defer close(f.done)
		f.build(snapshot)
		for producer := range f.ch {
			f.examine(producer)
		}
	}()
}

// close ends the funnel once no early execution can finish any more.
func (f *workAheadFunnel) close() {
	if f == nil {
		return
	}
	f.mu.Lock()
	if !f.closed {
		f.closed = true
		close(f.ch)
	}
	f.mu.Unlock()
}

// finished is called by ran, with the producer's entry locked, when an early
// execution finished with a result kept for its turn. It never blocks.
func (f *workAheadFunnel) finished(tx int) {
	if f == nil {
		return
	}
	f.mu.RLock()
	if !f.closed {
		select {
		case f.ch <- tx:
		default:
		}
	}
	f.mu.RUnlock()
}

func (f *workAheadFunnel) build(plan *dependencyPlan) {
	n := len(plan.inDegree)
	f.dependents = func(tx int) []uint32 { return plan.dependents[plan.offsets[tx]:plan.offsets[tx+1]] }
	f.prereqOffsets = make([]uint32, n+1)
	for tx, degree := range plan.inDegree {
		f.prereqOffsets[tx+1] = f.prereqOffsets[tx] + degree
	}
	f.prereqs = make([]uint32, f.prereqOffsets[n])
	fill := append([]uint32(nil), f.prereqOffsets[:n]...)
	for tx := range n {
		for _, d := range f.dependents(tx) {
			f.prereqs[fill[d]] = uint32(tx)
			fill[d]++
		}
	}
	f.writers = make(map[solana.PublicKey][]int32, 4*n)
	for tx := range n {
		for _, a := range f.w.accessOf(tx).writable() {
			f.writers[a.publicKey] = append(f.writers[a.publicKey], int32(tx))
		}
	}
}

// producerResult summarizes producer's finished early result: whether it
// failed, and for a success which of its writable accounts it changed (nil
// when the result is gone or unreadable).
func (f *workAheadFunnel) producerResult(producer int) (failed bool, changed map[solana.PublicKey]bool, ok bool) {
	e := f.w.entries[producer].Load()
	if e == nil {
		return false, nil, false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != workAheadFinished || e.panicked || !workAheadUsable(&e.output) {
		return false, nil, false
	}
	if e.output.ProcessingResult.TransactionError != nil {
		return true, nil, true
	}
	if e.output.ExecCtx == nil || e.output.ExecCtx.TransactionContext == nil {
		return false, nil, false
	}
	base := make(map[solana.PublicKey]*accounts.Account, len(e.keys))
	for i, key := range e.keys {
		if i < len(e.base) {
			base[solana.PublicKey(key)] = e.base[i]
		}
	}
	changed = make(map[solana.PublicKey]bool)
	for _, a := range f.w.accessOf(producer).writable() {
		before, known := base[a.publicKey]
		after := funnelOutputAccount(e.output.ExecCtx, a.publicKey)
		if !known || after == nil {
			return false, nil, false
		}
		changed[a.publicKey] = !specSameInput(after, before)
	}
	return false, changed, true
}

func funnelOutputAccount(ctx *sealevel.ExecutionCtx, key solana.PublicKey) *accounts.Account {
	for _, acct := range ctx.TransactionContext.Accounts.Accounts {
		if acct != nil && acct.Key == key {
			return acct
		}
	}
	return nil
}

func (f *workAheadFunnel) examine(producer int) {
	w := f.w
	f.stats.events.Add(1)
	failed, changed, ok := f.producerResult(producer)
	if !ok {
		f.stats.producerGone.Add(1)
		return
	}
	payer := w.payerOf(producer)
	nonce, hasNonce := nonceAccountOf(w.txs[producer])
	for _, d := range f.dependents(producer) {
		c := int(d)
		f.stats.candidates.Add(1)
		if f.hints[c].Load() != nil {
			continue // an earlier producer already hinted it
		}
		if e := w.entries[c].Load(); e != nil {
			e.mu.Lock()
			state, ineligible := e.state, e.checked && e.ineligible
			e.mu.Unlock()
			switch {
			case e == workAheadNone || state == workAheadClaimed || (state == workAheadDropped && !ineligible):
				f.stats.dropped.Add(1) // reached its turn, or over budget
				continue
			case ineligible:
				f.stats.ineligible.Add(1)
				continue
			case state != workAheadIdle:
				f.stats.attempted.Add(1)
				continue
			}
		}
		if c >= len(w.execute) || !w.execute[c] || !workAheadEligible(w.txs[c]) || isDebugTransaction(w.txs[c], w.dbgOpts) {
			f.stats.ineligible.Add(1)
			continue
		}
		// The producer's edge: which accounts of c it writes, and whether its
		// result leaves each as it was.
		var shared []solana.PublicKey
		verdict := ""
		for _, a := range w.accessOf(producer).writable() {
			if !funnelUses(w.accessOf(c), a.publicKey) {
				continue
			}
			shared = append(shared, a.publicKey)
			switch {
			case a.publicKey == payer || (hasNonce && a.publicKey == nonce):
				verdict = "payer" // a fee always changes the payer; predicted payers are today's mechanism
			case failed:
			case changed[a.publicKey]:
				if verdict == "" {
					verdict = "changed"
				}
			}
		}
		switch verdict {
		case "payer":
			f.stats.payerOverlap.Add(1)
			continue
		case "changed":
			f.stats.changed.Add(1)
			continue
		}
		// c's other unfinished prerequisites, under today's rules.
		var others []uint32
		for _, p := range f.prereqs[f.prereqOffsets[c]:f.prereqOffsets[c+1]] {
			if int(p) != producer && w.outcome[p].Load() == 0 {
				others = append(others, p)
			}
		}
		if len(others) > workAheadOfferMax {
			f.stats.otherBlockers.Add(1)
			continue
		}
		assumed := 0
		if len(others) > 0 {
			var ok bool
			if ok, assumed = workAheadAdmissible(w.accessOf, w.payerOf, w.predictFailureFor, others, c, workAheadFailDepth, workAheadPredictPayer); !ok {
				f.stats.otherBlockers.Add(1)
				continue
			}
		}
		if f.hiddenWriter(c, producer, shared, others) {
			f.stats.olderWriter.Add(1)
			continue
		}
		if workAheadFunnelOn {
			h := &funnelHint{producer: int32(producer), failed: failed, at: time.Now()}
			h.keys, h.versions, h.base = workAheadReadSet(w.slotCtx, w.overlay, w.txs[c])
			if f.hints[c].CompareAndSwap(nil, h) {
				f.stats.hints.Add(1)
			}
		}
		if workAheadCompletion {
			e := w.entry(c)
			e.mu.Lock()
			if w.admissibleLocked(e, c) && w.queueLocked(e, c, others, assumed) {
				f.stats.admitted.Add(1)
			}
			e.mu.Unlock()
		}
	}
}

func funnelUses(access plannerTransactionAccounts, key solana.PublicKey) bool {
	for _, a := range access.accounts {
		if a.publicKey == key {
			return true
		}
	}
	return false
}

// hiddenWriter reports whether an uncommitted transaction before c writes one
// of c's accounts without being accounted for: the only uncommitted writers
// allowed are the producer, for an account its result resolved (shared), and
// the last writer of the account before c when it is one of c's admissible
// other prerequisites. Any other is a writer whose value c would need but
// that its prerequisites hide.
func (f *workAheadFunnel) hiddenWriter(c, producer int, shared []solana.PublicKey, others []uint32) bool {
	for _, a := range f.w.accessOf(c).accounts {
		writers := f.writers[a.publicKey]
		last := int32(-1)
		for _, writer := range writers {
			if int(writer) >= c {
				break
			}
			last = writer
		}
		for _, writer := range writers {
			if int(writer) >= c {
				break
			}
			if f.w.outcome[writer].Load() != 0 {
				continue
			}
			switch {
			case int(writer) == producer && slices.Contains(shared, a.publicKey):
			case writer == last && slices.Contains(others, uint32(writer)):
			default:
				return true
			}
		}
	}
	return false
}

// atTurn evaluates tx's hint, if any, when tx reaches its turn (every
// prerequisite committed), before claim changes its entry.
func (f *workAheadFunnel) atTurn(tx int) {
	if f == nil {
		return
	}
	h := f.hints[tx].Load()
	if h == nil || h.evaluated {
		return
	}
	now := time.Now()
	w := f.w
	h.versionValid, h.valueValid = true, true
	for i, key := range h.keys {
		sameVersion := w.overlay.DeltaVersion(key) == h.versions[i]
		if !sameVersion {
			h.versionValid = false
		}
		if i >= len(h.base) {
			// program data: by version only, as the read set checks it
			h.valueValid = h.valueValid && sameVersion
			continue
		}
		committed, err := w.slotCtx.GetAccountShared(solana.PublicKey(key))
		if err != nil {
			committed = nil
		}
		if !specSameInput(committed, h.base[i]) {
			h.valueValid = false
		}
	}
	h.lead = now.Sub(h.at)
	if e := w.entries[tx].Load(); e != nil && e != workAheadNone {
		e.mu.Lock()
		if e.state == workAheadQueued || e.state == workAheadRunning || e.state == workAheadFinished {
			h.attempted = true
			if !e.began.IsZero() {
				h.earlier = e.began.Sub(h.at)
			}
		}
		e.mu.Unlock()
	}
	h.evaluated = true
	if h.versionValid {
		f.stats.versionValid.Add(1)
	}
	if h.valueValid {
		f.stats.valueValid.Add(1)
		f.stats.leadNs.Add(int64(h.lead))
	}
	if h.attempted {
		f.stats.attemptedAnyway.Add(1)
		f.stats.earlierNs.Add(int64(h.earlier))
	}
}

// report waits for the funnel goroutine, logs the block's counts and writes
// the hints.
func (f *workAheadFunnel) report(slot uint64) {
	if f == nil {
		return
	}
	f.close()
	f.mu.RLock()
	started := f.started
	f.mu.RUnlock()
	if !started {
		return // no plan ran (an empty block)
	}
	<-f.done
	s := &f.stats
	mlog.Log.Infof("work-ahead funnel slot %d: events %d producer-gone %d candidates %d attempted %d dropped %d ineligible %d payer %d changed %d hidden-writer %d other-blockers %d hints %d admitted %d version-valid %d value-valid %d attempted-anyway %d lead-us %d earlier-us %d",
		slot, s.events.Load(), s.producerGone.Load(), s.candidates.Load(), s.attempted.Load(), s.dropped.Load(), s.ineligible.Load(),
		s.payerOverlap.Load(), s.changed.Load(), s.olderWriter.Load(), s.otherBlockers.Load(), s.hints.Load(), s.admitted.Load(),
		s.versionValid.Load(), s.valueValid.Load(), s.attemptedAnyway.Load(), s.leadNs.Load()/1000, s.earlierNs.Load()/1000)
	if workAheadFunnelOut != "" {
		f.write(slot)
	}
}

type funnelRecord struct {
	Slot         uint64 `json:"slot"`
	Producer     int32  `json:"producer"`
	Tx           int    `json:"tx"`
	Failed       bool   `json:"producer_failed"`
	Evaluated    bool   `json:"evaluated"`
	VersionValid bool   `json:"version_valid"`
	ValueValid   bool   `json:"value_valid"`
	Attempted    bool   `json:"attempted_anyway"`
	LeadUs       int64  `json:"lead_us"`
	EarlierUs    int64  `json:"earlier_than_actual_us"`
	DurUs        int64  `json:"tx_us"`
	RequestedKCU int32  `json:"requested_kcu"`
}

var funnelOut struct {
	sync.Mutex
	f *os.File
	w *bufio.Writer
}

func (f *workAheadFunnel) write(slot uint64) {
	funnelOut.Lock()
	defer funnelOut.Unlock()
	if funnelOut.w == nil {
		file, err := os.OpenFile(workAheadFunnelOut, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			mlog.Log.Errorf("work-ahead funnel: %v", err)
			return
		}
		funnelOut.f, funnelOut.w = file, bufio.NewWriter(file)
	}
	enc := json.NewEncoder(funnelOut.w)
	for tx := range f.hints {
		h := f.hints[tx].Load()
		if h == nil {
			continue
		}
		_ = enc.Encode(funnelRecord{
			Slot: slot, Producer: h.producer, Tx: tx, Failed: h.failed, Evaluated: h.evaluated,
			VersionValid: h.versionValid, ValueValid: h.valueValid, Attempted: h.attempted,
			LeadUs: h.lead.Microseconds(), EarlierUs: h.earlier.Microseconds(),
			DurUs: f.w.durNs[tx].Load() / 1000, RequestedKCU: requestedKCU(f.w.txs[tx]),
		})
	}
	_ = funnelOut.w.Flush()
}
