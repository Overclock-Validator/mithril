package replay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// Diagnostic only, off unless MITHRIL_SPEC_SHADOW_DIR is set: shadow mode for
// dependency-chain speculation. When every unfinished prerequisite of a
// transaction has been dispatched and at most MITHRIL_SPEC_SHADOW_K of them are
// unfinished, a spare worker executes it early against committed slot state and
// publishes nothing. When the transaction becomes ready, the attempt is judged
// valid if no account it could read was stored since the attempt began. The
// transaction then runs normally, as always, and the two results are compared.
// Nothing from an attempt is used. Timing metrics include the attempts' work.
//
// Attempts run Speculative with PrivateParameters, so they never touch the
// shared program cache or the bank's parameter arena. Transactions that name a
// loader program (the only way to deploy, upgrade, close or retract a program,
// even by CPI) or use a durable nonce are never attempted. MITHRIL_SPEC_SHADOW_MODE=war attempts
// only transactions whose unfinished prerequisites write none of their accounts.
var (
	specShadowMode = os.Getenv("MITHRIL_SPEC_SHADOW_MODE")
	// MITHRIL_SPEC_SHADOW_WAIT=1: the normal execution waits for a running
	// attempt, so the two never overlap (isolates overlap effects).
	specShadowWait = os.Getenv("MITHRIL_SPEC_SHADOW_WAIT") == "1"
	// MITHRIL_SPEC_SHADOW_PRIVATE=1: attempts execute on deep copies of their
	// loaded accounts (isolates in-place changes to shared committed objects).
	specShadowPrivate = os.Getenv("MITHRIL_SPEC_SHADOW_PRIVATE") == "1"
	// MITHRIL_SPEC_SHADOW_PRIVATE_PROGRAMS=1: attempts also compile every program
	// privately instead of using the shared program cache.
	specShadowPrivatePrograms = os.Getenv("MITHRIL_SPEC_SHADOW_PRIVATE_PROGRAMS") == "1"
	specShadowDir             = os.Getenv("MITHRIL_SPEC_SHADOW_DIR")
	specShadowK               = specShadowEnvInt("MITHRIL_SPEC_SHADOW_K", 2)
	specShadowWorkers         = specShadowEnvInt("MITHRIL_SPEC_SHADOW_WORKERS", 4)
	specShadowMinCU, _        = strconv.ParseUint(os.Getenv("MITHRIL_SPEC_SHADOW_MIN_CU"), 10, 64)
	specShadowArenas          []*arena.Arena[sealevel.BorrowedAccount]
	specShadowArenasOnce      sync.Once
)

func specShadowEnvInt(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v >= 0 {
		return v
	}
	return fallback
}

const (
	specQueued = iota
	specRunning
	specDone
	specLate // the transaction became ready before the attempt started
)

type specAttempt struct {
	mu       sync.Mutex
	state    int
	finished chan struct{} // closed when the attempt ends (done or panicked)
	overtook []uint32      // unfinished prerequisites when offered
	warOnly  bool          // none of them writes an account of this transaction

	// Versions of every account the attempt could read: declared accounts and the
	// program data of declared upgradeable programs. base holds the declared
	// accounts as committed when the attempt began (nil if absent).
	keys     [][32]byte
	versions []*accounts.Account
	base     []*accounts.Account

	start, end, ready, normalEnd time.Duration
	judged, valid                bool
	changed                      int // index into keys of the first changed account, -1 if none
	baseDiff                     int // valid, yet this declared account's committed object changed; -1 if none
	diff                         string

	failed bool
	err    string
	cu     uint64
	final  map[[32]byte]*accounts.Account // touched accounts after a successful attempt

	haveNormal bool
	nFailed    bool
	nErr       string
	nCU        uint64
	nFinal     []*accounts.Account // declared accounts after the normal commit

	outcome string
	panic   string
}

type specShadow struct {
	slotCtx  *sealevel.SlotCtx
	overlay  *accounts.OverlayAccounts
	block    *b.Block
	execute  []bool
	access   []plannerTransactionAccounts
	attempts []*specAttempt
	queue    chan int
	wg       sync.WaitGroup
	loop     time.Time
}

func newSpecShadow(slotCtx *sealevel.SlotCtx, block *b.Block, execute []bool, loopStart time.Time) *specShadow {
	if specShadowDir == "" || specShadowWorkers == 0 {
		return nil
	}
	overlay, ok := slotCtx.Accounts.(*accounts.OverlayAccounts)
	if !ok {
		return nil
	}
	access, ok := plannerAccountsForBlock(block)
	if !ok {
		return nil
	}
	specShadowArenasOnce.Do(func() {
		// Match the workers: an arena per attempt worker if they have one.
		specShadowArenas = make([]*arena.Arena[sealevel.BorrowedAccount], specShadowWorkers)
		if len(sealevel.BorrowedAccountArenas) > 0 && sealevel.BorrowedAccountArenas[0] != nil {
			for i := range specShadowArenas {
				specShadowArenas[i] = arena.New[sealevel.BorrowedAccount](sealevel.BorrowedAccountArenas[0].Capacity())
			}
		}
	})
	s := &specShadow{
		slotCtx:  slotCtx,
		overlay:  overlay,
		block:    block,
		execute:  execute,
		access:   access,
		attempts: make([]*specAttempt, len(block.Transactions)),
		queue:    make(chan int, len(block.Transactions)),
		loop:     loopStart,
	}
	s.wg.Add(specShadowWorkers)
	for w := range specShadowWorkers {
		go s.worker(specShadowArenas[w])
	}
	return s
}

// offer is called by the dispatcher when every prerequisite of tx has been
// dispatched; unfinished lists those that have not completed.
func (s *specShadow) offer(tx int, unfinished []uint32) {
	if len(unfinished) == 0 || len(unfinished) > specShadowK || s.attempts[tx] != nil {
		return
	}
	if tx >= len(s.execute) || !s.execute[tx] || !workAheadEligible(s.block.Transactions[tx]) {
		return
	}
	mine := make(map[solana.PublicKey]struct{}, len(s.access[tx].accounts))
	for _, access := range s.access[tx].accounts {
		mine[access.publicKey] = struct{}{}
	}
	warOnly := true
	for _, prerequisite := range unfinished {
		for _, access := range s.access[prerequisite].writable() {
			if _, ok := mine[access.publicKey]; ok {
				warOnly = false
			}
		}
	}
	if specShadowMode == "war" && !warOnly {
		return
	}
	s.attempts[tx] = &specAttempt{finished: make(chan struct{}), overtook: append([]uint32(nil), unfinished...), warOnly: warOnly, changed: -1, baseDiff: -1}
	s.queue <- tx
}

func (s *specShadow) since() time.Duration { return time.Since(s.loop) }

func (s *specShadow) worker(workerArena *arena.Arena[sealevel.BorrowedAccount]) {
	defer s.wg.Done()
	for idx := range s.queue {
		a := s.attempts[idx]
		tx := s.block.Transactions[idx]
		a.mu.Lock()
		if a.state != specQueued {
			a.mu.Unlock()
			continue
		}
		a.state = specRunning
		a.start = s.since()
		s.captureVersions(a, tx)
		a.mu.Unlock()

		out, panicked := specShadowExecute(s.slotCtx, tx, workerArena)
		if panicked != "" {
			mlog.Log.Errorf("spec shadow PANIC slot %d tx %s: %s", s.block.Slot, tx.Signatures[0], panicked)
			a.mu.Lock()
			a.end, a.panic, a.state = s.since(), panicked, specDone
			a.outcome = "panic"
			close(a.finished)
			a.mu.Unlock()
			continue
		}
		cu := processTransactionComputeUnits(out.ExecCtx)
		var final map[[32]byte]*accounts.Account
		txErr := out.ProcessingResult.TransactionError
		if txErr == nil && out.ExecCtx != nil {
			// The arena is reset by this worker's next attempt, so keep copies.
			transactionAccounts := &out.ExecCtx.TransactionContext.Accounts
			final = make(map[[32]byte]*accounts.Account)
			for i, acct := range transactionAccounts.Accounts {
				if transactionAccounts.Touched[i] {
					final[acct.Key] = acct.Clone()
				}
			}
		}

		a.mu.Lock()
		a.end = s.since()
		a.cu = cu
		if txErr != nil {
			a.failed = true
			a.err = specTxErrString(txErr)
		}
		a.final = final
		a.state = specDone
		close(a.finished)
		if a.haveNormal {
			s.compare(a, tx)
		}
		a.mu.Unlock()
	}
}

func specShadowExecute(slotCtx *sealevel.SlotCtx, tx *solana.Transaction, workerArena *arena.Arena[sealevel.BorrowedAccount]) (out LoadAndExecuteTransactionOutput, panicked string) {
	defer func() {
		if r := recover(); r != nil {
			panicked = fmt.Sprint(r)
		}
	}()
	out = LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:           slotCtx,
		Transaction:       tx,
		Arena:             workerArena,
		LeanResult:        true,
		SkipTimingMetrics: true,
		Speculative:       true,
		PrivateParameters: true,
		PrivateAccounts:   specShadowPrivate,
		PrivatePrograms:   specShadowPrivatePrograms,
	})
	return out, ""
}

func (s *specShadow) captureVersions(a *specAttempt, tx *solana.Transaction) {
	a.keys, a.versions, a.base = workAheadReadSet(s.slotCtx, s.overlay, tx)
}

// ready is called by the normal worker just before it executes tx: every
// prerequisite has committed, and no later transaction can store to its
// accounts before it does, so this is the moment to judge the attempt.
func (s *specShadow) ready(tx int) {
	a := s.attempts[tx]
	if a == nil {
		return
	}
	a.mu.Lock()
	s.judge(a)
	running := a.state == specRunning
	a.mu.Unlock()
	if running && specShadowWait {
		<-a.finished
	}
}

func (s *specShadow) judge(a *specAttempt) {
	a.ready = s.since()
	if a.state == specQueued {
		a.state = specLate
		return
	}
	a.judged, a.valid = true, true
	for i, key := range a.keys {
		if s.overlay.DeltaVersion(key) != a.versions[i] {
			a.valid, a.changed = false, i
			break
		}
	}
	if a.valid {
		// Self-check of the version rule: the committed object of every declared
		// account must still be the one the attempt began with.
		for i, base := range a.base {
			if current, err := s.slotCtx.GetAccountShared(solana.PublicKey(a.keys[i])); err == nil && current != base || err != nil && base != nil {
				a.baseDiff = i
				break
			}
		}
	}
}

// normalDone is called by the normal worker after tx committed and before its
// dependents are released, so the declared accounts hold tx's own results.
func (s *specShadow) normalDone(tx int, err error, cu uint64) {
	a := s.attempts[tx]
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state == specLate {
		return
	}
	a.normalEnd = s.since()
	a.haveNormal = true
	a.nCU = cu
	if err != nil {
		a.nFailed, a.nErr = true, err.Error()
	}
	keys := s.block.Transactions[tx].Message.AccountKeys
	a.nFinal = make([]*accounts.Account, len(keys))
	for i, key := range keys {
		if acct, err := s.slotCtx.GetAccountShared(key); err == nil {
			a.nFinal[i] = acct
		}
	}
	if a.state == specDone {
		s.compare(a, s.block.Transactions[tx])
	}
}

// compare runs with a.mu held once both results are present.
func (s *specShadow) compare(a *specAttempt, tx *solana.Transaction) {
	if a.panic != "" {
		return
	}
	switch {
	case a.failed != a.nFailed:
		a.outcome = "outcome"
	case a.failed && a.err != a.nErr:
		a.outcome = "error"
	case a.cu != a.nCU:
		a.outcome = "cu"
	default:
		a.outcome = "equal"
		if !a.failed {
			for i, key := range tx.Message.AccountKeys {
				mine := a.base[i]
				if touched, ok := a.final[[32]byte(key)]; ok {
					mine = touched
				}
				if !specSameAccount(mine, a.nFinal[i]) {
					a.outcome = "account"
					_, touched := a.final[[32]byte(key)]
					a.diff = fmt.Sprintf("account %d %s touched=%v attempt=%s normal=%s base==normal %v",
						i, key, touched, specDescribe(mine), specDescribe(a.nFinal[i]), a.base[i] == a.nFinal[i])
					break
				}
			}
		}
	}
	if a.judged && a.valid && a.outcome != "equal" {
		mlog.Log.Errorf("spec shadow MISMATCH slot %d tx %s: judged valid but %s differs (attempt err=%q cu=%d, normal err=%q cu=%d) baseDiff=%d %s",
			s.block.Slot, tx.Signatures[0], a.outcome, a.err, a.cu, a.nErr, a.nCU, a.baseDiff, a.diff)
	}
}

func specDescribe(acct *accounts.Account) string {
	if acct == nil {
		return "absent"
	}
	return fmt.Sprintf("{lamports %d owner %s len %d exec %v rentEpoch %d}", acct.Lamports, solana.PublicKey(acct.Owner), len(acct.Data), acct.Executable, acct.RentEpoch)
}

func specSameAccount(x, y *accounts.Account) bool {
	empty := func(acct *accounts.Account) bool { return acct == nil || acct.Lamports == 0 }
	if empty(x) || empty(y) {
		return empty(x) && empty(y)
	}
	return x.Lamports == y.Lamports && x.Owner == y.Owner && x.Executable == y.Executable &&
		x.RentEpoch == y.RentEpoch && bytes.Equal(x.Data, y.Data)
}

func specTxErrString(txErr *TransactionError) string {
	if txErr.InstructionError != nil {
		return txErr.InstructionError.Error()
	}
	return txErr.ErrorType.String()
}

type specShadowRecord struct {
	Index     int      `json:"i"`
	Overtook  []uint32 `json:"k"`
	WarOnly   bool     `json:"war"`
	State     string   `json:"st"`
	Valid     bool     `json:"v"`
	Changed   int      `json:"ch"` // index into the declared keys (or past them: program data), -1 if none
	BaseDiff  int      `json:"bd"`
	StartNs   int64    `json:"as"`
	EndNs     int64    `json:"ae"`
	ReadyNs   int64    `json:"rd"`
	NormalEnd int64    `json:"ne"`
	Outcome   string   `json:"o,omitempty"`
	Failed    bool     `json:"f"`
}

// finish waits for outstanding attempts after the transaction loop and writes
// the block's records.
func (s *specShadow) finish(cus []uint64) {
	close(s.queue)
	s.wg.Wait()
	var total uint64
	for _, cu := range cus {
		total += cu
	}
	if total < specShadowMinCU {
		return
	}
	var records []specShadowRecord
	counts := map[string]int{}
	for idx, a := range s.attempts {
		if a == nil {
			continue
		}
		state := map[int]string{specQueued: "queued", specRunning: "running", specDone: "done", specLate: "late"}[a.state]
		records = append(records, specShadowRecord{
			Index: idx, Overtook: a.overtook, WarOnly: a.warOnly, State: state, Valid: a.judged && a.valid, Changed: a.changed, BaseDiff: a.baseDiff,
			StartNs: int64(a.start), EndNs: int64(a.end), ReadyNs: int64(a.ready), NormalEnd: int64(a.normalEnd),
			Outcome: a.outcome, Failed: a.nFailed,
		})
		counts["attempts"]++
		switch {
		case a.state == specLate:
			counts["late"]++
		case a.judged && a.valid:
			counts["valid"]++
			if a.outcome != "equal" {
				counts["valid_mismatch"]++
			}
		case a.judged:
			counts["invalid"]++
			if a.outcome == "equal" {
				counts["invalid_but_equal"]++
			}
		}
	}
	mlog.Log.Infof("spec shadow slot %d: %v", s.block.Slot, counts)
	out, err := json.Marshal(map[string]any{"slot": s.block.Slot, "cu": total, "k": specShadowK, "mode": specShadowMode, "attempts": records})
	if err == nil {
		_ = os.WriteFile(filepath.Join(specShadowDir, fmt.Sprintf("slot-%d.json", s.block.Slot)), out, 0o644)
	}
}

// dispatchDependencyPlanShadow is dispatchDependencyPlan plus offers to the shadow.
func dispatchDependencyPlanShadow(plan *dependencyPlan, out chan int, done chan int, shadow *specShadow) {
	dispatchDependencyPlanOffering(plan, out, done, specShadowK, shadow.offer)
}
