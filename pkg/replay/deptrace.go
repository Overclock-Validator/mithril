package replay

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// Diagnostic only, off unless MITHRIL_DEPTRACE_DIR is set: parallelTxLoop writes
// one JSON file per executed block of at least MITHRIL_DEPTRACE_MIN_CU consumed
// CU, with every transaction's account accesses, outcome, CU and execution
// interval, for offline dependency-chain analysis. It never changes execution.
//
// MITHRIL_DEPTRACE_CHANGED=1 also records which writable accounts each
// transaction changed. It copies those accounts before every transaction, which
// lengthens the critical path, so take timings from a run without it.
var (
	depTraceDir      = os.Getenv("MITHRIL_DEPTRACE_DIR")
	depTraceMinCU, _ = strconv.ParseUint(os.Getenv("MITHRIL_DEPTRACE_MIN_CU"), 10, 64)
	depTraceChanged  = os.Getenv("MITHRIL_DEPTRACE_CHANGED") == "1"
)

// failedPrefix maps a transaction that failed in an instruction to that
// instruction's index and the accounts of instructions 0..index (instruction
// accounts and program ids). A CPI can reach only its caller's accounts, so these
// are all the accounts the transaction used before failing.
var failedPrefix sync.Map

type depTraceFailure struct {
	instr int
	keys  []solana.PublicKey
}

func depTraceFailedPrefix(tx *solana.Transaction, instrAccts [][]sealevel.InstructionAccount, failed int) {
	var keys []solana.PublicKey
	for j := 0; j <= failed && j < len(tx.Message.Instructions); j++ {
		keys = append(keys, tx.Message.AccountKeys[tx.Message.Instructions[j].ProgramIDIndex])
		for _, acct := range instrAccts[j] {
			if int(acct.IndexInTransaction) < len(tx.Message.AccountKeys) {
				keys = append(keys, tx.Message.AccountKeys[acct.IndexInTransaction])
			}
		}
	}
	failedPrefix.Store(tx, depTraceFailure{instr: failed, keys: keys})
}

type depTrace struct {
	start, end []time.Duration
	errs       []string
	// early executions (work-ahead): window and whether the result was adopted
	aheadStart, aheadEnd []time.Duration
	adopted              []bool
	queuedAt             []time.Duration // when its offer was queued for early execution
	sentAt               []time.Duration // when the dispatcher handed the transaction to the workers
	// accounts: the planner's view of each transaction's accesses; changed[i]
	// marks which of i's writable accounts (in accounts[i].writable() order)
	// differ in content after i committed.
	accounts []plannerTransactionAccounts
	changed  [][]bool
}

func newDepTrace(block *b.Block) *depTrace {
	if depTraceDir == "" {
		return nil
	}
	n := len(block.Transactions)
	accounts, _ := plannerAccountsForBlock(block)
	return &depTrace{start: make([]time.Duration, n), end: make([]time.Duration, n), errs: make([]string, n),
		aheadStart: make([]time.Duration, n), aheadEnd: make([]time.Duration, n), adopted: make([]bool, n),
		queuedAt: make([]time.Duration, n), sentAt: make([]time.Duration, n), accounts: accounts, changed: make([][]bool, n)}
}

// recordQueued notes when transaction idx's offer was queued, relative to the loop start.
func (d *depTrace) recordQueued(idx int, at time.Duration) {
	d.queuedAt[idx] = at
}

// writableBefore copies transaction idx's writable accounts before it runs. The
// dependency plan keeps every other writer of those accounts out until idx
// commits, so writableChanged can compare against them afterwards.
func (d *depTrace) writableBefore(slotCtx *sealevel.SlotCtx, idx int) []*accounts.Account {
	if !depTraceChanged || idx >= len(d.accounts) {
		return nil
	}
	writable := d.accounts[idx].writable()
	before := make([]*accounts.Account, len(writable))
	for j, access := range writable {
		if acct, err := slotCtx.GetAccountShared(access.publicKey); err == nil && acct != nil {
			before[j] = acct.Clone()
		}
	}
	return before
}

// writableChanged records which of idx's writable accounts changed content.
func (d *depTrace) writableChanged(slotCtx *sealevel.SlotCtx, idx int, before []*accounts.Account) {
	if !depTraceChanged || idx >= len(d.accounts) {
		return
	}
	writable := d.accounts[idx].writable()
	changed := make([]bool, len(writable))
	for j, access := range writable {
		after, err := slotCtx.GetAccountShared(access.publicKey)
		if err != nil {
			after = nil
		}
		changed[j] = !specSameAccount(before[j], after)
	}
	d.changed[idx] = changed
}

// recordAhead notes transaction idx's early execution window relative to the
// loop start, and whether its result was adopted.
func (d *depTrace) recordAhead(idx int, began, ended time.Duration, adopted bool) {
	d.aheadStart[idx], d.aheadEnd[idx], d.adopted[idx] = began, ended, adopted
}

// recordSend is called on the dispatcher goroutine as it dispatches idx.
func (d *depTrace) recordSend(idx int, at time.Duration) {
	d.sentAt[idx] = at
}

// record is called by the worker that ran transaction idx; each index is written once.
func (d *depTrace) record(idx int, start, end time.Duration, err error) {
	d.start[idx], d.end[idx] = start, end
	if err != nil {
		msg := err.Error()
		if len(msg) > 80 {
			msg = msg[:80]
		}
		d.errs[idx] = msg
	}
}

type depTraceTx struct {
	Index    int    `json:"i"`
	Executed bool   `json:"x"`
	Failed   bool   `json:"f"`
	Err      string `json:"e,omitempty"`
	CU       uint64 `json:"cu"`
	StartNs  int64  `json:"s"`
	EndNs    int64  `json:"t"`
	Payer    int    `json:"p"`
	Writable []int  `json:"w"`
	Readonly []int  `json:"r"`
	// FailedInstr is the failing top-level instruction (-1: failed before any
	// instruction, or succeeded); Prefix the accounts instructions 0..FailedInstr used.
	FailedInstr int   `json:"fi"`
	NumInstrs   int   `json:"ni"`
	Prefix      []int `json:"px,omitempty"`
	// AheadStart/AheadEnd: early execution window (work-ahead), 0 if none.
	AheadStart int64 `json:"as,omitempty"`
	AheadEnd   int64 `json:"ae,omitempty"`
	Adopted    bool  `json:"ad,omitempty"`
	QueuedNs   int64 `json:"qa,omitempty"` // when the offer was queued, 0 if never
	SentNs     int64 `json:"ds"`
	// Programs: top-level program of each instruction (key indices, in order);
	// FailedProgram: the failing top-level instruction's program, or -1.
	Programs      []int `json:"pg"`
	FailedProgram int   `json:"fp"`
	// Changed: the writable accounts whose content changed when the
	// transaction committed (a subset of Writable); nil unless
	// MITHRIL_DEPTRACE_CHANGED=1.
	Changed []int `json:"cw"`
}

func (d *depTrace) write(block *b.Block, execute []bool, cus []uint64) {
	var total uint64
	for _, cu := range cus {
		total += cu
	}
	if total < depTraceMinCU {
		for _, tx := range block.Transactions {
			failedPrefix.Delete(tx)
		}
		return
	}
	accounts := d.accounts
	if len(accounts) != len(block.Transactions) {
		return
	}
	keyIndex := map[[32]byte]int{}
	var keys []string
	index := func(key [32]byte) int {
		if i, ok := keyIndex[key]; ok {
			return i
		}
		keyIndex[key] = len(keys)
		keys = append(keys, fmt.Sprintf("%x", key[:8]))
		return len(keys) - 1
	}
	programNames := map[int]string{}
	txs := make([]depTraceTx, len(block.Transactions))
	for i, tx := range block.Transactions {
		t := depTraceTx{Index: i, Executed: i < len(execute) && execute[i], Failed: d.errs[i] != "", Err: d.errs[i],
			CU: cus[i], StartNs: int64(d.start[i]), EndNs: int64(d.end[i]), Payer: -1, Writable: []int{}, Readonly: []int{},
			FailedInstr: -1, NumInstrs: len(tx.Message.Instructions),
			AheadStart: int64(d.aheadStart[i]), AheadEnd: int64(d.aheadEnd[i]), Adopted: d.adopted[i], FailedProgram: -1,
			QueuedNs: int64(d.queuedAt[i]),
			SentNs:   int64(d.sentAt[i])}
		for _, instr := range tx.Message.Instructions {
			if int(instr.ProgramIDIndex) < len(tx.Message.AccountKeys) {
				key := tx.Message.AccountKeys[instr.ProgramIDIndex]
				idx := index([32]byte(key))
				programNames[idx] = key.String()
				t.Programs = append(t.Programs, idx)
			}
		}
		if v, ok := failedPrefix.LoadAndDelete(tx); ok {
			failure := v.(depTraceFailure)
			t.FailedInstr = failure.instr
			if failure.instr < len(t.Programs) {
				t.FailedProgram = t.Programs[failure.instr]
			}
			for _, key := range failure.keys {
				t.Prefix = append(t.Prefix, index([32]byte(key)))
			}
		}
		if len(tx.Message.AccountKeys) > 0 {
			t.Payer = index([32]byte(tx.Message.AccountKeys[0]))
		}
		for _, access := range accounts[i].accounts {
			if access.writable {
				t.Writable = append(t.Writable, index([32]byte(access.publicKey)))
			} else {
				t.Readonly = append(t.Readonly, index([32]byte(access.publicKey)))
			}
		}
		if d.changed[i] != nil {
			t.Changed = []int{}
			for j, access := range accounts[i].writable() {
				if j < len(d.changed[i]) && d.changed[i][j] {
					t.Changed = append(t.Changed, index([32]byte(access.publicKey)))
				}
			}
		}
		txs[i] = t
	}
	out, err := json.Marshal(map[string]any{"slot": block.Slot, "cu": total, "keys": keys, "programs": programNames, "txs": txs})
	if err == nil {
		_ = os.WriteFile(filepath.Join(depTraceDir, fmt.Sprintf("slot-%d.json", block.Slot)), out, 0o644)
	}
}
