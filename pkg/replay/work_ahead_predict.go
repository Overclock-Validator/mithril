package replay

import (
	"bytes"
	"hash/maphash"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// The program predictor (MITHRIL_WORK_AHEAD_PREDICT=program) estimates the
// probability that a transaction fails from what it calls and what is
// happening to its accounts, using only committed outcomes:
//   - program history: the failure rate of its program combination (top-level
//     programs other than compute budget, system, token, associated token and
//     memo) across earlier blocks;
//   - account rate: the highest failure rate among its non-payer writable
//     accounts over this block's committed transactions.
//
// On the traced heavy blocks these two (weighted 0.77 and 0.45) flagged chain
// predecessors right 93% of the time and caught 93% of the failing ones; the
// signer added nothing once the program was known.
//
// MITHRIL_WORK_AHEAD_MIN_GAIN_US gates speculation on expected time saved:
// probability the prerequisite fails times the smaller of the two expected
// durations (running means per program combination), so cheap predecessors are
// not worth an early execution.
var workAheadMinGain = time.Duration(specShadowEnvInt("MITHRIL_WORK_AHEAD_MIN_GAIN_US", 0)) * time.Microsecond

const (
	programWeight        = 0.77
	accountWeight        = 0.45
	defaultAccountRate   = 0.3
	defaultDuration      = 100 * time.Microsecond
	durationMeanWeight   = 0.1 // weight of a new sample in the running mean
	programStatsShards   = 64
	programStatsPerShard = 1 << 12
)

var plumbingPrograms = []solana.PublicKey{
	addresses.ComputeBudgetProgramAddr, addresses.SystemProgramAddr,
	solana.TokenProgramID, solana.Token2022ProgramID, solana.SPLAssociatedTokenAccountProgramID, solana.MemoProgramID,
}

type programStat struct {
	fails, total uint32
	meanNs       float64
}

// programStats is shared across blocks; shards are emptied when full.
var programStats [programStatsShards]struct {
	mu    sync.RWMutex
	stats map[uint64]*programStat
	_     [32]byte
}

func lookupProgramStat(key uint64) (programStat, bool) {
	shard := &programStats[key%programStatsShards]
	shard.mu.RLock()
	s, ok := shard.stats[key]
	var out programStat
	if ok {
		out = *s
	}
	shard.mu.RUnlock()
	return out, ok
}

func recordProgramStat(key uint64, failed bool, elapsed time.Duration) {
	shard := &programStats[key%programStatsShards]
	shard.mu.Lock()
	if shard.stats == nil || len(shard.stats) >= programStatsPerShard {
		shard.stats = make(map[uint64]*programStat, programStatsPerShard)
	}
	s := shard.stats[key]
	if s == nil {
		s = &programStat{meanNs: float64(elapsed)}
		shard.stats[key] = s
	}
	s.total++
	if failed {
		s.fails++
	}
	if elapsed > 0 {
		s.meanNs += durationMeanWeight * (float64(elapsed) - s.meanNs)
	}
	shard.mu.Unlock()
}

// programKey identifies tx's program combination; cached per transaction.
func (w *workAhead) programKey(tx int) uint64 {
	if cached := w.progKey[tx].Load(); cached != 0 {
		return cached
	}
	keys := w.txs[tx].Message.AccountKeys
	var ids []solana.PublicKey
	for _, instr := range w.txs[tx].Message.Instructions {
		if int(instr.ProgramIDIndex) < len(keys) {
			ids = append(ids, keys[instr.ProgramIDIndex])
		}
	}
	key := programKeyOf(ids)
	w.progKey[tx].Store(key)
	return key
}

// programKeyOf hashes the distinct non-plumbing programs among the top-level
// instruction program ids, in sorted order; never 0, which marks "not computed".
func programKeyOf(ids []solana.PublicKey) uint64 {
	var buf [8]solana.PublicKey
	programs := buf[:0]
	for _, program := range ids {
		if !slices.Contains(plumbingPrograms, program) && !slices.Contains(programs, program) {
			programs = append(programs, program)
		}
	}
	slices.SortFunc(programs, func(a, b solana.PublicKey) int { return bytes.Compare(a[:], b[:]) })
	// FNV-1a, 64 bit
	h := uint64(14695981039346656037)
	for i := range programs {
		for _, c := range programs[i] {
			h = (h ^ uint64(c)) * 1099511628211
		}
	}
	return h | 1
}

// observe folds tx's committed outcome and execution time into the program
// table and this block's account table. It runs on the planner goroutine
// (direct dispatch) or the committing worker (dispatcher mode), after
// recordOutcome stored the outcome and duration.
func (w *workAhead) observe(tx int) {
	outcome := w.outcome[tx].Load()
	if outcome == 0 {
		return
	}
	failed := outcome == 2
	recordProgramStat(w.programKey(tx), failed, time.Duration(w.durNs[tx].Load()))
	payer := w.payerOf(tx)
	w.acctMu.Lock()
	for _, a := range w.accessOf(tx).writable() {
		if a.publicKey == payer {
			continue
		}
		c := w.acctFails[a.publicKey]
		c[1]++
		if failed {
			c[0]++
		}
		w.acctFails[a.publicKey] = c
	}
	w.acctMu.Unlock()
}

// failureProbability is the program predictor's estimate for tx.
func (w *workAhead) failureProbability(tx int) float64 {
	pProgram := 0.5
	if s, ok := lookupProgramStat(w.programKey(tx)); ok {
		pProgram = (float64(s.fails) + 1) / (float64(s.total) + 2)
	}
	pAccount := defaultAccountRate
	payer := w.payerOf(tx)
	w.acctMu.Lock()
	seen := false
	for _, a := range w.accessOf(tx).writable() {
		if a.publicKey == payer {
			continue
		}
		if c, ok := w.acctFails[a.publicKey]; ok && c[1] > 0 {
			rate := (float64(c[0]) + 1) / (float64(c[1]) + 2)
			if !seen || rate > pAccount {
				pAccount, seen = rate, true
			}
		}
	}
	w.acctMu.Unlock()
	return (programWeight*pProgram + accountWeight*pAccount) / (programWeight + accountWeight)
}

// expectedDuration is the running mean execution time of tx's program
// combination, or a default before any history.
func (w *workAhead) expectedDuration(tx int) time.Duration {
	if s, ok := lookupProgramStat(w.programKey(tx)); ok && s.meanNs > 0 {
		return time.Duration(s.meanNs)
	}
	return defaultDuration
}

// predictFailureFor decides whether tx may be executed early past the
// unfinished prerequisite on the assumption that it fails.
func (w *workAhead) predictFailureFor(prerequisite, tx int) bool {
	if w.predictFailure(prerequisite, tx) {
		return true
	}
	return workAheadUnchanged && w.predictUnchangedFor(prerequisite, tx)
}

func (w *workAhead) predictFailure(prerequisite, tx int) bool {
	switch workAheadPredict {
	case "inverse":
		// bench only: wrong most of the time, an adverse case for pressure tests
		return !payerLastFailed(w.payerOf(prerequisite))
	case "any":
		return true
	case "account":
		lw := w.lastWriter[prerequisite]
		return lw >= 0 && w.outcome[lw].Load() == 2
	case "program":
		p := w.failureProbability(prerequisite)
		if p < 0.5 {
			return false
		}
		if workAheadMinGain > 0 {
			overlap := min(w.expectedDuration(prerequisite), w.expectedDuration(tx))
			if time.Duration(p*float64(overlap)) < workAheadMinGain {
				return false
			}
		}
		return true
	default:
		return payerLastFailed(w.payerOf(prerequisite))
	}
}

// MITHRIL_WORK_AHEAD_UNCHANGED=1: a predecessor not predicted to fail may also
// be overtaken when history says its program combination leaves every account
// the transaction uses alone when it succeeds, and its fee payer is not one of
// them. This only widens what is offered early. Adoption is unchanged: the
// read-set check at the turn.
//
// What "alone" means follows the check that will judge the result, so the
// history predicts what can actually be adopted (learnUnchanged):
//   - by default (version check only): the predecessor never borrowed the
//     account for writing, so its commit installs no new version of it;
//   - with MITHRIL_WORK_AHEAD_VALUE_CHECK=1 as well: also an account it wrote
//     back with the content it had before its first write.
var workAheadUnchanged = os.Getenv("MITHRIL_WORK_AHEAD_UNCHANGED") == "1"

// MITHRIL_WORK_AHEAD_UNCHANGED_KEY=role keys that history by each role the
// account fills instead of by (program combination, account address): for every
// top-level instruction that lists the account, the instruction's program, its
// first data byte and the account's position in it. Whether a program changes an
// account is usually decided by the instruction and the account's position, not
// by which account it is, so roles generalize across accounts and recur far more
// often (on the mainnet corpus: recall 84% vs 58% of unchanged accounts, at 99.6%
// precision). An account is predicted unchanged when every role it fills is. An
// account no instruction lists is predicted unchanged without history; if
// something still publishes it (load-time rent-epoch normalization), the version
// check rejects the early result.
var workAheadUnchangedByRole = os.Getenv("MITHRIL_WORK_AHEAD_UNCHANGED_KEY") == "role"

func init() {
	if workAheadUnchanged && workAheadValueCheck {
		sealevel.RecordWriteOriginals = true
	}
}

const (
	unchangedMinObserved  = 3
	unchangedMinRateTenth = 9 // at least 90% unchanged

	// The history is one fixed table of packed words, shared across blocks:
	// no locks, no pointers for the collector to trace, nothing allocated per
	// key. A key hashes to a pair of adjacent slots (one cache line); a new key
	// takes the empty one or replaces the less observed. The table is advisory
	// (it only chooses what is offered early), so a lost update between racing
	// writers, an evicted key or a tag collision costs at most a missed or a
	// discarded early execution.
	unchangedSlotsLog2 = 20
	unchangedSlots     = 1 << unchangedSlotsLog2

	// word layout: tag (32) | total (16) | last changed (1) | unchanged (15)
	unchangedCountMax    = 1<<15 - 1
	unchangedLastChanged = 1 << 15
)

type unchangedKey struct {
	program uint64
	account [32]byte
}

var (
	unchangedTable [unchangedSlots]atomic.Uint64
	// seeded per process, so which keys share a slot cannot be chosen in advance
	unchangedSeed = maphash.MakeSeed()
)

// roleKey is the unchanged-history key of the account at position pos of a
// top-level instruction of program with the given data. Role keys put the
// program id in the account field and mark the program field with the top bit.
func roleKey(program solana.PublicKey, data []byte, pos int) unchangedKey {
	first := uint64(0x100) // no data
	if len(data) > 0 {
		first = uint64(data[0])
	}
	return unchangedKey{program: 1<<63 | first<<16 | uint64(pos), account: [32]byte(program)}
}

// unchangedSlot returns the first slot of k's pair and its tag.
func unchangedSlot(k unchangedKey) (slot int, tag uint64) {
	h := (maphash.Bytes(unchangedSeed, k.account[:]) ^ k.program) * 0x9E3779B97F4A7C15
	h ^= h >> 29
	return int(h>>(64-unchangedSlotsLog2)) &^ 1, (h & 0xFFFFFFFF) << 32
}

func unchangedCounts(word uint64) (unchanged, total uint64) {
	return word & unchangedCountMax, word >> 16 & 0xFFFF
}

// findUnchanged returns k's word, or 0 when it has no history.
func findUnchanged(k unchangedKey) uint64 {
	slot, tag := unchangedSlot(k)
	for i := slot; i < slot+2; i++ {
		if word := unchangedTable[i].Load(); word != 0 && word&^0xFFFFFFFF == tag {
			return word
		}
	}
	return 0
}

func recordUnchanged(k unchangedKey, unchanged bool) {
	slot, tag := unchangedSlot(k)
	first, second := unchangedTable[slot].Load(), unchangedTable[slot+1].Load()
	at, word := slot, uint64(0)
	_, firstTotal := unchangedCounts(first)
	_, secondTotal := unchangedCounts(second)
	switch {
	case first != 0 && first&^0xFFFFFFFF == tag:
		word = first
	case second != 0 && second&^0xFFFFFFFF == tag:
		at, word = slot+1, second
	case secondTotal < firstTotal:
		at = slot + 1 // new key: the empty or less observed slot
	}
	same, total := unchangedCounts(word)
	if total >= unchangedCountMax {
		same, total = same/2, total/2 // keep the rate, make room
	}
	total++
	word = tag | total<<16
	if unchanged {
		word |= same + 1
	} else {
		word |= same | unchangedLastChanged
	}
	unchangedTable[at].Store(word)
}

// predictsUnchanged: seen at least unchangedMinObserved times, at least 90%
// unchanged, and unchanged the last time.
func predictsUnchanged(k unchangedKey) bool {
	word := findUnchanged(k)
	same, total := unchangedCounts(word)
	return total >= unchangedMinObserved && word&unchangedLastChanged == 0 && 10*same >= unchangedMinRateTenth*total
}

// resetUnchangedHistory empties the table.
func resetUnchangedHistory() {
	for i := range unchangedTable {
		unchangedTable[i].Store(0)
	}
}

// learnUnchanged records, at a successful commit, which of the transaction's
// writable accounts other than the fee payer it left alone: not borrowed for
// writing, or, with the value check on, written back unchanged.
func learnUnchanged(execCtx *sealevel.ExecutionCtx) {
	txCtx := execCtx.TransactionContext
	if txCtx == nil {
		return
	}
	accts := &txCtx.Accounts
	if workAheadValueCheck && accts.Originals == nil {
		return
	}
	var program uint64
	if !workAheadUnchangedByRole {
		var buf [16]solana.PublicKey
		ids := buf[:0]
		for _, instr := range txCtx.AllInstructions {
			ids = append(ids, instr.ProgramId)
		}
		program = programKeyOf(ids)
	}
	for idx := 1; idx < len(accts.Accounts); idx++ {
		if idx >= len(accts.AcctMetas) || accts.AcctMetas[idx] == nil || !accts.AcctMetas[idx].IsWritable {
			continue
		}
		if idx >= len(accts.Touched) {
			continue
		}
		var unchanged bool
		switch {
		case !accts.Touched[idx]:
			unchanged = true
		case !workAheadValueCheck:
			unchanged = false // its commit installs a new version, whatever the content
		case idx < len(accts.Originals) && accts.Originals[idx] != nil:
			unchanged = specSameAccount(accts.Originals[idx], accts.Accounts[idx])
		default:
			continue // not shared when loaded: no earlier version to compare
		}
		key := accts.Accounts[idx].Key
		if !workAheadUnchangedByRole {
			recordUnchanged(unchangedKey{program: program, account: key}, unchanged)
			continue
		}
		for _, instr := range txCtx.AllInstructions {
			for pos, meta := range instr.Accounts {
				if meta.Pubkey == key {
					recordUnchanged(roleKey(instr.ProgramId, instr.Data, pos), unchanged)
				}
			}
		}
	}
}

// predictsUnchangedByRole: every role key filled by the account in tx's
// top-level instructions predicts unchanged (true if no instruction lists it).
func predictsUnchangedByRole(tx *solana.Transaction, account solana.PublicKey) bool {
	keys := tx.Message.AccountKeys
	for _, instr := range tx.Message.Instructions {
		if int(instr.ProgramIDIndex) >= len(keys) {
			return false
		}
		for pos, ai := range instr.Accounts {
			if int(ai) < len(keys) && keys[ai] == account && !predictsUnchanged(roleKey(keys[instr.ProgramIDIndex], instr.Data, pos)) {
				return false
			}
		}
	}
	return true
}

// predictUnchangedFor: overtaking the unfinished prerequisite is safe for tx if
// it succeeds as history predicts, leaving every account tx uses unchanged.
func (w *workAhead) predictUnchangedFor(prerequisite, tx int) bool {
	payer := w.payerOf(prerequisite)
	mine := w.accessOf(tx).accounts
	uses := func(key solana.PublicKey) bool {
		for _, a := range mine {
			if a.publicKey == key {
				return true
			}
		}
		return false
	}
	if uses(payer) {
		return false
	}
	if workAheadUnchangedByRole {
		for _, a := range w.accessOf(prerequisite).writable() {
			if uses(a.publicKey) && !predictsUnchangedByRole(w.txs[prerequisite], a.publicKey) {
				return false
			}
		}
		return true
	}
	program := w.programKey(prerequisite)
	for _, a := range w.accessOf(prerequisite).writable() {
		if uses(a.publicKey) && !predictsUnchanged(unchangedKey{program: program, account: [32]byte(a.publicKey)}) {
			return false
		}
	}
	return true
}
