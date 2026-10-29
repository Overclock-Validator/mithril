package blockprod

import (
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

// BatchSink receives flushed entry batches for shred/broadcast.
type BatchSink interface {
	OnEntryBatch(entries []turbine.Entry, batchBytes int)
}

// NopBatchSink discards flushed entry batches.
type NopBatchSink struct{}

func (NopBatchSink) OnEntryBatch([]turbine.Entry, int) {}

// WorkingBank is the leader's mutable slot state used by the forge sink.
type WorkingBank struct {
	mu sync.Mutex

	slotCtx *sealevel.SlotCtx
	slot    uint64
	leader  solana.PublicKey

	costs     *costmodel.CostTracker
	entries   *EntryBuilder
	sink      BatchSink
	forgedTxs []*solana.Transaction
	txFees    fees.TxFeeInfoAccumulator
	numSigs   uint64
	entryHash solana.Hash
	accepting bool
	// ancestorStatuses is the immutable transaction-status lineage pinned when
	// this bank's replay parent was selected.
	ancestorStatuses *replay.TransactionStatusView
	// seenMessages is the bank-local AlreadyProcessed status set. The TPU's
	// signature LRU is only an ingress optimization and is not authoritative.
	seenMessages map[[32]byte]struct{}
	// Execution and commit are serialized by mu. Borrowed accounts never escape
	// either phase, so their storage can be reset for the next transaction.
	borrowedAccounts *arena.Arena[sealevel.BorrowedAccount]
	preparer         *replay.TransactionPreparer
	// tpuAllowed admits TPU transactions. It only moves from false to true, so
	// a TPU source that observed it set never races a later close.
	tpuAllowed atomic.Bool
}

type BankConfig struct {
	SlotCtx             *sealevel.SlotCtx
	Slot                uint64
	Leader              solana.PublicKey
	Limits              costmodel.Limits
	EntryHash           solana.Hash
	Sink                BatchSink
	TransactionStatuses *replay.TransactionStatusView
	// TPUGated opens the bank with TPU admission closed until AllowTPU.
	TPUGated bool
}

func NewWorkingBank(cfg BankConfig) *WorkingBank {
	limits := cfg.Limits
	if limits.BlockCost == 0 {
		limits = costmodel.DefaultLimits()
	}
	if limits.MaxBatchBytes == 0 {
		limits.MaxBatchBytes = costmodel.DefaultTargetBatchBytes
	}
	if limits.MaxEntryBytes == 0 {
		limits.MaxEntryBytes = costmodel.DefaultPackEntryBytes()
	}
	sink := cfg.Sink
	if sink == nil {
		sink = NopBatchSink{}
	}
	var preparer *replay.TransactionPreparer
	if cfg.SlotCtx != nil {
		preparer = replay.NewTransactionPreparer(cfg.SlotCtx.Features)
	}
	bank := &WorkingBank{
		preparer:         preparer,
		slotCtx:          cfg.SlotCtx,
		slot:             cfg.Slot,
		leader:           cfg.Leader,
		costs:            costmodel.NewCostTracker(limits),
		entries:          NewEntryBuilder(limits, cfg.EntryHash),
		sink:             sink,
		entryHash:        cfg.EntryHash,
		accepting:        true,
		ancestorStatuses: cfg.TransactionStatuses,
		seenMessages:     make(map[[32]byte]struct{}),
		borrowedAccounts: arena.New[sealevel.BorrowedAccount](64),
	}
	bank.tpuAllowed.Store(!cfg.TPUGated)
	return bank
}

// TPUAllowed reports whether TPU sources may forge into this bank. Other
// producers forge directly and are not subject to the gate.
func (b *WorkingBank) TPUAllowed() bool {
	return b.tpuAllowed.Load()
}

// AllowTPU opens TPU admission. It is one-way and safe to call repeatedly.
func (b *WorkingBank) AllowTPU() {
	b.tpuAllowed.Store(true)
}

func (b *WorkingBank) Slot() uint64 {
	return b.slot
}

func (b *WorkingBank) Leader() solana.PublicKey {
	return b.leader
}

func (b *WorkingBank) SlotCtx() *sealevel.SlotCtx {
	return b.slotCtx
}

func (b *WorkingBank) CostTracker() *costmodel.CostTracker {
	return b.costs
}

func (b *WorkingBank) EntryBuilder() *EntryBuilder {
	return b.entries
}

func (b *WorkingBank) EntryHash() solana.Hash {
	return b.entryHash
}

func (b *WorkingBank) ForgedTransactions() []*solana.Transaction {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*solana.Transaction, len(b.forgedTxs))
	copy(out, b.forgedTxs)
	return out
}

func (b *WorkingBank) NumSignatures() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.numSigs
}

func (b *WorkingBank) TxFeeAccumulator() fees.TxFeeInfoAccumulator {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.txFees
}

// ReadAccount returns pubkey's account as this bank sees it: as written
// earlier in the slot, otherwise from parent state. ok is false for an
// account that does not exist.
func (b *WorkingBank) ReadAccount(pubkey solana.PublicKey) (*accounts.Account, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.slotCtx == nil {
		return nil, false
	}
	acct, err := b.slotCtx.GetAccount(pubkey)
	if err != nil && (b.slotCtx.UnrootedRead != nil || b.slotCtx.AccountsDb != nil) {
		acct, err = b.slotCtx.GetAccountFromAccountsDb(pubkey)
	}
	if err != nil || acct == nil || acct.Lamports == 0 {
		return nil, false
	}
	return acct, true
}

// Epoch is the bank's epoch.
func (b *WorkingBank) Epoch() uint64 {
	return b.slotCtx.Epoch
}

// LastBlockhash is the most recent blockhash the bank accepts: its parent's.
func (b *WorkingBank) LastBlockhash() solana.Hash {
	return b.slotCtx.LastBlockhash
}

// ForgeResult reports whether a transaction was committed into the working bank.
type ForgeResult int

const (
	ForgeAccepted ForgeResult = iota
	ForgeDroppedNoLeader
	ForgeDroppedVote
	ForgeDroppedParse
	ForgeDroppedCost
	ForgeDroppedExecution
	ForgeDroppedAlreadyProcessed
	ForgeDroppedAccountLocks
	ForgeDroppedLookup
)

func (r ForgeResult) String() string {
	switch r {
	case ForgeAccepted:
		return "accepted"
	case ForgeDroppedNoLeader:
		return "dropped_no_leader"
	case ForgeDroppedVote:
		return "dropped_vote"
	case ForgeDroppedParse:
		return "dropped_parse"
	case ForgeDroppedCost:
		return "dropped_cost"
	case ForgeDroppedExecution:
		return "dropped_execution"
	case ForgeDroppedAlreadyProcessed:
		return "dropped_already_processed"
	case ForgeDroppedAccountLocks:
		return "dropped_account_locks"
	case ForgeDroppedLookup:
		return "dropped_lookup"
	default:
		return "unknown"
	}
}

// Forge executes and commits a verified transaction wire into the working bank.
func (b *WorkingBank) Forge(wire []byte) (ForgeResult, costmodel.ExceedReason) {
	tx, err := solana.TransactionFromBytes(wire)
	if err != nil {
		return ForgeDroppedParse, costmodel.ExceedNone
	}
	return b.ForgeTransaction(tx, len(wire))
}

// ForgeTransaction executes and commits a parsed transaction. The caller must
// keep it immutable: accepted transactions are retained for entry publication.
func (b *WorkingBank) ForgeTransaction(tx *solana.Transaction, wireSize int) (ForgeResult, costmodel.ExceedReason) {
	return b.forgeTransaction(tx, wireSize, nil, EntryReservation{}, tpuForge)
}

// ForgePreparedTransaction reuses static work from owned, immutable TPU bytes.
// A different bank feature snapshot falls back to the ordinary execution path.
func (b *WorkingBank) ForgePreparedTransaction(tx *solana.Transaction, wireSize int, prepared *replay.PreparedTransaction) (ForgeResult, costmodel.ExceedReason) {
	return b.ForgeScheduledTransaction(tx, wireSize, prepared, EntryReservation{})
}

// ForgeScheduledTransaction forges a transaction whose entry bytes were
// reserved by PrepareSchedule. It consumes res whatever the result; prepared
// may be nil.
func (b *WorkingBank) ForgeScheduledTransaction(tx *solana.Transaction, wireSize int, prepared *replay.PreparedTransaction, res EntryReservation) (ForgeResult, costmodel.ExceedReason) {
	if b.slotCtx == nil || !b.preparer.Matches(prepared, tx, b.slotCtx.Features) {
		prepared = nil
	}
	return b.forgeTransaction(tx, wireSize, prepared, res, tpuForge)
}

// ForgeStandaloneTransaction forges a Harmonic block transaction that is not
// revert protected. Unlike TPU forging, block limits are checked against the
// units execution consumed, so a transaction may request more than the slot
// has left as long as what it uses fits.
func (b *WorkingBank) ForgeStandaloneTransaction(tx *solana.Transaction, wireSize int) (ForgeResult, costmodel.ExceedReason) {
	return b.forgeTransaction(tx, wireSize, nil, EntryReservation{}, standaloneForge)
}

// forgePolicy selects how a forge path checks block limits.
type forgePolicy struct {
	// admitOnActual checks block limits after execution against actual usage
	// instead of before it against requested units.
	admitOnActual bool
}

var (
	tpuForge        = forgePolicy{}
	standaloneForge = forgePolicy{admitOnActual: true}
)

// forgeCandidate is a transaction's forge work that needs no bank lock.
type forgeCandidate struct {
	// execTx is what executes: tx, or for a v0 transaction with address-table
	// lookups, a resolved copy. tx itself stays unresolved for entry encoding.
	execTx *solana.Transaction
	// prepared is the static preparation, if any, still valid for execTx.
	prepared                 *replay.PreparedTransaction
	serializedSize           int
	messageHash              [32]byte
	ancestorAlreadyProcessed bool
	ancestorStatusErr        error
	cost                     costmodel.TransactionCost
}

// prepareForge does the lock-free part of forging tx: full-wire validation,
// the message hash, the account lock rules, the ancestor status lookup,
// address-table lookups and the cost estimate. Unless it returns
// ForgeAccepted, tx cannot be forged.
func (b *WorkingBank) prepareForge(tx *solana.Transaction, wireSize int, prepared *replay.PreparedTransaction) (forgeCandidate, ForgeResult) {
	c := forgeCandidate{execTx: tx, prepared: prepared}
	if tx == nil {
		return c, ForgeDroppedParse
	}
	// Validate the full wire before execution can charge fees or publish
	// account changes. Entry append then reuses this measured size and has no
	// fallible serialization step after commit, including on the prepared path.
	var err error
	c.serializedSize, err = serializedTransactionSize(tx)
	if err != nil {
		return c, ForgeDroppedParse
	}
	if prepared != nil {
		c.messageHash = prepared.MessageHash()
	} else {
		c.messageHash, err = replay.TransactionMessageHash(tx)
	}
	if err != nil {
		return c, ForgeDroppedParse
	}
	// A transaction that repeats an account or exceeds the account lock limit
	// can never be processed, and a block containing one is invalid. The check
	// is static, so do it before the bank lock.
	var lockFeats *features.Features
	if b.slotCtx != nil {
		lockFeats = b.slotCtx.Features
	}
	if txErr := replay.ValidateAccountLocks(tx, lockFeats); txErr != nil {
		return c, ForgeDroppedAccountLocks
	}
	if b.ancestorStatuses == nil {
		c.ancestorStatusErr = &replay.IncompleteTransactionStatusCoverageError{}
	} else {
		// The view is immutable and internally synchronizes its lazy per-blockhash
		// index. Do that potentially large one-time lookup outside the bank lock.
		c.ancestorAlreadyProcessed, c.ancestorStatusErr = b.ancestorStatuses.ContainsMessage(tx.Message.RecentBlockhash, c.messageHash)
	}

	if !c.ancestorAlreadyProcessed && c.ancestorStatusErr == nil {
		if replay.HasAddressTableLookups(tx) {
			// Tables are read from the parent state, which this slot does not
			// change, so resolve outside the bank lock.
			resolved, txErr := replay.ResolveAddressTableLookups(b.slotCtx, tx)
			if txErr != nil {
				return c, ForgeDroppedLookup
			}
			// A loaded address can repeat a static key or another loaded
			// address, or take the account count past the lock limit.
			if txErr := replay.ValidateAccountLocks(resolved, lockFeats); txErr != nil {
				return c, ForgeDroppedAccountLocks
			}
			c.execTx = resolved
			// Static preparation, if any, was done on the unresolved message.
			c.prepared = nil
		}
		feats := b.slotCtx.Features
		if feats == nil {
			f := features.NewFeaturesDefault()
			feats = f
		}
		if c.prepared != nil {
			c.cost = c.prepared.Cost()
		} else {
			c.cost, err = costmodel.EstimateTransactionCost(c.execTx, feats)
		}
		if err != nil {
			return c, ForgeDroppedParse
		}
		c.cost.WireSize = wireSize
	}
	return c, ForgeAccepted
}

// statusLocked rejects a candidate this bank or its ancestors already
// processed.
func (b *WorkingBank) statusLocked(c forgeCandidate) ForgeResult {
	if _, alreadyProcessed := b.seenMessages[c.messageHash]; alreadyProcessed {
		return ForgeDroppedAlreadyProcessed
	}
	if c.ancestorAlreadyProcessed {
		return ForgeDroppedAlreadyProcessed
	}
	if c.ancestorStatusErr != nil {
		// A live leader is never constructed with incomplete coverage. Preserve a
		// fail-closed result for isolated callers that bypass LeaderLoop.
		return ForgeDroppedNoLeader
	}
	return ForgeAccepted
}

func (b *WorkingBank) forgeTransaction(tx *solana.Transaction, wireSize int, prepared *replay.PreparedTransaction, res EntryReservation, policy forgePolicy) (ForgeResult, costmodel.ExceedReason) {
	c, result := b.prepareForge(tx, wireSize, prepared)
	if result != ForgeAccepted {
		b.RebateSchedule(res)
		return result, costmodel.ExceedNone
	}
	cost, serializedSize, messageHash := c.cost, c.serializedSize, c.messageHash
	execTx, prepared := c.execTx, c.prepared

	b.mu.Lock()
	defer b.mu.Unlock()
	// The reservation only held budget while the lock was released. The lock
	// is now held until append, so admission below is checked exactly.
	b.entries.release(res)
	if !b.accepting {
		return ForgeDroppedNoLeader, costmodel.ExceedNone
	}
	if result := b.statusLocked(c); result != ForgeAccepted {
		return result, costmodel.ExceedNone
	}

	if !policy.admitOnActual {
		if reason := b.costs.WouldExceed(cost); reason != costmodel.ExceedNone {
			return ForgeDroppedCost, reason
		}
	}
	if reason := b.admitEntryBytesLocked(serializedSize); reason != costmodel.ExceedNone {
		return ForgeDroppedCost, reason
	}
	if err := b.preparer.PayerCanFund(b.slotCtx, execTx, prepared); err != nil {
		return ForgeDroppedExecution, costmodel.ExceedNone
	}

	output := b.preparer.LoadAndExecute(replay.LoadAndExecuteTransactionInput{
		SlotCtx:           b.slotCtx,
		Transaction:       execTx,
		LeanResult:        true,
		SkipTimingMetrics: true,
		Arena:             b.borrowedAccounts,
	}, prepared)
	// Execution only touched transaction-local accounts; nothing is published
	// until the apply below, so a transaction refused here leaves no trace.
	execCU, loadedCost := actualExecutionUsage(output)
	if policy.admitOnActual {
		if reason := b.costs.WouldExceed(cost.WithActualUsage(execCU, loadedCost)); reason != costmodel.ExceedNone {
			return ForgeDroppedCost, reason
		}
	}
	if output.ProcessingResult.TransactionError != nil {
		feeInfo, err := replay.ApplyFailedTransaction(b.slotCtx, execTx, output)
		if err != nil {
			return ForgeDroppedExecution, costmodel.ExceedNone
		}
		output.FeeInfo = feeInfo
	} else {
		if err := replay.ApplySuccessfulTransaction(b.slotCtx, output); err != nil {
			return ForgeDroppedExecution, costmodel.ExceedNone
		}
	}
	if b.seenMessages == nil {
		b.seenMessages = make(map[[32]byte]struct{})
	}
	b.seenMessages[messageHash] = struct{}{}
	if output.FeeInfo != nil {
		b.txFees.Add(output.FeeInfo)
	}
	b.forgedTxs = append(b.forgedTxs, tx)
	b.numSigs += uint64(tx.Message.Header.NumRequiredSignatures)

	b.costs.Record(cost)
	b.costs.Rebate(cost, execCU, loadedCost)
	if flushed, batchBytes, didFlush := b.entries.appendSerialized(*tx, wireSize, serializedSize); didFlush {
		b.entryHash = b.entries.CurrentEntryHash()
		b.sink.OnEntryBatch(flushed, batchBytes)
	}
	return ForgeAccepted, costmodel.ExceedNone
}

func actualExecutionUsage(output replay.LoadAndExecuteTransactionOutput) (execCU, loadedCost uint64) {
	loadedCost = costmodel.LoadedAccountsDataSizeCost(output.LoadedAccountsDataSize)
	execCtx := output.ExecCtx
	if execCtx == nil {
		return 0, loadedCost
	}
	execCU = execCtx.ComputeMeter.Used()
	return execCU, loadedCost
}

// BufferedDropReason classifies why a buffered transaction should be discarded.
type BufferedDropReason int

const (
	BufferedKeep BufferedDropReason = iota
	BufferedExpired
	BufferedAlreadyProcessed
)

// ClassifyBuffered reports whether a buffered transaction is expired or already
// processed relative to this bank.
func (b *WorkingBank) ClassifyBuffered(blockhash solana.Hash, messageHash [32]byte) BufferedDropReason {
	if rbh := sealevel.SysvarCache.RecentBlockHashes.Sysvar; rbh == nil || !rbh.IsBlockhashAgeValid(blockhash) {
		return BufferedExpired
	}
	if b.ancestorStatuses != nil {
		if ok, err := b.ancestorStatuses.ContainsMessage(blockhash, messageHash); err == nil && ok {
			return BufferedAlreadyProcessed
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, seen := b.seenMessages[messageHash]; seen {
		return BufferedAlreadyProcessed
	}
	return BufferedKeep
}

// PrepareSchedule reserves entry bytes at schedule time. If the next
// transaction would not fit in the current FEC set, the batch is
// closed and shredded first. If the tx would then exceed the remaining
// slot entry-byte budget, nothing is reserved. Pass the reservation to
// ForgeScheduledTransaction, or to RebateSchedule if it is not forged.
func (b *WorkingBank) PrepareSchedule(wireSize int) (EntryReservation, costmodel.ExceedReason) {
	return b.PrepareScheduleN([]int{wireSize})
}

// PrepareScheduleN is PrepareSchedule for transactions forged together, such
// as a bundle. The reservation covers any split into batches.
func (b *WorkingBank) PrepareScheduleN(wireSizes []int) (EntryReservation, costmodel.ExceedReason) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.accepting || len(wireSizes) == 0 {
		return EntryReservation{}, costmodel.ExceedNone
	}
	if b.entries.wouldOverflowBatch(wireSizes[0]) {
		b.flushEntriesLocked()
	}
	res, ok := b.entries.reserve(wireSizes)
	if !ok {
		return EntryReservation{}, costmodel.ExceedBatchBytes
	}
	return res, costmodel.ExceedNone
}

// RebateSchedule releases a schedule-time entry-byte reservation.
func (b *WorkingBank) RebateSchedule(res EntryReservation) {
	if res.bytes == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries.release(res)
}

// admitEntryBytesLocked checks the slot entry-byte budget for a transaction
// appended before the bank lock is released, closing a full batch first.
func (b *WorkingBank) admitEntryBytesLocked(serializedSize int) costmodel.ExceedReason {
	if b.entries.wouldOverflowBatch(serializedSize) {
		b.flushEntriesLocked()
	}
	if b.entries.wouldExceedSlot(serializedSize) {
		return costmodel.ExceedBatchBytes
	}
	return costmodel.ExceedNone
}

// EntryBytes is flushed plus pending plus reserved serialized entry bytes.
func (b *WorkingBank) EntryBytes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.entries.SlotBytes()
}

// FlushEntries emits any pending transactions as an entry batch.
func (b *WorkingBank) FlushEntries() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushEntriesLocked()
}

// Freeze stops transaction admission and emits the leftover entry batch even
// if it does not fill a FEC set. It waits for any forge already holding the
// bank lock, so every accepted transaction is included before the footer
// bank hash is computed. Last-in-slot is marked on the ending tick, not here.
func (b *WorkingBank) Freeze() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.accepting = false
	b.entries.dropReservation()
	b.flushEntriesLocked()
}

// Close stops transaction admission without emitting pending entries. It is
// used when an active slot is abandoned because its replay parent changed.
func (b *WorkingBank) Close() {
	b.mu.Lock()
	b.accepting = false
	b.entries.dropReservation()
	b.mu.Unlock()
}

func (b *WorkingBank) flushEntriesLocked() {
	entries, batchBytes := b.entries.Flush()
	if len(entries) == 0 {
		return
	}
	b.entryHash = b.entries.CurrentEntryHash()
	b.sink.OnEntryBatch(entries, batchBytes)
}
