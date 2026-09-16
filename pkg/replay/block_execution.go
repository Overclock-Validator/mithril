package replay

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/bankhash"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rent"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
)

// blockExecution is the resumable state of one bank's execution. ProcessBlock
// drives it in one pass (plan, load, execute every transaction, finalize).
// Streaming execution opens it before the block is complete, feeds
// transaction groups as their shreds arrive, and finalizes against the
// complete block. Both paths share the opening (bank sysvars, SlotCtx) and the
// tail (fees, rent, footer, bank hash, commit), so a bank produced either way
// runs the same end-of-block code over the same SlotCtx.
type blockExecution struct {
	acctsDb             *accountsdb.AccountsDb
	block               *b.Block
	epochSchedule       *sealevel.SysvarEpochSchedule
	txParallelism       int
	dbgOpts             *DebugOptions
	persistedHashes     *persistedTracker
	tail                unrootedState
	transactionStatuses *TransactionStatusCache
	alpenglowClock      bool
	parentBankSysvars   *sealevel.BankSysvars

	blockSrc          blockAccountSource
	slotCtx           *sealevel.SlotCtx
	parentAccts       accounts.MemAccounts
	accts             accounts.Accounts
	bankSysvars       *sealevel.BankSysvars
	bankEpochSchedule *sealevel.SysvarEpochSchedule

	ctx            context.Context
	task           *trace.Task
	setReplayStage func(string)
	watchdogDone   chan struct{}
	sigverifyWg    sync.WaitGroup
	closed         bool

	// Whole-block inputs to the tail, set by ProcessBlock.
	executionPlan     blockTransactionExecutionPlan
	statusPreparation *transactionStatusPreparation
	statusValidation  transactionStatusValidation

	// Incremental transaction bookkeeping in block order, maintained by
	// executeTransactionGroup. ProcessBlock does not use it.
	transactions        []*solana.Transaction
	identities          []txstatus.TransactionMessageIdentity
	execute             []bool
	seenMessages        map[[32]byte]int
	processedTxCount    uint64
	processedSignatures uint64
	groups              int

	txFeeAccumulator fees.TxFeeInfoAccumulator
	totalCU          uint64

	// Retained across global metric resets while a speculative stream waits.
	accountLoader metrics.AccountLoader
}

// newBlockExecution installs the per-bank trace task, the stage watchdog and
// the account source; it does not touch bank state.
func newBlockExecution(
	acctsDb *accountsdb.AccountsDb,
	block *b.Block,
	epochSchedule *sealevel.SysvarEpochSchedule,
	txParallelism int,
	dbgOpts *DebugOptions,
	persistedHashes *persistedTracker,
	tail unrootedState,
	transactionStatuses *TransactionStatusCache,
	alpenglowClock bool,
	parentBankSysvars *sealevel.BankSysvars,
) *blockExecution {
	exec := &blockExecution{
		acctsDb:             acctsDb,
		block:               block,
		epochSchedule:       epochSchedule,
		txParallelism:       txParallelism,
		dbgOpts:             dbgOpts,
		persistedHashes:     persistedHashes,
		tail:                tail,
		transactionStatuses: transactionStatuses,
		alpenglowClock:      alpenglowClock,
		parentBankSysvars:   parentBankSysvars,
		seenMessages:        make(map[[32]byte]int),
	}
	// In rooted-durable mode, block accounts/sysvars load through the unrooted
	// tail (overlay→durable) so execution sees confirmed-but-unrooted state.
	exec.blockSrc = acctsDb
	if tail != nil {
		exec.blockSrc = tail
	}

	ctx, task := trace.NewTask(context.Background(), "ProcessBlock")
	exec.ctx, exec.task = ctx, task
	trace.Log(ctx, "slot", fmt.Sprintf("%d", block.Slot))
	trace.Log(ctx, "txCount", fmt.Sprintf("%d", len(block.Transactions)))

	var replayStage atomic.Value
	var replayStageSince atomic.Int64
	exec.setReplayStage = func(stage string) {
		replayStage.Store(stage)
		replayStageSince.Store(time.Now().UnixNano())
	}

	exec.watchdogDone = make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		var lastLoggedStage string
		var lastLoggedSince int64
		for {
			select {
			case <-exec.watchdogDone:
				return
			case <-ticker.C:
				stageVal := replayStage.Load()
				stage, ok := stageVal.(string)
				if !ok || stage == "" {
					continue
				}
				sinceUnix := replayStageSince.Load()
				if sinceUnix == 0 {
					continue
				}
				if stage == lastLoggedStage && sinceUnix == lastLoggedSince {
					continue
				}
				stageDuration := time.Since(time.Unix(0, sinceUnix))
				if stageDuration < 10*time.Second {
					continue
				}
				mlog.Log.Warnf("REPLAY WATCHDOG: slot %d stuck in stage %s for %s | txs=%d | lightbringer=%t",
					block.Slot, stage, stageDuration.Round(time.Second), len(block.Transactions), block.FromLiveStream)
				lastLoggedStage = stage
				lastLoggedSince = sinceUnix
			}
		}
	}()
	return exec
}

// close joins outstanding signature verification, stops the watchdog and ends
// the trace task, in the order ProcessBlock's deferred cleanup always used. It
// is idempotent so a discarded stream and a finalized bank both call it.
func (exec *blockExecution) close() {
	if exec == nil || exec.closed {
		return
	}
	exec.closed = true
	sigverifyJoinStart := time.Now()
	exec.sigverifyWg.Wait()
	metrics.GlobalBlockReplay.SignatureVerificationJoin.AddTimingSince(sigverifyJoinStart)
	if exec.watchdogDone != nil {
		close(exec.watchdogDone)
	}
	if exec.task != nil {
		exec.task.End()
	}
}

// installSlotCtx publishes the loaded parent snapshot and derived bank sysvars
// as this bank's SlotCtx. The overlay/parent pair comes from
// loadBlockAccountsAndUpdateSysvars; streaming grows the parent snapshot
// afterwards, group by group, through loadTransactionAccounts.
func (exec *blockExecution) installSlotCtx(accts accounts.Accounts, parentAccts accounts.Accounts, accountMapCapacity int, bankSysvars *sealevel.BankSysvars) error {
	block := exec.block
	slotCtx := newSlotCtx(block, accts, parentAccts, exec.acctsDb, exec.tail, accountMapCapacity)
	if err := slotCtx.PublishBankSysvars(bankSysvars); err != nil {
		return fmt.Errorf("publish bank sysvars at slot %d: %w", block.Slot, err)
	}
	bankEpochScheduleValue, ok := bankSysvars.EpochSchedule()
	if !ok {
		return fmt.Errorf("bank-local EpochSchedule sysvar unavailable at slot %d", block.Slot)
	}
	slotCtx.TraceCtx = exec.ctx
	exec.slotCtx = slotCtx
	exec.accts = accts
	if mem, ok := parentAccts.(accounts.MemAccounts); ok {
		exec.parentAccts = mem
	}
	exec.bankSysvars = bankSysvars
	exec.bankEpochSchedule = &bankEpochScheduleValue
	return nil
}

// open performs the bank-start work that needs only the parent state: the
// parent sysvar pin and this bank's Clock/SlotHashes derivation, the parent
// snapshot for whatever transactions the block currently carries (none for a
// streaming shell), the overlay, and the SlotCtx. It is ProcessBlock's opening
// without the whole-block planner, so a streaming caller can start executing
// groups before any transaction of the block is known.
func (exec *blockExecution) open() error {
	defer exec.captureAccountLoader()()
	block := exec.block
	exec.setReplayStage("prepare_dependency_planner")
	if SerializedParameterArena != nil {
		SerializedParameterArena.Reset()
	}

	start := time.Now()
	exec.setReplayStage("load_accounts")
	loadAcctsRegion := trace.StartRegion(exec.ctx, "LoadBlockAccounts")
	accts, parentAccts, accountMapCapacity, bankSysvars, err := loadBlockAccountsAndUpdateSysvars(exec.blockSrc, block, exec.epochSchedule, exec.alpenglowClock, exec.parentBankSysvars, nil)
	loadAcctsRegion.End()
	if err != nil {
		return fmt.Errorf("load slot accounts and update sysvars at slot %d: %w", block.Slot, err)
	}
	if err := bankSysvars.ValidateForExecution(); err != nil {
		return fmt.Errorf("invalid bank sysvar snapshot at slot %d: %w", block.Slot, err)
	}
	metrics.GlobalBlockReplay.LoadBlockAccounts.AddTimingSince(start)

	slotCtxSetupStart := time.Now()
	if err := exec.installSlotCtx(accts, parentAccts, accountMapCapacity, bankSysvars); err != nil {
		return err
	}
	metrics.GlobalBlockReplay.SlotCtxSetup.AddTimingSince(slotCtxSetupStart)
	return nil
}

// errBlockExecutionClosed reports a group offered after close or finalize.
var errBlockExecutionClosed = errors.New("block execution is closed")

// groupIdentitiesFor returns prepared identities for a transaction group,
// hashing the messages when the caller has none from signature verification.
func groupIdentitiesFor(txs []*solana.Transaction, identities *b.PreparedTransactionMessageIdentities) (*b.PreparedTransactionMessageIdentities, error) {
	if identities != nil {
		if identities.Len() != len(txs) {
			return nil, fmt.Errorf("group identities cover %d transactions, group has %d", identities.Len(), len(txs))
		}
		return identities, nil
	}
	view := &b.Block{Transactions: txs}
	return view.PrepareTransactionMessageIdentities()
}

// executeTransactionGroup executes the next transactions of the block, in
// block order, against the open SlotCtx. Every check ProcessBlock applies to a
// whole block is applied incrementally: message versions against the bank's
// features, duplicate messages across every group so far (a duplicate makes
// the whole block invalid, exactly as planBlockTransactionExecution reports
// it), ancestor status-cache validation, address-table resolution, account
// loading into the same parent snapshot, and a dependency plan over the group
// executed by up to txParallelism workers. Groups run strictly one after
// another, so cross-group ordering is the sequential block order.
//
// identities may carry the verifier's message identities for exactly these
// transactions; nil hashes them here. shouldVerifySignatures is passed to
// ProcessTransaction unchanged.
func (exec *blockExecution) executeTransactionGroup(txs []*solana.Transaction, identities *b.PreparedTransactionMessageIdentities, shouldVerifySignatures bool) error {
	if exec == nil || exec.slotCtx == nil {
		return errors.New("block execution is not open")
	}
	if exec.closed {
		return errBlockExecutionClosed
	}
	if len(txs) == 0 {
		return nil
	}
	block := exec.block
	slot := block.Slot
	base := len(exec.transactions)

	view := &b.Block{Slot: slot, Transactions: txs, Features: block.Features}
	if err := validateBlockTransactionVersions(view); err != nil {
		return fmt.Errorf("validate transaction versions for slot %d: %w", slot, err)
	}

	prepared, err := groupIdentitiesFor(txs, identities)
	if err != nil {
		return fmt.Errorf("validate transaction messages for slot %d: %w", slot, err)
	}
	execute := make([]bool, len(txs))
	var duplicates *DuplicateTransactionMessagesError
	for idx, tx := range txs {
		if tx == nil {
			return fmt.Errorf("validate transaction messages for slot %d: transaction %d is nil", slot, base+idx)
		}
		identity := prepared.Identity(idx)
		if firstIndex, duplicate := exec.seenMessages[identity.MessageHash]; duplicate {
			if duplicates == nil {
				duplicates = &DuplicateTransactionMessagesError{Slot: slot}
			}
			duplicates.DuplicateCount++
			if len(duplicates.Occurrences) < maxDuplicateTransactionOccurrences {
				duplicates.Occurrences = append(duplicates.Occurrences, DuplicateTransactionOccurrence{
					Index: base + idx, FirstIndex: firstIndex,
				})
			}
			continue
		}
		exec.seenMessages[identity.MessageHash] = base + idx
		execute[idx] = true
	}
	if duplicates != nil {
		return fmt.Errorf("validate transaction messages for slot %d: %w", slot, duplicates)
	}
	if exec.transactionStatuses != nil {
		if err := exec.transactionStatuses.validateTransactionsAgainstAncestors(slot, prepared); err != nil {
			return fmt.Errorf("validate transaction statuses for slot %d: %w", slot, err)
		}
	}

	// Record the group before executing so a failure after this point still
	// leaves the block-order view consistent for finalize's prefix proof.
	for idx, tx := range txs {
		exec.transactions = append(exec.transactions, tx)
		exec.identities = append(exec.identities, prepared.Identity(idx))
		exec.execute = append(exec.execute, execute[idx])
		if execute[idx] {
			exec.processedTxCount++
			exec.processedSignatures += uint64(tx.Message.Header.NumRequiredSignatures)
		}
	}
	exec.slotCtx.NumSignatures = exec.processedSignatures

	exec.setReplayStage("load_accounts")
	if err := exec.loadTransactionAccounts(view); err != nil {
		return err
	}

	exec.setReplayStage("tx_loop")
	start := time.Now()
	txLoopRegion := trace.StartRegion(exec.ctx, "TxLoop")
	feeInfos, computeUnits, err := exec.runTransactionGroup(txs, execute, shouldVerifySignatures)
	txLoopRegion.End()
	metrics.GlobalBlockReplay.TxLoop.AddTimingSince(start)
	if err != nil {
		return err
	}
	for idx, txFeeInfo := range feeInfos {
		if !execute[idx] {
			continue
		}
		exec.totalCU += computeUnits[idx]
		if txFeeInfo == nil {
			reportNilFeeInfo(exec.slotCtx, txs[idx], slot)
		}
		exec.txFeeAccumulator.Add(txFeeInfo)
	}
	exec.slotCtx.TotalComputeUnitsConsumed = exec.totalCU
	exec.groups++
	metrics.GlobalBlockReplay.StreamingExecution.Groups++
	metrics.GlobalBlockReplay.StreamingExecution.Transactions += uint64(len(txs))
	return nil
}

// captureAccountLoader isolates this stream's loader work from the global
// record, which may belong to another replayed slot or be reset while waiting.
// Only the replay goroutine may enter this scope; loader workers are joined
// before it exits. Discarded streams never publish their retained totals.
func (exec *blockExecution) captureAccountLoader() func() {
	previous := metrics.GlobalBlockReplay.AccountLoader
	metrics.GlobalBlockReplay.AccountLoader = metrics.AccountLoader{}
	return func() {
		exec.accountLoader.Accumulate(metrics.GlobalBlockReplay.AccountLoader)
		metrics.GlobalBlockReplay.AccountLoader = previous
	}
}

// loadTransactionAccounts resolves the group's address-table lookups and adds
// the pristine parent image of every account the group can touch to the
// parent snapshot, exactly as the whole-block loader does for a block, except
// that accounts already present keep their earlier image: the batch read at
// block.Slot through the same source returns parent state regardless of the
// overlay, so the first image is the right one and later groups must not
// replace it.
func (exec *blockExecution) loadTransactionAccounts(view *b.Block) error {
	defer exec.captureAccountLoader()()
	phaseStart := time.Now()
	if err := resolveAddrTableLookups(exec.blockSrc, view); err != nil {
		return fmt.Errorf("resolve address table lookups at slot %d: %w", view.Slot, err)
	}
	metrics.GlobalBlockReplay.AccountLoader.AddressTableLookups.AddTimingSince(phaseStart)

	phaseStart = time.Now()
	dedupedAccts, _ := extractAndDedupeBlockAccts(view)
	if exec.parentAccts.Map != nil {
		filtered := dedupedAccts[:0]
		for _, key := range dedupedAccts {
			if _, loaded := exec.parentAccts.Map[key]; !loaded {
				filtered = append(filtered, key)
			}
		}
		dedupedAccts = filtered
	}
	metrics.GlobalBlockReplay.AccountLoader.DedupeBlockAccounts.AddTimingSince(phaseStart)
	if len(dedupedAccts) == 0 {
		return nil
	}

	phaseStart = time.Now()
	slotAccts, batchStats, err := getAccountsBatchSharedWithStats(context.Background(), exec.blockSrc, view.Slot, dedupedAccts)
	metrics.GlobalBlockReplay.AccountLoader.SourceBatch.AddTimingSince(phaseStart)
	recordAccountLoaderBatchStats(&metrics.GlobalBlockReplay.AccountLoader, batchStats)
	if err != nil {
		return fmt.Errorf("load transaction accounts at slot %d: %w", view.Slot, err)
	}
	if exec.parentAccts.Map == nil {
		return fmt.Errorf("load transaction accounts at slot %d: parent snapshot is not a memory account set", view.Slot)
	}
	phaseStart = time.Now()
	for _, acct := range slotAccts {
		if acct == nil {
			continue
		}
		if _, loaded := exec.parentAccts.Map[acct.Key]; loaded {
			continue
		}
		key := [32]byte(acct.Key)
		if err := exec.parentAccts.SetAccount(&key, acct); err != nil {
			return err
		}
	}
	metrics.GlobalBlockReplay.AccountLoader.ParentAccounts += uint64(len(slotAccts))
	metrics.GlobalBlockReplay.AccountLoader.ParentMapBuild.AddTimingSince(phaseStart)
	return nil
}

// runTransactionGroup is parallelTxLoop over a transaction slice with a plan
// built for the group alone (indices are group-local). Without the planner
// (txParallelism == 0, or an unresolvable lookup) it runs sequentially, which
// is always correct because groups are consumed in block order.
func (exec *blockExecution) runTransactionGroup(txs []*solana.Transaction, execute []bool, shouldVerifySignatures bool) ([]*fees.TxFeeInfo, []uint64, error) {
	slotCtx := exec.slotCtx
	feeInfos := make([]*fees.TxFeeInfo, len(txs))
	computeUnits := make([]uint64, len(txs))
	dbgOpts := exec.dbgOpts

	workers := exec.txParallelism
	if workers > len(txs) {
		workers = len(txs)
	}
	var plan *dependencyPlan
	if workers > 1 {
		plannerBuildStart := time.Now()
		plannerAccounts, available := plannerAccountsForBlock(&b.Block{Transactions: txs})
		if available {
			plan = buildDependencyPlan(plannerAccounts)
		}
		metrics.GlobalBlockReplay.DependencyPlannerBuild.AddTimingSince(plannerBuildStart)
	}
	if plan == nil {
		for idx, tx := range txs {
			if !execute[idx] {
				continue
			}
			feeInfos[idx], computeUnits[idx], _ = ProcessTransaction(slotCtx, &exec.sigverifyWg, tx, nil, dbgOpts, nil, shouldVerifySignatures)
		}
		return feeInfos, computeUnits, nil
	}

	metrics.GlobalBlockReplay.DependencyPlannerPrepared = 1
	do := make(chan int, len(txs))
	done := make(chan int, len(txs))
	plannerDone := make(chan struct{})
	go func() {
		defer close(plannerDone)
		plannerDispatchStart := time.Now()
		dispatchDependencyPlan(plan, do, done)
		metrics.GlobalBlockReplay.DependencyPlannerDispatch.AddTimingSince(plannerDispatchStart)
	}()

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(workerIdx int) {
			defer wg.Done()
			var workerArena *arena.Arena[sealevel.BorrowedAccount]
			if workerIdx < len(sealevel.BorrowedAccountArenas) {
				workerArena = sealevel.BorrowedAccountArenas[workerIdx]
			}
			for idx := range do {
				if !execute[idx] {
					done <- idx
					continue
				}
				feeInfos[idx], computeUnits[idx], _ = ProcessTransaction(slotCtx, &exec.sigverifyWg, txs[idx], nil, dbgOpts, workerArena, shouldVerifySignatures)
				done <- idx
			}
		}(i)
	}
	wg.Wait()
	close(done)
	<-plannerDone
	return feeInfos, computeUnits, nil
}

// reportNilFeeInfo reproduces ProcessBlock's diagnostic for a transaction whose
// fee information is missing, which only happens when blockhash validation
// failed for a transaction the block claims to have processed.
func reportNilFeeInfo(slotCtx *sealevel.SlotCtx, tx *solana.Transaction, slot uint64) {
	var recentBlockhashes sealevel.SysvarRecentBlockhashes
	if bankSysvars := slotCtx.BankSysvars(); bankSysvars != nil {
		recentBlockhashes, _ = bankSysvars.RecentBlockhashes()
	}
	mlog.Log.Errorf("txFeeInfo is nil for tx %s in slot %d", tx.Signatures[0], slot)
	mlog.Log.Errorf("  tx blockhash: %s", tx.Message.RecentBlockhash)
	mlog.Log.Errorf("  LatestEvictedBlockhash: %x", slotCtx.LatestEvictedBlockhash[:8])
	if len(recentBlockhashes) > 0 {
		mlog.Log.Errorf("  RecentBlockhashes: %d entries, newest=%x, oldest=%x",
			len(recentBlockhashes), recentBlockhashes[0].Blockhash[:8], recentBlockhashes[len(recentBlockhashes)-1].Blockhash[:8])
	} else {
		mlog.Log.Errorf("  RecentBlockhashes: nil or empty!")
	}
	panic(fmt.Sprintf("txFeeInfo is nil - blockhash validation failed for tx %s", tx.Signatures[0]))
}

// finalize runs the end-of-block phases over the open SlotCtx: fees to the
// leader, rent, incinerator, the Alpenglow footer clock and vote rewards, bank
// sysvar finalization, bank hash, footer verification, state publication and
// transaction status commit. It is the unchanged tail of ProcessBlock and is
// shared by streaming execution, which calls it once the complete block has
// been matched against the executed prefix.
func (exec *blockExecution) finalize() (*sealevel.SlotCtx, error) {
	block := exec.block
	slotCtx := exec.slotCtx
	acctsDb := exec.acctsDb
	tail := exec.tail
	setReplayStage := exec.setReplayStage
	alpenglowClock := exec.alpenglowClock
	bankEpochSchedule := exec.bankEpochSchedule
	txFeeAccumulator := exec.txFeeAccumulator
	executionPlan := exec.executionPlan
	transactionStatuses := exec.transactionStatuses
	persistedHashes := exec.persistedHashes
	var err error

	start := time.Now()
	setReplayStage("distribute_fees")

	// distribute tx fees to the slot leader
	// skip leader handling if there are zero transactions in this block
	if !global.ManageLeaderSchedule() && block.BlockReward != nil && len(block.Transactions) > 0 {
		slotCtx.LamportsBurnt = fees.DistributeTxFeesToSlotLeader(acctsDb, slotCtx, block.BlockReward.Leader, &txFeeAccumulator)
		slotCtx.RecordModifiedAcct(block.BlockReward.Leader)
	} else if global.ManageLeaderSchedule() && len(block.Transactions) > 0 {
		slotCtx.LamportsBurnt = fees.DistributeTxFeesToSlotLeader(acctsDb, slotCtx, block.Leader, &txFeeAccumulator)
		slotCtx.RecordModifiedAcct(block.Leader)
	}
	metrics.GlobalBlockReplay.Reward.AddTimingSince(start)

	start = time.Now()
	setReplayStage("collect_rent")
	bankRent, ok := slotCtx.BankSysvars().Rent()
	if !ok {
		return nil, fmt.Errorf("bank-local Rent sysvar unavailable at slot %d", block.Slot)
	}
	rentAccts := rent.CollectRentEagerly(slotCtx, &bankRent, bankEpochSchedule)
	metrics.GlobalBlockReplay.Rent.AddTimingSince(start)

	start = time.Now()
	setReplayStage("run_incinerator")
	runIncinerator(slotCtx)
	metrics.GlobalBlockReplay.RunIncinerator.AddTimingSince(start)

	// Alpenglow banks set the Clock timestamp from the block footer after execution.
	if alpenglowClock {
		footerClockStart := time.Now()
		if err := applyAlpenglowFooterClock(slotCtx, block, bankEpochSchedule); err != nil {
			metrics.GlobalBlockReplay.AlpenglowFooterClock.AddTimingSince(footerClockStart)
			return nil, fmt.Errorf("apply alpenglow footer clock at slot %d: %w", block.Slot, err)
		}
		if err := updateAlpenglowNanosecondClockAccount(slotCtx, block); err != nil {
			metrics.GlobalBlockReplay.AlpenglowFooterClock.AddTimingSince(footerClockStart)
			return nil, err
		}
		metrics.GlobalBlockReplay.AlpenglowFooterClock.AddTimingSince(footerClockStart)
		voteRewardsStart := time.Now()
		voteRewardsErr := ApplyAlpenglowVoteRewards(slotCtx, block, bankEpochSchedule, block.SkipRewardCert, block.NotarRewardCert, block.BlockFinalCert, block.AlpenglowShredVersion)
		metrics.GlobalBlockReplay.AlpenglowVoteRewards.AddTimingSince(voteRewardsStart)
		if voteRewardsErr != nil {
			return nil, voteRewardsErr
		}
	}
	if err := finalizeBankSysvars(slotCtx); err != nil {
		return nil, fmt.Errorf("finalize bank sysvars at slot %d: %w", block.Slot, err)
	}

	setReplayStage("compile_accounts")
	start = time.Now()
	writableAccts, modifiedAccts := compileWritableAndModifiedAccts(slotCtx, block, rentAccts)
	metrics.GlobalBlockReplay.CompileWritableAndModifiedAccts.AddTimingSince(start)
	start = time.Now()
	ensureParentsErr := ensureParentAccountsForModified(slotCtx, modifiedAccts)
	metrics.GlobalBlockReplay.EnsureParentAccountsForModified.AddTimingSince(start)
	if ensureParentsErr != nil {
		return nil, ensureParentsErr
	}

	start = time.Now()
	setReplayStage("bankhash")
	slotCtx.FinalBankhash = bankhash.CalculateBankHash(slotCtx, writableAccts, modifiedAccts, block.ParentBankhash, slotCtx.NumSignatures, block.Blockhash)
	metrics.GlobalBlockReplay.BankHash.AddTimingSince(start)
	if alpenglowClock {
		footerVerificationStart := time.Now()
		footerVerificationErr := verifyAlpenglowBlockFooter(slotCtx, block, alpenglowClock)
		metrics.GlobalBlockReplay.AlpenglowFooterVerification.AddTimingSince(footerVerificationStart)
		if footerVerificationErr != nil {
			writeFooterBankhashMismatchArtifact(footerVerificationErr, block, slotCtx, writableAccts, modifiedAccts)
			return nil, footerVerificationErr
		}
	}

	// Bankhash consensus enforcement is handled in the replay loop (not here)
	// because forkchoice is fed after ProcessBlock returns — checking here would
	// never see votes from recently submitted blocks and could deadlock.

	// Enter critical commit window - panics here may leave AccountsDB inconsistent
	commitSlot.Store(slotCtx.Slot)
	commitInProgress.Store(true)
	blockUpdateStart := time.Now()
	setReplayStage("store_accounts")
	persistedSlot := slotCtx.Slot
	persistedBankhash := append([]byte(nil), slotCtx.FinalBankhash...)
	persistedBlockSlot := block.Slot
	stakeIndexDir := filepath.Join(acctsDb.AcctsDir, "..")
	afterStoreAccounts := func() {
		if tail != nil {
			// Rooted-durable: accounts + bankhash are buffered in the overlay and
			// become durable only on promotion; nothing written here (rooted-only).
		} else {
			if berr := acctsDb.StoreBankHashForSlot(persistedSlot, persistedBankhash); berr != nil {
				mlog.Log.Infof("unable to store bankhash for slot %d", persistedSlot)
			}
		}
		if tail == nil {
			// Legacy/verify modes (no fork ambiguity): flush per block as before.
			// Rooted-durable replay flushes at FOLD time instead — entries stay
			// slot-scoped in RAM so a fork unwind can drop them, and scans merge
			// the pending set (StreamStakeAccounts) for completeness meanwhile.
			flushed, err := global.FlushPendingStakePubkeys(stakeIndexDir)
			if err != nil {
				mlog.Log.Errorf("failed to flush stake pubkey index: %v", err)
			} else if flushed > 0 {
				mlog.Log.Debugf("flushed %d new stake pubkeys to index", flushed)
			}
		}

		persistedHashes.Set(persistedBlockSlot, persistedBankhash)

		// Exit critical commit window - AccountsDB is now consistent
		commitInProgress.Store(false)
		commitSlot.Store(0)
	}

	if tail != nil {
		// Rooted-durable: buffer this slot's writes + bankhash in the RAM overlay
		// (always, even when empty, so the bankhash is recorded); no durable write.
		tail.Add(slotCtx.Slot, modifiedAccts, persistedBankhash)
		afterStoreAccounts()
	} else if len(modifiedAccts) > 0 {
		err = acctsDb.StoreAccounts(modifiedAccts, slotCtx.Slot, afterStoreAccounts)
	}
	// In rooted-durable mode the callback above is synchronous, so this includes
	// the complete critical-path overlay publication. Legacy StoreAccounts only
	// enqueues here; its asynchronous disk work deliberately belongs to no slot's
	// replay wall time and must never update a later slot's metrics record.
	metrics.GlobalBlockReplay.BlockUpdateAccounts.AddTimingSince(blockUpdateStart)
	if err != nil {
		return slotCtx, err
	}
	statusCommitStart := time.Now()
	statusWaitStart := time.Now()
	preparedStatuses := exec.statusPreparation.wait()
	metrics.GlobalBlockReplay.TransactionStatusPreparationWait.AddTimingSince(statusWaitStart)
	statusErr := transactionStatuses.commitBlockWithValidation(block, executionPlan, preparedStatuses, exec.statusValidation)
	metrics.GlobalBlockReplay.TransactionStatusCommit.AddTimingSince(statusCommitStart)
	if statusErr != nil {
		return nil, fmt.Errorf("commit transaction statuses for slot %d after bank state commit: %w", block.Slot, statusErr)
	}

	global.IncrTransactionCount(executionPlan.processedTxCount)
	setReplayStage("done")
	return slotCtx, err
}
