package replay

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

const defaultFoldBatchSlots = 128

// FoldBatchSlots controls the account fold unit. Full batches run
// asynchronously; graceful shutdown may force a finalized trailing partial.
var FoldBatchSlots = defaultFoldBatchSlots

type batchCommitter interface {
	CommitBatch(
		deltas []accounts.SlotDelta,
		throughSlot uint64,
		bankhashes map[uint64][32]byte,
		resumeCtx []byte,
	) (accountsdb.BatchCommitResult, error)
}

type speculativeFoldJob struct {
	deltas      []accounts.SlotDelta
	through     uint64
	bankhashes  map[uint64][32]byte
	bankhash    []byte
	snapshot    *ReplayHeadSnapshot
	resume      *state.ResumeContext
	resumeJSON  []byte
	stakeIdxDir string
}

type speculativeFoldResult struct {
	job *speculativeFoldJob
	err error
}

// ConfigureDurable enables crash-aware batch folding. It is intentionally
// separate from Enable so narrow replay tests can exercise fork decisions
// without opening an AccountsDB or starting a goroutine.
func (sr *SpeculativeReplay) ConfigureDurable(
	db *accountsdb.AccountsDb,
	mithrilState *state.MithrilState,
	stakeIndexDir string,
	batchSlots int,
) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.accountsDb = db
	sr.configureCommitterLocked(db, mithrilState, stakeIndexDir, batchSlots)
}

func (sr *SpeculativeReplay) configureCommitterLocked(
	committer batchCommitter,
	mithrilState *state.MithrilState,
	stakeIndexDir string,
	batchSlots int,
) {
	if committer == nil || sr.committer != nil {
		return
	}
	if batchSlots <= 0 {
		batchSlots = defaultFoldBatchSlots
	}
	sr.committer = committer
	sr.mithrilState = mithrilState
	sr.stakeIndexDir = stakeIndexDir
	sr.foldBatchSlots = batchSlots
	sr.foldJobs = make(chan *speculativeFoldJob, 1)
	sr.foldResults = make(chan speculativeFoldResult, 1)
	sr.foldDone = make(chan struct{})
	go sr.runFoldWorker()
}

func (sr *SpeculativeReplay) runFoldWorker() {
	defer close(sr.foldDone)
	for job := range sr.foldJobs {
		start := time.Now()
		err := runSpeculativeFold(sr.committer, job)
		if err == nil {
			mlog.Log.FileOnlyf("rooted-durable: folded %d executed slots through %d in %s",
				len(job.deltas), job.through, time.Since(start).Round(time.Millisecond))
		}
		sr.foldResults <- speculativeFoldResult{job: job, err: err}
	}
}

func runSpeculativeFold(committer batchCommitter, job *speculativeFoldJob) error {
	if job.stakeIdxDir != "" {
		// Persist this auxiliary index first. It is safe for the index to be a
		// superset of AccountsDB because readers validate every pubkey against the
		// canonical account store; the opposite ordering could commit an account
		// fold while permanently omitting its stake pubkey.
		if _, err := global.FlushPendingStakePubkeysThrough(job.stakeIdxDir, job.through); err != nil {
			return fmt.Errorf("fold through slot %d: flush stake index: %w", job.through, err)
		}
	}
	if _, err := committer.CommitBatch(job.deltas, job.through, job.bankhashes, job.resumeJSON); err != nil {
		return fmt.Errorf("fold through slot %d: %w", job.through, err)
	}
	if job.stakeIdxDir != "" {
		if err := global.CompactStakePubkeyIndex(job.stakeIdxDir); err != nil {
			// The durable commit already succeeded. Compaction is maintenance,
			// so report it without making in-memory durability lag the manifest.
			mlog.Log.Warnf("fold through slot %d: compact stake index: %v", job.through, err)
		}
	}
	return nil
}

// Err reports an irreversible consensus/durability error found by the fold
// gate. Replay must halt rather than execute indefinitely on an unpromotable
// suffix.
func (sr *SpeculativeReplay) Err() error {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.foldErr
}

func (sr *SpeculativeReplay) DurableHead() (uint64, []byte) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.headSnapshot == nil {
		return sr.committedSlot, nil
	}
	return sr.committedSlot, append([]byte(nil), sr.headSnapshot.FinalBankhash...)
}

func (sr *SpeculativeReplay) PopulateDurableResult(result *ReplayResult) {
	if result == nil {
		return
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	snapshot := sr.headSnapshot
	if snapshot == nil {
		return
	}
	result.LastBlockHeight = snapshot.BlockHeight
	if snapshot.AcctsLtHash != nil {
		result.LastAcctsLtHash = snapshot.AcctsLtHash.Clone()
	}
	if snapshot.FeeRateGovernor != nil {
		result.LastLamportsPerSignature = snapshot.FeeRateGovernor.LamportsPerSignature
		result.LastPrevLamportsPerSig = snapshot.FeeRateGovernor.PrevLamportsPerSignature
	}
	result.LastNumSignatures = snapshot.NumSignatures
	if snapshot.RecentBlockhashes != nil {
		recent := append(sealevel.SysvarRecentBlockhashes(nil), (*snapshot.RecentBlockhashes)...)
		result.LastRecentBlockhashes = &recent
	}
	result.LastEvictedBlockhash = snapshot.LatestEvictedBlockhash
	result.LastBlockhash = snapshot.Blockhash
	if snapshot.SlotHashes != nil {
		slotHashes := append(sealevel.SysvarSlotHashes(nil), (*snapshot.SlotHashes)...)
		result.LastSlotHashes = &slotHashes
	}
	result.LastCapitalization = snapshot.Capitalization
	result.LastSlotsPerYear = snapshot.SlotsPerYear
	result.LastInflation.Initial = snapshot.InflationInitial
	result.LastInflation.Terminal = snapshot.InflationTerminal
	result.LastInflation.Taper = snapshot.InflationTaper
	result.LastInflation.FoundationVal = snapshot.InflationFoundation
	result.LastInflation.FoundationTerm = snapshot.InflationFoundationTerm
	if snapshot.EpochResume != nil {
		result.ComputedEpochStakes = make(map[uint64][]byte, len(snapshot.EpochResume.ComputedEpochStakes))
		for epoch, encoded := range snapshot.EpochResume.ComputedEpochStakes {
			result.ComputedEpochStakes[epoch] = []byte(encoded)
		}
	}
}

func (sr *SpeculativeReplay) pumpFoldLocked(
	pt *persistedTracker,
	decisionSource func(anchorSlot uint64) (alpenglow.ChainDecision, bool),
	force bool,
) int {
	if sr.foldErr != nil {
		return 0
	}
	applied := sr.pollFoldLocked(pt)
	if sr.foldErr != nil {
		return applied
	}
	if err := sr.advanceFinalityLocked(decisionSource); err != nil {
		sr.foldErr = err
		return applied
	}
	if sr.committer == nil || sr.foldInFlight {
		return applied
	}
	job, err := sr.buildFoldJobLocked(force)
	if err != nil {
		sr.foldErr = err
		return applied
	}
	if job != nil {
		sr.foldJobs <- job
		sr.foldInFlight = true
	}
	return applied
}

func (sr *SpeculativeReplay) advanceFinalityLocked(
	decisionSource func(anchorSlot uint64) (alpenglow.ChainDecision, bool),
) error {
	if decisionSource == nil {
		return nil
	}
	cursor := sr.finalityCursor
	if cursor < sr.committedSlot {
		cursor = sr.committedSlot
	}
	for examined := 0; examined < maxSpeculativeLayers*2; examined++ {
		decision, ok := decisionSource(cursor)
		if !ok {
			break
		}
		if decision.Slot <= cursor {
			return fmt.Errorf("Alpenglow decision did not advance: anchor=%d decision=%d", cursor, decision.Slot)
		}
		switch decision.Kind {
		case alpenglow.ChainDecisionKindConflict:
			return fmt.Errorf("ALPENGLOW SAFETY: conflicting certified decisions at slot %d: %s", decision.Slot, decision.Reason)
		case alpenglow.ChainDecisionKindSkip:
			if pending := sr.pending[decision.Slot]; pending != nil {
				return fmt.Errorf("ALPENGLOW SAFETY: certified skip at slot %d still has executed block state", decision.Slot)
			}
			cursor = decision.Slot
		case alpenglow.ChainDecisionKindBlock:
			if !alpenglowCertConfirmsPersist(decision.CertificateType) {
				sr.finalityCursor = cursor
				return nil
			}
			pending := sr.pending[decision.Block.Slot]
			if pending == nil {
				sr.finalityCursor = cursor
				return nil
			}
			if !pending.HasAlpenglowBlockID {
				return fmt.Errorf("ALPENGLOW SAFETY: finalized slot %d has no replayed block identity", decision.Block.Slot)
			}
			if decision.Block.Hash != pending.AlpenglowBlockID {
				return fmt.Errorf("ALPENGLOW SAFETY: finalized block %s at slot %d differs from replayed block %s",
					base58.Encode(decision.Block.Hash[:]), decision.Block.Slot, base58.Encode(pending.AlpenglowBlockID[:]))
			}
			if sr.rootSink != nil {
				sr.rootSink(decision.Block)
			}
			cursor = decision.Slot
		default:
			return fmt.Errorf("unknown Alpenglow decision %q at slot %d", decision.Kind, decision.Slot)
		}
	}
	sr.finalityCursor = cursor
	return nil
}

func (sr *SpeculativeReplay) buildFoldJobLocked(force bool) (*speculativeFoldJob, error) {
	if sr.finalityCursor <= sr.committedSlot {
		return nil, nil
	}
	prefix, err := sr.selectFoldPrefixLocked(force)
	if err != nil {
		return nil, err
	}
	if len(prefix) == 0 {
		return nil, nil
	}
	through := prefix[len(prefix)-1].Slot
	snapshot := sr.snapshots[through]
	if snapshot == nil {
		return nil, fmt.Errorf("fold through slot %d has no replay resume snapshot", through)
	}
	resume, err := resumeContextFromSnapshot(snapshot)
	if err != nil {
		return nil, fmt.Errorf("fold through slot %d: %w", through, err)
	}
	if err := ValidateRootedResumeContext(resume); err != nil {
		return nil, fmt.Errorf("fold through slot %d produced invalid resume context: %w", through, err)
	}
	resumeJSON, err := json.Marshal(resume)
	if err != nil {
		return nil, fmt.Errorf("fold through slot %d: marshal resume context: %w", through, err)
	}
	bankhashes := make(map[uint64][32]byte, len(prefix))
	for _, delta := range prefix {
		pending := sr.pending[delta.Slot]
		if pending == nil || len(pending.Bankhash) != 32 {
			return nil, fmt.Errorf("fold slot %d has no complete bankhash", delta.Slot)
		}
		var bankhash [32]byte
		copy(bankhash[:], pending.Bankhash)
		bankhashes[delta.Slot] = bankhash
	}
	return &speculativeFoldJob{
		deltas:      append([]accounts.SlotDelta(nil), prefix...),
		through:     through,
		bankhashes:  bankhashes,
		bankhash:    append([]byte(nil), snapshot.FinalBankhash...),
		snapshot:    snapshot,
		resume:      resume,
		resumeJSON:  resumeJSON,
		stakeIdxDir: sr.stakeIndexDir,
	}, nil
}

// selectFoldPrefixLocked keeps partitioned epoch rewards out of partial durable
// state. The old-epoch prefix is folded immediately, even when it is smaller
// than the normal batch. Reward slots then remain replayable overlays until a
// finalized snapshot says every partition has been applied; that whole window
// is committed as one batch. A crash therefore restarts before the transition
// instead of requiring private mid-distribution worklist state.
func (sr *SpeculativeReplay) selectFoldPrefixLocked(force bool) ([]accounts.SlotDelta, error) {
	limit := sr.foldBatchSlots
	prefix := sr.store.PromotionPrefix(sr.finalityCursor, limit)
	if len(prefix) == 0 {
		return nil, nil
	}

	firstActive := -1
	for i, delta := range prefix {
		if pending := sr.pending[delta.Slot]; pending != nil && pending.AwaitingReplaySnapshot {
			if force && i > 0 {
				return prefix[:i], nil
			}
			return nil, nil
		}
		snapshot := sr.snapshots[delta.Slot]
		if snapshot == nil {
			return nil, fmt.Errorf("fold slot %d has no replay resume snapshot", delta.Slot)
		}
		if rewardDistributionActive(snapshot) {
			firstActive = i
			break
		}
	}

	if firstActive < 0 {
		if !force && len(prefix) < limit {
			return nil, nil
		}
		return prefix, nil
	}
	if firstActive > 0 {
		// Establish the restart point immediately before epoch rewards. This is
		// deliberately a partial batch rather than waiting for the normal size.
		return prefix[:firstActive], nil
	}

	// The durable head is already immediately before an active rewards window.
	// Search every retained finalized layer for its completion snapshot; normal
	// batch sizing must not create a checkpoint in the middle of this range.
	window := sr.store.PromotionPrefix(sr.finalityCursor, maxSpeculativeLayers)
	start := sr.snapshots[window[0].Slot]
	if start == nil || start.Aux.PartitionedRewardsInfo == nil {
		return nil, fmt.Errorf("active rewards fold slot %d has no distribution metadata", window[0].Slot)
	}
	spoolSlot := start.Aux.PartitionedRewardsInfo.SpoolSlot
	for i, delta := range window {
		snapshot := sr.snapshots[delta.Slot]
		if snapshot == nil {
			return nil, fmt.Errorf("fold slot %d has no replay resume snapshot", delta.Slot)
		}
		if rewardDistributionComplete(snapshot, spoolSlot) {
			return window[:i+1], nil
		}
	}
	return nil, nil
}

func rewardDistributionActive(snapshot *ReplayHeadSnapshot) bool {
	if snapshot == nil || snapshot.Aux.PartitionedRewardsInfo == nil {
		return false
	}
	info := snapshot.Aux.PartitionedRewardsInfo
	return info.NumRewardPartitions > 0 && info.NumRewardPartitionsRemaining > 0
}

func rewardDistributionComplete(snapshot *ReplayHeadSnapshot, spoolSlot uint64) bool {
	if snapshot == nil || snapshot.Aux.PartitionedRewardsInfo == nil {
		return false
	}
	info := snapshot.Aux.PartitionedRewardsInfo
	return info.SpoolSlot == spoolSlot &&
		info.NumRewardPartitions > 0 &&
		info.NumRewardPartitionsRemaining == 0
}

func resumeContextFromSnapshot(snapshot *ReplayHeadSnapshot) (*state.ResumeContext, error) {
	if snapshot == nil || len(snapshot.FinalBankhash) != 32 || snapshot.AcctsLtHash == nil {
		return nil, fmt.Errorf("incomplete replay head snapshot")
	}
	if !snapshot.HasAlpenglowIdentity || snapshot.AlpenglowBlockID == (solana.Hash{}) || snapshot.AlpenglowChainedRoot == (solana.Hash{}) {
		return nil, fmt.Errorf("replay head snapshot has no complete Alpenglow identity")
	}
	count := snapshot.TransactionCount
	resume := &state.ResumeContext{
		Slot:                    snapshot.Slot,
		Bankhash:                base58.Encode(snapshot.FinalBankhash),
		AlpenglowBlockID:        base58.Encode(snapshot.AlpenglowBlockID[:]),
		AlpenglowChainedRoot:    base58.Encode(snapshot.AlpenglowChainedRoot[:]),
		BlockHeight:             snapshot.BlockHeight,
		Epoch:                   snapshot.Epoch,
		AcctsLtHash:             base64.StdEncoding.EncodeToString(snapshot.AcctsLtHash.Hash()),
		NumSignatures:           snapshot.NumSignatures,
		RecentBlockhashes:       EncodeRecentBlockhashes(snapshot.RecentBlockhashes),
		EvictedBlockhash:        base58.Encode(snapshot.LatestEvictedBlockhash[:]),
		Blockhash:               base58.Encode(snapshot.Blockhash[:]),
		SlotHashes:              EncodeSlotHashes(snapshot.SlotHashes),
		Capitalization:          snapshot.Capitalization,
		SlotsPerYear:            snapshot.SlotsPerYear,
		InflationInitial:        snapshot.InflationInitial,
		InflationTerminal:       snapshot.InflationTerminal,
		InflationTaper:          snapshot.InflationTaper,
		InflationFoundation:     snapshot.InflationFoundation,
		InflationFoundationTerm: snapshot.InflationFoundationTerm,
		TransactionCount:        &count,
	}
	if snapshot.FeeRateGovernor != nil {
		resume.LamportsPerSignature = snapshot.FeeRateGovernor.LamportsPerSignature
		resume.PrevLamportsPerSig = snapshot.FeeRateGovernor.PrevLamportsPerSignature
	}
	if snapshot.Clock != nil {
		resume.Clock = base64.StdEncoding.EncodeToString(snapshot.Clock.MustMarshal())
	}
	if snapshot.EpochResume != nil {
		resume.ComputedEpochStakes = make(map[uint64]string, len(snapshot.EpochResume.ComputedEpochStakes))
		for epoch, encoded := range snapshot.EpochResume.ComputedEpochStakes {
			resume.ComputedEpochStakes[epoch] = encoded
		}
		resume.EpochAuthorizedVoters = make(map[string][]string, len(snapshot.EpochResume.AuthorizedVoters))
		for voteAcct, authorized := range snapshot.EpochResume.AuthorizedVoters {
			resume.EpochAuthorizedVoters[voteAcct] = append([]string(nil), authorized...)
		}
	}
	return resume, nil
}

func (sr *SpeculativeReplay) pollFoldLocked(pt *persistedTracker) int {
	if !sr.foldInFlight {
		return 0
	}
	select {
	case result := <-sr.foldResults:
		sr.foldInFlight = false
		return sr.applyFoldResultLocked(pt, result)
	default:
		return 0
	}
}

func (sr *SpeculativeReplay) finishFoldLocked(pt *persistedTracker) int {
	if !sr.foldInFlight {
		return 0
	}
	result := <-sr.foldResults
	sr.foldInFlight = false
	return sr.applyFoldResultLocked(pt, result)
}

func (sr *SpeculativeReplay) applyFoldResultLocked(pt *persistedTracker, result speculativeFoldResult) int {
	if result.err != nil {
		sr.foldErr = result.err
		return 0
	}
	job := result.job
	sr.committedSlot = job.through
	sr.headSnapshot = job.snapshot
	sr.store.SetFinalizedSlot(job.through)
	sr.store.PruneLayersThrough(job.through)
	for slot := range sr.pending {
		if slot <= job.through {
			delete(sr.pending, slot)
		}
	}
	for slot := range sr.snapshots {
		if slot <= job.through {
			delete(sr.snapshots, slot)
		}
	}
	for epoch := range sr.epochResumeByEpoch {
		if epoch+1 < job.snapshot.Epoch {
			delete(sr.epochResumeByEpoch, epoch)
		}
	}
	if pt != nil {
		pt.Set(job.through, job.bankhash)
	}
	if sr.durableRootSink != nil {
		sr.durableRootSink(job.through)
	}
	if sr.mithrilState != nil {
		sr.mithrilState.LastRootedSlot = job.through
		sr.mithrilState.LastRootedBankhash = base58.Encode(job.bankhash)
		sr.mithrilState.LastRootedContext = job.resume
		if len(job.resume.ComputedEpochStakes) != 0 {
			sr.mithrilState.ComputedEpochStakes = make(map[uint64]string, len(job.resume.ComputedEpochStakes))
			for epoch, encoded := range job.resume.ComputedEpochStakes {
				sr.mithrilState.ComputedEpochStakes[epoch] = encoded
			}
		}
		if len(job.resume.EpochAuthorizedVoters) != 0 {
			sr.mithrilState.ManifestEpochAuthorizedVoters = make(map[string][]string, len(job.resume.EpochAuthorizedVoters))
			for voteAcct, authorized := range job.resume.EpochAuthorizedVoters {
				sr.mithrilState.ManifestEpochAuthorizedVoters[voteAcct] = append([]string(nil), authorized...)
			}
		}
	}
	if info := job.snapshot.Aux.PartitionedRewardsInfo; info != nil &&
		info.NumRewardPartitionsRemaining == 0 &&
		info.NumRewardPartitions > 0 &&
		info.SpoolDir != "" {
		if _, cleaned := sr.cleanedRewardSpools[info.SpoolSlot]; !cleaned {
			rewards.CleanupPartitionedSpoolFiles(info.SpoolDir, info.SpoolSlot, info.NumRewardPartitions)
			sr.cleanedRewardSpools[info.SpoolSlot] = struct{}{}
		}
	}
	mlog.Log.Infof("rooted-durable: promoted %d executed slots through finalized slot %d", len(job.deltas), job.through)
	return len(job.deltas)
}

// FlushFinalized settles any in-flight batch and force-folds the remaining
// finalized partial chunk. It never promotes merely executed/notarized state.
func (sr *SpeculativeReplay) FlushFinalized(
	pt *persistedTracker,
	decisionSource func(anchorSlot uint64) (alpenglow.ChainDecision, bool),
) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if !sr.enabled || sr.committer == nil {
		return nil
	}
	sr.finishFoldLocked(pt)
	if sr.foldErr != nil {
		return sr.foldErr
	}
	if err := sr.advanceFinalityLocked(decisionSource); err != nil {
		sr.foldErr = err
		return err
	}
	for {
		job, err := sr.buildFoldJobLocked(true)
		if err != nil {
			sr.foldErr = err
			return err
		}
		if job == nil {
			return nil
		}
		sr.foldJobs <- job
		sr.foldInFlight = true
		sr.finishFoldLocked(pt)
		if sr.foldErr != nil {
			return sr.foldErr
		}
	}
}

// Close stops the fold worker. The normal replay path calls FlushFinalized
// first; the drain here is the crash-recovery-compatible fallback.
func (sr *SpeculativeReplay) Close() {
	sr.mu.Lock()
	if sr.foldClosed || sr.committer == nil {
		sr.mu.Unlock()
		return
	}
	sr.finishFoldLocked(nil)
	close(sr.foldJobs)
	sr.foldClosed = true
	done := sr.foldDone
	sr.mu.Unlock()
	<-done
}
