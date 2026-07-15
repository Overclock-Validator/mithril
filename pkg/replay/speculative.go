package replay

import (
	"encoding/base64"
	"errors"
	"fmt"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/forkchoice"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// DeferredBlockCommit holds account and bankhash writes deferred until Alpenglow confirms.
type DeferredBlockCommit struct {
	SlotCtx                 *sealevel.SlotCtx
	ModifiedAccts           []*accounts.Account
	BlockSlot               uint64
	BlockHeight             uint64
	Bankhash                []byte
	HasAlpenglowBlockID     bool
	AlpenglowBlockID        solana.Hash
	HasAlpenglowChainedRoot bool
	AlpenglowChainedRoot    solana.Hash
}

// ReplayAuxState contains replay-loop state that is not represented by account
// values alone. It is tiny and copied into each branch-head snapshot so reward
// distribution can resume at the exact partition after a RAM-only unwind.
type ReplayAuxState struct {
	PartitionedEpochRewardsEnabled bool
	PartitionedRewardsInfo         *rewards.PartitionedRewardDistributionInfo
}

// EpochResumeMetadata is shared by every speculative snapshot in one epoch.
// It is immutable once captured, avoiding per-slot copies while making an
// epoch-crossing fold independently restartable from its manifest.
type EpochResumeMetadata struct {
	ComputedEpochStakes map[uint64]string
	AuthorizedVoters    map[string][]string
}

func (state ReplayAuxState) clone() ReplayAuxState {
	cloned := state
	if state.PartitionedRewardsInfo != nil {
		info := *state.PartitionedRewardsInfo
		cloned.PartitionedRewardsInfo = &info
	}
	return cloned
}

// ReplayHeadSnapshot captures replay-visible chain head state at a persisted slot.
type ReplayHeadSnapshot struct {
	Slot                    uint64
	ParentSlot              uint64
	BlockHeight             uint64
	Epoch                   uint64
	FinalBankhash           []byte
	Blockhash               [32]byte
	LatestEvictedBlockhash  [32]byte
	NumSignatures           uint64
	AcctsLtHash             *lthash.LtHash
	FeeRateGovernor         *sealevel.FeeRateGovernor
	Capitalization          uint64
	SlotsPerYear            float64
	InflationInitial        float64
	InflationTerminal       float64
	InflationTaper          float64
	InflationFoundation     float64
	InflationFoundationTerm float64
	TransactionCount        uint64
	Clock                   *sealevel.SysvarClock
	SlotHashes              *sealevel.SysvarSlotHashes
	RecentBlockhashes       *sealevel.SysvarRecentBlockhashes
	Features                *features.Features
	HasAlpenglowIdentity    bool
	AlpenglowBlockID        solana.Hash
	AlpenglowChainedRoot    solana.Hash
	// EpochAuthorizedVoters is immutable after publication. Keeping the
	// epoch-scoped pointer avoids copying the full map for every speculative slot.
	EpochAuthorizedVoters *epochstakes.EpochAuthorizedVotersCache
	EpochResume           *EpochResumeMetadata
	Aux                   ReplayAuxState
}

// SpeculativeReplay defers AccountsDB persistence for turbine blocks until Alpenglow finalizes them.
type SpeculativeReplay struct {
	mu                  sync.Mutex
	enabled             bool
	committedSlot       uint64
	finalityCursor      uint64
	headSnapshot        *ReplayHeadSnapshot
	pending             map[uint64]*DeferredBlockCommit
	snapshots           map[uint64]*ReplayHeadSnapshot
	store               *SpeculativeStore
	accountsDb          *accountsdb.AccountsDb
	committer           batchCommitter
	mithrilState        *state.MithrilState
	stakeIndexDir       string
	foldBatchSlots      int
	foldJobs            chan *speculativeFoldJob
	foldResults         chan speculativeFoldResult
	foldDone            chan struct{}
	foldInFlight        bool
	foldClosed          bool
	foldErr             error
	rootSink            func(alpenglow.BlockID)
	cleanedRewardSpools map[uint64]struct{}
	epochResumeByEpoch  map[uint64]*EpochResumeMetadata
}

func NewSpeculativeReplay() *SpeculativeReplay {
	return &SpeculativeReplay{
		pending:             make(map[uint64]*DeferredBlockCommit),
		snapshots:           make(map[uint64]*ReplayHeadSnapshot),
		store:               newSpeculativeStore(),
		cleanedRewardSpools: make(map[uint64]struct{}),
		epochResumeByEpoch:  make(map[uint64]*EpochResumeMetadata),
	}
}

func captureEpochResumeMetadata() (*EpochResumeMetadata, error) {
	metadata := &EpochResumeMetadata{
		ComputedEpochStakes: make(map[uint64]string),
		AuthorizedVoters:    make(map[string][]string),
	}
	for _, epoch := range global.GetAllCachedEpochs() {
		encoded, err := global.SerializeEpochStakes(epoch)
		if err != nil {
			return nil, fmt.Errorf("serialize epoch %d stakes: %w", epoch, err)
		}
		if len(encoded) != 0 {
			metadata.ComputedEpochStakes[epoch] = string(encoded)
		}
	}
	if voters := global.EpochAuthorizedVoters(); voters != nil {
		for voteAcct, authorized := range voters.Entries() {
			values := make([]string, len(authorized))
			for i, voter := range authorized {
				values[i] = base58.Encode(voter[:])
			}
			metadata.AuthorizedVoters[base58.Encode(voteAcct[:])] = values
		}
	}
	return metadata, nil
}

func (sr *SpeculativeReplay) attachEpochResumeLocked(snapshot *ReplayHeadSnapshot) error {
	if snapshot == nil {
		return fmt.Errorf("nil replay snapshot")
	}
	metadata := sr.epochResumeByEpoch[snapshot.Epoch]
	if metadata == nil {
		var err error
		metadata, err = captureEpochResumeMetadata()
		if err != nil {
			return err
		}
		sr.epochResumeByEpoch[snapshot.Epoch] = metadata
	}
	snapshot.EpochResume = metadata
	return nil
}

func (sr *SpeculativeReplay) Enable() {
	sr.mu.Lock()
	sr.enabled = true
	sr.mu.Unlock()
}

func (sr *SpeculativeReplay) Enabled() bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.enabled
}

func (sr *SpeculativeReplay) SetRootSink(sink func(alpenglow.BlockID)) {
	sr.mu.Lock()
	sr.rootSink = sink
	sr.mu.Unlock()
}

func CaptureHeadSnapshot(slotCtx *sealevel.SlotCtx, replayCtx *ReplayCtx, blockHeight uint64) *ReplayHeadSnapshot {
	if slotCtx == nil || replayCtx == nil {
		return nil
	}
	snapshot := &ReplayHeadSnapshot{
		Slot:                    slotCtx.Slot,
		ParentSlot:              slotCtx.ParentSlot,
		BlockHeight:             blockHeight,
		Epoch:                   slotCtx.Epoch,
		FinalBankhash:           append([]byte(nil), slotCtx.FinalBankhash...),
		Blockhash:               slotCtx.Blockhash,
		LatestEvictedBlockhash:  slotCtx.LatestEvictedBlockhash,
		NumSignatures:           slotCtx.NumSignatures,
		Capitalization:          replayCtx.Capitalization,
		SlotsPerYear:            replayCtx.SlotsPerYear,
		InflationInitial:        replayCtx.Inflation.Initial,
		InflationTerminal:       replayCtx.Inflation.Terminal,
		InflationTaper:          replayCtx.Inflation.Taper,
		InflationFoundation:     replayCtx.Inflation.FoundationVal,
		InflationFoundationTerm: replayCtx.Inflation.FoundationTerm,
		TransactionCount:        global.TransactionCount(),
		EpochAuthorizedVoters:   global.EpochAuthorizedVoters(),
	}
	if slotCtx.AcctsLtHash != nil {
		snapshot.AcctsLtHash = slotCtx.AcctsLtHash.Clone()
	}
	if slotCtx.FeeRateGovernor != nil {
		gov := *slotCtx.FeeRateGovernor
		snapshot.FeeRateGovernor = &gov
	}
	if slotCtx.Features != nil {
		snapshot.Features = slotCtx.Features.Clone()
	}
	if sealevel.SysvarCache.Clock.Sysvar != nil {
		clock := *sealevel.SysvarCache.Clock.Sysvar
		snapshot.Clock = &clock
	}
	if sealevel.SysvarCache.SlotHashes.Sysvar != nil {
		slotHashes := append(sealevel.SysvarSlotHashes(nil), (*sealevel.SysvarCache.SlotHashes.Sysvar)...)
		snapshot.SlotHashes = &slotHashes
	}
	if sealevel.SysvarCache.RecentBlockHashes.Sysvar != nil {
		recent := append(sealevel.SysvarRecentBlockhashes(nil), (*sealevel.SysvarCache.RecentBlockHashes.Sysvar)...)
		snapshot.RecentBlockhashes = &recent
	}
	return snapshot
}

func (sr *SpeculativeReplay) FinalizedSlot() uint64 {
	return sr.store.FinalizedSlot()
}

func (sr *SpeculativeReplay) LayerCount() int {
	return sr.store.LayerCount()
}

func (sr *SpeculativeReplay) UseStoreForParent(parentSlot uint64) bool {
	if !sr.Enabled() {
		return false
	}
	return sr.store.UseStoreForParent(parentSlot)
}

func (sr *SpeculativeReplay) Resolve(endSlot uint64, pk solana.PublicKey, db *accountsdb.AccountsDb) (*accounts.Account, error) {
	return sr.store.Resolve(endSlot, pk, db)
}

func (sr *SpeculativeReplay) ParentNotPersisted(parentSlot uint64) bool {
	return sr.UseStoreForParent(parentSlot)
}

func (sr *SpeculativeReplay) BankhashAt(slot uint64) ([]byte, bool) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if pending := sr.pending[slot]; pending != nil && len(pending.Bankhash) == 32 {
		return append([]byte(nil), pending.Bankhash...), true
	}
	if sr.headSnapshot != nil && sr.headSnapshot.Slot == slot && len(sr.headSnapshot.FinalBankhash) == 32 {
		return append([]byte(nil), sr.headSnapshot.FinalBankhash...), true
	}
	return nil, false
}

func (sr *SpeculativeReplay) AlpenglowIdentityAt(slot uint64) (solana.Hash, solana.Hash, bool) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if pending := sr.pending[slot]; pending != nil && pending.HasAlpenglowBlockID && pending.HasAlpenglowChainedRoot {
		return pending.AlpenglowBlockID, pending.AlpenglowChainedRoot, true
	}
	if sr.headSnapshot != nil && sr.headSnapshot.Slot == slot && sr.headSnapshot.HasAlpenglowIdentity {
		return sr.headSnapshot.AlpenglowBlockID, sr.headSnapshot.AlpenglowChainedRoot, true
	}
	return solana.Hash{}, solana.Hash{}, false
}

func (sr *SpeculativeReplay) SeedFromManifest(mithrilState *state.MithrilState, replayCtx *ReplayCtx) {
	if mithrilState == nil || replayCtx == nil {
		return
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if !sr.enabled || mithrilState.ManifestParentSlot == 0 {
		return
	}

	snapshot := &ReplayHeadSnapshot{
		Slot:                    mithrilState.ManifestParentSlot,
		Epoch:                   mithrilState.SnapshotEpoch,
		BlockHeight:             mithrilState.ManifestBlockHeight,
		NumSignatures:           mithrilState.ManifestSignatureCount,
		Capitalization:          replayCtx.Capitalization,
		SlotsPerYear:            replayCtx.SlotsPerYear,
		InflationInitial:        replayCtx.Inflation.Initial,
		InflationTerminal:       replayCtx.Inflation.Terminal,
		InflationTaper:          replayCtx.Inflation.Taper,
		InflationFoundation:     replayCtx.Inflation.FoundationVal,
		InflationFoundationTerm: replayCtx.Inflation.FoundationTerm,
		TransactionCount:        mithrilState.ManifestTransactionCount,
		EpochAuthorizedVoters:   global.EpochAuthorizedVoters(),
	}
	if replayCtx.CurrentFeatures != nil {
		snapshot.Features = replayCtx.CurrentFeatures.Clone()
		snapshot.Aux.PartitionedEpochRewardsEnabled =
			replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochReward) ||
				replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochRewardsSuperfeature)
	}
	if mithrilState.ManifestParentBankhash != "" {
		if bankhash, err := base58.DecodeFromString(mithrilState.ManifestParentBankhash); err == nil {
			snapshot.FinalBankhash = append([]byte(nil), bankhash[:]...)
		}
	}
	if mithrilState.ManifestAcctsLtHash != "" {
		if ltHashBytes, err := base64.StdEncoding.DecodeString(mithrilState.ManifestAcctsLtHash); err == nil && len(ltHashBytes) == lthash.HashByteLen {
			snapshot.AcctsLtHash = new(lthash.LtHash).InitWithHash(ltHashBytes)
		}
	}
	if mithrilState.ManifestLamportsPerSignature != 0 {
		snapshot.FeeRateGovernor = &sealevel.FeeRateGovernor{
			LamportsPerSignature:     mithrilState.ManifestLamportsPerSignature,
			PrevLamportsPerSignature: mithrilState.ManifestLamportsPerSignature,
		}
	}
	if sealevel.SysvarCache.Clock.Sysvar != nil {
		clock := *sealevel.SysvarCache.Clock.Sysvar
		snapshot.Clock = &clock
	}
	if sealevel.SysvarCache.SlotHashes.Sysvar != nil {
		slotHashes := append(sealevel.SysvarSlotHashes(nil), (*sealevel.SysvarCache.SlotHashes.Sysvar)...)
		snapshot.SlotHashes = &slotHashes
	}
	if sealevel.SysvarCache.RecentBlockHashes.Sysvar != nil {
		recent := append(sealevel.SysvarRecentBlockhashes(nil), (*sealevel.SysvarCache.RecentBlockHashes.Sysvar)...)
		snapshot.RecentBlockhashes = &recent
	}
	if mithrilState.ManifestLastBlockhash != "" {
		if hash, err := base58.DecodeFromString(mithrilState.ManifestLastBlockhash); err == nil {
			snapshot.Blockhash = [32]byte(hash)
		}
	}

	sr.committedSlot = mithrilState.ManifestParentSlot
	sr.finalityCursor = mithrilState.ManifestParentSlot
	sr.headSnapshot = snapshot
	sr.store.SetFinalizedSlot(mithrilState.ManifestParentSlot)
	sr.store.Clear()
	sr.snapshots = make(map[uint64]*ReplayHeadSnapshot)
}

func (sr *SpeculativeReplay) SeedFromResume(resume *ResumeState, replayCtx *ReplayCtx) {
	if resume == nil || replayCtx == nil {
		return
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if !sr.enabled {
		return
	}

	snapshot := &ReplayHeadSnapshot{
		Slot:                    resume.ParentSlot,
		Epoch:                   resume.ParentEpoch,
		BlockHeight:             resume.ParentBlockHeight,
		FinalBankhash:           append([]byte(nil), resume.ParentBankhash...),
		Blockhash:               resume.LastBlockhash,
		LatestEvictedBlockhash:  resume.EvictedBlockhash,
		NumSignatures:           resume.NumSignatures,
		Capitalization:          replayCtx.Capitalization,
		SlotsPerYear:            replayCtx.SlotsPerYear,
		InflationInitial:        replayCtx.Inflation.Initial,
		InflationTerminal:       replayCtx.Inflation.Terminal,
		InflationTaper:          replayCtx.Inflation.Taper,
		InflationFoundation:     replayCtx.Inflation.FoundationVal,
		InflationFoundationTerm: replayCtx.Inflation.FoundationTerm,
		HasAlpenglowIdentity:    resume.HasAlpenglowIdentity,
		AlpenglowBlockID:        resume.AlpenglowBlockID,
		AlpenglowChainedRoot:    resume.AlpenglowChainedRoot,
		EpochAuthorizedVoters:   global.EpochAuthorizedVoters(),
	}
	if replayCtx.CurrentFeatures != nil {
		snapshot.Features = replayCtx.CurrentFeatures.Clone()
		snapshot.Aux.PartitionedEpochRewardsEnabled =
			replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochReward) ||
				replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochRewardsSuperfeature)
	}
	if resume.TransactionCount != nil {
		snapshot.TransactionCount = *resume.TransactionCount
	} else {
		snapshot.TransactionCount = global.TransactionCount()
	}
	if resume.AcctsLtHash != nil {
		snapshot.AcctsLtHash = resume.AcctsLtHash.Clone()
	}
	if resume.LamportsPerSignature != 0 || resume.PrevLamportsPerSignature != 0 {
		snapshot.FeeRateGovernor = &sealevel.FeeRateGovernor{
			LamportsPerSignature:     resume.LamportsPerSignature,
			PrevLamportsPerSignature: resume.PrevLamportsPerSignature,
		}
	}
	if resume.RecentBlockhashes != nil {
		recent := append(sealevel.SysvarRecentBlockhashes(nil), (*resume.RecentBlockhashes)...)
		snapshot.RecentBlockhashes = &recent
	}
	if resume.SlotHashes != nil {
		slotHashes := append(sealevel.SysvarSlotHashes(nil), (*resume.SlotHashes)...)
		snapshot.SlotHashes = &slotHashes
	}
	if sealevel.SysvarCache.Clock.Sysvar != nil {
		clock := *sealevel.SysvarCache.Clock.Sysvar
		snapshot.Clock = &clock
	}

	sr.committedSlot = resume.ParentSlot
	sr.finalityCursor = resume.ParentSlot
	sr.headSnapshot = snapshot
	sr.store.SetFinalizedSlot(resume.ParentSlot)
	sr.store.Clear()
	sr.snapshots = make(map[uint64]*ReplayHeadSnapshot)
}

func (sr *SpeculativeReplay) UpdateCommittedHead(slotCtx *sealevel.SlotCtx, replayCtx *ReplayCtx, blockHeight uint64) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if !sr.enabled || slotCtx == nil {
		return
	}
	sr.committedSlot = slotCtx.Slot
	sr.finalityCursor = slotCtx.Slot
	sr.headSnapshot = CaptureHeadSnapshot(slotCtx, replayCtx, blockHeight)
	sr.store.SetFinalizedSlot(slotCtx.Slot)
	sr.store.PruneLayersThrough(slotCtx.Slot)
	for slot := range sr.pending {
		if slot <= sr.committedSlot {
			delete(sr.pending, slot)
		}
	}
	for slot := range sr.snapshots {
		if slot <= sr.committedSlot {
			delete(sr.snapshots, slot)
		}
	}
}

func (sr *SpeculativeReplay) TrackPending(deferred *DeferredBlockCommit, replayCtx *ReplayCtx, aux ...ReplayAuxState) error {
	if deferred == nil {
		return nil
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	snapshot := capturePendingSnapshot(deferred, replayCtx, aux...)
	if err := sr.attachEpochResumeLocked(snapshot); err != nil {
		return fmt.Errorf("slot %d epoch resume metadata: %w", deferred.BlockSlot, err)
	}
	if err := sr.stagePendingLocked(deferred); err != nil {
		return err
	}
	sr.snapshots[deferred.BlockSlot] = snapshot
	return nil
}

func (sr *SpeculativeReplay) StagePending(deferred *DeferredBlockCommit) error {
	if deferred == nil {
		return nil
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.stagePendingLocked(deferred)
}

func (sr *SpeculativeReplay) stagePendingLocked(deferred *DeferredBlockCommit) error {
	if deferred.HasAlpenglowBlockID != deferred.HasAlpenglowChainedRoot {
		return fmt.Errorf("slot %d has incomplete Alpenglow identity", deferred.BlockSlot)
	}
	if deferred.HasAlpenglowBlockID && (deferred.AlpenglowBlockID == (solana.Hash{}) || deferred.AlpenglowChainedRoot == (solana.Hash{})) {
		return fmt.Errorf("slot %d has zero Alpenglow identity", deferred.BlockSlot)
	}
	if existing := sr.pending[deferred.BlockSlot]; existing != nil {
		if existing.AlpenglowBlockID != deferred.AlpenglowBlockID ||
			existing.AlpenglowChainedRoot != deferred.AlpenglowChainedRoot ||
			existing.SlotCtx.ParentSlot != deferred.SlotCtx.ParentSlot {
			return fmt.Errorf("slot %d already staged with a different branch identity", deferred.BlockSlot)
		}
		return nil
	}
	if err := sr.store.RecordLayer(deferred.BlockSlot, deferred.SlotCtx.ParentSlot, deferred.SlotCtx, deferred.ModifiedAccts); err != nil {
		return err
	}
	if sr.snapshots == nil {
		sr.snapshots = make(map[uint64]*ReplayHeadSnapshot)
	}
	sr.pending[deferred.BlockSlot] = deferred
	return nil
}

func (sr *SpeculativeReplay) FinalizePendingSnapshot(slot uint64, replayCtx *ReplayCtx, aux ...ReplayAuxState) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	pending := sr.pending[slot]
	if pending == nil {
		return fmt.Errorf("slot %d is not staged in speculative replay", slot)
	}
	snapshot := capturePendingSnapshot(pending, replayCtx, aux...)
	if snapshot == nil {
		return fmt.Errorf("slot %d produced no replay snapshot", slot)
	}
	if err := sr.attachEpochResumeLocked(snapshot); err != nil {
		return fmt.Errorf("slot %d epoch resume metadata: %w", slot, err)
	}
	sr.snapshots[slot] = snapshot
	return nil
}

func capturePendingSnapshot(deferred *DeferredBlockCommit, replayCtx *ReplayCtx, aux ...ReplayAuxState) *ReplayHeadSnapshot {
	if deferred == nil {
		return nil
	}
	snapshot := CaptureHeadSnapshot(deferred.SlotCtx, replayCtx, deferred.BlockHeight)
	if snapshot != nil && deferred.HasAlpenglowBlockID && deferred.HasAlpenglowChainedRoot {
		snapshot.HasAlpenglowIdentity = true
		snapshot.AlpenglowBlockID = deferred.AlpenglowBlockID
		snapshot.AlpenglowChainedRoot = deferred.AlpenglowChainedRoot
	}
	if snapshot != nil && len(aux) != 0 {
		snapshot.Aux = aux[0].clone()
	}
	return snapshot
}

func alpenglowCertConfirmsPersist(certType alpenglow.CertificateType) bool {
	return certType.IsFinalization() || certType == alpenglow.CertificateGenesis
}

// TryFlushPending commits consecutive finalized pending slots starting at
// committedSlot+1. It is safe to call before each block is processed, for
// example when Votor certificates arrive while waiting for the next block.
func (sr *SpeculativeReplay) TryFlushPending(
	acctsDb *accountsdb.AccountsDb,
	pt *persistedTracker,
	replayCtx *ReplayCtx,
	decisionSource func(anchorSlot uint64) (alpenglow.ChainDecision, bool),
) int {
	if decisionSource == nil {
		return 0
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()
	if !sr.enabled {
		return 0
	}
	return sr.pumpFoldLocked(pt, decisionSource, false)
}

func (sr *SpeculativeReplay) TryCommitPending(
	acctsDb *accountsdb.AccountsDb,
	pt *persistedTracker,
	block *b.Block,
	blockHeight uint64,
	replayCtx *ReplayCtx,
	decisionSource func(anchorSlot uint64) (alpenglow.ChainDecision, bool),
) bool {
	if block == nil || decisionSource == nil {
		return false
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()
	if !sr.enabled {
		return false
	}
	if _, ok := sr.pending[block.Slot]; !ok {
		return false
	}

	before := sr.committedSlot
	sr.pumpFoldLocked(pt, decisionSource, false)
	return sr.committedSlot >= block.Slot && sr.committedSlot > before
}

type SpeculativeRollbackParams struct {
	AcctsDb        *accountsdb.AccountsDb
	PT             *persistedTracker
	ReplayCtx      *ReplayCtx
	LastSlotCtx    **sealevel.SlotCtx
	BlockStream    *blockstream.BlockSource
	ForkChoice     *forkchoice.ForkChoiceService
	RPCServer      SlotCtxSetter
	RestoreSysvars func(*ReplayHeadSnapshot) error
	RestoreAux     func(*ReplayHeadSnapshot) error
}

// HandleParentMismatch rolls back speculative execution when a waiting block's parent
// does not connect to the last emitted slot.
func (sr *SpeculativeReplay) HandleParentMismatch(
	waitingSlot, observedParent, expectedParent uint64,
	params SpeculativeRollbackParams,
) bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if !sr.enabled || observedParent >= expectedParent {
		return false
	}
	sr.finishFoldLocked(params.PT)
	if sr.foldErr != nil {
		mlog.Log.Errorf("speculative replay: cannot unwind while durable fold is unhealthy: %v", sr.foldErr)
		return false
	}
	if sr.committedSlot > observedParent {
		mlog.Log.Errorf("speculative replay: cannot rollback to slot %d; already persisted through %d",
			observedParent, sr.committedSlot)
		return false
	}

	anchor := observedParent
	if err := sr.rollbackToLocked(anchor, params); err != nil {
		mlog.Log.Errorf("speculative replay: rollback to slot %d failed: %v", anchor, err)
		return false
	}
	params.BlockStream.RollbackEmissionFrontier(anchor, waitingSlot-1)
	mlog.Log.Warnf("speculative replay: parent mismatch at slot %d (observed_parent=%d expected_parent=%d); rolled back to %d",
		waitingSlot, observedParent, expectedParent, anchor)
	return true
}

// HandleParentIdentityMismatch switches an executed speculative parent when a
// descendant proves that Turbine is extending a different block ID at the same
// slot. The active suffix is discarded in memory; AccountsDB is unchanged.
func (sr *SpeculativeReplay) HandleParentIdentityMismatch(
	waitingSlot, parentSlot uint64,
	observedParentID, executedParentID solana.Hash,
	params SpeculativeRollbackParams,
) bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if !sr.enabled || observedParentID == (solana.Hash{}) || observedParentID == executedParentID {
		return false
	}
	if params.BlockStream == nil || params.ReplayCtx == nil || params.LastSlotCtx == nil {
		return false
	}
	sr.finishFoldLocked(params.PT)
	if sr.foldErr != nil {
		mlog.Log.Errorf("speculative replay: cannot switch fork while durable fold is unhealthy: %v", sr.foldErr)
		return false
	}
	activeParent := sr.pending[parentSlot]
	if activeParent == nil || !activeParent.HasAlpenglowBlockID || activeParent.AlpenglowBlockID != executedParentID {
		mlog.Log.Errorf("ALPENGLOW SAFETY: cannot identify executed parent %s at slot %d for waiting slot %d",
			executedParentID, parentSlot, waitingSlot)
		return false
	}
	anchor := activeParent.SlotCtx.ParentSlot
	if sr.committedSlot > anchor {
		mlog.Log.Errorf("ALPENGLOW SAFETY: fork switch at slot %d would cross durable root %d (common parent %d)",
			parentSlot, sr.committedSlot, anchor)
		return false
	}
	if err := sr.rollbackToLocked(anchor, params); err != nil {
		mlog.Log.Errorf("speculative replay: fork switch rollback to slot %d failed: %v", anchor, err)
		return false
	}

	var anchorID solana.Hash
	if snapshot := sr.headSnapshot; snapshot != nil && snapshot.Slot == anchor && snapshot.HasAlpenglowIdentity {
		anchorID = snapshot.AlpenglowBlockID
	} else if snapshot := sr.snapshots[anchor]; snapshot != nil && snapshot.HasAlpenglowIdentity {
		anchorID = snapshot.AlpenglowBlockID
	}
	params.BlockStream.RewindAlpenglowFork(anchor, anchorID, alpenglow.BlockID{Slot: parentSlot, Hash: observedParentID})
	mlog.Log.Warnf("speculative replay: switched fork for parent slot %d (executed=%s selected=%s); dropped RAM tail to slot %d",
		parentSlot, executedParentID, observedParentID, anchor)
	return true
}

func (sr *SpeculativeReplay) rollbackToLocked(anchor uint64, params SpeculativeRollbackParams) error {
	global.DropPendingStakePubkeysFrom(anchor + 1)
	for slot := range sr.pending {
		if slot > anchor {
			delete(sr.pending, slot)
		}
	}
	for slot := range sr.snapshots {
		if slot > anchor {
			delete(sr.snapshots, slot)
		}
	}
	sr.store.PruneLayersAbove(anchor)
	if params.AcctsDb != nil {
		params.AcctsDb.ClearProgramCache()
	}
	snapshot := sr.snapshots[anchor]
	if anchor == sr.committedSlot {
		snapshot = sr.headSnapshot
	}
	if snapshot == nil || snapshot.Slot != anchor {
		return fmt.Errorf("missing head snapshot for anchor slot %d", anchor)
	}
	for epoch := range sr.epochResumeByEpoch {
		if epoch > snapshot.Epoch {
			delete(sr.epochResumeByEpoch, epoch)
		}
	}
	return restoreReplayHeadFromSnapshot(snapshot, params)
}

func restoreReplayHeadFromSnapshot(snapshot *ReplayHeadSnapshot, params SpeculativeRollbackParams) error {
	if snapshot == nil {
		return fmt.Errorf("nil snapshot")
	}
	if params.RestoreSysvars != nil {
		if err := params.RestoreSysvars(snapshot); err != nil {
			return fmt.Errorf("restore sysvar cache at slot %d: %w", snapshot.Slot, err)
		}
	} else if err := restoreSysvarCacheFromSnapshot(snapshot, params.AcctsDb, snapshot.Slot); err != nil {
		return err
	}

	slotCtx := slotCtxFromSnapshot(snapshot)
	slotCtx.AccountsDb = params.AcctsDb
	slotCtx.AccountLoader = func(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
		return ResolveActiveAccount(params.AcctsDb, slot, pubkey)
	}
	*params.LastSlotCtx = slotCtx
	params.ReplayCtx.Capitalization = snapshot.Capitalization
	params.ReplayCtx.SlotsPerYear = snapshot.SlotsPerYear
	params.ReplayCtx.Inflation = rewards.Inflation{
		Initial:        snapshot.InflationInitial,
		Terminal:       snapshot.InflationTerminal,
		Taper:          snapshot.InflationTaper,
		FoundationVal:  snapshot.InflationFoundation,
		FoundationTerm: snapshot.InflationFoundationTerm,
	}
	if snapshot.Features != nil {
		params.ReplayCtx.CurrentFeatures = snapshot.Features.Clone()
	}
	global.SetSlot(snapshot.Slot)
	global.SetEpoch(snapshot.Epoch)
	global.SetBlockHeight(snapshot.BlockHeight)
	global.SetLatestBlockHash(snapshot.Blockhash)
	global.SetTransactionCount(snapshot.TransactionCount)
	UpdateChainTipFromSlotCtx(slotCtx, params.ReplayCtx.CurrentFeatures)
	if snapshot.EpochAuthorizedVoters != nil {
		global.SetEpochAuthorizedVoters(snapshot.EpochAuthorizedVoters)
	}
	if params.RestoreAux != nil {
		if err := params.RestoreAux(snapshot); err != nil {
			return fmt.Errorf("restore replay auxiliary state at slot %d: %w", snapshot.Slot, err)
		}
	}
	if params.ForkChoice != nil {
		params.ForkChoice.ObserveExecutionAnchor(snapshot.Slot, solana.Hash(snapshot.Blockhash))
	}
	if params.RPCServer != nil {
		params.RPCServer.SetSlotCtx(slotCtx)
	}
	return nil
}

func slotCtxFromSnapshot(snapshot *ReplayHeadSnapshot) *sealevel.SlotCtx {
	slotCtx := &sealevel.SlotCtx{
		Slot:                   snapshot.Slot,
		ParentSlot:             snapshot.ParentSlot,
		FinalBankhash:          append([]byte(nil), snapshot.FinalBankhash...),
		Blockhash:              snapshot.Blockhash,
		LatestEvictedBlockhash: snapshot.LatestEvictedBlockhash,
		NumSignatures:          snapshot.NumSignatures,
		AcctMapsMu:             &sync.Mutex{},
		ModifiedAccts:          make(map[solana.PublicKey]bool),
		WritableAccts:          make(map[solana.PublicKey]bool),
	}
	if snapshot.AcctsLtHash != nil {
		slotCtx.AcctsLtHash = snapshot.AcctsLtHash.Clone()
	}
	if snapshot.FeeRateGovernor != nil {
		gov := *snapshot.FeeRateGovernor
		slotCtx.FeeRateGovernor = &gov
	}
	if snapshot.Features != nil {
		slotCtx.Features = snapshot.Features.Clone()
	}
	return slotCtx
}

func restoreSysvarCacheFromSnapshot(snapshot *ReplayHeadSnapshot, acctsDb *accountsdb.AccountsDb, slot uint64) error {
	if acctsDb == nil {
		return fmt.Errorf("restore sysvar cache at slot %d: nil accounts database", slot)
	}
	load := func(name string, key solana.PublicKey) (*accounts.Account, error) {
		acct, err := acctsDb.GetAccount(slot, key)
		if err != nil {
			return nil, fmt.Errorf("load %s sysvar at slot %d: %w", name, slot, err)
		}
		return acct.Clone(), nil
	}

	clockAcct, err := load("clock", sealevel.SysvarClockAddr)
	if err != nil {
		return err
	}
	var clock sealevel.SysvarClock
	if err := clock.UnmarshalWithDecoder(bin.NewBinDecoder(clockAcct.Data)); err != nil {
		return fmt.Errorf("decode clock sysvar at slot %d: %w", slot, err)
	}
	sealevel.SysvarCache.Clock.Sysvar = &clock
	sealevel.SysvarCache.Clock.Acct = clockAcct

	slotHashesAcct, err := load("slot hashes", sealevel.SysvarSlotHashesAddr)
	if err != nil {
		return err
	}
	var slotHashes sealevel.SysvarSlotHashes
	if err := slotHashes.UnmarshalWithDecoder(bin.NewBinDecoder(slotHashesAcct.Data)); err != nil {
		return fmt.Errorf("decode slot hashes sysvar at slot %d: %w", slot, err)
	}
	sealevel.SysvarCache.SlotHashes.Sysvar = &slotHashes
	sealevel.SysvarCache.SlotHashes.Acct = slotHashesAcct

	recentAcct, err := load("recent blockhashes", sealevel.SysvarRecentBlockHashesAddr)
	if err != nil {
		return err
	}
	var recent sealevel.SysvarRecentBlockhashes
	if err := recent.UnmarshalWithDecoder(bin.NewBinDecoder(recentAcct.Data)); err != nil {
		return fmt.Errorf("decode recent blockhashes sysvar at slot %d: %w", slot, err)
	}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &recent
	sealevel.SysvarCache.RecentBlockHashes.Acct = recentAcct

	slotHistoryAcct, err := load("slot history", sealevel.SysvarSlotHistoryAddr)
	if err != nil {
		return err
	}
	var slotHistory sealevel.SysvarSlotHistory
	if err := slotHistory.UnmarshalWithDecoder(bin.NewBinDecoder(slotHistoryAcct.Data)); err != nil {
		return fmt.Errorf("decode slot history sysvar at slot %d: %w", slot, err)
	}
	sealevel.SysvarCache.SlotHistory.Sysvar = &slotHistory
	sealevel.SysvarCache.SlotHistory.Acct = slotHistoryAcct

	stakeHistoryAcct, err := load("stake history", sealevel.SysvarStakeHistoryAddr)
	if err != nil {
		return err
	}
	var stakeHistory sealevel.SysvarStakeHistory
	if err := stakeHistory.UnmarshalWithDecoder(bin.NewBinDecoder(stakeHistoryAcct.Data)); err != nil {
		return fmt.Errorf("decode stake history sysvar at slot %d: %w", slot, err)
	}
	sealevel.SysvarCache.StakeHistory.Sysvar = &stakeHistory
	sealevel.SysvarCache.StakeHistory.Acct = stakeHistoryAcct

	restartAcct, err := load("last restart slot", sealevel.SysvarLastRestartSlotAddr)
	if err != nil {
		return err
	}
	var restart sealevel.SysvarLastRestartSlot
	if err := restart.UnmarshalWithDecoder(bin.NewBinDecoder(restartAcct.Data)); err != nil {
		return fmt.Errorf("decode last restart slot sysvar at slot %d: %w", slot, err)
	}
	sealevel.SysvarCache.LastRestartSlot.Sysvar = &restart
	sealevel.SysvarCache.LastRestartSlot.Acct = restartAcct

	epochRewardsAcct, err := load("epoch rewards", sealevel.SysvarEpochRewardsAddr)
	if err != nil {
		return err
	}
	var epochRewards sealevel.SysvarEpochRewards
	if err := epochRewards.UnmarshalWithDecoder(bin.NewBinDecoder(epochRewardsAcct.Data)); err != nil {
		return fmt.Errorf("decode epoch rewards sysvar at slot %d: %w", slot, err)
	}
	sealevel.SysvarCache.EpochRewards.Sysvar = &epochRewards
	sealevel.SysvarCache.EpochRewards.Acct = epochRewardsAcct

	rentAcct, err := load("rent", sealevel.SysvarRentAddr)
	if err != nil {
		return err
	}
	var rent sealevel.SysvarRent
	if err := rent.UnmarshalWithDecoder(bin.NewBinDecoder(rentAcct.Data)); err != nil {
		return fmt.Errorf("decode rent sysvar at slot %d: %w", slot, err)
	}
	sealevel.SysvarCache.Rent.Sysvar = &rent
	sealevel.SysvarCache.Rent.Acct = rentAcct

	feesAcct, err := load("fees", sealevel.SysvarFeesAddr)
	if errors.Is(err, accountsdb.ErrNoAccount) {
		sealevel.SysvarCache.Fees.Sysvar = nil
		sealevel.SysvarCache.Fees.Acct = nil
		return nil
	}
	if err != nil {
		return err
	}
	var fees sealevel.SysvarFees
	if err := fees.UnmarshalWithDecoder(bin.NewBinDecoder(feesAcct.Data)); err != nil {
		return fmt.Errorf("decode fees sysvar at slot %d: %w", slot, err)
	}
	sealevel.SysvarCache.Fees.Sysvar = &fees
	sealevel.SysvarCache.Fees.Acct = feesAcct
	return nil
}
