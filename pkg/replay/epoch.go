package replay

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"maps"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

type ReplayCtx struct {
	CurrentFeatures   *features.Features
	Capitalization    uint64
	Inflation         rewards.Inflation
	SlotsPerYear      float64
	EpochAcctsHash    []byte
	HasEpochAcctsHash bool
}

// requireDurableEpochBoundaryParent protects the AccountsDB-wide epoch scans
// from observing a stale durable view while newer rooted writes still live only
// in the replay tail. Equality is the common case; a later durable slot is also
// safe when resuming across skipped slots.
func requireDurableEpochBoundaryParent(boundarySlot, parentSlot, durableSlot uint64) error {
	if durableSlot >= parentSlot {
		return nil
	}
	return fmt.Errorf("epoch boundary at slot %d requires durable parent %d before AccountsDB-wide scans; durable state is only at slot %d",
		boundarySlot, parentSlot, durableSlot)
}

// epochTransitionTargetEpochs returns every epoch whose metadata must be ready
// when crossing a boundary. The epoch being entered and the epoch whose leader
// schedule is selected at that slot are usually different, but some epoch
// schedules make them the same.
func epochTransitionTargetEpochs(newEpoch, leaderScheduleEpoch uint64) []uint64 {
	epochs := []uint64{newEpoch}
	if leaderScheduleEpoch != newEpoch {
		epochs = append(epochs, leaderScheduleEpoch)
	}
	return epochs
}

func prepareEpochTransitionLeaderSchedules(newEpoch, leaderScheduleEpoch uint64, epochSchedule *sealevel.SysvarEpochSchedule, logsDir string) error {
	for _, targetEpoch := range epochTransitionTargetEpochs(newEpoch, leaderScheduleEpoch) {
		var err error
		if len(global.EpochStakesVoteAccts(targetEpoch)) > 0 {
			_, err = PrepareLeaderScheduleLocal(targetEpoch, epochSchedule, logsDir)
		} else {
			_, err = PrepareLeaderScheduleLocalFromVoteCache(targetEpoch, epochSchedule, logsDir)
		}
		if err != nil {
			return fmt.Errorf("prepare leader schedule for epoch %d at epoch transition: %w", targetEpoch, err)
		}
	}
	return nil
}

// newReplayCtx creates a new ReplayCtx, preferring values from resumeState if available.
// This ensures resume uses fresh values instead of potentially stale manifest data.
func newReplayCtx(mithrilState *state.MithrilState, resumeState *ResumeState) (*ReplayCtx, error) {
	epochCtx := new(ReplayCtx)

	// Priority 1: Resume state (has most recent values)
	if resumeState != nil && resumeState.Capitalization > 0 {
		epochCtx.Capitalization = resumeState.Capitalization
		epochCtx.SlotsPerYear = resumeState.SlotsPerYear
		epochCtx.Inflation = rewards.Inflation{
			Initial:        resumeState.InflationInitial,
			Terminal:       resumeState.InflationTerminal,
			Taper:          resumeState.InflationTaper,
			FoundationVal:  resumeState.InflationFoundation,
			FoundationTerm: resumeState.InflationFoundationTerm,
		}
	} else if mithrilState != nil && mithrilState.ManifestCapitalization > 0 {
		// Priority 2: State file manifest_* fields (fresh start)
		epochCtx.Capitalization = mithrilState.ManifestCapitalization
		epochCtx.SlotsPerYear = mithrilState.ManifestSlotsPerYear
		epochCtx.Inflation = rewards.Inflation{
			Initial:        mithrilState.ManifestInflationInitial,
			Terminal:       mithrilState.ManifestInflationTerminal,
			Taper:          mithrilState.ManifestInflationTaper,
			FoundationVal:  mithrilState.ManifestInflationFoundation,
			FoundationTerm: mithrilState.ManifestInflationFoundationTerm,
		}
	} else {
		return nil, fmt.Errorf("state file missing manifest_capitalization - delete AccountsDB and rebuild from snapshot")
	}

	// Epoch account hash from state file (required)
	if mithrilState != nil && mithrilState.ManifestEpochAcctsHash != "" {
		epochAcctsHash, err := base64.StdEncoding.DecodeString(mithrilState.ManifestEpochAcctsHash)
		if err != nil {
			return nil, fmt.Errorf("corrupted state file: failed to decode manifest_epoch_accts_hash: %w", err)
		}
		if len(epochAcctsHash) == 32 {
			epochCtx.HasEpochAcctsHash = true
			epochCtx.EpochAcctsHash = epochAcctsHash
		}
	}
	// Note: epoch account hash may be empty for snapshots before SIMD-0160

	return epochCtx, nil
}

// BoundaryStakeScanResult holds the completed epoch's stake history and rewards
// denominators, plus the newly entered epoch's effective stake distribution.
type BoundaryStakeScanResult struct {
	// For stake history update
	StakeHistoryEffective    uint64
	StakeHistoryActivating   uint64
	StakeHistoryDeactivating uint64
	// For epoch stakes + vote cache
	VoteAcctStakes  map[solana.PublicKey]uint64
	EffectiveStakes map[solana.PublicKey]uint64
	// RewardEpochEffectiveStakes is the per-vote-account effective stake in
	// targetEpoch. Alpenglow reward credits are divided by this denominator;
	// EffectiveStakes above is for the newly entered epoch and will be stored
	// under the future leader-schedule epoch.
	RewardEpochEffectiveStakes map[solana.PublicKey]uint64
	TotalEffectiveStake        uint64
}

// scanStakesForEpochBoundary first completes targetEpoch's stake history, then
// uses that history to calculate effective stakes in newEpoch. The future
// leader-schedule epoch is only the key under which that distribution is saved.
// Two streaming passes avoid retaining all stake accounts or delegations.
func scanStakesForEpochBoundary(acctsDb *accountsdb.AccountsDb, slot uint64, targetEpoch uint64, newEpoch uint64, stakeHistory *sealevel.SysvarStakeHistory, epochSchedule *sealevel.SysvarEpochSchedule, f *features.Features) *BoundaryStakeScanResult {
	newRateActivationEpoch := newWarmupCooldownRateEpoch(epochSchedule, f)

	// Stake history accumulators (atomic — high contention from worker pool)
	var shEffective atomic.Uint64
	var shActivating atomic.Uint64
	var shDeactivating atomic.Uint64

	// Epoch stakes accumulators (mutex-guarded maps)
	voteAcctStakes := make(map[solana.PublicKey]uint64)
	var voteAcctStakesMu sync.Mutex
	effectiveStakes := make(map[solana.PublicKey]uint64)
	var effectiveStakesMu sync.Mutex
	rewardEpochEffectiveStakes := make(map[solana.PublicKey]uint64)
	var rewardEpochEffectiveStakesMu sync.Mutex
	var totalEffectiveStake atomic.Uint64

	_, err := global.StreamStakeAccounts(acctsDb, slot,
		func(pk solana.PublicKey, delegation *sealevel.Delegation, creditsObs uint64) {
			// --- Stake history accumulation ---
			if delegation.StakeLamports > 0 {
				entry := delegation.StakeActivatingAndDeactivating(targetEpoch, stakeHistory, newRateActivationEpoch)
				shEffective.Add(entry.Effective)
				shActivating.Add(entry.Activating)
				shDeactivating.Add(entry.Deactivating)
				if entry.Effective > 0 {
					rewardEpochEffectiveStakesMu.Lock()
					rewardEpochEffectiveStakes[delegation.VoterPubkey] += entry.Effective
					rewardEpochEffectiveStakesMu.Unlock()
				}
			}

			// Raw delegations also seed the vote-cache refresh.
			voteAcctStakesMu.Lock()
			voteAcctStakes[delegation.VoterPubkey] += delegation.StakeLamports
			voteAcctStakesMu.Unlock()
		})
	if err != nil {
		panic(fmt.Sprintf("error scanning stake history at epoch boundary: %s", err))
	}

	// Match Agave's calculate_activated_stake: append the completed epoch
	// before refreshing the new epoch's vote-account weights. Missing history
	// can otherwise treat newly activating stake as fully effective and newly
	// deactivating stake as fully cooled down. Leave the parent's history intact.
	newStakeHistory := append(sealevel.SysvarStakeHistory(nil), (*stakeHistory)...)
	newStakeHistory.Update(targetEpoch, sealevel.StakeHistoryEntry{
		Effective:    shEffective.Load(),
		Activating:   shActivating.Load(),
		Deactivating: shDeactivating.Load(),
	})

	_, err = global.StreamStakeAccounts(acctsDb, slot,
		func(pk solana.PublicKey, delegation *sealevel.Delegation, creditsObs uint64) {
			effectiveStake := delegation.Stake(newEpoch, &newStakeHistory, newRateActivationEpoch)
			if effectiveStake > 0 {
				effectiveStakesMu.Lock()
				effectiveStakes[delegation.VoterPubkey] += effectiveStake
				effectiveStakesMu.Unlock()
				totalEffectiveStake.Add(effectiveStake)
			}
		})
	if err != nil {
		panic(fmt.Sprintf("error scanning stake accounts at epoch boundary: %s", err))
	}

	return &BoundaryStakeScanResult{
		StakeHistoryEffective:      shEffective.Load(),
		StakeHistoryActivating:     shActivating.Load(),
		StakeHistoryDeactivating:   shDeactivating.Load(),
		VoteAcctStakes:             voteAcctStakes,
		EffectiveStakes:            effectiveStakes,
		RewardEpochEffectiveStakes: rewardEpochEffectiveStakes,
		TotalEffectiveStake:        totalEffectiveStake.Load(),
	}
}

// updateStakeHistorySysvar applies pre-computed stake history data to the sysvar.
func updateStakeHistorySysvar(acctsDb *accountsdb.AccountsDb, block *block.Block, prevSlotCtx *sealevel.SlotCtx, targetEpoch uint64, scanResult *BoundaryStakeScanResult) *sealevel.SysvarStakeHistory {
	stakeHistoryAcct, err := prevSlotCtx.GetAccount(sealevel.SysvarStakeHistoryAddr)
	if err != nil {
		stakeHistoryAcct, err = acctsDb.GetAccount(prevSlotCtx.Slot, sealevel.SysvarStakeHistoryAddr)
		if err != nil {
			panic(fmt.Sprintf("unable to retrieve stakehistory sysvar: %s", err))
		}
	}
	block.ParentEpochUpdatedAccts = append(block.ParentEpochUpdatedAccts, stakeHistoryAcct.Clone())

	decoder := bin.NewBinDecoder(stakeHistoryAcct.Data)
	var stakeHistory sealevel.SysvarStakeHistory
	stakeHistory.MustUnmarshalWithDecoder(decoder)

	var accumulatorStakeHistoryEntry sealevel.StakeHistoryEntry
	accumulatorStakeHistoryEntry.Effective = scanResult.StakeHistoryEffective
	accumulatorStakeHistoryEntry.Activating = scanResult.StakeHistoryActivating
	accumulatorStakeHistoryEntry.Deactivating = scanResult.StakeHistoryDeactivating
	stakeHistory.Update(targetEpoch, accumulatorStakeHistoryEntry)

	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	stakeHistory.MustMarshalWithEncoder(encoder)
	newStakeHistoryBytes := buf.Bytes()
	copy(stakeHistoryAcct.Data, newStakeHistoryBytes)

	err = acctsDb.StoreAccounts([]*accounts.Account{stakeHistoryAcct}, prevSlotCtx.Slot, nil)
	if err != nil {
		panic(fmt.Sprintf("error storing new StakeHistory sysvar to accountsdb: %s", err))
	}
	block.EpochUpdatedAccts = append(block.EpochUpdatedAccts, stakeHistoryAcct.Clone())

	return &stakeHistory
}

func handleEpochTransition(acctsDb *accountsdb.AccountsDb, partitionedEpochRewards bool, prevSlotCtx *sealevel.SlotCtx, replayCtx *ReplayCtx, epochSchedule *sealevel.SysvarEpochSchedule, f *features.Features, block *block.Block, epoch uint64, rpcc *rpcclient.RpcClient, dbgOpts *DebugOptions) *rewards.PartitionedRewardDistributionInfo {
	// No pre-scan index flush: StreamStakeAccounts merges the RAM-pending
	// stake entries (slots not yet folded) with the file-backed index, so the
	// scan is complete without durably writing entries for slots a fork
	// switch could still unwind. Entries reach the file only at fold time.

	// Load stake history (used by both scan and rewards)
	var stakeHistory sealevel.SysvarStakeHistory
	stakeHistoryAcct, err := prevSlotCtx.GetAccount(sealevel.SysvarStakeHistoryAddr)
	if err != nil {
		stakeHistoryAcct, err = acctsDb.GetAccount(prevSlotCtx.Slot, sealevel.SysvarStakeHistoryAddr)
		if err != nil {
			panic("unable to get stake history sysvar")
		}
	}
	decoder := bin.NewBinDecoder(stakeHistoryAcct.Data)
	stakeHistory.MustUnmarshalWithDecoder(decoder)

	var partitionedRewardsInfo *rewards.PartitionedRewardDistributionInfo
	newEpoch := epoch + 1
	leaderScheduleEpoch := epochSchedule.LeaderScheduleEpoch(block.Slot)

	// Complete the old epoch's history before calculating the new epoch's stakes.
	t0 := time.Now()
	scanResult := scanStakesForEpochBoundary(acctsDb, prevSlotCtx.Slot, epoch, newEpoch, &stakeHistory, epochSchedule, f)
	t1 := time.Now()

	updateEpochStakesAndRefreshVoteCache(leaderScheduleEpoch, block, acctsDb, prevSlotCtx.Slot, scanResult, f, epochSchedule)
	global.PutEpochVoteStateSnapshot(newEpoch, global.VoteCacheSnapshot())
	t2 := time.Now()

	if global.ManageLeaderSchedule() {
		if err = prepareEpochTransitionLeaderSchedules(newEpoch, leaderScheduleEpoch, epochSchedule, ""); err != nil {
			panic(err)
		}

		var hasLeader bool
		block.Leader, hasLeader = global.LeaderForSlot(block.Slot)
		if !hasLeader {
			panic(fmt.Sprintf("couldn't find leader for slot %d at epoch boundary", block.Slot))
		}
	}
	t3 := time.Now()

	if partitionedEpochRewards {
		// Agave calculates the new epoch's validator reward ceiling from the
		// parent bank capitalization, before any boundary accounts or voting
		// rewards are added to this bank.
		epochStartCapitalization := replayCtx.Capitalization
		migrationSlot, alpenglowActive := f.ActivationSlot(features.Alpenglow)
		migrationEpoch := epochSchedule.GetEpoch(migrationSlot)
		rewardedEpoch := newEpoch - 1
		admitted := global.EpochStakes(leaderScheduleEpoch)

		mode := rewards.RewardCalculationMode{}
		if alpenglowActive && rewardedEpoch > migrationEpoch {
			mode.FullAlpenglow = true
			mode.RewardEpochDelegatedStakes = scanResult.RewardEpochEffectiveStakes
		}

		// Agave persists these denominators for both the migration epoch and
		// every full Alpenglow epoch so reward partitions can be reconstructed
		// after a snapshot restore.
		if alpenglowActive && rewardedEpoch >= migrationEpoch {
			updated, parent, err := stageRewardEpochDelegatedStakes(
				acctsDb, prevSlotCtx.Slot, block.Slot, rewardedEpoch,
				admitted, scanResult.RewardEpochEffectiveStakes, replayCtx, f,
			)
			if err != nil {
				panic(err)
			}
			block.EpochUpdatedAccts = append(block.EpochUpdatedAccts, updated)
			block.ParentEpochUpdatedAccts = append(block.ParentEpochUpdatedAccts, parent)
		}

		if alpenglowClockFeatureActive(f) && f.IsActive(features.ValidatorAdmissionTicket) {
			updated, parents, err := applyAlpenglowBoundaryVAT(
				acctsDb, prevSlotCtx.Slot, block.Slot,
				alpenglowVATBurnPerEpoch(f, epochSchedule, block.Slot), admitted,
			)
			if err != nil {
				panic(err)
			}
			block.EpochUpdatedAccts = append(block.EpochUpdatedAccts, updated...)
			block.ParentEpochUpdatedAccts = append(block.ParentEpochUpdatedAccts, parents...)
		}

		var updated, parents []*accounts.Account
		var epochRewardsCapitalizationIncrease uint64
		partitionedRewardsInfo, updated, parents, epochRewardsCapitalizationIncrease = beginPartitionedEpochRewardsDistribution(
			acctsDb, prevSlotCtx, &stakeHistory, replayCtx, epochSchedule,
			block, f, newEpoch, block.Slot, rpcc, dbgOpts, mode, block.EpochUpdatedAccts,
		)
		block.EpochUpdatedAccts = append(block.EpochUpdatedAccts, updated...)
		block.ParentEpochUpdatedAccts = append(block.ParentEpochUpdatedAccts, parents...)

		// The pinned genesis-v1 revision predates the vote-reward inflation
		// account. Its clock uses Alpenglow, but rewards still use the older
		// partitioned-reward state. Creating this later PDA changes its bank hash.
		if alpenglowClockFeatureActive(f) && !genesisV1AlpenglowMetadata([]*features.Features{f}) {
			updated, parent, err := stageEpochInflationAccount(
				acctsDb, prevSlotCtx.Slot, block.Slot, replayCtx, epochSchedule, f,
				newEpoch, epochStartCapitalization, epochRewardsCapitalizationIncrease,
			)
			if err != nil {
				panic(err)
			}
			block.EpochUpdatedAccts = append(block.EpochUpdatedAccts, updated)
			block.ParentEpochUpdatedAccts = append(block.ParentEpochUpdatedAccts, parent)
		}
	} else {
		panic("only partitioned rewards supported")
	}
	t4 := time.Now()

	updateStakeHistorySysvar(acctsDb, block, prevSlotCtx, epoch, scanResult)
	t5 := time.Now()

	// Compact stake index at epoch boundary — removes duplicates from appends
	// (rewrites from the file-backed cache only; RAM-pending entries for
	// unfolded slots are untouched and flush at their own fold).
	if err := global.CompactStakePubkeyIndex(filepath.Join(acctsDb.AcctsDir, "..")); err != nil {
		mlog.Log.Errorf("failed to compact stake pubkey index: %v", err)
	}

	mlog.Log.Infof("Timing: scan=%.1fs epochStakes=%.1fs leaderSched=%.1fs rewards=%.1fs stakeHistory=%.1fs total=%.1fs",
		t1.Sub(t0).Seconds(), t2.Sub(t1).Seconds(), t3.Sub(t2).Seconds(),
		t4.Sub(t3).Seconds(), t5.Sub(t4).Seconds(), t5.Sub(t0).Seconds())
	mlog.Log.Infof("=======================")

	return partitionedRewardsInfo
}

// updateEpochStakesAndRefreshVoteCache applies pre-computed vote/effective stake data
// from the boundary scan to refresh the vote cache and store epoch stakes.
func updateEpochStakesAndRefreshVoteCache(leaderScheduleEpoch uint64, b *block.Block, acctsDb *accountsdb.AccountsDb, slot uint64, scanResult *BoundaryStakeScanResult, f *features.Features, epochSchedule *sealevel.SysvarEpochSchedule) {
	// Check if we need to compute epoch stakes (skip on resume)
	hasEpochStakes := global.HasEpochStakes(leaderScheduleEpoch)

	// ALWAYS refresh vote cache from AccountsDB, even if HasEpochStakes is true
	// This ensures the vote cache has fresh NodePubkey for leader schedule
	voteMetadata, rebuildErr := rebuildVoteCacheFromAccountsDBWithMetadata(acctsDb, slot, scanResult.VoteAcctStakes, 0)
	if rebuildErr != nil {
		mlog.Log.Errorf("failed to rebuild vote cache at epoch boundary: %v", rebuildErr)
	}

	// Skip epoch stakes storage if already cached (resume)
	if hasEpochStakes {
		mlog.Log.Infof("already had EpochStakes for epoch %d", leaderScheduleEpoch)
		return
	}

	// Store epoch stakes computed during scanning. VAT admission changes both the
	// leader schedule and the Alpenglow BLS rank map, so filter once at the shared
	// epoch-stakes boundary rather than only in certificate verification.
	voteCache := global.VoteCache()
	effectiveStakes := scanResult.EffectiveStakes
	totalEffectiveStake := scanResult.TotalEffectiveStake
	if f != nil && f.IsActive(features.ValidatorAdmissionTicket) {
		minimumBalance, err := minimumVoteAccountBalanceForVAT(f, epochSchedule, slot)
		if err != nil {
			panic(err)
		}
		effectiveStakes, totalEffectiveStake = filterEpochStakesForVAT(effectiveStakes, voteCache, voteMetadata, minimumBalance)
		mlog.Log.FileOnlyf("VAT epoch stakes: admitted=%d/%d minimum_vote_balance=%d", len(effectiveStakes), len(scanResult.EffectiveStakes), minimumBalance)
	}
	epochStakes := make(map[solana.PublicKey]uint64, len(effectiveStakes))
	epochVoteAccounts := make(map[solana.PublicKey]*epochstakes.VoteAccount, len(effectiveStakes))
	for votePk, stake := range effectiveStakes {
		voteAcct, exists := voteCache[votePk]
		meta, hasMeta := voteMetadata[votePk]
		if exists && hasMeta {
			lastTimestamp := voteAcct.LastTimestamp()
			var lastTimestampTs int64
			var lastTimestampSlot uint64
			if lastTimestamp != nil {
				lastTimestampTs = lastTimestamp.Timestamp
				lastTimestampSlot = lastTimestamp.Slot
			}
			var executable byte
			if meta.Executable {
				executable = 1
			}
			epochStakes[votePk] = stake
			epochVoteAccounts[votePk] = &epochstakes.VoteAccount{
				Lamports:            meta.Lamports,
				NodePubkey:          voteAcct.NodePubkey(),
				BlsPubkeyCompressed: voteAcct.BlsPubkeyCompressed(),
				LastTimestampTs:     lastTimestampTs,
				LastTimestampSlot:   lastTimestampSlot,
				Owner:               meta.Owner,
				Executable:          executable,
				RentEpoch:           meta.RentEpoch,
			}
		}
	}
	global.PutEpochStakes(leaderScheduleEpoch, epochStakes, epochVoteAccounts, totalEffectiveStake)

	// Epoch-stake maps are immutable bank snapshot state and are shared by every
	// slot in an epoch (including a concurrently forged leader bank). Allocate a
	// new map at the epoch boundary instead of mutating the parent's generation.
	b.EpochStakesPerVoteAcct = maps.Clone(epochStakes)
	b.TotalEpochStake = totalEffectiveStake
}
