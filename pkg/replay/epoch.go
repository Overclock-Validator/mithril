package replay

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"

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

func updateStakeHistorySysvar(acctsDb *accountsdb.AccountsDb, block *block.Block, prevSlotCtx *sealevel.SlotCtx, targetEpoch uint64, epochSchedule *sealevel.SysvarEpochSchedule, f *features.Features) *sealevel.SysvarStakeHistory {
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

	newRateActivationEpoch := newWarmupCooldownRateEpoch(epochSchedule, f)

	var effective atomic.Uint64
	var activating atomic.Uint64
	var deactivating atomic.Uint64

	// Stream stakes from AccountsDB instead of iterating cache
	// Note: callback is called from multiple goroutines (worker pool)
	_, err = global.StreamStakeAccounts(acctsDb, prevSlotCtx.Slot,
		func(pk solana.PublicKey, delegation *sealevel.Delegation, creditsObs uint64) {
			if delegation.StakeLamports == 0 {
				return
			}

			stakeHistoryEntry := delegation.StakeActivatingAndDeactivating(targetEpoch, &stakeHistory, newRateActivationEpoch)

			effective.Add(stakeHistoryEntry.Effective)
			activating.Add(stakeHistoryEntry.Activating)
			deactivating.Add(stakeHistoryEntry.Deactivating)
		})
	if err != nil {
		panic(fmt.Sprintf("error streaming stake accounts for stake history: %s", err))
	}

	var accumulatorStakeHistoryEntry sealevel.StakeHistoryEntry
	accumulatorStakeHistoryEntry.Activating = activating.Load()
	accumulatorStakeHistoryEntry.Effective = effective.Load()
	accumulatorStakeHistoryEntry.Deactivating = deactivating.Load()
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

func handleEpochTransition(acctsDb *accountsdb.AccountsDb, rpcc *rpcclient.RpcClient, rpcBackups []string, partitionedEpochRewards bool, prevSlotCtx *sealevel.SlotCtx, replayCtx *ReplayCtx, epochSchedule *sealevel.SysvarEpochSchedule, f *features.Features, block *block.Block, epoch uint64) *rewards.PartitionedRewardDistributionInfo {
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
	firstSlotInEpoch := epochSchedule.FirstSlotInEpoch(newEpoch)

	leaderScheduleEpoch := epochSchedule.LeaderScheduleEpoch(block.Slot)
	updateEpochStakesAndRefreshVoteCache(leaderScheduleEpoch, block, epochSchedule, f, acctsDb, prevSlotCtx.Slot)

	if global.ManageLeaderSchedule() {
		if len(global.EpochStakesVoteAccts(newEpoch)) > 0 {
			_, err = PrepareLeaderScheduleLocal(newEpoch, epochSchedule, "")
		} else {
			_, err = PrepareLeaderScheduleLocalFromVoteCache(newEpoch, epochSchedule, "")
		}

		if err != nil {
			panic(err)
		}

		var hasLeader bool
		block.Leader, hasLeader = global.LeaderForSlot(block.Slot)
		if !hasLeader {
			panic(fmt.Sprintf("couldn't find leader for slot %d at epoch boundary", block.Slot))
		}
	}

	if partitionedEpochRewards {
		partitionedRewardsInfo, block.EpochUpdatedAccts, block.ParentEpochUpdatedAccts = beginPartitionedEpochRewardsDistribution(acctsDb, prevSlotCtx, &stakeHistory, replayCtx, epochSchedule, rpcc, rpcBackups, block, f, newEpoch, firstSlotInEpoch)
	} else {
		panic("only partitioned rewards supported")
	}

	updateStakeHistorySysvar(acctsDb, block, prevSlotCtx, epoch, epochSchedule, f)
	mlog.Log.Infof("epoch transition %d -> %d done.", epoch, newEpoch)

	return partitionedRewardsInfo
}

type epochStakesVoteAcctData struct {
	nodePubkey solana.PublicKey
	stake      atomic.Uint64
}

type epochStakesBuilder struct {
	mu             sync.Mutex
	epoch          uint64
	epochStakesMap map[solana.PublicKey]*epochStakesVoteAcctData
	totalStake     atomic.Uint64
}

func newEpochStakesBuilder(epoch uint64, voteCache map[solana.PublicKey]*sealevel.VoteStateVersions) *epochStakesBuilder {
	epochStakesMap := make(map[solana.PublicKey]*epochStakesVoteAcctData, len(voteCache))
	for votePk, voteAcct := range voteCache {
		epochStakesMap[votePk] = &epochStakesVoteAcctData{nodePubkey: voteAcct.NodePubkey()}
	}
	return &epochStakesBuilder{epoch: epoch, epochStakesMap: epochStakesMap}
}

func (esb *epochStakesBuilder) AddStakeForVoteAcct(voteAcct solana.PublicKey, stake uint64) {
	info := esb.epochStakesMap[voteAcct]
	info.stake.Add(stake)
	esb.totalStake.Add(stake)
}

func (esb *epochStakesBuilder) Finish() {
	for voterPubkey, entry := range esb.epochStakesMap {
		global.PutEpochStakesEntry(esb.epoch, voterPubkey, entry.stake.Load(), &epochstakes.VoteAccount{NodePubkey: entry.nodePubkey})
	}
	global.PutEpochTotalStake(esb.epoch, esb.totalStake.Load())
}

func (esb *epochStakesBuilder) TotalEpochStake() uint64 {
	return esb.totalStake.Load()
}

func updateEpochStakesAndRefreshVoteCache(leaderScheduleEpoch uint64, b *block.Block, epochSchedule *sealevel.SysvarEpochSchedule, f *features.Features, acctsDb *accountsdb.AccountsDb, slot uint64) {
	newRateActivationEpoch := newWarmupCooldownRateEpoch(epochSchedule, f)

	// Check if we need to compute epoch stakes (skip on resume)
	hasEpochStakes := global.HasEpochStakes(leaderScheduleEpoch)

	// Build vote totals by streaming from AccountsDB
	// Use per-batch local maps + mutex merge for thread-safety
	voteAcctStakes := make(map[solana.PublicKey]uint64)
	var voteAcctStakesMu sync.Mutex

	// For epoch stakes calculation, we'll also track effective stakes
	// We pre-compute these even if hasEpochStakes is true (minor overhead vs 2 passes)
	effectiveStakes := make(map[solana.PublicKey]uint64)
	var effectiveStakesMu sync.Mutex
	var totalEffectiveStake atomic.Uint64

	// Single streaming pass to build both raw totals and effective stakes
	_, err := global.StreamStakeAccounts(acctsDb, slot,
		func(pk solana.PublicKey, delegation *sealevel.Delegation, creditsObs uint64) {
			// Accumulate raw stake totals for vote cache refresh
			voteAcctStakesMu.Lock()
			voteAcctStakes[delegation.VoterPubkey] += delegation.StakeLamports
			voteAcctStakesMu.Unlock()

			// Compute effective stake for epoch stakes
			effectiveStake := delegation.Stake(leaderScheduleEpoch, sealevel.SysvarCache.StakeHistory.Sysvar, newRateActivationEpoch)
			if effectiveStake > 0 {
				effectiveStakesMu.Lock()
				effectiveStakes[delegation.VoterPubkey] += effectiveStake
				effectiveStakesMu.Unlock()
				totalEffectiveStake.Add(effectiveStake)
			}
		})
	if err != nil {
		mlog.Log.Errorf("failed to stream stake accounts: %v", err)
	}

	// ALWAYS refresh vote cache from AccountsDB, even if HasEpochStakes is true
	// This ensures the vote cache has fresh NodePubkey for leader schedule
	if err := RebuildVoteCacheFromAccountsDB(acctsDb, slot, voteAcctStakes, 0); err != nil {
		mlog.Log.Errorf("failed to rebuild vote cache at epoch boundary: %v", err)
	}

	// Skip epoch stakes storage if already cached (resume)
	if hasEpochStakes {
		mlog.Log.Infof("already had EpochStakes for epoch %d", leaderScheduleEpoch)
		return
	}

	// Store epoch stakes computed during streaming
	voteCache := global.VoteCache()
	for votePk, stake := range effectiveStakes {
		voteAcct, exists := voteCache[votePk]
		if exists {
			global.PutEpochStakesEntry(leaderScheduleEpoch, votePk, stake, &epochstakes.VoteAccount{NodePubkey: voteAcct.NodePubkey()})
		}
	}
	global.PutEpochTotalStake(leaderScheduleEpoch, totalEffectiveStake.Load())

	maps.Copy(b.EpochStakesPerVoteAcct, global.EpochStakes(leaderScheduleEpoch))
	b.TotalEpochStake = totalEffectiveStake.Load()
}
