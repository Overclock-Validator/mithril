package replay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/wide"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

func newWarmupCooldownRateEpoch(epochSchedule *sealevel.SysvarEpochSchedule, f *features.Features) *uint64 {
	slot, existed := f.ActivationSlot(features.ReduceStakeWarmupCooldown)
	if !existed {
		return nil
	}
	epoch := epochSchedule.GetEpoch(slot)
	return &epoch
}

type votingRewardEntry struct {
	Pubkey      string  `json:"pubkey"`
	Lamports    int64   `json:"lamports"`
	Commission  *uint8  `json:"commission,omitempty"`
	PostBalance *uint64 `json:"post_balance,omitempty"`
}

type localVotingRewardSnapshot struct {
	TrackedVoteAccounts int                 `json:"tracked_vote_accounts"`
	RewardCount         int                 `json:"reward_count"`
	TotalLamports       int64               `json:"total_lamports"`
	Rewards             []votingRewardEntry `json:"rewards"`
}

type rpcVotingRewardSnapshot struct {
	RewardCount   int                 `json:"reward_count"`
	TotalLamports int64               `json:"total_lamports"`
	Rewards       []votingRewardEntry `json:"rewards"`
}

type votingRewardValue struct {
	Lamports    int64
	Commission  *uint8
	PostBalance *uint64
}

type votingRewardComparisonSummary struct {
	LeftTotalLamports  int64 `json:"left_total_lamports"`
	RightTotalLamports int64 `json:"right_total_lamports"`
	TotalLamportsDelta int64 `json:"total_lamports_delta"`
	MismatchedCount    int   `json:"mismatched_count"`
	MissingInLeft      int   `json:"missing_in_left"`
	MissingInRight     int   `json:"missing_in_right"`
}

type votingRewardMismatchRow struct {
	Pubkey string `json:"pubkey"`

	PresentInLocal        bool   `json:"present_in_local"`
	LocalLamports         int64  `json:"local_lamports"`
	PresentInSourceBlock  bool   `json:"present_in_source_block"`
	SourceBlockLamports   int64  `json:"source_block_lamports"`
	SourceBlockCommission *uint8 `json:"source_block_commission,omitempty"`

	PresentInRPCConfirmed  bool   `json:"present_in_rpc_confirmed"`
	RPCConfirmedLamports   int64  `json:"rpc_confirmed_lamports"`
	RPCConfirmedCommission *uint8 `json:"rpc_confirmed_commission,omitempty"`

	PresentInRPCFinalized  bool   `json:"present_in_rpc_finalized"`
	RPCFinalizedLamports   int64  `json:"rpc_finalized_lamports"`
	RPCFinalizedCommission *uint8 `json:"rpc_finalized_commission,omitempty"`
}

type epochBoundaryVotingRewardArtifact struct {
	Slot       uint64 `json:"slot"`
	Epoch      uint64 `json:"epoch"`
	RewardType string `json:"reward_type"`

	GeneratedAt string `json:"generated_at"`
	RPCEndpoint string `json:"rpc_endpoint,omitempty"`

	Local       localVotingRewardSnapshot `json:"local"`
	SourceBlock rpcVotingRewardSnapshot   `json:"source_block"`

	RPCConfirmed      *rpcVotingRewardSnapshot `json:"rpc_confirmed,omitempty"`
	RPCConfirmedError string                   `json:"rpc_confirmed_error,omitempty"`

	RPCFinalized      *rpcVotingRewardSnapshot `json:"rpc_finalized,omitempty"`
	RPCFinalizedError string                   `json:"rpc_finalized_error,omitempty"`

	LocalVsSource        votingRewardComparisonSummary  `json:"local_vs_source"`
	LocalVsRPCConfirmed  *votingRewardComparisonSummary `json:"local_vs_rpc_confirmed,omitempty"`
	LocalVsRPCFinalized  *votingRewardComparisonSummary `json:"local_vs_rpc_finalized,omitempty"`
	SourceVsRPCConfirmed *votingRewardComparisonSummary `json:"source_vs_rpc_confirmed,omitempty"`
	SourceVsRPCFinalized *votingRewardComparisonSummary `json:"source_vs_rpc_finalized,omitempty"`

	Mismatches []votingRewardMismatchRow `json:"mismatches"`
}

func cloneUint8Ptr(v *uint8) *uint8 {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func cloneUint64Ptr(v *uint64) *uint64 {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func rewardEntriesFromValues(values map[string]votingRewardValue) []votingRewardEntry {
	entries := make([]votingRewardEntry, 0, len(values))
	for pubkey, reward := range values {
		entries = append(entries, votingRewardEntry{
			Pubkey:      pubkey,
			Lamports:    reward.Lamports,
			Commission:  cloneUint8Ptr(reward.Commission),
			PostBalance: cloneUint64Ptr(reward.PostBalance),
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Pubkey < entries[j].Pubkey
	})
	return entries
}

func collectLocalVotingRewards(validatorRewards map[solana.PublicKey]*atomic.Uint64) (localVotingRewardSnapshot, map[string]votingRewardValue) {
	rewardsByPubkey := make(map[string]votingRewardValue)
	var totalLamports int64

	for votePubkey, reward := range validatorRewards {
		lamports := int64(reward.Load())
		if lamports == 0 {
			continue
		}
		pubkey := votePubkey.String()
		rewardsByPubkey[pubkey] = votingRewardValue{Lamports: lamports}
		totalLamports += lamports
	}

	return localVotingRewardSnapshot{
		TrackedVoteAccounts: len(validatorRewards),
		RewardCount:         len(rewardsByPubkey),
		TotalLamports:       totalLamports,
		Rewards:             rewardEntriesFromValues(rewardsByPubkey),
	}, rewardsByPubkey
}

func collectRPCVotingRewards(blockRewards []rpc.BlockReward) (rpcVotingRewardSnapshot, map[string]votingRewardValue) {
	rewardsByPubkey := make(map[string]votingRewardValue)
	var totalLamports int64

	for _, reward := range blockRewards {
		if string(reward.RewardType) != rewards.RewardTypeVoting {
			continue
		}

		pubkey := reward.Pubkey.String()
		rewardsByPubkey[pubkey] = votingRewardValue{
			Lamports:    reward.Lamports,
			Commission:  cloneUint8Ptr(reward.Commission),
			PostBalance: cloneUint64Ptr(&reward.PostBalance),
		}
		totalLamports += reward.Lamports
	}

	return rpcVotingRewardSnapshot{
		RewardCount:   len(rewardsByPubkey),
		TotalLamports: totalLamports,
		Rewards:       rewardEntriesFromValues(rewardsByPubkey),
	}, rewardsByPubkey
}

func rewardValueEqual(left, right votingRewardValue) bool {
	return left.Lamports == right.Lamports
}

func summarizeVotingRewardComparison(left, right map[string]votingRewardValue) votingRewardComparisonSummary {
	summary := votingRewardComparisonSummary{}

	for _, reward := range left {
		summary.LeftTotalLamports += reward.Lamports
	}
	for _, reward := range right {
		summary.RightTotalLamports += reward.Lamports
	}
	summary.TotalLamportsDelta = summary.LeftTotalLamports - summary.RightTotalLamports

	keys := make(map[string]struct{}, len(left)+len(right))
	for pubkey := range left {
		keys[pubkey] = struct{}{}
	}
	for pubkey := range right {
		keys[pubkey] = struct{}{}
	}

	for pubkey := range keys {
		leftReward, leftOK := left[pubkey]
		rightReward, rightOK := right[pubkey]

		if !leftOK {
			summary.MissingInLeft++
		}
		if !rightOK {
			summary.MissingInRight++
		}
		if leftOK != rightOK || (leftOK && !rewardValueEqual(leftReward, rightReward)) {
			summary.MismatchedCount++
		}
	}

	return summary
}

func buildVotingRewardMismatchRows(local map[string]votingRewardValue, source map[string]votingRewardValue, rpcConfirmed map[string]votingRewardValue, haveRPCConfirmed bool, rpcFinalized map[string]votingRewardValue, haveRPCFinalized bool) []votingRewardMismatchRow {
	keys := make(map[string]struct{}, len(local)+len(source)+len(rpcConfirmed)+len(rpcFinalized))
	for pubkey := range local {
		keys[pubkey] = struct{}{}
	}
	for pubkey := range source {
		keys[pubkey] = struct{}{}
	}
	if haveRPCConfirmed {
		for pubkey := range rpcConfirmed {
			keys[pubkey] = struct{}{}
		}
	}
	if haveRPCFinalized {
		for pubkey := range rpcFinalized {
			keys[pubkey] = struct{}{}
		}
	}

	pubkeys := make([]string, 0, len(keys))
	for pubkey := range keys {
		pubkeys = append(pubkeys, pubkey)
	}
	sort.Strings(pubkeys)

	rows := make([]votingRewardMismatchRow, 0)
	for _, pubkey := range pubkeys {
		localReward, localOK := local[pubkey]
		sourceReward, sourceOK := source[pubkey]
		confirmedReward, confirmedOK := rpcConfirmed[pubkey]
		finalizedReward, finalizedOK := rpcFinalized[pubkey]

		type rewardPresence struct {
			enabled bool
			present bool
			reward  votingRewardValue
		}

		presences := []rewardPresence{
			{enabled: true, present: localOK, reward: localReward},
			{enabled: true, present: sourceOK, reward: sourceReward},
			{enabled: haveRPCConfirmed, present: confirmedOK, reward: confirmedReward},
			{enabled: haveRPCFinalized, present: finalizedOK, reward: finalizedReward},
		}

		var baseline rewardPresence
		var haveBaseline bool
		var mismatch bool
		for _, presence := range presences {
			if !presence.enabled {
				continue
			}
			if !haveBaseline {
				baseline = presence
				haveBaseline = true
				continue
			}
			if baseline.present != presence.present {
				mismatch = true
				break
			}
			if baseline.present && !rewardValueEqual(baseline.reward, presence.reward) {
				mismatch = true
				break
			}
		}
		if !mismatch {
			continue
		}

		rows = append(rows, votingRewardMismatchRow{
			Pubkey: pubkey,

			PresentInLocal:        localOK,
			LocalLamports:         localReward.Lamports,
			PresentInSourceBlock:  sourceOK,
			SourceBlockLamports:   sourceReward.Lamports,
			SourceBlockCommission: cloneUint8Ptr(sourceReward.Commission),

			PresentInRPCConfirmed:  haveRPCConfirmed && confirmedOK,
			RPCConfirmedLamports:   confirmedReward.Lamports,
			RPCConfirmedCommission: cloneUint8Ptr(confirmedReward.Commission),

			PresentInRPCFinalized:  haveRPCFinalized && finalizedOK,
			RPCFinalizedLamports:   finalizedReward.Lamports,
			RPCFinalizedCommission: cloneUint8Ptr(finalizedReward.Commission),
		})
	}

	return rows
}

func writeReplayArtifact(subdir string, filename string, data any) {
	logDir := mlog.GetLogDir()
	if logDir == "" {
		return
	}
	dir := filepath.Join(logDir, subdir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		mlog.Log.Warnf("artifact: failed to create directory %s: %v", dir, err)
		return
	}

	artifactJSON, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		mlog.Log.Warnf("artifact: failed to marshal %s/%s: %v", subdir, filename, err)
		return
	}

	artifactPath := filepath.Join(dir, filename)
	if err := os.WriteFile(artifactPath, artifactJSON, 0644); err != nil {
		mlog.Log.Warnf("artifact: failed to write %s: %v", artifactPath, err)
		return
	}

	mlog.Log.FileOnlyf("artifact written: %s", artifactPath)
}

func maybeDumpEpochVotingRewardDiff(dbgOpts *DebugOptions, rpcc *rpcclient.RpcClient, block *block.Block, epoch uint64, slot uint64, validatorRewards map[solana.PublicKey]*atomic.Uint64) {
	if dbgOpts == nil || !dbgOpts.DumpEpochVotingRewardDiff() {
		return
	}

	localSnapshot, localRewards := collectLocalVotingRewards(validatorRewards)
	sourceSnapshot, sourceRewards := collectRPCVotingRewards(block.Rewards)

	artifact := epochBoundaryVotingRewardArtifact{
		Slot:          slot,
		Epoch:         epoch,
		RewardType:    rewards.RewardTypeVoting,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Local:         localSnapshot,
		SourceBlock:   sourceSnapshot,
		LocalVsSource: summarizeVotingRewardComparison(localRewards, sourceRewards),
	}

	var confirmedRewards map[string]votingRewardValue
	var finalizedRewards map[string]votingRewardValue
	haveRPCConfirmed := false
	haveRPCFinalized := false

	if rpcc != nil {
		artifact.RPCEndpoint = rpcc.Endpoint()

		confirmedBlockRewards, err := rpcc.GetRewardsForSlotWithCommitment(slot, rpc.CommitmentConfirmed, 2*time.Second)
		if err != nil {
			artifact.RPCConfirmedError = err.Error()
		} else {
			confirmedSnapshot, rewardsByPubkey := collectRPCVotingRewards(confirmedBlockRewards)
			artifact.RPCConfirmed = &confirmedSnapshot
			artifact.LocalVsRPCConfirmed = ptrToVotingRewardSummary(summarizeVotingRewardComparison(localRewards, rewardsByPubkey))
			artifact.SourceVsRPCConfirmed = ptrToVotingRewardSummary(summarizeVotingRewardComparison(sourceRewards, rewardsByPubkey))
			confirmedRewards = rewardsByPubkey
			haveRPCConfirmed = true
		}

		finalizedBlockRewards, err := rpcc.GetRewardsForSlotWithCommitment(slot, rpc.CommitmentFinalized, 2*time.Second)
		if err != nil {
			artifact.RPCFinalizedError = err.Error()
		} else {
			finalizedSnapshot, rewardsByPubkey := collectRPCVotingRewards(finalizedBlockRewards)
			artifact.RPCFinalized = &finalizedSnapshot
			artifact.LocalVsRPCFinalized = ptrToVotingRewardSummary(summarizeVotingRewardComparison(localRewards, rewardsByPubkey))
			artifact.SourceVsRPCFinalized = ptrToVotingRewardSummary(summarizeVotingRewardComparison(sourceRewards, rewardsByPubkey))
			finalizedRewards = rewardsByPubkey
			haveRPCFinalized = true
		}
	}

	artifact.Mismatches = buildVotingRewardMismatchRows(localRewards, sourceRewards, confirmedRewards, haveRPCConfirmed, finalizedRewards, haveRPCFinalized)

	writeReplayArtifact("rewards", fmt.Sprintf("epoch_boundary_voting_rewards_slot_%d.json", slot), artifact)
}

func ptrToVotingRewardSummary(summary votingRewardComparisonSummary) *votingRewardComparisonSummary {
	return &summary
}

// epochRewardAccountLoader composes same-bank staged epoch writes over the
// speculative parent bank. AccountsDB is only the fallback: in rooted-durable
// mode it deliberately does not contain unrooted writes.
func epochRewardAccountLoader(acctsDb *accountsdb.AccountsDb, slot uint64, parentCtx *sealevel.SlotCtx, staged []*accounts.Account) rewards.RewardAccountLoader {
	stagedByKey := make(map[solana.PublicKey]*accounts.Account, len(staged))
	for _, acct := range staged {
		if acct != nil {
			stagedByKey[acct.Key] = acct
		}
	}
	return func(pubkey solana.PublicKey) (*accounts.Account, error) {
		if acct := stagedByKey[pubkey]; acct != nil {
			return acct.Clone(), nil
		}
		if parentCtx != nil {
			return parentCtx.GetAccountFromAccountsDb(pubkey)
		}
		return acctsDb.GetAccount(slot, pubkey)
	}
}

// capitalizingEpochRewards mirrors Bank::begin_partitioned_rewards' return
// value: commissions paid now plus all calculated staker rewards paid by the
// upcoming partitions. EpochInflationAccountState uses that full eventual
// capitalization increase, not just the boundary-slot commissions.
func capitalizingEpochRewards(votingRewards, stakerRewards uint64) uint64 {
	total, err := safemath.CheckedAddU64(votingRewards, stakerRewards)
	if err != nil {
		panic(fmt.Sprintf("overflow in capitalizing epoch rewards: voting=%d staker=%d", votingRewards, stakerRewards))
	}
	return total
}

func beginPartitionedEpochRewardsDistribution(acctsDb *accountsdb.AccountsDb, slotCtx *sealevel.SlotCtx, stakeHistory *sealevel.SysvarStakeHistory, epochCtx *ReplayCtx, epochSchedule *sealevel.SysvarEpochSchedule, block *block.Block, f *features.Features, epoch uint64, slot uint64, rpcc *rpcclient.RpcClient, dbgOpts *DebugOptions, mode rewards.RewardCalculationMode, stagedEpochAccts []*accounts.Account) (*rewards.PartitionedRewardDistributionInfo, []*accounts.Account, []*accounts.Account, uint64) {
	partitionedRewardsInfo := rewards.DeterminePartitionedStakingRewardsInfo(epochSchedule, &epochCtx.Inflation, epochCtx.Capitalization, epoch, epoch-1, slot, epochCtx.SlotsPerYear, f)
	totalRewards, err := partitionedRewardsBudget(
		slotCtx, epochSchedule, f, epoch-1, partitionedRewardsInfo.TotalStakingRewards,
	)
	if err != nil {
		panic(err)
	}
	// Keep the distribution descriptor and PointValue/EpochRewards sysvar on
	// the same recorded ceiling.  The calculated stake and voting payouts may
	// be smaller, but they must never redefine the epoch's original budget.
	partitionedRewardsInfo.TotalStakingRewards = totalRewards

	newWarmupCooldownRateEpoch := newWarmupCooldownRateEpoch(epochSchedule, f)
	voteCacheSnapshot := global.VoteCacheSnapshot()
	if f.IsActive(features.ValidatorAdmissionTicket) {
		admitted := global.EpochStakes(epochSchedule.LeaderScheduleEpoch(block.Slot))
		for votePubkey := range voteCacheSnapshot {
			if _, ok := admitted[votePubkey]; !ok {
				delete(voteCacheSnapshot, votePubkey)
			}
		}
	}

	pointValue := rewards.PointValue{Rewards: totalRewards, Points: wide.Uint128{}}
	streamResult, streamErr := rewards.CalculateRewardsStreaming(
		acctsDb, slot, stakeHistory, newWarmupCooldownRateEpoch,
		voteCacheSnapshot, pointValue, epoch-1, slotCtx.Blockhash, slotCtx, f, mode)
	if streamErr != nil {
		panic(fmt.Sprintf("streaming rewards calculation failed: %s", streamErr))
	}
	// An active EpochRewards sysvar must always have at least one partition so
	// the distribution loop has a block on which to deactivate it. The reward
	// calculator follows Agave and returns one empty partition for zero rewards;
	// keep this check before any reward-account mutation as a fail-closed guard.
	if streamResult.NumPartitions == 0 {
		panic("streaming rewards calculation returned zero partitions")
	}

	partitionedRewardsInfo.SpoolDir = streamResult.SpoolDir
	partitionedRewardsInfo.SpoolSlot = streamResult.SpoolSlot
	partitionedRewardsInfo.NumRewardPartitionsRemaining = streamResult.NumPartitions

	maybeDumpEpochCalculatedRewards(dbgOpts, epoch, slot, streamResult)
	maybeDumpEpochVotingRewardDiff(dbgOpts, rpcc, block, epoch, slot, streamResult.ValidatorRewards)

	rewardLoader := epochRewardAccountLoader(acctsDb, slot, slotCtx, stagedEpochAccts)
	updatedAccts, parentUpdatedAccts, voteRewardsDistributed := rewards.DistributeVotingRewards(acctsDb, streamResult.ValidatorRewards, slot, rewardLoader)

	newEpochRewards := sealevel.SysvarEpochRewards{DistributionStartingBlockHeight: block.BlockHeight + 1,
		NumPartitions: streamResult.NumPartitions, ParentBlockhash: block.LastBlockhash,
		TotalRewards: totalRewards, DistributedRewards: voteRewardsDistributed, TotalPoints: streamResult.TotalPoints, Active: true}

	epochRewardsAcct, err := rewardLoader(sealevel.SysvarEpochRewardsAddr)
	if err != nil {
		panic(fmt.Sprintf("unable to get EpochRewards from acctsdb: %s", err))
	}
	epochRewardsAcct = epochRewardsAcct.Clone()
	parentUpdatedAccts = append(parentUpdatedAccts, epochRewardsAcct.Clone())

	writer := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(writer)
	newEpochRewards.MustMarshalWithEncoder(encoder)
	copy(epochRewardsAcct.Data, writer.Bytes())

	err = acctsDb.StoreAccounts([]*accounts.Account{epochRewardsAcct}, slot, nil)
	if err != nil {
		panic(fmt.Sprintf("unable to update EpochRewards sysvar to acctsdb: %s", err))
	}
	sealevel.SysvarCache.EpochRewards.Acct = epochRewardsAcct
	sealevel.SysvarCache.EpochRewards.Sysvar = &newEpochRewards

	updatedAccts = append(updatedAccts, epochRewardsAcct.Clone())
	epochCtx.Capitalization += voteRewardsDistributed

	return partitionedRewardsInfo, updatedAccts, parentUpdatedAccts,
		capitalizingEpochRewards(voteRewardsDistributed, streamResult.TotalStakerRewards)
}

func distributePartitionedEpochRewardsForSlot(acctsDb *accountsdb.AccountsDb, parentCtx *sealevel.SlotCtx, stagedEpochAccts []*accounts.Account, epochCtx *ReplayCtx, partitionedEpochRewardsInfo *rewards.PartitionedRewardDistributionInfo, currentSlot uint64, currentBlockHeight uint64) ([]*accounts.Account, []*accounts.Account) {
	rewardLoader := epochRewardAccountLoader(acctsDb, currentSlot, parentCtx, stagedEpochAccts)
	epochRewardsAcct, err := rewardLoader(sealevel.SysvarEpochRewardsAddr)
	if err != nil {
		panic(fmt.Sprintf("unable to get EpochRewards from acctsdb: %s", err))
	}

	var epochRewards sealevel.SysvarEpochRewards
	decoder := bin.NewBinDecoder(epochRewardsAcct.Data)
	epochRewards.MustUnmarshalWithDecoder(decoder)

	// Reward distribution is scheduled by block height, not slot. If the first
	// slots of an epoch are skipped, the epoch-boundary bank can already be past
	// FirstStakingRewardSlot while its block height is still one before the
	// distribution start recorded in the freshly staged EpochRewards sysvar.
	if currentBlockHeight < epochRewards.DistributionStartingBlockHeight {
		return nil, nil
	}

	partitionIdx := currentBlockHeight - epochRewards.DistributionStartingBlockHeight

	distributedAccts, parentDistributedAccts, distributedLamports, burnedLamports := rewards.DistributeStakingRewardsFromSpool(acctsDb, partitionedEpochRewardsInfo.SpoolDir, partitionedEpochRewardsInfo.SpoolSlot, partitionIdx, currentSlot, rewardLoader)
	parentDistributedAccts = append(parentDistributedAccts, epochRewardsAcct.Clone())

	// EpochRewards sysvar advances by distributed + burned (matching Agave/FD),
	// but capitalization only increases by distributed.
	epochRewards.Distribute(distributedLamports + burnedLamports)
	partitionedEpochRewardsInfo.NumRewardPartitionsRemaining--

	distributionComplete := false
	if partitionedEpochRewardsInfo.NumRewardPartitionsRemaining == 0 {
		epochRewards.Active = false
		distributionComplete = true
		rewards.CleanupPartitionedSpoolFiles(partitionedEpochRewardsInfo.SpoolDir, partitionedEpochRewardsInfo.SpoolSlot, epochRewards.NumPartitions)
	}

	writer := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(writer)
	epochRewards.MustMarshalWithEncoder(encoder)
	copy(epochRewardsAcct.Data, writer.Bytes())

	err = acctsDb.StoreAccounts([]*accounts.Account{epochRewardsAcct}, currentSlot, nil)
	if err != nil {
		panic(fmt.Sprintf("unable to update EpochRewards sysvar to acctsdb: %s", err))
	}
	sealevel.SysvarCache.EpochRewards.Acct = epochRewardsAcct
	sealevel.SysvarCache.EpochRewards.Sysvar = &epochRewards

	distributedAccts = append(distributedAccts, epochRewardsAcct.Clone())
	epochCtx.Capitalization += distributedLamports

	if distributionComplete {
		mlog.Log.Infof("epoch rewards distribution complete: slot=%d partitions=%d active=%t sysvar_distributed_rewards=%d sysvar_total_rewards=%d", currentSlot, epochRewards.NumPartitions, epochRewards.Active, epochRewards.DistributedRewards, epochRewards.TotalRewards)
	}

	if burnedLamports > 0 {
		mlog.Log.Warnf("partition %d: distributed=%d burned=%d", partitionIdx, distributedLamports, burnedLamports)
	}

	return distributedAccts, parentDistributedAccts
}
