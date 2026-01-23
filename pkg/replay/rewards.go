package replay

import (
	"bytes"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"

	//"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/wide"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

func newWarmupCooldownRateEpoch(epochSchedule *sealevel.SysvarEpochSchedule, f *features.Features) *uint64 {
	slot, existed := f.ActivationSlot(features.ReduceStakeWarmupCooldown)
	if !existed {
		return nil
	}
	epoch := epochSchedule.GetEpoch(slot)
	return &epoch
}

func validateCalculatedValidatorRewards(rpcRewards []rpc.BlockReward, calculateRewards map[solana.PublicKey]*atomic.Uint64) {
	for _, rpcReward := range rpcRewards {
		if calculateRewards[rpcReward.Pubkey].Load() != uint64(rpcReward.Lamports) {
			mlog.Log.Infof("reward for vote acct %s - rpc reward %d did not match calculated reward %d", rpcReward.Pubkey, rpcReward.Lamports, calculateRewards[rpcReward.Pubkey])
		}
	}
}

func logRewardSnapshots(epoch uint64, slot uint64, stakeCache map[solana.PublicKey]*sealevel.Delegation, voteCache map[solana.PublicKey]*sealevel.VoteStateVersions) {
	stakeCount, stakeLamports, stakeSum, stakeXor := stakeSnapshotDigest(stakeCache)
	voteCount, voteCredits, voteSum, voteXor := voteSnapshotDigest(voteCache)

	mlog.Log.Infof(
		"stake & vote acct snapshots: epoch=%d slot=%d stake_accts=%d vote_accts=%d stake_lamports=%d stake_hash_sum=%016x stake_hash_xor=%016x vote_credits=%d vote_hash_sum=%016x vote_hash_xor=%016x",
		epoch,
		slot,
		stakeCount,
		voteCount,
		stakeLamports,
		stakeSum,
		stakeXor,
		voteCredits,
		voteSum,
		voteXor,
	)
}

func stakeSnapshotDigest(stakeCache map[solana.PublicKey]*sealevel.Delegation) (int, uint64, uint64, uint64) {
	var count int
	var totalStake uint64
	var sum uint64
	var xor uint64

	for stakePk, delegation := range stakeCache {
		if delegation == nil {
			continue
		}
		h := fnvOffset64
		h = fnv1a64Add(h, stakePk[:])
		h = fnv1a64Add(h, delegation.VoterPubkey[:])
		h = fnv1a64AddUint64(h, delegation.StakeLamports)
		h = fnv1a64AddUint64(h, delegation.ActivationEpoch)
		h = fnv1a64AddUint64(h, delegation.DeactivationEpoch)
		h = fnv1a64AddUint64(h, math.Float64bits(delegation.WarmupCooldownRate))
		h = fnv1a64AddUint64(h, delegation.CreditsObserved)

		sum += h
		xor ^= h
		totalStake += delegation.StakeLamports
		count++
	}

	return count, totalStake, sum, xor
}

func voteSnapshotDigest(voteCache map[solana.PublicKey]*sealevel.VoteStateVersions) (int, uint64, uint64, uint64) {
	var count int
	var creditsSum uint64
	var sum uint64
	var xor uint64

	for votePk, voteState := range voteCache {
		if voteState == nil {
			continue
		}

		var commission byte
		var epochCredits []sealevel.EpochCredits
		switch voteState.Type {
		case sealevel.VoteStateVersionCurrent:
			commission = voteState.Current.Commission
			epochCredits = voteState.Current.EpochCredits
		case sealevel.VoteStateVersionV0_23_5:
			commission = voteState.V0_23_5.Commission
			epochCredits = voteState.V0_23_5.EpochCredits
		case sealevel.VoteStateVersionV1_14_11:
			commission = voteState.V1_14_11.Commission
			epochCredits = voteState.V1_14_11.EpochCredits
		default:
			continue
		}

		var last sealevel.EpochCredits
		if len(epochCredits) != 0 {
			last = epochCredits[len(epochCredits)-1]
			creditsSum += last.Credits
		}

		h := fnvOffset64
		h = fnv1a64Add(h, votePk[:])
		nodePk := voteState.NodePubkey()
		h = fnv1a64Add(h, nodePk[:])
		h = fnv1a64AddUint64(h, uint64(voteState.Type))
		h = fnv1a64AddUint64(h, uint64(commission))
		h = fnv1a64AddUint64(h, uint64(len(epochCredits)))
		h = fnv1a64AddUint64(h, last.Epoch)
		h = fnv1a64AddUint64(h, last.Credits)
		h = fnv1a64AddUint64(h, last.PrevCredits)

		sum += h
		xor ^= h
		count++
	}

	return count, creditsSum, sum, xor
}

func fnv1a64Add(h uint64, b []byte) uint64 {
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return h
}

func fnv1a64AddUint64(h uint64, v uint64) uint64 {
	for i := 0; i < 8; i++ {
		h ^= uint64(byte(v))
		h *= fnvPrime64
		v >>= 8
	}
	return h
}

func beginPartitionedEpochRewardsDistribution(acctsDb *accountsdb.AccountsDb, slotCtx *sealevel.SlotCtx, stakeHistory *sealevel.SysvarStakeHistory, epochCtx *ReplayCtx, epochSchedule *sealevel.SysvarEpochSchedule, rpcc *rpcclient.RpcClient, rpcBackups []string, block *block.Block, f *features.Features, epoch uint64, slot uint64) (*rewards.PartitionedRewardDistributionInfo, []*accounts.Account, []*accounts.Account) {
	partitionedRewardsInfo := rewards.DeterminePartitionedStakingRewardsInfo(rpcc, rpcBackups, epochSchedule, &epochCtx.Inflation, epochCtx.Capitalization, epoch, epoch-1, slot, epochCtx.SlotsPerYear, f)
	totalRewards := partitionedRewardsInfo.TotalStakingRewards

	newWarmupCooldownRateEpoch := newWarmupCooldownRateEpoch(epochSchedule, f)
	var points wide.Uint128
	var pointsPerStakeAcct map[solana.PublicKey]*rewards.CalculatedStakePoints
	stakeCacheSnapshot := global.StakeCacheSnapshot()
	voteCacheSnapshot := global.VoteCacheSnapshot()

	pointsPerStakeAcct, points = rewards.CalculateStakePoints(acctsDb, slotCtx, slot, stakeHistory, newWarmupCooldownRateEpoch, stakeCacheSnapshot, voteCacheSnapshot)
	pointValue := rewards.PointValue{Rewards: totalRewards, Points: points}

	var validatorRewards map[solana.PublicKey]*atomic.Uint64
	partitionedRewardsInfo.StakingRewards, validatorRewards, partitionedRewardsInfo.RewardPartitions = rewards.CalculateStakeRewardsAndPartitions(pointsPerStakeAcct, slotCtx, stakeHistory, slot, epoch-1, pointValue, newWarmupCooldownRateEpoch, slotCtx.Features, stakeCacheSnapshot, voteCacheSnapshot)
	updatedAccts, parentUpdatedAccts, voteRewardsDistributed := rewards.DistributeVotingRewards(acctsDb, validatorRewards, slot)
	partitionedRewardsInfo.NumRewardPartitionsRemaining = partitionedRewardsInfo.RewardPartitions.NumPartitions()

	newEpochRewards := sealevel.SysvarEpochRewards{DistributionStartingBlockHeight: block.BlockHeight + 1,
		NumPartitions: partitionedRewardsInfo.NumRewardPartitionsRemaining, ParentBlockhash: block.LastBlockhash,
		TotalRewards: totalRewards, DistributedRewards: voteRewardsDistributed, TotalPoints: points, Active: true}

	epochRewardsAcct, err := acctsDb.GetAccount(slot, sealevel.SysvarEpochRewardsAddr)
	if err != nil {
		panic(fmt.Sprintf("unable to get EpochRewards from acctsdb: %s", err))
	}
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

	return partitionedRewardsInfo, updatedAccts, parentUpdatedAccts
}

func distributePartitionedEpochRewardsForSlot(acctsDb *accountsdb.AccountsDb, epochCtx *ReplayCtx, partitionedEpochRewardsInfo *rewards.PartitionedRewardDistributionInfo, currentSlot uint64, currentBlockHeight uint64) ([]*accounts.Account, []*accounts.Account) {
	epochRewardsAcct, err := acctsDb.GetAccount(currentSlot, sealevel.SysvarEpochRewardsAddr)
	if err != nil {
		panic(fmt.Sprintf("unable to get EpochRewards from acctsdb: %s", err))
	}

	var epochRewards sealevel.SysvarEpochRewards
	decoder := bin.NewBinDecoder(epochRewardsAcct.Data)
	epochRewards.MustUnmarshalWithDecoder(decoder)

	partitionIdx := currentBlockHeight - epochRewards.DistributionStartingBlockHeight
	distributedAccts, parentDistributedAccts, distributedLamports := rewards.DistributeStakingRewardsForPartition(acctsDb, partitionedEpochRewardsInfo.RewardPartitions.Partition(partitionIdx), partitionedEpochRewardsInfo.StakingRewards, currentSlot)
	parentDistributedAccts = append(parentDistributedAccts, epochRewardsAcct.Clone())

	epochRewards.Distribute(distributedLamports)
	partitionedEpochRewardsInfo.NumRewardPartitionsRemaining--

	if partitionedEpochRewardsInfo.NumRewardPartitionsRemaining == 0 {
		epochRewards.Active = false
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

	return distributedAccts, parentDistributedAccts
}
