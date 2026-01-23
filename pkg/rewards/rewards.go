package rewards

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/wide"
	"github.com/dgryski/go-sip13"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/panjf2000/ants/v2"
)

const (
	RewardTypeFee     string = "Fee"
	RewardTypeRent    string = "Rent"
	RewardTypeVoting  string = "Voting"
	RewardTypeStaking string = "Staking"
)

type PartitionedRewardDistributionInfo struct {
	TotalStakingRewards          uint64
	FirstStakingRewardSlot       uint64
	NumRewardPartitionsRemaining uint64
	Credits                      map[solana.PublicKey]CalculatedStakePoints
	RewardPartitions             Partitions
	StakingRewards               map[solana.PublicKey]*CalculatedStakeRewards
}

type CalculatedStakePoints struct {
	Points                              wide.Uint128
	NewCreditsObserved                  uint64
	ForceCreditsUpdateWithSkippedReward bool
}

func SlotInYearForInflation(epochSchedule *sealevel.SysvarEpochSchedule, slotsPerYear float64, epoch uint64, f *features.Features) float64 {
	numSlots := GetInflationNumSlots(epochSchedule, epoch, f)
	return float64(numSlots) / slotsPerYear
}

func GetInflationNumSlots(epochSchedule *sealevel.SysvarEpochSchedule, epoch uint64, f *features.Features) uint64 {
	inflationActivationSlot := GetInflationStartSlot(f)
	inflationStartSlot := epochSchedule.FirstSlotInEpoch(safemath.SaturatingSubU64(epochSchedule.GetEpoch(inflationActivationSlot), 1))
	return epochSchedule.FirstSlotInEpoch(epoch) - inflationStartSlot
}

func GetInflationStartSlot(f *features.Features) uint64 {
	fullInflationFeatures := f.FullInflationFeaturesEnabled()
	var activationSlots []uint64

	for _, inflationFeature := range fullInflationFeatures {
		activationSlot, _ := f.ActivationSlot(inflationFeature)
		activationSlots = append(activationSlots, activationSlot)
	}

	sort.Slice(activationSlots, func(i, j int) bool {
		return activationSlots[i] < activationSlots[j]
	})

	if len(activationSlots) == 0 {
		picoActivationSlot, isActivated := f.ActivationSlot(features.PicoInflation)
		if !isActivated {
			return 0
		} else {
			return picoActivationSlot
		}
	} else {
		return activationSlots[0]
	}
}

func CalculatePreviousEpochInflationRewards(epochSchedule *sealevel.SysvarEpochSchedule, inflation *Inflation, prevEpochCapitalization, epoch, prevEpoch uint64, slotsPerYear float64, f *features.Features) uint64 {
	slotInYear := SlotInYearForInflation(epochSchedule, slotsPerYear, epoch, f)
	validatorRate := inflation.Validator(slotInYear)
	prevEpochDurationInYears := float64(epochSchedule.SlotsInEpoch(prevEpoch)) / slotsPerYear

	validatorRewards := validatorRate * float64(prevEpochCapitalization) * prevEpochDurationInYears
	return uint64(validatorRewards)
}

func IsWithinRewardsPeriod(epoch uint64, slot uint64, epochSchedule *sealevel.SysvarEpochSchedule) bool {
	firstSlotInEpoch := epochSchedule.FirstSlotInEpoch(epoch)
	if slot < (firstSlotInEpoch + 243) {
		return true
	} else {
		return false
	}
}

// DeterminePartitionedStakingRewardsInfo fetches reward partition info from RPC with failover support.
// It tries the primary RPC first with retries, then falls back to backup endpoints.
func DeterminePartitionedStakingRewardsInfo(rpcc *rpcclient.RpcClient, rpcBackups []string, epochSchedule *sealevel.SysvarEpochSchedule, inflation *Inflation, prevEpochCapitalization uint64, epoch uint64, prevEpoch uint64, slot uint64, slotsPerYear float64, f *features.Features) *PartitionedRewardDistributionInfo {
	firstSlotInEpoch := epochSchedule.FirstSlotInEpoch(epoch)
	totalStakingRewards := CalculatePreviousEpochInflationRewards(epochSchedule, inflation, prevEpochCapitalization, epoch, prevEpoch, slotsPerYear, f)
	return &PartitionedRewardDistributionInfo{TotalStakingRewards: totalStakingRewards, FirstStakingRewardSlot: firstSlotInEpoch + 1}
}

type idxAndReward struct {
	idx    int
	reward rpc.BlockReward
}

type idxAndRewardNew struct {
	idx     int
	reward  uint64
	voterPk solana.PublicKey
}

func DistributeVotingRewards(acctsDb *accountsdb.AccountsDb, validatorRewards map[solana.PublicKey]*atomic.Uint64, slot uint64) ([]*accounts.Account, []*accounts.Account, uint64) {
	var totalVotingRewards atomic.Uint64

	updatedAccts := make([]*accounts.Account, len(validatorRewards))
	parentUpdatedAccts := make([]*accounts.Account, len(validatorRewards))

	var wg sync.WaitGroup

	size := runtime.GOMAXPROCS(0) * 2
	workerPool, _ := ants.NewPoolWithFunc(size, func(i interface{}) {
		defer wg.Done()

		r := i.(idxAndRewardNew)
		reward := r.reward
		voterPk := r.voterPk
		idx := r.idx

		voteAcct, err := acctsDb.GetAccount(slot, voterPk)
		if err != nil {
			return
		}
		parentUpdatedAccts[idx] = voteAcct.Clone()

		voteAcct.Lamports, err = safemath.CheckedAddU64(voteAcct.Lamports, uint64(reward))
		if err != nil {
			panic(fmt.Sprintf("overflow in voting rewards distribution in slot %d to acct %s: %s", slot, voterPk, err))
		}

		updatedAccts[idx] = voteAcct

		new := totalVotingRewards.Add(uint64(reward))
		if new < uint64(reward) {
			panic(fmt.Sprintf("overflow in accumulating voting rewards in slot %d", slot))
		}
	})

	var idx int
	for votePk, reward := range validatorRewards {
		r := idxAndRewardNew{idx: idx, reward: reward.Load(), voterPk: votePk}
		wg.Add(1)
		workerPool.Invoke(r)
		idx++
	}

	wg.Wait()
	workerPool.Release()
	ants.Release()

	err := acctsDb.StoreAccounts(updatedAccts, slot, nil)
	if err != nil {
		panic(fmt.Sprintf("error updating accounts for voting rewards in slot %d: %s", slot, err))
	}

	return updatedAccts, parentUpdatedAccts, totalVotingRewards.Load()
}

type idxAndPubkey struct {
	idx    int
	pubkey solana.PublicKey
}

func DistributeStakingRewardsForPartition(acctsDb *accountsdb.AccountsDb, partition *Partition, stakingRewards map[solana.PublicKey]*CalculatedStakeRewards, slot uint64) ([]*accounts.Account, []*accounts.Account, uint64) {
	var distributedLamports atomic.Uint64
	accts := make([]*accounts.Account, partition.NumPubkeys())
	parentAccts := make([]*accounts.Account, partition.NumPubkeys())

	var wg sync.WaitGroup

	size := runtime.GOMAXPROCS(0) * 8
	workerPool, _ := ants.NewPoolWithFunc(size, func(i interface{}) {
		defer wg.Done()

		ip := i.(idxAndPubkey)
		idx := ip.idx
		stakePk := ip.pubkey

		reward, ok := stakingRewards[stakePk]
		if !ok {
			return
		}

		stakeAcct, err := acctsDb.GetAccount(slot, stakePk)
		if err != nil {
			panic(fmt.Sprintf("unable to get acct %s from acctsdb for partitioned epoch rewards distribution in slot %d", stakePk, slot))
		}
		parentAccts[idx] = stakeAcct.Clone()

		// update the delegation in the stake account state
		stakeState, err := sealevel.UnmarshalStakeState(stakeAcct.Data)
		if err != nil {
			return
		}

		stakeState.Stake.Stake.CreditsObserved = reward.NewCreditsObserved
		stakeState.Stake.Stake.Delegation.StakeLamports = safemath.SaturatingAddU64(stakeState.Stake.Stake.Delegation.StakeLamports, uint64(reward.StakerRewards))

		newStakeStateBytes, err := sealevel.MarshalStakeStake(stakeState)
		if err != nil {
			panic(fmt.Sprintf("unable to serialize new stake account state in distributing partitioned rewards: %s", err))
		}
		copy(stakeAcct.Data, newStakeStateBytes)

		// update lamports in stake account
		stakeAcct.Lamports, err = safemath.CheckedAddU64(stakeAcct.Lamports, uint64(reward.StakerRewards))
		if err != nil {
			panic(fmt.Sprintf("overflow in partitioned epoch rewards distribution in slot %d to acct %s: %s", slot, stakePk, err))
		}

		accts[idx] = stakeAcct
		distributedLamports.Add(reward.StakerRewards)

		// update the stake cache
		delegationToCache := stakeState.Stake.Stake.Delegation
		delegationToCache.CreditsObserved = stakeState.Stake.Stake.CreditsObserved
		global.PutStakeCacheItem(stakePk, &delegationToCache)
	})

	for idx, stakePk := range partition.Pubkeys() {
		ip := idxAndPubkey{idx: idx, pubkey: stakePk}
		wg.Add(1)
		workerPool.Invoke(ip)
	}
	wg.Wait()

	workerPool.Release()
	ants.Release()

	err := acctsDb.StoreAccounts(accts, slot, nil)
	if err != nil {
		panic(fmt.Sprintf("error updating accounts for partitioned epoch rewards in slot %d: %s", slot, err))
	}

	return accts, parentAccts, distributedLamports.Load()
}

func minimumStakeDelegation(slotCtx *sealevel.SlotCtx) uint64 {
	if !slotCtx.Features.IsActive(features.StakeMinimumDelegationForRewards) {
		return 0
	}

	if slotCtx.Features.IsActive(features.StakeRaiseMinimumDelegationTo1Sol) {
		return 1000000000
	}

	return 1
}

func CalculateRewardPartitionForPubkey(pubkey solana.PublicKey, blockhash [32]byte, numPartitions uint64) uint64 {
	var data [64]byte
	copy(data[:32], blockhash[:])
	copy(data[32:], pubkey[:])
	hash := sip13.Sum64(0, 0, data[:])

	ulongMaxPlus1 := wide.Uint128FromUint64(math.MaxUint64).Add(wide.Uint128FromUint64(1))
	partitionIdx := wide.Uint128FromUint64(numPartitions).Mul(wide.Uint128FromUint64(hash)).Div(ulongMaxPlus1)
	partitionIdx64 := partitionIdx.Uint64()

	return partitionIdx64
}

type PointValue struct {
	Rewards uint64
	Points  wide.Uint128
}

type CalculatedStakeRewards struct {
	StakerRewards      uint64
	VoterRewards       uint64
	VoterPubkey        solana.PublicKey
	NewCreditsObserved uint64
}

func CalculateStakeRewardsAndPartitions(pointsPerStakeAcct map[solana.PublicKey]*CalculatedStakePoints, slotCtx *sealevel.SlotCtx, stakeHistory *sealevel.SysvarStakeHistory, slot uint64, rewardedEpoch uint64, pointValue PointValue, newRateActivationEpoch *uint64, f *features.Features, stakeCache map[solana.PublicKey]*sealevel.Delegation, voteCache map[solana.PublicKey]*sealevel.VoteStateVersions) (map[solana.PublicKey]*CalculatedStakeRewards, map[solana.PublicKey]*atomic.Uint64, Partitions) {
	stakeInfoResults := make(map[solana.PublicKey]*CalculatedStakeRewards, 1500000)
	validatorRewards := make(map[solana.PublicKey]*atomic.Uint64, 2000)

	minimumStakeDelegation := minimumStakeDelegation(slotCtx)

	var stakeMu sync.Mutex
	var wg sync.WaitGroup

	workerPool, _ := ants.NewPoolWithFunc(runtime.GOMAXPROCS(0)*8, func(i interface{}) {
		defer wg.Done()

		delegation := i.(*delegationAndPubkey)

		if delegation.delegation.StakeLamports < minimumStakeDelegation {
			return
		}

		voterPk := delegation.delegation.VoterPubkey
		voteStateVersioned := voteCache[voterPk]
		if voteStateVersioned == nil {
			return
		}

		pointsForStakeAcct := pointsPerStakeAcct[delegation.pubkey]
		calculatedStakeRewards := CalculateStakeRewardsForAcct(delegation.pubkey, pointsForStakeAcct, delegation.delegation, voteStateVersioned, rewardedEpoch, pointValue, newRateActivationEpoch)
		if calculatedStakeRewards != nil {
			stakeMu.Lock()
			stakeInfoResults[delegation.pubkey] = calculatedStakeRewards
			stakeMu.Unlock()

			validatorRewards[voterPk].Add(calculatedStakeRewards.VoterRewards)
		}
	})

	for _, delegation := range stakeCache {
		_, exists := validatorRewards[delegation.VoterPubkey]
		if !exists {
			validatorRewards[delegation.VoterPubkey] = &atomic.Uint64{}
		}
	}

	for pk, delegation := range stakeCache {
		d := &delegationAndPubkey{delegation: delegation, pubkey: pk}
		wg.Add(1)
		workerPool.Invoke(d)
	}
	wg.Wait()
	workerPool.Release()

	numRewardPartitions := CalculateNumRewardPartitions(uint64(len(stakeInfoResults)))
	partitions := NewPartitions(numRewardPartitions)

	partitionCalcWorkerPool, _ := ants.NewPoolWithFunc(runtime.GOMAXPROCS(0)*8, func(i interface{}) {
		defer wg.Done()

		stakePk := i.(solana.PublicKey)
		idx := CalculateRewardPartitionForPubkey(stakePk, slotCtx.Blockhash, numRewardPartitions)
		partitions.AddPubkey(idx, stakePk)
	})

	for stakePk := range stakeInfoResults {
		wg.Add(1)
		partitionCalcWorkerPool.Invoke(stakePk)
	}
	wg.Wait()
	partitionCalcWorkerPool.Release()
	ants.Release()

	return stakeInfoResults, validatorRewards, partitions
}

func CalculateStakeRewardsForAcct(pubkey solana.PublicKey, stakePointsResult *CalculatedStakePoints, delegation *sealevel.Delegation, voteState *sealevel.VoteStateVersions, rewardedEpoch uint64, pointValue PointValue, newRateActivationEpoch *uint64) *CalculatedStakeRewards {
	if pointValue.Rewards == 0 || delegation.ActivationEpoch == rewardedEpoch {
		stakePointsResult.ForceCreditsUpdateWithSkippedReward = true
	}

	if stakePointsResult.ForceCreditsUpdateWithSkippedReward {
		result := &CalculatedStakeRewards{NewCreditsObserved: stakePointsResult.NewCreditsObserved}
		return result
	}

	zero128 := wide.Uint128FromUint64(0)
	if stakePointsResult.Points.Eq(zero128) || pointValue.Points.Eq(zero128) {
		//mlog.Log.Debugf("CalculateStakeRewardsForAcct: returning nil for %s. stakePointsResult.Points = %d, pointValue.Points = %d", stakePubkey, stakePointsResult.Points.Uint64(), pointValue.Points.Uint64())
		return nil
	}

	rewards128 := stakePointsResult.Points.Mul(wide.Uint128FromUint64(pointValue.Rewards)).Div(pointValue.Points)
	if !rewards128.IsUint64() {
		//mlog.Log.Debugf("CalculateStakeRewardsForAcct: returning nil for %s. rewards128 not a uint64. %s", stakePubkey, rewards128)
		return nil
	}

	rewards := rewards128.Uint64()
	if rewards == 0 {
		//mlog.Log.Debugf("CalculateStakeRewardsForAcct: returning nil for %s. rewards == 0", stakePubkey)
		return nil
	}

	splitResult := voteCommissionSplit(voteState, rewards)
	if splitResult.IsSplit && (splitResult.VoterPortion == 0 || splitResult.StakerPortion == 0) {
		//mlog.Log.Debugf("CalculateStakeRewardsForAcct: returning nil for %s. IsSplit = %t, splitResult.VoterPortion = %d, splitResult.StakerPortion = %d", stakePubkey, splitResult.VoterPortion, splitResult.StakerPortion)
		return nil
	}

	result := &CalculatedStakeRewards{StakerRewards: splitResult.StakerPortion,
		VoterRewards: splitResult.VoterPortion, NewCreditsObserved: stakePointsResult.NewCreditsObserved,
		VoterPubkey: delegation.VoterPubkey}

	//mlog.Log.Debugf("returning CalculatedStakeRewards for %s. %+v", stakePubkey, result)

	return result
}

type CommissionSplit struct {
	VoterPortion  uint64
	StakerPortion uint64
	IsSplit       bool
}

func mulDivPercent(on uint64, pct uint64) uint64 {
	// pct must be 0..100
	q := on / 100
	r := on % 100
	return q*pct + (r*pct)/100
}

func voteCommissionSplit(voteState *sealevel.VoteStateVersions, rewards uint64) CommissionSplit {
	var commission byte

	switch voteState.Type {
	case sealevel.VoteStateVersionCurrent:
		commission = voteState.Current.Commission
	case sealevel.VoteStateVersionV0_23_5:
		commission = voteState.V0_23_5.Commission
	case sealevel.VoteStateVersionV1_14_11:
		commission = voteState.V1_14_11.Commission
	}

	commissionRate := uint64(min(commission, 100))
	result := CommissionSplit{}

	switch commissionRate {
	case 0:
		// no commission, all rewards go to staker
		result.StakerPortion = rewards
	case 100:
		// 100% commission, all rewards go to validator
		result.VoterPortion = rewards
	default:
		mine := mulDivPercent(rewards, commissionRate)
		theirs := mulDivPercent(rewards, 100-commissionRate)

		result.VoterPortion = mine
		result.StakerPortion = theirs
		result.IsSplit = true
	}

	return result
}

type delegationAndPubkey struct {
	delegation *sealevel.Delegation
	pubkey     solana.PublicKey
}

func CalculateStakePoints(
	acctsDb *accountsdb.AccountsDb,
	slotCtx *sealevel.SlotCtx,
	slot uint64,
	stakeHistory *sealevel.SysvarStakeHistory,
	newWarmupCooldownRateEpoch *uint64,
	stakeCache map[solana.PublicKey]*sealevel.Delegation,
	voteCache map[solana.PublicKey]*sealevel.VoteStateVersions,
) (map[solana.PublicKey]*CalculatedStakePoints, wide.Uint128) {
	minimum := minimumStakeDelegation(slotCtx)

	n := len(stakeCache)
	pks := make([]solana.PublicKey, 0, n)
	for pk := range stakeCache {
		pks = append(pks, pk)
	}

	pointsAccum := NewCalculatedStakePointsAccumulator(pks)
	var wg sync.WaitGroup

	size := runtime.GOMAXPROCS(0) * 8
	workerPool, _ := ants.NewPoolWithFunc(size, func(i interface{}) {
		defer wg.Done()

		t := i.(*delegationAndPubkey)
		d := t.delegation
		if d.StakeLamports < minimum {
			return
		}

		voterPk := d.VoterPubkey
		voteState := voteCache[voterPk]
		if voteState == nil {
			return
		}

		pcs := calculateStakePointsAndCredits(t.pubkey, stakeHistory, d, voteState, newWarmupCooldownRateEpoch)
		pointsAccum.Add(t.pubkey, pcs)
	})

	for pk, delegation := range stakeCache {
		wg.Add(1)
		workerPool.Invoke(&delegationAndPubkey{delegation: delegation, pubkey: pk})
	}

	wg.Wait()
	workerPool.Release()
	ants.Release()

	return pointsAccum.CalculatedStakePoints(), pointsAccum.TotalPoints()
}

func calculateStakePointsAndCredits(
	pubkey solana.PublicKey,
	stakeHistory *sealevel.SysvarStakeHistory,
	delegation *sealevel.Delegation,
	voteState *sealevel.VoteStateVersions,
	newRateActivationEpoch *uint64,
) CalculatedStakePoints {
	creditsInStake := delegation.CreditsObserved

	var epochCredits []sealevel.EpochCredits
	switch voteState.Type {
	case sealevel.VoteStateVersionCurrent:
		epochCredits = voteState.Current.EpochCredits
	case sealevel.VoteStateVersionV0_23_5:
		epochCredits = voteState.V0_23_5.EpochCredits
	case sealevel.VoteStateVersionV1_14_11:
		epochCredits = voteState.V1_14_11.EpochCredits
	default:
		panic("invalid vote state - should be impossible")
	}

	var creditsInVote uint64
	if len(epochCredits) != 0 {
		creditsInVote = epochCredits[len(epochCredits)-1].Credits
	}

	if creditsInVote < creditsInStake {
		return CalculatedStakePoints{
			NewCreditsObserved:                  creditsInVote,
			ForceCreditsUpdateWithSkippedReward: true,
		}
	}

	if creditsInVote == creditsInStake || len(epochCredits) == 0 {
		return CalculatedStakePoints{NewCreditsObserved: creditsInVote}
	}

	/*start := sort.Search(len(epochCredits), func(i int) bool {
		return epochCredits[i].Credits > creditsInStake
	})
	if start >= len(epochCredits) {
		return CalculatedStakePoints{NewCreditsObserved: creditsInVote}
	}*/

	var points wide.Uint128
	newObserved := creditsInStake

	for _, ec := range epochCredits {
		final := ec.Credits
		initial := ec.PrevCredits

		var earnedCredits uint64
		if creditsInStake < initial {
			earnedCredits = final - initial
		} else if creditsInStake < final {
			earnedCredits = final - newObserved
		}

		if earnedCredits != 0 {
			stakeAmt := delegation.StakeActivatingAndDeactivating(ec.Epoch, stakeHistory, newRateActivationEpoch).Effective
			earnedPoints := wide.Uint128FromUint64(stakeAmt).Mul(wide.Uint128FromUint64(earnedCredits))
			points = points.Add(earnedPoints)

		}

		newObserved = max(newObserved, final)
	}

	return CalculatedStakePoints{
		Points:             points,
		NewCreditsObserved: newObserved,
	}
}

func CalculateNumRewardPartitions(numStakingRewards uint64) uint64 {
	numEligible := numStakingRewards
	target := uint64(4096)
	slotsInEpoch := uint64(432000)
	unclamped := (numEligible + (target - 1)) / target
	cap := slotsInEpoch / 10
	numRewardPartitions := min(unclamped, cap)

	return numRewardPartitions
}
