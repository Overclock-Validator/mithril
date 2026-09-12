package rewards

import (
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/wide"
	"github.com/dgryski/go-sip13"
	"github.com/gagliardetto/solana-go"
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
	SpoolDir                     string
	SpoolSlot                    uint64
}

type CalculatedStakePoints struct {
	Points                              wide.Uint128
	NewCreditsObserved                  uint64
	ForceCreditsUpdateWithSkippedReward bool
}

const legacyInflationSlotsPerYear = 78_892_314.984

type inflationSlotTimeTransition struct {
	gate         features.FeatureGate
	slotsPerYear float64
}

var inflationSlotTimeTransitions = [...]inflationSlotTimeTransition{
	{gate: features.ReduceSlotTimeTo350ms, slotsPerYear: 90_162_645.696},
	{gate: features.ReduceSlotTimeTo300ms, slotsPerYear: 105_189_753.312},
	{gate: features.ReduceSlotTimeTo250ms, slotsPerYear: 126_227_703.974},
	{gate: features.ReduceSlotTimeTo200ms, slotsPerYear: 157_784_629.968},
}

type effectiveInflationSlotTime struct {
	slot         uint64
	slotsPerYear float64
}

// inflationSlotTimeArchive mirrors Agave's SlotParamsArchive for the values
// that affect inflation. Slot-time gates activate at the following epoch
// boundary, and out-of-order longer-slot transitions are discarded.
func inflationSlotTimeArchive(
	epochSchedule *sealevel.SysvarEpochSchedule,
	restoredSlotsPerYear float64,
	bankEpoch uint64,
	f *features.Features,
) (float64, []effectiveInflationSlotTime) {
	baseline := restoredSlotsPerYear
	if baseline == 0 {
		baseline = legacyInflationSlotsPerYear
	}
	if f == nil {
		return baseline, nil
	}

	type candidate struct {
		effectiveSlot uint64
		slotsPerYear  float64
	}
	candidates := make([]candidate, len(inflationSlotTimeTransitions))
	for i, transition := range inflationSlotTimeTransitions {
		activationSlot, active := f.ActivationSlot(transition.gate)
		if !active {
			continue
		}
		activationEpoch := epochSchedule.GetEpoch(activationSlot)
		candidates[i] = candidate{
			effectiveSlot: epochSchedule.FirstSlotInEpoch(safemath.SaturatingAddU64(activationEpoch, 1)),
			slotsPerYear:  transition.slotsPerYear,
		}
	}

	// Once a reduction is effective, Agave reconstructs the historical archive
	// from the genesis (400 ms) baseline rather than the current snapshot value.
	bankFirstSlot := epochSchedule.FirstSlotInEpoch(bankEpoch)
	for _, candidate := range candidates {
		if candidate.slotsPerYear != 0 && candidate.effectiveSlot <= bankFirstSlot {
			baseline = legacyInflationSlotsPerYear
			break
		}
	}

	archive := make([]effectiveInflationSlotTime, 0, len(candidates))
	earliestSameOrShorter := uint64(math.MaxUint64)
	for i := len(candidates) - 1; i >= 0; i-- {
		candidate := candidates[i]
		if candidate.slotsPerYear == 0 || candidate.effectiveSlot >= earliestSameOrShorter {
			continue
		}
		archive = append(archive, effectiveInflationSlotTime{
			slot: candidate.effectiveSlot, slotsPerYear: candidate.slotsPerYear,
		})
		earliestSameOrShorter = candidate.effectiveSlot
	}
	sort.Slice(archive, func(i, j int) bool { return archive[i].slot < archive[j].slot })
	return baseline, archive
}

func inflationSlotRangeDurationInYears(
	epochSchedule *sealevel.SysvarEpochSchedule,
	restoredSlotsPerYear float64,
	bankEpoch, startSlot, endSlot uint64,
	f *features.Features,
) float64 {
	if startSlot >= endSlot {
		return 0
	}
	baseline, archive := inflationSlotTimeArchive(epochSchedule, restoredSlotsPerYear, bankEpoch, f)
	cursor := startSlot
	currentSlotsPerYear := baseline
	var duration float64
	for _, transition := range archive {
		if transition.slot <= startSlot {
			currentSlotsPerYear = transition.slotsPerYear
			continue
		}
		if transition.slot >= endSlot {
			break
		}
		duration += float64(transition.slot-cursor) / currentSlotsPerYear
		cursor = transition.slot
		currentSlotsPerYear = transition.slotsPerYear
	}
	return duration + float64(endSlot-cursor)/currentSlotsPerYear
}

func inflationSlotsPerYearAtSlot(
	epochSchedule *sealevel.SysvarEpochSchedule,
	restoredSlotsPerYear float64,
	bankEpoch, slot uint64,
	f *features.Features,
) float64 {
	baseline, archive := inflationSlotTimeArchive(epochSchedule, restoredSlotsPerYear, bankEpoch, f)
	slotsPerYear := baseline
	for _, transition := range archive {
		if transition.slot > slot {
			break
		}
		slotsPerYear = transition.slotsPerYear
	}
	return slotsPerYear
}

func SlotInYearForInflation(epochSchedule *sealevel.SysvarEpochSchedule, slotsPerYear float64, epoch uint64, f *features.Features) float64 {
	inflationActivationSlot := GetInflationStartSlot(f)
	inflationStartSlot := epochSchedule.FirstSlotInEpoch(safemath.SaturatingSubU64(epochSchedule.GetEpoch(inflationActivationSlot), 1))
	return inflationSlotRangeDurationInYears(
		epochSchedule, slotsPerYear, epoch, inflationStartSlot,
		epochSchedule.FirstSlotInEpoch(epoch), f,
	)
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
	prevEpochSlotsPerYear := inflationSlotsPerYearAtSlot(
		epochSchedule, slotsPerYear, epoch,
		epochSchedule.FirstSlotInEpoch(prevEpoch), f,
	)
	prevEpochDurationInYears := float64(epochSchedule.SlotsInEpoch(prevEpoch)) / prevEpochSlotsPerYear

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

// DeterminePartitionedStakingRewardsInfo calculates the total staking rewards for the epoch.
func DeterminePartitionedStakingRewardsInfo(epochSchedule *sealevel.SysvarEpochSchedule, inflation *Inflation, prevEpochCapitalization uint64, epoch uint64, prevEpoch uint64, slot uint64, slotsPerYear float64, f *features.Features) *PartitionedRewardDistributionInfo {
	firstSlotInEpoch := epochSchedule.FirstSlotInEpoch(epoch)
	totalStakingRewards := CalculatePreviousEpochInflationRewards(epochSchedule, inflation, prevEpochCapitalization, epoch, prevEpoch, slotsPerYear, f)
	return &PartitionedRewardDistributionInfo{TotalStakingRewards: totalStakingRewards, FirstStakingRewardSlot: firstSlotInEpoch + 1}
}

type idxAndRewardNew struct {
	idx     int
	reward  uint64
	voterPk solana.PublicKey
}

// RewardAccountLoader resolves the account state on which a reward mutation
// must build. In rooted-durable replay this is the speculative parent/epoch
// staging overlay rather than AccountsDB, which intentionally contains only
// finalized state.
type RewardAccountLoader func(solana.PublicKey) (*accounts.Account, error)

func loadRewardAccount(acctsDb *accountsdb.AccountsDb, slot uint64, loader RewardAccountLoader, pubkey solana.PublicKey) (*accounts.Account, error) {
	if loader != nil {
		return loader(pubkey)
	}
	return acctsDb.GetAccount(slot, pubkey)
}

func DistributeVotingRewards(acctsDb *accountsdb.AccountsDb, validatorRewards map[solana.PublicKey]*atomic.Uint64, slot uint64, loader RewardAccountLoader) ([]*accounts.Account, []*accounts.Account, uint64) {
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

		voteAcct, err := loadRewardAccount(acctsDb, slot, loader, voterPk)
		if err != nil {
			return
		}
		// Loaders may return a retained speculative-overlay value. Reward
		// distribution owns its mutation, so never modify that parent in place.
		voteAcct = voteAcct.Clone()
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

	err := acctsDb.StoreAccounts(updatedAccts, slot, nil)
	if err != nil {
		panic(fmt.Sprintf("error updating accounts for voting rewards in slot %d: %s", slot, err))
	}

	return updatedAccts, parentUpdatedAccts, totalVotingRewards.Load()
}

func DistributeStakingRewardsFromSpool(acctsDb *accountsdb.AccountsDb, spoolDir string, spoolSlot uint64, partitionIdx uint64, currentSlot uint64, loader RewardAccountLoader) ([]*accounts.Account, []*accounts.Account, uint64, uint64) {
	reader, err := NewPartitionReader(spoolDir, spoolSlot, uint32(partitionIdx))
	if err != nil {
		panic(fmt.Sprintf("unable to open partition %d spool for distribution: %s", partitionIdx, err))
	}
	if reader == nil {
		return nil, nil, 0, 0
	}
	defer reader.Close()

	var records []*SpoolRecord
	for {
		rec, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(fmt.Sprintf("unable to read partition %d spool record: %s", partitionIdx, err))
		}
		records = append(records, rec)
	}

	if len(records) == 0 {
		return nil, nil, 0, 0
	}

	accts := make([]*accounts.Account, len(records))
	parentAccts := make([]*accounts.Account, len(records))
	var distributedLamports atomic.Uint64
	var burnedLamports atomic.Uint64

	var wg sync.WaitGroup
	workerPool, _ := ants.NewPoolWithFunc(runtime.GOMAXPROCS(0)*8, func(i interface{}) {
		defer wg.Done()
		idx := i.(int)
		rec := records[idx]

		// Per-record failures burn rewards instead of panicking, matching Agave
		// distribution.rs:282-294 and Firedancer fd_rewards.c:958-968.
		// Burned rewards advance the EpochRewards sysvar but do NOT increase
		// capitalization.

		stakeAcct, err := loadRewardAccount(acctsDb, currentSlot, loader, rec.StakePubkey)
		if err != nil {
			if errors.Is(err, accountsdb.ErrNoAccount) {
				// AccountNotFound — matches Agave DistributionError::AccountNotFound
				mlog.Log.Warnf("spool distribution: account %s not found in slot %d, %d lamports burned", rec.StakePubkey, currentSlot, rec.RewardLamports)
				burnedLamports.Add(rec.RewardLamports)
				return
			}
			// Storage/IO error — hard fail, do not burn
			panic(fmt.Sprintf("spool distribution: GetAccount failed for %s in slot %d: %v", rec.StakePubkey, currentSlot, err))
		}
		// See DistributeVotingRewards: the speculative parent is immutable.
		stakeAcct = stakeAcct.Clone()
		parentAccts[idx] = stakeAcct.Clone()

		stakeState, err := sealevel.UnmarshalStakeState(stakeAcct.Data)
		if err != nil {
			// Stake state decode failure — matches FD fd_stake_get_state != 0
			mlog.Log.Warnf("spool distribution: stake state decode failed for %s, %d lamports burned: %v", rec.StakePubkey, rec.RewardLamports, err)
			burnedLamports.Add(rec.RewardLamports)
			return
		}

		if stakeState.Status != sealevel.StakeStateV2StatusStake {
			// Non-stake state — matches FD !fd_stake_state_v2_is_stake
			mlog.Log.Warnf("spool distribution: account %s not in Stake state (status=%d), %d lamports burned", rec.StakePubkey, stakeState.Status, rec.RewardLamports)
			burnedLamports.Add(rec.RewardLamports)
			return
		}

		stakeState.Stake.Stake.CreditsObserved = rec.CreditsObserved
		stakeState.Stake.Stake.Delegation.StakeLamports = safemath.SaturatingAddU64(stakeState.Stake.Stake.Delegation.StakeLamports, rec.RewardLamports)

		err = sealevel.MarshalStakeStakeInto(stakeState, stakeAcct.Data)
		if err != nil {
			// Re-encode failure — matches Agave DistributionError::UnableToSetState
			mlog.Log.Warnf("spool distribution: stake state encode failed for %s, %d lamports burned: %v", rec.StakePubkey, rec.RewardLamports, err)
			burnedLamports.Add(rec.RewardLamports)
			return
		}

		stakeAcct.Lamports, err = safemath.CheckedAddU64(stakeAcct.Lamports, rec.RewardLamports)
		if err != nil {
			// Arithmetic overflow — matches Agave DistributionError::ArithmeticOverflow
			mlog.Log.Warnf("spool distribution: lamports overflow for %s, %d lamports burned: %v", rec.StakePubkey, rec.RewardLamports, err)
			burnedLamports.Add(rec.RewardLamports)
			return
		}

		accts[idx] = stakeAcct
		distributedLamports.Add(rec.RewardLamports)
	})

	for idx := range records {
		wg.Add(1)
		workerPool.Invoke(idx)
	}
	wg.Wait()
	workerPool.Release()

	// Filter out nil entries (burned records) before storing
	var filteredAccts []*accounts.Account
	for _, a := range accts {
		if a != nil {
			filteredAccts = append(filteredAccts, a)
		}
	}

	if len(filteredAccts) > 0 {
		err = acctsDb.StoreAccounts(filteredAccts, currentSlot, nil)
		if err != nil {
			panic(fmt.Sprintf("error updating accounts for spool distribution in slot %d: %s", currentSlot, err))
		}
	}

	return accts, parentAccts, distributedLamports.Load(), burnedLamports.Load()
}

func minimumStakeDelegation(slotCtx *sealevel.SlotCtx) uint64 {
	if !slotCtx.Features.IsActive(features.StakeMinimumDelegationForRewards) {
		return 0
	}

	if slotCtx.Features.IsActive(features.StakeRaiseMinimumDelegationTo1Sol) ||
		slotCtx.Features.IsActive(features.UpgradeBpfStakeProgramToV5) {
		return 1000000000
	}

	return 1
}

func selectCommissionVoteState(
	votePubkey solana.PublicKey,
	distributionVoteStates map[solana.PublicKey]*sealevel.VoteStateVersions,
	rewardedEpochVoteStates map[solana.PublicKey]*sealevel.VoteStateVersions,
	snapshotEpochVoteStates map[solana.PublicKey]*sealevel.VoteStateVersions,
) *sealevel.VoteStateVersions {
	if snapshotEpochVoteStates != nil {
		if voteState := snapshotEpochVoteStates[votePubkey]; voteState != nil {
			return voteState
		}
	}

	if rewardedEpochVoteStates != nil {
		if voteState := rewardedEpochVoteStates[votePubkey]; voteState != nil {
			return voteState
		}
	}

	return distributionVoteStates[votePubkey]
}

func buildDelayedCommissionVoteStateCache(
	distributionVoteStates map[solana.PublicKey]*sealevel.VoteStateVersions,
	rewardedEpoch uint64,
) map[solana.PublicKey]*sealevel.VoteStateVersions {
	if len(distributionVoteStates) == 0 {
		return nil
	}

	rewardedEpochVoteStates := global.EpochVoteStateSnapshot(rewardedEpoch)
	var snapshotEpochVoteStates map[solana.PublicKey]*sealevel.VoteStateVersions
	if rewardedEpoch > 0 {
		snapshotEpochVoteStates = global.EpochVoteStateSnapshot(rewardedEpoch - 1)
	}

	commissionVoteStates := make(map[solana.PublicKey]*sealevel.VoteStateVersions, len(distributionVoteStates))
	for votePubkey := range distributionVoteStates {
		voteState := selectCommissionVoteState(votePubkey, distributionVoteStates, rewardedEpochVoteStates, snapshotEpochVoteStates)
		if voteState != nil {
			commissionVoteStates[votePubkey] = voteState
		}
	}

	return commissionVoteStates
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

// RewardCalculationMode selects the credit semantics used for an epoch reward
// calculation.  Tower credits are vote counts and are stake-weighted before
// being scaled by PointValue.  Once Alpenglow is fully active, vote-account
// credits are already lamport-denominated validator rewards; a delegation is
// entitled to its effective-stake fraction of those credits directly.
type RewardCalculationMode struct {
	FullAlpenglow              bool
	RewardEpochDelegatedStakes map[solana.PublicKey]uint64
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

	splitResult := voteCommissionSplit(voteState, rewards, false, false)
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

func mulDivBPS(on uint64, bps uint64) uint64 {
	// bps must be 0..10_000. Splitting the quotient and remainder avoids a
	// u64 overflow while retaining the exact floor(on*bps/10_000).
	q := on / 10_000
	r := on % 10_000
	return q*bps + (r*bps)/10_000
}

func voteInflationCommissionBPS(voteState *sealevel.VoteStateVersions, useBasisPoints bool) uint16 {
	var commissionBPS uint16

	switch voteState.Type {
	case sealevel.VoteStateVersionCurrent:
		commissionBPS = uint16(voteState.Current.Commission) * 100
	case sealevel.VoteStateVersionV0_23_5:
		commissionBPS = uint16(voteState.V0_23_5.Commission) * 100
	case sealevel.VoteStateVersionV1_14_11:
		commissionBPS = uint16(voteState.V1_14_11.Commission) * 100
	case sealevel.VoteStateVersionV4:
		commissionBPS = voteState.V4.InflationRewardsCommissionBps
	}
	if !useBasisPoints {
		commissionBPS = (commissionBPS / 100) * 100
	}
	return min(commissionBPS, uint16(10_000))
}

func voteCommissionSplit(voteState *sealevel.VoteStateVersions, rewards uint64, preserveLamports, useBasisPoints bool) CommissionSplit {
	commissionBPS := uint64(voteInflationCommissionBPS(voteState, useBasisPoints))

	result := CommissionSplit{}

	switch commissionBPS {
	case 0:
		// no commission, all rewards go to staker
		result.StakerPortion = rewards
	case 10_000:
		// 100% commission, all rewards go to validator
		result.VoterPortion = rewards
	default:
		// Alpenglow assigns the fractional-lamport remainder to the voter so
		// every earned lamport is preserved. Tower retains the historical
		// symmetric truncation behavior.
		if preserveLamports {
			result.StakerPortion = mulDivBPS(rewards, 10_000-commissionBPS)
			result.VoterPortion = rewards - result.StakerPortion
		} else {
			result.VoterPortion = mulDivBPS(rewards, commissionBPS)
			result.StakerPortion = mulDivBPS(rewards, 10_000-commissionBPS)
		}
		result.IsSplit = true
	}

	return result
}

func voteInflationRewardsCollector(votePubkey solana.PublicKey, voteState *sealevel.VoteStateVersions, customCollector bool) solana.PublicKey {
	if customCollector && voteState != nil && voteState.Type == sealevel.VoteStateVersionV4 {
		return voteState.V4.InflationRewardsCollector
	}
	return votePubkey
}

func calculateStakePointsAndCredits(
	pubkey solana.PublicKey,
	stakeHistory *sealevel.SysvarStakeHistory,
	delegation *sealevel.Delegation,
	voteState *sealevel.VoteStateVersions,
	newRateActivationEpoch *uint64,
	rewardedEpoch uint64,
	mode RewardCalculationMode,
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
	case sealevel.VoteStateVersionV4:
		epochCredits = voteState.V4.EpochCredits
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

	if mode.FullAlpenglow {
		latest := epochCredits[len(epochCredits)-1]
		if latest.Epoch != rewardedEpoch {
			return CalculatedStakePoints{NewCreditsObserved: creditsInStake}
		}

		newObserved := creditsInStake
		var earnedCredits uint64
		if creditsInStake < latest.PrevCredits {
			earnedCredits = latest.Credits - latest.PrevCredits
		} else if creditsInStake < latest.Credits {
			earnedCredits = latest.Credits - newObserved
		}
		newObserved = max(newObserved, latest.Credits)

		effectiveStake := delegation.StakeActivatingAndDeactivating(
			rewardedEpoch, stakeHistory, newRateActivationEpoch,
		).Effective
		if earnedCredits == 0 || effectiveStake == 0 {
			return CalculatedStakePoints{NewCreditsObserved: newObserved}
		}
		totalStake := mode.RewardEpochDelegatedStakes[delegation.VoterPubkey]
		if totalStake == 0 {
			return CalculatedStakePoints{
				NewCreditsObserved:                  newObserved,
				ForceCreditsUpdateWithSkippedReward: true,
			}
		}
		points := wide.Uint128FromUint64(earnedCredits).
			Mul(wide.Uint128FromUint64(effectiveStake)).
			Div(wide.Uint128FromUint64(totalStake))
		return CalculatedStakePoints{Points: points, NewCreditsObserved: newObserved}
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
	// Agave always schedules at least one distribution block, including when
	// there are no eligible stake rewards. The empty partition is what advances
	// EpochRewards from active to inactive on the next block; returning zero here
	// would persist an active sysvar that no distribution slot can ever finish.
	numRewardPartitions := max(uint64(1), min(unclamped, cap))

	return numRewardPartitions
}

// StreamingRewardsResult holds the results from streaming rewards calculation.
type StreamingRewardsResult struct {
	SpoolDir           string // Base directory for per-partition spool files
	SpoolSlot          uint64 // Slot for spool file naming
	TotalPoints        wide.Uint128
	TotalStakerRewards uint64
	ValidatorRewards   map[solana.PublicKey]*atomic.Uint64
	NumStakeRewards    uint64
	NumPartitions      uint64
}

// spoolWriteRequest is sent to the single-writer goroutine for spool writes.
type spoolWriteRequest struct {
	record *SpoolRecord
}

func shouldForceCreditsOnly(
	pcs CalculatedStakePoints,
	pointValueRewards, activationEpoch, rewardedEpoch, creditsObserved uint64,
	mode RewardCalculationMode,
) bool {
	return pcs.ForceCreditsUpdateWithSkippedReward ||
		pointValueRewards == 0 ||
		activationEpoch == rewardedEpoch ||
		(mode.FullAlpenglow && pcs.Points.Eq(wide.Uint128{}) && pcs.NewCreditsObserved != creditsObserved)
}

// CalculateRewardsStreaming performs a streaming calculation of stake rewards.
// Phase 1: Stream stakes to calculate total points + write points spool (single AccountsDB scan)
// Phase 2: Replay points spool to compute rewards + write temp spool (sequential file I/O only)
// Phase 3: Read temp spool, assign partitions, write per-partition spools
func CalculateRewardsStreaming(
	acctsDb *accountsdb.AccountsDb,
	slot uint64,
	stakeHistory *sealevel.SysvarStakeHistory,
	newWarmupCooldownRateEpoch *uint64,
	voteCache map[solana.PublicKey]*sealevel.VoteStateVersions,
	pointValue PointValue,
	rewardedEpoch uint64,
	blockhash [32]byte,
	slotCtx *sealevel.SlotCtx,
	f *features.Features,
	mode RewardCalculationMode,
) (*StreamingRewardsResult, error) {
	minimum := minimumStakeDelegation(slotCtx)
	spoolDir := filepath.Join(acctsDb.AcctsDir, "..")

	mlog.Log.Infof("Rewards: voteCache has %d entries, minimum stake delegation=%d", len(voteCache), minimum)

	// ==================== PHASE 1: Calculate total points + write points spool ====================
	pointsWriter, err := NewPointsSpoolWriter(spoolDir, slot)
	if err != nil {
		return nil, fmt.Errorf("creating points spool: %w", err)
	}
	pointsPath := pointsWriter.Path()

	var totalPoints wide.Uint128
	var totalPointsMu sync.Mutex
	var phase1StakeCount atomic.Int64
	var phase1BelowMinimum atomic.Int64
	var phase1NoVoteState atomic.Int64
	var phase1ZeroPoints atomic.Int64
	var phase1TotalStakeLamports atomic.Uint64

	// Channel + single-writer goroutine for points spool writes
	type pointsWriteRequest struct {
		record *PointsSpoolRecord
	}
	pointsWriteChan := make(chan pointsWriteRequest, 10000)
	var pointsWriteErr atomic.Pointer[error]
	var pointsWriterWg sync.WaitGroup
	pointsWriterWg.Add(1)

	go func() {
		defer pointsWriterWg.Done()
		for req := range pointsWriteChan {
			if pointsWriteErr.Load() != nil {
				continue
			}
			if err := pointsWriter.WriteRecord(req.record); err != nil {
				pointsWriteErr.Store(&err)
			}
		}
	}()

	_, err = global.StreamStakeAccounts(acctsDb, slot,
		func(pk solana.PublicKey, delegation *sealevel.Delegation, creditsObs uint64) {
			if delegation.StakeLamports < minimum {
				phase1BelowMinimum.Add(1)
				return
			}

			voterPk := delegation.VoterPubkey
			voteState := voteCache[voterPk]
			if voteState == nil {
				phase1NoVoteState.Add(1)
				return
			}

			delegWithCredits := *delegation
			delegWithCredits.CreditsObserved = creditsObs
			pcs := calculateStakePointsAndCredits(pk, stakeHistory, &delegWithCredits, voteState, newWarmupCooldownRateEpoch, rewardedEpoch, mode)

			zero128 := wide.Uint128FromUint64(0)
			if pcs.Points.Eq(zero128) {
				phase1ZeroPoints.Add(1)
			}

			totalPointsMu.Lock()
			totalPoints = totalPoints.Add(pcs.Points)
			totalPointsMu.Unlock()
			phase1StakeCount.Add(1)
			phase1TotalStakeLamports.Add(delegation.StakeLamports)

			// Precompute the full forceCreditsUpdate flag using the same three
			// triggers as CalculateStakeRewardsForAcct:
			forceCredits := shouldForceCreditsOnly(
				pcs, pointValue.Rewards, delegation.ActivationEpoch,
				rewardedEpoch, creditsObs, mode,
			)

			// Only write points records that Phase 2 will actually use:
			// - forceCredits records produce spool records with 0 rewards
			// - Non-zero points records produce actual rewards
			// - Zero points without forceCredits → nil reward → skip
			if pcs.Points.Eq(zero128) && !forceCredits {
				return
			}

			pointsWriteChan <- pointsWriteRequest{record: &PointsSpoolRecord{
				StakePubkey:                         pk,
				VotePubkey:                          delegation.VoterPubkey,
				Points:                              pcs.Points,
				NewCreditsObserved:                  pcs.NewCreditsObserved,
				StakeLamports:                       delegation.StakeLamports,
				ForceCreditsUpdateWithSkippedReward: forceCredits,
			}}
		})

	close(pointsWriteChan)
	pointsWriterWg.Wait()

	if err != nil {
		pointsWriter.Close()
		CleanupPointsSpoolFile(pointsPath)
		return nil, fmt.Errorf("phase 1 streaming stakes for points: %w", err)
	}

	if werr := pointsWriteErr.Load(); werr != nil {
		pointsWriter.Close()
		CleanupPointsSpoolFile(pointsPath)
		return nil, fmt.Errorf("points spool write failed: %w", *werr)
	}

	if err := pointsWriter.Close(); err != nil {
		CleanupPointsSpoolFile(pointsPath)
		return nil, fmt.Errorf("points spool close failed: %w", err)
	}

	mlog.Log.Infof("Rewards Phase 1: %d stakes (belowMin=%d, noVote=%d, zeroPoints=%d), totalStakeLamports=%d, totalPoints=%s, totalRewards=%d, pointsSpoolRecords=%d",
		phase1StakeCount.Load(), phase1BelowMinimum.Load(), phase1NoVoteState.Load(), phase1ZeroPoints.Load(),
		phase1TotalStakeLamports.Load(), totalPoints.String(), pointValue.Rewards, pointsWriter.Count())

	// ==================== PHASE 2: Replay points spool → compute rewards → write temp spool ====================
	pv := PointValue{Rewards: pointValue.Rewards, Points: totalPoints}
	var commissionVoteStates map[solana.PublicKey]*sealevel.VoteStateVersions
	if f.IsActive(features.DelayCommissionUpdates) {
		commissionVoteStates = buildDelayedCommissionVoteStateCache(voteCache, rewardedEpoch)
	}

	tempWriter, err := NewTempSpoolWriter(spoolDir, slot)
	if err != nil {
		CleanupPointsSpoolFile(pointsPath)
		return nil, fmt.Errorf("creating temp spool: %w", err)
	}
	tempPath := tempWriter.Path()

	validatorRewards := make(map[solana.PublicKey]*atomic.Uint64)

	pointsReader, err := NewPointsSpoolReader(pointsPath)
	if err != nil {
		CleanupPointsSpoolFile(pointsPath)
		tempWriter.Close()
		CleanupTempSpoolFile(tempPath)
		return nil, fmt.Errorf("opening points spool reader: %w", err)
	}

	var phase2SkippedNilReward int64
	var phase2TotalStakerRewards uint64
	zero128 := wide.Uint128FromUint64(0)

	for {
		rec, err := pointsReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			pointsReader.Close()
			CleanupPointsSpoolFile(pointsPath)
			tempWriter.Close()
			CleanupTempSpoolFile(tempPath)
			return nil, fmt.Errorf("reading points spool: %w", err)
		}

		// ForceCreditsUpdateWithSkippedReward was fully precomputed in Phase 1
		// (covers pcs.ForceCredits, pointValue.Rewards==0, activationEpoch==rewardedEpoch)
		if rec.ForceCreditsUpdateWithSkippedReward {
			// Credits update only — write spool record with 0 rewards
			if err := tempWriter.WriteRecord(&SpoolRecord{
				StakePubkey:     rec.StakePubkey,
				VotePubkey:      rec.VotePubkey,
				StakeLamports:   rec.StakeLamports,
				CreditsObserved: rec.NewCreditsObserved,
				RewardLamports:  0,
			}); err != nil {
				pointsReader.Close()
				CleanupPointsSpoolFile(pointsPath)
				tempWriter.Close()
				CleanupTempSpoolFile(tempPath)
				return nil, fmt.Errorf("temp spool write failed: %w", err)
			}
			continue
		}

		// 2. Zero points or zero totalPoints → nil reward
		if rec.Points.Eq(zero128) || pv.Points.Eq(zero128) {
			phase2SkippedNilReward++
			continue
		}

		// 3. Tower points are scaled across the inflation pool. Full
		// Alpenglow points are already the delegation's lamport reward.
		rewards128 := rec.Points
		if !mode.FullAlpenglow {
			rewards128 = rec.Points.Mul(wide.Uint128FromUint64(pv.Rewards)).Div(pv.Points)
		}
		if !rewards128.IsUint64() {
			phase2SkippedNilReward++
			continue
		}
		rewards := rewards128.Uint64()
		if rewards == 0 {
			phase2SkippedNilReward++
			continue
		}

		// 4. Commission split
		voteState := voteCache[rec.VotePubkey]
		if voteState == nil {
			phase2SkippedNilReward++
			continue
		}
		commissionVoteState := voteState
		if commissionVoteStates != nil {
			if historicalVoteState := commissionVoteStates[rec.VotePubkey]; historicalVoteState != nil {
				commissionVoteState = historicalVoteState
			}
		}
		useBasisPoints := f.IsActive(features.CommissionRateInBasisPoints)
		splitResult := voteCommissionSplit(commissionVoteState, rewards, mode.FullAlpenglow, useBasisPoints)
		if !mode.FullAlpenglow && splitResult.IsSplit && (splitResult.VoterPortion == 0 || splitResult.StakerPortion == 0) {
			phase2SkippedNilReward++
			continue
		}

		phase2TotalStakerRewards += splitResult.StakerPortion

		if err := tempWriter.WriteRecord(&SpoolRecord{
			StakePubkey:     rec.StakePubkey,
			VotePubkey:      rec.VotePubkey,
			StakeLamports:   rec.StakeLamports,
			CreditsObserved: rec.NewCreditsObserved,
			RewardLamports:  splitResult.StakerPortion,
		}); err != nil {
			pointsReader.Close()
			CleanupPointsSpoolFile(pointsPath)
			tempWriter.Close()
			CleanupTempSpoolFile(tempPath)
			return nil, fmt.Errorf("temp spool write failed: %w", err)
		}

		if splitResult.VoterPortion > 0 {
			collector := voteInflationRewardsCollector(
				rec.VotePubkey, voteState, f.IsActive(features.CustomCommissionCollector),
			)
			accumulator := validatorRewards[collector]
			if accumulator == nil {
				accumulator = &atomic.Uint64{}
				validatorRewards[collector] = accumulator
			}
			accumulator.Add(splitResult.VoterPortion)
		}
	}

	pointsReader.Close()
	CleanupPointsSpoolFile(pointsPath)

	if err := tempWriter.Close(); err != nil {
		CleanupTempSpoolFile(tempPath)
		return nil, fmt.Errorf("temp spool close failed: %w", err)
	}

	// ==================== Calculate numPartitions from ACTUAL count ====================
	actualRewardCount := uint64(tempWriter.Count())
	numPartitions := CalculateNumRewardPartitions(actualRewardCount)

	var totalVotingRewards uint64
	for _, v := range validatorRewards {
		totalVotingRewards += v.Load()
	}
	mlog.Log.Infof("Rewards Phase 2: %d records (skippedNilReward=%d), totalStakerRewards=%d, totalVotingRewards=%d, validatorCount=%d, numPartitions=%d",
		actualRewardCount, phase2SkippedNilReward,
		phase2TotalStakerRewards, totalVotingRewards, len(validatorRewards), numPartitions)

	// ==================== PHASE 3: Read temp spool, assign partitions, write per-partition spools ====================
	if err := resetPartitionedSpoolFiles(spoolDir, slot, numPartitions); err != nil {
		CleanupTempSpoolFile(tempPath)
		return nil, err
	}
	tempReader, err := NewTempSpoolReader(tempPath)
	if err != nil {
		CleanupTempSpoolFile(tempPath)
		return nil, fmt.Errorf("opening temp spool reader: %w", err)
	}

	partitionWriters := NewPartitionedSpoolWriters(spoolDir, slot, numPartitions)

	for {
		rec, err := tempReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			tempReader.Close()
			partitionWriters.Close()
			CleanupTempSpoolFile(tempPath)
			CleanupPartitionedSpoolFiles(spoolDir, slot, numPartitions)
			return nil, fmt.Errorf("reading temp spool: %w", err)
		}

		rec.PartitionIndex = uint32(CalculateRewardPartitionForPubkey(rec.StakePubkey, blockhash, numPartitions))

		if err := partitionWriters.WriteRecord(rec); err != nil {
			tempReader.Close()
			partitionWriters.Close()
			CleanupTempSpoolFile(tempPath)
			CleanupPartitionedSpoolFiles(spoolDir, slot, numPartitions)
			return nil, fmt.Errorf("partition spool write: %w", err)
		}
	}

	tempReader.Close()
	CleanupTempSpoolFile(tempPath)

	if err := partitionWriters.Close(); err != nil {
		CleanupPartitionedSpoolFiles(spoolDir, slot, numPartitions)
		return nil, fmt.Errorf("partition spool close failed: %w", err)
	}

	reportedPoints := totalPoints
	if mode.FullAlpenglow {
		// Agave records zero total_points for a full Alpenglow epoch. The
		// per-delegation values above are already final lamport rewards.
		reportedPoints = wide.Uint128{}
	}

	return &StreamingRewardsResult{
		SpoolDir:           spoolDir,
		SpoolSlot:          slot,
		TotalPoints:        reportedPoints,
		TotalStakerRewards: phase2TotalStakerRewards,
		ValidatorRewards:   validatorRewards,
		NumStakeRewards:    actualRewardCount,
		NumPartitions:      numPartitions,
	}, nil
}
