package sealevel

import (
	"bytes"
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/features"

	//"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/wide"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

type Authorized struct {
	Staker     solana.PublicKey
	Withdrawer solana.PublicKey
}

type StakeLockup struct {
	UnixTimeStamp uint64
	Epoch         uint64
	Custodian     solana.PublicKey
}

type Meta struct {
	RentExemptReserve uint64
	Authorized        Authorized
	Lockup            StakeLockup
}

type Delegation struct {
	VoterPubkey        solana.PublicKey
	StakeLamports      uint64
	ActivationEpoch    uint64
	DeactivationEpoch  uint64
	WarmupCooldownRate float64
	CreditsObserved    uint64
}

var (
	StakeFlagsMustFullyActivateBeforeDeactivationIsPermitted = StakeFlags{Bits: 1}
)

type StakeFlags struct {
	Bits byte
}

type Stake struct {
	Delegation      Delegation
	CreditsObserved uint64
}

type StakeConfig struct {
	WarmupCooldownRate float64
	SlashPenalty       byte
}

const (
	StakeStateV2StatusUninitialized = iota
	StakeStateV2StatusInitialized
	StakeStateV2StatusStake
	StakeStateV2StatusRewardsPool
)

const (
	StakeAuthorizeStaker = iota
	StakeAuthorizeWithdrawer
)

type StakeStateV2Initialized struct {
	Meta Meta
}

type StakeStateV2Stake struct {
	Meta       Meta
	Stake      Stake
	StakeFlags StakeFlags
}

type StakeStateV2 struct {
	Status      uint32
	Initialized StakeStateV2Initialized
	Stake       StakeStateV2Stake
}

const (
	MergeKindStatusInactive = iota
	MergeKindStatusActivationEpoch
	MergeKindStatusFullyActive
)

type MergeKindInactive struct {
	Meta          Meta
	StakeLamports uint64
	StakeFlags    StakeFlags
}

type MergeKindActivationEpoch struct {
	Meta       Meta
	Stake      Stake
	StakeFlags StakeFlags
}

type MergeKindFullyActive struct {
	Meta  Meta
	Stake Stake
}

type MergeKind struct {
	Status          uint64
	Inactive        MergeKindInactive
	ActivationEpoch MergeKindActivationEpoch
	FullyActive     MergeKindFullyActive
}

func (authorized *Authorized) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	pk, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(authorized.Staker[:], pk)

	pk, err = decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(authorized.Withdrawer[:], pk)
	return nil
}

func (authorized *Authorized) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error
	err = encoder.WriteBytes(authorized.Staker[:], false)
	if err != nil {
		return err
	}

	err = encoder.WriteBytes(authorized.Withdrawer[:], false)
	return err
}

func (authorized *Authorized) Check(signers []solana.PublicKey, stakeAuthorize uint32) error {
	switch stakeAuthorize {
	case StakeAuthorizeStaker:
		{
			err := verifySigner(authorized.Staker, signers)
			if err != nil {
				return InstrErrMissingRequiredSignature
			} else {
				return nil
			}
		}

	case StakeAuthorizeWithdrawer:
		{
			err := verifySigner(authorized.Withdrawer, signers)
			if err != nil {
				return InstrErrMissingRequiredSignature
			} else {
				return nil
			}
		}

	default:
		{
			panic("shouldn't be possible")
		}
	}

}

func (authorized *Authorized) Authorize(signers []solana.PublicKey, newAuthorized solana.PublicKey, stakeAuthorize uint32, lockup StakeLockup, clock SysvarClock, custodian *solana.PublicKey) error {

	switch stakeAuthorize {
	case StakeAuthorizeStaker:
		{
			err1 := verifySigner(authorized.Staker, signers)
			err2 := verifySigner(authorized.Withdrawer, signers)

			if err1 != nil && err2 != nil {
				return InstrErrMissingRequiredSignature
			}

			authorized.Staker = newAuthorized
		}

	case StakeAuthorizeWithdrawer:
		{
			if lockup.IsInForce(clock, nil) {
				if custodian == nil {
					return StakeErrCustodianMissing
				} else {
					err := verifySigner(*custodian, signers)
					if err != nil {
						return StakeErrCustodianSignatureMissing
					}

					if lockup.IsInForce(clock, custodian) {
						return StakeErrLockupInForce
					}
				}
			}

			err := authorized.Check(signers, stakeAuthorize)
			if err != nil {
				return err
			}

			authorized.Withdrawer = newAuthorized
		}

	default:
		{
			panic("shouldn't be possible")
		}
	}

	return nil
}

func (lockup *StakeLockup) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	lockup.UnixTimeStamp, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	lockup.Epoch, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	pk, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(lockup.Custodian[:], pk)

	return nil
}

func (lockup *StakeLockup) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error
	err = encoder.WriteUint64(lockup.UnixTimeStamp, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(lockup.Epoch, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteBytes(lockup.Custodian[:], false)
	return err
}

func (lockup *StakeLockup) IsInForce(clock SysvarClock, custodian *solana.PublicKey) bool {
	if custodian != nil && *custodian == lockup.Custodian {
		return false
	}

	return lockup.UnixTimeStamp > uint64(clock.UnixTimestamp) || lockup.Epoch > clock.Epoch
}

func (meta *Meta) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	meta.RentExemptReserve, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	err = meta.Authorized.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	err = meta.Lockup.UnmarshalWithDecoder(decoder)
	return err
}

func (meta *Meta) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error
	err = encoder.WriteUint64(meta.RentExemptReserve, bin.LE)
	if err != nil {
		return err
	}

	err = meta.Authorized.MarshalWithEncoder(encoder)
	if err != nil {
		return err
	}

	err = meta.Lockup.MarshalWithEncoder(encoder)
	return err
}

func (meta *Meta) SetLockup(lockup StakeInstrSetLockup, signers []solana.PublicKey, clock SysvarClock) error {
	if meta.Lockup.IsInForce(clock, nil) {
		err := verifySigner(meta.Lockup.Custodian, signers)
		if err != nil {
			return err
		}
	} else if err := verifySigner(meta.Authorized.Withdrawer, signers); err != nil {
		return err
	}

	if lockup.UnixTimestamp != nil {
		meta.Lockup.UnixTimeStamp = *lockup.UnixTimestamp
	}

	if lockup.Epoch != nil {
		meta.Lockup.Epoch = *lockup.Epoch
	}

	if lockup.Custodian != nil {
		meta.Lockup.Custodian = *lockup.Custodian
	}

	return nil
}

func (delegation *Delegation) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	voterPubkey, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(delegation.VoterPubkey[:], voterPubkey)

	delegation.StakeLamports, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	delegation.ActivationEpoch, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	delegation.DeactivationEpoch, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	delegation.WarmupCooldownRate, err = decoder.ReadFloat64(bin.LE)
	return err
}

func (delegation *Delegation) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteBytes(delegation.VoterPubkey[:], false)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(delegation.StakeLamports, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(delegation.ActivationEpoch, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(delegation.DeactivationEpoch, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteFloat64(delegation.WarmupCooldownRate, bin.LE)
	return err
}

func (delegation *Delegation) Stake(epoch uint64, stakeHistory *SysvarStakeHistory, newRateActivationEpoch *uint64) uint64 {
	return delegation.StakeActivatingAndDeactivating(epoch, stakeHistory, newRateActivationEpoch).Effective
}

func (delegation *Delegation) IsBootstrap() bool {
	return delegation.ActivationEpoch == math.MaxUint64
}

func (delegation *Delegation) StakeAndActivating(targetEpoch uint64, stakeHistory *SysvarStakeHistory, newRateActivationEpoch *uint64) (uint64, uint64) {
	delegatedStake := delegation.StakeLamports

	if delegation.IsBootstrap() {
		return delegatedStake, 0
	} else if delegation.ActivationEpoch == delegation.DeactivationEpoch {
		return 0, 0
	} else if targetEpoch == delegation.ActivationEpoch {
		return 0, delegatedStake
	} else if targetEpoch < delegation.ActivationEpoch {
		return 0, 0
	} else if pcs := stakeHistory.Get(delegation.ActivationEpoch); pcs != nil {
		prevEpoch := delegation.ActivationEpoch
		currentEpoch := uint64(0)
		currentEffectiveStake := uint64(0)
		prevClusterStake := pcs

		for {
			currentEpoch = prevEpoch + 1

			if prevClusterStake.Activating == 0 {
				break
			}

			remainingActivatingStake := delegatedStake - currentEffectiveStake
			weight := float64(remainingActivatingStake) / float64(prevClusterStake.Activating)
			warmupCooldownRate := warmupCooldownRate(currentEpoch, newRateActivationEpoch)

			newlyEffectiveClusterStake := float64(prevClusterStake.Effective) * warmupCooldownRate
			newlyEffectiveStake := max(uint64(weight*newlyEffectiveClusterStake), 1)

			currentEffectiveStake += newlyEffectiveStake
			if currentEffectiveStake >= delegatedStake {
				currentEffectiveStake = delegatedStake
				break
			}

			if currentEpoch >= targetEpoch || currentEpoch >= delegation.DeactivationEpoch {
				break
			}

			if currentClusterStake := stakeHistory.Get(currentEpoch); currentClusterStake != nil {
				prevEpoch = currentEpoch
				prevClusterStake = currentClusterStake
			} else {
				break
			}
		}
		return currentEffectiveStake, (delegatedStake - currentEffectiveStake)
	} else {
		return delegatedStake, 0
	}
}

func (delegation *Delegation) StakeActivatingAndDeactivating(targetEpoch uint64, stakeHistory *SysvarStakeHistory, newRateActivationEpoch *uint64) StakeHistoryEntry {
	effectiveStake, activatingStake := delegation.StakeAndActivating(targetEpoch, stakeHistory, newRateActivationEpoch)

	if targetEpoch < delegation.DeactivationEpoch {
		if activatingStake == 0 {
			return StakeHistoryEntry{Effective: effectiveStake}
		} else {
			return StakeHistoryEntry{Effective: effectiveStake, Activating: activatingStake}
		}
	} else if targetEpoch == delegation.DeactivationEpoch {
		return StakeHistoryEntry{Effective: effectiveStake, Deactivating: effectiveStake}
	} else if prevClusterStake := stakeHistory.Get(delegation.DeactivationEpoch); prevClusterStake != nil {
		prevEpoch := delegation.DeactivationEpoch
		currentEpoch := uint64(0)
		currentEffectiveStake := effectiveStake

		for {
			currentEpoch = prevEpoch + 1

			if prevClusterStake.Deactivating == 0 {
				break
			}

			weight := float64(currentEffectiveStake) / float64(prevClusterStake.Deactivating)
			warmupCooldownRate := warmupCooldownRate(currentEpoch, newRateActivationEpoch)

			newlyNotEffectiveClusterStake := float64(prevClusterStake.Effective) * warmupCooldownRate
			newlyNotEffectiveStake := max(uint64(weight*newlyNotEffectiveClusterStake), 1)

			currentEffectiveStake = safemath.SaturatingSubU64(currentEffectiveStake, newlyNotEffectiveStake)
			if currentEffectiveStake == 0 {
				break
			}

			if currentEpoch >= targetEpoch {
				break
			}

			if currentClusterStake := stakeHistory.Get(currentEpoch); currentClusterStake != nil {
				prevEpoch = currentEpoch
				prevClusterStake = currentClusterStake
			} else {
				break
			}
		}
		return StakeHistoryEntry{Effective: currentEffectiveStake, Deactivating: currentEffectiveStake}
	} else {
		return StakeHistoryEntry{}
	}
}

func (stakeFlags *StakeFlags) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	stakeFlags.Bits, err = decoder.ReadByte()
	return err
}

func (stakeFlags *StakeFlags) MarshalWithEncoder(encoder *bin.Encoder) error {
	return encoder.WriteByte(stakeFlags.Bits)
}

func (stakeFlags *StakeFlags) Union(other StakeFlags) StakeFlags {
	return StakeFlags{Bits: stakeFlags.Bits | other.Bits}
}

func (stakeFlags *StakeFlags) Contains(other StakeFlags) bool {
	return (stakeFlags.Bits & other.Bits) != 0
}

func (stakeFlags *StakeFlags) Remove(other StakeFlags) {
	stakeFlags.Bits &= ^other.Bits
}

func (initialized *StakeStateV2Initialized) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	err := initialized.Meta.UnmarshalWithDecoder(decoder)
	return err
}

func (initialized *StakeStateV2Initialized) MarshalWithEncoder(encoder *bin.Encoder) error {
	return initialized.Meta.MarshalWithEncoder(encoder)
}

func (stake *Stake) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	err := stake.Delegation.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	stake.CreditsObserved, err = decoder.ReadUint64(bin.LE)
	return err
}

func (stake *Stake) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error
	err = stake.Delegation.MarshalWithEncoder(encoder)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(stake.CreditsObserved, bin.LE)
	return err
}

func (stake *Stake) Stake(epoch uint64, stakeHistory *SysvarStakeHistory, newRateActivationEpoch *uint64) uint64 {
	return stake.Delegation.Stake(epoch, stakeHistory, newRateActivationEpoch)
}

func (stake *Stake) Split(remainingStakeDelta uint64, splitStakeAmount uint64) (Stake, error) {
	if remainingStakeDelta > stake.Delegation.StakeLamports {
		return Stake{}, StakeErrInsufficientStake
	}
	stake.Delegation.StakeLamports -= remainingStakeDelta

	stakeObj := *stake
	newStake := stakeObj
	newStake.Delegation.StakeLamports = splitStakeAmount
	return newStake, nil
}

func (stake *Stake) StakeWeightCreditsObserved(absorbedLamports uint64, absorbedCreditsObserved uint64) (uint64, error) {
	if stake.CreditsObserved == absorbedCreditsObserved {
		return stake.CreditsObserved, nil
	} else {
		totalStake, err := safemath.CheckedAddU64(stake.Delegation.StakeLamports, absorbedLamports)
		if err != nil {
			return 0, err
		}
		totalStakeU128 := wide.Uint128FromUint64(totalStake)

		stakeWeightedCredits, err := safemath.CheckedMulU128(wide.Uint128FromUint64(stake.CreditsObserved), wide.Uint128FromUint64(stake.Delegation.StakeLamports))
		if err != nil {
			return 0, err
		}

		absorbedWeightedCredits, err := safemath.CheckedMulU128(wide.Uint128FromUint64(absorbedCreditsObserved), wide.Uint128FromUint64(absorbedLamports))
		if err != nil {
			return 0, err
		}

		x, err := safemath.CheckedAddU128(stakeWeightedCredits, absorbedWeightedCredits)
		if err != nil {
			return 0, err
		}

		x, err = safemath.CheckedAddU128(x, totalStakeU128)
		if err != nil {
			return 0, err
		}

		totalWeightedCredits, err := safemath.CheckedAddU128(x, wide.Uint128FromUint64(1))
		if err != nil {
			return 0, err
		}

		y, err := safemath.CheckedDivU128(totalWeightedCredits, totalStakeU128)
		if err != nil {
			return 0, err
		}

		return y.Uint64(), nil
	}
}

func (stake *Stake) MergeDelegationStakeAndCreditsObserved(absorbedLamports uint64, absorbedCreditsObserved uint64) error {
	var err error
	stake.CreditsObserved, err = stake.StakeWeightCreditsObserved(absorbedLamports, absorbedCreditsObserved)
	if err != nil {
		return InstrErrArithmeticOverflow
	}
	stake.Delegation.StakeLamports, err = safemath.CheckedAddU64(stake.Delegation.StakeLamports, absorbedLamports)
	if err != nil {
		return InstrErrInsufficientFunds
	}
	return nil
}

func (stake *Stake) Deactivate(epoch uint64) error {
	if stake.Delegation.DeactivationEpoch != math.MaxUint64 {
		return StakeErrAlreadyDeactivated
	} else {
		stake.Delegation.DeactivationEpoch = epoch
		return nil
	}
}

func (stake *StakeStateV2Stake) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	err := stake.Meta.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	err = stake.Stake.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	err = stake.StakeFlags.UnmarshalWithDecoder(decoder)
	return err
}

func (stake *StakeStateV2Stake) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = stake.Meta.MarshalWithEncoder(encoder)
	if err != nil {
		return err
	}

	err = stake.Stake.MarshalWithEncoder(encoder)
	if err != nil {
		return err
	}

	err = stake.StakeFlags.MarshalWithEncoder(encoder)
	return err
}

func (state *StakeStateV2) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	state.Status, err = decoder.ReadUint32(bin.LE)
	if err != nil {
		return err
	}

	switch state.Status {
	case StakeStateV2StatusUninitialized:
		{
			// nothing to deserialize
		}

	case StakeStateV2StatusInitialized:
		{
			err = state.Initialized.UnmarshalWithDecoder(decoder)
		}

	case StakeStateV2StatusStake:
		{
			err = state.Stake.UnmarshalWithDecoder(decoder)
		}

	case StakeStateV2StatusRewardsPool:
		{
			// nothing to deserialize
		}

	default:
		{
			err = InstrErrInvalidInstructionData
		}
	}

	return err
}

func (state *StakeStateV2) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint32(state.Status, bin.LE)
	if err != nil {
		return err
	}

	switch state.Status {
	case StakeStateV2StatusUninitialized:
		{
			// nothing to serialize up
		}

	case StakeStateV2StatusInitialized:
		{
			err = state.Initialized.MarshalWithEncoder(encoder)
		}

	case StakeStateV2StatusStake:
		{
			err = state.Stake.MarshalWithEncoder(encoder)
		}

	case StakeStateV2StatusRewardsPool:
		{
			// nothing to serialize up
		}

	default:
		{
			panic("attempting to serialize up invalid stake state")
		}
	}

	return err
}

func (config *StakeConfig) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	config.WarmupCooldownRate, err = decoder.ReadFloat64(bin.LE)
	if err != nil {
		return err
	}

	config.SlashPenalty, err = decoder.ReadByte()
	return err
}

func UnmarshalStakeState(data []byte) (*StakeStateV2, error) {
	var state StakeStateV2
	decoder := bin.NewBinDecoder(data)

	err := state.UnmarshalWithDecoder(decoder)
	if err != nil {
		return nil, InstrErrInvalidAccountData
	} else {
		return &state, nil
	}
}

func MarshalStakeStake(state *StakeStateV2) ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buffer)

	err := state.MarshalWithEncoder(encoder)
	if err != nil {
		return nil, err
	} else {
		return buffer.Bytes(), nil
	}
}

// fixedSliceWriter implements io.Writer over a fixed-size byte slice,
// avoiding allocation during serialization.
type fixedSliceWriter struct {
	buf []byte
	pos int
}

func (w *fixedSliceWriter) Write(p []byte) (int, error) {
	if w.pos+len(p) > len(w.buf) {
		return 0, fmt.Errorf("write exceeds buffer: pos=%d, write=%d, cap=%d", w.pos, len(p), len(w.buf))
	}
	copy(w.buf[w.pos:], p)
	w.pos += len(p)
	return len(p), nil
}

// MarshalStakeStakeInto writes the stake state directly into dst, avoiding allocation.
// dst should be at least StakeStateV2Size (200) bytes for valid stake accounts.
func MarshalStakeStakeInto(state *StakeStateV2, dst []byte) error {
	writer := &fixedSliceWriter{buf: dst, pos: 0}
	encoder := bin.NewBinEncoder(writer)

	return state.MarshalWithEncoder(encoder)
}

func setStakeAccountState(acct *BorrowedAccount, stakeState *StakeStateV2, f features.Features) error {
	stakeStateBytes, err := MarshalStakeStake(stakeState)
	if err != nil {
		return err
	}

	err = acct.SetState(f, stakeStateBytes)
	return err
}

func NewWarmupCooldownRateEpoch(execCtx *ExecutionCtx) (*uint64, error) {
	f := execCtx.Features
	slot, existed := f.ActivationSlot(features.ReduceStakeWarmupCooldown)
	if !existed {
		return nil, nil
	}

	epochSchedule, err := ReadEpochScheduleSysvar(execCtx)
	if err != nil {
		return nil, err
	}

	epoch := epochSchedule.GetEpoch(slot)
	return &epoch, nil
}

func NewWarmupCooldownRateEpochWithSlotCtx(slotCtx *SlotCtx, epochSchedule *SysvarEpochSchedule) *uint64 {
	slot, existed := slotCtx.Features.ActivationSlot(features.ReduceStakeWarmupCooldown)
	if !existed {
		return nil
	}

	epoch := epochSchedule.GetEpoch(slot)
	return &epoch
}

func modifyStakeForRedelegation(execCtx *ExecutionCtx, stake *Stake, stakeLamports uint64, voterPubkey solana.PublicKey, voteState *VoteState, clock SysvarClock, stakeHistory SysvarStakeHistory) error {
	newRateActivationEpoch, err := NewWarmupCooldownRateEpoch(execCtx)
	if err != nil {
		return err
	}

	if stake.Stake(clock.Epoch, &stakeHistory, newRateActivationEpoch) != 0 {
		var stakeLamportsOk bool
		if execCtx.Features.IsActive(features.StakeRedelegateInstruction) {
			stakeLamportsOk = stakeLamports >= stake.Delegation.StakeLamports
		} else {
			stakeLamportsOk = true
		}
		if stake.Delegation.VoterPubkey == voterPubkey &&
			clock.Epoch == stake.Delegation.DeactivationEpoch && stakeLamportsOk {
			stake.Delegation.DeactivationEpoch = math.MaxUint64
			return nil
		} else {
			return StakeErrTooSoonToRedelegate
		}
	}

	stake.Delegation.StakeLamports = stakeLamports
	stake.Delegation.ActivationEpoch = clock.Epoch
	stake.Delegation.DeactivationEpoch = math.MaxUint64
	stake.Delegation.VoterPubkey = voterPubkey
	stake.CreditsObserved = voteState.Credits()
	return nil
}

func getMergeKindIfMergeable(execCtx *ExecutionCtx, stakeState *StakeStateV2, stakeLamports uint64, clock SysvarClock, stakeHistory SysvarStakeHistory) (*MergeKind, error) {
	switch stakeState.Status {
	case StakeStateV2StatusStake:
		{
			epoch, err := NewWarmupCooldownRateEpoch(execCtx)
			if err != nil {
				return nil, err
			}

			status := stakeState.Stake.Stake.Delegation.StakeActivatingAndDeactivating(clock.Epoch, &stakeHistory, epoch)
			if status.Effective == 0 && status.Activating == 0 && status.Deactivating == 0 {
				return &MergeKind{Status: MergeKindStatusInactive, Inactive: MergeKindInactive{Meta: stakeState.Stake.Meta, StakeLamports: stakeLamports, StakeFlags: stakeState.Stake.StakeFlags}}, nil
			} else if status.Effective == 0 {
				return &MergeKind{Status: MergeKindStatusActivationEpoch, ActivationEpoch: MergeKindActivationEpoch{Meta: stakeState.Stake.Meta, Stake: stakeState.Stake.Stake, StakeFlags: stakeState.Stake.StakeFlags}}, nil
			} else if status.Activating == 0 && status.Deactivating == 0 {
				return &MergeKind{Status: MergeKindStatusFullyActive, FullyActive: MergeKindFullyActive{Meta: stakeState.Stake.Meta, Stake: stakeState.Stake.Stake}}, nil
			} else {
				return nil, StakeErrMergeTransientStake
			}
		}

	case StakeStateV2StatusInitialized:
		{
			return &MergeKind{Status: MergeKindStatusInactive, Inactive: MergeKindInactive{Meta: stakeState.Initialized.Meta, StakeLamports: stakeLamports}}, nil
		}

	default:
		{
			return nil, InstrErrInvalidAccountData
		}
	}
}

func metasCanMerge(stakeMeta *Meta, srcMeta *Meta, clock SysvarClock) error {
	canMergeLockups := stakeMeta.Lockup == srcMeta.Lockup ||
		(!stakeMeta.Lockup.IsInForce(clock, nil) && !srcMeta.Lockup.IsInForce(clock, nil))

	if stakeMeta.Authorized == srcMeta.Authorized && canMergeLockups {
		return nil
	} else {
		//mlog.Log.Debugf("unable to merge due to metadata mismatch")
		return StakeErrMergeMismatch
	}

}

func (mergeKind *MergeKind) Meta() *Meta {
	switch mergeKind.Status {
	case MergeKindStatusInactive:
		{
			return &mergeKind.Inactive.Meta
		}
	case MergeKindStatusActivationEpoch:
		{
			return &mergeKind.ActivationEpoch.Meta
		}

	case MergeKindStatusFullyActive:
		{
			return &mergeKind.FullyActive.Meta
		}
	default:
		{
			panic("MergeKind in invalid state - shouldn't be possible")
		}
	}
}

func (mergeKind *MergeKind) ActiveStake() *Stake {
	switch mergeKind.Status {
	case MergeKindStatusInactive:
		{
			return nil
		}

	case MergeKindStatusActivationEpoch:
		{
			return &mergeKind.ActivationEpoch.Stake
		}

	case MergeKindStatusFullyActive:
		{
			return &mergeKind.FullyActive.Stake
		}
	default:
		{
			panic("MergeKind in invalid state - shouldn't be possible")
		}
	}
}

func activeDelegationsCanMerge(stake *Delegation, src *Delegation) error {
	if stake.VoterPubkey != src.VoterPubkey {
		//mlog.Log.Debugf("unable to merge due to voter mismatch")
		return StakeErrMergeMismatch
	} else if stake.DeactivationEpoch == math.MaxUint64 && src.DeactivationEpoch == math.MaxUint64 {
		return nil
	} else {
		//mlog.Log.Debugf("unable to merge due to stake deactivation")
		return StakeErrMergeMismatch
	}
}

func (mergeKind *MergeKind) Merge(execCtx *ExecutionCtx, src *MergeKind, clock SysvarClock) (*StakeStateV2, error) {
	err := metasCanMerge(mergeKind.Meta(), src.Meta(), clock)
	if err != nil {
		return nil, err
	}

	stakeStake := mergeKind.ActiveStake()
	srcStake := src.ActiveStake()

	if stakeStake != nil && srcStake != nil {
		err = activeDelegationsCanMerge(&stakeStake.Delegation, &srcStake.Delegation)
		if err != nil {
			return nil, err
		}
	}

	if mergeKind.Status == MergeKindStatusInactive && src.Status == MergeKindStatusInactive {
		return nil, nil
	} else if mergeKind.Status == MergeKindStatusInactive && src.Status == MergeKindStatusActivationEpoch {
		return nil, nil
	} else if mergeKind.Status == MergeKindStatusActivationEpoch && src.Status == MergeKindStatusInactive {
		mergeKind.ActivationEpoch.Stake.Delegation.StakeLamports, err = safemath.CheckedAddU64(mergeKind.ActivationEpoch.Stake.Delegation.StakeLamports, src.Inactive.StakeLamports)
		if err != nil {
			return nil, InstrErrInsufficientFunds
		}
		return &StakeStateV2{Status: StakeStateV2StatusStake, Stake: StakeStateV2Stake{Meta: mergeKind.ActivationEpoch.Meta, Stake: mergeKind.ActivationEpoch.Stake, StakeFlags: mergeKind.ActivationEpoch.StakeFlags.Union(src.Inactive.StakeFlags)}}, nil
	} else if mergeKind.Status == MergeKindStatusActivationEpoch && src.Status == MergeKindStatusActivationEpoch {
		srcLamports, err := safemath.CheckedAddU64(src.ActivationEpoch.Meta.RentExemptReserve, src.ActivationEpoch.Stake.Delegation.StakeLamports)
		if err != nil {
			return nil, InstrErrInsufficientFunds
		}

		err = mergeKind.ActivationEpoch.Stake.MergeDelegationStakeAndCreditsObserved(srcLamports, src.ActivationEpoch.Stake.CreditsObserved)
		if err != nil {
			return nil, err
		}

		return &StakeStateV2{Status: StakeStateV2StatusStake, Stake: StakeStateV2Stake{Meta: mergeKind.ActivationEpoch.Meta, Stake: mergeKind.ActivationEpoch.Stake, StakeFlags: mergeKind.ActivationEpoch.StakeFlags.Union(src.ActivationEpoch.StakeFlags)}}, nil
	} else if mergeKind.Status == MergeKindStatusFullyActive && src.Status == MergeKindStatusFullyActive {
		err = mergeKind.FullyActive.Stake.MergeDelegationStakeAndCreditsObserved(src.FullyActive.Stake.Delegation.StakeLamports, src.FullyActive.Stake.CreditsObserved)
		if err != nil {
			return nil, err
		}

		return &StakeStateV2{Status: StakeStateV2StatusStake, Stake: StakeStateV2Stake{Meta: mergeKind.FullyActive.Meta, Stake: mergeKind.FullyActive.Stake}}, nil
	} else {
		return nil, StakeErrMergeMismatch
	}
}

func deactivateStake(execCtx *ExecutionCtx, stake *Stake, stakeFlags *StakeFlags, epoch uint64) error {
	if execCtx.Features.IsActive(features.StakeRedelegateInstruction) {
		if stakeFlags.Contains(StakeFlagsMustFullyActivateBeforeDeactivationIsPermitted) {
			stakeHistory, err := ReadStakeHistorySysvar(execCtx)
			if err != nil {
				return err
			}

			newRateActivationEpoch, err := NewWarmupCooldownRateEpoch(execCtx)
			if err != nil {
				return err
			}
			status := stake.Delegation.StakeActivatingAndDeactivating(epoch, &stakeHistory, newRateActivationEpoch)
			if status.Activating != 0 {
				return StakeErrRedelegatedStakeMustFullyActivateBeforeDeactivationIsPermitted
			} else {
				err = stake.Deactivate(epoch)
				if err != nil {
					return err
				}
				stakeFlags.Remove(StakeFlagsMustFullyActivateBeforeDeactivationIsPermitted)
				return nil
			}
		}
	}
	return stake.Deactivate(epoch)
}

func getStakeStatus(execCtx *ExecutionCtx, stake *Stake, clock SysvarClock) (StakeHistoryEntry, error) {
	stakeHistory, err := ReadStakeHistorySysvar(execCtx)
	if err != nil {
		return StakeHistoryEntry{}, err
	}

	newRateActivationEpoch, err := NewWarmupCooldownRateEpoch(execCtx)
	if err != nil {
		return StakeHistoryEntry{}, err
	}
	return stake.Delegation.StakeActivatingAndDeactivating(clock.Epoch, &stakeHistory, newRateActivationEpoch), nil
}
