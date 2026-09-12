package replay

import (
	"errors"
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/wincode"
)

// EpochInflationState stores the per-epoch reward budget metadata used by
// Alpenglow vote rewards.
type EpochInflationState struct {
	MaxPossibleValidatorReward uint64
	SlotsPerEpoch              uint64
	Epoch                      uint64
}

type EpochInflationAccountState struct {
	Current EpochInflationState
	Prev    *EpochInflationState
}

func encodeEpochInflationState(w *wincode.Writer, state EpochInflationState) {
	w.WriteU64(state.MaxPossibleValidatorReward)
	w.WriteU64(state.SlotsPerEpoch)
	w.WriteU64(state.Epoch)
}

func encodeEpochInflationAccountState(state EpochInflationAccountState) []byte {
	w := wincode.NewWriter(49)
	encodeEpochInflationState(w, state.Current)
	if state.Prev == nil {
		w.WriteU8(0)
	} else {
		w.WriteU8(1)
		encodeEpochInflationState(w, *state.Prev)
	}
	return w.Bytes()
}

func decodeEpochInflationState(r *wincode.Reader) (EpochInflationState, error) {
	maxReward, err := r.ReadU64()
	if err != nil {
		return EpochInflationState{}, err
	}
	slotsPerEpoch, err := r.ReadU64()
	if err != nil {
		return EpochInflationState{}, err
	}
	epoch, err := r.ReadU64()
	if err != nil {
		return EpochInflationState{}, err
	}
	return EpochInflationState{MaxPossibleValidatorReward: maxReward, SlotsPerEpoch: slotsPerEpoch, Epoch: epoch}, nil
}

func decodeEpochInflationAccountState(data []byte) (EpochInflationAccountState, error) {
	r := wincode.NewReader(data)
	current, err := decodeEpochInflationState(r)
	if err != nil {
		return EpochInflationAccountState{}, fmt.Errorf("decode current epoch inflation: %w", err)
	}
	tag, err := r.ReadBytes(1)
	if err != nil {
		return EpochInflationAccountState{}, fmt.Errorf("decode previous epoch inflation tag: %w", err)
	}
	var prev *EpochInflationState
	switch tag[0] {
	case 0:
	case 1:
		state, err := decodeEpochInflationState(r)
		if err != nil {
			return EpochInflationAccountState{}, fmt.Errorf("decode previous epoch inflation: %w", err)
		}
		prev = &state
	default:
		return EpochInflationAccountState{}, fmt.Errorf("invalid previous epoch inflation tag %d", tag[0])
	}
	if err := r.EnsureEOF(); err != nil {
		return EpochInflationAccountState{}, err
	}
	return EpochInflationAccountState{Current: current, Prev: prev}, nil
}

func (s EpochInflationAccountState) epochState(epoch uint64) (EpochInflationState, bool) {
	if s.Current.Epoch == epoch {
		return s.Current, true
	}
	if s.Prev != nil && s.Prev.Epoch == epoch {
		return *s.Prev, true
	}
	return EpochInflationState{}, false
}

// partitionedRewardsBudget selects the inflation ceiling Agave records in the
// EpochRewards sysvar and uses as PointValue.rewards.  Once Alpenglow is
// active, the rewarded epoch's ceiling was fixed at that epoch's start and is
// persisted in the vote-reward metadata account.  Recomputing it from the
// capitalization at the following boundary is not equivalent: VAT burns and
// other capitalization changes have happened in the meantime.
func partitionedRewardsBudget(
	slotCtx *sealevel.SlotCtx,
	epochSchedule *sealevel.SysvarEpochSchedule,
	f *features.Features,
	rewardedEpoch, calculatedFallback uint64,
) (uint64, error) {
	migrationSlot, alpenglowActive := f.ActivationSlot(features.Alpenglow)
	if !alpenglowActive || rewardedEpoch < epochSchedule.GetEpoch(migrationSlot) {
		return calculatedFallback, nil
	}

	state, err := loadEpochInflationAccountStateForReplay(slotCtx)
	if err != nil {
		return 0, fmt.Errorf("load recorded inflation budget for rewarded epoch %d: %w", rewardedEpoch, err)
	}
	inflation, ok := state.epochState(rewardedEpoch)
	if !ok {
		return 0, fmt.Errorf(
			"vote reward account has no inflation budget for rewarded epoch %d (current=%d)",
			rewardedEpoch, state.Current.Epoch,
		)
	}
	return inflation.MaxPossibleValidatorReward, nil
}

func calculateEpochInflationRewards(
	epochSchedule *sealevel.SysvarEpochSchedule,
	inflation *rewards.Inflation,
	capitalization, epoch uint64,
	slotsPerYear float64,
	f *features.Features,
) uint64 {
	slotInYear := rewards.SlotInYearForInflation(epochSchedule, slotsPerYear, epoch, f)
	validatorRate := inflation.Validator(slotInYear)
	epochDurationInYears := float64(epochSchedule.SlotsInEpoch(epoch)) / slotsPerYear
	return uint64(validatorRate * float64(capitalization) * epochDurationInYears)
}

func newEpochInflationState(
	epochSchedule *sealevel.SysvarEpochSchedule,
	inflation *rewards.Inflation,
	capitalization, additionalValidatorRewards, epoch uint64,
	slotsPerYear float64,
	f *features.Features,
) EpochInflationState {
	rewardBudget := capitalization
	if additionalValidatorRewards > math.MaxUint64-capitalization {
		rewardBudget = math.MaxUint64
	} else {
		rewardBudget += additionalValidatorRewards
	}
	return EpochInflationState{
		MaxPossibleValidatorReward: calculateEpochInflationRewards(
			epochSchedule, inflation, rewardBudget, epoch, slotsPerYear, f,
		),
		SlotsPerEpoch: epochSchedule.SlotsPerEpoch,
		Epoch:         epoch,
	}
}

// stageEpochInflationAccount mirrors Agave's epoch-boundary update of the
// vote-reward metadata PDA. This account is consensus state: its rent reserve
// changes capitalization, and both its old and new versions participate in the
// boundary bank hash/LtHash calculation.
func stageEpochInflationAccount(
	acctsDb *accountsdb.AccountsDb,
	readSlot, storeSlot uint64,
	replayCtx *ReplayCtx,
	epochSchedule *sealevel.SysvarEpochSchedule,
	f *features.Features,
	newEpoch, epochStartCapitalization, additionalValidatorRewards uint64,
) (*accounts.Account, *accounts.Account, error) {
	key := VoteRewardAccountAddr()
	parent, err := acctsDb.GetAccount(readSlot, key)
	if err != nil {
		if !errors.Is(err, accountsdb.ErrNoAccount) {
			return nil, nil, fmt.Errorf("load vote reward account at slot %d: %w", readSlot, err)
		}
		parent = missingParentAccount(key)
	} else {
		parent = parent.Clone()
	}

	var prev *EpochInflationState
	if len(parent.Data) != 0 {
		existing, err := decodeEpochInflationAccountState(parent.Data)
		if err != nil {
			return nil, nil, fmt.Errorf("decode vote reward account at slot %d: %w", readSlot, err)
		}
		previousCurrent := existing.Current
		prev = &previousCurrent
	}

	current := newEpochInflationState(
		epochSchedule, &replayCtx.Inflation, epochStartCapitalization,
		additionalValidatorRewards, newEpoch, replayCtx.SlotsPerYear, f,
	)
	data := encodeEpochInflationAccountState(EpochInflationAccountState{
		Current: current,
		Prev:    prev,
	})

	rnt := sealevel.SysvarCache.Rent.Sysvar
	if rnt == nil {
		return nil, nil, fmt.Errorf("rent sysvar unavailable while storing vote reward account")
	}
	lamports := rnt.MinimumBalance(uint64(len(data)))
	if lamports == 0 {
		lamports = 1
	}

	updated := &accounts.Account{
		Key:        key,
		Lamports:   lamports,
		Data:       data,
		Owner:      a.SystemProgramAddr,
		Executable: false,
		RentEpoch:  0,
	}
	if err := acctsDb.StoreAccounts([]*accounts.Account{updated}, storeSlot, nil); err != nil {
		return nil, nil, fmt.Errorf("store vote reward account at slot %d: %w", storeSlot, err)
	}
	if updated.Lamports >= parent.Lamports {
		replayCtx.Capitalization = safemath.SaturatingAddU64(replayCtx.Capitalization, updated.Lamports-parent.Lamports)
	} else {
		replayCtx.Capitalization = safemath.SaturatingSubU64(replayCtx.Capitalization, parent.Lamports-updated.Lamports)
	}
	return updated.Clone(), parent, nil
}

func loadEpochInflationAccountStateForReplay(slotCtx *sealevel.SlotCtx) (EpochInflationAccountState, error) {
	if slotCtx == nil {
		return EpochInflationAccountState{}, fmt.Errorf("missing slot context")
	}

	// The first executed bank after an epoch boundary stages the new inflation
	// account in its current-bank accounts. This is especially important when
	// the first eight slots are skipped: that same bank's reward certificate
	// already targets the new epoch, while the parent still contains only the
	// previous epoch's inflation state.
	if slotCtx.Accounts != nil {
		if acct, err := slotCtx.GetAccount(VoteRewardAccountAddr()); err == nil && acct != nil {
			return decodeEpochInflationAccountFromAcct(acct, slotCtx.Slot)
		}
	}

	acct, err := slotCtx.GetAccountFromAccountsDb(VoteRewardAccountAddr())
	if err != nil {
		return EpochInflationAccountState{}, fmt.Errorf("load vote reward account at parent slot %d: %w", slotCtx.ParentSlot, err)
	}
	return decodeEpochInflationAccountFromAcct(acct, slotCtx.ParentSlot)
}

func decodeEpochInflationAccountFromAcct(acct *accounts.Account, slot uint64) (EpochInflationAccountState, error) {
	if acct == nil || len(acct.Data) == 0 {
		return EpochInflationAccountState{}, fmt.Errorf("vote reward account missing at slot %d", slot)
	}
	state, err := decodeEpochInflationAccountState(acct.Data)
	if err != nil {
		return EpochInflationAccountState{}, fmt.Errorf("decode vote reward account at slot %d: %w", slot, err)
	}
	return state, nil
}
