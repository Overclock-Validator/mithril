package replay

import (
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/wincode"
)

// EpochInflationState stores per-epoch validator inflation metadata for Alpenglow vote rewards.
type EpochInflationState struct {
	MaxPossibleValidatorReward uint64
	SlotsPerEpoch              uint64
	Epoch                      uint64
}

// EpochInflationAccountState mirrors Agave's vote-reward metadata account.
type EpochInflationAccountState struct {
	Current EpochInflationState
	Prev    *EpochInflationState
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
	return EpochInflationState{
		MaxPossibleValidatorReward: maxReward,
		SlotsPerEpoch:              slotsPerEpoch,
		Epoch:                      epoch,
	}, nil
}

func decodeEpochInflationAccountState(data []byte) (EpochInflationAccountState, error) {
	r := wincode.NewReader(data)
	current, err := decodeEpochInflationState(r)
	if err != nil {
		return EpochInflationAccountState{}, fmt.Errorf("decode current epoch inflation: %w", err)
	}
	if r.Remaining() < 1 {
		return EpochInflationAccountState{}, fmt.Errorf("decode prev tag: truncated")
	}
	tagBytes, err := r.ReadBytes(1)
	if err != nil {
		return EpochInflationAccountState{}, fmt.Errorf("decode prev tag: %w", err)
	}
	var prev *EpochInflationState
	switch tagBytes[0] {
	case 0:
	case 1:
		state, err := decodeEpochInflationState(r)
		if err != nil {
			return EpochInflationAccountState{}, fmt.Errorf("decode prev epoch inflation: %w", err)
		}
		prev = &state
	default:
		return EpochInflationAccountState{}, fmt.Errorf("invalid prev tag %d", tagBytes[0])
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

func loadEpochInflationAccountState(acctsDb *accountsdb.AccountsDb, slot uint64) (EpochInflationAccountState, error) {
	acct, err := acctsDb.GetAccount(slot, VoteRewardAccountAddr())
	if err != nil {
		return EpochInflationAccountState{}, fmt.Errorf("load vote reward account at slot %d: %w", slot, err)
	}
	return decodeEpochInflationAccountFromAcct(acct, slot)
}

func loadEpochInflationAccountStateForReplay(
	acctsDb *accountsdb.AccountsDb,
	spec *SpeculativeReplay,
	parentSlot, blockSlot uint64,
) (EpochInflationAccountState, error) {
	acct, err := loadAccountForBlockReplay(acctsDb, spec, parentSlot, blockSlot, VoteRewardAccountAddr())
	if err != nil {
		return EpochInflationAccountState{}, fmt.Errorf("load vote reward account at parent slot %d: %w", parentSlot, err)
	}
	return decodeEpochInflationAccountFromAcct(acct, parentSlot)
}

func decodeEpochInflationAccountFromAcct(acct *accounts.Account, slot uint64) (EpochInflationAccountState, error) {
	if len(acct.Data) == 0 {
		return EpochInflationAccountState{}, fmt.Errorf("vote reward account missing at slot %d", slot)
	}
	state, err := decodeEpochInflationAccountState(acct.Data)
	if err != nil {
		return EpochInflationAccountState{}, fmt.Errorf("decode vote reward account at slot %d: %w", slot, err)
	}
	return state, nil
}

func encodeEpochInflationState(w *wincode.Writer, state EpochInflationState) {
	w.WriteU64(state.MaxPossibleValidatorReward)
	w.WriteU64(state.SlotsPerEpoch)
	w.WriteU64(state.Epoch)
}

func encodeEpochInflationAccountState(state EpochInflationAccountState) []byte {
	w := wincode.NewWriter(64)
	encodeEpochInflationState(w, state.Current)
	if state.Prev == nil {
		w.WriteBytes([]byte{0})
	} else {
		w.WriteBytes([]byte{1})
		encodeEpochInflationState(w, *state.Prev)
	}
	return w.Bytes()
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

func tryLoadEpochInflationAccountState(acctsDb *accountsdb.AccountsDb, slotCtx *sealevel.SlotCtx) (*EpochInflationAccountState, *accounts.Account) {
	var acct *accounts.Account
	var err error
	if slotCtx != nil {
		acct, err = slotCtx.GetAccount(VoteRewardAccountAddr())
	}
	if (err != nil || acct == nil) && slotCtx != nil {
		acct, err = acctsDb.GetAccount(slotCtx.Slot, VoteRewardAccountAddr())
	}
	if err != nil || acct == nil || len(acct.Data) == 0 {
		return nil, nil
	}
	state, err := decodeEpochInflationAccountState(acct.Data)
	if err != nil {
		return nil, acct
	}
	return &state, acct
}

// newEpochUpdateEpochInflationAccount mirrors Agave
// EpochInflationAccountState::new_epoch_update_account at the epoch boundary.
func newEpochUpdateEpochInflationAccount(
	acctsDb *accountsdb.AccountsDb,
	prevSlotCtx *sealevel.SlotCtx,
	replayCtx *ReplayCtx,
	epochSchedule *sealevel.SysvarEpochSchedule,
	f *features.Features,
	newEpoch uint64,
	epochStartCapitalization, additionalValidatorRewards uint64,
) ([]*accounts.Account, []*accounts.Account) {
	existingState, existingAcct := tryLoadEpochInflationAccountState(acctsDb, prevSlotCtx)

	var prev *EpochInflationState
	if existingState != nil {
		prev = &existingState.Current
	}

	current := newEpochInflationState(
		epochSchedule, &replayCtx.Inflation, epochStartCapitalization, additionalValidatorRewards,
		newEpoch, replayCtx.SlotsPerYear, f,
	)
	state := EpochInflationAccountState{Current: current, Prev: prev}

	data := encodeEpochInflationAccountState(state)
	rent, err := loadRentSysvar(acctsDb, prevSlotCtx)
	if err != nil {
		panic(fmt.Sprintf("load rent sysvar for vote reward account: %s", err))
	}
	lamports := rent.MinimumBalance(uint64(len(data)))
	if lamports == 0 {
		lamports = 1
	}

	var parentAcct *accounts.Account
	if existingAcct != nil {
		parentAcct = existingAcct.Clone()
	} else {
		parentAcct = &accounts.Account{
			Key:   VoteRewardAccountAddr(),
			Owner: a.SystemProgramAddr,
		}
	}

	acct := parentAcct.Clone()
	acct.Data = append([]byte(nil), data...)
	acct.Lamports = lamports
	acct.Owner = a.SystemProgramAddr

	if lamports > parentAcct.Lamports {
		replayCtx.Capitalization += lamports - parentAcct.Lamports
	} else if parentAcct.Lamports > lamports {
		replayCtx.Capitalization -= parentAcct.Lamports - lamports
	}

	if !acctsDb.RootedDurable {
		if err := acctsDb.StoreAccounts([]*accounts.Account{acct}, prevSlotCtx.Slot, nil); err != nil {
			panic(fmt.Sprintf("store vote reward account at slot %d: %s", prevSlotCtx.Slot, err))
		}
	}

	return []*accounts.Account{acct.Clone()}, []*accounts.Account{parentAcct}
}
