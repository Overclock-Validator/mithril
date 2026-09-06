package rent

import (
	"crypto/sha256"
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"

	//"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/util"
	"github.com/gagliardetto/solana-go"
)

const (
	RentStateUninitialized = iota
	RentStateRentPaying
	RentStateRentExempt
)

type RentPayingInfo struct {
	Lamports uint64
	DataSize uint64
	Pk       solana.PublicKey
}

type RentStateInfo struct {
	RentState      uint64
	RentPayingInfo RentPayingInfo
	owner          solana.PublicKey
	// relaxPostExecMinBalanceCheck records the transaction feature snapshot.
	// NewRentStateInfo is used for both the pre- and post-execution snapshots,
	// while SIMD-0392 needs both snapshots to derive the effective states.
	relaxPostExecMinBalanceCheck bool
}

// RentStateTransitionError identifies the transaction account whose rent-state
// transition failed. TransactionError::InsufficientFundsForRent carries this
// exact u8 index on the wire.
type RentStateTransitionError struct {
	AccountIndex uint8
	Err          error
}

func (e *RentStateTransitionError) Error() string {
	return e.Err.Error()
}

func (e *RentStateTransitionError) Unwrap() error {
	return e.Err
}

func rentStateFromAcct(acct *accounts.Account, rent *sealevel.SysvarRent, relaxPostExecMinBalanceCheck bool) *RentStateInfo {
	info := &RentStateInfo{
		RentPayingInfo: RentPayingInfo{
			Lamports: acct.Lamports,
			DataSize: uint64(len(acct.Data)),
			Pk:       acct.Key,
		},
		owner:                        acct.Owner,
		relaxPostExecMinBalanceCheck: relaxPostExecMinBalanceCheck,
	}
	if acct.Lamports == 0 {
		info.RentState = RentStateUninitialized
	} else if rent.IsExempt(acct.Lamports, uint64(len(acct.Data))) {
		info.RentState = RentStateRentExempt
	} else {
		info.RentState = RentStateRentPaying
	}
	return info
}

func NewRentStateInfo(rent *sealevel.SysvarRent, txCtx *sealevel.TransactionCtx, f *features.Features) []*RentStateInfo {
	rentStateInfos := make([]*RentStateInfo, 0, len(txCtx.Accounts.Accounts))
	acctsMetas := txCtx.Accounts.AcctMetas
	relaxPostExecMinBalanceCheck := f != nil && f.IsActive(features.RelaxPostExecMinBalanceCheck)

	for idx, acct := range txCtx.Accounts.Accounts {
		if sealevel.IsWritable(acctsMetas[idx], f) {
			rentStateInfo := rentStateFromAcct(acct, rent, relaxPostExecMinBalanceCheck)
			rentStateInfos = append(rentStateInfos, rentStateInfo)
		} else {
			rentStateInfos = append(rentStateInfos, nil)
		}
	}

	return rentStateInfos
}

// effectiveRentStates applies the SIMD-0392 pre/post classifications. Before
// activation, the raw legacy classifications are returned unchanged.
//
// Once active, a pre-existing rent-paying account is treated as rent-exempt.
// A post-execution, nonzero sub-minimum balance may remain effectively exempt
// only when the owner is unchanged, data did not grow, and the balance did not
// decrease. This is the relaxation which lets legacy rent-paying accounts be
// left unchanged or credited, while preventing any further debit.
func effectiveRentStates(preRentState *RentStateInfo, postRentState *RentStateInfo) (uint64, uint64) {
	if !preRentState.relaxPostExecMinBalanceCheck {
		return preRentState.RentState, postRentState.RentState
	}

	effectivePre := preRentState.RentState
	if effectivePre == RentStateRentPaying {
		effectivePre = RentStateRentExempt
	}

	effectivePost := postRentState.RentState
	relaxPostCriteria := effectivePre == RentStateRentExempt &&
		preRentState.RentPayingInfo.DataSize >= postRentState.RentPayingInfo.DataSize &&
		preRentState.owner == postRentState.owner
	if relaxPostCriteria &&
		effectivePost == RentStateRentPaying &&
		postRentState.RentPayingInfo.Lamports >= preRentState.RentPayingInfo.Lamports {
		effectivePost = RentStateRentExempt
	}

	return effectivePre, effectivePost
}

func checkRentStateTransitionAllowed(preRentState *RentStateInfo, postRentState *RentStateInfo, txCtx *sealevel.TransactionCtx, idx uint64) error {
	if preRentState == nil && postRentState == nil {
		return nil
	} else if preRentState == nil && postRentState != nil {
		panic("programming error - shouldn't be possible")
	} else if preRentState != nil && postRentState == nil {
		panic("programming error - shouldn't be possible")
	}

	acct, err := txCtx.AccountAtIndex(idx)
	if err != nil {
		panic("programming error - acct didn't exist in TransactionAccounts")
	}

	if acct.Key != a.IncineratorAddr {
		preState, postState := effectiveRentStates(preRentState, postRentState)
		if postState == RentStateUninitialized {
			return nil
		} else if postState == RentStateRentExempt {
			return nil
		} else if postState == RentStateRentPaying {
			if preState == RentStateUninitialized {
				return newRentStateTransitionError(idx, "[1] rent state transition not allowed. pre: %+v, post: %+v", preRentState, postRentState)
			} else if preState == RentStateRentExempt {
				return newRentStateTransitionError(idx, "[2] rent state transition not allowed. pre: %+v, post: %+v", preRentState, postRentState)
			} else if preState == RentStateRentPaying {
				if postRentState.RentPayingInfo.DataSize == preRentState.RentPayingInfo.DataSize && postRentState.RentPayingInfo.Lamports <= preRentState.RentPayingInfo.Lamports {
					return nil
				} else {
					return newRentStateTransitionError(idx, "[3] rent state transition not allowed. pre: %+v, post: %+v", preRentState, postRentState)
				}
			}
		}
	}

	return nil
}

func newRentStateTransitionError(accountIndex uint64, format string, args ...interface{}) error {
	return &RentStateTransitionError{
		AccountIndex: uint8(accountIndex),
		Err:          fmt.Errorf(format, args...),
	}
}

func VerifyRentStateChanges(preStates []*RentStateInfo, postStates []*RentStateInfo, txCtx *sealevel.TransactionCtx) error {
	if len(preStates) != len(postStates) {
		panic("programming error - pre tx states and post tx states must be same length")
	}

	for count := uint64(0); count < uint64(len(preStates)); count++ {
		err := checkRentStateTransitionAllowed(preStates[count], postStates[count], txCtx, count)
		if err != nil {
			return err
		}
	}

	return nil
}

func MaybeSetRentExemptRentEpochMax(slotCtx *sealevel.SlotCtx, rent *sealevel.SysvarRent, f *features.Features, txAccts *sealevel.TransactionAccounts) {
	for idx := range txAccts.Accounts {
		if ShouldSetRentExemptRentEpochMax(slotCtx, rent, f, txAccts.Accounts[idx]) {
			touchedAcct, err := txAccts.Touch(uint64(idx))
			if err != nil {
				panic("unable to mark rent-exempt account as touched")
			}
			touchedAcct.RentEpoch = math.MaxUint64
		}
	}
}

func isNativeProgram(pubkey solana.PublicKey) bool {
	if pubkey == a.SystemProgramAddr || pubkey == a.BpfLoaderUpgradeableAddr ||
		pubkey == a.BpfLoader2Addr || pubkey == a.BpfLoaderDeprecatedAddr ||
		pubkey == a.VoteProgramAddr || pubkey == a.StakeProgramAddr ||
		pubkey == a.AddressLookupTableAddr || pubkey == a.ConfigProgramAddr ||
		pubkey == a.ComputeBudgetProgramAddr {
		return true
	} else {
		return false
	}
}

func ShouldSetRentExemptRentEpochMax(slotCtx *sealevel.SlotCtx, rent *sealevel.SysvarRent, f *features.Features, acct *accounts.Account) bool {
	if f.IsActive(features.DisableRentFeesCollection) {
		if acct.RentEpoch != math.MaxUint64 && acct.Lamports >= rent.MinimumBalance(uint64(len(acct.Data))) {
			return true
		}
		return false
	}

	if isNativeProgram(acct.Key) {
		return false
	}

	if acct.IsDummy {
		return false
	}

	if acct.RentEpoch == math.MaxUint64 || acct.RentEpoch > slotCtx.Epoch {
		return false
	}

	if acct.IsExecutable() || acct.Key == a.IncineratorAddr {
		return true
	}

	if acct.Lamports != 0 && acct.Lamports < rent.MinimumBalance(uint64(len(acct.Data))) {
		return false
	}

	if acct.Key == sealevel.SysvarInstructionsAddr {
		return false
	}

	return true
}

const (
	RentExempt = iota
	RentNoCollectionNow
	RentCollectRent
)

func calculateRentResult(slotCtx *sealevel.SlotCtx, rent *sealevel.SysvarRent, acct *accounts.Account) int {
	if acct.RentEpoch == math.MaxUint64 || acct.RentEpoch > slotCtx.Epoch {
		return RentNoCollectionNow
	}

	if acct.Executable || acct.Key == a.IncineratorAddr {
		return RentExempt
	}

	if acct.Lamports >= rent.MinimumBalance(uint64(len(acct.Data))) {
		return RentExempt
	}

	// TODO: implement collection logic (testnet/devnet still have rent paying accounts)
	return RentExempt
}

func collectRentFromAcct(slotCtx *sealevel.SlotCtx, rent *sealevel.SysvarRent, acct *accounts.Account) (*accounts.Account, bool) {
	if !slotCtx.Features.IsActive(features.DisableRentFeesCollection) {
		result := calculateRentResult(slotCtx, rent, acct)

		if result == RentExempt {
			acct.RentEpoch = math.MaxUint64
			return acct, true
		} else if result == RentNoCollectionNow {
			return acct, false
		} else /*result == RentCollectRent*/ {
			panic("mainnet-beta shouldn't have any rent paying accounts")
		}
	} else {
		if acct.RentEpoch != math.MaxUint64 && acct.Lamports >= rent.MinimumBalance(uint64(len(acct.Data))) {
			acct.RentEpoch = math.MaxUint64
			return acct, true
		} else {
			return acct, false
		}
	}
}

func collectRent(slotCtx *sealevel.SlotCtx, rent *sealevel.SysvarRent, pubkey solana.PublicKey) (*accounts.Account, bool) {
	acct, err := slotCtx.GetAccount(pubkey)
	if err != nil {
		acct, err = slotCtx.AccountsDb.GetAccount(slotCtx.Slot, pubkey)
		if err != nil {
			panic("unable to find account for rent collection")
		}
	}

	if acct.Lamports == 0 {
		return nil, false
	}

	h := sha256.New()
	h.Write(acct.Data)
	fmt.Printf("collectRent acct: pubkey %s, lamports %d, owner %s, rent_epoch %d, data hash: %s\n", acct.Key, acct.Lamports, solana.PublicKeyFromBytes(acct.Owner[:]), acct.RentEpoch, solana.HashFromBytes(h.Sum(nil)))

	return collectRentFromAcct(slotCtx, rent, acct)
}

func CollectRentEagerly(slotCtx *sealevel.SlotCtx, rent *sealevel.SysvarRent, epochSchedule *sealevel.SysvarEpochSchedule) []*accounts.Account {
	//mlog.Log.Debugf("CollectRentEagerly ParentSlot = %d\n", slotCtx.ParentSlot)

	if slotCtx.Features.IsActive(features.SkipRentRewrites) {
		//mlog.Log.Debugf("SkipRentRewrites enabled - skipping.")
		return nil
	}

	partitions := RentCollectionPartitions(slotCtx.ParentSlot, slotCtx.Slot, epochSchedule)
	pkRange := pubkeyRangeFromPartition(partitions[0])

	rentPubkeys := slotCtx.AccountsDb.KeysBetweenPrefixes(pkRange.StartPrefix, pkRange.EndPrefix)
	rentPubkeys = util.DedupePubkeys(rentPubkeys)

	accts := make([]*accounts.Account, 0)

	for _, pk := range rentPubkeys {
		acct, _ := collectRent(slotCtx, rent, pk)
		if acct != nil {
			accts = append(accts, acct)
		}
	}

	return accts
}
