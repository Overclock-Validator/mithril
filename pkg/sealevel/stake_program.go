package sealevel

import (
	"encoding/binary"
	"errors"
	"math"
	"unicode/utf8"

	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/mlog"

	"github.com/Overclock-Validator/mithril/pkg/safemath"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

const (
	StakeStateV2Size = 200
)

const (
	StakeProgramInstrTypeInitialize = iota
	StakeProgramInstrTypeAuthorize
	StakeProgramInstrTypeDelegateStake
	StakeProgramInstrTypeSplit
	StakeProgramInstrTypeWithdraw
	StakeProgramInstrTypeDeactivate
	StakeProgramInstrTypeSetLockup
	StakeProgramInstrTypeMerge
	StakeProgramInstrTypeAuthorizeWithSeed
	StakeProgramInstrTypeInitializeChecked
	StakeProgramInstrTypeAuthorizeChecked
	StakeProgramInstrTypeAuthorizeCheckedWithSeed
	StakeProgramInstrTypeSetLockupChecked
	StakeProgramInstrTypeGetMinimumDelegation
	StakeProgramInstrTypeDeactivateDelinquent
	StakeProgramInstrTypeRedelegate
	StakeProgramInstrTypeMoveStake
	StakeProgramInstrTypeMoveLamports
)

// stake errors
var (
	StakeErrCustodianMissing                                               = errors.New("StakeErrCustodianMissing")
	StakeErrCustodianSignatureMissing                                      = errors.New("StakeErrCustodianSignatureMissing")
	StakeErrLockupInForce                                                  = errors.New("StakeErrLockupInForce")
	StakeErrInsufficientDelegation                                         = errors.New("StakeErrInsufficientDelegation")
	StakeErrTooSoonToRedelegate                                            = errors.New("StakeErrTooSoonToRedelegate")
	StakeErrInsufficientStake                                              = errors.New("StakeErrInsufficientStake")
	StakeErrMergeTransientStake                                            = errors.New("StakeErrMergeTransientStake")
	StakeErrMergeMismatch                                                  = errors.New("StakeErrMergeMismatch")
	StakeErrAlreadyDeactivated                                             = errors.New("StakeErrAlreadyDeactivated")
	StakeErrRedelegatedStakeMustFullyActivateBeforeDeactivationIsPermitted = errors.New("StakeErrRedelegatedStakeMustFullyActivateBeforeDeactivationIsPermitted")
	StakeErrInsufficientReferenceVotes                                     = errors.New("StakeErrInsufficientReferenceVotes")
	StakeErrVoteAddressMismatch                                            = errors.New("StakeErrVoteAddressMismatch")
	StakeErrMinimumDelinquentEpochsForDeactivationNotMet                   = errors.New("StakeErrMinimumDelinquentEpochsForDeactivationNotMet")
	StakeErrRedelegateTransientOrInactiveStake                             = errors.New("StakeErrRedelegateTransientOrInactiveStake")
	StakeErrRedelegateToSameVoteAccount                                    = errors.New("StakeErrRedelegateToSameVoteAccount")
	StakeErrEpochRewardsActive                                             = errors.New("StakeErrEpochRewardsActive")
)

type StakeInstrInitialize struct {
	Authorized Authorized
	Lockup     StakeLockup
}

type StakeInstrAuthorize struct {
	Pubkey         solana.PublicKey
	StakeAuthorize uint32
}

type StakeInstrSplit struct {
	Lamports uint64
}

type StakeInstrWithdraw struct {
	Lamports uint64
}

type StakeInstrSetLockup struct {
	UnixTimestamp *uint64
	Epoch         *uint64
	Custodian     *solana.PublicKey
}

type StakeInstrAuthorizeWithSeed struct {
	NewAuthorizedPubkey solana.PublicKey
	StakeAuthorize      uint32
	AuthoritySeed       string
	AuthorityOwner      solana.PublicKey
}

type StakeInstrAuthorizeChecked struct {
	StakeAuthorize uint32
}

type StakeInstrAuthorizeCheckedWithSeed struct {
	StakeAuthorize uint32
	AuthoritySeed  string
	AuthorityOwner solana.PublicKey
}

type StakeInstrSetLockupChecked struct {
	UnixTimestamp *uint64
	Epoch         *uint64
}

type StakeInstrMoveStake struct {
	Lamports uint64
}

type StakeInstrMoveLamports struct {
	Lamports uint64
}

func (initialize *StakeInstrInitialize) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	err = initialize.Authorized.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	err = initialize.Lockup.UnmarshalWithDecoder(decoder)
	return err
}

func (auth *StakeInstrAuthorize) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	pk, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(auth.Pubkey[:], pk)

	auth.StakeAuthorize, err = decoder.ReadUint32(bin.LE)
	if err != nil {
		return err
	}

	if auth.StakeAuthorize != StakeAuthorizeStaker && auth.StakeAuthorize != StakeAuthorizeWithdrawer {
		return invalidEnumValue
	}

	return err
}

func (split *StakeInstrSplit) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	split.Lamports, err = decoder.ReadUint64(bin.LE)
	return err
}

func (withdraw *StakeInstrWithdraw) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	withdraw.Lamports, err = decoder.ReadUint64(bin.LE)
	return err
}

func (lockup *StakeInstrSetLockup) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	timeStampExists, err := ReadBool(decoder)
	if err != nil {
		return err
	}
	if timeStampExists {
		ts, err := decoder.ReadUint64(bin.LE)
		if err != nil {
			return err
		}
		lockup.UnixTimestamp = &ts
	}

	epochExists, err := ReadBool(decoder)
	if err != nil {
		return err
	}
	if epochExists {
		epoch, err := decoder.ReadUint64(bin.LE)
		if err != nil {
			return err
		}
		lockup.Epoch = &epoch
	}

	custodianExists, err := ReadBool(decoder)
	if err != nil {
		return err
	}
	if custodianExists {
		custodianPkBytes, err := decoder.ReadBytes(solana.PublicKeyLength)
		if err != nil {
			return err
		}

		pk := solana.PublicKeyFromBytes(custodianPkBytes)
		lockup.Custodian = &pk
	}

	return nil
}

func (authWithSeed *StakeInstrAuthorizeWithSeed) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	pk, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(authWithSeed.NewAuthorizedPubkey[:], pk)

	authWithSeed.StakeAuthorize, err = decoder.ReadUint32(bin.LE)
	if err != nil {
		return err
	}
	if authWithSeed.StakeAuthorize != StakeAuthorizeStaker && authWithSeed.StakeAuthorize != StakeAuthorizeWithdrawer {
		return invalidEnumValue
	}

	authWithSeed.AuthoritySeed, err = decoder.ReadRustString()
	if err != nil {
		return err
	}
	if !utf8.ValidString(authWithSeed.AuthoritySeed) {
		return InstrErrInvalidInstructionData
	}

	pk, err = decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(authWithSeed.AuthorityOwner[:], pk)
	return checkWithinDeserializationLimit(decoder)
}

func (authChecked *StakeInstrAuthorizeChecked) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	authChecked.StakeAuthorize, err = decoder.ReadUint32(bin.LE)
	if err != nil {
		return err
	}

	if authChecked.StakeAuthorize != StakeAuthorizeStaker && authChecked.StakeAuthorize != StakeAuthorizeWithdrawer {
		return invalidEnumValue
	}

	return nil
}

func (authCheckedWithSeed *StakeInstrAuthorizeCheckedWithSeed) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	authCheckedWithSeed.StakeAuthorize, err = decoder.ReadUint32(bin.LE)
	if err != nil {
		return err
	}
	if authCheckedWithSeed.StakeAuthorize != StakeAuthorizeStaker && authCheckedWithSeed.StakeAuthorize != StakeAuthorizeWithdrawer {
		return invalidEnumValue
	}

	authCheckedWithSeed.AuthoritySeed, err = decoder.ReadRustString()
	if err != nil {
		return err
	}
	if !utf8.ValidString(authCheckedWithSeed.AuthoritySeed) {
		return InstrErrInvalidInstructionData
	}

	pk, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(authCheckedWithSeed.AuthorityOwner[:], pk)
	return checkWithinDeserializationLimit(decoder)
}

func (lockup *StakeInstrSetLockupChecked) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	timeStampExists, err := ReadBool(decoder)
	if err != nil {
		return err
	}
	if timeStampExists {
		ts, err := decoder.ReadUint64(bin.LE)
		if err != nil {
			return err
		}
		lockup.UnixTimestamp = &ts
	}

	epochExists, err := ReadBool(decoder)
	if err != nil {
		return err
	}
	if epochExists {
		epoch, err := decoder.ReadUint64(bin.LE)
		if err != nil {
			return err
		}
		lockup.Epoch = &epoch
	}

	return nil
}

func (moveStake *StakeInstrMoveStake) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	moveStake.Lamports, err = decoder.ReadUint64(bin.LE)
	return err
}

func (moveLamports *StakeInstrMoveLamports) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	moveLamports.Lamports, err = decoder.ReadUint64(bin.LE)
	return err
}

func getOptionalPubkey(txCtx *TransactionCtx, instrCtx *InstructionCtx, instrAcctIdx uint64, mustBeSigner bool) (*solana.PublicKey, error) {
	if instrAcctIdx < instrCtx.NumberOfInstructionAccounts() {
		isSigner, err := instrCtx.IsInstructionAccountSigner(instrAcctIdx)
		if err != nil {
			return nil, err
		}

		if mustBeSigner && !isSigner {
			return nil, InstrErrMissingRequiredSignature
		}

		idxInTx, err := instrCtx.IndexOfInstructionAccountInTransaction(instrAcctIdx)
		if err != nil {
			return nil, err
		}

		pubkey, err := txCtx.KeyOfAccountAtIndex(idxInTx)
		if err != nil {
			return nil, err
		} else {
			return &pubkey, nil
		}
	} else { // no pubkey, not an error
		return nil, nil
	}
}

func StakeProgramExecute(execCtx *ExecutionCtx) error {
	mlog.Log.Infof("in Stake program")

	err := execCtx.ComputeMeter.Consume(cu.CUStakeProgramDefaultComputeUnits)
	if err != nil {
		return err
	}

	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	data := instrCtx.Data

	getStakeAccount := func() (*BorrowedAccount, error) {
		acct, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
		if err != nil {
			return nil, err
		}
		if acct.Owner() != a.StakeProgramAddr {
			return nil, InstrErrInvalidAccountOwner
		}
		return acct, nil
	}

	var epochRewardsActive bool
	epochRewards, err := ReadEpochRewardsSysvar(execCtx)
	if err == nil {
		epochRewardsActive = epochRewards.Active
	}

	signers, err := instrCtx.Signers(txCtx)
	if err != nil {
		return err
	}

	decoder := bin.NewBinDecoder(data)
	instructionType, err := decoder.ReadUint32(bin.LE)
	if err != nil {
		return InstrErrInvalidInstructionData
	}

	txCtx.ModifiedStakeAccts = true

	switch instructionType {
	case StakeProgramInstrTypeInitialize:
		{
			var initialize StakeInstrInitialize
			err = initialize.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			err = checkAcctForRentSysvar(txCtx, instrCtx, 1)
			if err != nil {
				return err
			}

			var rent SysvarRent
			rent, err = ReadRentSysvar(execCtx)
			if err != nil {
				return err
			}

			err = StakeProgramInitialize(me, initialize.Authorized, initialize.Lockup, rent, execCtx.Features)
		}

	case StakeProgramInstrTypeAuthorize:
		{
			var authorize StakeInstrAuthorize
			err = authorize.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			err = checkAcctForClockSysvar(txCtx, instrCtx, 1)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			err = instrCtx.CheckNumOfInstructionAccounts(3)
			if err != nil {
				return err
			}

			var custodianPubkey *solana.PublicKey
			custodianPubkey, err = getOptionalPubkey(txCtx, instrCtx, 3, false)
			if err != nil {
				return err
			}

			err = StakeProgramAuthorize(me, signers, authorize.Pubkey, authorize.StakeAuthorize, clock, custodianPubkey, execCtx.Features)
		}

	case StakeProgramInstrTypeAuthorizeWithSeed:
		{
			var authorizeWithSeed StakeInstrAuthorizeWithSeed
			err = authorizeWithSeed.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			err = instrCtx.CheckNumOfInstructionAccounts(2)
			if err != nil {
				return err
			}

			err = checkAcctForClockSysvar(txCtx, instrCtx, 2)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			var custodianPubkey *solana.PublicKey
			custodianPubkey, err = getOptionalPubkey(txCtx, instrCtx, 3, false)
			if err != nil {
				return err
			}

			err = StakeProgramAuthorizeWithSeed(txCtx, instrCtx, me, 1, authorizeWithSeed.AuthoritySeed, authorizeWithSeed.AuthorityOwner, authorizeWithSeed.NewAuthorizedPubkey, authorizeWithSeed.StakeAuthorize, clock, custodianPubkey, execCtx.Features)
		}

	case StakeProgramInstrTypeDelegateStake:
		{
			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			err = instrCtx.CheckNumOfInstructionAccounts(2)
			if err != nil {
				return err
			}

			err = checkAcctForClockSysvar(txCtx, instrCtx, 2)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			var stakeHistory SysvarStakeHistory
			stakeHistory, err = ReadStakeHistorySysvar(execCtx)
			if err != nil {
				return err
			}

			err = checkAcctForStakeHistorySysvar(txCtx, instrCtx, 3)
			if err != nil {
				return err
			}

			err = instrCtx.CheckNumOfInstructionAccounts(5)
			if err != nil {
				return err
			}
			me.Drop()

			err = StakeProgramDelegate(execCtx, txCtx, instrCtx, 0, 1, clock, stakeHistory, signers, execCtx.Features)
		}

	case StakeProgramInstrTypeSplit:
		{
			var split StakeInstrSplit
			err = split.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			err = instrCtx.CheckNumOfInstructionAccounts(2)
			if err != nil {
				return err
			}
			me.Drop()

			err = StakeProgramSplit(execCtx, txCtx, instrCtx, 0, split.Lamports, 1, signers)
		}

	case StakeProgramInstrTypeMerge:
		{
			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			err = instrCtx.CheckNumOfInstructionAccounts(2)
			if err != nil {
				return err
			}

			err = checkAcctForClockSysvar(txCtx, instrCtx, 2)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			var stakeHistory SysvarStakeHistory
			stakeHistory, err = ReadStakeHistorySysvar(execCtx)
			if err != nil {
				return err
			}

			err = checkAcctForStakeHistorySysvar(txCtx, instrCtx, 3)
			if err != nil {
				return err
			}
			me.Drop()

			err = StakeProgramMerge(execCtx, txCtx, instrCtx, 0, 1, clock, stakeHistory, signers)
		}

	case StakeProgramInstrTypeWithdraw:
		{
			var withdraw StakeInstrWithdraw
			err = withdraw.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			err = instrCtx.CheckNumOfInstructionAccounts(2)
			if err != nil {
				return err
			}

			err = checkAcctForClockSysvar(txCtx, instrCtx, 2)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			err = checkAcctForStakeHistorySysvar(txCtx, instrCtx, 3)
			if err != nil {
				return err
			}

			var stakeHistory SysvarStakeHistory
			stakeHistory, err = ReadStakeHistorySysvar(execCtx)
			if err != nil {
				return err
			}

			err = instrCtx.CheckNumOfInstructionAccounts(5)
			if err != nil {
				return err
			}

			me.Drop()

			var custodianIndex *uint64
			if instrCtx.NumberOfInstructionAccounts() >= 6 {
				i := uint64(5)
				custodianIndex = &i
			}

			var epoch *uint64
			epoch, err = NewWarmupCooldownRateEpoch(execCtx)
			if err != nil {
				return err
			}
			err = StakeProgramWithdraw(txCtx, instrCtx, 0, withdraw.Lamports, 1, clock, stakeHistory, 4, custodianIndex, epoch, execCtx.Features)
		}

	case StakeProgramInstrTypeDeactivate:
		{
			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			err = checkAcctForClockSysvar(txCtx, instrCtx, 1)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			err = StakeProgramDeactivate(execCtx, me, clock, signers)
		}

	case StakeProgramInstrTypeSetLockup:
		{
			var lockup StakeInstrSetLockup
			err = lockup.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			err = StakeProgramSetLockup(me, lockup, signers, clock, execCtx.Features)
		}

	case StakeProgramInstrTypeInitializeChecked:
		{
			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			err = instrCtx.CheckNumOfInstructionAccounts(4)
			if err != nil {
				return err
			}

			var idxInTxStaker uint64
			idxInTxStaker, err = instrCtx.IndexOfInstructionAccountInTransaction(2)
			if err != nil {
				return err
			}

			var stakerPubkey solana.PublicKey
			stakerPubkey, err = txCtx.KeyOfAccountAtIndex(idxInTxStaker)
			if err != nil {
				return err
			}

			var idxInTxWithdrawer uint64
			idxInTxWithdrawer, err = instrCtx.IndexOfInstructionAccountInTransaction(3)
			if err != nil {
				return err
			}

			var withdrawerPubkey solana.PublicKey
			withdrawerPubkey, err = txCtx.KeyOfAccountAtIndex(idxInTxWithdrawer)
			if err != nil {
				return err
			}

			var isSigner bool
			isSigner, err = instrCtx.IsInstructionAccountSigner(3)
			if err != nil {
				return err
			}
			if !isSigner {
				return InstrErrMissingRequiredSignature
			}

			authorized := Authorized{Staker: stakerPubkey, Withdrawer: withdrawerPubkey}

			var rent SysvarRent
			rent, err = ReadRentSysvar(execCtx)
			if err != nil {
				return err
			}
			err = checkAcctForRentSysvar(txCtx, instrCtx, 1)
			if err != nil {
				return err
			}

			err = StakeProgramInitialize(me, authorized, StakeLockup{}, rent, execCtx.Features)
		}

	case StakeProgramInstrTypeAuthorizeChecked:
		{
			var authorizeChecked StakeInstrAuthorizeChecked
			err = authorizeChecked.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			err = checkAcctForClockSysvar(txCtx, instrCtx, 1)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			err = instrCtx.CheckNumOfInstructionAccounts(4)
			if err != nil {
				return err
			}

			var idxInTx uint64
			idxInTx, err = instrCtx.IndexOfInstructionAccountInTransaction(3)
			if err != nil {
				return err
			}

			var authorizedPubkey solana.PublicKey
			authorizedPubkey, err = txCtx.KeyOfAccountAtIndex(idxInTx)
			if err != nil {
				return err
			}

			var isSigner bool
			isSigner, err = instrCtx.IsInstructionAccountSigner(3)
			if err != nil {
				return err
			}
			if !isSigner {
				return InstrErrMissingRequiredSignature
			}

			var custodianPubkey *solana.PublicKey
			custodianPubkey, err = getOptionalPubkey(txCtx, instrCtx, 4, false)
			if err != nil {
				return err
			}

			err = StakeProgramAuthorize(me, signers, authorizedPubkey, authorizeChecked.StakeAuthorize, clock, custodianPubkey, execCtx.Features)
		}

	case StakeProgramInstrTypeAuthorizeCheckedWithSeed:
		{
			var authorizeCheckedWithSeed StakeInstrAuthorizeCheckedWithSeed
			err = authorizeCheckedWithSeed.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			err = instrCtx.CheckNumOfInstructionAccounts(2)
			if err != nil {
				return err
			}

			err = checkAcctForClockSysvar(txCtx, instrCtx, 2)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			err = instrCtx.CheckNumOfInstructionAccounts(4)
			if err != nil {
				return err
			}

			var idxInTx uint64
			idxInTx, err = instrCtx.IndexOfInstructionAccountInTransaction(3)
			if err != nil {
				return err
			}

			var authorizedPubkey solana.PublicKey
			authorizedPubkey, err = txCtx.KeyOfAccountAtIndex(idxInTx)
			if err != nil {
				return err
			}

			var isSigner bool
			isSigner, err = instrCtx.IsInstructionAccountSigner(3)
			if err != nil {
				return err
			}
			if !isSigner {
				return InstrErrMissingRequiredSignature
			}

			var custodianPubkey *solana.PublicKey
			custodianPubkey, err = getOptionalPubkey(txCtx, instrCtx, 4, false)
			if err != nil {
				return err
			}

			err = StakeProgramAuthorizeWithSeed(txCtx, instrCtx, me, 1, authorizeCheckedWithSeed.AuthoritySeed, authorizeCheckedWithSeed.AuthorityOwner, authorizedPubkey, authorizeCheckedWithSeed.StakeAuthorize, clock, custodianPubkey, execCtx.Features)
		}

	case StakeProgramInstrTypeSetLockupChecked:
		{
			var setLockupChecked StakeInstrSetLockupChecked
			err = setLockupChecked.UnmarshalWithDecoder(decoder)
			if err != nil {
				return err
			}

			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			var custodianPubkey *solana.PublicKey
			custodianPubkey, err = getOptionalPubkey(txCtx, instrCtx, 2, true)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			lockup := StakeInstrSetLockup{UnixTimestamp: setLockupChecked.UnixTimestamp, Epoch: setLockupChecked.Epoch, Custodian: custodianPubkey}

			err = StakeProgramSetLockup(me, lockup, signers, clock, execCtx.Features)
		}

	case StakeProgramInstrTypeGetMinimumDelegation:
		{
			minimumDelegation := determineMinimumDelegation(execCtx.Features)
			minimumDelegationBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(minimumDelegationBytes, minimumDelegation)
			txCtx.SetReturnData(a.StakeProgramAddr, minimumDelegationBytes)
		}

	case StakeProgramInstrTypeDeactivateDelinquent:
		{
			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			err = instrCtx.CheckNumOfInstructionAccounts(3)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			err = StakeProgramDeactivateDelinquent(execCtx, txCtx, instrCtx, me, 1, 2, clock.Epoch)
		}

	case StakeProgramInstrTypeRedelegate:
		{
			if epochRewardsActive {
				return StakeErrEpochRewardsActive
			}

			var me *BorrowedAccount
			me, err = getStakeAccount()
			if err != nil {
				return err
			}
			defer me.Drop()

			if execCtx.Features.IsActive(features.StakeRedelegateInstruction) {
				err = instrCtx.CheckNumOfInstructionAccounts(3)
				if err != nil {
					return err
				}

				err = StakeProgramRedelegate(execCtx, txCtx, instrCtx, me, 1, 2, signers)

			} else {
				return InstrErrInvalidInstructionData
			}
		}

	case StakeProgramInstrTypeMoveStake:
		{
			var moveStake StakeInstrMoveStake
			err = moveStake.UnmarshalWithDecoder(decoder)
			if err != nil {
				return err
			}

			if execCtx.Features.IsActive(features.MoveStakeAndMoveLamportsIxs) {
				err = instrCtx.CheckNumOfInstructionAccounts(3)
				if err != nil {
					return err
				}

				err = StakeProgramMoveStake(execCtx, instrCtx, 0, moveStake.Lamports, 1, 2)
			} else {
				return InstrErrInvalidInstructionData
			}
		}

	case StakeProgramInstrTypeMoveLamports:
		{
			var moveLamports StakeInstrMoveLamports
			err = moveLamports.UnmarshalWithDecoder(decoder)
			if err != nil {
				return err
			}

			if execCtx.Features.IsActive(features.MoveStakeAndMoveLamportsIxs) {
				err = instrCtx.CheckNumOfInstructionAccounts(3)
				if err != nil {
					return err
				}

				err = StakeProgramMoveLamports(execCtx, instrCtx, 0, moveLamports.Lamports, 1, 2)
			} else {
				return InstrErrInvalidInstructionData
			}

		}

	default:
		{
			err = InstrErrInvalidInstructionData
		}
	}

	return err
}

func StakeProgramInitialize(stakeAcct *BorrowedAccount, authorized Authorized, lockup StakeLockup, rent SysvarRent, f features.Features) error {
	//mlog.Log.Debugf("StakeProgramInitialize")

	if len(stakeAcct.Data()) != StakeStateV2Size {
		return InstrErrInvalidAccountData
	}

	state, err := UnmarshalStakeState(stakeAcct.Data())
	if err != nil {
		return err
	}

	if state.Status == StakeStateV2StatusUninitialized {
		rentExemptReserve := rent.MinimumBalance(uint64(len(stakeAcct.Data())))
		if stakeAcct.Lamports() >= rentExemptReserve {
			newStakeState := new(StakeStateV2)
			newStakeState.Status = StakeStateV2StatusInitialized
			newStakeState.Initialized = StakeStateV2Initialized{Meta: Meta{RentExemptReserve: rentExemptReserve, Authorized: authorized, Lockup: lockup}}
			return setStakeAccountState(stakeAcct, newStakeState, f)
		} else {
			return InstrErrInsufficientFunds
		}
	} else {
		return InstrErrInvalidAccountData
	}
}

// MinimumDelegation is the stake program's minimum delegation under f.
func MinimumDelegation(f features.Features) uint64 { return determineMinimumDelegation(f) }

func determineMinimumDelegation(f features.Features) uint64 {
	if f.IsActive(features.StakeRaiseMinimumDelegationTo1Sol) {
		minimumDelegationSol := 1
		lamportsPerSol := 1000000000
		return uint64(minimumDelegationSol * lamportsPerSol)
	} else {
		return 1
	}
}

func validateAndReturnDelegatedAmount(stakeAcct *BorrowedAccount, meta Meta, f features.Features) (uint64, error) {
	stakeAmount := safemath.SaturatingSubU64(stakeAcct.Lamports(), meta.RentExemptReserve)
	minimumDelegation := determineMinimumDelegation(f)

	if stakeAmount < minimumDelegation {
		return 0, StakeErrInsufficientDelegation
	}

	return stakeAmount, nil
}

func StakeProgramAuthorize(stakeAcct *BorrowedAccount, signers []solana.PublicKey, newAuthority solana.PublicKey, stakeAuthorize uint32, clock SysvarClock, custodianPubkey *solana.PublicKey, f features.Features) error {
	//mlog.Log.Debugf("StakeProgramAuthorize")

	state, err := UnmarshalStakeState(stakeAcct.Data())
	if err != nil {
		return err
	}

	switch state.Status {
	case StakeStateV2StatusStake:
		{
			err = state.Stake.Meta.Authorized.Authorize(signers, newAuthority, stakeAuthorize, state.Stake.Meta.Lockup, clock, custodianPubkey)
			if err != nil {
				return err
			}

			err = setStakeAccountState(stakeAcct, state, f)
		}

	case StakeStateV2StatusInitialized:
		{
			err = state.Initialized.Meta.Authorized.Authorize(signers, newAuthority, stakeAuthorize, state.Stake.Meta.Lockup, clock, custodianPubkey)
			if err != nil {
				return err
			}

			err = setStakeAccountState(stakeAcct, state, f)
		}

	default:
		{
			err = InstrErrInvalidAccountData
		}
	}

	return err
}

func StakeProgramAuthorizeWithSeed(txCtx *TransactionCtx, instrCtx *InstructionCtx, stakeAcct *BorrowedAccount, authorityBaseIndex uint64, authoritySeed string, authorityOwner solana.PublicKey, newAuthority solana.PublicKey, stakeAuthorize uint32, clock SysvarClock, custodian *solana.PublicKey, f features.Features) error {
	//mlog.Log.Debugf("StakeProgramAuthorizeWithSeed")

	var signers []solana.PublicKey

	isSigner, err := instrCtx.IsInstructionAccountSigner(authorityBaseIndex)
	if err != nil {
		return err
	}

	if isSigner {
		idx, err := instrCtx.IndexOfInstructionAccountInTransaction(authorityBaseIndex)
		if err != nil {
			return err
		}

		basePubkey, err := txCtx.KeyOfAccountAtIndex(idx)
		if err != nil {
			return err
		}
		pk, err := ValidateAndCreateWithSeed(basePubkey, authoritySeed, authorityOwner)
		if err != nil {
			return err
		}
		signers = append(signers, pk)
	}

	return StakeProgramAuthorize(stakeAcct, signers, newAuthority, stakeAuthorize, clock, custodian, f)
}

var DefaultWarmupCooldownRate float64 = 0.25
var NewWarmupCooldownRate float64 = 0.09

func warmupCooldownRate(currentEpoch uint64, newRateActivationEpoch *uint64) float64 {
	var n uint64
	if newRateActivationEpoch == nil {
		n = uint64(math.MaxUint64)
	} else {
		n = *newRateActivationEpoch
	}

	if currentEpoch < n {
		return DefaultWarmupCooldownRate
	} else {
		return NewWarmupCooldownRate
	}
}

func StakeProgramDelegate(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx, stakeAcctIdx uint64, voteAcctIdx uint64, clock SysvarClock, stakeHistory SysvarStakeHistory, signers []solana.PublicKey, f features.Features) error {
	//mlog.Log.Debugf("StakeProgramDelegate")

	voteAcct, err := instrCtx.BorrowInstructionAccount(txCtx, voteAcctIdx)
	if err != nil {
		return err
	}
	defer voteAcct.Drop()

	if voteAcct.Owner() != a.VoteProgramAddr {
		return InstrErrIncorrectProgramId
	}

	votePubkey := voteAcct.Key()
	versionedVoteState, voteUnmarshalErr := UnmarshalVersionedVoteState(voteAcct.Data())
	voteAcct.Drop()

	stakeAcct, err := instrCtx.BorrowInstructionAccount(txCtx, stakeAcctIdx)
	if err != nil {
		return err
	}
	defer stakeAcct.Drop()

	stakeState, err := UnmarshalStakeState(stakeAcct.Data())
	if err != nil {
		return err
	}

	switch stakeState.Status {
	case StakeStateV2StatusInitialized:
		{
			err = stakeState.Initialized.Meta.Authorized.Check(signers, StakeAuthorizeStaker)
			if err != nil {
				return err
			}
			stakeAmount, err := validateAndReturnDelegatedAmount(stakeAcct, stakeState.Initialized.Meta, f)
			if err != nil {
				return err
			}

			if voteUnmarshalErr != nil {
				return voteUnmarshalErr
			}

			credits := versionedVoteState.ConvertToCurrent().Credits()
			stake := Stake{Delegation: Delegation{VoterPubkey: votePubkey, StakeLamports: stakeAmount, ActivationEpoch: clock.Epoch, DeactivationEpoch: math.MaxUint64, WarmupCooldownRate: 0.25},
				CreditsObserved: credits}

			stakeState.Status = StakeStateV2StatusStake
			stakeState.Stake = StakeStateV2Stake{Meta: stakeState.Initialized.Meta, Stake: stake}
			err = setStakeAccountState(stakeAcct, stakeState, f)
			if err != nil {
				return err
			}
		}

	case StakeStateV2StatusStake:
		{
			err = stakeState.Stake.Meta.Authorized.Check(signers, StakeAuthorizeStaker)
			if err != nil {
				return err
			}
			stakeAmount, err := validateAndReturnDelegatedAmount(stakeAcct, stakeState.Stake.Meta, f)
			if err != nil {
				return err
			}

			if voteUnmarshalErr != nil {
				//mlog.Log.Debugf("failed to unmarshal vote state")
				return voteUnmarshalErr
			}

			err = modifyStakeForRedelegation(execCtx, &stakeState.Stake.Stake, stakeAmount, votePubkey, versionedVoteState.ConvertToCurrent(), clock, stakeHistory)
			if err != nil {
				return err
			}

			err = setStakeAccountState(stakeAcct, stakeState, f)
			if err != nil {
				return err
			}
		}

	default:
		{
			return InstrErrInvalidAccountData
		}
	}

	return nil
}

type validatedSplitInfo struct {
	SrcRemainingBalance   uint64
	DestRentExemptReserve uint64
}

func validateSplitAmount(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx, srcAcctIdx uint64, destAcctIdx uint64, lamports uint64, sourceMeta Meta, additionalRequiredLamports uint64, srcIsActive bool) (validatedSplitInfo, error) {
	srcAcct, err := instrCtx.BorrowInstructionAccount(txCtx, srcAcctIdx)
	if err != nil {
		return validatedSplitInfo{}, err
	}
	defer srcAcct.Drop()

	srcLamports := srcAcct.Lamports()
	srcAcct.Drop()

	dstAcct, err := instrCtx.BorrowInstructionAccount(txCtx, destAcctIdx)
	if err != nil {
		return validatedSplitInfo{}, err
	}
	defer dstAcct.Drop()

	dstLamports := dstAcct.Lamports()
	dstDataLen := uint64(len(dstAcct.Data()))
	dstAcct.Drop()

	if lamports == 0 {
		return validatedSplitInfo{}, InstrErrInsufficientFunds
	}

	if lamports > srcLamports {
		return validatedSplitInfo{}, InstrErrInsufficientFunds
	}

	srcMinimumBalance := safemath.SaturatingAddU64(sourceMeta.RentExemptReserve, additionalRequiredLamports)
	srcRemainingBalance := safemath.SaturatingSubU64(srcLamports, lamports)
	if srcRemainingBalance != 0 && srcRemainingBalance < srcMinimumBalance {
		return validatedSplitInfo{}, InstrErrInsufficientFunds
	}

	rent, err := ReadRentSysvar(execCtx)
	if err != nil {
		return validatedSplitInfo{}, err
	}

	dstRentExemptReserve := rent.MinimumBalance(dstDataLen)

	if execCtx.Features.IsActive(features.RequireRentExemptSplitDestination) &&
		srcIsActive && srcRemainingBalance != 0 && dstLamports < dstRentExemptReserve {
		return validatedSplitInfo{}, InstrErrInsufficientFunds
	}

	dstMinimumBalance := safemath.SaturatingAddU64(dstRentExemptReserve, additionalRequiredLamports)
	dstBalanceDeficit := safemath.SaturatingSubU64(dstMinimumBalance, dstLamports)
	if lamports < dstBalanceDeficit {
		return validatedSplitInfo{}, InstrErrInsufficientFunds
	}

	return validatedSplitInfo{SrcRemainingBalance: srcRemainingBalance, DestRentExemptReserve: dstRentExemptReserve}, nil
}

func StakeProgramSplit(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx, stakeAcctIdx uint64, lamports uint64, splitIdx uint64, signers []solana.PublicKey) error {
	//mlog.Log.Debugf("StakeProgramSplit")

	split, err := instrCtx.BorrowInstructionAccount(txCtx, splitIdx)
	if err != nil {
		return err
	}
	defer split.Drop()

	if split.Owner() != a.StakeProgramAddr {
		return InstrErrIncorrectProgramId
	}

	if len(split.Data()) != StakeStateV2Size {
		return InstrErrInvalidAccountData
	}

	splitStakeState, err := UnmarshalStakeState(split.Data())
	if err != nil {
		return err
	}

	if splitStakeState.Status != StakeStateV2StatusUninitialized {
		return InstrErrInvalidAccountData
	}

	splitLamportBalance := split.Lamports()
	split.Drop()

	stakeAcct, err := instrCtx.BorrowInstructionAccount(txCtx, stakeAcctIdx)
	if err != nil {
		return err
	}
	defer stakeAcct.Drop()

	if lamports > stakeAcct.Lamports() {
		return InstrErrInsufficientFunds
	}

	stakeState, err := UnmarshalStakeState(stakeAcct.Data())
	if err != nil {
		return err
	}
	stakeAcct.Drop()

	switch stakeState.Status {
	case StakeStateV2StatusStake:
		{
			err = stakeState.Stake.Meta.Authorized.Check(signers, StakeAuthorizeStaker)
			if err != nil {
				return err
			}

			minimumDelegation := determineMinimumDelegation(execCtx.Features)

			var isActive bool
			if execCtx.Features.IsActive(features.RequireRentExemptSplitDestination) {
				clock, err := ReadClockSysvar(execCtx)
				if err != nil {
					return err
				}

				stakeHistory, err := ReadStakeHistorySysvar(execCtx)
				if err != nil {
					return err
				}

				epoch, err := NewWarmupCooldownRateEpoch(execCtx)
				if err != nil {
					return err
				}
				stakeHistoryEntry := stakeState.Stake.Stake.Delegation.StakeActivatingAndDeactivating(clock.Epoch, &stakeHistory, epoch)
				if stakeHistoryEntry.Effective > 0 {
					isActive = true
				}
			}

			validatedSplitInfo, err := validateSplitAmount(execCtx, txCtx, instrCtx, stakeAcctIdx, splitIdx, lamports, stakeState.Stake.Meta, minimumDelegation, isActive)
			if err != nil {
				return err
			}

			var remainingStakeDelta uint64
			var splitStakeAmount uint64

			if validatedSplitInfo.SrcRemainingBalance == 0 {
				remainingStakeDelta = safemath.SaturatingSubU64(lamports, stakeState.Stake.Meta.RentExemptReserve)
				splitStakeAmount = remainingStakeDelta
			} else {
				if safemath.SaturatingSubU64(stakeState.Stake.Stake.Delegation.StakeLamports, lamports) < minimumDelegation {
					return StakeErrInsufficientDelegation
				}
				remainingStakeDelta = lamports
				splitStakeAmount = safemath.SaturatingSubU64(lamports, safemath.SaturatingSubU64(validatedSplitInfo.DestRentExemptReserve, splitLamportBalance))
			}

			if splitStakeAmount < minimumDelegation {
				return StakeErrInsufficientDelegation
			}

			splitStake, err := stakeState.Stake.Stake.Split(remainingStakeDelta, splitStakeAmount)
			if err != nil {
				return err
			}

			splitMeta := stakeState.Stake.Meta
			splitMeta.RentExemptReserve = validatedSplitInfo.DestRentExemptReserve

			stakeAcct, err := instrCtx.BorrowInstructionAccount(txCtx, stakeAcctIdx)
			if err != nil {
				return err
			}
			defer stakeAcct.Drop()

			err = setStakeAccountState(stakeAcct, stakeState, execCtx.Features)
			if err != nil {
				return err
			}

			stakeAcct.Drop()

			split, err := instrCtx.BorrowInstructionAccount(txCtx, splitIdx)
			if err != nil {
				return err
			}
			defer split.Drop()

			newSplitStakeState := StakeStateV2{Status: StakeStateV2StatusStake, Stake: StakeStateV2Stake{Meta: splitMeta, Stake: splitStake, StakeFlags: stakeState.Stake.StakeFlags}}
			err = setStakeAccountState(split, &newSplitStakeState, execCtx.Features)
			if err != nil {
				return err
			}
			split.Drop()
		}

	case StakeStateV2StatusInitialized:
		{
			err = stakeState.Initialized.Meta.Authorized.Check(signers, StakeAuthorizeStaker)
			if err != nil {
				return err
			}

			validatedSplitInfo, err := validateSplitAmount(execCtx, txCtx, instrCtx, stakeAcctIdx, splitIdx, lamports, stakeState.Initialized.Meta, 0, false)
			if err != nil {
				return err
			}

			splitMeta := stakeState.Initialized.Meta
			splitMeta.RentExemptReserve = validatedSplitInfo.DestRentExemptReserve

			split, err := instrCtx.BorrowInstructionAccount(txCtx, splitIdx)
			if err != nil {
				return err
			}
			defer split.Drop()

			newStakeState := StakeStateV2{Status: StakeStateV2StatusInitialized, Initialized: StakeStateV2Initialized{Meta: splitMeta}}
			err = setStakeAccountState(split, &newStakeState, execCtx.Features)
			if err != nil {
				return err
			}
			split.Drop()
		}

	case StakeStateV2StatusUninitialized:
		{
			//mlog.Log.Debugf("StakeStateV2StatusUninitialized")
			idxInTx, err := instrCtx.IndexOfInstructionAccountInTransaction(stakeAcctIdx)
			if err != nil {
				return err
			}
			stakePubkey, err := txCtx.KeyOfAccountAtIndex(idxInTx)
			if err != nil {
				return err
			}

			err = verifySigner(stakePubkey, signers)
			if err != nil {
				return err
			}
		}

	default:
		{
			err = InstrErrInvalidAccountData
		}
	}

	// deinit account upon zero balance
	stakeAcct, err = instrCtx.BorrowInstructionAccount(txCtx, stakeAcctIdx)
	if err != nil {
		return err
	}
	defer stakeAcct.Drop()

	if lamports == stakeAcct.Lamports() {
		newStakeState := StakeStateV2{Status: StakeStateV2StatusUninitialized}
		err = setStakeAccountState(stakeAcct, &newStakeState, execCtx.Features)
		if err != nil {
			return err
		}
	}
	stakeAcct.Drop()

	split, err = instrCtx.BorrowInstructionAccount(txCtx, splitIdx)
	if err != nil {
		return err
	}
	defer split.Drop()

	err = split.CheckedAddLamports(lamports, execCtx.Features)
	if err != nil {
		return err
	}
	split.Drop()

	stakeAcct, err = instrCtx.BorrowInstructionAccount(txCtx, stakeAcctIdx)
	if err != nil {
		return err
	}
	err = stakeAcct.CheckedSubLamports(lamports, execCtx.Features)

	return err
}

func StakeProgramMerge(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx, stakeAcctIdx uint64, srcAcctIdx uint64, clock SysvarClock, stakeHistory SysvarStakeHistory, signers []solana.PublicKey) error {
	//mlog.Log.Debugf("StakeProgramMerge")

	srcAcct, err := instrCtx.BorrowInstructionAccount(txCtx, srcAcctIdx)
	if err != nil {
		return err
	}
	defer srcAcct.Drop()

	if srcAcct.Owner() != a.StakeProgramAddr {
		return InstrErrIncorrectProgramId
	}

	idxInTxStakeAcct, err := instrCtx.IndexOfInstructionAccountInTransaction(stakeAcctIdx)
	if err != nil {
		return err
	}

	idxInTxSrcAcct, err := instrCtx.IndexOfInstructionAccountInTransaction(srcAcctIdx)
	if err != nil {
		return err
	}

	if idxInTxStakeAcct == idxInTxSrcAcct {
		return InstrErrInvalidArgument
	}

	stakeAcct, err := instrCtx.BorrowInstructionAccount(txCtx, stakeAcctIdx)
	if err != nil {
		return err
	}
	defer stakeAcct.Drop()

	stakeAcctState, err := UnmarshalStakeState(stakeAcct.Data())
	if err != nil {
		return err
	}

	stakeAcctMergeKind, err := getMergeKindIfMergeable(execCtx, stakeAcctState, stakeAcct.Lamports(), clock, stakeHistory)
	if err != nil {
		return err
	}

	err = stakeAcctMergeKind.Meta().Authorized.Check(signers, StakeAuthorizeStaker)
	if err != nil {
		//mlog.Log.Debugf("staker did not sign")
		return err
	}

	sourceAcctState, err := UnmarshalStakeState(srcAcct.Data())
	if err != nil {
		return err
	}

	sourceAcctMergeKind, err := getMergeKindIfMergeable(execCtx, sourceAcctState, srcAcct.Lamports(), clock, stakeHistory)
	if err != nil {
		return err
	}

	mergedState, err := stakeAcctMergeKind.Merge(execCtx, sourceAcctMergeKind, clock)
	if err != nil {
		return err
	}

	if mergedState != nil {
		err = setStakeAccountState(stakeAcct, mergedState, execCtx.Features)
		if err != nil {
			return err
		}
	}

	uninitializedState := &StakeStateV2{Status: StakeStateV2StatusUninitialized}
	err = setStakeAccountState(srcAcct, uninitializedState, execCtx.Features)
	if err != nil {
		return err
	}

	lamports := srcAcct.Lamports()
	err = srcAcct.CheckedSubLamports(lamports, execCtx.Features)
	if err != nil {
		return err
	}

	err = stakeAcct.CheckedAddLamports(lamports, execCtx.Features)
	if err != nil {
		return err
	}

	return nil
}

func StakeProgramWithdraw(txCtx *TransactionCtx, instrCtx *InstructionCtx, stakeAcctIdx uint64, lamports uint64, toIndex uint64, clock SysvarClock, stakeHistory SysvarStakeHistory, withdrawAuthorityIdx uint64, custodianIdx *uint64, newRateActivationEpoch *uint64, f features.Features) error {
	//mlog.Log.Debugf("StakeProgramWithdraw")

	idxInTx, err := instrCtx.IndexOfInstructionAccountInTransaction(withdrawAuthorityIdx)
	if err != nil {
		return err
	}
	withdrawAuthorityPubkey, err := txCtx.KeyOfAccountAtIndex(idxInTx)
	if err != nil {
		return err
	}

	isSigner, err := instrCtx.IsInstructionAccountSigner(withdrawAuthorityIdx)
	if err != nil {
		return err
	}
	if !isSigner {
		return InstrErrMissingRequiredSignature
	}

	var signers []solana.PublicKey
	signers = append(signers, withdrawAuthorityPubkey)

	stakeAcct, err := instrCtx.BorrowInstructionAccount(txCtx, stakeAcctIdx)
	if err != nil {
		return err
	}
	defer stakeAcct.Drop()

	stakeState, err := UnmarshalStakeState(stakeAcct.Data())
	if err != nil {
		return err
	}

	var lockup StakeLockup
	var reserve uint64
	var isStaked bool

	switch stakeState.Status {
	case StakeStateV2StatusStake:
		{
			err = stakeState.Stake.Meta.Authorized.Check(signers, StakeAuthorizeWithdrawer)
			if err != nil {
				return err
			}

			var staked uint64
			if clock.Epoch >= stakeState.Stake.Stake.Delegation.DeactivationEpoch {
				staked = stakeState.Stake.Stake.Stake(clock.Epoch, &stakeHistory, newRateActivationEpoch)
			} else {
				staked = stakeState.Stake.Stake.Delegation.StakeLamports
			}

			stakedAndReserve, err := safemath.CheckedAddU64(staked, stakeState.Stake.Meta.RentExemptReserve)
			if err != nil {
				return InstrErrInsufficientFunds
			}

			lockup = stakeState.Stake.Meta.Lockup
			reserve = stakedAndReserve
			isStaked = staked != 0
		}

	case StakeStateV2StatusInitialized:
		{
			err = stakeState.Initialized.Meta.Authorized.Check(signers, StakeAuthorizeWithdrawer)
			if err != nil {
				return err
			}
			lockup = stakeState.Initialized.Meta.Lockup
			reserve = stakeState.Initialized.Meta.RentExemptReserve
			isStaked = false
		}

	case StakeStateV2StatusUninitialized:
		{
			err = verifySigner(stakeAcct.Key(), signers)
			if err != nil {
				return err
			}
			lockup = StakeLockup{}
			reserve = 0
			isStaked = false
		}

	default:
		{
			return InstrErrInvalidAccountData
		}
	}

	var custodianPubkey *solana.PublicKey
	if custodianIdx != nil {
		isSigner, err = instrCtx.IsInstructionAccountSigner(*custodianIdx)
		if err != nil {
			return err
		}

		if isSigner {
			idxInTx, err := instrCtx.IndexOfInstructionAccountInTransaction(*custodianIdx)
			if err != nil {
				return err
			}
			pk, err := txCtx.KeyOfAccountAtIndex(idxInTx)
			custodianPubkey = &pk
		}
	}

	if lockup.IsInForce(clock, custodianPubkey) {
		return StakeErrLockupInForce
	}

	lamportsAndReserve, err := safemath.CheckedAddU64(lamports, reserve)
	if err != nil {
		return InstrErrInsufficientFunds
	}

	if isStaked && lamportsAndReserve > stakeAcct.Lamports() {
		return InstrErrInsufficientFunds
	}

	if lamports != stakeAcct.Lamports() && lamportsAndReserve > stakeAcct.Lamports() {
		return InstrErrInsufficientFunds
	}

	if lamports == stakeAcct.Lamports() {
		uninitializedState := &StakeStateV2{Status: StakeStateV2StatusUninitialized}
		err = setStakeAccountState(stakeAcct, uninitializedState, f)
		if err != nil {
			return err
		}
	}

	err = stakeAcct.CheckedSubLamports(lamports, f)
	if err != nil {
		return err
	}

	stakeAcct.Drop()

	to, err := instrCtx.BorrowInstructionAccount(txCtx, toIndex)
	if err != nil {
		return err
	}
	defer to.Drop()

	err = to.CheckedAddLamports(lamports, f)
	return err
}

func StakeProgramDeactivate(execCtx *ExecutionCtx, stakeAcct *BorrowedAccount, clock SysvarClock, signers []solana.PublicKey) error {
	//mlog.Log.Debugf("StakeProgramDeactivate")

	stakeState, err := UnmarshalStakeState(stakeAcct.Data())
	if err != nil {
		return err
	}

	if stakeState.Status == StakeStateV2StatusStake {
		err = stakeState.Stake.Meta.Authorized.Check(signers, StakeAuthorizeStaker)
		if err != nil {
			return err
		}

		err = deactivateStake(execCtx, &stakeState.Stake.Stake, &stakeState.Stake.StakeFlags, clock.Epoch)
		if err != nil {
			return err
		}

		err = setStakeAccountState(stakeAcct, stakeState, execCtx.Features)
		return err
	} else {
		return InstrErrInvalidAccountData
	}
}

func StakeProgramSetLockup(stakeAcct *BorrowedAccount, lockup StakeInstrSetLockup, signers []solana.PublicKey, clock SysvarClock, f features.Features) error {
	//mlog.Log.Debugf("StakeProgramSetLockup")

	stakeState, err := UnmarshalStakeState(stakeAcct.Data())
	if err != nil {
		return err
	}

	switch stakeState.Status {
	case StakeStateV2StatusInitialized:
		{
			err = stakeState.Initialized.Meta.SetLockup(lockup, signers, clock)
			if err != nil {
				return err
			}

			err = setStakeAccountState(stakeAcct, stakeState, f)
			return err
		}

	case StakeStateV2StatusStake:
		{
			err = stakeState.Stake.Meta.SetLockup(lockup, signers, clock)
			if err != nil {
				return err
			}

			err = setStakeAccountState(stakeAcct, stakeState, f)
			return err
		}

	default:
		{
			return InstrErrInvalidAccountData
		}
	}
}

func StakeProgramMoveStake(execCtx *ExecutionCtx, instrCtx *InstructionCtx, srcAcctIdx uint64, lamports uint64, dstAcctIdx uint64, stakeAuthorityIdx uint64) error {
	txCtx := execCtx.TransactionContext

	srcAcct, err := instrCtx.BorrowInstructionAccount(txCtx, srcAcctIdx)
	if err != nil {
		return err
	}
	defer srcAcct.Drop()

	dstAcct, err := instrCtx.BorrowInstructionAccount(txCtx, dstAcctIdx)
	if err != nil {
		return err
	}
	defer dstAcct.Drop()

	srcMergeKind, dstMergeKind, err := moveStakeOrLamportsSharedChecks(execCtx, txCtx, instrCtx, srcAcct, lamports, dstAcct, stakeAuthorityIdx)
	if err != nil {
		return err
	}

	if len(srcAcct.Data()) != StakeStateV2Size || len(dstAcct.Data()) != StakeStateV2Size {
		return InstrErrInvalidAccountData
	}

	if srcMergeKind.Status != MergeKindStatusFullyActive {
		return InstrErrInvalidAccountData
	}

	srcStake := srcMergeKind.FullyActive.Stake
	srcMeta := srcMergeKind.FullyActive.Meta
	minimumDelegation := determineMinimumDelegation(execCtx.Features)
	srcEffectiveStake := srcStake.Delegation.StakeLamports

	srcFinalStake, err := safemath.CheckedSubU64(srcEffectiveStake, lamports)
	if err != nil {
		return InstrErrInvalidArgument
	}

	if srcFinalStake != 0 && srcFinalStake < minimumDelegation {
		return InstrErrInvalidArgument
	}

	var destinationMeta *Meta

	switch dstMergeKind.Status {
	case MergeKindStatusFullyActive:
		{
			dstMeta := dstMergeKind.FullyActive.Meta
			dstStake := dstMergeKind.FullyActive.Stake

			if srcStake.Delegation.VoterPubkey != dstStake.Delegation.VoterPubkey {
				return StakeErrVoteAddressMismatch
			}

			dstEffectiveStake := dstStake.Delegation.StakeLamports
			dstFinalStake, err := safemath.CheckedAddU64(dstEffectiveStake, lamports)
			if err != nil {
				return InstrErrArithmeticOverflow
			}

			if dstFinalStake < minimumDelegation {
				return InstrErrInvalidArgument
			}

			err = dstStake.MergeDelegationStakeAndCreditsObserved(lamports, srcStake.CreditsObserved)
			if err != nil {
				return err
			}

			newDstStakeState := &StakeStateV2{Status: StakeStateV2StatusStake, Stake: StakeStateV2Stake{Meta: dstMeta, Stake: dstStake}}
			err = setStakeAccountState(dstAcct, newDstStakeState, execCtx.Features)
			if err != nil {
				return err
			}

			destinationMeta = &dstMeta
		}

	case MergeKindStatusInactive:
		{
			dstMeta := dstMergeKind.Inactive.Meta

			if lamports < minimumDelegation {
				return InstrErrInvalidArgument
			}

			dstStake := srcStake
			dstStake.Delegation.StakeLamports = lamports

			newDstStakeState := &StakeStateV2{Status: StakeStateV2StatusStake, Stake: StakeStateV2Stake{Meta: dstMeta, Stake: dstStake}}
			err = setStakeAccountState(dstAcct, newDstStakeState, execCtx.Features)
			if err != nil {
				return err
			}

			destinationMeta = &dstMeta
		}

	default:
		{
			return InstrErrInvalidAccountData
		}
	}

	if srcFinalStake == 0 {
		newSrcAcctStakeState := &StakeStateV2{Status: StakeStateV2StatusInitialized, Initialized: StakeStateV2Initialized{srcMeta}}
		err = setStakeAccountState(srcAcct, newSrcAcctStakeState, execCtx.Features)
		if err != nil {
			return err
		}
	} else {
		srcStake.Delegation.StakeLamports = srcFinalStake

		newSrcAcctStakeState := &StakeStateV2{Status: StakeStateV2StatusStake, Stake: StakeStateV2Stake{Meta: srcMeta, Stake: srcStake}}
		err = setStakeAccountState(srcAcct, newSrcAcctStakeState, execCtx.Features)
		if err != nil {
			return err
		}
	}

	err = srcAcct.CheckedSubLamports(lamports, execCtx.Features)
	if err != nil {
		return err
	}

	err = dstAcct.CheckedAddLamports(lamports, execCtx.Features)
	if err != nil {
		return err
	}

	if srcAcct.Lamports() < srcMeta.RentExemptReserve || dstAcct.Lamports() < destinationMeta.RentExemptReserve {
		//mlog.Log.Debugf("Delegation calculations violated lamport balance assumptions")
		return InstrErrInvalidArgument
	}

	return nil
}

func StakeProgramMoveLamports(execCtx *ExecutionCtx, instrCtx *InstructionCtx, srcAcctIdx uint64, lamports uint64, dstAcctIdx uint64, stakeAuthorityIdx uint64) error {
	txCtx := execCtx.TransactionContext

	srcAcct, err := instrCtx.BorrowInstructionAccount(txCtx, srcAcctIdx)
	if err != nil {
		return err
	}
	defer srcAcct.Drop()

	dstAcct, err := instrCtx.BorrowInstructionAccount(txCtx, dstAcctIdx)
	if err != nil {
		return err
	}
	defer dstAcct.Drop()

	srcMergeKind, _, err := moveStakeOrLamportsSharedChecks(execCtx, txCtx, instrCtx, srcAcct, lamports, dstAcct, stakeAuthorityIdx)
	if err != nil {
		return err
	}

	var srcFreeLamports uint64

	switch srcMergeKind.Status {
	case MergeKindStatusFullyActive:
		{
			srcMeta := srcMergeKind.FullyActive.Meta
			srcStake := srcMergeKind.FullyActive.Stake

			srcFreeLamports = safemath.SaturatingSubU64(safemath.SaturatingSubU64(srcAcct.Lamports(), srcStake.Delegation.StakeLamports), srcMeta.RentExemptReserve)
		}

	case MergeKindStatusInactive:
		{
			srcMeta := srcMergeKind.FullyActive.Meta
			srcLamports := srcMergeKind.Inactive.StakeLamports

			srcFreeLamports = safemath.SaturatingSubU64(srcLamports, srcMeta.RentExemptReserve)
		}

	default:
		{
			return InstrErrInvalidAccountData
		}
	}

	if lamports > srcFreeLamports {
		return InstrErrInvalidArgument
	}

	err = srcAcct.CheckedSubLamports(lamports, execCtx.Features)
	if err != nil {
		return err
	}

	err = dstAcct.CheckedAddLamports(lamports, execCtx.Features)

	return err
}

func moveStakeOrLamportsSharedChecks(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx, srcAcct *BorrowedAccount, lamports uint64, dstAcct *BorrowedAccount, stakeAuthorityIdx uint64) (*MergeKind, *MergeKind, error) {
	stakeAuthPkIdx, err := instrCtx.IndexOfInstructionAccountInTransaction(stakeAuthorityIdx)
	if err != nil {
		return nil, nil, err
	}

	stakeAuthorityPubkey, err := txCtx.KeyOfAccountAtIndex(stakeAuthPkIdx)
	if err != nil {
		return nil, nil, err
	}

	isSigner, err := instrCtx.IsInstructionAccountSigner(stakeAuthorityIdx)
	if err != nil {
		return nil, nil, err
	}

	if !isSigner {
		return nil, nil, InstrErrMissingRequiredSignature
	}

	signers := []solana.PublicKey{stakeAuthorityPubkey}

	if srcAcct.Owner() != a.StakeProgramAddr || dstAcct.Owner() != a.StakeProgramAddr {
		return nil, nil, InstrErrIncorrectProgramId
	}

	if srcAcct.Key() == dstAcct.Key() {
		return nil, nil, InstrErrInvalidInstructionData
	}

	if !srcAcct.IsWritable() || !dstAcct.IsWritable() {
		return nil, nil, InstrErrInvalidInstructionData
	}

	if lamports == 0 {
		return nil, nil, InstrErrInvalidArgument
	}

	clock, err := ReadClockSysvar(execCtx)
	if err != nil {
		return nil, nil, err
	}

	stakeHistory, err := ReadStakeHistorySysvar(execCtx)
	if err != nil {
		return nil, nil, err
	}

	srcStakeState, err := UnmarshalStakeState(srcAcct.Data())
	if err != nil {
		return nil, nil, err
	}

	srcMergeKind, err := getMergeKindIfMergeable(execCtx, srcStakeState, srcAcct.Lamports(), clock, stakeHistory)
	if err != nil {
		return nil, nil, err
	}

	err = srcMergeKind.Meta().Authorized.Check(signers, StakeAuthorizeStaker)
	if err != nil {
		return nil, nil, err
	}

	dstStakeState, err := UnmarshalStakeState(dstAcct.Data())
	if err != nil {
		return nil, nil, err
	}

	dstMergeKind, err := getMergeKindIfMergeable(execCtx, dstStakeState, dstAcct.Lamports(), clock, stakeHistory)
	if err != nil {
		return nil, nil, err
	}

	err = metasCanMerge(srcMergeKind.Meta(), dstMergeKind.Meta(), clock)
	if err != nil {
		return nil, nil, err
	}

	return srcMergeKind, dstMergeKind, nil
}

const MinimumDelinquentEpochsForDeactivation = 5

func acceptableReferenceEpochCredits(epochCredits []EpochCredits, currentEpoch uint64) bool {
	epochIndex, err := safemath.CheckedSubU64(uint64(len(epochCredits)), MinimumDelinquentEpochsForDeactivation)
	if err != nil {
		return false
	} else {
		epoch := currentEpoch

		relevantEpochCredits := epochCredits[epochIndex:]
		for i, j := 0, len(relevantEpochCredits)-1; i < j; i, j = i+1, j-1 {
			relevantEpochCredits[i], relevantEpochCredits[j] = relevantEpochCredits[j], relevantEpochCredits[i]
		}

		for _, epochCreditsVal := range relevantEpochCredits {
			if epochCreditsVal.Epoch != epoch {
				return false
			}
			epoch = safemath.SaturatingSubU64(epoch, 1)
		}
		return true
	}
}

func eligibleForDeactivateDelinquent(epochCredits []EpochCredits, currentEpoch uint64) bool {
	if len(epochCredits) == 0 {
		return true
	}

	lastElement := epochCredits[len(epochCredits)-1]
	minimumEpoch, err := safemath.CheckedSubU64(currentEpoch, MinimumDelinquentEpochsForDeactivation)
	if err != nil {
		return false
	}

	return lastElement.Epoch <= minimumEpoch

}

func StakeProgramDeactivateDelinquent(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx, stakeAcct *BorrowedAccount, delinquentVoteAcctIdx uint64, referenceVoteAcctIdx uint64, currentEpoch uint64) error {
	//mlog.Log.Debugf("StakeProgramDeactivateDelinquent")

	delinquentVoteAcctIdxInTx, err := instrCtx.IndexOfInstructionAccountInTransaction(delinquentVoteAcctIdx)
	if err != nil {
		return err
	}

	delinquentVoteAcctPubkey, err := txCtx.KeyOfAccountAtIndex(delinquentVoteAcctIdxInTx)
	if err != nil {
		return err
	}

	delinquentVoteAcct, err := instrCtx.BorrowInstructionAccount(txCtx, delinquentVoteAcctIdx)
	if err != nil {
		return err
	}
	defer delinquentVoteAcct.Drop()

	if delinquentVoteAcct.Owner() != a.VoteProgramAddr {
		return InstrErrIncorrectProgramId
	}

	delinquentVoteStateVersioned, err := UnmarshalVersionedVoteState(delinquentVoteAcct.Data())
	if err != nil {
		return err
	}
	delinquentVoteState := delinquentVoteStateVersioned.ConvertToCurrent()

	referenceVoteAcct, err := instrCtx.BorrowInstructionAccount(txCtx, referenceVoteAcctIdx)
	if err != nil {
		return err
	}
	defer referenceVoteAcct.Drop()

	if referenceVoteAcct.Owner() != a.VoteProgramAddr {
		return InstrErrIncorrectProgramId
	}

	referenceVoteStateVersioned, err := UnmarshalVersionedVoteState(referenceVoteAcct.Data())
	if err != nil {
		return err
	}
	referenceVoteState := referenceVoteStateVersioned.ConvertToCurrent()

	if !acceptableReferenceEpochCredits(referenceVoteState.EpochCredits, currentEpoch) {
		return StakeErrInsufficientReferenceVotes
	}

	stakeState, err := UnmarshalStakeState(stakeAcct.Data())
	if err != nil {
		return err
	}

	if stakeState.Status == StakeStateV2StatusStake {
		if stakeState.Stake.Stake.Delegation.VoterPubkey != delinquentVoteAcctPubkey {
			return StakeErrVoteAddressMismatch
		}

		if eligibleForDeactivateDelinquent(delinquentVoteState.EpochCredits, currentEpoch) {
			//mlog.Log.Debugf("eligible for DeactivateDelinquent")
			err = deactivateStake(execCtx, &stakeState.Stake.Stake, &stakeState.Stake.StakeFlags, currentEpoch)
			if err != nil {
				return err
			}
			err = setStakeAccountState(stakeAcct, stakeState, execCtx.Features)
			return err
		} else {
			return StakeErrMinimumDelinquentEpochsForDeactivationNotMet
		}

	} else {
		return InstrErrInvalidAccountData
	}
}

func StakeProgramRedelegate(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx, stakeAcct *BorrowedAccount, uninitializedStakeAcctIdx uint64, voteAcctIdx uint64, signers []solana.PublicKey) error {
	//mlog.Log.Debugf("StakeProgramRedelegate")

	clock, err := ReadClockSysvar(execCtx)
	if err != nil {
		return err
	}

	uninitializedStakeAcct, err := instrCtx.BorrowInstructionAccount(txCtx, uninitializedStakeAcctIdx)
	if err != nil {
		return err
	}
	defer uninitializedStakeAcct.Drop()

	if uninitializedStakeAcct.Owner() != a.StakeProgramAddr {
		return InstrErrIncorrectProgramId
	}

	if len(uninitializedStakeAcct.Data()) != StakeStateV2Size {
		return InstrErrInvalidAccountData
	}

	uninitStakeState, err := UnmarshalStakeState(uninitializedStakeAcct.Data())
	if err != nil {
		return err
	}

	if uninitStakeState.Status != StakeStateV2StatusUninitialized {
		return InstrErrAccountAlreadyInitialized
	}

	voteAcct, err := instrCtx.BorrowInstructionAccount(txCtx, voteAcctIdx)
	if err != nil {
		return err
	}
	defer voteAcct.Drop()

	if voteAcct.Owner() != a.VoteProgramAddr {
		return InstrErrIncorrectProgramId
	}

	votePubkey := voteAcct.Key()
	versionedVoteState, err := UnmarshalVersionedVoteState(voteAcct.Data())
	if err != nil {
		return err
	}

	stakeState, err := UnmarshalStakeState(stakeAcct.Data())
	if err != nil {
		return err
	}

	var stakeMeta *Meta
	var effectiveStake uint64
	if stakeState.Status == StakeStateV2StatusStake {
		status, err := getStakeStatus(execCtx, &stakeState.Stake.Stake, clock)
		if err != nil {
			return err
		}

		if status.Effective == 0 || status.Activating != 0 || status.Deactivating != 0 {
			return StakeErrRedelegateTransientOrInactiveStake
		}
		if stakeState.Stake.Stake.Delegation.VoterPubkey == votePubkey {
			return StakeErrRedelegateToSameVoteAccount
		}
		stakeMeta = &stakeState.Stake.Meta
		effectiveStake = status.Effective
	} else {
		return InstrErrInvalidAccountData
	}

	err = StakeProgramDeactivate(execCtx, stakeAcct, clock, signers)
	if err != nil {
		return err
	}

	err = stakeAcct.CheckedSubLamports(effectiveStake, execCtx.Features)
	if err != nil {
		return err
	}
	err = uninitializedStakeAcct.CheckedAddLamports(effectiveStake, execCtx.Features)
	if err != nil {
		return err
	}

	rent, err := ReadRentSysvar(execCtx)
	if err != nil {
		return err
	}

	uninitializedStakeMeta := *stakeMeta
	uninitializedStakeMeta.RentExemptReserve = rent.MinimumBalance(uint64(len(uninitializedStakeAcct.Data())))

	stakeAmount, err := validateAndReturnDelegatedAmount(uninitializedStakeAcct, uninitializedStakeMeta, execCtx.Features)
	if err != nil {
		return err
	}

	credits := versionedVoteState.ConvertToCurrent().Credits()
	newState := StakeStateV2{Status: StakeStateV2StatusStake,
		Stake: StakeStateV2Stake{Meta: uninitializedStakeMeta,
			Stake: Stake{Delegation: Delegation{VoterPubkey: votePubkey, StakeLamports: stakeAmount, ActivationEpoch: clock.Epoch, DeactivationEpoch: math.MaxUint64, WarmupCooldownRate: 0.25},
				CreditsObserved: credits}, StakeFlags: StakeFlagsMustFullyActivateBeforeDeactivationIsPermitted}}

	err = setStakeAccountState(uninitializedStakeAcct, &newState, execCtx.Features)

	return err
}
