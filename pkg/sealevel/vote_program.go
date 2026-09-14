package sealevel

import (
	"errors"
	"math"
	"sync"
	"unicode/utf8"

	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/gammazero/deque"
	"k8s.io/klog/v2"
)

const (
	VoteProgramInstrTypeInitializeAccount = iota
	VoteProgramInstrTypeAuthorize
	VoteProgramInstrTypeVote
	VoteProgramInstrTypeWithdraw
	VoteProgramInstrTypeUpdateValidatorIdentity
	VoteProgramInstrTypeUpdateCommission
	VoteProgramInstrTypeVoteSwitch
	VoteProgramInstrTypeAuthorizeChecked
	VoteProgramInstrTypeUpdateVoteState
	VoteProgramInstrTypeUpdateVoteStateSwitch
	VoteProgramInstrTypeAuthorizeWithSeed
	VoteProgramInstrTypeAuthorizeCheckedWithSeed
	VoteProgramInstrTypeCompactUpdateVoteState
	VoteProgramInstrTypeCompactUpdateVoteStateSwitch
	VoteProgramInstrTypeTowerSync
	VoteProgramInstrTypeTowerSyncSwitch
	VoteProgramInstrTypeInitializeAccountV2 = 16
)

var (
	VoteErrTooSoonToReauthorize        = errors.New("VoteErrTooSoonToReauthorize")
	VoteErrCommissionUpdateTooLate     = errors.New("VoteErrCommissionUpdateTooLate")
	VoteErrEmptySlots                  = errors.New("VoteErrEmptySlots")
	VoteErrVotesTooOldAllFiltered      = errors.New("VoteErrVotesTooOldAllFiltered")
	VoteErrVoteTooOld                  = errors.New("VoteErrVoteTooOld")
	VoteErrSlotsMismatch               = errors.New("VoteErrSlotsMismatch")
	VoteErrSlotHashMismatch            = errors.New("VoteErrSlotHashMismatch")
	VoteErrTimestampTooOld             = errors.New("VoteErrTimestampTooOld")
	VoteErrSlotsNotOrdered             = errors.New("VoteErrSlotsNotOrdered")
	VoteErrRootOnDifferentFork         = errors.New("VoteErrRootOnDifferentFork")
	VoteErrTooManyVotes                = errors.New("VoteErrTooManyVotes")
	VoteErrRootRollback                = errors.New("VoteErrRootRollback")
	VoteErrZeroConfirmations           = errors.New("VoteErrZeroConfirmations")
	VoteErrConfirmationTooLarge        = errors.New("VoteErrConfirmationTooLarge")
	VoteErrSlotSmallerThanRoot         = errors.New("VoteErrSlotSmallerThanRoot")
	VoteErrConfirmationsNotOrdered     = errors.New("VoteErrConfirmationsNotOrdered")
	VoteErrNewVoteStateLockoutMismatch = errors.New("VoteErrNewVoteStateLockoutMismatch")
	VoteErrLockoutConflict             = errors.New("VoteErrLockoutConflict")
	VoteErrConfirmationRollback        = errors.New("VoteErrConfirmationRollback")
	VoteErrActiveVoteAccountClose      = errors.New("VoteErrActiveVoteAccountClose")
)

type VoteInstrVoteInit struct {
	NodePubkey           solana.PublicKey
	AuthorizedVoter      solana.PublicKey
	AuthorizedWithdrawer solana.PublicKey
	Commission           byte
}

type VoteInstrVoteInitV2 struct {
	NodePubkey                          solana.PublicKey
	AuthorizedVoter                     solana.PublicKey
	AuthorizedVoterBLSPubkey            [blsPublicKeyCompressedSize]byte
	AuthorizedVoterBLSProofOfPossession [blsProofOfPossessionCompressedSize]byte
	AuthorizedWithdrawer                solana.PublicKey
	InflationRewardsCommissionBps       uint16
	BlockRevenueCommissionBps           uint16
}

const (
	VoteAuthorizeTypeVoter = iota
	VoteAuthorizeTypeWithdrawer
)

type VoteInstrVoteAuthorize struct {
	Pubkey    solana.PublicKey
	Authorize VoteAuthorizeKind
}

type VoteInstrVote struct {
	Slots     []uint64
	Hash      [32]byte
	Timestamp *int64
}

type VoteInstrWithdraw struct {
	Lamports uint64
}

type VoteInstrUpdateCommission struct {
	Commission byte
}

type VoteInstrVoteSwitch struct {
	Vote VoteInstrVote
	Hash [32]byte
}

type VoteInstrVoteAuthorizeChecked struct {
	Authorize VoteAuthorizeKind
}

type VoteInstrUpdateVoteState struct {
	Lockouts  deque.Deque[VoteLockout]
	Root      *uint64
	Hash      [32]byte
	Timestamp *int64
}

type VoteInstrUpdateVoteStateSwitch struct {
	UpdateVoteState VoteInstrUpdateVoteState
	Hash            [32]byte
}

type VoteInstrAuthorizeWithSeed struct {
	Authorize                       VoteAuthorizeKind
	CurrentAuthorityDerivedKeyOwner solana.PublicKey
	CurrentAuthorityDerivedKeySeed  string
	NewAuthority                    solana.PublicKey
}

type VoteInstrAuthorizeCheckedWithSeed struct {
	Authorize                       VoteAuthorizeKind
	CurrentAuthorityDerivedKeyOwner solana.PublicKey
	CurrentAuthorityDerivedKeySeed  string
}

type LockoutOffset struct {
	Offset            uint64
	ConfirmationCount byte
}

type CompactUpdateVoteState struct {
	Root           uint64
	LockoutOffsets []LockoutOffset
	Hash           [32]byte
	Timestamp      *int64
}

type VoteInstrCompactUpdateVoteState struct {
	UpdateVoteState VoteInstrUpdateVoteState
}

type VoteInstrCompactUpdateVoteStateSwitch struct {
	UpdateVoteState VoteInstrUpdateVoteState
	Hash            [32]byte
}

type VoteInstrTowerSync struct {
	Lockouts  deque.Deque[VoteLockout]
	Root      *uint64
	Hash      [32]byte
	Timestamp *int64
	BlockId   [32]byte
}

type VoteInstrTowerSyncSwitch struct {
	TowerSync VoteInstrTowerSync
	Hash      [32]byte
}

func (voteInit *VoteInstrVoteInit) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	nodePk, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteInit.NodePubkey[:], nodePk)

	authVoter, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteInit.AuthorizedVoter[:], authVoter)

	authWithdrawer, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteInit.AuthorizedWithdrawer[:], authWithdrawer)

	voteInit.Commission, err = decoder.ReadByte()
	return err
}

func (voteInit *VoteInstrVoteInitV2) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	nodePubkey, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteInit.NodePubkey[:], nodePubkey)

	authorizedVoter, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteInit.AuthorizedVoter[:], authorizedVoter)

	blsPubkey, err := decoder.ReadBytes(blsPublicKeyCompressedSize)
	if err != nil {
		return err
	}
	copy(voteInit.AuthorizedVoterBLSPubkey[:], blsPubkey)

	blsProof, err := decoder.ReadBytes(blsProofOfPossessionCompressedSize)
	if err != nil {
		return err
	}
	copy(voteInit.AuthorizedVoterBLSProofOfPossession[:], blsProof)

	authorizedWithdrawer, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteInit.AuthorizedWithdrawer[:], authorizedWithdrawer)

	voteInit.InflationRewardsCommissionBps, err = decoder.ReadUint16(bin.LE)
	if err != nil {
		return err
	}
	voteInit.BlockRevenueCommissionBps, err = decoder.ReadUint16(bin.LE)
	return err
}

func (voteAuthorize *VoteInstrVoteAuthorize) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	pk, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteAuthorize.Pubkey[:], pk)

	return voteAuthorize.Authorize.UnmarshalWithDecoder(decoder)
}

func (vote *VoteInstrVote) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	slotsLen, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if slotsLen > uint64(decoder.Remaining()/8) {
		return InstrErrInvalidInstructionData
	}

	for count := uint64(0); count < slotsLen; count++ {
		slot, err := decoder.ReadUint64(bin.LE)
		if err != nil {
			return err
		}
		vote.Slots = append(vote.Slots, slot)
	}

	hash, err := decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	copy(vote.Hash[:], hash)

	hasTimestamp, err := ReadBool(decoder)
	if err != nil {
		return err
	}

	if hasTimestamp {
		timestamp, err := decoder.ReadInt64(bin.LE)
		if err != nil {
			return err
		}
		vote.Timestamp = &timestamp
	}

	if decoder.Position() > 1232 {
		return InstrErrInvalidInstructionData
	}

	return nil
}

func (withdraw *VoteInstrWithdraw) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	withdraw.Lamports, err = decoder.ReadUint64(bin.LE)
	return err
}

func (updateCommission *VoteInstrUpdateCommission) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	updateCommission.Commission, err = decoder.ReadByte()
	return err
}

func (voteSwitch *VoteInstrVoteSwitch) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	err := voteSwitch.Vote.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	hash, err := decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	copy(voteSwitch.Hash[:], hash)

	if decoder.Position() > 1232 {
		return InstrErrInvalidInstructionData
	}

	return nil
}

func (voteAuthChecked *VoteInstrVoteAuthorizeChecked) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	return voteAuthChecked.Authorize.UnmarshalWithDecoder(decoder)
}

func (updateVoteState *VoteInstrUpdateVoteState) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	numLockouts, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	_, err = safemath.CheckedMulU64(numLockouts, 12)
	if err != nil {
		return err
	}
	if numLockouts > MaxLockoutHistory || numLockouts > uint64(decoder.Remaining()/12) {
		return InstrErrInvalidInstructionData
	}

	updateVoteState.Lockouts.Clear()
	updateVoteState.Lockouts.SetBaseCap(int(numLockouts))
	for count := uint64(0); count < numLockouts; count++ {
		var lockout VoteLockout
		err = lockout.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		updateVoteState.Lockouts.PushBack(lockout)
	}

	hasRoot, err := ReadBool(decoder)
	if err != nil {
		return err
	}

	if hasRoot {
		root, err := decoder.ReadUint64(bin.LE)
		if err != nil {
			return err
		}
		updateVoteState.Root = &root
	}

	hash, err := decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	copy(updateVoteState.Hash[:], hash)

	hasTimestamp, err := ReadBool(decoder)
	if err != nil {
		return err
	}

	if hasTimestamp {
		timestamp, err := decoder.ReadInt64(bin.LE)
		if err != nil {
			return err
		}
		updateVoteState.Timestamp = &timestamp
	}

	if decoder.Position() > 1232 {
		return InstrErrInvalidInstructionData
	}

	return nil
}

func (uvss *VoteInstrUpdateVoteStateSwitch) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	err := uvss.UpdateVoteState.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	hash, err := decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	copy(uvss.Hash[:], hash)

	if decoder.Position() > 1232 {
		return InstrErrInvalidInstructionData
	}

	return nil
}

func (updateVoteState *VoteInstrUpdateVoteState) BuildFromCompactUpdateVoteState(compactUpdateVoteState *CompactUpdateVoteState) error {
	if compactUpdateVoteState.Root != math.MaxUint64 {
		updateVoteState.Root = &compactUpdateVoteState.Root
	}

	var slot uint64
	if updateVoteState.Root != nil {
		slot = *updateVoteState.Root
	}

	updateVoteState.Lockouts.Clear()
	for _, lockoutOffset := range compactUpdateVoteState.LockoutOffsets {
		nextSlot, err := safemath.CheckedAddU64(slot, lockoutOffset.Offset)
		if err != nil {
			return InstrErrInvalidInstructionData
		}
		updateVoteState.Lockouts.PushBack(VoteLockout{Slot: nextSlot, ConfirmationCount: uint32(lockoutOffset.ConfirmationCount)})
		slot = nextSlot
	}

	updateVoteState.Hash = compactUpdateVoteState.Hash
	updateVoteState.Timestamp = compactUpdateVoteState.Timestamp

	return nil
}

func (compactUpdateVoteState *VoteInstrCompactUpdateVoteState) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var compactUpdate CompactUpdateVoteState
	err := compactUpdate.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}
	return compactUpdateVoteState.UpdateVoteState.BuildFromCompactUpdateVoteState(&compactUpdate)
}

func (compactUpdateVoteState *VoteInstrCompactUpdateVoteStateSwitch) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var compactUpdate CompactUpdateVoteState
	err := compactUpdate.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}
	err = compactUpdateVoteState.UpdateVoteState.BuildFromCompactUpdateVoteState(&compactUpdate)
	if err != nil {
		return err
	}
	hash, err := decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	copy(compactUpdate.Hash[:], hash)
	return nil
}

func (authWithSeed *VoteInstrAuthorizeWithSeed) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	if err := authWithSeed.Authorize.UnmarshalWithDecoder(decoder); err != nil {
		return err
	}

	currentAuthorityDerivedKeyOwner, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(authWithSeed.CurrentAuthorityDerivedKeyOwner[:], currentAuthorityDerivedKeyOwner)

	authWithSeed.CurrentAuthorityDerivedKeySeed, err = decoder.ReadRustString()
	if err != nil {
		return err
	}
	if !utf8.ValidString(authWithSeed.CurrentAuthorityDerivedKeySeed) {
		return InstrErrInvalidInstructionData
	}

	newAuthority, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(authWithSeed.NewAuthority[:], newAuthority)

	return nil
}

func (acws *VoteInstrAuthorizeCheckedWithSeed) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	if err := acws.Authorize.UnmarshalWithDecoder(decoder); err != nil {
		return err
	}

	currentAuthorityDerivedKeyOwner, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(acws.CurrentAuthorityDerivedKeyOwner[:], currentAuthorityDerivedKeyOwner)

	acws.CurrentAuthorityDerivedKeySeed, err = decoder.ReadRustString()
	if err != nil {
		return err
	}
	if !utf8.ValidString(acws.CurrentAuthorityDerivedKeySeed) {
		return InstrErrInvalidInstructionData
	}

	return nil
}

func (lockoutOffset *LockoutOffset) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	lockoutOffset.Offset, err = decoder.ReadUvarint64()
	if err != nil {
		return err
	}

	lockoutOffset.ConfirmationCount, err = decoder.ReadByte()
	return err
}

func (cuvs *CompactUpdateVoteState) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	cuvs.Root, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	lockoutsLen, err := decoder.ReadCompactU16()
	if err != nil {
		return err
	}
	if lockoutsLen > MaxLockoutHistory {
		return InstrErrInvalidInstructionData
	}

	for count := 0; count < lockoutsLen; count++ {
		var lockoutOffset LockoutOffset
		err = lockoutOffset.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		cuvs.LockoutOffsets = append(cuvs.LockoutOffsets, lockoutOffset)
	}

	hash, err := decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	copy(cuvs.Hash[:], hash)

	hasTimestamp, err := ReadBool(decoder)
	if err != nil {
		return err
	}

	if hasTimestamp {
		timestamp, err := decoder.ReadInt64(bin.LE)
		if err != nil {
			return err
		}
		cuvs.Timestamp = &timestamp
	}
	return nil
}

func (towerSync *VoteInstrTowerSync) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	root, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	if root != math.MaxUint64 {
		towerSync.Root = &root
	}

	var lockoutOffsetsLen int
	lockoutOffsetsLen, err = decoder.ReadCompactU16()
	if err != nil {
		return err
	}
	if lockoutOffsetsLen > MaxLockoutHistory {
		return InstrErrInvalidInstructionData
	}

	var lastSlot uint64
	if towerSync.Root != nil {
		lastSlot = *towerSync.Root
	}

	towerSync.Lockouts.Clear()
	towerSync.Lockouts.SetBaseCap(int(lockoutOffsetsLen))
	for i := uint64(0); i < uint64(lockoutOffsetsLen); i++ {
		var lockoutOffset LockoutOffset
		err = lockoutOffset.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}

		slot, err := safemath.CheckedAddU64(lastSlot, lockoutOffset.Offset)
		if err != nil {
			return err
		}

		lo := VoteLockout{Slot: slot, ConfirmationCount: uint32(lockoutOffset.ConfirmationCount)}
		towerSync.Lockouts.PushBack(lo)
		lastSlot = lo.Slot
	}

	hashBytes, err := decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	copy(towerSync.Hash[:], hashBytes)

	hasTimestamp, err := decoder.ReadBool()
	if err != nil {
		return err
	}

	if hasTimestamp {
		ts, err := decoder.ReadInt64(bin.LE)
		if err != nil {
			return err
		}
		towerSync.Timestamp = &ts
	}

	blockIdBytes, err := decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	copy(towerSync.BlockId[:], blockIdBytes)

	return nil
}

func (towerSyncSwitch *VoteInstrTowerSyncSwitch) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	err := towerSyncSwitch.TowerSync.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	hashBytes, err := decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	copy(towerSyncSwitch.Hash[:], hashBytes)

	return nil
}

func VoteProgramExecute(execCtx *ExecutionCtx) error {
	err := execCtx.ComputeMeter.Consume(cu.CUVoteProgramDefaultComputeUnits)
	if err != nil {
		return InstrErrComputationalBudgetExceeded
	}

	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	me, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}

	if me.Owner() != a.VoteProgramAddr {
		return InstrErrInvalidAccountOwner
	}

	txCtx.ModifiedVoteAccts = true

	signers, err := instrCtx.Signers(txCtx)
	if err != nil {
		return err
	}
	me.Drop()

	decoder := bin.NewBinDecoder(instrCtx.Data)

	instructionType, err := decoder.ReadUint32(bin.LE)
	if err != nil {
		return InstrErrInvalidInstructionData
	}

	var isVoteSwitch bool
	var isUpdateVoteStateSwitch bool
	var isTowerSyncSwitch bool

	switch instructionType {
	case VoteProgramInstrTypeInitializeAccount:
		{
			var voteInit VoteInstrVoteInit
			err = voteInit.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			err = checkAcctForRentSysvar(txCtx, instrCtx, 1)
			if err != nil {
				return err
			}
			var rent SysvarRent
			rent, err = ReadRentSysvar(execCtx)
			if err != nil {
				return err
			}

			if !rent.IsExempt(me.Lamports(), uint64(len(me.Data()))) {
				return InstrErrInsufficientFunds
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

			err = VoteProgramInitializeAccount(execCtx, me, voteInit, signers, clock, execCtx.Features)
		}

	case VoteProgramInstrTypeAuthorize:
		{
			var voteAuthorize VoteInstrVoteAuthorize
			err = voteAuthorize.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			err = checkAcctForClockSysvar(txCtx, instrCtx, 1)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			err = VoteProgramAuthorize(execCtx, me, voteAuthorize.Pubkey, voteAuthorize.Authorize, signers, clock, execCtx.Features)
		}

	case VoteProgramInstrTypeAuthorizeWithSeed:
		{
			var voteAuthWithSeed VoteInstrAuthorizeWithSeed
			err = voteAuthWithSeed.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			err = instrCtx.CheckNumOfInstructionAccounts(3)
			if err != nil {
				return err
			}

			err = VoteProgramAuthorizeWithSeed(execCtx, instrCtx, me, voteAuthWithSeed.NewAuthority, voteAuthWithSeed.Authorize, voteAuthWithSeed.CurrentAuthorityDerivedKeyOwner, voteAuthWithSeed.CurrentAuthorityDerivedKeySeed)
		}

	case VoteProgramInstrTypeAuthorizeCheckedWithSeed:
		{
			var voteAuthCheckedWithSeed VoteInstrAuthorizeCheckedWithSeed
			err = voteAuthCheckedWithSeed.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			err = instrCtx.CheckNumOfInstructionAccounts(4)
			if err != nil {
				return err
			}

			var idx uint64
			idx, err = instrCtx.IndexOfInstructionAccountInTransaction(3)
			if err != nil {
				return err
			}

			var newAuthority solana.PublicKey
			newAuthority, err = txCtx.KeyOfAccountAtIndex(idx)
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

			err = VoteProgramAuthorizeWithSeed(execCtx, instrCtx, me, newAuthority, voteAuthCheckedWithSeed.Authorize, voteAuthCheckedWithSeed.CurrentAuthorityDerivedKeyOwner, voteAuthCheckedWithSeed.CurrentAuthorityDerivedKeySeed)
		}

	case VoteProgramInstrTypeUpdateValidatorIdentity:
		{
			err = instrCtx.CheckNumOfInstructionAccounts(2)
			if err != nil {
				return err
			}

			var idx uint64
			idx, err = instrCtx.IndexOfInstructionAccountInTransaction(1)
			if err != nil {
				return err
			}

			var nodePubkey solana.PublicKey
			nodePubkey, err = txCtx.KeyOfAccountAtIndex(idx)
			if err != nil {
				return err
			}

			err = VoteProgramUpdateValidatorIdentity(execCtx, me, nodePubkey, signers, execCtx.Features)
		}

	case VoteProgramInstrTypeUpdateCommission:
		{
			var updateCommission VoteInstrUpdateCommission
			err = updateCommission.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			var epochSchedule SysvarEpochSchedule
			epochSchedule, err = ReadEpochScheduleSysvar(execCtx)
			if err != nil {
				return err
			}

			err = VoteProgramUpdateCommission(execCtx, me, updateCommission.Commission, signers, epochSchedule, clock, execCtx.Features)
		}

	case VoteProgramInstrTypeVoteSwitch:
		isVoteSwitch = true
		fallthrough
	case VoteProgramInstrTypeVote:
		{
			var vote VoteInstrVote
			if isVoteSwitch {
				var voteSwitch VoteInstrVoteSwitch
				err = voteSwitch.UnmarshalWithDecoder(decoder)
				if err != nil {
					return InstrErrInvalidInstructionData
				}
				vote = voteSwitch.Vote
			} else {
				err = vote.UnmarshalWithDecoder(decoder)
				if err != nil {
					return InstrErrInvalidInstructionData
				}
			}

			err = checkAcctForSlotHashesSysvar(txCtx, instrCtx, 1)
			if err != nil {
				return err
			}
			var slotHashes SysvarSlotHashes
			slotHashes, err = ReadSlotHashesSysvar(execCtx)
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

			err = VoteProgramProcessVote(execCtx, me, slotHashes, clock, &vote, signers, execCtx.Features)
		}
	case VoteProgramInstrTypeUpdateVoteStateSwitch:
		isUpdateVoteStateSwitch = true
		fallthrough

	case VoteProgramInstrTypeUpdateVoteState:
		{
			var updateVoteState VoteInstrUpdateVoteState
			if isUpdateVoteStateSwitch {
				var updateVoteStateSwitch VoteInstrUpdateVoteStateSwitch
				err = updateVoteStateSwitch.UnmarshalWithDecoder(decoder)
				if err != nil {
					return InstrErrInvalidInstructionData
				}
				updateVoteState = updateVoteStateSwitch.UpdateVoteState
			} else {
				err = updateVoteState.UnmarshalWithDecoder(decoder)
				if err != nil {
					return InstrErrInvalidInstructionData
				}
			}

			var slotHashes SysvarSlotHashes
			slotHashes, err = ReadSlotHashesSysvar(execCtx)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			err = VoteProgramProcessVoteStateUpdate(execCtx, me, slotHashes, clock, &updateVoteState, signers, execCtx.Features)
		}

	case VoteProgramInstrTypeCompactUpdateVoteStateSwitch:
		isUpdateVoteStateSwitch = true
		fallthrough

	case VoteProgramInstrTypeCompactUpdateVoteState:
		{
			var updateVoteState *VoteInstrUpdateVoteState
			if isUpdateVoteStateSwitch {
				var compactUpdateVoteStateSwitch VoteInstrCompactUpdateVoteStateSwitch
				err = compactUpdateVoteStateSwitch.UnmarshalWithDecoder(decoder)
				if err != nil {
					return InstrErrInvalidInstructionData
				}
				updateVoteState = &compactUpdateVoteStateSwitch.UpdateVoteState
			} else {
				var compactUpdateVoteState VoteInstrCompactUpdateVoteState
				err = compactUpdateVoteState.UnmarshalWithDecoder(decoder)
				if err != nil {
					return InstrErrInvalidInstructionData
				}
				updateVoteState = &compactUpdateVoteState.UpdateVoteState
			}

			var slotHashes SysvarSlotHashes
			slotHashes, err = ReadSlotHashesSysvar(execCtx)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			err = VoteProgramProcessVoteStateUpdate(execCtx, me, slotHashes, clock, updateVoteState, signers, execCtx.Features)
		}

	case VoteProgramInstrTypeWithdraw:
		{
			var withdraw VoteInstrWithdraw
			err = withdraw.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			err = instrCtx.CheckNumOfInstructionAccounts(2)
			if err != nil {
				return err
			}

			var rent SysvarRent
			rent, err = ReadRentSysvar(execCtx)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			me.Drop()

			err = VoteProgramWithdraw(execCtx, txCtx, instrCtx, 0, withdraw.Lamports, 1, signers, rent, clock, execCtx.Features)
		}

	case VoteProgramInstrTypeAuthorizeChecked:
		{
			var voteAuthorize VoteInstrVoteAuthorizeChecked
			err = voteAuthorize.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			err = instrCtx.CheckNumOfInstructionAccounts(4)
			if err != nil {
				return err
			}

			var idx uint64
			idx, err = instrCtx.IndexOfInstructionAccountInTransaction(3)
			if err != nil {
				return err
			}

			var voterPubkey solana.PublicKey
			voterPubkey, err = txCtx.KeyOfAccountAtIndex(idx)
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

			err = checkAcctForClockSysvar(txCtx, instrCtx, 1)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			err = VoteProgramAuthorize(execCtx, me, voterPubkey, voteAuthorize.Authorize, signers, clock, execCtx.Features)
		}

	case VoteProgramInstrTypeTowerSyncSwitch:
		isTowerSyncSwitch = true
		fallthrough

	case VoteProgramInstrTypeTowerSync:
		{
			var towerSyncInstr *VoteInstrTowerSync
			if isTowerSyncSwitch {
				var towerSyncSwitch VoteInstrTowerSyncSwitch
				err = towerSyncSwitch.UnmarshalWithDecoder(decoder)
				if err != nil {
					return err
				}
				towerSyncInstr = &towerSyncSwitch.TowerSync
			} else {
				var towerSync VoteInstrTowerSync
				err = towerSync.UnmarshalWithDecoder(decoder)
				if err != nil {
					return err
				}
				towerSyncInstr = &towerSync
			}

			if !execCtx.Features.IsActive(features.EnableTowerSyncIx) {
				return InstrErrInvalidInstructionData
			}

			var slotHashes SysvarSlotHashes
			slotHashes, err = ReadSlotHashesSysvar(execCtx)
			if err != nil {
				return err
			}

			var clock SysvarClock
			clock, err = ReadClockSysvar(execCtx)
			if err != nil {
				return err
			}

			err = VoteProgramProcessTowerSync(execCtx, me, slotHashes, clock, towerSyncInstr, signers, execCtx.Features)
		}

	case VoteProgramInstrTypeInitializeAccountV2:
		{
			if !voteProgramInitializeAccountV2Enabled(execCtx.Features) {
				return InstrErrInvalidInstructionData
			}
			if err = instrCtx.CheckNumOfInstructionAccounts(4); err != nil {
				return err
			}

			var voteInit VoteInstrVoteInitV2
			if err = voteInit.UnmarshalWithDecoder(decoder); err != nil {
				return InstrErrInvalidInstructionData
			}

			clock, clockErr := ReadClockSysvar(execCtx)
			if clockErr != nil {
				return clockErr
			}
			rent, rentErr := ReadRentSysvar(execCtx)
			if rentErr != nil {
				return rentErr
			}

			err = VoteProgramInitializeAccountV2(
				txCtx,
				instrCtx,
				voteInit,
				signers,
				clock,
				rent,
				execCtx,
				execCtx.Features,
			)
		}

	default: // invalid instruction
		{
			err = InstrErrInvalidInstructionData
		}
	}

	return err
}

func voteProgramInitializeAccountV2Enabled(f features.Features) bool {
	return f.IsActive(features.BlsPubkeyManagementInVoteAccount) &&
		f.IsActive(features.CommissionRateInBasisPoints) &&
		f.IsActive(features.CustomCommissionCollector) &&
		f.IsActive(features.BlockRevenueSharing) &&
		f.IsActive(features.VoteAccountInitializeV2)
}

func VoteProgramInitializeAccount(execCtx *ExecutionCtx, voteAccount *BorrowedAccount, voteInit VoteInstrVoteInit, signers []solana.PublicKey, clock SysvarClock, f features.Features) error {
	if uint64(len(voteAccount.Data())) != sizeOfVersionedVoteState(f) {
		return InstrErrInvalidAccountData
	}

	versionedVoteState, err := UnmarshalVersionedVoteState(voteAccount.Data())
	if err != nil {
		return err
	}

	if versionedVoteState.IsInitialized() {
		return InstrErrAccountAlreadyInitialized
	}

	err = verifySigner(voteInit.NodePubkey, signers)
	if err != nil {
		return err
	}

	voteState := newVoteStateFromVoteInit(voteInit, clock)
	return setVoteAccountState(execCtx, voteAccount, voteState, f)
}

func VoteProgramInitializeAccountV2(
	txCtx *TransactionCtx,
	instrCtx *InstructionCtx,
	voteInit VoteInstrVoteInitV2,
	signers []solana.PublicKey,
	clock SysvarClock,
	rent SysvarRent,
	execCtx *ExecutionCtx,
	f features.Features,
) error {
	voteAccount, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer voteAccount.Drop()

	if uint64(len(voteAccount.Data())) != sizeOfVersionedVoteState(f) {
		return InstrErrInvalidAccountData
	}
	versionedVoteState, err := UnmarshalVersionedVoteState(voteAccount.Data())
	if err != nil {
		return err
	}
	if versionedVoteState.IsInitialized() {
		return InstrErrAccountAlreadyInitialized
	}
	if err := verifySigner(voteInit.NodePubkey, signers); err != nil {
		return err
	}

	voteKey := voteAccount.Key()
	inflationCollector, err := resolveVoteCommissionCollector(txCtx, instrCtx, voteKey, 2, rent)
	if err != nil {
		return err
	}
	blockRevenueCollector, err := resolveVoteCommissionCollector(txCtx, instrCtx, voteKey, 3, rent)
	if err != nil {
		return err
	}

	if err := execCtx.ComputeMeter.Consume(voteBLSProofOfPossessionComputeUnits); err != nil {
		return InstrErrComputationalBudgetExceeded
	}
	blsArgs := &VoterWithBLSArgs{
		BlsPubkeyCompressed:  voteInit.AuthorizedVoterBLSPubkey,
		BlsProofOfPossession: voteInit.AuthorizedVoterBLSProofOfPossession,
	}
	if err := verifyVoteBLSProofOfPossession(voteKey, blsArgs); err != nil {
		return err
	}

	voteState := newVoteStateFromVoteInitV2(voteInit, inflationCollector, blockRevenueCollector, clock)
	return setVoteAccountState(execCtx, voteAccount, voteState, f)
}

func resolveVoteCommissionCollector(
	txCtx *TransactionCtx,
	instrCtx *InstructionCtx,
	voteKey solana.PublicKey,
	accountIndex uint64,
	rent SysvarRent,
) (solana.PublicKey, error) {
	accountKey, err := instrCtx.KeyOfInstructionAccount(txCtx, accountIndex)
	if err != nil {
		return solana.PublicKey{}, err
	}
	if accountKey == voteKey {
		return voteKey, nil
	}

	collectorAccount, err := instrCtx.BorrowInstructionAccount(txCtx, accountIndex)
	if err != nil {
		return solana.PublicKey{}, err
	}
	defer collectorAccount.Drop()

	if collectorAccount.Owner() != a.SystemProgramAddr {
		return solana.PublicKey{}, InstrErrInvalidAccountOwner
	}
	if !rent.IsExempt(collectorAccount.Lamports(), uint64(len(collectorAccount.Data()))) {
		return solana.PublicKey{}, InstrErrInsufficientFunds
	}
	if !collectorAccount.IsWritable() {
		return solana.PublicKey{}, InstrErrInvalidArgument
	}
	return accountKey, nil
}

func newVoteStateFromVoteInitV2(
	voteInit VoteInstrVoteInitV2,
	inflationCollector solana.PublicKey,
	blockRevenueCollector solana.PublicKey,
	clock SysvarClock,
) *VoteState {
	voteState := newVoteStateFromVoteInit(VoteInstrVoteInit{
		NodePubkey:           voteInit.NodePubkey,
		AuthorizedVoter:      voteInit.AuthorizedVoter,
		AuthorizedWithdrawer: voteInit.AuthorizedWithdrawer,
		Commission:           0,
	}, clock)

	voteState.wasV4 = true
	voteState.v4InflationRewardsCollector = inflationCollector
	voteState.v4BlockRevenueCollector = blockRevenueCollector
	voteState.v4InflationRewardsCommBps = voteInit.InflationRewardsCommissionBps
	voteState.v4BlockRevenueCommBps = voteInit.BlockRevenueCommissionBps
	blsPubkey := voteInit.AuthorizedVoterBLSPubkey
	voteState.v4BlsPubkeyCompressed = &blsPubkey
	return voteState
}

func VoteProgramAuthorize(execCtx *ExecutionCtx, voteAcct *BorrowedAccount, authorized solana.PublicKey, authorize VoteAuthorizeKind, signers []solana.PublicKey, clock SysvarClock, f features.Features) error {
	voteStateVersions, err := UnmarshalVersionedVoteState(voteAcct.Data())
	if err != nil {
		return err
	}

	voteState := voteStateVersions.ConvertToCurrent()

	switch authorize.Type {
	case VoteAuthorizeTypeVoter:
		{
			if f.IsActive(features.BlsPubkeyManagementInVoteAccount) && voteStateHasBLSPubkey(voteState) {
				return InstrErrInvalidInstructionData
			}
			var authorizedWithDrawerSigner bool
			if verifySigner(voteState.AuthorizedWithdrawer, signers) == nil {
				authorizedWithDrawerSigner = true
			}

			err = voteState.SetNewAuthorizedVoter(authorized, clock.Epoch, clock.LeaderScheduleEpoch+1, nil, func(epochAuthorizedVoter solana.PublicKey) error {
				if authorizedWithDrawerSigner {
					return nil
				} else {
					return verifySigner(epochAuthorizedVoter, signers)
				}
			}, f)
			if err != nil {
				return err
			}
		}

	case VoteAuthorizeTypeWithdrawer:
		{
			err = verifySigner(voteState.AuthorizedWithdrawer, signers)
			if err != nil {
				return err
			}
			voteState.AuthorizedWithdrawer = authorized
		}

	case VoteAuthorizeTypeVoterWithBLS:
		{
			if !f.IsActive(features.BlsPubkeyManagementInVoteAccount) || authorize.VoterWithBLS == nil {
				return InstrErrInvalidInstructionData
			}
			if err := execCtx.ComputeMeter.Consume(voteBLSProofOfPossessionComputeUnits); err != nil {
				return InstrErrComputationalBudgetExceeded
			}
			if err := verifyVoteBLSProofOfPossession(voteAcct.Key(), authorize.VoterWithBLS); err != nil {
				return err
			}

			authorizedWithDrawerSigner := verifySigner(voteState.AuthorizedWithdrawer, signers) == nil
			blsPubkey := authorize.VoterWithBLS.BlsPubkeyCompressed
			err = voteState.SetNewAuthorizedVoter(authorized, clock.Epoch, clock.LeaderScheduleEpoch+1, &blsPubkey, func(epochAuthorizedVoter solana.PublicKey) error {
				if authorizedWithDrawerSigner {
					return nil
				}
				return verifySigner(epochAuthorizedVoter, signers)
			}, f)
			if err != nil {
				return err
			}
		}

	default:
		return InstrErrInvalidInstructionData
	}

	err = setVoteAccountState(execCtx, voteAcct, voteState, f)
	return err
}

func VoteProgramAuthorizeWithSeed(execCtx *ExecutionCtx, instrCtx *InstructionCtx, voteAcct *BorrowedAccount, newAuthority solana.PublicKey, authorize VoteAuthorizeKind, currentAuthorityDerivedKeyOwner solana.PublicKey, currentAuthorityDerivedKeySeed string) error {
	txCtx := execCtx.TransactionContext

	err := checkAcctForClockSysvar(txCtx, instrCtx, 1)
	if err != nil {
		return err
	}
	clock, err := ReadClockSysvar(execCtx)
	if err != nil {
		return err
	}

	var expectedAuthorityKeys []solana.PublicKey

	isSigner, err := instrCtx.IsInstructionAccountSigner(2)
	if err != nil {
		return err
	}

	if isSigner {
		idxInTx, err := instrCtx.IndexOfInstructionAccountInTransaction(2)
		if err != nil {
			return err
		}
		basePubkey, err := txCtx.KeyOfAccountAtIndex(idxInTx)
		if err != nil {
			return err
		}

		authKey, err := ValidateAndCreateWithSeed(basePubkey, currentAuthorityDerivedKeySeed, currentAuthorityDerivedKeyOwner)
		if err != nil {
			return err
		}
		expectedAuthorityKeys = append(expectedAuthorityKeys, authKey)
	}

	err = VoteProgramAuthorize(execCtx, voteAcct, newAuthority, authorize, expectedAuthorityKeys, clock, execCtx.Features)
	return err
}

func VoteProgramUpdateValidatorIdentity(execCtx *ExecutionCtx, voteAcct *BorrowedAccount, nodePubkey solana.PublicKey, signers []solana.PublicKey, f features.Features) error {
	voteStateVersions, err := UnmarshalVersionedVoteState(voteAcct.Data())
	if err != nil {
		return err
	}

	voteState := voteStateVersions.ConvertToCurrent()

	err = verifySigner(voteState.AuthorizedWithdrawer, signers)
	if err != nil {
		return err
	}

	err = verifySigner(nodePubkey, signers)
	if err != nil {
		return err
	}

	voteState.NodePubkey = nodePubkey
	err = setVoteAccountState(execCtx, voteAcct, voteState, f)

	return err
}

func isCommissionUpdateAllowed(slot uint64, epochSchedule SysvarEpochSchedule) bool {
	if epochSchedule.SlotsPerEpoch > 0 {
		relativeSlot := safemath.SaturatingSubU64(slot, epochSchedule.FirstNormalSlot)
		relativeSlot %= epochSchedule.SlotsPerEpoch
		return safemath.SaturatingMulU64(relativeSlot, 2) <= epochSchedule.SlotsPerEpoch
	} else {
		return true
	}
}

func VoteProgramUpdateCommission(execCtx *ExecutionCtx, voteAcct *BorrowedAccount, commission byte, signers []solana.PublicKey, epochSchedule SysvarEpochSchedule, clock SysvarClock, f features.Features) error {
	voteStateVersioned, err := UnmarshalVersionedVoteState(voteAcct.Data())
	if err != nil {
		return err
	}
	voteState := voteStateVersioned.ConvertToCurrent()

	enforceCommissionUpdateRule := !f.IsActive(features.DelayCommissionUpdates) &&
		commission > voteState.Commission
	if enforceCommissionUpdateRule {
		if !isCommissionUpdateAllowed(clock.Slot, epochSchedule) {
			return VoteErrCommissionUpdateTooLate
		}
	}

	err = verifySigner(voteState.AuthorizedWithdrawer, signers)
	if err != nil {
		return err
	}

	voteState.Commission = commission
	if voteState.wasV4 {
		voteState.v4InflationRewardsCommBps = uint16(commission) * 100
	}
	err = setVoteAccountState(execCtx, voteAcct, voteState, f)

	return err
}

func verifyAndGetVoteState(voteAcct *BorrowedAccount, clock SysvarClock, signers []solana.PublicKey, f features.Features) (*VoteState, error) {
	versioned, err := UnmarshalVersionedVoteState(voteAcct.Data())
	if err != nil {
		return nil, err
	}

	if !versioned.IsInitialized() {
		return nil, InstrErrUninitializedAccount
	}

	voteState := versioned.ConvertToCurrent()
	authVoter, err := voteState.GetAndUpdateAuthorizedVoter(clock.Epoch, f)
	if err != nil {
		return nil, err
	}

	err = verifySigner(authVoter, signers)
	if err != nil {
		return nil, err
	}

	return voteState, nil
}

func checkSlotsAreValid(voteState *VoteState, voteSlots []uint64, voteHash [32]byte, slotHashes SysvarSlotHashes) error {
	var err error
	i := uint64(0)
	j := uint64(len(slotHashes))

	for i < uint64(len(voteSlots)) && j > 0 {

		// "1) increment `i` to find the smallest slot `s` in `vote_slots`
		// where `s` >= `last_voted_slot`""
		lastVotedSlot, ok := voteState.LastVotedSlot()
		if ok && voteSlots[i] <= lastVotedSlot {
			i, err = safemath.CheckedAddU64(i, 1)
			if err != nil {
				panic("`i` is bounded by `MAX_LOCKOUT_HISTORY` when finding larger slots")
			}
			continue
		}

		// "2) Find the hash for this slot `s`.""
		k, err := safemath.CheckedSubU64(j, 1)
		if err != nil {
			panic("`j` is positive")
		}
		if voteSlots[i] != slotHashes[k].Slot {
			// Decrement `j` to find newer slots
			j, err = safemath.CheckedSubU64(j, 1)
			if err != nil {
				panic("`j` is positive when finding newer slots")
			}
			continue
		}

		// "3) Once the hash for `s` is found, bump `s` to the next slot
		// in `vote_slots` and continue."
		i, err = safemath.CheckedAddU64(i, 1)
		if err != nil {
			panic("`i` is bounded by `MAX_LOCKOUT_HISTORY` when hash is found")
		}
		j, err = safemath.CheckedSubU64(j, 1)
		if err != nil {
			panic("`j` is positive when hash is found")
		}
	}

	if j == uint64(len(slotHashes)) {
		klog.Errorf("%s dropped vote slots %#v, vote hash %s, slot hashes: %#v, too old ", voteState.NodePubkey, voteSlots, voteHash, slotHashes)
		return VoteErrVoteTooOld
	}

	if i != uint64(len(voteSlots)) {
		mlog.Log.Infof("%s dropped vote slots %#v failed to match slot hashes: %#v", voteState.NodePubkey, voteSlots, slotHashes)
		return VoteErrSlotsMismatch
	}

	if slotHashes[j].Hash != voteHash {
		mlog.Log.Infof("%s dropped vote slots. failed to match hash %#v vs. %#v (prev slot)", voteState.NodePubkey, voteHash, slotHashes[j].Hash[:])
		return VoteErrSlotHashMismatch
	}

	return nil
}

func processVoteUnfiltered(voteState *VoteState, voteSlots []uint64, vote *VoteInstrVote, slotHashes SysvarSlotHashes, epoch uint64, currentSlot uint64, timelyVoteCredits, deprecateUnusedLegacyVotePlumbing bool) error {
	err := checkSlotsAreValid(voteState, voteSlots, vote.Hash, slotHashes)
	if err != nil {
		return err
	}

	for _, voteSlot := range voteSlots {
		voteState.ProcessNextVoteSlot(voteSlot, epoch, currentSlot, timelyVoteCredits, deprecateUnusedLegacyVotePlumbing)
	}

	return nil
}

func processVote(voteState *VoteState, vote *VoteInstrVote, slotHashes SysvarSlotHashes, epoch uint64, currentSlot uint64, timelyVoteCredits, deprecateUnusedLegacyVotePlumbing bool) error {
	if len(vote.Slots) == 0 {
		return VoteErrEmptySlots
	}

	var earliestSlotInHistory uint64
	if len(slotHashes) != 0 {
		earliestSlotInHistory = slotHashes[len(slotHashes)-1].Slot
	}

	var voteSlots []uint64
	for _, slot := range vote.Slots {
		if slot >= earliestSlotInHistory {
			voteSlots = append(voteSlots, slot)
		}
	}

	if len(voteSlots) == 0 {
		return VoteErrVotesTooOldAllFiltered
	}

	return processVoteUnfiltered(voteState, voteSlots, vote, slotHashes, epoch, currentSlot, timelyVoteCredits, deprecateUnusedLegacyVotePlumbing)
}

func VoteProgramProcessVote(execCtx *ExecutionCtx, voteAcct *BorrowedAccount, slotHashes SysvarSlotHashes, clock SysvarClock, vote *VoteInstrVote, signers []solana.PublicKey, f features.Features) error {
	voteState, err := verifyAndGetVoteState(voteAcct, clock, signers, f)
	if err != nil {
		return err
	}

	timelyVoteCredits := f.IsActive(features.TimelyVoteCredits)
	deprecateUnusedLegacyVotePlumbing := f.IsActive(features.DeprecateUnusedLegacyVotePlumbing)

	err = processVote(voteState, vote, slotHashes, clock.Epoch, clock.Slot, timelyVoteCredits, deprecateUnusedLegacyVotePlumbing)
	if err != nil {
		return err
	}

	if vote.Timestamp != nil {
		if len(vote.Slots) == 0 {
			return VoteErrEmptySlots
		}
		maxSlot := vote.Slots[0]
		for _, slot := range vote.Slots {
			if slot > maxSlot {
				maxSlot = slot
			}
		}
		err = voteState.ProcessTimestamp(maxSlot, *vote.Timestamp)
		if err != nil {
			return err
		}
	}

	err = setVoteAccountState(execCtx, voteAcct, voteState, f)

	return err
}

func checkUpdateVoteStateAndSlotsAreValid(voteState *VoteState, proposedLockouts *deque.Deque[VoteLockout], proposedRoot **uint64, proposedHash [32]byte, slotHashes SysvarSlotHashes) error {
	if proposedLockouts.Len() == 0 {
		return VoteErrEmptySlots
	}

	lastVoteStateUpdateLockout := proposedLockouts.Back()

	lastVoteStateUpdateSlot := lastVoteStateUpdateLockout.Slot

	if voteState.Votes.Len() > 0 {
		lastLandedVote := voteState.Votes.Back()
		lastVoteSlot := lastLandedVote.Lockout.Slot
		if lastVoteStateUpdateSlot <= lastVoteSlot {
			return VoteErrVoteTooOld
		}
	}

	if len(slotHashes) == 0 {
		return VoteErrSlotsMismatch
	}

	earliestSlotHashInHistory := slotHashes[len(slotHashes)-1].Slot

	if lastVoteStateUpdateSlot < earliestSlotHashInHistory {
		return VoteErrVoteTooOld
	}

	if proposedRoot != nil && *proposedRoot != nil {
		pRoot := **proposedRoot
		if pRoot < earliestSlotHashInHistory {
			*proposedRoot = voteState.RootSlot

			// Agave iterates in reverse, so we collect all entries into a slice
			// and then reverse the order
			for i := voteState.Votes.Len() - 1; i >= 0; i-- {
				vote := voteState.Votes.At(i)
				if vote.Lockout.Slot <= pRoot {
					*proposedRoot = &vote.Lockout.Slot
					break
				}
			}
		}
	}

	var rootToCheck *uint64
	if proposedRoot != nil {
		rootToCheck = *proposedRoot
	}
	voteStateUpdateIndex := uint64(0)
	slotHashesIndex := uint64(len(slotHashes))
	var voteStateUpdateIndicesToFilter []uint64

	for voteStateUpdateIndex < uint64(proposedLockouts.Len()) && slotHashesIndex > 0 {
		var proposedVoteSlot uint64
		if rootToCheck != nil {
			proposedVoteSlot = *rootToCheck
		} else {
			proposedVoteSlot = proposedLockouts.At(int(voteStateUpdateIndex)).Slot
		}

		if rootToCheck == nil && voteStateUpdateIndex > 0 {
			i, err := safemath.CheckedSubU64(voteStateUpdateIndex, 1)
			if err != nil {
				panic("`vote_state_update_index` is positive when checking `SlotsNotOrdered`")
			}
			if proposedVoteSlot <= proposedLockouts.At(int(i)).Slot {
				return VoteErrSlotsNotOrdered
			}
		}

		j, err := safemath.CheckedSubU64(slotHashesIndex, 1)
		if err != nil {
			panic("`slot_hashes_index` is positive when computing `ancestor_slot`")
		}
		ancestorSlot := slotHashes[j].Slot

		if proposedVoteSlot < ancestorSlot {
			if slotHashesIndex == uint64(len(slotHashes)) {
				if proposedVoteSlot >= earliestSlotHashInHistory {
					panic("proposed_vote_slot < earliest_slot_hash_in_history not true")
				}
				if !voteState.ContainsSlot(proposedVoteSlot) && rootToCheck == nil {
					voteStateUpdateIndicesToFilter = append(voteStateUpdateIndicesToFilter, voteStateUpdateIndex)
				}

				if rootToCheck != nil {
					newProposedRoot := *rootToCheck
					if newProposedRoot != proposedVoteSlot {
						panic("newProposedRoot != proposedVoteSlot")
					}
					if newProposedRoot >= earliestSlotHashInHistory {
						panic("new_proposed_root < earliest_slot_hash_in_history not true")
					}
					rootToCheck = nil
				} else {
					voteStateUpdateIndex, err = safemath.CheckedAddU64(voteStateUpdateIndex, 1)
					if err != nil {
						panic("`vote_state_update_index` is bounded by `MAX_LOCKOUT_HISTORY` when `proposed_vote_slot` is too old to be in SlotHashes history")
					}
				}
				continue
			} else {
				if rootToCheck != nil {
					return VoteErrRootOnDifferentFork
				} else {
					return VoteErrSlotsMismatch
				}
			}
		} else if proposedVoteSlot > ancestorSlot {
			slotHashesIndex, err = safemath.CheckedSubU64(slotHashesIndex, 1)
			if err != nil {
				panic("`slot_hashes_index` is positive when finding newer slots in SlotHashes history")
			}
			continue

		} else { // proposedVoteSlot == ancestorSlot
			if rootToCheck != nil {
				rootToCheck = nil
			} else {
				voteStateUpdateIndex, err = safemath.CheckedAddU64(voteStateUpdateIndex, 1)
				if err != nil {
					panic("`vote_state_update_index` is bounded by `MAX_LOCKOUT_HISTORY` when match is found in SlotHashes history")
				}
				slotHashesIndex, err = safemath.CheckedSubU64(slotHashesIndex, 1)
				if err != nil {
					panic("`slot_hashes_index` is positive when match is found in SlotHashes history")
				}
			}
		}
	}

	if voteStateUpdateIndex != uint64(proposedLockouts.Len()) {
		return VoteErrSlotsMismatch
	}

	if lastVoteStateUpdateSlot != slotHashes[slotHashesIndex].Slot {
		panic("lastVoteStateUpdateSlot != slotHashes[slotHashesIndex].Slot not true")
	}

	if slotHashes[slotHashesIndex].Hash != proposedHash {
		mlog.Log.Infof("%s dropped vote. failed to match hash %s vs. %s", voteState.NodePubkey, solana.HashFromBytes(proposedHash[:]), solana.HashFromBytes(slotHashes[slotHashesIndex].Hash[:]))
		return VoteErrSlotHashMismatch
	}

	voteStateUpdateIndex = 0
	filterVotesIndex := uint64(0)
	var lockoutsToKeep []VoteLockout

	for i := 0; i < proposedLockouts.Len(); i++ {
		var err error
		lockout := proposedLockouts.At(i)
		if filterVotesIndex == uint64(len(voteStateUpdateIndicesToFilter)) {
			lockoutsToKeep = append(lockoutsToKeep, lockout)
		} else if voteStateUpdateIndex == voteStateUpdateIndicesToFilter[filterVotesIndex] {
			filterVotesIndex = filterVotesIndex + 1
		} else {
			lockoutsToKeep = append(lockoutsToKeep, lockout)
		}
		voteStateUpdateIndex, err = safemath.CheckedAddU64(voteStateUpdateIndex, 1)
		if err != nil {
			panic("`vote_state_update_index` is bounded by `MAX_LOCKOUT_HISTORY` when filtering out irrelevant votes")
		}
	}

	proposedLockouts.Clear()

	for _, lockout := range lockoutsToKeep {
		proposedLockouts.PushBack(lockout)
	}

	return nil
}

type latencyUpdate struct {
	idx   int
	state LandedVote
}

func processNewVoteState(voteState *VoteState, newState *deque.Deque[LandedVote], newRoot *uint64, timestamp *int64, epoch uint64, currentSlot uint64, f features.Features) error {
	if newState.Len() == 0 {
		panic("newState should not be empty")
	}

	if newState.Len() > MaxLockoutHistory {
		return VoteErrTooManyVotes
	}

	if newRoot != nil && voteState.RootSlot != nil {
		currentRoot := *voteState.RootSlot
		if *newRoot < currentRoot {
			return VoteErrRootRollback
		}
	} else if newRoot == nil && voteState.RootSlot != nil {
		return VoteErrRootRollback
	}

	var previousVote *LandedVote

	var errToReturn error

	for i := 0; i < newState.Len(); i++ {
		vote := newState.At(i)
		if vote.Lockout.ConfirmationCount == 0 {
			errToReturn = VoteErrZeroConfirmations
			break
		} else if vote.Lockout.ConfirmationCount > MaxLockoutHistory {
			errToReturn = VoteErrConfirmationTooLarge
			break
		} else if newRoot != nil {
			if vote.Lockout.Slot <= *newRoot && *newRoot != 0 {
				errToReturn = VoteErrSlotSmallerThanRoot
				break
			}
		}

		if previousVote != nil {
			if previousVote.Lockout.Slot >= vote.Lockout.Slot {
				errToReturn = VoteErrSlotsNotOrdered
				break
			} else if previousVote.Lockout.ConfirmationCount <= vote.Lockout.ConfirmationCount {
				errToReturn = VoteErrConfirmationsNotOrdered
				break
			} else if vote.Lockout.Slot > previousVote.Lockout.LastLockedOutSlot() {
				errToReturn = VoteErrNewVoteStateLockoutMismatch
				break
			}
		}
		previousVote = &vote
	}

	if errToReturn != nil {
		return errToReturn
	}

	currentVoteStateIndex := uint64(0)
	newVoteStateIndex := uint64(0)

	timelyVoteCredits := f.IsActive(features.TimelyVoteCredits)
	deprecateUnusedLegacyVotePlumbing := f.IsActive(features.DeprecateUnusedLegacyVotePlumbing)

	var earnedCredits uint64

	if timelyVoteCredits {
		earnedCredits = 0
	} else {
		earnedCredits = 1
	}

	if newRoot != nil {
		for i := 0; i < voteState.Votes.Len(); i++ {
			currentVote := voteState.Votes.At(i)
			var err error
			if currentVote.Lockout.Slot <= *newRoot {
				if timelyVoteCredits || currentVote.Lockout.Slot != *newRoot {
					earnedCredits, err = safemath.CheckedAddU64(earnedCredits, voteState.CreditsForVoteAtIndex(currentVoteStateIndex, timelyVoteCredits, deprecateUnusedLegacyVotePlumbing))
					if err != nil {
						panic("`earned_credits` does not overflow")
					}
				}

				currentVoteStateIndex, err = safemath.CheckedAddU64(currentVoteStateIndex, 1)
				if err != nil {
					panic("`current_vote_state_index` is bounded by `MAX_LOCKOUT_HISTORY` when processing new root")
				}
				continue
			}
			break
		}
	}

	var stateUpdates []latencyUpdate

	for currentVoteStateIndex < uint64(voteState.Votes.Len()) && newVoteStateIndex < uint64(newState.Len()) {
		var err error
		currentVote := voteState.Votes.At(int(currentVoteStateIndex))
		newVote := newState.At(int(newVoteStateIndex))

		if currentVote.Lockout.Slot < newVote.Lockout.Slot {
			if currentVote.Lockout.LastLockedOutSlot() >= newVote.Lockout.Slot {
				return VoteErrLockoutConflict
			}
			currentVoteStateIndex, err = safemath.CheckedAddU64(currentVoteStateIndex, 1)
			if err != nil {
				panic("`current_vote_state_index` is bounded by `MAX_LOCKOUT_HISTORY` when slot is less than proposed")
			}
		} else if currentVote.Lockout.Slot == newVote.Lockout.Slot {
			if newVote.Lockout.ConfirmationCount < currentVote.Lockout.ConfirmationCount {
				return VoteErrConfirmationRollback
			}

			newVote.Latency = voteState.Votes.At(int(currentVoteStateIndex)).Latency
			stateUpdate := latencyUpdate{idx: int(newVoteStateIndex), state: newVote}
			stateUpdates = append(stateUpdates, stateUpdate)

			currentVoteStateIndex, err = safemath.CheckedAddU64(currentVoteStateIndex, 1)
			if err != nil {
				panic("`current_vote_state_index` is bounded by `MAX_LOCKOUT_HISTORY` when slot is equal to proposed")
			}
			newVoteStateIndex, err = safemath.CheckedAddU64(newVoteStateIndex, 1)
			if err != nil {
				panic("`new_vote_state_index` is bounded by `MAX_LOCKOUT_HISTORY` when slot is equal to proposed")
			}
		} else { // currentVote.Lockout.Slot > newVote.Lockout.Slot
			newVoteStateIndex, err = safemath.CheckedAddU64(newVoteStateIndex, 1)
			if err != nil {
				panic("`new_vote_state_index` is bounded by `MAX_LOCKOUT_HISTORY` when slot is greater than proposed")
			}

		}
	}

	for _, update := range stateUpdates {
		newState.Set(update.idx, update.state)
	}

	var latencyStateUpdates []latencyUpdate

	if timelyVoteCredits {
		for i := 0; i < newState.Len(); i++ {
			newVote := newState.At(i)
			if newVote.Latency == 0 {
				newLandedVote := newVote
				newLandedVote.Latency = computeVoteLatency(newVote.Lockout.Slot, currentSlot)
				lsa := latencyUpdate{idx: i, state: newLandedVote}
				latencyStateUpdates = append(latencyStateUpdates, lsa)
			}
		}

		for _, update := range latencyStateUpdates {
			newState.Set(update.idx, update.state)
		}
	}

	if (voteState.RootSlot == nil && newRoot != nil) || (voteState.RootSlot != nil && newRoot == nil) || (voteState.RootSlot != nil && newRoot != nil && *voteState.RootSlot != *newRoot) {
		voteState.IncrementCredits(epoch, earnedCredits)
	}

	if timestamp != nil {
		var err error
		lastLandedVote := newState.Back()
		lastSlot := lastLandedVote.Lockout.Slot
		err = voteState.ProcessTimestamp(lastSlot, *timestamp)
		if err != nil {
			return err
		}
	}

	voteState.RootSlot = newRoot
	// newState may be a pooled deque. Own the backing storage before its
	// caller returns it to the pool: the resulting state can escape into the
	// shared vote cache after this instruction completes.
	var owned deque.Deque[LandedVote]
	for i := 0; i < newState.Len(); i++ {
		owned.PushBack(newState.At(i))
	}
	voteState.Votes = owned

	return nil
}

func VoteProgramProcessVoteStateUpdate(execCtx *ExecutionCtx, voteAcct *BorrowedAccount, slotHashes SysvarSlotHashes, clock SysvarClock, voteStateUpdate *VoteInstrUpdateVoteState, signers []solana.PublicKey, f features.Features) error {
	voteState, err := verifyAndGetVoteState(voteAcct, clock, signers, f)
	if err != nil {
		return err
	}

	err = checkUpdateVoteStateAndSlotsAreValid(voteState, &voteStateUpdate.Lockouts, &voteStateUpdate.Root, voteStateUpdate.Hash, slotHashes)
	if err != nil {
		return err
	}

	newState := &deque.Deque[LandedVote]{}
	newState.SetBaseCap(voteStateUpdate.Lockouts.Len())
	for i := 0; i < voteStateUpdate.Lockouts.Len(); i++ {
		newState.PushBack(LandedVote{Latency: 0, Lockout: voteStateUpdate.Lockouts.At(i)})
	}

	err = processNewVoteState(voteState, newState, voteStateUpdate.Root, voteStateUpdate.Timestamp, clock.Epoch, clock.Slot, f)
	if err != nil {
		return err
	}

	err = setVoteAccountState(execCtx, voteAcct, voteState, f)

	return err
}

func VoteProgramWithdraw(execCtx *ExecutionCtx, txCtx *TransactionCtx, instrCtx *InstructionCtx, voteAcctIdx uint64, lamports uint64, toAcctIdx uint64, signers []solana.PublicKey, rent SysvarRent, clock SysvarClock, f features.Features) error {
	voteAcct, err := instrCtx.BorrowInstructionAccount(txCtx, voteAcctIdx)
	if err != nil {
		return err
	}
	defer voteAcct.Drop()

	versionedVoteState, err := UnmarshalVersionedVoteState(voteAcct.Data())
	if err != nil {
		return err
	}
	voteState := versionedVoteState.ConvertToCurrent()

	err = verifySigner(voteState.AuthorizedWithdrawer, signers)
	if err != nil {
		return err
	}

	remainingBalance, err := safemath.CheckedSubU64(voteAcct.Lamports(), lamports)
	if err != nil {
		return InstrErrInsufficientFunds
	}

	if remainingBalance == 0 {
		var rejectActiveVoteAcctClose bool
		if len(voteState.EpochCredits) != 0 {
			lastEpochWithCredits := voteState.EpochCredits[len(voteState.EpochCredits)-1].Epoch
			currentEpoch := clock.Epoch
			if safemath.SaturatingSubU64(currentEpoch, lastEpochWithCredits) < 2 {
				rejectActiveVoteAcctClose = true
			}
		}

		if rejectActiveVoteAcctClose {
			return VoteErrActiveVoteAccountClose
		} else {
			newDefaultVoteState := new(VoteState)
			newDefaultVoteState.PriorVoters.Index = 31
			newDefaultVoteState.PriorVoters.IsEmpty = true
			err = setVoteAccountState(execCtx, voteAcct, newDefaultVoteState, f)
			if err != nil {
				return err
			}
		}
	} else {
		minRentExemptBalance := rent.MinimumBalance(uint64(len(voteAcct.Data())))
		if remainingBalance < minRentExemptBalance {
			return InstrErrInsufficientFunds
		}
	}

	err = voteAcct.CheckedSubLamports(lamports, f)
	if err != nil {
		return err
	}
	voteAcct.Drop()

	toAcct, err := instrCtx.BorrowInstructionAccount(txCtx, toAcctIdx)
	if err != nil {
		return err
	}
	defer toAcct.Drop()

	err = toAcct.CheckedAddLamports(lamports, f)

	return err
}

func newVoteDeque() *deque.Deque[LandedVote] {
	return &deque.Deque[LandedVote]{}
}

var (
	voteDequePool = &sync.Pool{
		New: func() interface{} {
			return newVoteDeque()
		}}
)

func VoteProgramProcessTowerSync(execCtx *ExecutionCtx, voteAcct *BorrowedAccount, slotHashes SysvarSlotHashes, clock SysvarClock, towerSync *VoteInstrTowerSync, signers []solana.PublicKey, f features.Features) error {
	voteState, err := verifyAndGetVoteState(voteAcct, clock, signers, f)
	if err != nil {
		return err
	}

	err = checkUpdateVoteStateAndSlotsAreValid(voteState, &towerSync.Lockouts, &towerSync.Root, towerSync.Hash, slotHashes)
	if err != nil {
		return err
	}

	var newState *deque.Deque[LandedVote]
	if sbpf.UsePool {
		newState = voteDequePool.Get().(*deque.Deque[LandedVote])
		newState.Clear()
		defer voteDequePool.Put(newState)
	} else {
		newState = newVoteDeque()
	}
	for i := 0; i < towerSync.Lockouts.Len(); i++ {
		newState.PushBack(LandedVote{Latency: 0, Lockout: towerSync.Lockouts.At(i)})
	}

	err = processNewVoteState(voteState, newState, towerSync.Root, towerSync.Timestamp, clock.Epoch, clock.Slot, f)
	if err != nil {
		return err
	}

	err = setVoteAccountState(execCtx, voteAcct, voteState, f)

	return err
}
