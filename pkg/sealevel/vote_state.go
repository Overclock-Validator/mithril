package sealevel

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/gammazero/deque"
	"github.com/tidwall/btree"
)

const (
	VoteStateVersionV0_23_5 = iota
	VoteStateVersionV1_14_11
	VoteStateVersionCurrent
	VoteStateVersionV4
)

const (
	VoteStateV2Size = 3731
	VoteStateV3Size = 3762
	// VoteStateV4 keeps the same serialized account allocation as V3.
	VoteStateV4Size = VoteStateV3Size
)

func validateVoteStateCollectionLength(
	decoder *bin.Decoder,
	name string,
	count uint64,
	minimumElementBytes uint64,
	protocolMaximum uint64,
) error {
	if decoder == nil || minimumElementBytes == 0 {
		return fmt.Errorf("vote state: invalid %s decoder bound", name)
	}
	if protocolMaximum != 0 && count > protocolMaximum {
		return fmt.Errorf("vote state: %s count %d exceeds protocol maximum %d", name, count, protocolMaximum)
	}
	remaining := decoder.Remaining()
	if remaining < 0 || count > uint64(remaining)/minimumElementBytes {
		return fmt.Errorf("vote state: %s count %d cannot fit in %d remaining bytes", name, count, remaining)
	}
	return nil
}

func sizeOfVersionedVoteState(f features.Features) uint64 {
	if f.IsActive(features.VoteStateAddVoteLatency) || f.IsActive(features.VoteStateV4) {
		return VoteStateV3Size
	} else {
		return VoteStateV2Size
	}
}

type PriorVoter struct {
	Pubkey     solana.PublicKey
	EpochStart uint64
	EpochEnd   uint64
	Slot       uint64
}

type PriorVoters0_23_5 struct {
	Buf   [32]PriorVoter
	Index uint64
}

type PriorVoters struct {
	Buf     [32]PriorVoter
	Index   uint64
	IsEmpty bool
}

type EpochCredits struct {
	Epoch       uint64
	Credits     uint64
	PrevCredits uint64
}

type BlockTimestamp struct {
	Slot      uint64
	Timestamp int64
}

type AuthorizedVoter struct {
	Epoch  uint64
	Pubkey solana.PublicKey
}

type AuthorizedVoters struct {
	AuthorizedVoters btree.Map[uint64, solana.PublicKey]
}

type VoteLockout struct {
	Slot              uint64
	ConfirmationCount uint32
}

type LandedVote struct {
	Latency byte
	Lockout VoteLockout
}

type VoteState0_23_5 struct {
	NodePubkey           solana.PublicKey
	AuthorizedVoter      solana.PublicKey
	AuthorizedVoterEpoch uint64
	PriorVoters          PriorVoters0_23_5
	AuthorizedWithdrawer solana.PublicKey
	Commission           byte
	Votes                deque.Deque[VoteLockout]
	RootSlot             *uint64
	EpochCredits         []EpochCredits
	LastTimestamp        BlockTimestamp
}

type VoteState1_14_11 struct {
	NodePubkey           solana.PublicKey
	AuthorizedWithdrawer solana.PublicKey
	Commission           byte
	Votes                deque.Deque[VoteLockout]
	RootSlot             *uint64
	AuthorizedVoters     AuthorizedVoters
	PriorVoters          PriorVoters
	EpochCredits         []EpochCredits
	LastTimestamp        BlockTimestamp
}

type VoteState struct {
	NodePubkey           solana.PublicKey
	AuthorizedWithdrawer solana.PublicKey
	Commission           byte
	Votes                deque.Deque[LandedVote]
	RootSlot             *uint64
	AuthorizedVoters     AuthorizedVoters
	PriorVoters          PriorVoters
	EpochCredits         []EpochCredits
	LastTimestamp        BlockTimestamp

	// V4-specific fields preserved through the processing loop.
	// Populated by ConvertToCurrent when source is V4; used by newVoteState4FromCurrent.
	wasV4                       bool
	v4InflationRewardsCollector solana.PublicKey
	v4BlockRevenueCollector     solana.PublicKey
	v4InflationRewardsCommBps   uint16
	v4BlockRevenueCommBps       uint16
	v4PendingDelegatorRewards   uint64
	v4BlsPubkeyCompressed       *[48]byte
}

type VoteState4 struct {
	NodePubkey                    solana.PublicKey
	AuthorizedWithdrawer          solana.PublicKey
	InflationRewardsCollector     solana.PublicKey
	BlockRevenueCollector         solana.PublicKey
	InflationRewardsCommissionBps uint16
	BlockRevenueCommissionBps     uint16
	PendingDelegatorRewards       uint64
	BlsPubkeyCompressed           *[48]byte // Option<[u8;48]>, nil = None
	Votes                         deque.Deque[LandedVote]
	RootSlot                      *uint64
	AuthorizedVoters              AuthorizedVoters
	EpochCredits                  []EpochCredits
	LastTimestamp                 BlockTimestamp
}

type VoteStateVersions struct {
	Type     uint32
	V0_23_5  VoteState0_23_5
	V1_14_11 VoteState1_14_11
	Current  VoteState
	V4       VoteState4
}

func (priorVoter *PriorVoter) UnmarshalWithDecoder(decoder *bin.Decoder, isVersion0_23_5 bool) error {
	pk, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(priorVoter.Pubkey[:], pk)

	priorVoter.EpochStart, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	priorVoter.EpochEnd, err = decoder.ReadUint64(bin.LE)

	if isVersion0_23_5 {
		priorVoter.Slot, err = decoder.ReadUint64(bin.LE)
	}

	return err
}

func (priorVoter *PriorVoter) MarshalWithEncoder(encoder *bin.Encoder, isVersion0_23_5 bool) error {
	err := encoder.WriteBytes(priorVoter.Pubkey[:], false)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(priorVoter.EpochStart, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(priorVoter.EpochEnd, bin.LE)
	if err != nil {
		return err
	}

	if isVersion0_23_5 {
		err = encoder.WriteUint64(priorVoter.Slot, bin.LE)
	}

	return nil
}

func (priorVoters *PriorVoters0_23_5) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	for count := 0; count < 32; count++ {
		var priorVoter PriorVoter
		err = priorVoter.UnmarshalWithDecoder(decoder, true)
		if err != nil {
			return err
		}
		priorVoters.Buf[count] = priorVoter
	}
	priorVoters.Index, err = decoder.ReadUint64(bin.LE)
	return err
}

func (priorVoters *PriorVoters0_23_5) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	for count := 0; count < 32; count++ {
		err = priorVoters.Buf[count].MarshalWithEncoder(encoder, true)
		if err != nil {
			return err
		}
	}

	err = encoder.WriteUint64(priorVoters.Index, bin.LE)
	if err != nil {
		return err
	}
	return nil
}

func (priorVoters *PriorVoters) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	for count := 0; count < 32; count++ {
		err = priorVoters.Buf[count].UnmarshalWithDecoder(decoder, false)
		if err != nil {
			return err
		}
	}
	priorVoters.Index, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	priorVoters.IsEmpty, err = ReadBool(decoder)
	return err
}

func (priorVoters *PriorVoters) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	for count := 0; count < 32; count++ {
		err = priorVoters.Buf[count].MarshalWithEncoder(encoder, false)
		if err != nil {
			return err
		}
	}

	err = encoder.WriteUint64(priorVoters.Index, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteBool(priorVoters.IsEmpty)
	if err != nil {
		return err
	}

	return nil
}

func (priorVoters *PriorVoters) Last() *PriorVoter {
	if !priorVoters.IsEmpty {
		if priorVoters.Index >= uint64(len(priorVoters.Buf)) {
			return nil
		} else {
			return &priorVoters.Buf[priorVoters.Index]
		}
	} else {
		return nil
	}
}

func (priorVoters *PriorVoters) Append(priorVoter PriorVoter) {
	newIdx, err := safemath.CheckedAddU64(priorVoters.Index, 1)
	if err != nil {
		panic("overflow in PriorVoters.Append()")
	}

	newIdx %= 32
	priorVoters.Index = newIdx
	priorVoters.Buf[priorVoters.Index] = priorVoter
	priorVoters.IsEmpty = false
}

func (priorVoters *PriorVoters0_23_5) Append(priorVoter PriorVoter) {
	newIdx, err := safemath.CheckedAddU64(priorVoters.Index, 1)
	if err != nil {
		panic("overflow in PriorVoters.Append()")
	}

	newIdx %= 32
	priorVoters.Index = newIdx
	priorVoters.Buf[priorVoters.Index] = priorVoter
}

func (epochCredits *EpochCredits) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	epochCredits.Epoch, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	epochCredits.Credits, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	epochCredits.PrevCredits, err = decoder.ReadUint64(bin.LE)
	return err
}

func (epochCredits *EpochCredits) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint64(epochCredits.Epoch, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(epochCredits.Credits, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(epochCredits.PrevCredits, bin.LE)
	return err
}

func (landedVote *LandedVote) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	landedVote.Latency, err = decoder.ReadByte()
	if err != nil {
		return err
	}

	err = landedVote.Lockout.UnmarshalWithDecoder(decoder)
	return err
}

func (landedVote *LandedVote) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteByte(landedVote.Latency)
	if err != nil {
		return err
	}

	err = landedVote.Lockout.MarshalWithEncoder(encoder)
	return err
}

func (blockTimestamp *BlockTimestamp) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	blockTimestamp.Slot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	blockTimestamp.Timestamp, err = decoder.ReadInt64(bin.LE)
	return err
}

func (blockTimestamp *BlockTimestamp) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint64(blockTimestamp.Slot, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteInt64(blockTimestamp.Timestamp, bin.LE)
	return err
}

func (voteState *VoteState0_23_5) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	nodePk, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteState.NodePubkey[:], nodePk)

	authVoter, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteState.AuthorizedVoter[:], authVoter)

	voteState.AuthorizedVoterEpoch, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	err = voteState.PriorVoters.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	authWithdrawer, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteState.AuthorizedWithdrawer[:], authWithdrawer)

	voteState.Commission, err = decoder.ReadByte()
	if err != nil {
		return err
	}

	numLockouts, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateVoteStateCollectionLength(decoder, "lockout", numLockouts, 12, MaxLockoutHistory); err != nil {
		return err
	}

	voteState.Votes.Clear()
	voteState.Votes.SetBaseCap(int(numLockouts))
	for count := uint64(0); count < numLockouts; count++ {
		var lockout VoteLockout
		err = lockout.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		voteState.Votes.PushBack(lockout)
	}

	hasRootSlot, err := ReadBool(decoder)
	if err != nil {
		return err
	}

	if hasRootSlot {
		rootSlot, err := decoder.ReadUint64(bin.LE)
		if err != nil {
			return err
		}
		voteState.RootSlot = &rootSlot
	}

	numEpochCredits, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateVoteStateCollectionLength(decoder, "epoch credits", numEpochCredits, 24, MaxEpochCreditsHistory); err != nil {
		return err
	}

	voteState.EpochCredits = voteState.EpochCredits[:0]
	for count := uint64(0); count < numEpochCredits; count++ {
		var epochCredits EpochCredits
		err = epochCredits.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		voteState.EpochCredits = append(voteState.EpochCredits, epochCredits)
	}

	err = voteState.LastTimestamp.UnmarshalWithDecoder(decoder)
	return err
}

func (voteState *VoteState0_23_5) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteBytes(voteState.NodePubkey[:], false)
	if err != nil {
		return err
	}

	err = encoder.WriteBytes(voteState.AuthorizedVoter[:], false)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(voteState.AuthorizedVoterEpoch, bin.LE)
	if err != nil {
		return err
	}

	err = voteState.PriorVoters.MarshalWithEncoder(encoder)
	if err != nil {
		return err
	}

	err = encoder.WriteBytes(voteState.AuthorizedWithdrawer[:], false)
	if err != nil {
		return err
	}

	err = encoder.WriteByte(voteState.Commission)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(uint64(voteState.Votes.Len()), bin.LE)
	if err != nil {
		return err
	}
	for i := 0; i < voteState.Votes.Len(); i++ {
		lockout := voteState.Votes.At(i)
		err = lockout.MarshalWithEncoder(encoder)
		if err != nil {
			break
		}
	}

	if err != nil {
		return err
	}

	if voteState.RootSlot != nil {
		err = encoder.WriteBool(true)
		if err != nil {
			return err
		}

		err = encoder.WriteUint64(*voteState.RootSlot, bin.LE)
		if err != nil {
			return err
		}
	} else {
		err = encoder.WriteBool(false)
		if err != nil {
			return err
		}
	}

	err = encoder.WriteUint64(uint64(len(voteState.EpochCredits)), bin.LE)
	if err != nil {
		return err
	}
	for _, epochCredit := range voteState.EpochCredits {
		err = epochCredit.MarshalWithEncoder(encoder)
		if err != nil {
			return err
		}
	}

	err = voteState.LastTimestamp.MarshalWithEncoder(encoder)
	return err
}

func (authVoter *AuthorizedVoter) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	authVoter.Epoch, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	pk, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(authVoter.Pubkey[:], pk)
	return nil
}

func (authVoter *AuthorizedVoter) MarshalWithEncoder(encoder *bin.Encoder) error {
	err := encoder.WriteUint64(authVoter.Epoch, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteBytes(authVoter.Pubkey[:], false)
	return err
}

func (authVoters *AuthorizedVoters) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	numAuthVoters, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateVoteStateCollectionLength(decoder, "authorized voter", numAuthVoters, 40, 0); err != nil {
		return err
	}

	authVoters.AuthorizedVoters.Clear()
	count := uint64(0)
	for ; count < numAuthVoters; count++ {
		var authVoter AuthorizedVoter
		err = authVoter.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		authVoters.AuthorizedVoters.Set(authVoter.Epoch, authVoter.Pubkey)
	}

	return nil
}

func (authVoters *AuthorizedVoters) MarshalWithEncoder(encoder *bin.Encoder) error {
	err := encoder.WriteUint64(uint64(authVoters.AuthorizedVoters.Len()), bin.LE)
	if err != nil {
		return err
	}

	iter := authVoters.AuthorizedVoters.Iter()
	hasMore := iter.First()

	if !hasMore {
		return nil
	}

	for ; hasMore; hasMore = iter.Next() {
		key := iter.Key()
		val := iter.Value()
		authVoter := AuthorizedVoter{Epoch: key, Pubkey: val}
		err = authVoter.MarshalWithEncoder(encoder)
		if err != nil {
			return err
		}
	}

	return nil
}

func (authVoters *AuthorizedVoters) GetOrCalculateAuthorizedVoterForEpoch(epoch uint64) (solana.PublicKey, bool, error) {
	res, exists := authVoters.AuthorizedVoters.Get(epoch)
	if exists {
		return res, true, nil
	} else {
		latestEpoch := uint64(0)
		var prevPk *solana.PublicKey

		iter := authVoters.AuthorizedVoters.Iter()
		hasEntries := iter.First()
		if !hasEntries {
			return solana.PublicKey{}, false, fmt.Errorf("not found")
		}

		for ; hasEntries; hasEntries = iter.Next() {
			key := iter.Key()
			val := iter.Value()
			if key < epoch && (latestEpoch == 0 || key > latestEpoch) {
				latestEpoch = key
				prevPk = &val
			}
		}

		if prevPk == nil {
			return solana.PublicKey{}, false, fmt.Errorf("not found")
		} else {
			return *prevPk, false, nil
		}
	}
}

func (authVoters *AuthorizedVoters) GetAndCacheAuthorizedVoterForEpoch(epoch uint64) (solana.PublicKey, error) {
	voter, existed, err := authVoters.GetOrCalculateAuthorizedVoterForEpoch(epoch)
	if err != nil {
		return voter, err
	}

	if !existed {
		authVoters.AuthorizedVoters.Set(epoch, voter)
	}
	return voter, nil
}

func (authVoters *AuthorizedVoters) PurgeAuthorizedVoters(currentEpoch uint64) bool {
	var expiredKeys []uint64

	keys, _ := authVoters.AuthorizedVoters.KeyValues()
	for _, key := range keys {
		if key < currentEpoch {
			expiredKeys = append(expiredKeys, key)
		}
	}

	for _, key := range expiredKeys {
		_, success := authVoters.AuthorizedVoters.Delete(key)
		if !success {
			panic("there was no key to remove - programming error")
		}
	}

	if authVoters.AuthorizedVoters.Len() == 0 {
		panic("invariant - AuthorizedVoters should not be empty")
	}
	return true
}

func (lockout *VoteLockout) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	lockout.Slot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	lockout.ConfirmationCount, err = decoder.ReadUint32(bin.LE)
	return err
}

func (lockout *VoteLockout) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint64(lockout.Slot, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteUint32(lockout.ConfirmationCount, bin.LE)
	return err
}

const InitialLockout = 2

func (lockout *VoteLockout) Lockout() uint64 {
	return uint64(math.Pow(InitialLockout, float64(lockout.ConfirmationCount)))
}

func (lockout *VoteLockout) LastLockedOutSlot() uint64 {
	return safemath.SaturatingAddU64(lockout.Slot, lockout.Lockout())
}

func (lockout *VoteLockout) IsLockedOutAtSlot(slot uint64) bool {
	return lockout.LastLockedOutSlot() >= slot
}

func (lockout *VoteLockout) IncreaseConfirmationCount(by uint32) {
	lockout.ConfirmationCount = safemath.SaturatingAddU32(lockout.ConfirmationCount, by)
}

func (voteState *VoteState1_14_11) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	nodePk, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteState.NodePubkey[:], nodePk)

	authWithdrawer, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteState.AuthorizedWithdrawer[:], authWithdrawer)

	voteState.Commission, err = decoder.ReadByte()
	if err != nil {
		return err
	}

	numLockouts, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateVoteStateCollectionLength(decoder, "lockout", numLockouts, 12, MaxLockoutHistory); err != nil {
		return err
	}

	voteState.Votes.Clear()
	voteState.Votes.SetBaseCap(int(numLockouts))
	for count := uint64(0); count < numLockouts; count++ {
		var lockout VoteLockout
		err = lockout.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		voteState.Votes.PushBack(lockout)
	}

	hasRootSlot, err := ReadBool(decoder)
	if err != nil {
		return err
	}

	if hasRootSlot {
		rootSlot, err := decoder.ReadUint64(bin.LE)
		if err != nil {
			return err
		}
		voteState.RootSlot = &rootSlot
	}

	err = voteState.AuthorizedVoters.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	err = voteState.PriorVoters.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	numEpochCredits, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateVoteStateCollectionLength(decoder, "epoch credits", numEpochCredits, 24, MaxEpochCreditsHistory); err != nil {
		return err
	}

	voteState.EpochCredits = voteState.EpochCredits[:0]
	for count := uint64(0); count < numEpochCredits; count++ {
		var epochCredits EpochCredits
		err = epochCredits.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		voteState.EpochCredits = append(voteState.EpochCredits, epochCredits)
	}

	err = voteState.LastTimestamp.UnmarshalWithDecoder(decoder)
	return err
}

func (voteState *VoteState1_14_11) MarshalWithEncoder(encoder *bin.Encoder) error {
	err := encoder.WriteBytes(voteState.NodePubkey[:], false)
	if err != nil {
		return err
	}

	err = encoder.WriteBytes(voteState.AuthorizedWithdrawer[:], false)
	if err != nil {
		return err
	}

	err = encoder.WriteByte(voteState.Commission)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(uint64(voteState.Votes.Len()), bin.LE)
	if err != nil {
		return err
	}

	for i := 0; i < voteState.Votes.Len(); i++ {
		lockout := voteState.Votes.At(i)
		err = lockout.MarshalWithEncoder(encoder)
		if err != nil {
			break
		}
	}

	if voteState.RootSlot != nil {
		err = encoder.WriteBool(true)
		if err != nil {
			return err
		}

		err = encoder.WriteUint64(*voteState.RootSlot, bin.LE)
		if err != nil {
			return err
		}
	} else {
		err = encoder.WriteBool(false)
		if err != nil {
			return err
		}
	}

	err = voteState.AuthorizedVoters.MarshalWithEncoder(encoder)
	if err != nil {
		return err
	}

	err = voteState.PriorVoters.MarshalWithEncoder(encoder)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(uint64(len(voteState.EpochCredits)), bin.LE)
	if err != nil {
		return err
	}

	for _, epochCredits := range voteState.EpochCredits {
		err = epochCredits.MarshalWithEncoder(encoder)
		if err != nil {
			return err
		}
	}

	err = voteState.LastTimestamp.MarshalWithEncoder(encoder)
	return err
}

func (voteState *VoteState) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	nodePk, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteState.NodePubkey[:], nodePk)

	authWithdrawer, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteState.AuthorizedWithdrawer[:], authWithdrawer)

	voteState.Commission, err = decoder.ReadByte()
	if err != nil {
		return err
	}

	numLockouts, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateVoteStateCollectionLength(decoder, "landed vote", numLockouts, 13, MaxLockoutHistory); err != nil {
		return err
	}

	voteState.Votes.Clear()
	voteState.Votes.SetBaseCap(int(numLockouts))
	for count := uint64(0); count < numLockouts; count++ {
		var landedVote LandedVote
		err = landedVote.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		voteState.Votes.PushBack(landedVote)
	}

	hasRootSlot, err := ReadBool(decoder)
	if err != nil {
		return err
	}

	if hasRootSlot {
		rootSlot, err := decoder.ReadUint64(bin.LE)
		if err != nil {
			return err
		}
		voteState.RootSlot = &rootSlot
	}

	err = voteState.AuthorizedVoters.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	err = voteState.PriorVoters.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	numEpochCredits, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateVoteStateCollectionLength(decoder, "epoch credits", numEpochCredits, 24, MaxEpochCreditsHistory); err != nil {
		return err
	}

	voteState.EpochCredits = slices.Grow(voteState.EpochCredits[:0], int(numEpochCredits))
	for count := uint64(0); count < numEpochCredits; count++ {
		var epochCredits EpochCredits
		err = epochCredits.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		voteState.EpochCredits = append(voteState.EpochCredits, epochCredits)
	}

	err = voteState.LastTimestamp.UnmarshalWithDecoder(decoder)
	return err
}

func (voteState *VoteState) MarshalWithEncoder(encoder *bin.Encoder) error {
	err := encoder.WriteBytes(voteState.NodePubkey[:], false)
	if err != nil {
		return err
	}

	err = encoder.WriteBytes(voteState.AuthorizedWithdrawer[:], false)
	if err != nil {
		return err
	}

	err = encoder.WriteByte(voteState.Commission)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(uint64(voteState.Votes.Len()), bin.LE)
	if err != nil {
		return err
	}

	for i := 0; i < voteState.Votes.Len(); i++ {
		landedVote := voteState.Votes.At(i)
		err = landedVote.MarshalWithEncoder(encoder)
		if err != nil {
			break
		}
	}

	if voteState.RootSlot != nil {
		err = encoder.WriteBool(true)
		if err != nil {
			return err
		}

		err = encoder.WriteUint64(*voteState.RootSlot, bin.LE)
		if err != nil {
			return err
		}
	} else {
		err = encoder.WriteBool(false)
		if err != nil {
			return err
		}
	}

	err = voteState.AuthorizedVoters.MarshalWithEncoder(encoder)
	if err != nil {
		return err
	}

	err = voteState.PriorVoters.MarshalWithEncoder(encoder)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(uint64(len(voteState.EpochCredits)), bin.LE)
	if err != nil {
		return err
	}

	for _, epochCredits := range voteState.EpochCredits {
		err = epochCredits.MarshalWithEncoder(encoder)
		if err != nil {
			return err
		}
	}

	err = voteState.LastTimestamp.MarshalWithEncoder(encoder)
	return err
}

func (voteState *VoteState4) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	nodePk, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteState.NodePubkey[:], nodePk)

	authWithdrawer, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteState.AuthorizedWithdrawer[:], authWithdrawer)

	inflationCollector, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteState.InflationRewardsCollector[:], inflationCollector)

	blockCollector, err := decoder.ReadBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(voteState.BlockRevenueCollector[:], blockCollector)

	voteState.InflationRewardsCommissionBps, err = decoder.ReadUint16(bin.LE)
	if err != nil {
		return err
	}

	voteState.BlockRevenueCommissionBps, err = decoder.ReadUint16(bin.LE)
	if err != nil {
		return err
	}

	voteState.PendingDelegatorRewards, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	// Option<[u8; 48]>
	hasBls, err := ReadBool(decoder)
	if err != nil {
		return err
	}
	if hasBls {
		blsBytes, err := decoder.ReadBytes(48)
		if err != nil {
			return err
		}
		var bls [48]byte
		copy(bls[:], blsBytes)
		voteState.BlsPubkeyCompressed = &bls
	} else {
		voteState.BlsPubkeyCompressed = nil
	}

	numLockouts, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateVoteStateCollectionLength(decoder, "landed vote", numLockouts, 13, MaxLockoutHistory); err != nil {
		return err
	}

	voteState.Votes.Clear()
	voteState.Votes.SetBaseCap(int(numLockouts))
	for count := uint64(0); count < numLockouts; count++ {
		var landedVote LandedVote
		err = landedVote.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		voteState.Votes.PushBack(landedVote)
	}

	hasRootSlot, err := ReadBool(decoder)
	if err != nil {
		return err
	}

	if hasRootSlot {
		rootSlot, err := decoder.ReadUint64(bin.LE)
		if err != nil {
			return err
		}
		voteState.RootSlot = &rootSlot
	}

	err = voteState.AuthorizedVoters.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	numEpochCredits, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateVoteStateCollectionLength(decoder, "epoch credits", numEpochCredits, 24, MaxEpochCreditsHistory); err != nil {
		return err
	}

	voteState.EpochCredits = slices.Grow(voteState.EpochCredits[:0], int(numEpochCredits))
	for count := uint64(0); count < numEpochCredits; count++ {
		var epochCredits EpochCredits
		err = epochCredits.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		voteState.EpochCredits = append(voteState.EpochCredits, epochCredits)
	}

	err = voteState.LastTimestamp.UnmarshalWithDecoder(decoder)
	return err
}

func (voteState *VoteState4) MarshalWithEncoder(encoder *bin.Encoder) error {
	err := encoder.WriteBytes(voteState.NodePubkey[:], false)
	if err != nil {
		return err
	}

	err = encoder.WriteBytes(voteState.AuthorizedWithdrawer[:], false)
	if err != nil {
		return err
	}

	err = encoder.WriteBytes(voteState.InflationRewardsCollector[:], false)
	if err != nil {
		return err
	}

	err = encoder.WriteBytes(voteState.BlockRevenueCollector[:], false)
	if err != nil {
		return err
	}

	err = encoder.WriteUint16(voteState.InflationRewardsCommissionBps, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteUint16(voteState.BlockRevenueCommissionBps, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(voteState.PendingDelegatorRewards, bin.LE)
	if err != nil {
		return err
	}

	// Option<[u8; 48]>
	if voteState.BlsPubkeyCompressed != nil {
		err = encoder.WriteBool(true)
		if err != nil {
			return err
		}
		err = encoder.WriteBytes(voteState.BlsPubkeyCompressed[:], false)
		if err != nil {
			return err
		}
	} else {
		err = encoder.WriteBool(false)
		if err != nil {
			return err
		}
	}

	err = encoder.WriteUint64(uint64(voteState.Votes.Len()), bin.LE)
	if err != nil {
		return err
	}

	for i := 0; i < voteState.Votes.Len(); i++ {
		landedVote := voteState.Votes.At(i)
		err = landedVote.MarshalWithEncoder(encoder)
		if err != nil {
			break
		}
	}

	if voteState.RootSlot != nil {
		err = encoder.WriteBool(true)
		if err != nil {
			return err
		}

		err = encoder.WriteUint64(*voteState.RootSlot, bin.LE)
		if err != nil {
			return err
		}
	} else {
		err = encoder.WriteBool(false)
		if err != nil {
			return err
		}
	}

	err = voteState.AuthorizedVoters.MarshalWithEncoder(encoder)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(uint64(len(voteState.EpochCredits)), bin.LE)
	if err != nil {
		return err
	}

	for _, epochCredits := range voteState.EpochCredits {
		err = epochCredits.MarshalWithEncoder(encoder)
		if err != nil {
			return err
		}
	}

	err = voteState.LastTimestamp.MarshalWithEncoder(encoder)
	return err
}

func (voteState *VoteState) GetAndUpdateAuthorizedVoter(currentEpoch uint64, f features.Features) (solana.PublicKey, error) {
	pubkey, err := voteState.AuthorizedVoters.GetAndCacheAuthorizedVoterForEpoch(currentEpoch)
	if err != nil {
		return pubkey, InstrErrInvalidAccountData
	}

	purgeEpoch := currentEpoch
	if f.IsActive(features.VoteStateV4) {
		purgeEpoch = safemath.SaturatingSubU64(currentEpoch, 1)
	}
	voteState.AuthorizedVoters.PurgeAuthorizedVoters(purgeEpoch)
	return pubkey, nil
}

func (voteState *VoteState) Credits() uint64 {
	if len(voteState.EpochCredits) == 0 {
		return 0
	} else {
		return voteState.EpochCredits[len(voteState.EpochCredits)-1].Credits
	}
}

func (voteState *VoteState) SetNewAuthorizedVoter(authorized solana.PublicKey, currentEpoch uint64, targetEpoch uint64, blsPubkey *[blsPublicKeyCompressedSize]byte, verify func(epochAuthorizedVoter solana.PublicKey) error, f features.Features) error {
	epochAuthorizedVoter, err := voteState.GetAndUpdateAuthorizedVoter(currentEpoch, f)
	if err != nil {
		return err
	}

	err = verify(epochAuthorizedVoter)
	if err != nil {
		return err
	}

	_, exists := voteState.AuthorizedVoters.AuthorizedVoters.Get(targetEpoch)
	if exists {
		return VoteErrTooSoonToReauthorize
	}

	iter := voteState.AuthorizedVoters.AuthorizedVoters.Iter()
	exists = iter.Last()
	if !exists {
		return InstrErrInvalidAccountData
	}

	latestEpoch := iter.Key()
	latestAuthPubkey := iter.Value()

	if latestAuthPubkey != authorized {
		// V4: skip PriorVoters tracking entirely (V4 has no PriorVoters field)
		if !f.IsActive(features.VoteStateV4) {
			var epochOfLastAuthorizedSwitch uint64
			last := voteState.PriorVoters.Last()
			if last != nil {
				epochOfLastAuthorizedSwitch = last.EpochEnd
			} else {
				epochOfLastAuthorizedSwitch = 0
			}

			voteState.PriorVoters.Append(PriorVoter{Pubkey: latestAuthPubkey, EpochStart: epochOfLastAuthorizedSwitch, EpochEnd: targetEpoch})
		}

		if targetEpoch <= latestEpoch {
			return InstrErrInvalidAccountData
		}
	}

	voteState.AuthorizedVoters.AuthorizedVoters.Set(targetEpoch, authorized)
	if blsPubkey != nil {
		copied := *blsPubkey
		voteState.v4BlsPubkeyCompressed = &copied
	}
	return nil
}

func (voteState *VoteState) LastLockout() *VoteLockout {
	if voteState.Votes.Len() == 0 {
		return nil
	}
	landedVote := voteState.Votes.Back()
	return &landedVote.Lockout
}

func (voteState *VoteState) LastVotedSlot() (uint64, bool) {
	lastLockout := voteState.LastLockout()
	if lastLockout == nil {
		return 0, false
	} else {
		return lastLockout.Slot, true
	}
}

func (voteState *VoteState) PopExpiredVotes(nextVoteSlot uint64) {
	var vote *VoteLockout
	for {
		vote = voteState.LastLockout()
		if vote == nil {
			break
		}
		if !vote.IsLockedOutAtSlot(nextVoteSlot) {
			voteState.Votes.PopBack()
		} else {
			break
		}
	}
}

func (voteState *VoteState) DoubleLockouts() {
	var indicesToIncreaseConfirmationCount []int
	stackDepth := uint64(voteState.Votes.Len())
	for idx := 0; idx < voteState.Votes.Len(); idx++ {
		landedVote := voteState.Votes.At(idx)
		j, err := safemath.CheckedAddU64(uint64(idx), uint64(landedVote.Lockout.ConfirmationCount))
		if err != nil {
			panic("`confirmation_count` and tower_size should be bounded by `MAX_LOCKOUT_HISTORY`")
		}
		if stackDepth > j {
			indicesToIncreaseConfirmationCount = append(indicesToIncreaseConfirmationCount, idx)
		}
	}

	for _, idx := range indicesToIncreaseConfirmationCount {
		landedVote := voteState.Votes.At(idx)
		landedVote.Lockout.IncreaseConfirmationCount(1)
		voteState.Votes.Set(idx, landedVote)
	}
}

func (voteState *VoteState) ContainsSlot(candidateSlot uint64) bool {
	for i := 0; i < voteState.Votes.Len(); i++ {
		v := voteState.Votes.At(i)
		if v.Lockout.Slot == candidateSlot {
			return true
		}
	}
	return false
}

const (
	MaxLockoutHistory            = 31
	VoteCreditsGraceSlots        = 2
	VoteCreditsMaximumPerSlot    = 16
	VoteCreditsMaximumPerSlotOld = 8
	MaxEpochCreditsHistory       = 64
)

func (voteState *VoteState) CreditsForVoteAtIndex(index uint64, timelyVoteCredits, deprecateUnusedLegacyVotePlumbing bool) uint64 {
	landedVote := voteState.Votes.At(int(index))
	latency := landedVote.Latency

	var maxCredits byte
	if deprecateUnusedLegacyVotePlumbing {
		maxCredits = VoteCreditsMaximumPerSlot
	} else {
		maxCredits = VoteCreditsMaximumPerSlotOld
	}

	if latency == 0 || (deprecateUnusedLegacyVotePlumbing && !timelyVoteCredits) {
		return 1
	} else {
		diff, err := safemath.CheckedSubU8(latency, VoteCreditsGraceSlots)
		if err != nil || diff == 0 {
			return uint64(maxCredits)
		} else {
			credits, err := safemath.CheckedSubU8(maxCredits, diff)
			if err != nil || credits == 0 {
				return 1
			} else {
				return uint64(credits)
			}
		}
	}
}

func (voteState *VoteState) IncrementCredits(epoch uint64, credits uint64) {
	if len(voteState.EpochCredits) == 0 {
		voteState.EpochCredits = append(voteState.EpochCredits, EpochCredits{Epoch: epoch, Credits: 0, PrevCredits: 0})
	} else if epoch != voteState.EpochCredits[len(voteState.EpochCredits)-1].Epoch {
		ec := voteState.EpochCredits[len(voteState.EpochCredits)-1]
		if ec.Credits != ec.PrevCredits {
			voteState.EpochCredits = append(voteState.EpochCredits, EpochCredits{Epoch: epoch, Credits: ec.Credits, PrevCredits: ec.Credits})
		} else {
			voteState.EpochCredits[len(voteState.EpochCredits)-1].Epoch = epoch
		}

		if len(voteState.EpochCredits) > MaxEpochCreditsHistory {
			voteState.EpochCredits = voteState.EpochCredits[1:]
		}
	}

	newCredits := safemath.SaturatingAddU64(voteState.EpochCredits[len(voteState.EpochCredits)-1].Credits, credits)
	voteState.EpochCredits[len(voteState.EpochCredits)-1].Credits = newCredits
}

func computeVoteLatency(votedForSlot uint64, currentSlot uint64) byte {
	return byte(min(safemath.SaturatingSubU64(currentSlot, votedForSlot), math.MaxUint8))
}

func (voteState *VoteState) ProcessNextVoteSlot(nextVoteSlot uint64, epoch uint64, currentSlot uint64, timelyVoteCredits, deprecateUnusedLegacyVotePlumbing bool) {
	lastVotedSlot, ok := voteState.LastVotedSlot()
	if ok && nextVoteSlot <= lastVotedSlot {
		return
	}

	voteState.PopExpiredVotes(nextVoteSlot)

	var latency byte
	if timelyVoteCredits || !deprecateUnusedLegacyVotePlumbing {
		latency = computeVoteLatency(nextVoteSlot, currentSlot)
	}

	landedVote := LandedVote{Latency: latency, Lockout: VoteLockout{Slot: nextVoteSlot, ConfirmationCount: 1}}

	if voteState.Votes.Len() == MaxLockoutHistory {
		credits := voteState.CreditsForVoteAtIndex(0, timelyVoteCredits, deprecateUnusedLegacyVotePlumbing)
		landedVote := voteState.Votes.PopFront()
		voteState.RootSlot = &landedVote.Lockout.Slot

		voteState.IncrementCredits(epoch, credits)
	}

	voteState.Votes.PushBack(landedVote)
	voteState.DoubleLockouts()
}

func (voteState *VoteState) ProcessTimestamp(slot uint64, timestamp int64) error {
	if (slot < voteState.LastTimestamp.Slot || timestamp < voteState.LastTimestamp.Timestamp) ||
		(slot == voteState.LastTimestamp.Slot &&
			(slot != voteState.LastTimestamp.Slot || timestamp != voteState.LastTimestamp.Timestamp) &&
			voteState.LastTimestamp.Slot != 0) {
		return VoteErrTimestampTooOld
	}

	voteState.LastTimestamp = BlockTimestamp{Slot: slot, Timestamp: timestamp}
	return nil
}

func (voteStateVersions *VoteStateVersions) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	voteStateVersions.Type, err = decoder.ReadUint32(bin.LE)
	if err != nil {
		return err
	}

	switch voteStateVersions.Type {
	case VoteStateVersionV0_23_5:
		{
			err = voteStateVersions.V0_23_5.UnmarshalWithDecoder(decoder)
		}
	case VoteStateVersionV1_14_11:
		{
			err = voteStateVersions.V1_14_11.UnmarshalWithDecoder(decoder)
		}
	case VoteStateVersionCurrent:
		{
			err = voteStateVersions.Current.UnmarshalWithDecoder(decoder)
		}
	case VoteStateVersionV4:
		{
			err = voteStateVersions.V4.UnmarshalWithDecoder(decoder)
		}
	default:
		{
			//mlog.Log.Debugf("invalid vote state type: %d", voteStateVersions.Type)
			err = InstrErrInvalidAccountData
		}
	}
	return err
}

func (voteStateVersions *VoteStateVersions) MarshalWithEncoder(encoder *bin.Encoder) error {
	err := encoder.WriteUint32(voteStateVersions.Type, bin.LE)
	if err != nil {
		return err
	}

	switch voteStateVersions.Type {
	case VoteStateVersionV0_23_5:
		{
			err = voteStateVersions.V0_23_5.MarshalWithEncoder(encoder)
		}
	case VoteStateVersionV1_14_11:
		{
			err = voteStateVersions.V1_14_11.MarshalWithEncoder(encoder)
		}
	case VoteStateVersionCurrent:
		{
			err = voteStateVersions.Current.MarshalWithEncoder(encoder)
		}
	case VoteStateVersionV4:
		{
			err = voteStateVersions.V4.MarshalWithEncoder(encoder)
		}
	}

	return err
}

func (voteStateVersions *VoteStateVersions) IsInitialized() bool {
	switch voteStateVersions.Type {
	case VoteStateVersionV0_23_5:
		{
			return voteStateVersions.V0_23_5.AuthorizedVoter != solana.PublicKey{}
		}
	case VoteStateVersionV1_14_11:
		{
			return voteStateVersions.V1_14_11.AuthorizedVoters.AuthorizedVoters.Len() != 0
		}
	case VoteStateVersionCurrent:
		{
			return voteStateVersions.Current.AuthorizedVoters.AuthorizedVoters.Len() != 0
		}
	case VoteStateVersionV4:
		{
			return voteStateVersions.V4.AuthorizedVoters.AuthorizedVoters.Len() != 0
		}
	default:
		{
			panic("VoteStateVersions in invalid state - programming error")
		}
	}
}

func (voteStateVersions *VoteStateVersions) ConvertToCurrent() *VoteState {
	switch voteStateVersions.Type {
	case VoteStateVersionV0_23_5:
		{
			state := &voteStateVersions.V0_23_5

			var authVoters AuthorizedVoters
			authVoters.AuthorizedVoters.Set(state.AuthorizedVoterEpoch, state.AuthorizedVoter)

			newVoteState := &VoteState{NodePubkey: state.NodePubkey,
				AuthorizedWithdrawer: state.AuthorizedWithdrawer,
				Commission:           state.Commission,
				RootSlot:             state.RootSlot,
				AuthorizedVoters:     authVoters,
				EpochCredits:         state.EpochCredits,
				LastTimestamp:        state.LastTimestamp,
				PriorVoters:          PriorVoters{Index: 31, IsEmpty: true},
			}

			newVoteState.Votes.Clear()
			newVoteState.Votes.SetBaseCap(state.Votes.Len())
			for i := 0; i < state.Votes.Len(); i++ {
				lockout := state.Votes.At(i)
				newVoteState.Votes.PushBack(LandedVote{Latency: 0, Lockout: lockout})
			}

			return newVoteState
		}

	case VoteStateVersionV1_14_11:
		{
			state := &voteStateVersions.V1_14_11

			newVoteState := &VoteState{NodePubkey: state.NodePubkey,
				AuthorizedWithdrawer: state.AuthorizedWithdrawer,
				Commission:           state.Commission,
				RootSlot:             state.RootSlot,
				AuthorizedVoters:     state.AuthorizedVoters,
				PriorVoters:          state.PriorVoters,
				EpochCredits:         state.EpochCredits,
				LastTimestamp:        state.LastTimestamp}

			newVoteState.Votes.Clear()
			newVoteState.Votes.SetBaseCap(state.Votes.Len())
			for i := 0; i < state.Votes.Len(); i++ {
				lockout := state.Votes.At(i)
				newVoteState.Votes.PushBack(LandedVote{Latency: 0, Lockout: lockout})
			}

			return newVoteState
		}

	case VoteStateVersionCurrent:
		{
			return &voteStateVersions.Current
		}

	case VoteStateVersionV4:
		{
			state := &voteStateVersions.V4

			newVoteState := &VoteState{
				NodePubkey:           state.NodePubkey,
				AuthorizedWithdrawer: state.AuthorizedWithdrawer,
				Commission:           byte(state.InflationRewardsCommissionBps / 100),
				RootSlot:             state.RootSlot,
				AuthorizedVoters:     state.AuthorizedVoters,
				PriorVoters:          PriorVoters{Index: 31, IsEmpty: true},
				EpochCredits:         state.EpochCredits,
				LastTimestamp:        state.LastTimestamp,
				Votes:                state.Votes,

				// Preserve V4-specific fields for the write path
				wasV4:                       true,
				v4InflationRewardsCollector: state.InflationRewardsCollector,
				v4BlockRevenueCollector:     state.BlockRevenueCollector,
				v4InflationRewardsCommBps:   state.InflationRewardsCommissionBps,
				v4BlockRevenueCommBps:       state.BlockRevenueCommissionBps,
				v4PendingDelegatorRewards:   state.PendingDelegatorRewards,
				v4BlsPubkeyCompressed:       state.BlsPubkeyCompressed,
			}

			return newVoteState
		}

	default:
		{
			panic("vote account in invalid state - potential programming error")
		}
	}
}

func (voteStateVersions *VoteStateVersions) LastTimestamp() *BlockTimestamp {
	switch voteStateVersions.Type {
	case VoteStateVersionV0_23_5:
		{
			return &voteStateVersions.V0_23_5.LastTimestamp
		}

	case VoteStateVersionV1_14_11:
		{
			return &voteStateVersions.V1_14_11.LastTimestamp
		}

	case VoteStateVersionCurrent:
		{
			return &voteStateVersions.Current.LastTimestamp
		}

	case VoteStateVersionV4:
		{
			return &voteStateVersions.V4.LastTimestamp
		}

	default:
		{
			panic("vote account in invalid state - potential programming error")
		}
	}
}

var unknownVoteStateVersionOnce sync.Once

func (voteStateVersions *VoteStateVersions) NodePubkey() solana.PublicKey {
	if voteStateVersions == nil {
		return solana.PublicKey{}
	}
	switch voteStateVersions.Type {
	case VoteStateVersionV0_23_5:
		return voteStateVersions.V0_23_5.NodePubkey
	case VoteStateVersionV1_14_11:
		return voteStateVersions.V1_14_11.NodePubkey
	case VoteStateVersionCurrent:
		return voteStateVersions.Current.NodePubkey
	case VoteStateVersionV4:
		return voteStateVersions.V4.NodePubkey
	default:
		// Log unknown version once to avoid flooding hot paths
		unknownVoteStateVersionOnce.Do(func() {
			fmt.Printf("WARNING: NodePubkey() encountered unknown vote state version type %d\n", voteStateVersions.Type)
		})
		return solana.PublicKey{} // Unknown version - count as missing
	}
}

func (voteStateVersions *VoteStateVersions) BlsPubkeyCompressed() *[48]byte {
	if voteStateVersions == nil {
		return nil
	}
	switch voteStateVersions.Type {
	case VoteStateVersionV4:
		return voteStateVersions.V4.BlsPubkeyCompressed
	default:
		return nil
	}
}

func UnmarshalVersionedVoteState(data []byte) (*VoteStateVersions, error) {
	versioned := new(VoteStateVersions)
	decoder := bin.NewBinDecoder(data)

	err := versioned.UnmarshalWithDecoder(decoder)
	if err != nil {
		return nil, InstrErrInvalidAccountData
	} else {
		return versioned, nil
	}
}

var voteStateBufPool = &sync.Pool{New: func() any {
	return make([]byte, 0, 4096)
}}

func marshalVersionedVoteState(voteStateVersions *VoteStateVersions) ([]byte, error) {
	var buffer *bytes.Buffer
	if sbpf.UsePool {
		bs := voteStateBufPool.Get().([]byte)
		bs = bs[:0]
		buffer = bytes.NewBuffer(bs)
	} else {
		buffer = &bytes.Buffer{}
	}
	encoder := bin.NewBinEncoder(buffer)

	err := voteStateVersions.MarshalWithEncoder(encoder)
	if err != nil {
		return nil, err
	} else {
		return buffer.Bytes(), nil
	}
}

// MarshalVersionedVoteState serializes a versioned vote account state.
func MarshalVersionedVoteState(voteStateVersions *VoteStateVersions) ([]byte, error) {
	return marshalVersionedVoteState(voteStateVersions)
}

// WriteVersionedVoteStateInPlace serializes a vote state without changing the
// account data length. Vote accounts retain zero padding after the encoded state.
func WriteVersionedVoteStateInPlace(data []byte, voteStateVersions *VoteStateVersions) error {
	marshaled, err := marshalVersionedVoteState(voteStateVersions)
	if err != nil {
		return err
	}
	if len(marshaled) > len(data) {
		return fmt.Errorf("vote state serialized length %d exceeds account data capacity %d", len(marshaled), len(data))
	}
	copy(data, marshaled)
	clear(data[len(marshaled):])
	return nil
}

func newVoteState1_14_11FromCurrent(voteState *VoteState) *VoteState1_14_11 {
	newVoteState := new(VoteState1_14_11)
	newVoteState.NodePubkey = voteState.NodePubkey
	newVoteState.AuthorizedWithdrawer = voteState.AuthorizedWithdrawer
	newVoteState.Commission = voteState.Commission
	newVoteState.RootSlot = voteState.RootSlot
	newVoteState.AuthorizedVoters = voteState.AuthorizedVoters
	newVoteState.PriorVoters = voteState.PriorVoters
	newVoteState.EpochCredits = voteState.EpochCredits
	newVoteState.LastTimestamp = voteState.LastTimestamp

	newVoteState.Votes.SetBaseCap(voteState.Votes.Len())
	for i := 0; i < voteState.Votes.Len(); i++ {
		landedVote := voteState.Votes.At(i)
		newVoteState.Votes.PushBack(landedVote.Lockout)
	}

	return newVoteState
}

func newVoteStateFromVoteInit(voteInit VoteInstrVoteInit, clock SysvarClock) *VoteState {
	voteState := new(VoteState)
	voteState.NodePubkey = voteInit.NodePubkey

	var authVoters AuthorizedVoters
	authVoters.AuthorizedVoters.Set(clock.Epoch, voteInit.AuthorizedVoter)
	voteState.AuthorizedVoters = authVoters

	voteState.AuthorizedWithdrawer = voteInit.AuthorizedWithdrawer
	voteState.Commission = voteInit.Commission
	voteState.PriorVoters.Index = 31
	voteState.PriorVoters.IsEmpty = true

	return voteState
}

func newVoteState4FromCurrent(vs *VoteState, votePubkey solana.PublicKey) *VoteState4 {
	vs4 := &VoteState4{
		NodePubkey:           vs.NodePubkey,
		AuthorizedWithdrawer: vs.AuthorizedWithdrawer,
		Votes:                vs.Votes,
		RootSlot:             vs.RootSlot,
		AuthorizedVoters:     vs.AuthorizedVoters,
		EpochCredits:         vs.EpochCredits,
		LastTimestamp:        vs.LastTimestamp,
	}

	if vs.wasV4 {
		// Preserve V4-specific fields from the original V4 account
		vs4.InflationRewardsCollector = vs.v4InflationRewardsCollector
		vs4.BlockRevenueCollector = vs.v4BlockRevenueCollector
		vs4.InflationRewardsCommissionBps = vs.v4InflationRewardsCommBps
		vs4.BlockRevenueCommissionBps = vs.v4BlockRevenueCommBps
		vs4.PendingDelegatorRewards = vs.v4PendingDelegatorRewards
		vs4.BlsPubkeyCompressed = vs.v4BlsPubkeyCompressed
	} else {
		// First-time V3/V1_14_11 → V4 conversion: use Agave defaults
		vs4.InflationRewardsCollector = votePubkey
		vs4.BlockRevenueCollector = vs.NodePubkey
		vs4.InflationRewardsCommissionBps = uint16(vs.Commission) * 100
		vs4.BlockRevenueCommissionBps = 10000
		vs4.PendingDelegatorRewards = 0
		vs4.BlsPubkeyCompressed = nil
	}

	return vs4
}

func setVoteAccountState(execCtx *ExecutionCtx, acct *BorrowedAccount, voteState *VoteState, f features.Features) error {
	var err error
	if f.IsActive(features.VoteStateV4) {
		vsz := VoteStateV3Size
		resizeNeeded := len(acct.Data()) < vsz

		resizeRentExempt := acct.IsRentExemptAtDataLength(uint64(vsz))
		resizeFailed := false

		if resizeNeeded && resizeRentExempt {
			err = acct.SetDataLength(VoteStateV3Size, f)
			if err != nil {
				resizeFailed = true
			}
		}

		// V4: NO V1_14_11 fallback — error on resize failure (per SIMD-0185)
		if resizeNeeded && (!resizeRentExempt || resizeFailed) {
			return InstrErrAccountNotRentExempt
		}

		vs4 := newVoteState4FromCurrent(voteState, acct.Key())
		newVersioned := &VoteStateVersions{Type: VoteStateVersionV4}
		newVersioned.V4 = *vs4
		execCtx.AddModifiedVoteState(acct.Key(), newVersioned)
		voteStateBytes, err := marshalVersionedVoteState(newVersioned)
		defer voteStateBufPool.Put(voteStateBytes)
		if err != nil {
			return err
		}
		return acct.SetState(f, voteStateBytes)
	} else if f.IsActive(features.VoteStateAddVoteLatency) {
		vsz := VoteStateV3Size
		resizeNeeded := len(acct.Data()) < vsz

		resizeRentExempt := acct.IsRentExemptAtDataLength(uint64(vsz))
		resizeFailed := false

		if resizeNeeded && resizeRentExempt {
			err = acct.SetDataLength(VoteStateV3Size, f)
			if err != nil {
				resizeFailed = true
			}
		}

		if resizeNeeded && (!resizeRentExempt || resizeFailed) {
			newVoteState := newVoteState1_14_11FromCurrent(voteState)
			newVoteStateVersioned := new(VoteStateVersions)
			newVoteStateVersioned.Type = VoteStateVersionV1_14_11
			newVoteStateVersioned.V1_14_11 = *newVoteState
			execCtx.AddModifiedVoteState(acct.Key(), newVoteStateVersioned)
			voteStateBytes, err := marshalVersionedVoteState(newVoteStateVersioned)
			defer voteStateBufPool.Put(voteStateBytes)
			if err != nil {
				return err
			}
			err = acct.SetState(f, voteStateBytes)
			return err
		}

		newCurrent := new(VoteStateVersions)
		newCurrent.Type = VoteStateVersionCurrent
		newCurrent.Current = *voteState
		execCtx.AddModifiedVoteState(acct.Key(), newCurrent)
		voteStateBytes, err := marshalVersionedVoteState(newCurrent)
		defer voteStateBufPool.Put(voteStateBytes)
		if err != nil {
			return err
		}
		err = acct.SetState(f, voteStateBytes)
		return err
	} else {
		newVoteState := newVoteState1_14_11FromCurrent(voteState)
		newVoteStateVersioned := new(VoteStateVersions)
		newVoteStateVersioned.Type = VoteStateVersionV1_14_11
		newVoteStateVersioned.V1_14_11 = *newVoteState
		execCtx.AddModifiedVoteState(acct.Key(), newVoteStateVersioned)
		voteStateBytes, err := marshalVersionedVoteState(newVoteStateVersioned)
		defer voteStateBufPool.Put(voteStateBytes)
		if err != nil {
			return err
		}
		err = acct.SetState(f, voteStateBytes)
		return err
	}
}
