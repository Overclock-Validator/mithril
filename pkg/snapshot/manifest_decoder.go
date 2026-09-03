package snapshot

import (
	"fmt"

	//"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/util"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

type HashAge struct {
	FeeCalculator sealevel.FeeCalculator
	HashIndex     uint64
	Timestamp     uint64
}

type HashAgePair struct {
	Key [32]byte
	Val HashAge
}

type BlockHashVec struct {
	LastHashIndex uint64
	LastHash      *[32]byte
	HashAndAge    []HashAgePair
	MaxAge        uint64
}

type SlotPair struct {
	Slot uint64
	Val  uint64
}

type RentCollector struct {
	Epoch         uint64
	EpochSchedule sealevel.SysvarEpochSchedule
	SlotsPerYear  float64
	Rent          sealevel.SysvarRent
}

type VoteAccount struct {
	Lamports            uint64
	NodePubkey          solana.PublicKey
	BlsPubkeyCompressed *[48]byte
	LastTimestampTs     int64
	LastTimestampSlot   uint64
	Owner               solana.PublicKey
	Executable          byte
	RentEpoch           uint64
}

type VoteAccountsPair struct {
	Key   solana.PublicKey
	Stake uint64
	Value VoteAccount
}

type Stakes struct {
	VoteAccounts []VoteAccountsPair
	Delegations  []DelegationPair
	Unused       uint64
	Epoch        uint64
	StakeHistory sealevel.SysvarStakeHistory
}

type DelegationPair struct {
	Account    solana.PublicKey
	Delegation Delegation
}

type Delegation struct {
	VoterPubkey        solana.PublicKey
	Stake              uint64
	ActivationEpoch    uint64
	DeactivationEpoch  uint64
	WarmupCooldownRate float64
}

type Stake struct {
	VoteAccounts     []VoteAccountsPair
	StakeDelegations map[solana.PublicKey]StakePair
	Unused           uint64
	Epoch            uint64
	StakeHistory     sealevel.SysvarStakeHistory
}

type StakeInner struct {
	Delegation      Delegation
	CreditsObserved uint64
}

type StakePair struct {
	Account [32]byte
	Stake   StakeInner
}

type UnusedAccountsU64Pair struct {
	_0 solana.PublicKey
	_1 uint64
}

type UnusedAccounts struct {
	Unused1 []solana.PublicKey
	Unused2 []solana.PublicKey
	Unused3 []UnusedAccountsU64Pair
}

type PubkeyPair struct {
	Key solana.PublicKey
	Val solana.PublicKey
}

type NodeVoteAccounts struct {
	VoteAccounts []solana.PublicKey
	TotalStake   uint64
}

type NodeVoteAccountsPair struct {
	Key solana.PublicKey
	Val NodeVoteAccounts
}

type EpochStakes struct {
	Stakes                Stakes
	TotalStake            uint64
	NodeIdToVoteAccounts  []NodeVoteAccountsPair
	EpochAuthorizedVoters []PubkeyPair
}

type EpochStakesPair struct {
	Key uint64
	Val EpochStakes
}

type VersionedEpochStakes struct {
	Stakes                Stake
	TotalStake            uint64
	NodeIdToVoteAccounts  []NodeVoteAccountsPair
	EpochAuthorizedVoters []PubkeyPair
}

type VersionedEpochStakesPair struct {
	Epoch uint64
	Val   VersionedEpochStakes
}

type DeserializableVersionedBank struct {
	BlockhashQueue      BlockHashVec
	Ancestors           []SlotPair
	Hash                [32]byte
	ParentHash          [32]byte
	ParentSlot          uint64
	HardForks           []SlotPair
	TransactionCount    uint64
	TickHeight          uint64
	SignatureCount      uint64
	Capitalization      uint64
	MaxTickHeight       uint64
	HashesPerTick       *uint64 `bin:"optional"`
	TicksPerSlot        uint64
	NsPerSlot           bin.Uint128
	GenesisCreationTime uint64
	SlotsPerYear        float64
	AccountsDataLen     uint64
	Slot                uint64
	Epoch               uint64
	BlockHeight         uint64
	CollectorId         solana.PublicKey
	CollectorFees       uint64
	FeeCalculator       sealevel.FeeCalculator
	FeeRateGovernor     sealevel.FeeRateGovernor
	CollectedRent       uint64
	RentCollector       RentCollector
	EpochSchedule       sealevel.SysvarEpochSchedule
	Inflation           rewards.Inflation
	Stakes              Stakes
	UnusedAccounts      UnusedAccounts
	EpochStakes         []EpochStakesPair
	IsDelta             bool
}

type AcctVec struct {
	Id       uint64
	FileSize uint64
}

type SlotAcctVecs struct {
	Slot     uint64
	AcctVecs []AcctVec
}

type BankHashStats struct {
	NumUpdatedAccts    uint64
	NumRemovedAccts    uint64
	NumLamportsStored  uint64
	TotalDataLen       uint64
	NumExecutableAccts uint64
}

type BankHashInfo struct {
	Hash         [32]byte
	SnapshotHash [32]byte
	Stats        BankHashStats
}

type SlotMapPair struct {
	Slot uint64
	Hash [32]byte
}

type AccountsDbFields struct {
	//Storages                []SlotAcctVecs
	Storages                map[uint64]SlotAcctVecs
	Version                 uint64
	Slot                    uint64
	BankHashInfo            BankHashInfo
	HistoricalRoots         []uint64
	HistoricalRootsWithHash []SlotMapPair
}

type BankIncrementalSnapshotPersistence struct {
	FullSlot                  uint64
	FullHash                  [32]byte
	FullCapitalization        uint64
	IncrementalHash           [32]byte
	IncrementalCapitalization uint64
}

type RewardInfo struct {
	RewardType         uint32
	Lamports           uint64
	StakerRewards      uint64
	NewCreditsObserved uint64
	PostBalance        uint64
	Commission         uint64
}

type SerializableStakeRewards struct {
	StakePubkey solana.PublicKey
	RewardInfo  RewardInfo
}

type StartBlockHeightAndRewards struct {
	StartBlockHeight        uint64
	StakeRewardsByPartition map[solana.PublicKey]SerializableStakeRewards
}

type SerializableEpochRewardStatus struct {
	Type   uint32
	Active StartBlockHeightAndRewards
}

type SnapshotManifest struct {
	Bank                               *DeserializableVersionedBank
	AccountsDb                         *AccountsDbFields
	LamportsPerSignature               uint64
	BankIncrementalSnapshotPersistence *BankIncrementalSnapshotPersistence
	EpochAccountHash                   [32]byte
	VersionedEpochStakes               []VersionedEpochStakesPair
	LtHash                             *lthash.LtHash
	BlockID                            *solana.Hash
	rawDigest                          [32]byte
	hasRawDigest                       bool
}

// validateManifestCount rejects length prefixes which cannot possibly fit in
// the bytes that remain. Besides producing a useful truncation error early,
// this prevents a corrupt snapshot from turning a tiny manifest into an
// attacker-controlled allocation or a very long decode loop. minItemBytes is
// deliberately a lower bound; variable-sized records are validated again by
// their individual decoders.
func validateManifestCount(decoder *bin.Decoder, field string, count, minItemBytes uint64) error {
	if minItemBytes == 0 {
		return fmt.Errorf("%s: invalid zero minimum item size", field)
	}
	remaining := decoder.Remaining()
	if remaining < 0 || count > uint64(remaining)/minItemBytes {
		return fmt.Errorf(
			"%s count %d exceeds remaining manifest capacity (%d bytes, at least %d bytes per item)",
			field, count, remaining, minItemBytes,
		)
	}
	return nil
}

func manifestInitialCapacity(count uint64) int {
	return int(min(count, 4096))
}

func readManifestBool(decoder *bin.Decoder, field string) (bool, error) {
	encoded, err := decoder.ReadByte()
	if err != nil {
		return false, err
	}
	if encoded > 1 {
		return false, fmt.Errorf("%s has invalid boolean encoding %d", field, encoded)
	}
	return encoded == 1, nil
}

func (bhv *BlockHashVec) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	bhv.LastHashIndex, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	hasLastHash, err := readManifestBool(decoder, "blockhash queue last hash")
	if err != nil {
		return err
	}
	if hasLastHash {
		lastHash, err := decoder.ReadBytes(32)
		if err != nil {
			return err
		}
		var lh [32]byte
		copy(lh[:], lastHash)
		bhv.LastHash = &lh
	}

	var numAges uint64
	numAges, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "blockhash queue ages", numAges, 56); err != nil {
		return err
	}

	bhv.HashAndAge = make([]HashAgePair, 0, manifestInitialCapacity(numAges))
	for count := uint64(0); count < numAges; count++ {
		var age HashAgePair
		err = age.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		bhv.HashAndAge = append(bhv.HashAndAge, age)
	}

	bhv.MaxAge, err = decoder.ReadUint64(bin.LE)

	return err
}

func (age *HashAge) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	age.FeeCalculator.LamportsPerSignature, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	age.HashIndex, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	age.Timestamp, err = decoder.ReadUint64(bin.LE)
	return err
}

func (hashAgePair *HashAgePair) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	key, err := decoder.ReadBytes(32)
	if err != nil {
		return err
	}

	copy(hashAgePair.Key[:], key)

	err = hashAgePair.Val.UnmarshalWithDecoder(decoder)
	return err
}

func (slotPair *SlotPair) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	slotPair.Slot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	slotPair.Val, err = decoder.ReadUint64(bin.LE)
	return err
}

func (rentCollector *RentCollector) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	rentCollector.Epoch, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	err = rentCollector.EpochSchedule.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	rentCollector.SlotsPerYear, err = decoder.ReadFloat64(bin.LE)
	if err != nil {
		return err
	}

	err = rentCollector.Rent.UnmarshalWithDecoder(decoder)
	return err
}

func (voteAcct *VoteAccount) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	voteAcct.Lamports, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	var dataLen uint64
	dataLen, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	if dataLen > uint64(decoder.Remaining()) {
		return fmt.Errorf("vote account data length %d exceeds %d remaining bytes", dataLen, decoder.Remaining())
	}
	if dataLen > 0 {
		voteStateBytes, err := decoder.ReadBytes(int(dataLen))
		if err != nil {
			return err
		}
		var voteState sealevel.VoteStateVersions
		err = voteState.UnmarshalWithDecoder(bin.NewBinDecoder(voteStateBytes))
		if err != nil {
			return fmt.Errorf("decode vote account state: %w", err)
		}

		var voteTimestamp sealevel.BlockTimestamp

		switch voteState.Type {
		case sealevel.VoteStateVersionCurrent:
			{
				voteTimestamp = voteState.Current.LastTimestamp
				voteAcct.NodePubkey = voteState.Current.NodePubkey
			}

		case sealevel.VoteStateVersionV0_23_5:
			{
				voteTimestamp = voteState.V0_23_5.LastTimestamp
				voteAcct.NodePubkey = voteState.V0_23_5.NodePubkey
			}

		case sealevel.VoteStateVersionV1_14_11:
			{
				voteTimestamp = voteState.V1_14_11.LastTimestamp
				voteAcct.NodePubkey = voteState.V1_14_11.NodePubkey
			}

		case sealevel.VoteStateVersionV4:
			{
				voteTimestamp = voteState.V4.LastTimestamp
				voteAcct.NodePubkey = voteState.V4.NodePubkey
				if voteState.V4.BlsPubkeyCompressed != nil {
					blsPubkey := *voteState.V4.BlsPubkeyCompressed
					voteAcct.BlsPubkeyCompressed = &blsPubkey
				}
			}

		default:
			{
				return fmt.Errorf("unsupported vote state version %d", voteState.Type)
			}
		}

		voteAcct.LastTimestampTs = voteTimestamp.Timestamp
		voteAcct.LastTimestampSlot = voteTimestamp.Slot
	}

	var owner []byte
	owner, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	voteAcct.Owner = solana.PublicKeyFromBytes(owner)

	voteAcct.Executable, err = decoder.ReadByte()
	if err != nil {
		return err
	}
	if voteAcct.Executable > 1 {
		return fmt.Errorf("vote account executable has invalid boolean encoding %d", voteAcct.Executable)
	}

	voteAcct.RentEpoch, err = decoder.ReadUint64(bin.LE)
	return err
}

func (voteAcctsPair *VoteAccountsPair) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	var key []byte
	key, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	voteAcctsPair.Key = solana.PublicKeyFromBytes(key)

	voteAcctsPair.Stake, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	err = voteAcctsPair.Value.UnmarshalWithDecoder(decoder)
	return err
}

func (stakeInner *StakeInner) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	err := stakeInner.Delegation.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	stakeInner.CreditsObserved, err = decoder.ReadUint64(bin.LE)
	return err
}

func (stakes *Stakes) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	var numVoteAccts uint64
	numVoteAccts, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "stakes vote accounts", numVoteAccts, 97); err != nil {
		return err
	}

	stakes.VoteAccounts = make([]VoteAccountsPair, 0, manifestInitialCapacity(numVoteAccts))
	for count := uint64(0); count < numVoteAccts; count++ {
		var pair VoteAccountsPair
		err = pair.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		stakes.VoteAccounts = append(stakes.VoteAccounts, pair)
	}

	var numStakeDelegations uint64
	numStakeDelegations, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "stake delegations", numStakeDelegations, 96); err != nil {
		return err
	}

	stakes.Delegations = make([]DelegationPair, 0, manifestInitialCapacity(numStakeDelegations))
	for count := uint64(0); count < numStakeDelegations; count++ {
		var delegationPair DelegationPair
		err = delegationPair.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		stakes.Delegations = append(stakes.Delegations, delegationPair)
	}

	stakes.Unused, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	stakes.Epoch, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	err = stakes.StakeHistory.UnmarshalWithDecoder(decoder)
	return err
}

func (delegation *Delegation) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	var voterPubkey []byte
	voterPubkey, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	delegation.VoterPubkey = solana.PublicKeyFromBytes(voterPubkey)

	delegation.Stake, err = decoder.ReadUint64(bin.LE)
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

func (delegationPair *DelegationPair) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	var pk []byte
	pk, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	delegationPair.Account = solana.PublicKeyFromBytes(pk)

	err = delegationPair.Delegation.UnmarshalWithDecoder(decoder)
	return err
}

func (stakes *Stake) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	var numVoteAccts uint64
	numVoteAccts, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "versioned stakes vote accounts", numVoteAccts, 97); err != nil {
		return err
	}

	stakes.VoteAccounts = make([]VoteAccountsPair, 0, manifestInitialCapacity(numVoteAccts))
	for count := uint64(0); count < numVoteAccts; count++ {
		var pair VoteAccountsPair
		err = pair.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		stakes.VoteAccounts = append(stakes.VoteAccounts, pair)
	}

	var numStakeDelegations uint64
	numStakeDelegations, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "versioned stake delegations", numStakeDelegations, 104); err != nil {
		return err
	}

	stakes.StakeDelegations = make(map[solana.PublicKey]StakePair)
	for count := uint64(0); count < numStakeDelegations; count++ {
		var stakeDelegationPair StakePair
		err = stakeDelegationPair.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		if _, exists := stakes.StakeDelegations[stakeDelegationPair.Account]; exists {
			return fmt.Errorf("duplicate versioned stake delegation for %s", stakeDelegationPair.Account)
		}
		stakes.StakeDelegations[stakeDelegationPair.Account] = stakeDelegationPair
	}

	stakes.Unused, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	stakes.Epoch, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	err = stakes.StakeHistory.UnmarshalWithDecoder(decoder)
	return err
}

func (stakePair *StakePair) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	var pk []byte
	pk, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	stakePair.Account = solana.PublicKeyFromBytes(pk)

	err = stakePair.Stake.UnmarshalWithDecoder(decoder)
	return err
}

func (pair *UnusedAccountsU64Pair) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	var pkBytes []byte
	pkBytes, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	pair._0 = solana.PublicKeyFromBytes(pkBytes)
	pair._1, err = decoder.ReadUint64(bin.LE)
	return err
}

func (unusedAccts *UnusedAccounts) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	var numUnused1 uint64
	numUnused1, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "unused accounts set 1", numUnused1, 32); err != nil {
		return err
	}

	for count := uint64(0); count < numUnused1; count++ {
		_, err = decoder.ReadBytes(32)
		if err != nil {
			return err
		}

		/*pk := solana.PublicKeyFromBytes(unused1PkBytes)
		unusedAccts.Unused1 = append(unusedAccts.Unused1, pk)*/
	}

	var numUnused2 uint64
	numUnused2, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "unused accounts set 2", numUnused2, 32); err != nil {
		return err
	}

	for count := uint64(0); count < numUnused2; count++ {
		_, err = decoder.ReadBytes(32)
		if err != nil {
			return err
		}

		/*pk := solana.PublicKeyFromBytes(unused2PkBytes)
		unusedAccts.Unused2 = append(unusedAccts.Unused2, pk)*/
	}

	var numUnused3 uint64
	numUnused3, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "unused accounts set 3", numUnused3, 40); err != nil {
		return err
	}

	for count := uint64(0); count < numUnused3; count++ {
		var unused3 UnusedAccountsU64Pair
		err = unused3.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		//unusedAccts.Unused3 = append(unusedAccts.Unused3, unused3)
	}

	return nil
}

func (nodeVoteAccts *NodeVoteAccounts) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	var numVoteAccts uint64
	numVoteAccts, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "node vote accounts", numVoteAccts, 32); err != nil {
		return err
	}

	//nodeVoteAccts.VoteAccounts = make([]solana.PublicKey, 0, numVoteAccts)
	for count := uint64(0); count < numVoteAccts; count++ {
		//var pkBytes []byte
		/*pkBytes*/
		_, err = decoder.ReadBytes(32)
		if err != nil {
			return err
		}
		//pk := solana.PublicKeyFromBytes(pkBytes)
		//nodeVoteAccts.VoteAccounts = append(nodeVoteAccts.VoteAccounts, pk)
	}

	nodeVoteAccts.TotalStake, err = decoder.ReadUint64(bin.LE)
	return err
}

func (pair *NodeVoteAccountsPair) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	var pkBytes []byte
	pkBytes, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	pair.Key = solana.PublicKeyFromBytes(pkBytes)

	err = pair.Val.UnmarshalWithDecoder(decoder)
	return err
}

func (pubkeyPair *PubkeyPair) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	var pk []byte

	pk, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	pubkeyPair.Key = solana.PublicKeyFromBytes(pk)

	pk, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	pubkeyPair.Val = solana.PublicKeyFromBytes(pk)

	return nil
}

func (epochStakes *EpochStakes) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	err = epochStakes.Stakes.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	epochStakes.TotalStake, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	var numAccts uint64
	numAccts, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "epoch node vote accounts", numAccts, 48); err != nil {
		return err
	}

	//epochStakes.NodeIdToVoteAccounts = make([]NodeVoteAccountsPair, 0, numAccts)
	for count := uint64(0); count < numAccts; count++ {
		var pair NodeVoteAccountsPair
		err = pair.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		//epochStakes.NodeIdToVoteAccounts = append(epochStakes.NodeIdToVoteAccounts, pair)
	}

	var numEpochAuthVoters uint64
	numEpochAuthVoters, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "epoch authorized voters", numEpochAuthVoters, 64); err != nil {
		return err
	}

	//epochStakes.EpochAuthorizedVoters = make([]PubkeyPair, 0, numEpochAuthVoters)
	for count := uint64(0); count < numEpochAuthVoters; count++ {
		var pair PubkeyPair
		err = pair.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		//epochStakes.EpochAuthorizedVoters = append(epochStakes.EpochAuthorizedVoters, pair)
	}

	return nil
}

func (epochStakesPair *EpochStakesPair) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	epochStakesPair.Key, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	err = epochStakesPair.Val.UnmarshalWithDecoder(decoder)
	return err
}

func (versionedEpochStakes *VersionedEpochStakes) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	version, err := decoder.ReadUint32(bin.LE)
	if err != nil {
		return err
	}

	if version != 0 {
		return fmt.Errorf("unsupported versioned epoch stakes version %d", version)
	}

	err = versionedEpochStakes.Stakes.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	versionedEpochStakes.TotalStake, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	var numAccts uint64
	numAccts, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "versioned epoch node vote accounts", numAccts, 48); err != nil {
		return err
	}

	versionedEpochStakes.NodeIdToVoteAccounts = make([]NodeVoteAccountsPair, 0, manifestInitialCapacity(numAccts))
	for count := uint64(0); count < numAccts; count++ {
		var pair NodeVoteAccountsPair
		err = pair.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		versionedEpochStakes.NodeIdToVoteAccounts = append(versionedEpochStakes.NodeIdToVoteAccounts, pair)
	}

	var numEpochAuthVoters uint64
	numEpochAuthVoters, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "versioned epoch authorized voters", numEpochAuthVoters, 64); err != nil {
		return err
	}

	versionedEpochStakes.EpochAuthorizedVoters = make([]PubkeyPair, 0, manifestInitialCapacity(numEpochAuthVoters))
	for count := uint64(0); count < numEpochAuthVoters; count++ {
		var pair PubkeyPair
		err = pair.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		versionedEpochStakes.EpochAuthorizedVoters = append(versionedEpochStakes.EpochAuthorizedVoters, pair)
	}

	return err
}

func (epochStakesPair *VersionedEpochStakesPair) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	epochStakesPair.Epoch, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	err = epochStakesPair.Val.UnmarshalWithDecoder(decoder)
	return err
}

func (dsv *DeserializableVersionedBank) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	err = dsv.BlockhashQueue.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	var numAncestors uint64
	numAncestors, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "bank ancestors", numAncestors, 16); err != nil {
		return err
	}

	//dsv.Ancestors = make([]SlotPair, 0, numAncestors)
	for count := uint64(0); count < numAncestors; count++ {
		var ancestor SlotPair
		err = ancestor.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		//dsv.Ancestors = append(dsv.Ancestors, ancestor)
	}

	var hash []byte
	hash, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	copy(dsv.Hash[:], hash)

	hash, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	copy(dsv.ParentHash[:], hash)

	dsv.ParentSlot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	var numHardForks uint64
	numHardForks, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "bank hard forks", numHardForks, 16); err != nil {
		return err
	}

	//dsv.HardForks = make([]SlotPair, 0, numHardForks)
	for count := uint64(0); count < numHardForks; count++ {
		var hardFork SlotPair
		err = hardFork.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		//dsv.HardForks = append(dsv.HardForks, hardFork)
	}

	dsv.TransactionCount, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	dsv.TickHeight, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	dsv.SignatureCount, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	dsv.Capitalization, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	dsv.MaxTickHeight, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	var hasHashesPerTick bool
	hasHashesPerTick, err = readManifestBool(decoder, "bank hashes per tick")
	if err != nil {
		return err
	}

	if hasHashesPerTick {
		var hashesPerTick uint64
		hashesPerTick, err = decoder.ReadUint64(bin.LE)
		if err != nil {
			return err
		}
		dsv.HashesPerTick = &hashesPerTick
	}

	dsv.TicksPerSlot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	dsv.NsPerSlot, err = decoder.ReadUint128(bin.LE)
	if err != nil {
		return err
	}

	dsv.GenesisCreationTime, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	dsv.SlotsPerYear, err = decoder.ReadFloat64(bin.LE)
	if err != nil {
		return err
	}

	dsv.AccountsDataLen, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	dsv.Slot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	dsv.Epoch, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	dsv.BlockHeight, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	var collectorId []byte
	collectorId, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	dsv.CollectorId = solana.PublicKeyFromBytes(collectorId)

	dsv.CollectorFees, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	dsv.FeeCalculator.LamportsPerSignature, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	err = dsv.FeeRateGovernor.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	dsv.CollectedRent, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	err = dsv.RentCollector.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	err = dsv.EpochSchedule.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	err = dsv.Inflation.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	err = dsv.Stakes.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	err = dsv.UnusedAccounts.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	var numEpochStakes uint64
	numEpochStakes, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "bank epoch stakes", numEpochStakes, 8); err != nil {
		return err
	}

	//dsv.EpochStakes = make([]EpochStakesPair, 0, numEpochStakes)
	for count := uint64(0); count < numEpochStakes; count++ {
		var epochStakesPair EpochStakesPair
		err = epochStakesPair.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		//dsv.EpochStakes = append(dsv.EpochStakes, epochStakesPair)
	}

	dsv.IsDelta, err = readManifestBool(decoder, "bank delta marker")

	return err
}

func (acctVec *AcctVec) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	acctVec.Id, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	acctVec.FileSize, err = decoder.ReadUint64(bin.LE)
	return err
}

func (slotAcctVecs *SlotAcctVecs) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	slotAcctVecs.Slot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	var numAcctVecs uint64
	numAcctVecs, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "slot account vectors", numAcctVecs, 16); err != nil {
		return err
	}

	slotAcctVecs.AcctVecs = make([]AcctVec, 0, manifestInitialCapacity(numAcctVecs))
	seenFileIDs := make(map[uint64]struct{}, manifestInitialCapacity(numAcctVecs))
	for count := uint64(0); count < numAcctVecs; count++ {
		var acctVec AcctVec
		err = acctVec.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		if _, exists := seenFileIDs[acctVec.Id]; exists {
			return fmt.Errorf("duplicate appendvec file ID %d in storage slot %d", acctVec.Id, slotAcctVecs.Slot)
		}
		seenFileIDs[acctVec.Id] = struct{}{}
		slotAcctVecs.AcctVecs = append(slotAcctVecs.AcctVecs, acctVec)
	}

	return nil
}

func (stats *BankHashStats) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	stats.NumUpdatedAccts, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	stats.NumRemovedAccts, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	stats.NumLamportsStored, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	stats.TotalDataLen, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	stats.NumExecutableAccts, err = decoder.ReadUint64(bin.LE)
	return err
}

func (info *BankHashInfo) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	var hash []byte
	hash, err = decoder.ReadBytes(32)
	if err != nil {
		return fmt.Errorf("bank hash: %w", err)
	}
	copy(info.Hash[:], hash)

	hash, err = decoder.ReadBytes(32)
	if err != nil {
		return fmt.Errorf("snapshot hash: %w", err)
	}
	copy(info.SnapshotHash[:], hash)

	err = info.Stats.UnmarshalWithDecoder(decoder)
	if err != nil {
		return fmt.Errorf("bank hash stats: %w", err)
	}

	return nil
}

func (pair *SlotMapPair) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	pair.Slot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	var hash []byte
	hash, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	copy(pair.Hash[:], hash)

	return nil
}

func (bankIncrSnapshotPersistence *BankIncrementalSnapshotPersistence) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	bankIncrSnapshotPersistence.FullSlot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("FullSlot: %s", err)
	}

	var pkBytes []byte
	pkBytes, err = decoder.ReadBytes(32)
	if err != nil {
		return fmt.Errorf("FullHash: %s", err)
	}
	bankIncrSnapshotPersistence.FullHash = solana.PublicKeyFromBytes(pkBytes)

	bankIncrSnapshotPersistence.FullCapitalization, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("FullCapitalization: %s", err)
	}

	pkBytes, err = decoder.ReadBytes(32)
	if err != nil {
		return fmt.Errorf("IncrementalHash: %s", err)
	}
	bankIncrSnapshotPersistence.IncrementalHash = solana.PublicKeyFromBytes(pkBytes)

	bankIncrSnapshotPersistence.IncrementalCapitalization, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("IncrementalCapitalization: %s", err)
	}

	return nil
}

func (acctDbFields *AccountsDbFields) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	var numStorages uint64
	numStorages, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		util.VerboseHandleError(err)
		return err
	}
	if err := validateManifestCount(decoder, "AccountsDB storages", numStorages, 16); err != nil {
		return err
	}

	acctDbFields.Storages = make(map[uint64]SlotAcctVecs)

	for count := uint64(0); count < numStorages; count++ {
		var slotAcctVecs SlotAcctVecs
		err = slotAcctVecs.UnmarshalWithDecoder(decoder)
		if err != nil {
			util.VerboseHandleError(err)
			return err
		}
		if _, exists := acctDbFields.Storages[slotAcctVecs.Slot]; exists {
			return fmt.Errorf("duplicate AccountsDB storage slot %d", slotAcctVecs.Slot)
		}
		acctDbFields.Storages[slotAcctVecs.Slot] = slotAcctVecs
	}

	acctDbFields.Version, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		util.VerboseHandleError(err)
		return err
	}

	acctDbFields.Slot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	err = acctDbFields.BankHashInfo.UnmarshalWithDecoder(decoder)
	if err != nil {
		return err
	}

	var numHistoricalRoots uint64
	numHistoricalRoots, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		util.VerboseHandleError(err)
		return err
	}
	if err := validateManifestCount(decoder, "AccountsDB historical roots", numHistoricalRoots, 8); err != nil {
		return err
	}

	//acctDbFields.HistoricalRoots = make([]uint64, 0, numHistoricalRoots)
	for count := uint64(0); count < numHistoricalRoots; count++ {
		//var historicalRoot uint64
		/*historicalRoot*/
		_, err = decoder.ReadUint64(bin.LE)
		if err != nil {
			util.VerboseHandleError(err)
			return err
		}
		//acctDbFields.HistoricalRoots = append(acctDbFields.HistoricalRoots, historicalRoot)
	}

	var numHistoricalRootsWithHash uint64
	numHistoricalRootsWithHash, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		util.VerboseHandleError(err)
		return err
	}
	if err := validateManifestCount(decoder, "AccountsDB historical roots with hash", numHistoricalRootsWithHash, 40); err != nil {
		return err
	}

	//acctDbFields.HistoricalRootsWithHash = make([]SlotMapPair, 0)
	for count := uint64(0); count < numHistoricalRootsWithHash; count++ {
		var pair SlotMapPair
		err = pair.UnmarshalWithDecoder(decoder)
		if err != nil {
			util.VerboseHandleError(err)
			return err
		}
		//acctDbFields.HistoricalRootsWithHash = append(acctDbFields.HistoricalRootsWithHash, pair)
	}

	return nil
}

func (epochRewardStatus *SerializableEpochRewardStatus) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	epochRewardStatus.Type, err = decoder.ReadUint32(bin.LE)
	if err != nil {
		return err
	}

	switch epochRewardStatus.Type {
	case 0:
		{
			err = epochRewardStatus.Active.UnmarshalWithDecoder(decoder)
		}

	case 1:
		{
			break
		}

	default:
		{
			return fmt.Errorf("invalid epoch reward status type %d", epochRewardStatus.Type)
		}
	}

	return err
}

func (startBlockHeightAndRewards *StartBlockHeightAndRewards) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	startBlockHeightAndRewards.StartBlockHeight, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	var numStakeRewardsByPartition uint64
	numStakeRewardsByPartition, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "stake rewards by partition", numStakeRewardsByPartition, 76); err != nil {
		return err
	}

	startBlockHeightAndRewards.StakeRewardsByPartition = make(map[solana.PublicKey]SerializableStakeRewards)

	//startBlockHeightAndRewards.StakeRewardsByPartition = make([]SerializableStakeRewards, 0, numStakeRewardsByPartition)
	for count := uint64(0); count < numStakeRewardsByPartition; count++ {
		var stakeRewards SerializableStakeRewards
		err = stakeRewards.UnmarshalWithDecoder(decoder)
		if err != nil {
			return err
		}
		if _, exists := startBlockHeightAndRewards.StakeRewardsByPartition[stakeRewards.StakePubkey]; exists {
			return fmt.Errorf("duplicate stake reward for %s", stakeRewards.StakePubkey)
		}
		startBlockHeightAndRewards.StakeRewardsByPartition[stakeRewards.StakePubkey] = stakeRewards
	}

	return nil
}

func (rewardInfo *RewardInfo) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	rewardInfo.RewardType, err = decoder.ReadUint32(bin.LE)
	if err != nil {
		return err
	}

	rewardInfo.Lamports, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	rewardInfo.StakerRewards, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	rewardInfo.NewCreditsObserved, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	rewardInfo.PostBalance, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	rewardInfo.Commission, err = decoder.ReadUint64(bin.LE)

	return err
}

func (stakeRewards *SerializableStakeRewards) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	var pkBytes []byte
	pkBytes, err = decoder.ReadBytes(32)
	if err != nil {
		return err
	}
	stakeRewards.StakePubkey = solana.PublicKeyFromBytes(pkBytes)

	err = stakeRewards.RewardInfo.UnmarshalWithDecoder(decoder)

	return err
}

func (snapshot *SnapshotManifest) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	snapshot.Bank = new(DeserializableVersionedBank)
	err = snapshot.Bank.UnmarshalWithDecoder(decoder)
	if err != nil {
		util.VerboseHandleError(err)
		return err
	}

	snapshot.AccountsDb = new(AccountsDbFields)
	err = snapshot.AccountsDb.UnmarshalWithDecoder(decoder)
	if err != nil {
		util.VerboseHandleError(err)
		return err
	}

	snapshot.LamportsPerSignature, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	return snapshot.unmarshalOptionalFields(decoder)
}

func (snapshot *SnapshotManifest) unmarshalOptionalFields(decoder *bin.Decoder) error {
	var err error

	if !decoder.HasRemaining() {
		return nil
	}

	var hasIncrementalSnapshotPersistence bool
	hasIncrementalSnapshotPersistence, err = readManifestBool(decoder, "incremental snapshot persistence marker")
	if err != nil {
		return err
	}

	if hasIncrementalSnapshotPersistence {
		mlog.Log.Infof("hasIncrementalSnapshotPersistence")
		snapshot.BankIncrementalSnapshotPersistence = new(BankIncrementalSnapshotPersistence)
		err = snapshot.BankIncrementalSnapshotPersistence.UnmarshalWithDecoder(decoder)
		if err != nil {
			return fmt.Errorf("incremental snapshot persistence: %w", err)
		}
	}

	if !decoder.HasRemaining() {
		return nil
	}

	var hashEpochAcctHash bool
	hashEpochAcctHash, err = readManifestBool(decoder, "epoch account hash marker")
	if err != nil {
		return err
	}

	if hashEpochAcctHash {
		var pkBytes []byte
		pkBytes, err = decoder.ReadBytes(32)
		if err != nil {
			return err
		}
		snapshot.EpochAccountHash = solana.PublicKeyFromBytes(pkBytes)
	}

	if !decoder.HasRemaining() {
		return nil
	}

	numVersionedEpochStakes, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if err := validateManifestCount(decoder, "versioned epoch stakes", numVersionedEpochStakes, 12); err != nil {
		return err
	}

	if numVersionedEpochStakes != 0 {
		// The 12-byte structural lower bound is intentionally permissive, but
		// using that attacker-controlled maximum as the exact capacity can turn
		// a 256 MiB manifest into a multi-gigabyte allocation before the first
		// nested item fails. Grow in proportion to successfully decoded items.
		initialCapacity := int(min(numVersionedEpochStakes, 1024))
		snapshot.VersionedEpochStakes = make([]VersionedEpochStakesPair, 0, initialCapacity)
		for range numVersionedEpochStakes {
			var pair VersionedEpochStakesPair
			err = pair.UnmarshalWithDecoder(decoder)
			if err != nil {
				return err
			}
			snapshot.VersionedEpochStakes = append(snapshot.VersionedEpochStakes, pair)
		}
	}

	if !decoder.HasRemaining() {
		return nil
	}

	var hasLtHash bool
	hasLtHash, err = readManifestBool(decoder, "accounts LtHash marker")
	if err != nil {
		return err
	}

	if hasLtHash {
		ltHashBytes, err := decoder.ReadBytes(2048)
		if err != nil {
			return err
		}
		snapshot.LtHash = new(lthash.LtHash).InitWithHash(ltHashBytes)
	}

	// Older 1.2.0 snapshots end after AccountsLtHash. Current Agave appends an
	// Option<Hash> carrying the Alpenglow block identity without changing the
	// outer snapshot version, so EOF here is the only unambiguous legacy form.
	if !decoder.HasRemaining() {
		return nil
	}
	var hasBlockID bool
	hasBlockID, err = readManifestBool(decoder, "Alpenglow block ID marker")
	if err != nil {
		return err
	}
	if hasBlockID {
		blockIDBytes, err := decoder.ReadBytes(32)
		if err != nil {
			return fmt.Errorf("Alpenglow block ID: %w", err)
		}
		blockID := solana.HashFromBytes(blockIDBytes)
		snapshot.BlockID = &blockID
	}
	if decoder.HasRemaining() {
		return fmt.Errorf("snapshot manifest has %d unsupported trailing bytes", decoder.Remaining())
	}

	return nil
}
