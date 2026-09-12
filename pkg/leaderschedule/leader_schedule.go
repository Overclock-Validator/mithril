package leaderschedule

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"slices"
	"sort"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/nixberg/chacha-rng-go"
)

type slotLeader struct {
	nodePubkey     solana.PublicKey
	voteAccount    solana.PublicKey
	hasVoteAccount bool
}

type LeaderSchedule struct {
	lsMap map[uint64]slotLeader
}

func NewLeaderScheduleFromKeyedSlots(ls map[solana.PublicKey][]uint64, epochStartSlot uint64) *LeaderSchedule {
	lsMap := make(map[uint64]slotLeader)

	for pubkey, epochIndices := range ls {
		for _, idx := range epochIndices {
			slot, err := safemath.CheckedAddU64(idx, epochStartSlot)
			if err != nil {
				panic(fmt.Sprintf("overflow for %s, idx %d, epochStartSlot = %d", pubkey, idx, epochStartSlot))
			}
			existingLeader, exists := lsMap[slot]
			if exists {
				panic(fmt.Sprintf("error adding %s as leader for slot %d - there's already an entry for %s", pubkey, slot, existingLeader.nodePubkey))
			}
			lsMap[slot] = slotLeader{nodePubkey: pubkey}
		}
	}

	return &LeaderSchedule{lsMap: lsMap}
}

type pubkeyAndStakePair struct {
	pubkey solana.PublicKey
	stake  uint64
}

// voteAcctAndNodePair holds vote account info for vote-keyed scheduling
type voteAcctAndNodePair struct {
	voteAcct   solana.PublicKey
	nodePubkey solana.PublicKey
	stake      uint64
}

func New(
	epochVoteAcctsMap map[solana.PublicKey]*epochstakes.VoteAccount,
	epochVoteAcctStakes map[solana.PublicKey]uint64,
	epochSchedule *sealevel.SysvarEpochSchedule,
	epoch uint64,
	length uint64,
	repeat uint64) *LeaderSchedule {

	// VOTE-KEYED SCHEDULE (matches Agave's VoteKeyedLeaderSchedule):
	// 1. Sort by VOTE ACCOUNT pubkey (stake desc, vote pubkey desc)
	// 2. Sample weighted by stake → returns vote account pubkeys
	// 3. Convert each selected vote account → node identity
	//
	// This differs from identity-keyed which would aggregate and sort by node identity.
	// The tie-break is on VOTE ACCOUNT pubkey, not node identity!

	// Build vote account entries with their node mappings
	var voteAccts []voteAcctAndNodePair
	for voteAcctPubkey, stake := range epochVoteAcctStakes {
		if stake == 0 {
			continue
		}
		voteAcct := epochVoteAcctsMap[voteAcctPubkey]
		if voteAcct == nil {
			continue
		}
		nodePubkey := voteAcct.NodePubkey
		var zeroPk solana.PublicKey
		if nodePubkey == zeroPk {
			continue
		}
		voteAccts = append(voteAccts, voteAcctAndNodePair{
			voteAcct:   voteAcctPubkey,
			nodePubkey: nodePubkey,
			stake:      stake,
		})
	}

	// Build keyed stakes using VOTE ACCOUNT pubkeys (not node identities)
	keyedStakes := make([]pubkeyAndStakePair, 0, len(voteAccts))
	for _, va := range voteAccts {
		keyedStakes = append(keyedStakes, pubkeyAndStakePair{pubkey: va.voteAcct, stake: va.stake})
	}

	// Sample vote accounts (sorting happens inside stakeWeightedSlotLeaders)
	voteAccountLeaders := stakeWeightedSlotLeaders(keyedStakes, epoch, length, repeat)

	// Build vote account → node identity lookup
	voteToNode := make(map[solana.PublicKey]solana.PublicKey, len(voteAccts))
	for _, va := range voteAccts {
		voteToNode[va.voteAcct] = va.nodePubkey
	}

	// Preserve both sides of the vote-keyed selection. The node identity remains
	// the public leader used for shred verification, while the selected vote
	// account is needed by Alpenglow to credit the leader reward deterministically
	// when multiple vote accounts share one node identity.
	leaderScheduleMap := make(map[uint64]slotLeader, len(voteAccountLeaders))
	firstSlotInEpoch := epochSchedule.FirstSlotInEpoch(epoch)
	for i, voteAcctPk := range voteAccountLeaders {
		slotNum := uint64(i) + firstSlotInEpoch
		leaderScheduleMap[slotNum] = slotLeader{
			nodePubkey:     voteToNode[voteAcctPk],
			voteAccount:    voteAcctPk,
			hasVoteAccount: true,
		}
	}

	return &LeaderSchedule{
		lsMap: leaderScheduleMap,
	}
}

func stakeWeightedSlotLeaders(keyedStakes []pubkeyAndStakePair,
	epoch uint64,
	length uint64,
	repeat uint64) []solana.PublicKey {
	if repeat == 0 {
		panic("stakeWeightedSlotLeaders: repeat cannot be 0")
	}

	keyedStakes = sortStakes(keyedStakes)

	// Build cumulative weights, preserving input order (stake desc, pubkey desc)
	// This matches Agave's WeightedU64Index which does NOT re-sort
	cumulative := make([]uint64, len(keyedStakes))
	var total uint64
	for i, pair := range keyedStakes {
		newTotal, err := safemath.CheckedAddU64(total, pair.stake)
		if err != nil {
			panic(fmt.Sprintf("stakeWeightedSlotLeaders: cumulative stake overflow at index %d", i))
		}
		total = newTotal
		cumulative[i] = total
	}

	if total == 0 {
		panic("stakeWeightedSlotLeaders: total stake is zero")
	}

	// Create ChaCha20 RNG with epoch as seed, matching Agave's seeding:
	// let mut seed = [0u8; 32];
	// seed[0..8].copy_from_slice(&epoch.to_le_bytes());
	// let rng = &mut ChaChaRng::from_seed(seed);
	//
	// The nixberg/chacha-rng-go library takes [8]uint32 which is the same 32 bytes
	// interpreted as little-endian uint32s.
	var seedBytes [32]byte
	binary.LittleEndian.PutUint64(seedBytes[:], epoch)
	var seed [8]uint32
	for i := 0; i < 8; i++ {
		seed[i] = binary.LittleEndian.Uint32(seedBytes[i*4:])
	}
	rng := chacha.Seeded20(seed, 0) // stream=0 matches default

	leaders := make([]solana.PublicKey, 0, length)
	var currentSlotLeader solana.PublicKey

	for i := range length {
		if i%repeat == 0 {
			// Generate random in [0, total) using rejection sampling
			// This matches Rust's gen_range behavior
			r := uint64n(rng, total)
			idx := sort.Search(len(cumulative), func(j int) bool {
				return cumulative[j] > r
			})
			currentSlotLeader = keyedStakes[idx].pubkey
		}
		leaders = append(leaders, currentSlotLeader)
	}

	return leaders
}

// uint64n generates a uniform random uint64 in [0,n) matching Agave's UniformU64Sampler.
// This matches agave_random::weighted::UniformU64Sampler::new_like_instance_sample
// which is used for leader schedule computation.
//
// The algorithm uses wide multiplication (128-bit product) with rejection sampling:
// - zone = u64::MAX - ((u64::MAX - n + 1) % n)
// - Accept when lo <= zone, reject when lo > zone
func uint64n(rng *chacha.ChaCha, n uint64) uint64 {
	if n == 0 {
		panic("uint64n: n cannot be 0")
	}

	// Calculate zone following Agave's new_like_instance_sample:
	// ints_to_reject = (u64::MAX - range_end + 1) % range_end
	// zone = u64::MAX - ints_to_reject
	intsToReject := (^uint64(0) - n + 1) % n
	zone := ^uint64(0) - intsToReject

	for {
		x := rng.Uint64()
		// Compute 128-bit product: m = x * n
		// mHi = high 64 bits (result), mLo = low 64 bits (for rejection test)
		mHi, mLo := bits.Mul64(x, n)

		// Accept if lo <= zone (Agave's acceptance condition)
		if mLo <= zone {
			return mHi
		}
		// Reject and retry
	}
}

func sortStakes(stakes []pubkeyAndStakePair) []pubkeyAndStakePair {
	slices.SortFunc(stakes, func(l, r pubkeyAndStakePair) int {
		if r.stake != l.stake {
			// Sort by stake descending (matches Agave's r_stake.cmp(l_stake))
			if r.stake > l.stake {
				return 1
			}
			return -1
		}
		// Tiebreak by pubkey descending (matches Agave's r_pubkey.cmp(l_pubkey))
		return bytes.Compare(r.pubkey[:], l.pubkey[:])
	})
	return slices.Compact(stakes)
}

// TieBreakEntry represents an entry in a tie-break group for debugging
type TieBreakEntry struct {
	Rank     int
	NodePk   solana.PublicKey
	Stake    uint64
	RawBytes []byte // First 8 bytes of pubkey for comparison
	BytesCmp int    // Comparison result vs previous entry
}

// GetSortedStakesDebug returns sorted stakes with tie-break debugging info.
// Used to verify tie-break ordering matches Agave's vote-keyed schedule.
// NOTE: This now uses VOTE ACCOUNT pubkeys for sorting (not node identities)
// to match the actual leader schedule computation.
func GetSortedStakesDebug(
	epochVoteAcctsMap map[solana.PublicKey]*epochstakes.VoteAccount,
	epochVoteAcctStakes map[solana.PublicKey]uint64,
) ([]TieBreakEntry, map[uint64][]TieBreakEntry) {
	// Build vote account entries (matching New())
	type voteEntry struct {
		voteAcct   solana.PublicKey
		nodePubkey solana.PublicKey
		stake      uint64
	}
	var voteEntries []voteEntry
	for voteAcctPubkey, stake := range epochVoteAcctStakes {
		if stake == 0 {
			continue
		}
		voteAcct := epochVoteAcctsMap[voteAcctPubkey]
		if voteAcct == nil {
			continue
		}
		nodePubkey := voteAcct.NodePubkey
		var zeroPk solana.PublicKey
		if nodePubkey == zeroPk {
			continue
		}
		voteEntries = append(voteEntries, voteEntry{
			voteAcct:   voteAcctPubkey,
			nodePubkey: nodePubkey,
			stake:      stake,
		})
	}

	// Sort by VOTE ACCOUNT pubkey (stake desc, vote pubkey desc) - matches New()
	keyedStakes := make([]pubkeyAndStakePair, 0, len(voteEntries))
	for _, ve := range voteEntries {
		keyedStakes = append(keyedStakes, pubkeyAndStakePair{pubkey: ve.voteAcct, stake: ve.stake})
	}
	keyedStakes = sortStakes(keyedStakes)

	// Build lookup for vote → node
	voteToNode := make(map[solana.PublicKey]solana.PublicKey, len(voteEntries))
	for _, ve := range voteEntries {
		voteToNode[ve.voteAcct] = ve.nodePubkey
	}

	// Build debug entries showing the actual schedule order
	entries := make([]TieBreakEntry, len(keyedStakes))
	for i, pair := range keyedStakes {
		entry := TieBreakEntry{
			Rank:     i + 1,
			NodePk:   voteToNode[pair.pubkey], // Show node identity (the actual leader)
			Stake:    pair.stake,
			RawBytes: pair.pubkey[:8], // Show vote account bytes (the sort key)
		}
		if i > 0 {
			// Compare VOTE ACCOUNT pubkeys (the actual sort key)
			entry.BytesCmp = bytes.Compare(pair.pubkey[:], keyedStakes[i-1].pubkey[:])
		}
		entries[i] = entry
	}

	// Group ties by stake
	tieGroups := make(map[uint64][]TieBreakEntry)
	for i := 0; i < len(entries); i++ {
		stake := entries[i].Stake
		// Find all entries with same stake
		j := i
		for j < len(entries) && entries[j].Stake == stake {
			j++
		}
		if j-i > 1 { // More than one entry = tie
			tieGroups[stake] = entries[i:j]
		}
		i = j - 1
	}

	return entries, tieGroups
}

func (ls *LeaderSchedule) LeaderForSlot(slot uint64) (solana.PublicKey, bool) {
	leader, exists := ls.lsMap[slot]
	if !exists {
		return solana.PublicKey{}, false
	}
	return leader.nodePubkey, true
}

// NextSlotForLeader returns the earliest scheduled slot at or after from
// whose node identity is identity.
func (ls *LeaderSchedule) NextSlotForLeader(identity solana.PublicKey, from uint64) (uint64, bool) {
	if ls == nil {
		return 0, false
	}
	var best uint64
	found := false
	for slot, leader := range ls.lsMap {
		if leader.nodePubkey != identity || slot < from {
			continue
		}
		if !found || slot < best {
			best = slot
			found = true
		}
	}
	return best, found
}

// LeaderForSlotWithVoteAccount returns the coherent node and vote-account pair
// selected by the vote-keyed schedule. Schedules constructed from node-keyed
// RPC data do not have the vote account and return false.
func (ls *LeaderSchedule) LeaderForSlotWithVoteAccount(slot uint64) (solana.PublicKey, solana.PublicKey, bool) {
	leader, exists := ls.lsMap[slot]
	if !exists || !leader.hasVoteAccount {
		return solana.PublicKey{}, solana.PublicKey{}, false
	}
	return leader.nodePubkey, leader.voteAccount, true
}
