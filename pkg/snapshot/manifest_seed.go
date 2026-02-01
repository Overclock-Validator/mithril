package snapshot

import (
	"encoding/base64"
	"encoding/json"
	"sort"

	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/state"
)

// PopulateManifestSeed copies manifest data to state file for replay context.
// Called ONCE after AccountsDB build completes, before writing state file.
// This eliminates the need to read the manifest at runtime.
func PopulateManifestSeed(s *state.MithrilState, m *SnapshotManifest) {
	// Block config
	s.ManifestParentSlot = m.Bank.Slot
	s.ManifestParentBankhash = base58.Encode(m.Bank.Hash[:])

	// LtHash: use Hash() method, encode as base64
	if m.LtHash != nil {
		s.ManifestAcctsLtHash = base64.StdEncoding.EncodeToString(m.LtHash.Hash())
	}

	// Fee rate governor (static fields only)
	s.ManifestFeeRateGovernor = &state.ManifestFeeRateGovernorSeed{
		TargetLamportsPerSignature: m.Bank.FeeRateGovernor.TargetLamportsPerSignature,
		TargetSignaturesPerSlot:    m.Bank.FeeRateGovernor.TargetSignaturesPerSlot,
		MinLamportsPerSignature:    m.Bank.FeeRateGovernor.MinLamportsPerSignature,
		MaxLamportsPerSignature:    m.Bank.FeeRateGovernor.MaxLamportsPerSignature,
		BurnPercent:                m.Bank.FeeRateGovernor.BurnPercent,
	}

	// Signature/fee state
	s.ManifestSignatureCount = m.Bank.SignatureCount
	s.ManifestLamportsPerSignature = m.LamportsPerSignature

	// Blockhash context (sort by hash_index descending)
	ages := make([]HashAgePair, len(m.Bank.BlockhashQueue.HashAndAge))
	copy(ages, m.Bank.BlockhashQueue.HashAndAge)
	sort.Slice(ages, func(i, j int) bool {
		return ages[i].Val.HashIndex > ages[j].Val.HashIndex
	})

	// Store top 150 blockhashes
	numBlockhashes := min(150, len(ages))
	s.ManifestRecentBlockhashes = make([]state.BlockhashEntry, numBlockhashes)
	for i := 0; i < numBlockhashes; i++ {
		s.ManifestRecentBlockhashes[i] = state.BlockhashEntry{
			Blockhash:            base58.Encode(ages[i].Key[:]),
			LamportsPerSignature: ages[i].Val.FeeCalculator.LamportsPerSignature,
		}
	}

	// Guard: only access ages[150] if we have at least 151 entries
	if len(ages) > 150 {
		s.ManifestEvictedBlockhash = base58.Encode(ages[150].Key[:])
	}

	// ReplayCtx seed
	s.ManifestCapitalization = m.Bank.Capitalization
	s.ManifestSlotsPerYear = m.Bank.SlotsPerYear
	s.ManifestInflationInitial = m.Bank.Inflation.Initial
	s.ManifestInflationTerminal = m.Bank.Inflation.Terminal
	s.ManifestInflationTaper = m.Bank.Inflation.Taper
	s.ManifestInflationFoundation = m.Bank.Inflation.FoundationVal
	s.ManifestInflationFoundationTerm = m.Bank.Inflation.FoundationTerm

	// Epoch account hash (base64 for consistency with LtHash)
	if m.EpochAccountHash != [32]byte{} {
		s.ManifestEpochAcctsHash = base64.StdEncoding.EncodeToString(m.EpochAccountHash[:])
	}

	// Transaction count at snapshot
	s.ManifestTransactionCount = m.Bank.TransactionCount

	// Epoch authorized voters (for snapshot epoch only)
	// Supports multiple authorized voters per vote account (matches original manifest behavior)
	s.ManifestEpochAuthorizedVoters = make(map[string][]string)
	for _, epochStake := range m.VersionedEpochStakes {
		if epochStake.Epoch == m.Bank.Epoch {
			for _, entry := range epochStake.Val.EpochAuthorizedVoters {
				voteAcctStr := base58.Encode(entry.Key[:])
				authorizedVoterStr := base58.Encode(entry.Val[:])
				s.ManifestEpochAuthorizedVoters[voteAcctStr] = append(s.ManifestEpochAuthorizedVoters[voteAcctStr], authorizedVoterStr)
			}
		}
	}

	// Epoch stakes: convert VersionedEpochStakes to PersistedEpochStakes format
	// This stores ONLY vote-account aggregates, NOT full stake account data
	s.ManifestEpochStakes = convertVersionedEpochStakesToPersisted(m.VersionedEpochStakes)
}

// convertVersionedEpochStakesToPersisted converts manifest epoch stakes to
// the same PersistedEpochStakes JSON format used by ComputedEpochStakes.
// Only stores vote-account stakes (aggregated), NOT full stake account data.
func convertVersionedEpochStakesToPersisted(stakes []VersionedEpochStakesPair) map[uint64]string {
	result := make(map[uint64]string, len(stakes))

	for _, epochStake := range stakes {
		// Build PersistedEpochStakes from manifest data
		persisted := epochstakes.PersistedEpochStakes{
			Epoch:      epochStake.Epoch,
			TotalStake: epochStake.Val.TotalStake,
			Stakes:     make(map[string]uint64),
			VoteAccts:  make(map[string]*epochstakes.VoteAccountJSON),
		}

		// Extract vote accounts from Stakes.VoteAccounts (aggregated data)
		for _, va := range epochStake.Val.Stakes.VoteAccounts {
			pkStr := base58.Encode(va.Key[:])
			persisted.Stakes[pkStr] = va.Stake
			persisted.VoteAccts[pkStr] = &epochstakes.VoteAccountJSON{
				Lamports:          va.Value.Lamports,
				NodePubkey:        base58.Encode(va.Value.NodePubkey[:]),
				LastTimestampTs:   va.Value.LastTimestampTs,
				LastTimestampSlot: va.Value.LastTimestampSlot,
				Owner:             base58.Encode(va.Value.Owner[:]),
				Executable:        va.Value.Executable,
				RentEpoch:         va.Value.RentEpoch,
			}
		}

		data, err := json.Marshal(persisted)
		if err != nil {
			continue
		}
		result[epochStake.Epoch] = string(data)
	}
	return result
}
