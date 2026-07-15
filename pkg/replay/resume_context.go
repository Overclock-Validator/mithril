package replay

import (
	"encoding/base64"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	bin "github.com/gagliardetto/binary"
	"github.com/mr-tron/base58"
)

// EncodeRecentBlockhashes detaches the sysvar representation for persisted
// resume state.
func EncodeRecentBlockhashes(entries *sealevel.SysvarRecentBlockhashes) []state.BlockhashEntry {
	if entries == nil {
		return nil
	}
	out := make([]state.BlockhashEntry, 0, len(*entries))
	for _, entry := range *entries {
		out = append(out, state.BlockhashEntry{
			Blockhash:            base58.Encode(entry.Blockhash[:]),
			LamportsPerSignature: entry.FeeCalculator.LamportsPerSignature,
		})
	}
	return out
}

func DecodeRecentBlockhashes(entries []state.BlockhashEntry) sealevel.SysvarRecentBlockhashes {
	out := make(sealevel.SysvarRecentBlockhashes, 0, len(entries))
	for _, entry := range entries {
		decoded, err := base58.Decode(entry.Blockhash)
		if err != nil || len(decoded) != 32 {
			continue
		}
		var hash [32]byte
		copy(hash[:], decoded)
		out = append(out, sealevel.RecentBlockHashesEntry{
			Blockhash:     hash,
			FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: entry.LamportsPerSignature},
		})
	}
	return out
}

func EncodeSlotHashes(entries *sealevel.SysvarSlotHashes) []state.SlotHashEntry {
	if entries == nil {
		return nil
	}
	out := make([]state.SlotHashEntry, 0, len(*entries))
	for _, entry := range *entries {
		out = append(out, state.SlotHashEntry{Slot: entry.Slot, Hash: base58.Encode(entry.Hash[:])})
	}
	return out
}

func DecodeSlotHashes(entries []state.SlotHashEntry) sealevel.SysvarSlotHashes {
	out := make(sealevel.SysvarSlotHashes, 0, len(entries))
	for _, entry := range entries {
		decoded, err := base58.Decode(entry.Hash)
		if err != nil || len(decoded) != 32 {
			continue
		}
		var hash [32]byte
		copy(hash[:], decoded)
		out = append(out, sealevel.SlotHash{Slot: entry.Slot, Hash: hash})
	}
	return out
}

func decodeRecentBlockhashesStrict(entries []state.BlockhashEntry) (sealevel.SysvarRecentBlockhashes, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("rooted resume context has no recent blockhashes")
	}
	out := make(sealevel.SysvarRecentBlockhashes, 0, len(entries))
	for i, entry := range entries {
		decoded, err := base58.Decode(entry.Blockhash)
		if err != nil || len(decoded) != 32 {
			return nil, fmt.Errorf("decode recent blockhash %d", i)
		}
		var hash [32]byte
		copy(hash[:], decoded)
		out = append(out, sealevel.RecentBlockHashesEntry{
			Blockhash:     hash,
			FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: entry.LamportsPerSignature},
		})
	}
	return out, nil
}

func decodeSlotHashesStrict(entries []state.SlotHashEntry) (sealevel.SysvarSlotHashes, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("rooted resume context has no slot hashes")
	}
	out := make(sealevel.SysvarSlotHashes, 0, len(entries))
	for i, entry := range entries {
		decoded, err := base58.Decode(entry.Hash)
		if err != nil || len(decoded) != 32 {
			return nil, fmt.Errorf("decode slot hash %d", i)
		}
		var hash [32]byte
		copy(hash[:], decoded)
		out = append(out, sealevel.SlotHash{Slot: entry.Slot, Hash: hash})
	}
	return out, nil
}

func decodeHash32(name, encoded string) ([32]byte, error) {
	var hash [32]byte
	decoded, err := base58.Decode(encoded)
	if err != nil || len(decoded) != len(hash) {
		return hash, fmt.Errorf("decode %s", name)
	}
	copy(hash[:], decoded)
	return hash, nil
}

func validateEpochConsensusMetadata(epochStakes map[uint64]string, voters map[string][]string) error {
	for expectedEpoch, encoded := range epochStakes {
		cache := epochstakes.NewEpochStakesCache()
		decodedEpoch, err := cache.DeserializeAndLoadEpoch([]byte(encoded))
		if err != nil {
			return fmt.Errorf("decode epoch %d stakes: %w", expectedEpoch, err)
		}
		if decodedEpoch != expectedEpoch {
			return fmt.Errorf("epoch stakes key %d contains epoch %d", expectedEpoch, decodedEpoch)
		}
	}
	for voteAcct, authorized := range voters {
		if _, err := decodeHash32("authorized-voter vote account", voteAcct); err != nil {
			return err
		}
		if len(authorized) == 0 {
			return fmt.Errorf("authorized-voter entry %s has no voters", voteAcct)
		}
		for _, voter := range authorized {
			if _, err := decodeHash32("authorized voter", voter); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateRootedResumeContext verifies the durable checkpoint as one coherent
// bundle. A corrupt member must never be silently omitted and replaced with
// snapshot-era state.
func ValidateRootedResumeContext(rc *state.ResumeContext) error {
	if rc == nil {
		return fmt.Errorf("nil rooted resume context")
	}
	if _, err := decodeHash32("rooted bankhash", rc.Bankhash); err != nil {
		return err
	}
	ltBytes, err := base64.StdEncoding.DecodeString(rc.AcctsLtHash)
	if err != nil || len(ltBytes) != lthash.HashByteLen {
		return fmt.Errorf("decode rooted accounts lt-hash")
	}
	if (rc.AlpenglowBlockID == "") != (rc.AlpenglowChainedRoot == "") {
		return fmt.Errorf("rooted Alpenglow identity is incomplete")
	}
	if rc.AlpenglowBlockID == "" {
		return fmt.Errorf("rooted resume context has no Alpenglow identity")
	}
	if _, err := decodeHash32("rooted Alpenglow block id", rc.AlpenglowBlockID); err != nil {
		return err
	}
	if _, err := decodeHash32("rooted Alpenglow chained root", rc.AlpenglowChainedRoot); err != nil {
		return err
	}
	if rc.TransactionCount == nil {
		return fmt.Errorf("rooted resume context has no exact transaction count")
	}
	if _, err := decodeRecentBlockhashesStrict(rc.RecentBlockhashes); err != nil {
		return err
	}
	if _, err := decodeHash32("evicted blockhash", rc.EvictedBlockhash); err != nil {
		return err
	}
	if _, err := decodeHash32("last blockhash", rc.Blockhash); err != nil {
		return err
	}
	if _, err := decodeSlotHashesStrict(rc.SlotHashes); err != nil {
		return err
	}
	clockBytes, err := base64.StdEncoding.DecodeString(rc.Clock)
	if err != nil || len(clockBytes) == 0 {
		return fmt.Errorf("decode rooted clock")
	}
	var clock sealevel.SysvarClock
	if err := clock.UnmarshalWithDecoder(bin.NewBinDecoder(clockBytes)); err != nil {
		return fmt.Errorf("decode rooted clock: %w", err)
	}
	if clock.Slot != rc.Slot {
		return fmt.Errorf("rooted clock names slot %d, want %d", clock.Slot, rc.Slot)
	}
	return validateEpochConsensusMetadata(rc.ComputedEpochStakes, rc.EpochAuthorizedVoters)
}

// ResumeStateFromRootedContext reconstructs replay state from a fold manifest
// or the matching state-file checkpoint.
func ResumeStateFromRootedContext(rc *state.ResumeContext, epochStakes map[uint64]string) (*ResumeState, error) {
	if err := ValidateRootedResumeContext(rc); err != nil {
		return nil, err
	}
	bankhash, _ := base58.Decode(rc.Bankhash)
	ltBytes, err := base64.StdEncoding.DecodeString(rc.AcctsLtHash)
	if err != nil {
		return nil, err
	}
	ltHash := new(lthash.LtHash).InitWithHash(ltBytes)
	rs := &ResumeState{
		ParentSlot:               rc.Slot,
		ParentEpoch:              rc.Epoch,
		ParentBlockHeight:        rc.BlockHeight,
		ParentBankhash:           append([]byte(nil), bankhash...),
		AcctsLtHash:              ltHash,
		LamportsPerSignature:     rc.LamportsPerSignature,
		PrevLamportsPerSignature: rc.PrevLamportsPerSig,
		NumSignatures:            rc.NumSignatures,
		Capitalization:           rc.Capitalization,
		SlotsPerYear:             rc.SlotsPerYear,
		InflationInitial:         rc.InflationInitial,
		InflationTerminal:        rc.InflationTerminal,
		InflationTaper:           rc.InflationTaper,
		InflationFoundation:      rc.InflationFoundation,
		InflationFoundationTerm:  rc.InflationFoundationTerm,
	}
	if rc.AlpenglowBlockID != "" {
		blockID, err := base58.Decode(rc.AlpenglowBlockID)
		if err != nil || len(blockID) != 32 {
			return nil, fmt.Errorf("decode rooted Alpenglow block id")
		}
		chainedRoot, err := base58.Decode(rc.AlpenglowChainedRoot)
		if err != nil || len(chainedRoot) != 32 {
			return nil, fmt.Errorf("decode rooted Alpenglow chained root")
		}
		rs.HasAlpenglowIdentity = true
		copy(rs.AlpenglowBlockID[:], blockID)
		copy(rs.AlpenglowChainedRoot[:], chainedRoot)
	}
	if rc.TransactionCount != nil {
		count := *rc.TransactionCount
		rs.TransactionCount = &count
	}
	recent, _ := decodeRecentBlockhashesStrict(rc.RecentBlockhashes)
	rs.RecentBlockhashes = &recent
	rs.EvictedBlockhash, _ = decodeHash32("evicted blockhash", rc.EvictedBlockhash)
	rs.LastBlockhash, _ = decodeHash32("last blockhash", rc.Blockhash)
	slotHashes, _ := decodeSlotHashesStrict(rc.SlotHashes)
	rs.SlotHashes = &slotHashes
	rootedEpochStakes := epochStakes
	if len(rc.ComputedEpochStakes) > 0 {
		rootedEpochStakes = rc.ComputedEpochStakes
	}
	if len(rootedEpochStakes) > 0 {
		rs.ComputedEpochStakes = make(map[uint64][]byte, len(rootedEpochStakes))
		for epoch, encoded := range rootedEpochStakes {
			rs.ComputedEpochStakes[epoch] = []byte(encoded)
		}
	}
	return rs, nil
}

// ResumeStateFromCheckpoint reconstructs the pre-rooted-durable graceful
// checkpoint format. The legacy bundle is still decoded strictly so a partial
// file cannot combine a recent AccountsDB with snapshot-era PoH/sysvar state.
func ResumeStateFromCheckpoint(s *state.MithrilState) (*ResumeState, error) {
	if s == nil || s.LastSlot == 0 {
		return nil, fmt.Errorf("checkpoint has no replayed slot")
	}
	bankhash, err := decodeHash32("checkpoint bankhash", s.LastBankhash)
	if err != nil {
		return nil, err
	}
	ltBytes, err := base64.StdEncoding.DecodeString(s.LastAcctsLtHash)
	if err != nil || len(ltBytes) != lthash.HashByteLen {
		return nil, fmt.Errorf("decode checkpoint accounts lt-hash")
	}
	recent, err := decodeRecentBlockhashesStrict(s.LastRecentBlockhashes)
	if err != nil {
		return nil, err
	}
	evicted, err := decodeHash32("checkpoint evicted blockhash", s.LastEvictedBlockhash)
	if err != nil {
		return nil, err
	}
	lastBlockhash, err := decodeHash32("checkpoint last blockhash", s.LastBlockhash)
	if err != nil {
		return nil, err
	}
	slotHashes, err := decodeSlotHashesStrict(s.LastSlotHashes)
	if err != nil {
		return nil, err
	}
	if err := validateEpochConsensusMetadata(s.ComputedEpochStakes, s.ManifestEpochAuthorizedVoters); err != nil {
		return nil, err
	}
	return &ResumeState{
		ParentSlot:               s.LastSlot,
		ParentEpoch:              s.LastEpoch,
		ParentBlockHeight:        s.LastBlockHeight,
		ParentBankhash:           append([]byte(nil), bankhash[:]...),
		AcctsLtHash:              new(lthash.LtHash).InitWithHash(ltBytes),
		LamportsPerSignature:     s.LastLamportsPerSignature,
		PrevLamportsPerSignature: s.LastPrevLamportsPerSig,
		NumSignatures:            s.LastNumSignatures,
		RecentBlockhashes:        &recent,
		EvictedBlockhash:         evicted,
		LastBlockhash:            lastBlockhash,
		SlotHashes:               &slotHashes,
		Capitalization:           s.LastCapitalization,
		SlotsPerYear:             s.LastSlotsPerYear,
		InflationInitial:         s.LastInflationInitial,
		InflationTerminal:        s.LastInflationTerminal,
		InflationTaper:           s.LastInflationTaper,
		InflationFoundation:      s.LastInflationFoundation,
		InflationFoundationTerm:  s.LastInflationFoundationTerm,
		ComputedEpochStakes: func() map[uint64][]byte {
			if len(s.ComputedEpochStakes) == 0 {
				return nil
			}
			out := make(map[uint64][]byte, len(s.ComputedEpochStakes))
			for epoch, encoded := range s.ComputedEpochStakes {
				out[epoch] = []byte(encoded)
			}
			return out
		}(),
	}, nil
}
