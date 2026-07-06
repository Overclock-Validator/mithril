package replay

import (
	"encoding/base64"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/mr-tron/base58"
)

// Rebuilding replay's resume state from a rooted (or retained in-RAM) resume
// context: used at startup for checkpoint resume and in-loop by the
// execute-on-receipt fork switch to re-run a certified sibling without a
// process restart.

// resolveInitialTransactionCount picks the transaction count to seed a replay
// run with: the checkpoint's exact count when the context recorded one, else
// the snapshot manifest's count (exact only when nothing folded past the
// snapshot — a checkpoint written before the field existed loses the folded
// span, so the caller warns that the count is approximate until re-bootstrap).
func resolveInitialTransactionCount(rs *ResumeState, manifestCount uint64) (count uint64, exact bool) {
	if rs != nil && rs.TransactionCount != nil {
		return *rs.TransactionCount, true
	}
	return manifestCount, false
}

func DecodeRecentBlockhashes(entries []state.BlockhashEntry) sealevel.SysvarRecentBlockhashes {
	result := make(sealevel.SysvarRecentBlockhashes, 0, len(entries))
	dropped := 0
	for _, entry := range entries {
		hashBytes, err := base58.Decode(entry.Blockhash)
		if err != nil || len(hashBytes) != 32 {
			dropped++
			continue
		}
		var blockhash [32]byte
		copy(blockhash[:], hashBytes)
		result = append(result, sealevel.RecentBlockHashesEntry{
			Blockhash:     blockhash,
			FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: entry.LamportsPerSignature},
		})
	}
	if dropped > 0 {
		mlog.Log.Errorf("dropped %d/%d RecentBlockhashes entries due to invalid base58 - state file may be corrupted", dropped, len(entries))
	}
	return result
}

// decodeSlotHashes converts state.SlotHashEntry list to sealevel.SysvarSlotHashes
func DecodeSlotHashes(entries []state.SlotHashEntry) sealevel.SysvarSlotHashes {
	result := make(sealevel.SysvarSlotHashes, 0, len(entries))
	dropped := 0
	for _, entry := range entries {
		hashBytes, err := base58.Decode(entry.Hash)
		if err != nil || len(hashBytes) != 32 {
			dropped++
			continue
		}
		var hash [32]byte
		copy(hash[:], hashBytes)
		result = append(result, sealevel.SlotHash{
			Slot: entry.Slot,
			Hash: hash,
		})
	}
	if dropped > 0 {
		mlog.Log.Errorf("dropped %d/%d SlotHashes entries due to invalid base58 - state file may be corrupted", dropped, len(entries))
	}
	return result
}

// resumeStateFromRootedContext builds a replay.ResumeState from the context
// captured at promotion (as of the last rooted slot); the next block is the slot
// after the last rooted slot, whose parent is the last rooted slot.
func ResumeStateFromRootedContext(rc *state.ResumeContext, epochStakes map[uint64]string) (*ResumeState, error) {
	parentBankhash, err := base58.Decode(rc.Bankhash)
	if err != nil {
		return nil, fmt.Errorf("decode rooted bankhash: %w", err)
	}
	ltHashBytes, err := base64.StdEncoding.DecodeString(rc.AcctsLtHash)
	if err != nil {
		return nil, fmt.Errorf("decode rooted accts_lt_hash: %w", err)
	}
	ltHash := &lthash.LtHash{}
	ltHash.InitWithHash(ltHashBytes)

	rs := &ResumeState{
		ParentSlot:               rc.Slot,
		ParentBlockHeight:        rc.BlockHeight,
		ParentBankhash:           parentBankhash,
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
	if rc.TransactionCount != nil {
		txc := *rc.TransactionCount // deep copy: contexts must not share pointers
		rs.TransactionCount = &txc
	}

	if len(rc.RecentBlockhashes) > 0 {
		recentBlockhashes := DecodeRecentBlockhashes(rc.RecentBlockhashes)
		rs.RecentBlockhashes = &recentBlockhashes
		if rc.EvictedBlockhash != "" {
			if evb, err := base58.Decode(rc.EvictedBlockhash); err == nil && len(evb) == 32 {
				copy(rs.EvictedBlockhash[:], evb)
			}
		}
		if rc.Blockhash != "" {
			if bb, err := base58.Decode(rc.Blockhash); err == nil && len(bb) == 32 {
				copy(rs.LastBlockhash[:], bb)
			}
		}
	}
	if len(rc.SlotHashes) > 0 {
		slotHashes := DecodeSlotHashes(rc.SlotHashes)
		rs.SlotHashes = &slotHashes
	}
	if rc.Clock != "" {
		clockData, err := base64.StdEncoding.DecodeString(rc.Clock)
		if err != nil {
			return nil, fmt.Errorf("decode rooted clock sysvar: %w", err)
		}
		rs.Clock = clockData
	}
	if len(epochStakes) > 0 {
		rs.ComputedEpochStakes = make(map[uint64][]byte, len(epochStakes))
		for epoch, data := range epochStakes {
			rs.ComputedEpochStakes[epoch] = []byte(data)
		}
	}
	return rs, nil
}
