package blockstream

import (
	"maps"
	"slices"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

// AlpenglowRecoveryState carries branch exclusions across an in-process rooted
// replay restart. Account state comes from the durable checkpoint, but forgetting
// the source's selection lets the shred spool immediately re-emit the discarded
// suffix. This is opaque, transient source state, not a finality decision or a
// substitute for validating the parent link of a newly assembled block.
type AlpenglowRecoveryState struct {
	rejected      map[uint64]map[solana.Hash]struct{}
	invalid       map[uint64]map[solana.Hash]struct{}
	authoritative map[uint64]solana.Hash
	skipRanges    []alpenglowSlotRange
}

func cloneAlpenglowExclusions(src map[uint64]map[solana.Hash]struct{}) map[uint64]map[solana.Hash]struct{} {
	dst := make(map[uint64]map[solana.Hash]struct{}, len(src))
	for slot, ids := range src {
		dst[slot] = maps.Clone(ids)
	}
	return dst
}

// CaptureAlpenglowRecoveryState must run after the old source has stopped, so
// its last accepted switch and any later certificate overrides are included.
func (bs *BlockSource) CaptureAlpenglowRecoveryState() *AlpenglowRecoveryState {
	if bs.sourceType != BlockSourceTurbine || !bs.turbineAlpenglowBlockIDHints {
		return nil
	}
	bs.rejectedAlpenglowMu.RLock()
	defer bs.rejectedAlpenglowMu.RUnlock()
	return &AlpenglowRecoveryState{
		rejected:      cloneAlpenglowExclusions(bs.rejectedAlpenglowBlockIDs),
		invalid:       cloneAlpenglowExclusions(bs.invalidAlpenglowBlockIDs),
		authoritative: maps.Clone(bs.authoritativeAlpenglowBlockIDs),
		skipRanges:    slices.Clone(bs.rejectedAlpenglowSkipRanges),
	}
}

// Restore before intake starts. Each replacement owns its maps: pruning or a
// new certificate must not mutate the state retained for another retry.
func (bs *BlockSource) restoreAlpenglowRecoveryState(recovery *AlpenglowRecoveryState, startSlot uint64) {
	if recovery == nil || bs.sourceType != BlockSourceTurbine || !bs.turbineAlpenglowBlockIDHints {
		return
	}
	bs.rejectedAlpenglowBlockIDs = cloneAlpenglowExclusions(recovery.rejected)
	bs.invalidAlpenglowBlockIDs = cloneAlpenglowExclusions(recovery.invalid)
	bs.authoritativeAlpenglowBlockIDs = maps.Clone(recovery.authoritative)
	if bs.authoritativeAlpenglowBlockIDs == nil {
		bs.authoritativeAlpenglowBlockIDs = make(map[uint64]solana.Hash)
	}
	bs.rejectedAlpenglowSkipRanges = slices.Clone(recovery.skipRanges)
	// Nothing below the resume frontier can be replayed in this attempt.
	for _, ids := range []map[uint64]map[solana.Hash]struct{}{bs.rejectedAlpenglowBlockIDs, bs.invalidAlpenglowBlockIDs} {
		for slot := range ids {
			if slot < startSlot {
				delete(ids, slot)
			}
		}
	}
	for slot := range bs.authoritativeAlpenglowBlockIDs {
		if slot < startSlot {
			delete(bs.authoritativeAlpenglowBlockIDs, slot)
		}
	}
	kept := bs.rejectedAlpenglowSkipRanges[:0]
	for _, r := range bs.rejectedAlpenglowSkipRanges {
		if r.to <= startSlot {
			continue
		}
		r.from = max(r.from, startSlot)
		kept = append(kept, r)
	}
	bs.rejectedAlpenglowSkipRanges = kept
	mlog.Log.Infof("ALPENGLOW branch recovery: restored exclusions at %d slots and %d speculative skip ranges from slot %d; certificates remain authoritative",
		len(bs.rejectedAlpenglowBlockIDs), len(kept), startSlot)
}
