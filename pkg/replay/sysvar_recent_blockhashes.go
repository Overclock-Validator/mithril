package replay

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
)

// RecentBlockhashesFromState decodes persisted blockhash entries into the sysvar form.
func RecentBlockhashesFromState(entries []state.BlockhashEntry) (sealevel.SysvarRecentBlockhashes, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("no RecentBlockhashes entries")
	}
	result := make(sealevel.SysvarRecentBlockhashes, 0, len(entries))
	for _, entry := range entries {
		hash, err := base58.DecodeFromString(entry.Blockhash)
		if err != nil {
			return nil, fmt.Errorf("decode RecentBlockhashes entry: %w", err)
		}
		result = append(result, sealevel.RecentBlockHashesEntry{
			Blockhash: hash,
			FeeCalculator: sealevel.FeeCalculator{
				LamportsPerSignature: entry.LamportsPerSignature,
			},
		})
	}
	return result, nil
}

// SeedRecentBlockhashesCache stores RBH in SysvarCache if not already populated.
func SeedRecentBlockhashesCache(recent sealevel.SysvarRecentBlockhashes) {
	if sealevel.SysvarCache.RecentBlockHashes.Sysvar != nil {
		return
	}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &recent
}

// cloneRecentBlockhashesFromCache returns a copy of the legacy ordered-replay
// bootstrap deque. Executing replay and leader banks use SlotCtx.BankSysvars;
// this singleton must not be treated as transaction-visible bank state.
func cloneRecentBlockhashesFromCache() (sealevel.SysvarRecentBlockhashes, error) {
	if sealevel.SysvarCache.RecentBlockHashes.Sysvar == nil {
		return nil, fmt.Errorf("RecentBlockhashes sysvar cache is nil")
	}
	src := *sealevel.SysvarCache.RecentBlockHashes.Sysvar
	if len(src) == 0 {
		return nil, fmt.Errorf("RecentBlockhashes sysvar cache is empty")
	}
	clone := make(sealevel.SysvarRecentBlockhashes, len(src))
	copy(clone, src)
	return clone, nil
}
