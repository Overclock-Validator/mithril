package sealevel

import (
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
)

// NewScratchSlotCtx returns a slot context that executes against base's state
// without changing it. Account writes go to an overlay on base's accounts;
// vote-cache changes are deferred and program-cache insertions are recorded,
// so a caller that discards the scratch context can evict them. Execution
// only reads the remaining fields, which are shared with base.
func NewScratchSlotCtx(base *SlotCtx) *SlotCtx {
	scratch := &SlotCtx{
		Accounts:                  accounts.NewOverlayAccounts(base.Accounts),
		ParentAccts:               base.ParentAccts,
		AccountsDb:                base.AccountsDb,
		UnrootedRead:              base.UnrootedRead,
		FeeRateGovernor:           base.FeeRateGovernor,
		Slot:                      base.Slot,
		ParentSlot:                base.ParentSlot,
		Epoch:                     base.Epoch,
		AcctMapsMu:                &sync.Mutex{},
		ModifiedAccts:             make(map[solana.PublicKey]bool),
		WritableAccts:             make(map[solana.PublicKey]bool),
		NumSignatures:             base.NumSignatures,
		Blockhash:                 base.Blockhash,
		LastBlockhash:             base.LastBlockhash,
		SlotBank:                  base.SlotBank,
		Features:                  base.Features,
		VoteTimestampMu:           &sync.Mutex{},
		VoteTimestamps:            make(map[solana.PublicKey]BlockTimestamp),
		VoteAccts:                 base.VoteAccts,
		TotalEpochStake:           base.TotalEpochStake,
		Replay:                    base.Replay,
		LatestEvictedBlockhash:    base.LatestEvictedBlockhash,
		SerializedParameterArena:  base.SerializedParameterArena,
		TraceCtx:                  base.TraceCtx,
		DeferVoteCachePublication: true,
		TrackProgramCacheAdds:     true,
	}
	scratch.bankSysvars.Store(base.BankSysvars())
	return scratch
}
