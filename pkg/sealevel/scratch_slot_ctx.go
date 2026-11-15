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
	return newDerivedSlotCtx(base, accounts.NewOverlayAccounts(base.Accounts))
}

// NewSealedSlotCtx returns a slot context for an execution that must not change
// base and whose account reads are all made through view, which the caller
// controls (a read-only recording view). Everything else an execution reads is
// constant for the block and shared with base; the bookkeeping an execution
// could write is private to the returned context, as in a scratch context.
// The result of such an execution is committed, if at all, against base.
//
// What reaches an execution without going through view, and why none of it
// can change between the execution and its transaction's turn (audited
// 2026-11-13):
//
//   - Parent-bank account versions (GetAccountFromAccountsDb and the helpers
//     built on it). The parent is frozen before the block starts and the
//     block's stores go to the overlay. Every such read in the loading and
//     loader paths is a fallback taken only after the slot's accounts, that is
//     view, were asked for the same key and had no entry, and view records
//     that absence: a later store to the key is then a recorded read that
//     changed.
//   - Program bytes. The loader resolves the program account and, for
//     upgradeable programs, the program data account through the slot's
//     accounts first, so both are recorded by view. The compiled-program cache
//     is consulted only for an entry that matches those exact bytes and the
//     feature set, so a compiled program is a function of recorded reads and
//     block constants; an execution marked Speculative never inserts into it.
//   - Address lookup tables, resolved from parent state before execution.
//   - The bank sysvar snapshot, feature set, blockhashes, fee rate and epoch
//     stakes: set before the block's transactions run. Sysvar accounts cannot
//     be written by transactions.
func NewSealedSlotCtx(base *SlotCtx, view accounts.Accounts) *SlotCtx {
	return newDerivedSlotCtx(base, view)
}

// newDerivedSlotCtx is the one place that decides, field by field, what a
// context derived from base shares with it. TestDerivedSlotCtxFieldsClassified
// fails when SlotCtx gains a field that is not classified there.
func newDerivedSlotCtx(base *SlotCtx, accts accounts.Accounts) *SlotCtx {
	scratch := &SlotCtx{
		Accounts:                  accts,
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
