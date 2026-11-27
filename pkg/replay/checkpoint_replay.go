package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

// Replay of blocks the node has already executed, from an earlier fold
// boundary, beside the node's own replay. It reads accounts as of the boundary
// through a CheckpointView, keeps every change in memory that belongs to this
// replay alone, and publishes nothing: no account, cache entry or bank becomes
// visible to the node's own replay or to the store.

// ErrCheckpointReplayCrossesEpoch means the blocks leave the boundary's epoch.
// Epoch transitions compute rewards and stakes from process-wide state, so a
// private replay stops before one.
var ErrCheckpointReplayCrossesEpoch = errors.New("checkpoint replay cannot cross an epoch boundary")

// checkpointTail is the account source of a private replay: this replay's own
// blocks first, then the accounts loaded for it ahead of time, then the view.
type checkpointTail struct {
	view    *accountsdb.CheckpointView
	overlay *accounts.WorkingSet
	// preloaded holds accounts as of the boundary, nil for an absent account.
	preloaded map[solana.PublicKey]*accounts.Account
	// fetched holds boundary accounts read during the replay (the ones no
	// block of the fold changed: programs, read-only inputs), so each is read
	// from the store once. Values are *accounts.Account, nil for absent.
	fetched sync.Map
	// deltas, when set, sizes each block's changes (bench).
	deltas *deltaStats

	// versions, when kept, lists every version each account got during the
	// replay, oldest first, so the state after any replayed slot can be read
	// back later (see FoldReplay). Written only by the replay that owns the
	// tail; read only after that replay has finished.
	versions map[solana.PublicKey][]accountVersion
	// asOf, when set, makes this tail a view of another tail's finished
	// replay as it stood after slot through: reads fall to asOf's versions
	// up to that slot, then to asOf's boundary accounts. This tail's own
	// writes stay in its own overlay.
	asOf    *checkpointTail
	through uint64
}

type accountVersion struct {
	slot uint64
	acct *accounts.Account
}

// versionAt returns the account as the kept replay left it after slot
// through, if any replayed block up to then changed it.
func (t *checkpointTail) versionAt(key solana.PublicKey, through uint64) (*accounts.Account, bool) {
	versions := t.versions[key]
	// the first version after through; the one before it is the answer
	i := sort.Search(len(versions), func(i int) bool { return versions[i].slot > through })
	if i == 0 {
		return nil, false
	}
	return versions[i-1].acct, true
}

// viewAfter returns a tail that reads this finished replay's state as of the
// end of slot through.
func (t *checkpointTail) viewAfter(through uint64) *checkpointTail {
	return &checkpointTail{view: t.view, overlay: accounts.NewWorkingSet(), asOf: t, through: through}
}

func newCheckpointTail(view *accountsdb.CheckpointView) *checkpointTail {
	return &checkpointTail{view: view, overlay: accounts.NewWorkingSet(), preloaded: make(map[solana.PublicKey]*accounts.Account)}
}

// preload records accounts already read as of the boundary, so the replay
// does not look them up again. It must not run while the replay executes.
func (t *checkpointTail) preload(keys []solana.PublicKey, loaded []*accounts.Account) {
	for i, key := range keys {
		t.preloaded[key] = loaded[i]
	}
}

// boundary returns the account as of the boundary: shared, or nil if absent.
func (t *checkpointTail) boundary(ctx context.Context, keys []solana.PublicKey, out []*accounts.Account, missing []int) error {
	if t.asOf != nil {
		// Boundary accounts are the kept replay's: one cache for every query.
		return t.asOf.boundary(ctx, keys, out, missing)
	}
	var lookups []solana.PublicKey
	var positions []int
	for _, i := range missing {
		if acct, ok := t.preloaded[keys[i]]; ok {
			out[i] = acct
			continue
		}
		if acct, ok := t.fetched.Load(keys[i]); ok {
			out[i] = acct.(*accounts.Account)
			continue
		}
		lookups = append(lookups, keys[i])
		positions = append(positions, i)
	}
	if len(lookups) == 0 {
		return nil
	}
	loaded, _, err := t.view.LoadAccounts(ctx, lookups)
	if err != nil {
		return err
	}
	for n, i := range positions {
		// Two workers may read the same account at once; both keep the first
		// stored value, so a shared reader always sees one identity per key.
		kept, _ := t.fetched.LoadOrStore(lookups[n], loaded[n])
		out[i] = kept.(*accounts.Account)
	}
	return nil
}

// warm loads boundary accounts in the background, a chunk at a time, into the
// replay's read cache, so a replay can start before its accounts are all in
// memory: a key the replay needs first is simply read by the replay itself.
func (t *checkpointTail) warm(ctx context.Context, keys []solana.PublicKey, chunk, workers int) {
	for start := 0; start < len(keys) && ctx.Err() == nil; start += chunk {
		part := keys[start:min(len(keys), start+chunk)]
		loaded, _, err := t.view.LoadAccountsWith(ctx, part, workers)
		if err != nil {
			return
		}
		for i, key := range part {
			t.fetched.LoadOrStore(key, loaded[i])
		}
	}
}

// GetAccountShared returns the bank-visible account without copying it. The
// caller must not change it: the value is this replay's parent version.
func (t *checkpointTail) GetAccountShared(_ uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if acct, ok := t.overlay.Lookup([32]byte(pubkey)); ok {
		return acct, nil
	}
	if t.asOf != nil {
		if acct, ok := t.asOf.versionAt(pubkey, t.through); ok {
			return acct, nil
		}
	}
	out := make([]*accounts.Account, 1)
	if err := t.boundary(context.Background(), []solana.PublicKey{pubkey}, out, []int{0}); err != nil {
		return nil, err
	}
	if out[0] == nil {
		return nil, accountsdb.ErrNoAccount
	}
	return out[0], nil
}

func (t *checkpointTail) GetAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	acct, err := t.GetAccountShared(slot, pubkey)
	if err != nil {
		return nil, err
	}
	return acct.Clone(), nil
}

func (t *checkpointTail) GetAccountDataLen(slot uint64, pubkey solana.PublicKey) (uint64, error) {
	acct, err := t.GetAccountShared(slot, pubkey)
	if err != nil {
		return 0, err
	}
	return uint64(len(acct.Data)), nil
}

// GetAccountWithDataLimit serves the bank-context builder.
func (t *checkpointTail) GetAccountWithDataLimit(slot uint64, pubkey solana.PublicKey, maxDataBytes uint64) (*accounts.Account, error) {
	acct, err := t.GetAccount(slot, pubkey)
	if err != nil {
		return nil, err
	}
	if uint64(len(acct.Data)) > maxDataBytes {
		return nil, fmt.Errorf("%w: %d bytes exceeds %d", accountsdb.ErrAccountDataLimit, len(acct.Data), maxDataBytes)
	}
	return acct, nil
}

func (t *checkpointTail) GetAccountsBatch(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	out, err := t.GetAccountsBatchShared(ctx, slot, pks)
	if err != nil {
		return nil, err
	}
	for i, acct := range out {
		if acct != nil {
			out[i] = acct.Clone()
		}
	}
	return out, nil
}

// GetAccountsBatchShared has the store's batch semantics: an account that is
// absent or holds no lamports comes back as an empty system-owned placeholder.
func (t *checkpointTail) GetAccountsBatchShared(ctx context.Context, _ uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	out := make([]*accounts.Account, len(pks))
	t.overlay.LookupBatch(pks, out)
	var missing []int
	for i, acct := range out {
		if acct == nil && t.asOf != nil {
			// A version from the kept replay counts as this bank's own.
			acct, _ = t.asOf.versionAt(pks[i], t.through)
			out[i] = acct
		}
		if acct == nil {
			missing = append(missing, i)
		}
	}
	if err := t.boundary(ctx, pks, out, missing); err != nil {
		return nil, err
	}
	for _, i := range missing {
		if out[i] == nil || out[i].Lamports == 0 {
			out[i] = &accounts.Account{Key: pks[i], Owner: addresses.SystemProgramAddr, RentEpoch: math.MaxUint64}
		}
	}
	return out, nil
}

func (t *checkpointTail) Add(slot uint64, delta []*accounts.Account, _ []byte) {
	t.overlay.Add(slot, delta)
	if t.versions != nil {
		for _, acct := range delta {
			if acct != nil {
				t.versions[acct.Key] = append(t.versions[acct.Key], accountVersion{slot, acct})
			}
		}
	}
	if t.deltas != nil {
		t.deltas.add(delta)
	}
}

// deltaStats sizes what each block changes (bench): the per-slot differences a
// node would keep to answer for any slot without replaying up to it.
type deltaStats struct {
	slots, accounts, bytes, voteAccounts, voteBytes uint64
}

func (stats *deltaStats) add(delta []*accounts.Account) {
	stats.slots++
	for _, acct := range delta {
		if acct == nil {
			continue
		}
		// 32-byte key and owner, lamports, rent epoch and flags, then data.
		size := uint64(len(acct.Data)) + 32 + 32 + 8 + 8 + 2
		stats.accounts++
		stats.bytes += size
		if acct.Owner == addresses.VoteProgramAddr {
			stats.voteAccounts++
			stats.voteBytes += size
		}
	}
}

func (t *checkpointTail) SetContext(uint64, *state.ResumeContext, ...*sealevel.BankSysvars) {}
func (t *checkpointTail) promote(uint64) (uint64, *state.ResumeContext, error)              { return 0, nil, nil }
func (t *checkpointTail) flush(uint64) (uint64, *state.ResumeContext, error)                { return 0, nil, nil }
func (t *checkpointTail) OverCap() bool                                                     { return false }

// CheckpointReplay replays blocks from a fold boundary. It is not safe for
// concurrent use; several may run at once.
type CheckpointReplay struct {
	db            *accountsdb.AccountsDb
	tail          *checkpointTail
	epochSchedule *sealevel.SysvarEpochSchedule
	workers       int
	statuses      *TransactionStatusCache
	hashes        *persistedTracker

	parent        *sealevel.SlotCtx
	parentSysvars *sealevel.BankSysvars
	blockHeight   uint64
	epoch         uint64

	// useHints replays each block with the hints this node stored for it.
	useHints bool
	// HintedBlocks and SkippedFailures count what the hints did.
	HintedBlocks, SkippedFailures int64
}

// ErrReplayHintsDisagree means a block did not behave as its stored hints
// said. The replay's state is not to be trusted from that block on: discard
// it and replay again without hints.
var ErrReplayHintsDisagree = errors.New("a block did not match this node's stored replay hints")

// UseHints makes the replay use this node's stored hints for each block that
// has them (see replay_hints.go). The caller must compare every bank hash with
// the recorded one and, on a mismatch or ErrReplayHintsDisagree, discard this
// replay and run it again without hints.
func (r *CheckpointReplay) UseHints(on bool) { r.useHints = on }

// NewCheckpointReplay prepares a replay that starts after view's boundary.
// persisted supplies what a resume record does not carry (the fee governor's
// fixed parameters, the epoch schedule and epoch stakes).
func NewCheckpointReplay(ctx context.Context, db *accountsdb.AccountsDb, view *accountsdb.CheckpointView, persisted *state.MithrilState, workers int) (*CheckpointReplay, error) {
	if db == nil || view == nil || persisted == nil {
		return nil, errors.New("checkpoint replay needs a store, a view and the node's persisted state")
	}
	var record state.ResumeContext
	if err := json.Unmarshal(view.ResumeContext(), &record); err != nil {
		return nil, fmt.Errorf("decode the checkpoint's resume record: %w", err)
	}
	if record.Slot != view.ThroughSlot() {
		return nil, fmt.Errorf("resume record is for slot %d, the checkpoint ends at %d", record.Slot, view.ThroughSlot())
	}
	resume, err := ResumeStateFromRootedContext(&record, nil)
	if err != nil {
		return nil, err
	}
	tail := newCheckpointTail(view)
	bank, err := bankContextFromRecord(ctx, tail, persisted, &record, resume)
	if err != nil {
		return nil, fmt.Errorf("checkpoint bank at slot %d: %w", record.Slot, err)
	}
	schedule, ok := bankEpochScheduleFromState(persisted)
	if !ok {
		value, found := bank.BankSysvars.EpochSchedule()
		if !found {
			return nil, errors.New("checkpoint bank has no epoch schedule")
		}
		schedule = &value
	}
	parent := &sealevel.SlotCtx{
		Slot:                   bank.Slot,
		Epoch:                  schedule.GetEpoch(bank.Slot),
		FinalBankhash:          bank.Bankhash[:],
		AcctsLtHash:            resume.AcctsLtHash,
		VoteTimestamps:         make(map[solana.PublicKey]sealevel.BlockTimestamp),
		VoteAccts:              bank.EpochStakes,
		TotalEpochStake:        bank.TotalEpochStake,
		LatestEvictedBlockhash: bank.LatestEvictedBlockhash,
		FeeRateGovernor:        bank.PrevFeeGovernor,
		NumSignatures:          resume.NumSignatures,
		Blockhash:              bank.LastBlockhash,
		Features:               bank.Features,
	}
	return &CheckpointReplay{
		db: db, tail: tail, epochSchedule: schedule, workers: max(1, workers),
		statuses: NewTransactionStatusCache(), hashes: &persistedTracker{},
		parent: parent, parentSysvars: bank.BankSysvars, blockHeight: bank.BlockHeight, epoch: parent.Epoch,
	}, nil
}

// Preload hands the replay accounts already read as of the boundary (nil for
// an absent account), typically everything the next fold touches.
func (r *CheckpointReplay) Preload(keys []solana.PublicKey, loaded []*accounts.Account) {
	r.tail.preload(keys, loaded)
}

// Slot is the last slot replayed, or the boundary before the first block.
func (r *CheckpointReplay) Slot() uint64 { return r.parent.Slot }

// Next replays block on top of the last one and returns its bank. The block
// must be the next block of the chain the node executed after the boundary.
func (r *CheckpointReplay) Next(block *b.Block) (*sealevel.SlotCtx, error) {
	return r.next(block, nil, nil)
}

// next is Next, optionally capturing metadata for selected transactions and
// executing one transaction in full even if it is a recorded failure.
func (r *CheckpointReplay) next(block *b.Block, capture *blockMetadataCapture, execute *solana.Signature) (*sealevel.SlotCtx, error) {
	if block == nil || block.Slot <= r.parent.Slot {
		return nil, fmt.Errorf("checkpoint replay at slot %d cannot take this block", r.parent.Slot)
	}
	block.PrivateReplay = true
	block.Epoch = r.epochSchedule.GetEpoch(block.Slot)
	if block.Epoch != r.epoch {
		return nil, ErrCheckpointReplayCrossesEpoch
	}
	if err := configureBlockFromParent(block, r.parent, r.epochSchedule, false); err != nil {
		return nil, err
	}
	block.LastBlockhash = r.parent.Blockhash
	block.BlockHeight = r.blockHeight + 1
	block.Features = r.parent.Features
	var use *hintUse
	if r.useHints {
		if hints := hintsFor(block); hints != nil {
			use = &hintUse{hints: hints, execute: execute}
			block.ReplayHints = use
		}
	}
	slotCtx, err := processBlock(r.db, block, r.epochSchedule, r.workers, nil, r.hashes, r.tail, r.statuses, true, r.parentSysvars, capture)
	if err != nil {
		return nil, fmt.Errorf("checkpoint replay of slot %d: %w", block.Slot, err)
	}
	if use != nil {
		if use.violated.Load() {
			return nil, fmt.Errorf("slot %d: %w", block.Slot, ErrReplayHintsDisagree)
		}
		r.HintedBlocks++
		r.SkippedFailures += use.skipped.Load()
	}
	r.parent, r.parentSysvars, r.blockHeight = slotCtx, slotCtx.BankSysvars(), block.BlockHeight
	return slotCtx, nil
}
