package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

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
	var lookups []solana.PublicKey
	var positions []int
	for _, i := range missing {
		if acct, ok := t.preloaded[keys[i]]; ok {
			out[i] = acct
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
		out[i] = loaded[n]
	}
	return nil
}

func (t *checkpointTail) GetAccount(_ uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if acct, ok := t.overlay.Lookup([32]byte(pubkey)); ok {
		return acct.Clone(), nil
	}
	out := make([]*accounts.Account, 1)
	if err := t.boundary(context.Background(), []solana.PublicKey{pubkey}, out, []int{0}); err != nil {
		return nil, err
	}
	if out[0] == nil {
		return nil, accountsdb.ErrNoAccount
	}
	return out[0].Clone(), nil
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
}

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
	if block == nil || block.Slot <= r.parent.Slot {
		return nil, fmt.Errorf("checkpoint replay at slot %d cannot take this block", r.parent.Slot)
	}
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
	slotCtx, err := processBlock(r.db, block, r.epochSchedule, r.workers, nil, r.hashes, r.tail, r.statuses, true, r.parentSysvars, nil)
	if err != nil {
		return nil, fmt.Errorf("checkpoint replay of slot %d: %w", block.Slot, err)
	}
	r.parent, r.parentSysvars, r.blockHeight = slotCtx, slotCtx.BankSysvars(), block.BlockHeight
	return slotCtx, nil
}
