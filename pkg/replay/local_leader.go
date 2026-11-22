package replay

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// LocalLeaderCommit is the already-mutated producer bank for a slot this
// validator forged. Replay adopts it instead of running ProcessBlock.
type LocalLeaderCommit struct {
	SlotCtx          *sealevel.SlotCtx
	EpochRewards     *LeaderEpochRewards
	ParentSlot       uint64
	ParentBankhash   [32]byte
	BlockID          [32]byte
	HasBlockID       bool
	ParentBlockID    [32]byte
	HasParentBlockID bool
}

// ErrLocalLeaderCommitMismatch prevents a stale producer bank from publishing
// state after replay has selected a different parent or block at the same slot.
var ErrLocalLeaderCommitMismatch = errors.New("local producer bank does not match replay")

var ErrLocalLeaderCommitMissing = errors.New("missing producer SlotCtx")

var (
	localLeaderMu      sync.Mutex
	localLeaderCommits = map[uint64]LocalLeaderCommit{}
)

// RegisterLocalLeaderCommit publishes a frozen producer SlotCtx for ordered adopt.
func RegisterLocalLeaderCommit(slotCtx *sealevel.SlotCtx, rewardReceipts ...*LeaderEpochRewards) {
	RegisterLocalLeaderBlockCommit(slotCtx, nil, rewardReceipts...)
}

// RegisterLocalLeaderBlockCommit binds the frozen bank to the exact produced
// block before replay configures (and may rewrite) its parent-dependent fields.
func RegisterLocalLeaderBlockCommit(slotCtx *sealevel.SlotCtx, produced *b.Block, rewardReceipts ...*LeaderEpochRewards) {
	if slotCtx == nil {
		return
	}
	var receipt *LeaderEpochRewards
	if len(rewardReceipts) != 0 {
		receipt = rewardReceipts[0]
	}
	commit := LocalLeaderCommit{SlotCtx: slotCtx, EpochRewards: receipt, ParentSlot: slotCtx.ParentSlot}
	if produced != nil {
		commit.ParentBankhash = produced.ParentBankhash
		commit.BlockID, commit.HasBlockID = produced.AlpenglowBlockID, produced.HasAlpenglowBlockID
		commit.ParentBlockID, commit.HasParentBlockID = produced.AlpenglowParentBlockID, produced.HasAlpenglowParentBlockID
	}
	localLeaderMu.Lock()
	localLeaderCommits[slotCtx.Slot] = commit
	localLeaderMu.Unlock()
}

// TakeLocalLeaderCommit removes and returns the registered commit for slot.
func TakeLocalLeaderCommit(slot uint64) (LocalLeaderCommit, bool) {
	localLeaderMu.Lock()
	defer localLeaderMu.Unlock()
	commit, ok := localLeaderCommits[slot]
	if ok {
		delete(localLeaderCommits, slot)
	}
	return commit, ok
}

// ResetLocalLeaderCommits drops unpublished producer banks (rewind / restart).
func ResetLocalLeaderCommits() {
	localLeaderMu.Lock()
	localLeaderCommits = map[uint64]LocalLeaderCommit{}
	localLeaderMu.Unlock()
}

// adoptLocalLeaderBlock installs a locally forged SlotCtx as the replay tip.
// It does not re-execute transactions, verify signatures, or publish into the
// process-global sysvar cache. The next ProcessBlock reads parent sysvars from
// lastSlotCtx.BankSysvars().
func adoptLocalLeaderBlock(
	block *b.Block,
	tail unrootedState,
	transactionStatuses *TransactionStatusCache,
	persistedHashes *persistedTracker,
) (*sealevel.SlotCtx, error) {
	if block == nil {
		return nil, fmt.Errorf("adopt local leader block: nil block")
	}
	commit, ok := TakeLocalLeaderCommit(block.Slot)
	if !ok || commit.SlotCtx == nil {
		return nil, fmt.Errorf("adopt local leader block slot %d: %w", block.Slot, ErrLocalLeaderCommitMissing)
	}
	slotCtx := commit.SlotCtx
	if slotCtx.BankSysvars() == nil {
		return nil, fmt.Errorf("adopt local leader block: missing bank sysvars for slot %d", block.Slot)
	}
	if slotCtx.Slot != block.Slot || commit.ParentSlot != block.ParentSlot || commit.ParentBankhash != block.ParentBankhash || (block.SourceParentSlot != 0 && block.SourceParentSlot != commit.ParentSlot) {
		return nil, fmt.Errorf("%w: slot %d parent slot/hash differs", ErrLocalLeaderCommitMismatch, block.Slot)
	}
	if commit.HasBlockID && (!block.HasAlpenglowBlockID || commit.BlockID != block.AlpenglowBlockID) {
		return nil, fmt.Errorf("%w: slot %d block identity differs", ErrLocalLeaderCommitMismatch, block.Slot)
	}
	if commit.HasParentBlockID && (!block.HasAlpenglowParentBlockID || commit.ParentBlockID != block.AlpenglowParentBlockID) {
		return nil, fmt.Errorf("%w: slot %d parent identity differs", ErrLocalLeaderCommitMismatch, block.Slot)
	}
	if len(slotCtx.FinalBankhash) != 32 {
		return nil, fmt.Errorf("%w: slot %d has no finalized bank hash", ErrLocalLeaderCommitMismatch, block.Slot)
	}
	bankHash := solana.HashFromBytes(slotCtx.FinalBankhash)
	if (block.HasExpectedBankhash && bankHash != block.ExpectedBankhash) || (block.BankHash != [32]byte{} && bankHash != block.BankHash) {
		return nil, fmt.Errorf("%w: slot %d bank hash differs", ErrLocalLeaderCommitMismatch, block.Slot)
	}
	if err := commit.EpochRewards.validate(block); err != nil {
		return nil, fmt.Errorf("adopt local leader block slot %d: %w", block.Slot, err)
	}
	if err := transactionStatuses.CommitBlock(block); err != nil {
		return nil, fmt.Errorf("adopt local leader block slot %d: %w", block.Slot, err)
	}
	modified := collectAdoptAccounts(slotCtx, block)
	bankhash := append([]byte(nil), slotCtx.FinalBankhash...)
	if tail := normalizeUnrootedState(tail); tail != nil {
		recordSlotStakeChange(tail, slotCtx, modified)
		tail.Add(slotCtx.Slot, modified, bankhash)
	}
	if persistedHashes != nil {
		persistedHashes.Set(block.Slot, bankhash)
	}
	publishDeferredVoteCache(slotCtx)
	global.IncrTransactionCount(uint64(len(block.Transactions)))
	return slotCtx, nil
}

func collectAdoptAccounts(slotCtx *sealevel.SlotCtx, block *b.Block) []*accounts.Account {
	seen := make(map[solana.PublicKey]struct{}, len(slotCtx.ModifiedAccts)+8)
	out := make([]*accounts.Account, 0, len(slotCtx.ModifiedAccts)+8)
	add := func(acct *accounts.Account) {
		if acct == nil {
			return
		}
		if _, ok := seen[acct.Key]; ok {
			return
		}
		seen[acct.Key] = struct{}{}
		out = append(out, acct)
	}
	for key := range slotCtx.ModifiedAccts {
		acct, err := slotCtx.GetAccount(key)
		if err == nil {
			add(acct)
		}
	}
	if block != nil {
		for _, acct := range block.EpochUpdatedAccts {
			if acct == nil {
				continue
			}
			current, err := slotCtx.GetAccount(acct.Key)
			if err != nil {
				current = acct
			}
			add(current)
		}
	}
	return out
}
