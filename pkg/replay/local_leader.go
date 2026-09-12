package replay

import (
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
	SlotCtx *sealevel.SlotCtx
}

var (
	localLeaderMu      sync.Mutex
	localLeaderCommits = map[uint64]LocalLeaderCommit{}
)

// RegisterLocalLeaderCommit publishes a frozen producer SlotCtx for ordered adopt.
func RegisterLocalLeaderCommit(slotCtx *sealevel.SlotCtx) {
	if slotCtx == nil {
		return
	}
	localLeaderMu.Lock()
	localLeaderCommits[slotCtx.Slot] = LocalLeaderCommit{SlotCtx: slotCtx}
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
		return nil, fmt.Errorf("adopt local leader block: missing producer SlotCtx for slot %d", block.Slot)
	}
	slotCtx := commit.SlotCtx
	if slotCtx.BankSysvars() == nil {
		return nil, fmt.Errorf("adopt local leader block: missing bank sysvars for slot %d", block.Slot)
	}
	modified := collectAdoptAccounts(slotCtx, block)
	bankhash := append([]byte(nil), slotCtx.FinalBankhash...)
	if tail != nil {
		tail.Add(slotCtx.Slot, modified, bankhash)
	}
	if persistedHashes != nil {
		persistedHashes.Set(block.Slot, bankhash)
	}
	if err := transactionStatuses.CommitBlock(block); err != nil {
		return nil, fmt.Errorf("adopt local leader block slot %d: %w", block.Slot, err)
	}
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
