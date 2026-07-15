package replay

import (
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

// LocalLeaderCommit records a slot finalized by local block production so replay
// can advance without re-executing the same transactions from RPC/turbine.
type LocalLeaderCommit struct {
	SlotCtx       *sealevel.SlotCtx
	Block         *b.Block
	ModifiedAccts []*accounts.Account
}

var (
	localLeaderMu             sync.RWMutex
	localLeaderCommits        = map[uint64]LocalLeaderCommit{}
	localLeaderCommitNotifier func(slot uint64)
)

func RegisterLocalLeaderCommit(slotCtx *sealevel.SlotCtx) {
	if slotCtx == nil {
		return
	}
	RegisterLocalLeaderCommitDetails(LocalLeaderCommit{SlotCtx: slotCtx})
}

func RegisterLocalLeaderCommitDetails(commit LocalLeaderCommit) {
	slotCtx := commit.SlotCtx
	if slotCtx == nil {
		return
	}
	localLeaderMu.Lock()
	localLeaderCommits[slotCtx.Slot] = commit
	localLeaderMu.Unlock()
	UpdateChainTipFromSlotCtx(slotCtx, slotCtx.Features)
	if localLeaderCommitNotifier != nil {
		localLeaderCommitNotifier(slotCtx.Slot)
	}
}

func SetLocalLeaderCommitNotifier(fn func(slot uint64)) {
	localLeaderCommitNotifier = fn
}

func ClearLocalLeaderCommitNotifier() {
	localLeaderCommitNotifier = nil
}

func TakeLocalLeaderCommit(slot uint64) (LocalLeaderCommit, bool) {
	localLeaderMu.Lock()
	defer localLeaderMu.Unlock()
	commit, ok := localLeaderCommits[slot]
	if ok {
		delete(localLeaderCommits, slot)
	}
	return commit, ok
}

func HasLocalLeaderCommit(slot uint64) bool {
	localLeaderMu.RLock()
	defer localLeaderMu.RUnlock()
	_, ok := localLeaderCommits[slot]
	return ok
}
