package replay

import (
	"fmt"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/gagliardetto/solana-go"
)

var activeSpeculative struct {
	sync.RWMutex
	replay *SpeculativeReplay
}

func publishActiveSpeculativeReplay(sr *SpeculativeReplay) func() {
	stopResolver := func() {}
	if sr != nil && sr.accountsDb != nil {
		stopResolver = sr.accountsDb.InstallAccountResolver(func(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
			return sr.Resolve(slot, pubkey, sr.accountsDb)
		})
	}
	activeSpeculative.Lock()
	activeSpeculative.replay = sr
	activeSpeculative.Unlock()
	return func() {
		stopResolver()
		activeSpeculative.Lock()
		if activeSpeculative.replay == sr {
			activeSpeculative.replay = nil
		}
		activeSpeculative.Unlock()
	}
}

// ResolveActiveAccount gives local production and simulation the same
// overlay-first account view used by replay.
func ResolveActiveAccount(
	db *accountsdb.AccountsDb,
	parentSlot uint64,
	pubkey solana.PublicKey,
) (*accounts.Account, error) {
	activeSpeculative.RLock()
	sr := activeSpeculative.replay
	activeSpeculative.RUnlock()
	if sr != nil && sr.UseStoreForParent(parentSlot) {
		return sr.Resolve(parentSlot, pubkey, db)
	}
	if db == nil {
		return nil, fmt.Errorf("nil AccountsDB while resolving %s", pubkey)
	}
	acct, err := db.GetAccountDurable(parentSlot, pubkey)
	if err != nil {
		return nil, err
	}
	return acct.Clone(), nil
}

func ResolveActiveBankhash(db *accountsdb.AccountsDb, slot uint64) (solana.Hash, bool) {
	activeSpeculative.RLock()
	sr := activeSpeculative.replay
	activeSpeculative.RUnlock()
	if sr != nil {
		if bankhash, ok := sr.BankhashAt(slot); ok {
			return solana.HashFromBytes(bankhash), true
		}
	}
	if db == nil {
		return solana.Hash{}, false
	}
	bankhash, err := db.GetBankHashForSlot(slot)
	if err != nil || len(bankhash) != 32 {
		return solana.Hash{}, false
	}
	return solana.HashFromBytes(bankhash), true
}

// ResolveActiveAlpenglowIdentity returns an ID/root pair from one executed
// branch view. Consensus candidate IDs deliberately do not enter this path.
func ResolveActiveAlpenglowIdentity(slot uint64) (solana.Hash, solana.Hash, bool) {
	activeSpeculative.RLock()
	sr := activeSpeculative.replay
	activeSpeculative.RUnlock()
	if sr != nil {
		return sr.AlpenglowIdentityAt(slot)
	}
	blockID, blockOK := global.AlpenglowBlockID(slot)
	chainedRoot, rootOK := global.AlpenglowChainedMerkleRoot(slot)
	if !blockOK || !rootOK {
		return solana.Hash{}, solana.Hash{}, false
	}
	return blockID, chainedRoot, true
}

func stageActiveSpeculativeCommit(deferred *DeferredBlockCommit) error {
	activeSpeculative.RLock()
	sr := activeSpeculative.replay
	activeSpeculative.RUnlock()
	if sr == nil {
		return fmt.Errorf("no active speculative replay engine")
	}
	return sr.StagePending(deferred)
}
