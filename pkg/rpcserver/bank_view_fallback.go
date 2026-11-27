package rpcserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// Replay publishes every completed bank as a bank view. Until it has, such as
// at node startup, the RPC serves the banks the node published through
// SetRootedBankState, SetRootedExecutionBank and SetSlotCtx: the durable root
// and the processed chain tip. publishFallbackViews rebuilds those views on
// each such call; once replay publishes a bank, bankview.Publisher ignores it.

// fallbackAccounts is the account state behind fallback views: the durable
// store, with the chain tip's unrooted reader at and above the tip's slot,
// and the published sysvar snapshots for sysvar accounts.
type fallbackAccounts struct {
	server *RpcServer
	db     *accountsdb.AccountsDb
	root   *rootedBankState
	tip    *replay.ChainTipSnapshot
}

// Generation changes whenever the node publishes a new tip or root, or the
// tip's reader reports a new account version, so a read overlapping either
// is retried.
func (f fallbackAccounts) Generation() uint64 {
	f.server.slotCtxMu.RLock()
	gen := f.server.executionGeneration
	f.server.slotCtxMu.RUnlock()
	if f.tip != nil {
		if versioned, ok := f.tip.UnrootedRead.(interface{ AccountReadVersion() uint64 }); ok {
			gen += versioned.AccountReadVersion() << 32
		}
	}
	return 2 * gen
}

func (f fallbackAccounts) LookupAt(pubkey [32]byte, maxSlot uint64) (*accounts.Account, bool) {
	acct, ok, err := f.LookupAtErr(pubkey, maxSlot)
	return acct, ok && err == nil
}

// LookupAtErr serves sysvars from the published snapshots and other accounts
// from the chain tip's reader at and above the tip's slot. The reader's
// failures are returned, never read past.
func (f fallbackAccounts) LookupAtErr(pubkey [32]byte, maxSlot uint64) (*accounts.Account, bool, error) {
	key := solana.PublicKey(pubkey)
	if sealevel.IsBankSysvarAccount(key) {
		var sysvars *sealevel.BankSysvars
		switch {
		case f.tip != nil && maxSlot >= f.tip.Slot:
			sysvars = f.tip.BankSysvars
		case f.root != nil && f.root.executionBank != nil && maxSlot >= f.root.Slot:
			sysvars = f.root.executionBank.BankSysvars
		}
		if sysvars != nil {
			acct, ok := sysvars.CloneAccount(key)
			if !ok {
				return nil, true, nil
			}
			return acct, true, nil
		}
	}
	if f.tip == nil || f.tip.UnrootedRead == nil || maxSlot < f.tip.Slot {
		return nil, false, nil
	}
	acct, err := f.tip.UnrootedRead.GetAccount(f.tip.Slot, key)
	if errors.Is(err, accountsdb.ErrNoAccount) || errors.Is(err, accounts.ErrAccountNotFound) {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	if acct == nil {
		return nil, false, fmt.Errorf("account reader returned no value or absence for %s", key)
	}
	acct.Key = key
	return acct, true, nil
}

func (f fallbackAccounts) ReadPublication() (uint64, uint64, bool) {
	return bankview.DurableAccounts{DB: f.db}.ReadPublication()
}

// durableMatchesRoot reports whether the durable store still ends at the
// published root. After a failed rewind repair it may not, and the root's
// accounts must not be served from it; the root's scalars still may.
func (f fallbackAccounts) durableMatchesRoot() bool {
	if f.db == nil || f.root == nil {
		return true
	}
	durable := f.db.DurableThrough()
	return durable == 0 || durable == f.root.Slot
}

func (f fallbackAccounts) GetDurable(pubkey solana.PublicKey) (*accounts.Account, error) {
	if !f.durableMatchesRoot() {
		return nil, bankview.ErrInconsistent
	}
	return bankview.DurableAccounts{DB: f.db}.GetDurable(pubkey)
}

func (f fallbackAccounts) GetDurableBatch(ctx context.Context, pubkeys []solana.PublicKey, maxDataBytes uint64) ([]*accounts.Account, error) {
	if !f.durableMatchesRoot() {
		return nil, bankview.ErrInconsistent
	}
	return bankview.DurableAccounts{DB: f.db}.GetDurableBatch(ctx, pubkeys, maxDataBytes)
}

// executionView is the bank view of a published execution bank.
func executionView(bank *replay.ChainTipSnapshot, view *bankview.View) {
	view.Bankhash = bank.Bankhash
	view.BlockID, view.HasBlockID = bank.AlpenglowBlockID, bank.HasAlpenglowBlockID
	view.Blockhash = bank.LastBlockhash
	view.Features = bank.Features
	if bank.PrevFeeGovernor != nil {
		view.FeeRateGovernor = *bank.PrevFeeGovernor
		view.LamportsPerSignature = bank.PrevFeeGovernor.LamportsPerSignature
	}
	view.LatestEvictedBlockhash = solana.Hash(bank.LatestEvictedBlockhash)
	view.EpochStakes, view.TotalEpochStake = bank.EpochStakes, bank.TotalEpochStake
}

func (rpcServer *RpcServer) publishFallbackViews() {
	root := rpcServer.rootedBank.Load()
	if root == nil {
		rpcServer.bankViews.PublishFallback(nil, nil, nil)
		return
	}
	rootView := &bankview.View{Slot: root.Slot, BlockHeight: root.BlockHeight, TransactionCount: root.TransactionCount}
	if root.executionBank != nil && root.executionBank.Slot == root.Slot {
		executionView(root.executionBank, rootView)
	} else if rpcServer.acctsDb != nil {
		// Without execution parameters, the blockhash is the head of the
		// root's RecentBlockhashes sysvar.
		if acct, err := rpcServer.acctsDb.GetAccount(root.Slot, solana.PublicKey(sealevel.SysvarRecentBlockHashesAddr)); err == nil {
			var recent sealevel.SysvarRecentBlockhashes
			if err := recent.UnmarshalWithDecoder(bin.NewBinDecoder(acct.Data)); err == nil && len(recent) > 0 {
				rootView.Blockhash = solana.Hash(recent[0].Blockhash)
				rootView.LamportsPerSignature = recent[0].FeeCalculator.LamportsPerSignature
			}
		}
	}
	accts := fallbackAccounts{server: rpcServer, db: rpcServer.acctsDb, root: root}
	var tipView *bankview.View
	rpcServer.slotCtxMu.RLock()
	completed, ready := rpcServer.slotCtx, rpcServer.healthReady
	rpcServer.slotCtxMu.RUnlock()
	// The tip must be the completed bank itself, not another bank that
	// reused its slot number.
	if tip := replay.ChainTipParentContext(); ready && completed != nil && tip.Generation != 0 && tip.BankSysvars != nil &&
		tip.Slot == completed.Slot && tip.BankSysvars == completed.BankSysvars() &&
		tip.LastBlockhash == solana.Hash(completed.Blockhash) && tip.Slot >= root.Slot {
		tipView = &bankview.View{Slot: tip.Slot, ParentSlot: completed.ParentSlot, BlockHeight: tip.BlockHeight}
		executionView(&tip, tipView)
		accts.tip = &tip
	}
	rpcServer.bankViews.PublishFallback(accts, rootView, tipView)
}
