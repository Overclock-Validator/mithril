package rpcserver

import (
	"context"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/rpcdb"
	"github.com/Overclock-Validator/mithril/pkg/rpcindex"
	"github.com/Overclock-Validator/mithril/pkg/watch"
	"github.com/gagliardetto/solana-go"
)

// EnableHistory opens the personal history store at dir, registers watched
// wallets for tenant 0, and starts the indexer. Replay captures transactions
// only after this.
func (rpcServer *RpcServer) EnableHistory(dir string, genesis solana.Hash, wallets []solana.PublicKey, durableSlot uint64) error {
	store, err := rpcdb.Open(dir, genesis)
	if err != nil {
		return err
	}
	registry := watch.NewRegistry()
	persisted, err := store.Watches()
	if err != nil {
		_ = store.Close()
		return err
	}
	keys := make([]watch.Key, 0, len(persisted)+len(wallets))
	for _, w := range persisted {
		keys = append(keys, watch.Key{Tenant: w.Tenant, Key: w.Addr, Kind: watch.Kind(w.Kind)})
	}
	for _, wallet := range wallets {
		keys = append(keys, watch.Key{Key: wallet, Kind: watch.Wallet})
	}
	added := registry.AddKeys(keys...)
	if len(added) > 0 {
		since, err := store.Watermark()
		if err != nil {
			_ = store.Close()
			return err
		}
		batch := store.NewBatch()
		for _, k := range added {
			if err := batch.PutWatch(k.Tenant, k.Key, uint8(k.Kind), since); err != nil {
				_ = store.Close()
				return err
			}
		}
		if err := batch.Commit(); err != nil {
			_ = store.Close()
			return err
		}
	}
	// Folds and compaction write token owner sidecars from here on; files
	// without one are read whole by the first scan, which writes theirs.
	if rpcServer.acctsDb != nil {
		if err := rpcServer.acctsDb.EnableKeySidecars(rpcindex.TokenOwnerSidecar); err != nil {
			_ = store.Close()
			return err
		}
	}
	ix := rpcindex.New(store, registry)
	enum, err := rpcindex.NewEnumerator(ix, rpcServer.accountScanner, TokenScanConfig, durableSlot)
	if err != nil {
		_ = store.Close()
		return err
	}
	rpcServer.history = ix
	rpcServer.enumerator = enum
	go ix.Run(context.Background())
	go enum.Run(context.Background())
	var owners []solana.PublicKey
	for _, k := range keys {
		if registry.Kinds(k.Key)&watch.Wallet != 0 {
			owners = append(owners, k.Key)
		}
	}
	enum.Want(owners...)
	return nil
}

// TokenScanConfig bounds the account store scans that enumerate a newly
// watched wallet's token accounts.
var TokenScanConfig = accountsdb.ScanRecordsConfig{Workers: 2, MaxBytesPerSecond: 1 << 30}

// accountScanner returns the account store replay installed, once it has.
func (rpcServer *RpcServer) accountScanner() rpcindex.AccountScanner {
	s, _ := rpcServer.bankViews.Accounts().(rpcindex.AccountScanner)
	return s
}

// RPCIndexer is the history indexer, or nil when history is disabled.
func (rpcServer *RpcServer) RPCIndexer() *rpcindex.Indexer {
	return rpcServer.history
}

// watchSentSignature tracks a signature this node forwarded until its
// blockhash can no longer land.
func (rpcServer *RpcServer) watchSentSignature(sig solana.Signature) {
	if rpcServer.history == nil {
		return
	}
	view, _, err := rpcServer.bankViews.Current(bankview.Processed)
	if err != nil {
		return
	}
	reg := rpcServer.history.Registry()
	reg.ExpireSignatures(view.BlockHeight)
	reg.AddSignature(sig, view.BlockHeight+maxProcessingAge)
}

// historyAvailable returns the indexer, or an error when history is off.
func (rpcServer *RpcServer) historyAvailable() (*rpcindex.Indexer, error) {
	if rpcServer.history == nil {
		return nil, &InvalidParamsError{Message: "transaction history is not enabled on this node"}
	}
	if err := rpcServer.history.Err(); err != nil {
		return nil, fmt.Errorf("transaction history store failed: %w", err)
	}
	return rpcServer.history, nil
}
