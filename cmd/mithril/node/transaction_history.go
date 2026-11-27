package node

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/gagliardetto/solana-go"
)

// transactionHistoryConfig applies only to the optional non-voting RPC profile.
// Logical retention and a free-space reserve are not a physical disk quota.
type transactionHistoryConfig struct {
	Enabled        bool
	RetentionSlots int64
	MaxMiB         int64
	ReserveMiB     int64
}

func (cfg transactionHistoryConfig) validate(cluster, mode string, port int) error {
	if !cfg.Enabled {
		return nil
	}
	if cluster != "alpenglow" || mode != "verifying" {
		return errors.New("RPC transaction history requires Alpenglow verifying mode")
	}
	if port < 1 || port > 65535 {
		return errors.New("RPC transaction history requires an RPC port between 1 and 65535")
	}
	if cfg.RetentionSlots < 1 {
		return errors.New("RPC transaction history retention slots must be positive")
	}
	if cfg.MaxMiB < 80 || cfg.MaxMiB > math.MaxInt64>>20 {
		return errors.New("RPC transaction history maximum size must be at least 80 MiB and fit in signed bytes")
	}
	if cfg.ReserveMiB < 1 || cfg.ReserveMiB > math.MaxInt64>>20 {
		return errors.New("RPC transaction history free-space reserve must be positive and fit in signed bytes")
	}
	return nil
}

func openNodeTransactionHistory(cfg transactionHistoryConfig, cluster, mode string, port int, accountsDir, genesisText string) (*transactionhistory.Store, *replay.TransactionHistorySession, error) {
	if err := cfg.validate(cluster, mode, port); err != nil {
		return nil, nil, err
	}
	if !cfg.Enabled {
		return nil, nil, nil
	}
	genesis, err := solana.HashFromBase58(genesisText)
	if err != nil || accountsDir == "" || genesis == (solana.Hash{}) {
		return nil, nil, errors.New("RPC transaction history requires an accounts directory and genesis hash")
	}
	store, err := transactionhistory.OpenWithRetention(filepath.Join(accountsDir, "rpc-transaction-history"),
		genesis, transactionhistory.DefaultLimits(), transactionhistory.Retention{
			Slots: uint64(cfg.RetentionSlots), MaxBytes: uint64(cfg.MaxMiB) << 20, MinFreeBytes: uint64(cfg.ReserveMiB) << 20,
		})
	if err != nil {
		return nil, nil, err
	}
	session, err := replay.NewTransactionHistorySession(store, mode)
	if err != nil {
		return nil, nil, errors.Join(err, store.Close())
	}
	return store, session, nil
}

// stopNodeOptionalRPC runs after replay returns. A timeout leaves the borrowed
// databases open; the caller must exit without racing their remaining users.
func stopNodeOptionalRPC(server *rpcserver.RpcServer, session *replay.TransactionHistorySession, store *transactionhistory.Store, results *replay.TransactionResultSession) (bool, error) {
	if store == nil && results == nil {
		return true, nil
	}
	if server == nil || (store != nil && session == nil) {
		return false, errors.New("optional RPC has no server or history replay owner")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	readersJoined, rpcErr := server.Shutdown(ctx)
	if store == nil {
		return readersJoined, rpcErr
	}
	writerJoined, writerErr := session.Stop(ctx)
	if !readersJoined || !writerJoined {
		return false, errors.Join(rpcErr, writerErr)
	}
	closeErr := store.Close()
	return !errors.Is(closeErr, transactionhistory.ErrSnapshotsOpen), errors.Join(rpcErr, writerErr, closeErr)
}
