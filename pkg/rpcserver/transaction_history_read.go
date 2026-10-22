package rpcserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/blockhistory"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/gagliardetto/solana-go"
)

// Only this exact internal error permits one fresh selection. A joined cleanup
// or storage error is never retryable, even if it contains this sentinel.
var errHistoryViewChanged = fmt.Errorf("transaction history view changed: %w", transactionhistory.ErrUnavailable)

// InitTransactionHistory attaches optional history before the HTTP server starts.
// The node owns the store and closes it after the writer and RPC readers finish.
func (server *RpcServer) InitTransactionHistory(store *transactionhistory.Store) {
	server.transactionHistory = store
	if store != nil {
		// ponytail: two concurrent history reads; tune only after measuring full
		// response memory, including encoded copies and pinned engine snapshots.
		server.transactionHistoryReads = make(chan struct{}, 2)
		server.transactionHistoryResponses = make(chan struct{}, 2)
	} else {
		server.transactionHistoryReads = nil
		server.transactionHistoryResponses = nil
	}
}

func (server *RpcServer) acquireTransactionHistory(ctx context.Context) (*transactionhistory.ReadSnapshot, func() error, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if server.transactionHistory == nil || server.transactionHistoryReads == nil {
		return nil, nil, transactionhistory.ErrUnavailable
	}
	select {
	case server.transactionHistoryReads <- struct{}{}:
	default:
		return nil, nil, transactionhistory.ErrUnavailable
	}
	// Snapshot waits for acknowledged writes. Do not hold the recovery gate
	// while waiting on optional storage; this admission bounds waiting readers.
	snapshot, err := server.transactionHistory.Snapshot()
	if err != nil {
		<-server.transactionHistoryReads
		return nil, nil, err
	}
	release := func() error {
		err := snapshot.Close()
		<-server.transactionHistoryReads
		return err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, errors.Join(err, release())
	}
	return snapshot, release, nil
}

// selectTransactionHistoryBlock reuses the published durable branch. A history
// completion record or an AccountsDB bankhash alone does not choose that branch.
// Optional storage reads stay outside the recovery gate. Selection is fenced
// before payload reads; an acquired view may finish after a later rewind.
func (server *RpcServer) selectTransactionHistoryBlock(snapshot *transactionhistory.ReadSnapshot, id transactionhistory.BlockID) (blockhistory.Record, error) {
	server.historyRecoveryMu.RLock()
	if server.historyRecoveryPending || server.blockHistory == nil || server.acctsDb == nil {
		server.historyRecoveryMu.RUnlock()
		return blockhistory.Record{}, transactionhistory.ErrUnavailable
	}
	bank := server.rootedBank.Load()
	version, stable := server.acctsDb.CommittedAccountVersion()
	stamp := historyReadStamp{bank: bank, version: version, durable: server.acctsDb.DurableThrough()}
	if bank == nil || !stable || id.Slot > bank.Slot || id.Slot > stamp.durable {
		server.historyRecoveryMu.RUnlock()
		return blockhistory.Record{}, transactionhistory.ErrUnavailable
	}
	summary, err := server.blockHistory.Get(id.Slot)
	if err == nil && !server.historyStampValid(stamp) {
		err = transactionhistory.ErrUnavailable
	}
	server.historyRecoveryMu.RUnlock()
	if err != nil {
		return blockhistory.Record{}, err
	}
	blockhash, err := solana.HashFromBase58(summary.Blockhash)
	if err != nil || [32]byte(blockhash) != id.Blockhash {
		return blockhistory.Record{}, transactionhistory.ErrUnavailable
	}
	header, err := snapshot.BlockHeader(id)
	if err != nil {
		return blockhistory.Record{}, err
	}
	bankhash, err := server.acctsDb.GetBankHashForSlot(id.Slot)
	if err != nil || !bytes.Equal(bankhash, header.Bankhash[:]) {
		return blockhistory.Record{}, transactionhistory.ErrUnavailable
	}
	server.historyRecoveryMu.RLock()
	defer server.historyRecoveryMu.RUnlock()
	// Summary replacement or pruning can happen without changing the bank.
	// Recheck the selected identity and its binding while holding the gate.
	summary, err = server.blockHistory.Get(id.Slot)
	if err != nil {
		return blockhistory.Record{}, transactionhistory.ErrUnavailable
	}
	blockhash, err = solana.HashFromBase58(summary.Blockhash)
	if err != nil || [32]byte(blockhash) != id.Blockhash {
		return blockhistory.Record{}, transactionhistory.ErrUnavailable
	}
	previous, err := solana.HashFromBase58(summary.PreviousBlockhash)
	if err != nil || header.ParentSlot != summary.ParentSlot || header.PreviousBlockhash != [32]byte(previous) ||
		header.CaptureVersion != transactionhistory.CaptureVersion {
		return blockhistory.Record{}, transactionhistory.ErrUnavailable
	}
	if !server.historyStampValid(stamp) {
		return blockhistory.Record{}, server.historyReadChange(stamp)
	}
	return summary, nil
}

func (server *RpcServer) readTransactionHistoryBlock(ctx context.Context, slot uint64) (summary blockhistory.Record, block transactionhistory.Block, err error) {
	for attempt := 0; ; attempt++ {
		summary, block, err = server.readTransactionHistoryBlockOnce(ctx, slot)
		if err != errHistoryViewChanged {
			return summary, block, err
		}
		if attempt == 1 {
			return blockhistory.Record{}, transactionhistory.Block{}, transactionhistory.ErrUnavailable
		}
		if err = waitForRootedPublication(ctx); err != nil {
			return blockhistory.Record{}, transactionhistory.Block{}, err
		}
	}
}

func (server *RpcServer) readTransactionHistoryBlockOnce(ctx context.Context, slot uint64) (summary blockhistory.Record, block transactionhistory.Block, err error) {
	snapshot, release, err := server.acquireTransactionHistory(ctx)
	if err != nil {
		return summary, block, err
	}
	defer func() {
		if releaseErr := release(); releaseErr != nil {
			err = errors.Join(err, releaseErr)
		}
	}()
	server.historyRecoveryMu.RLock()
	if server.historyRecoveryPending || server.blockHistory == nil {
		err = transactionhistory.ErrUnavailable
	} else {
		summary, err = server.blockHistory.Get(slot)
	}
	server.historyRecoveryMu.RUnlock()
	var id transactionhistory.BlockID
	if err == nil {
		var hash solana.Hash
		hash, err = solana.HashFromBase58(summary.Blockhash)
		id = transactionhistory.BlockID{Slot: slot, Blockhash: hash}
		if err == nil {
			summary, err = server.selectTransactionHistoryBlock(snapshot, id)
		}
	}
	if err == nil {
		block, err = snapshot.Block(id)
	}
	if err != nil {
		return blockhistory.Record{}, transactionhistory.Block{}, err
	}
	return summary, block, nil
}
