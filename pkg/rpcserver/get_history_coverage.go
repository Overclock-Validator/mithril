package rpcserver

import (
	"context"
	"errors"

	"github.com/Overclock-Validator/mithril/pkg/blockhistory"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

// History is recorded as replay allows, so it can have holes: a block whose
// capture was refused, a write the store turned away, a stopped recorder. A
// lookup cannot tell a hole from "no such transaction". getHistoryCoverage
// reports what the node actually holds, so a client can.

const (
	// historyCoverageScanSlots bounds one coverage scan. Each slot costs a map
	// probe and at most one store lookup.
	historyCoverageScanSlots = 65_536
	// historyCoverageListed bounds how many missing slots are named.
	historyCoverageListed = 64
)

type HistoryCoverageResp struct {
	Blocks            *BlockCoverage       `json:"blocks"`
	Transactions      *TransactionCoverage `json:"transactions"`
	SignatureStatuses bool                 `json:"signatureStatuses"`
}

type BlockCoverage struct {
	// FirstSlot is the oldest retained slot; RootedSlot the newest served.
	FirstSlot  uint64 `json:"firstSlot"`
	RootedSlot uint64 `json:"rootedSlot"`
}

type TransactionCoverage struct {
	FirstSlot    uint64 `json:"firstSlot"`
	LastSlot     uint64 `json:"lastSlot"`
	Blocks       uint64 `json:"blocks"`
	LogicalBytes uint64 `json:"logicalBytes"`
	// WatchedAddresses is nonzero when only transactions naming a watched
	// address are recorded. A recorded block then holds a subset.
	WatchedAddresses int64 `json:"watchedAddresses"`
	// Recording is false once the recorder has stopped; StoppedReason says why.
	Recording     bool   `json:"recording"`
	StoppedReason string `json:"stoppedReason,omitempty"`
	// Rooted blocks from CheckedFromSlot through CheckedThroughSlot that have
	// no recorded transactions. MissingSlots names the newest of them.
	CheckedFromSlot    uint64   `json:"checkedFromSlot"`
	CheckedThroughSlot uint64   `json:"checkedThroughSlot"`
	MissingBlocks      uint64   `json:"missingBlocks"`
	MissingSlots       []uint64 `json:"missingSlots"`
}

// SetHistoryRecorderStatus supplies the replay recorder's state: whether it is
// still recording, and why not.
func (rpcServer *RpcServer) SetHistoryRecorderStatus(status func() (recording bool, stoppedReason string)) {
	rpcServer.slotCtxMu.Lock()
	rpcServer.historyRecorderStatus = status
	rpcServer.slotCtxMu.Unlock()
}

// SetHistoryWatch records how many addresses limit transaction history; zero
// means every transaction is recorded.
func (rpcServer *RpcServer) SetHistoryWatch(addresses int) {
	rpcServer.historyWatched.Store(int64(addresses))
}

func (rpcServer *RpcServer) GetHistoryCoverage(ctx context.Context, p jsonrpc.RawParams) (HistoryCoverageResp, error) {
	if err := requireNoParams(p, "getHistoryCoverage"); err != nil {
		return HistoryCoverageResp{}, err
	}
	rpcServer.historyRecoveryMu.RLock()
	pending := rpcServer.historyRecoveryPending
	rpcServer.historyRecoveryMu.RUnlock()
	if pending {
		return HistoryCoverageResp{}, errors.New("RPC history is unavailable during account recovery")
	}
	resp := HistoryCoverageResp{SignatureStatuses: rpcServer.resultCache != nil}
	blocks := rpcServer.blockHistory
	if blocks != nil {
		// An empty store has no first slot yet; it reports zero.
		first, _ := blocks.MinimumLedgerSlot()
		resp.Blocks = &BlockCoverage{FirstSlot: first, RootedSlot: blocks.Rooted()}
	}
	store := rpcServer.transactionHistory
	if store == nil {
		return resp, nil
	}
	stats, err := store.Stats()
	if err != nil {
		return HistoryCoverageResp{}, err
	}
	coverage := &TransactionCoverage{
		FirstSlot: stats.FirstSlot, LastSlot: stats.LastSlot, Blocks: stats.Blocks, LogicalBytes: stats.LogicalBytes,
		Recording: true, MissingSlots: []uint64{}, WatchedAddresses: rpcServer.historyWatched.Load(),
	}
	rpcServer.slotCtxMu.RLock()
	status := rpcServer.historyRecorderStatus
	rpcServer.slotCtxMu.RUnlock()
	if status != nil {
		coverage.Recording, coverage.StoppedReason = status()
	}
	resp.Transactions = coverage
	if blocks == nil || stats.Blocks == 0 {
		return resp, nil
	}
	// Holes are judged against rooted blocks only, newest first, so the named
	// slots are the ones a client is most likely to ask about.
	through := blocks.Rooted()
	from := stats.FirstSlot
	if through >= historyCoverageScanSlots && from < through-historyCoverageScanSlots+1 {
		from = through - historyCoverageScanSlots + 1
	}
	if from > through {
		return resp, nil
	}
	snapshot, err := store.Snapshot()
	if err != nil {
		return HistoryCoverageResp{}, err
	}
	defer snapshot.Close()
	coverage.CheckedFromSlot, coverage.CheckedThroughSlot = from, through
	for slot := through; ; slot-- {
		if slot&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return HistoryCoverageResp{}, err
			}
		}
		if record, err := blocks.Get(slot); err == nil {
			hash, hashErr := solana.HashFromBase58(record.Blockhash)
			recorded, readErr := snapshot.HasBlock(transactionhistory.BlockID{Slot: slot, Blockhash: hash})
			if readErr != nil {
				return HistoryCoverageResp{}, readErr
			}
			if hashErr != nil || !recorded {
				coverage.MissingBlocks++
				if len(coverage.MissingSlots) < historyCoverageListed {
					coverage.MissingSlots = append(coverage.MissingSlots, slot)
				}
			}
		} else if !errors.Is(err, blockhistory.ErrSlotSkipped) && !errors.Is(err, blockhistory.ErrNotAvailable) {
			return HistoryCoverageResp{}, err
		}
		if slot == from {
			break
		}
	}
	return resp, nil
}
