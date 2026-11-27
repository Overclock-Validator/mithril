package rpcserver

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

const (
	// recentPrioritizationFeeBlocks is how many replayed blocks the fee record
	// keeps, as Agave's MAX_NUM_RECENT_BLOCKS.
	recentPrioritizationFeeBlocks = 150
	// maxPrioritizationFeeAccounts is the most addresses one request may name,
	// as Agave's MAX_TX_ACCOUNT_LOCKS.
	maxPrioritizationFeeAccounts = 128
	// prioritizationFeeQueue is how many completed blocks may wait for the
	// fee worker before replay drops one rather than wait.
	prioritizationFeeQueue = 64
)

// RecentPrioritizationFee is one block in a getRecentPrioritizationFees
// response.
type RecentPrioritizationFee struct {
	Slot              uint64 `json:"slot"`
	PrioritizationFee uint64 `json:"prioritizationFee"`
}

// blockFees is one replayed block's fee record: the lowest compute unit price
// among its non-vote transactions, and for each account it write-locked, the
// lowest price among the transactions that locked it. Accounts whose lowest
// price does not exceed the block's are left out: they cannot raise a result.
type blockFees struct {
	slot     uint64
	min      uint64
	accounts []accountFee // sorted by key
}

type accountFee struct {
	key solana.PublicKey
	fee uint64
}

func (bf *blockFees) fee(keys []solana.PublicKey) uint64 {
	fee := bf.min
	for _, k := range keys {
		i, ok := slices.BinarySearchFunc(bf.accounts, k, func(a accountFee, k solana.PublicKey) int {
			return bytes.Compare(a.key[:], k[:])
		})
		if ok {
			fee = max(fee, bf.accounts[i].fee)
		}
	}
	return fee
}

// prioritizationFees records recent blocks' fees off the replay thread.
type prioritizationFees struct {
	enabled atomic.Bool
	start   sync.Once
	queue   chan completedFees
	dropped atomic.Uint64

	mu     sync.RWMutex
	blocks []*blockFees // sorted by slot
}

// completedFees is a completed block's executed non-vote transactions.
type completedFees struct {
	slot     uint64
	features *features.Features
	txs      []*solana.Transaction
}

// RecordPrioritizationFees queues a completed block's executed non-vote
// transactions, with lookup tables resolved, for the fee record. Replay calls
// it once per completed block; it never blocks.
// EnablePrioritizationFees makes replay record prioritization fees for
// getRecentPrioritizationFees. Call it before replay starts.
func (rpcServer *RpcServer) EnablePrioritizationFees() { rpcServer.priorityFees.enabled.Store(true) }

// PrioritizationFeesEnabled reports whether replay should record fees.
func (rpcServer *RpcServer) PrioritizationFeesEnabled() bool {
	return rpcServer.priorityFees.enabled.Load()
}

func (rpcServer *RpcServer) RecordPrioritizationFees(slot uint64, f *features.Features, txs []*solana.Transaction) {
	pf := &rpcServer.priorityFees
	pf.start.Do(func() {
		pf.queue = make(chan completedFees, prioritizationFeeQueue)
		go pf.run()
	})
	select {
	case pf.queue <- completedFees{slot: slot, features: f, txs: txs}:
	default:
		pf.dropped.Add(1)
	}
}

func (pf *prioritizationFees) run() {
	for c := range pf.queue {
		pf.insert(computeBlockFees(c))
	}
}

func computeBlockFees(c completedFees) *blockFees {
	bf := &blockFees{slot: c.slot}
	byKey := map[solana.PublicKey]uint64{}
	var writable []solana.PublicKey
	seen := false
	for _, tx := range c.txs {
		price, keys, ok := replay.TransactionPrioritizationFee(tx, c.features, writable[:0])
		writable = keys
		if !ok {
			continue
		}
		if !seen || price < bf.min {
			bf.min = price
		}
		seen = true
		for _, k := range keys {
			if fee, ok := byKey[k]; !ok || price < fee {
				byKey[k] = price
			}
		}
	}
	for k, fee := range byKey {
		if fee > bf.min {
			bf.accounts = append(bf.accounts, accountFee{key: k, fee: fee})
		}
	}
	slices.SortFunc(bf.accounts, func(x, y accountFee) int { return bytes.Compare(x.key[:], y.key[:]) })
	return bf
}

// insert adds or replaces a slot's record, keeping the newest blocks. A slot
// replayed again, after a fork switch, replaces its earlier record.
func (pf *prioritizationFees) insert(bf *blockFees) {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	i, found := slices.BinarySearchFunc(pf.blocks, bf.slot, func(x *blockFees, slot uint64) int {
		return compareUint64(x.slot, slot)
	})
	if found {
		pf.blocks[i] = bf
		return
	}
	pf.blocks = slices.Insert(pf.blocks, i, bf)
	if n := len(pf.blocks) - recentPrioritizationFeeBlocks; n > 0 {
		pf.blocks = slices.Delete(pf.blocks, 0, n)
	}
}

func (pf *prioritizationFees) recent(keys []solana.PublicKey) []RecentPrioritizationFee {
	pf.mu.RLock()
	defer pf.mu.RUnlock()
	out := make([]RecentPrioritizationFee, len(pf.blocks))
	for i, bf := range pf.blocks {
		out[i] = RecentPrioritizationFee{Slot: bf.slot, PrioritizationFee: bf.fee(keys)}
	}
	return out
}

func compareUint64(x, y uint64) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	}
	return 0
}

// GetRecentPrioritizationFees returns, for each recently replayed block, the
// lowest compute unit price (in micro-lamports) a non-vote transaction paid to
// land in it. With addresses, each block reports the highest of that and the
// lowest price paid by a transaction write-locking any of them. Like Agave,
// it covers replayed blocks, including ones not yet confirmed.
func (rpcServer *RpcServer) GetRecentPrioritizationFees(ctx context.Context, p jsonrpc.RawParams) ([]RecentPrioritizationFee, error) {
	if !rpcServer.PrioritizationFeesEnabled() {
		return nil, &methodNotServedError{method: "getRecentPrioritizationFees", level: "wallet"}
	}
	params, err := decodeParams(p)
	if err != nil {
		return nil, err
	}
	var keys []solana.PublicKey
	if len(params) > 0 && params[0] != nil {
		list, ok := params[0].([]any)
		if !ok {
			return nil, &InvalidParamsError{Message: "Invalid params: expected an array of account addresses"}
		}
		if len(list) > maxPrioritizationFeeAccounts {
			return nil, &InvalidParamsError{Message: fmt.Sprintf("Too many inputs provided; max %d", maxPrioritizationFeeAccounts)}
		}
		for _, v := range list {
			s, _ := v.(string)
			key, err := solana.PublicKeyFromBase58(s)
			if err != nil {
				return nil, &InvalidParamsError{Message: fmt.Sprintf("Invalid param: %v", v)}
			}
			keys = append(keys, key)
		}
	}
	return rpcServer.priorityFees.recent(keys), nil
}
