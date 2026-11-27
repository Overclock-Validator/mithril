package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

// A query for what one transaction did, answered by replaying privately from
// the fold boundary before its slot up to and including its block.
//
// The blocks before the target run with this node's stored hints where it has
// them. The target block runs in full for the target transaction (a recorded
// failure is executed so its real error and logs come back) and its metadata
// is captured. Every replayed block's bank hash is compared with the one the
// node recorded; a result is returned only if all of them match. If a hinted
// replay disagrees anywhere, the query runs again without hints.

var (
	// ErrSlotNotCheckpointed means the slot is newer than the last fold or
	// older than the oldest fold boundary still kept.
	ErrSlotNotCheckpointed = errors.New("the slot is not inside a fold this node can replay from")
	// ErrNoBlockAtSlot means the node recorded no block at the slot.
	ErrNoBlockAtSlot = errors.New("this node recorded no block at the slot")
	// ErrTransactionNotInBlock means the block does not hold the signature.
	ErrTransactionNotInBlock = errors.New("the block at the slot does not contain the transaction")
)

// CheckpointQuerier answers transaction queries from fold checkpoints and
// this node's spooled shreds.
type CheckpointQuerier struct {
	DB        *accountsdb.AccountsDb
	Persisted *state.MithrilState
	// ShredDir holds the spooled shred files of the slots to replay.
	ShredDir string
	// Workers is the transaction parallelism of the private replay.
	Workers int
	// NoHints replays every block as declared (measurement and fallback).
	NoHints bool
	// KeepFolds, when above zero, keeps that many most recently queried
	// folds replayed in memory (FoldReplay). The first query in a fold then
	// replays the whole fold; later ones execute only their own block.
	KeepFolds int

	kept keptFolds
}

// TransactionReplay is what a transaction did, as replayed.
type TransactionReplay struct {
	Slot      uint64
	Signature solana.Signature
	// Boundary is the fold boundary the replay started from.
	Boundary uint64
	// BlocksReplayed counts the blocks executed, the target's included.
	BlocksReplayed int

	Fee                  uint64
	Error                json.RawMessage // nil when the transaction succeeded
	ComputeUnitsConsumed uint64
	AccountKeys          []solana.PublicKey
	PreBalances          []uint64
	PostBalances         []uint64
	LogMessages          []string
	InnerInstructions    []InnerInstructionsList
	ReturnData           *TransactionReturnData

	// FromKeptFold is set when the answer came from a fold kept in memory;
	// BuiltFold when this query was the one that replayed and kept it.
	FromKeptFold, BuiltFold bool
	// KeptFoldBytes estimates the memory the kept fold holds.
	KeptFoldBytes uint64
	// UsedHints reports whether stored hints sped the replay up; FellBack is
	// set when a hinted attempt disagreed and the query ran again without.
	UsedHints, FellBack bool
	// Setup is opening the checkpoint and building the starting bank; Replay
	// is executing the blocks; Total is the whole query.
	Setup, Replay, Total time.Duration
}

// ReplayTransaction replays up to the transaction's block and returns what it
// did there.
func (q *CheckpointQuerier) ReplayTransaction(ctx context.Context, slot uint64, signature solana.Signature) (*TransactionReplay, error) {
	if q == nil || q.DB == nil || q.Persisted == nil || q.ShredDir == "" || q.Workers < 1 {
		return nil, errors.New("checkpoint query needs a store, the node's persisted state, a shred directory and workers")
	}
	start := time.Now()
	attempt := q.attempt
	if q.KeepFolds > 0 {
		attempt = q.attemptKept
	}
	result, err := attempt(ctx, slot, signature, !q.NoHints)
	if err != nil && !q.NoHints && ctx.Err() == nil && (errors.Is(err, ErrReplayHintsDisagree) || errors.Is(err, errBankHashDiffers)) {
		// Without hints, and without a kept fold a hinted replay built.
		result, err = q.attempt(ctx, slot, signature, false)
		if result != nil {
			result.FellBack = true
		}
	}
	if result != nil {
		result.Total = time.Since(start)
	}
	return result, err
}

var errBankHashDiffers = errors.New("a replayed block's bank hash differs from the recorded one")

// boundaryBefore returns the latest fold boundary before slot.
func (q *CheckpointQuerier) boundaryBefore(slot uint64) (uint64, error) {
	boundaries, err := q.DB.CheckpointBoundaries()
	if err != nil {
		return 0, err
	}
	at := sort.Search(len(boundaries), func(i int) bool { return boundaries[i] >= slot })
	if at == 0 {
		return 0, ErrSlotNotCheckpointed
	}
	return boundaries[at-1], nil
}

// attemptKept answers from the kept replay of the slot's fold, building it
// first if this is the fold's first query.
func (q *CheckpointQuerier) attemptKept(ctx context.Context, slot uint64, signature solana.Signature, hints bool) (*TransactionReplay, error) {
	boundary, err := q.boundaryBefore(slot)
	if err != nil {
		return nil, err
	}
	fold, built, err := q.kept.get(boundary, q.KeepFolds, func() (*FoldReplay, error) { return q.buildFold(ctx, boundary, hints) })
	if err != nil {
		return nil, err
	}
	if len(fold.slots) == 0 || fold.slots[len(fold.slots)-1] < slot {
		return nil, ErrSlotNotCheckpointed
	}
	result, err := fold.transaction(ctx, q, slot, signature, hints)
	if result != nil {
		result.BuiltFold, result.KeptFoldBytes = built, fold.Bytes
	}
	return result, err
}

// take copies the captured transaction into the result.
func (result *TransactionReplay) take(capture *blockMetadataCapture) error {
	if capture.Err != nil {
		return fmt.Errorf("capture the transaction: %w", capture.Err)
	}
	if capture.Record == nil || len(capture.Record.Transactions) == 0 {
		return ErrTransactionNotInBlock
	}
	record := capture.Record.Transactions[0]
	result.Fee, result.ComputeUnitsConsumed = record.Fee, record.ComputeUnitsConsumed
	if len(record.Error) > 0 && string(record.Error) != "null" {
		result.Error = record.Error
	}
	result.AccountKeys, result.PreBalances, result.PostBalances = record.AccountKeys, record.PreBalances, record.PostBalances
	result.LogMessages, result.InnerInstructions, result.ReturnData = record.LogMessages, record.InnerInstructions, record.ReturnData
	return nil
}

func (q *CheckpointQuerier) attempt(ctx context.Context, slot uint64, signature solana.Signature, hints bool) (*TransactionReplay, error) {
	start := time.Now()
	boundary, err := q.boundaryBefore(slot)
	if err != nil {
		return nil, err
	}
	view, err := q.DB.OpenCheckpointView(boundary)
	if err != nil {
		return nil, err
	}
	fold, err := view.FoldAfter()
	if err != nil {
		return nil, err
	}
	if fold == nil || fold.ThroughSlot < slot {
		return nil, ErrSlotNotCheckpointed
	}
	var slots []uint64
	var recorded [][32]byte
	found := false
	for _, block := range fold.Bankhashes {
		if block.Slot > slot {
			break
		}
		slots = append(slots, block.Slot)
		recorded = append(recorded, block.Bankhash)
		found = block.Slot == slot
	}
	if !found {
		return nil, ErrNoBlockAtSlot
	}

	// Decoding the first blocks, warming the fold's changed accounts and
	// building the starting bank overlap. The replay does not wait for the
	// warming: an account it needs first it reads itself.
	work, stop := context.WithCancel(ctx)
	var warming sync.WaitGroup
	defer func() {
		stop()
		warming.Wait()
	}()
	decoded := DecodeSpooledBlocksAhead(work, q.ShredDir, slots, spooledDecodeAhead)
	replay, err := NewCheckpointReplay(ctx, q.DB, view, q.Persisted, q.Workers)
	if err != nil {
		return nil, err
	}
	replay.UseHints(hints)
	if len(slots) > queryWarmFromBlocks {
		warming.Add(1)
		go func() {
			defer warming.Done()
			replay.tail.warm(work, fold.Keys, queryWarmChunk, queryWarmWorkers)
		}()
	}
	result := &TransactionReplay{Slot: slot, Signature: signature, Boundary: boundary}
	result.Setup = time.Since(start)

	replayStart := time.Now()
	for i, blockSlot := range slots {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		next, open := <-decoded
		if !open {
			return nil, ctx.Err()
		}
		if next.Err != nil {
			return nil, next.Err
		}
		var capture *blockMetadataCapture
		var execute *solana.Signature
		if blockSlot == slot {
			capture = &blockMetadataCapture{watch: watchSignature(signature)}
			execute = &signature
		}
		slotCtx, err := replay.next(next.Block, capture, execute)
		if err != nil {
			return nil, err
		}
		result.BlocksReplayed++
		if !bytes.Equal(slotCtx.FinalBankhash, recorded[i][:]) {
			return nil, fmt.Errorf("slot %d: %w", blockSlot, errBankHashDiffers)
		}
		if capture == nil {
			continue
		}
		if err := result.take(capture); err != nil {
			return nil, fmt.Errorf("slot %d: %w", slot, err)
		}
	}
	result.Replay = time.Since(replayStart)
	result.UsedHints = replay.HintedBlocks > 0
	return result, nil
}

const (
	// A query for one of a fold's first blocks touches few of the fold's
	// changed accounts; warming them all would cost more than it saves.
	queryWarmFromBlocks = 8
	queryWarmChunk      = 4096
	queryWarmWorkers    = 2
)
