package replay

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

// A FoldReplay is one fold replayed privately once and kept: every account
// version its blocks produced, and the bank after each block. A query for any
// transaction of the fold then executes only that transaction's block, on the
// state the kept replay had just before it, instead of replaying up to it.
//
// A kept fold is read-only once built, so any number of queries may use it at
// once. It holds about as much memory as the fold's blocks changed (every
// version of every changed account).
type FoldReplay struct {
	boundary uint64
	base     *CheckpointReplay
	// banks[i] is the bank after slots[i-1]; banks[0] is the boundary bank.
	slots    []uint64
	recorded [][32]byte
	banks    []foldBank
	position map[uint64]int

	// Built is how long the replay of the whole fold took; Bytes estimates
	// the memory the kept versions hold.
	Built time.Duration
	Bytes uint64
}

type foldBank struct {
	slotCtx     *sealevel.SlotCtx
	sysvars     *sealevel.BankSysvars
	blockHeight uint64
}

// bankShell copies what a child block takes from its parent bank. Two things
// in a bank are changed in place by the block built on it, the running
// account hash and the vote timestamps, so a kept bank and every use of it
// need their own copies; the rest is shared and never written.
func bankShell(bank *sealevel.SlotCtx) *sealevel.SlotCtx {
	shell := &sealevel.SlotCtx{
		Slot:                   bank.Slot,
		Epoch:                  bank.Epoch,
		FinalBankhash:          bank.FinalBankhash,
		VoteTimestamps:         maps.Clone(bank.VoteTimestamps),
		VoteAccts:              bank.VoteAccts,
		TotalEpochStake:        bank.TotalEpochStake,
		LatestEvictedBlockhash: bank.LatestEvictedBlockhash,
		FeeRateGovernor:        bank.FeeRateGovernor,
		NumSignatures:          bank.NumSignatures,
		Blockhash:              bank.Blockhash,
		Features:               bank.Features,
	}
	if bank.AcctsLtHash != nil {
		shell.AcctsLtHash = bank.AcctsLtHash.Clone()
	}
	if bank.FeeRateGovernor != nil {
		governor := *bank.FeeRateGovernor
		shell.FeeRateGovernor = &governor
	}
	return shell
}

// Boundary is the fold boundary the kept replay started from.
func (fold *FoldReplay) Boundary() uint64 { return fold.boundary }

// buildFold replays the whole fold after boundary and keeps it. Every bank
// hash is compared with the recorded one.
func (q *CheckpointQuerier) buildFold(ctx context.Context, boundary uint64, hints bool) (*FoldReplay, error) {
	start := time.Now()
	view, err := q.DB.OpenCheckpointView(boundary)
	if err != nil {
		return nil, err
	}
	fold, err := view.FoldAfter()
	if err != nil {
		return nil, err
	}
	if fold == nil || len(fold.Bankhashes) == 0 {
		return nil, ErrSlotNotCheckpointed
	}
	kept := &FoldReplay{boundary: boundary, position: make(map[uint64]int, len(fold.Bankhashes))}
	for i, block := range fold.Bankhashes {
		kept.slots = append(kept.slots, block.Slot)
		kept.recorded = append(kept.recorded, block.Bankhash)
		kept.position[block.Slot] = i
	}
	work, stop := context.WithCancel(ctx)
	var warming sync.WaitGroup
	defer func() {
		stop()
		warming.Wait()
	}()
	decoded := DecodeSpooledBlocksAhead(work, q.ShredDir, kept.slots, spooledDecodeAhead)
	replay, err := NewCheckpointReplay(ctx, q.DB, view, q.Persisted, q.Workers)
	if err != nil {
		return nil, err
	}
	replay.UseHints(hints)
	replay.tail.versions = make(map[solana.PublicKey][]accountVersion, len(fold.Keys))
	warming.Add(1)
	go func() {
		defer warming.Done()
		replay.tail.warm(work, fold.Keys, queryWarmChunk, queryWarmWorkers)
	}()
	kept.banks = append(kept.banks, foldBank{bankShell(replay.parent), replay.parentSysvars, replay.blockHeight})
	for i, slot := range kept.slots {
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
		slotCtx, err := replay.Next(next.Block)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(slotCtx.FinalBankhash, kept.recorded[i][:]) {
			return nil, fmt.Errorf("slot %d: %w", slot, errBankHashDiffers)
		}
		// Copied now: the next block changes parts of this bank in place.
		kept.banks = append(kept.banks, foldBank{bankShell(replay.parent), replay.parentSysvars, replay.blockHeight})
	}
	kept.base = replay
	kept.Bytes = replay.tail.overlay.RetainedBytes()
	kept.Built = time.Since(start)
	return kept, nil
}

// transaction executes the block at slot on the kept state before it and
// returns what the transaction did there.
func (fold *FoldReplay) transaction(ctx context.Context, q *CheckpointQuerier, slot uint64, signature solana.Signature, hints bool) (*TransactionReplay, error) {
	start := time.Now()
	at, ok := fold.position[slot]
	if !ok {
		return nil, ErrNoBlockAtSlot
	}
	packets, err := turbine.ReadSpoolFile(filepath.Join(q.ShredDir, turbine.SpoolFileName(slot)))
	if err != nil {
		return nil, fmt.Errorf("read spooled slot %d: %w", slot, err)
	}
	// Decoded afresh for every query: a replay changes the block it is given.
	block, err := turbine.BlockFromSpooledPackets(slot, packets)
	if err != nil {
		return nil, fmt.Errorf("decode spooled slot %d: %w", slot, err)
	}
	parent := fold.banks[at]
	parent.slotCtx = bankShell(parent.slotCtx) // this query's own copy
	replay := &CheckpointReplay{
		db: fold.base.db, tail: fold.base.tail.viewAfter(parent.slotCtx.Slot),
		epochSchedule: fold.base.epochSchedule, workers: q.Workers,
		statuses: NewTransactionStatusCache(), hashes: &persistedTracker{},
		parent: parent.slotCtx, parentSysvars: parent.sysvars, blockHeight: parent.blockHeight, epoch: fold.base.epoch,
		useHints: hints,
	}
	result := &TransactionReplay{Slot: slot, Signature: signature, Boundary: fold.boundary, FromKeptFold: true}
	result.Setup = time.Since(start)
	replayStart := time.Now()
	capture := &blockMetadataCapture{watch: watchSignature(signature)}
	slotCtx, err := replay.next(block, capture, &signature)
	if err != nil {
		return nil, err
	}
	result.BlocksReplayed = 1
	if !bytes.Equal(slotCtx.FinalBankhash, fold.recorded[at][:]) {
		return nil, fmt.Errorf("slot %d: %w", slot, errBankHashDiffers)
	}
	if err := result.take(capture); err != nil {
		return nil, fmt.Errorf("slot %d: %w", slot, err)
	}
	result.Replay = time.Since(replayStart)
	result.UsedHints = replay.HintedBlocks > 0
	return result, nil
}

// keptFolds is a small most-recently-used set of kept folds. A fold is built
// once even if several queries ask for it at the same time.
type keptFolds struct {
	mu      sync.Mutex
	entries map[uint64]*keptFold
	order   []uint64 // least recently used first
}

type keptFold struct {
	once sync.Once
	fold *FoldReplay
	err  error
}

func (kept *keptFolds) get(boundary uint64, limit int, build func() (*FoldReplay, error)) (fold *FoldReplay, built bool, err error) {
	kept.mu.Lock()
	if kept.entries == nil {
		kept.entries = make(map[uint64]*keptFold)
	}
	entry := kept.entries[boundary]
	if entry == nil {
		entry = &keptFold{}
		kept.entries[boundary] = entry
		for len(kept.order) >= limit && len(kept.order) > 0 {
			delete(kept.entries, kept.order[0])
			kept.order = kept.order[1:]
		}
	} else {
		for i, other := range kept.order {
			if other == boundary {
				kept.order = append(kept.order[:i], kept.order[i+1:]...)
				break
			}
		}
	}
	kept.order = append(kept.order, boundary)
	kept.mu.Unlock()

	entry.once.Do(func() {
		built = true
		entry.fold, entry.err = build()
		if entry.err != nil {
			// Do not keep a failure: the next query tries again.
			kept.mu.Lock()
			if kept.entries[boundary] == entry {
				delete(kept.entries, boundary)
				for i, other := range kept.order {
					if other == boundary {
						kept.order = append(kept.order[:i], kept.order[i+1:]...)
						break
					}
				}
			}
			kept.mu.Unlock()
		}
	})
	return entry.fold, built, entry.err
}
