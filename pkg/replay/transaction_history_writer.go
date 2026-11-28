package replay

import (
	"context"
	"errors"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
)

// TransactionHistorySession allows one capture or write at a time. Its methods
// belong to the replay goroutine; the worker receives only the sealed record.
// Its caller owns it across ReplayBlocks retries and stops it before closing
// the store. The configured observer mode must remain unchanged while replay
// uses it.
type TransactionHistorySession struct {
	write   func(transactionhistory.Block) error
	current *blockMetadataCapture
	pending *transactionHistoryWrite
	stopped bool
	err     error
}

type transactionHistoryWrite struct {
	done chan struct{}
	err  error
	slot uint64
}

// NewTransactionHistorySession borrows a store for explicitly non-voting replay.
// The node must reject ineligible modes before opening that store; this check
// cannot undo a prior Open. A nil store leaves history disabled.
func NewTransactionHistorySession(store *transactionhistory.Store, consensusMode string) (*TransactionHistorySession, error) {
	if store == nil {
		return nil, nil
	}
	if consensusMode != "verifying" {
		return nil, errors.New("transaction history requires verifying mode")
	}
	return &TransactionHistorySession{write: func(block transactionhistory.Block) error {
		err := store.WriteBlock(block)
		if state, stateErr := store.Stats(); stateErr == nil {
			statsd.Gauge(statsd.TransactionHistoryBlocks, float64(state.Blocks), nil)
			statsd.Gauge(statsd.TransactionHistoryBytes, float64(state.LogicalBytes), nil)
			statsd.Gauge(statsd.TransactionHistoryFirstSlot, float64(state.FirstSlot), nil)
			statsd.Gauge(statsd.TransactionHistoryLastSlot, float64(state.LastSlot), nil)
		}
		return err
	}}, nil
}

// begin does not wait for storage. A busy writer skips the entire next capture.
func (w *TransactionHistorySession) begin() *blockMetadataCapture {
	if w == nil || w.poll() != nil || w.stopped {
		return nil
	}
	if w.current != nil || w.pending != nil {
		statsd.Count(statsd.TransactionHistoryCaptures, 1, []string{"busy"})
		return nil
	}
	statsd.Count(statsd.TransactionHistoryCaptures, 1, []string{"started"})
	w.current = &blockMetadataCapture{}
	return w.current
}

// finish consumes only the current capture. Acceptance must come from the
// authoritative block boundary, not merely from the presence of a sealed record.
// After transfer, the caller must not access the record or its nested buffers.
func (w *TransactionHistorySession) finish(capture *blockMetadataCapture, accepted bool) {
	if w == nil || capture == nil || capture != w.current {
		return
	}
	w.current = nil
	record := capture.Record
	capture.Record, capture.records = nil, nil
	if !accepted || capture.Err != nil || record == nil {
		result := "discarded"
		if accepted || capture.Err != nil {
			result = "capture_refused"
		}
		statsd.Count(statsd.TransactionHistoryCaptures, 1, []string{result})
		return
	}
	job := &transactionHistoryWrite{done: make(chan struct{}), slot: record.Slot}
	w.pending = job
	statsd.Gauge(statsd.TransactionHistoryPendingWriteStarted, float64(time.Now().UnixNano())/1e9, nil)
	write := w.write
	go func() {
		defer close(job.done)
		defer statsd.Gauge(statsd.TransactionHistoryPendingWriteStarted, 0, nil)
		started := time.Now()
		defer func() { _ = statsd.Duration(statsd.TransactionHistoryWrite, time.Since(started), nil) }()
		block, err := record.historyBlock()
		if err == nil {
			err = write(block)
		}
		job.err = err
	}()
}

// poll retires a finished write. Conversion or storage failures stop recording;
// retention pressure and per-block capture refusals allow later attempts.
func (w *TransactionHistorySession) poll() error {
	if w == nil {
		return nil
	}
	if w.pending != nil {
		select {
		case <-w.pending.done:
			result := "written"
			if errors.Is(w.pending.err, transactionhistory.ErrRetention) {
				// Retention pressure omits this block, but later captures can retry.
				result = "retention_refused"
			} else if w.pending.err != nil {
				result = "write_failed"
				if w.err == nil {
					w.err = w.pending.err
					mlog.Log.Errorf("transaction history recording stopped at slot %d: %v", w.pending.slot, w.err)
				}
			}
			statsd.Count(statsd.TransactionHistoryCaptures, 1, []string{result})
			w.pending = nil
		default:
		}
	}
	return w.err
}

// wait paces observer replay behind the previous write without admitting another
// record. Cancellation releases replay, not the borrowed store or its worker.
func (w *TransactionHistorySession) wait(ctx context.Context) error {
	if w == nil || w.err != nil || w.pending == nil {
		return w.poll()
	}
	select {
	case <-w.pending.done:
		return w.poll()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop prevents admission and joins an already transferred write. joined is false
// only when the context ends before completion; the caller must then keep the
// store open and call Stop again. A joined write may still return a storage error.
// Pebble Sync cannot be cancelled. Stop never closes the caller's store.
func (w *TransactionHistorySession) Stop(ctx context.Context) (joined bool, err error) {
	if w == nil {
		return true, nil
	}
	w.stopped = true
	w.discard()
	if w.pending != nil {
		select {
		case <-w.pending.done:
			return true, w.poll()
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return true, w.err
}

func (w *TransactionHistorySession) discard() {
	if w != nil {
		w.finish(w.current, false)
	}
}

// acceptBlock is called only after the replay loop's synchronous acceptance
// gates. Later verification, canonical selection and rooting remain separate.
func (w *TransactionHistorySession) acceptBlock(block *b.Block) {
	if w == nil {
		return
	}
	capture := w.current
	accepted := capture != nil && capture.Record != nil && block != nil &&
		capture.Record.Slot == block.Slot && capture.Record.Blockhash == block.Blockhash
	if capture != nil && capture.Record != nil && !accepted {
		w.disable(errors.New("transaction history capture does not match accepted block"))
		return
	}
	w.finish(capture, accepted)
}

func (w *TransactionHistorySession) disable(err error) {
	if w == nil {
		return
	}
	w.stopped = true
	w.discard()
	if w.err == nil {
		w.err = err
		statsd.Count(statsd.TransactionHistoryCaptures, 1, []string{"capture_failed"})
		mlog.Log.Errorf("transaction history recording stopped: %v", err)
	}
}

func (w *TransactionHistorySession) validateReplay(opts *ConsensusOpts, fetch *BlockFetchOpts) error {
	if w == nil {
		return nil
	}
	if w.write == nil || !initializedObserverReplay(opts) {
		return errors.New("transaction history requires an initialized Alpenglow observer session")
	}
	if observerReplayHasProduction(opts, fetch) {
		return errors.New("transaction history does not support voting or local block production")
	}
	return nil
}
