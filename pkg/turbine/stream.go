package turbine

import (
	"context"
	"errors"
	"sort"
	"time"
	"weak"

	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

// Streaming feed: the assembler already decodes and signature-verifies every
// closed DATA_COMPLETE range of a slot while its shreds arrive (the entry
// prefetch). The feed exposes those batches, in the order they become ready,
// to one subscriber — replay's streaming executor — as immutable views, so the
// block can be executed while the rest of it is still in flight.
//
// The feed is advisory. Events are wake-ups: a full subscriber channel drops
// the event, and the subscriber recovers by asking PendingStreamBatches and
// StreamStatus, which read the assembler's own state under its lock. Nothing
// here changes how a slot completes, how its identity is attached, or how the
// complete block is emitted; the complete block remains the authority.

// StreamGeneration identifies one assembly of a slot. A slot that is reset
// and assembled again is a different generation. It is opaque: consumers
// compare it for equality and pass it back to the assembler.
type StreamGeneration struct {
	slot  uint64
	state *slotState
}

// Slot returns the generation's slot.
func (g StreamGeneration) Slot() uint64 { return g.slot }

// IsZero reports whether the generation was never set.
func (g StreamGeneration) IsZero() bool { return g.state == nil }

// NewDetachedStreamGeneration returns a non-zero generation for slot that no
// assembler knows about. It exists so consumers (replay's streaming executor)
// can unit-test their state machines with a fake feed; a real assembler
// reports it as StreamGone.
func NewDetachedStreamGeneration(slot uint64) StreamGeneration {
	return StreamGeneration{slot: slot, state: &slotState{slot: slot}}
}

// NewDetachedStreamBatch builds a ready entry-batch view for consumers' unit
// tests: txs are its transactions and identities, when non-nil, is a
// completed verification result for exactly those transactions (as
// txverify.BatchVerifier.VerifyWithMessageIdentities produces). With nil
// identities the batch reports itself unverified. The assembler never builds
// batches this way.
func NewDetachedStreamBatch(g StreamGeneration, start, end uint32, txs []*solana.Transaction, identities []txverify.VerifiedMessageIdentity) *StreamBatch {
	ready := make(chan struct{})
	close(ready)
	batch := &prefetchedShredBatch{start: start, end: end, ready: ready}
	if identities != nil {
		batch.verification = &transactionVerification{done: ready, cancel: func() {}, identities: identities, finishedAt: time.Now()}
	}
	view := newStreamBatch(g, batch)
	view.Transactions = txs
	return view
}

// NewDetachedStreamMarker builds a marker batch view (header, update-parent
// or footer) for consumers' unit tests.
func NewDetachedStreamMarker(g StreamGeneration, start, end uint32, kind StreamMarkerKind, parentSlot uint64, parentBlockID solana.Hash) *StreamBatch {
	ready := make(chan struct{})
	close(ready)
	view := newStreamBatch(g, &prefetchedShredBatch{start: start, end: end, ready: ready, marker: true})
	view.Marker = kind
	view.ParentSlot = parentSlot
	view.ParentBlockID = parentBlockID
	return view
}

// StreamMarkerKind classifies a batch that carries an Alpenglow block
// component instead of entries.
type StreamMarkerKind uint8

const (
	// StreamMarkerNone is an ordinary entry batch.
	StreamMarkerNone StreamMarkerKind = iota
	// StreamMarkerHeader is the block header (FEC set 0): parent slot and ID.
	StreamMarkerHeader
	// StreamMarkerUpdateParent selects an older parent and abandons every
	// batch before ReplayFECSetIndex (the optimistic prefix).
	StreamMarkerUpdateParent
	// StreamMarkerFooter is the block footer (certificates, bank hash, clock).
	StreamMarkerFooter
)

// StreamBatch is an immutable view of one decoded DATA_COMPLETE range. Its
// transactions are the same objects the complete block will reference when
// completion reuses this batch (byte-identical shreds), which is what lets a
// streaming consumer prove its executed prefix is the block by identity.
type StreamBatch struct {
	Slot       uint64
	Generation StreamGeneration
	Start, End uint32
	Marker     StreamMarkerKind
	// Parent fields are set for header and UpdateParent markers.
	ParentSlot        uint64
	ParentBlockID     solana.Hash
	ReplayFECSetIndex uint32
	// Transactions is empty for markers and for batches that failed to decode.
	Transactions []*solana.Transaction
	// Err is the decode error; a batch with Err makes the whole slot invalid.
	Err     error
	ReadyAt time.Time

	batch *prefetchedShredBatch
}

// ErrStreamBatchUnverified reports that no signature-verification result is
// attached to the batch (no transactions, or admission was refused); the
// consumer must verify signatures itself.
var ErrStreamBatchUnverified = errors.New("stream batch has no verification result")

// WaitVerification observes the batch's asynchronous signature verification and
// returns the verifier's message identities, one per transaction, bound to
// Transactions (see block.PrepareVerifiedTransactionMessageIdentities). A
// nil error with verified == false means no result is attached and the
// caller must verify itself; any other error means a signature failed (the
// slot is invalid) or ctx ended. Unlike the owning verifier wait, a context
// timeout returns without cancelling or joining the job: turbine retains the
// immutable transaction storage and joins readers before releasing reservations.
// A caller timing out must not mutate the batch or its transactions.
func (sb *StreamBatch) WaitVerification(ctx context.Context) (identities []txverify.VerifiedMessageIdentity, verified bool, err error) {
	if sb == nil || sb.batch == nil {
		return nil, false, ErrStreamBatchUnverified
	}
	if sb.batch.verification == nil {
		return nil, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	future := sb.batch.verification
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case <-future.done:
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if future.err != nil {
		return nil, false, future.err
	}
	if len(sb.batch.verification.identities) != len(sb.Transactions) {
		return nil, false, nil
	}
	return sb.batch.verification.identities, true, nil
}

// StreamEventKind is the kind of a feed wake-up.
type StreamEventKind uint8

const (
	// StreamBatchReady: Batch was decoded (and its verification submitted).
	StreamBatchReady StreamEventKind = iota
	// StreamCancelled: the generation's state is gone without a complete
	// block (reset, eviction, invalid identity, shutdown). Reason says why.
	StreamCancelled
	// StreamCompleted: the generation assembled and the complete block is on
	// its way through the normal emission path.
	StreamCompleted
)

// StreamEvent is an advisory wake-up. Call Resolve before inspecting Generation
// or Batch. Queued notifications hold only weak references, so a stalled
// subscriber cannot retain retired slot buffers outside the prefetch budget.
type StreamEvent struct {
	Kind       StreamEventKind
	Slot       uint64
	Generation StreamGeneration
	Batch      *StreamBatch
	Reason     string
	state      weak.Pointer[slotState]
	batch      weak.Pointer[prefetchedShredBatch]
}

// Resolve acquires ownership of a still-live notification. A false result means
// the opportunity has expired; whole-block replay remains authoritative. Resolved
// generations retain their state for polling, including after completion. Detached
// events supplied by test feeds are already resolved.
func (e StreamEvent) Resolve() (StreamEvent, bool) {
	if !e.Generation.IsZero() {
		return e, true
	}
	state := e.state.Value()
	if state == nil {
		return StreamEvent{}, false
	}
	e.Generation = StreamGeneration{slot: e.Slot, state: state}
	if e.Kind == StreamBatchReady {
		batch := e.batch.Value()
		if batch == nil {
			return StreamEvent{}, false
		}
		e.Batch = newStreamBatch(e.Generation, batch)
	}
	return e, true
}

// StreamStatus is the assembler's view of a generation.
type StreamStatus uint8

const (
	// StreamActive: the generation is the slot's current assembly.
	StreamActive StreamStatus = iota
	// StreamDone: the generation completed and its block was (or is being)
	// emitted.
	StreamDone
	// StreamGone: the generation was discarded without a block.
	StreamGone
)

// SubscribeStream installs the single feed subscriber. Events are sent
// without blocking; a full channel drops the event and counts it.
func (a *SlotAssembler) SubscribeStream(ch chan<- StreamEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.streamSubscriber = ch
	if ch == nil {
		a.streamRepairParent = nil
		a.streamRepairChild = nil
		a.streamRepairInvalidChild = nil
	}
}

// StreamDroppedEvents reports wake-ups dropped because the subscriber was
// full; the subscriber polls PendingStreamBatches after any wake-up, so a
// non-zero count is a sizing hint, not a correctness problem.
func (a *SlotAssembler) StreamDroppedEvents() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.streamDroppedEvents
}

func (a *SlotAssembler) publishStreamLocked(event StreamEvent) {
	if a.streamSubscriber == nil {
		return
	}
	select {
	case a.streamSubscriber <- event:
	default:
		a.streamDroppedEvents++
	}
}

// StreamStatusOf reports whether a generation is still the slot's current
// assembly, completed into a block, or gone.
func (a *SlotAssembler) StreamStatusOf(g StreamGeneration) StreamStatus {
	if g.state == nil {
		return StreamGone
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.streamStatusLocked(g)
}

func (a *SlotAssembler) streamStatusLocked(g StreamGeneration) StreamStatus {
	if a.slots[g.slot] == g.state {
		// Failed completions retain state for diagnostics. Polling must still
		// see cancellation when the bounded event channel dropped its wake-up.
		if g.state.streamCancelReason != "" {
			return StreamGone
		}
		return StreamActive
	}
	if g.state.streamCompleted {
		return StreamDone
	}
	return StreamGone
}

// PendingStreamBatches returns every decoded batch of the generation whose
// range starts at or after fromStart, in shred-index order. It reads the
// prefetch state directly, so it is the authoritative recovery path after a
// dropped wake-up. A completed generation still owns its immutable ready
// results, so completion does not hide batches behind queued/lost notifications.
// Cancelled generations return nothing. No new prefetch work is scheduled here.
func (a *SlotAssembler) PendingStreamBatches(g StreamGeneration, fromStart uint32) []*StreamBatch {
	if g.state == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	status := a.streamStatusLocked(g)
	if status == StreamGone || g.state.prefetch == nil || (g.state.prefetch.released && status != StreamDone) {
		return nil
	}
	var out []*StreamBatch
	for start, batch := range g.state.prefetch.batches {
		if start < fromStart {
			continue
		}
		select {
		case <-batch.ready:
		default:
			continue
		}
		out = append(out, newStreamBatch(g, batch))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}

// newStreamBatch builds the immutable view; it must only be called after the
// batch's ready channel closed (its fields are immutable from then on).
func newStreamBatch(g StreamGeneration, batch *prefetchedShredBatch) *StreamBatch {
	view := &StreamBatch{
		Slot:       g.slot,
		Generation: g,
		Start:      batch.start,
		End:        batch.end,
		Err:        batch.err,
		ReadyAt:    time.Now(),
		batch:      batch,
	}
	switch {
	case batch.err != nil:
	case batch.marker && batch.parent != nil:
		view.ParentSlot = batch.parent.ParentSlot
		view.ParentBlockID = batch.parent.ParentBlockID
		view.ReplayFECSetIndex = batch.parent.ReplayFECSetIndex
		if batch.parent.FromUpdateParent {
			view.Marker = StreamMarkerUpdateParent
		} else {
			view.Marker = StreamMarkerHeader
		}
	case batch.marker && batch.footer != nil:
		view.Marker = StreamMarkerFooter
	case batch.marker:
		// A marker without decoded content is treated like a footer-less
		// component boundary: nothing to execute, nothing to select.
		view.Marker = StreamMarkerFooter
	default:
		view.Transactions = batch.transactions
	}
	return view
}

// publishStreamBatchReady is called by the prefetch worker, under the
// assembler lock, after the batch's ready channel closed.
func (a *SlotAssembler) publishStreamBatchReadyLocked(s *slotState, batch *prefetchedShredBatch) {
	if a.streamSubscriber == nil || s == nil || batch == nil {
		return
	}
	a.noteChildRepairHeaderLocked(s, batch)
	a.publishStreamLocked(StreamEvent{Kind: StreamBatchReady, Slot: s.slot, state: weak.Make(s), batch: weak.Make(batch)})
}

// publishStreamReleaseLocked is called from releasePrefetchLocked, i.e. from
// every path that drops a slot generation, and tells the subscriber whether a
// complete block follows (finalizeCompletion marked it) or the state is gone.
func (a *SlotAssembler) publishStreamReleaseLocked(s *slotState, reason string) {
	if a.streamSubscriber == nil || s == nil {
		return
	}
	state := weak.Make(s)
	if s.streamCompleted {
		a.publishStreamLocked(StreamEvent{Kind: StreamCompleted, Slot: s.slot, state: state})
		return
	}
	a.publishStreamLocked(StreamEvent{Kind: StreamCancelled, Slot: s.slot, state: state, Reason: reason})
}
