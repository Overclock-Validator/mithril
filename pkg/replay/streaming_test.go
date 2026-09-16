package replay

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// fakeStreamFeed stands in for the block source's view of the turbine feed.
type fakeStreamFeed struct {
	events      chan turbine.StreamEvent
	status      map[turbine.StreamGeneration]turbine.StreamStatus
	pending     map[turbine.StreamGeneration][]*turbine.StreamBatch
	prioritized []uint64
}

func newFakeStreamFeed() *fakeStreamFeed {
	return &fakeStreamFeed{
		events:  make(chan turbine.StreamEvent, 64),
		status:  make(map[turbine.StreamGeneration]turbine.StreamStatus),
		pending: make(map[turbine.StreamGeneration][]*turbine.StreamBatch),
	}
}

func (f *fakeStreamFeed) StreamEvents() <-chan turbine.StreamEvent { return f.events }

func (f *fakeStreamFeed) StreamStatusOf(g turbine.StreamGeneration) turbine.StreamStatus {
	if status, ok := f.status[g]; ok {
		return status
	}
	return turbine.StreamGone
}

func (f *fakeStreamFeed) PendingStreamBatches(g turbine.StreamGeneration, fromStart uint32) []*turbine.StreamBatch {
	var out []*turbine.StreamBatch
	for _, batch := range f.pending[g] {
		if batch.Start >= fromStart {
			out = append(out, batch)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}

func (f *fakeStreamFeed) PrioritizeStreamRepair(slot uint64) {
	f.prioritized = append(f.prioritized, slot)
}

// fakeUnrootedState satisfies the tail interface for eligibility checks; no
// method is ever called on it.
type fakeUnrootedState struct{ unrootedState }

// verifiedIdentities runs the real batch verifier so the batches carry the
// identities the assembler's verifier would attach.
func verifiedIdentities(t *testing.T, txs []*solana.Transaction) []txverify.VerifiedMessageIdentity {
	t.Helper()
	var verifier txverify.BatchVerifier
	errs := make([]error, len(txs))
	identities := make([]txverify.VerifiedMessageIdentity, len(txs))
	verifier.VerifyWithMessageIdentities(txs, errs, identities)
	for i, err := range errs {
		require.NoError(t, err, "fixture transaction %d must verify", i)
	}
	return identities
}

// streamingTestHarness is an executor with a stream already open on the group
// execution environment's bank (slot 42 on parent 41), which is what
// openStream would have produced without the bank machinery.
type streamingTestHarness struct {
	env      *groupExecutionEnv
	feed     *fakeStreamFeed
	exec     *streamingExecutor
	gen      turbine.StreamGeneration
	parentID solana.Hash
	frontier uint64
	lastCtx  *sealevel.SlotCtx
}

func newStreamingTestHarness(t *testing.T) *streamingTestHarness {
	t.Helper()
	env := newGroupExecutionEnv(t, 2, 10_000_000_000)
	t.Cleanup(env.cleanup)
	feed := newFakeStreamFeed()
	h := &streamingTestHarness{env: env, feed: feed, parentID: solana.Hash{7, 7, 7}, frontier: 41}
	h.gen = turbine.NewDetachedStreamGeneration(env.exec.block.Slot)
	feed.status[h.gen] = turbine.StreamActive
	h.lastCtx = &sealevel.SlotCtx{Slot: 41, Epoch: env.exec.block.Epoch}
	epochSchedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000, FirstNormalEpoch: 0, FirstNormalSlot: 0}
	h.exec = newStreamingExecutor(streamingDeps{
		feed:                feed,
		epochSchedule:       &epochSchedule,
		tail:                fakeUnrootedState{},
		transactionStatuses: NewTransactionStatusCache(),
		alpenglowMode:       true,
		unrootedTailUsed:    true,
		lastSlotCtx:         func() *sealevel.SlotCtx { return h.lastCtx },
		frontier:            func() uint64 { return h.frontier },
		currentFeatures:     func() *features.Features { return env.exec.block.Features },
		currentEpoch:        func() uint64 { return env.exec.block.Epoch },
		rewardsInFlight:     func() bool { return false },
		switchPending:       func() bool { return false },
		executedBlockID: func(slot uint64) (solana.Hash, bool) {
			if slot == 41 {
				return h.parentID, true
			}
			return solana.Hash{}, false
		},
	})
	env.exec.parentBankSysvars = &sealevel.BankSysvars{}
	h.open()
	return h
}

// open installs the stream the way openStream does after its bank opened.
func (h *streamingTestHarness) open() {
	h.exec.current = &streamingSlot{
		slot:       h.env.exec.block.Slot,
		generation: h.gen,
		parentSlot: 41,
		parentID:   h.parentID,
		exec:       h.env.exec,
		pending:    make(map[uint32]*turbine.StreamBatch),
		headerAt:   time.Now(),
		openedAt:   time.Now(),
	}
	h.exec.handleEvent(h.event(h.header()))
}

func (h *streamingTestHarness) header() *turbine.StreamBatch {
	return turbine.NewDetachedStreamMarker(h.gen, 0, 0, turbine.StreamMarkerHeader, 41, h.parentID)
}

func (h *streamingTestHarness) batch(t *testing.T, start, end uint32, txs []*solana.Transaction) *turbine.StreamBatch {
	t.Helper()
	return turbine.NewDetachedStreamBatch(h.gen, start, end, txs, verifiedIdentities(t, txs))
}

func (h *streamingTestHarness) event(batch *turbine.StreamBatch) turbine.StreamEvent {
	return turbine.StreamEvent{Kind: turbine.StreamBatchReady, Slot: batch.Slot, Generation: batch.Generation, Batch: batch}
}

// executed returns the block objects the stream has executed (in order); the
// bank itself ran stream-owned copies, which are checked to be copies of
// exactly those objects.
func (h *streamingTestHarness) executed() []*solana.Transaction {
	if h.exec.current == nil {
		return nil
	}
	return h.exec.current.origin
}

// sameCopies asserts that executed holds fresh copies of want, in order: the
// same signatures and static keys, never the same objects, and never sharing
// the originals' account-key storage.
func sameCopies(t *testing.T, want, executed []*solana.Transaction) {
	t.Helper()
	require.Len(t, executed, len(want))
	for i := range want {
		require.NotSame(t, want[i], executed[i], "transaction %d must be a copy", i)
		require.Equal(t, want[i].Signatures, executed[i].Signatures, "transaction %d signatures", i)
		require.Equal(t, want[i].Message.GetVersion(), executed[i].Message.GetVersion())
		require.Equal(t, want[i].Message.RecentBlockhash, executed[i].Message.RecentBlockhash)
		if want[i].Message.GetVersion() == solana.MessageVersionV0 {
			require.False(t, want[i].Message.IsResolved(), "transaction %d: the block's object must stay untouched", i)
		}
		if len(want[i].Message.AccountKeys) > 0 {
			require.NotSame(t, &want[i].Message.AccountKeys[0], &executed[i].Message.AccountKeys[0], "transaction %d shares account-key storage", i)
		}
	}
}

func (h *streamingTestHarness) discardReason() string {
	return metrics.GlobalBlockReplay.StreamingExecution.DiscardReason
}

func sameTransactions(t *testing.T, want, got []*solana.Transaction) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.Same(t, want[i], got[i], "transaction %d", i)
	}
}

func TestStreamingConsumeExecutesContiguousGroupsInOrder(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newStreamingTestHarness(t)
	txs := transferTransactions(t, 9, 500)
	a, bb, c := h.batch(t, 1, 3, txs[0:3]), h.batch(t, 4, 6, txs[3:6]), h.batch(t, 7, 9, txs[6:9])

	require.Equal(t, uint32(1), h.exec.current.nextStart, "header consumed")
	h.exec.handleEvent(h.event(bb))
	require.Empty(t, h.executed(), "a gap before the batch holds it")
	h.exec.handleEvent(h.event(a))
	sameTransactions(t, txs[0:6], h.executed())
	sameCopies(t, txs[0:6], h.env.exec.transactions)
	require.Len(t, h.exec.current.groups, 1, "contiguous batches execute as one group")
	require.Equal(t, uint32(7), h.exec.current.nextStart)

	// Duplicate wake-ups for consumed or pending ranges are ignored.
	h.exec.handleEvent(h.event(a))
	h.exec.handleEvent(h.event(bb))
	sameTransactions(t, txs[0:6], h.executed())

	h.exec.handleEvent(h.event(c))
	sameTransactions(t, txs, h.executed())
	sameCopies(t, txs, h.env.exec.transactions)
	require.Len(t, h.exec.current.groups, 2)
	require.Equal(t, uint32(10), h.exec.current.nextStart)
	require.Equal(t, uint64(9), h.env.exec.processedTxCount)
}

func TestStreamingTickPullsBatchesMissedByDroppedWakeups(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true, MaxOpenAge: time.Minute}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newStreamingTestHarness(t)
	txs := transferTransactions(t, 6, 600)
	h.feed.pending[h.gen] = []*turbine.StreamBatch{h.batch(t, 4, 6, txs[3:6]), h.batch(t, 1, 3, txs[0:3])}
	h.exec.handleTick()
	sameTransactions(t, txs, h.executed())
	require.Len(t, h.exec.current.groups, 1)
}

func TestStreamingConsumeHoldsUntilMinGroupBatches(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true, MinGroupBatches: 2}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newStreamingTestHarness(t)
	txs := transferTransactions(t, 9, 700)
	a, bb, c := h.batch(t, 1, 3, txs[0:3]), h.batch(t, 4, 6, txs[3:6]), h.batch(t, 7, 9, txs[6:9])

	h.exec.handleEvent(h.event(a))
	require.Empty(t, h.executed(), "one batch is below the group minimum")
	require.Equal(t, uint32(1), h.exec.current.nextStart, "held batch is put back")
	h.exec.handleEvent(h.event(bb))
	sameTransactions(t, txs[0:6], h.executed())

	h.exec.handleEvent(h.event(c))
	require.Len(t, h.executed(), 6, "a lone trailing batch waits")
	h.exec.handleEvent(turbine.StreamEvent{Kind: turbine.StreamCompleted, Slot: c.Slot, Generation: h.gen})
	require.True(t, h.exec.current.completed)
	sameTransactions(t, txs, h.executed())
}

func TestStreamingDiscardsOnUpdateParentDecodeErrorAndUnverified(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()

	h := newStreamingTestHarness(t)
	txs := transferTransactions(t, 3, 800)
	h.exec.handleEvent(h.event(h.batch(t, 1, 3, txs)))
	sameTransactions(t, txs, h.executed())
	update := turbine.NewDetachedStreamMarker(h.gen, 4, 4, turbine.StreamMarkerUpdateParent, 40, solana.Hash{1})
	h.exec.handleEvent(h.event(update))
	require.Nil(t, h.exec.current)
	require.True(t, h.env.exec.closed, "discard closes the execution")
	require.Equal(t, "update_parent", h.discardReason())
	require.Equal(t, uint64(1), metrics.GlobalBlockReplay.StreamingExecution.Discarded)

	h = newStreamingTestHarness(t)
	broken := h.batch(t, 1, 3, txs)
	broken.Err = errors.New("bad entry")
	h.exec.handleEvent(h.event(broken))
	require.Nil(t, h.exec.current)
	require.Equal(t, "decode_error", h.discardReason())

	h = newStreamingTestHarness(t)
	unverified := turbine.NewDetachedStreamBatch(h.gen, 1, 3, txs, nil)
	_, verified, err := unverified.WaitVerification(context.Background())
	require.NoError(t, err)
	require.False(t, verified)
	h.exec.handleEvent(h.event(unverified))
	require.Nil(t, h.exec.current, "a batch the verifier did not admit is never self-verified")
	require.Equal(t, "unverified_batch", h.discardReason())
	require.Empty(t, h.executed())
}

func TestStreamingGroupFailureDiscards(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newStreamingTestHarness(t)
	txs := transferTransactions(t, 4, 900)
	h.exec.executeFn = func(exec *blockExecution, group []*solana.Transaction, identities *b.PreparedTransactionMessageIdentities, shouldVerify bool) error {
		require.False(t, shouldVerify, "verified batches never re-verify")
		return &DuplicateTransactionMessagesError{Slot: exec.block.Slot, DuplicateCount: 1}
	}
	h.exec.handleEvent(h.event(h.batch(t, 1, 2, txs[:2])))
	require.Nil(t, h.exec.current)
	require.Equal(t, "duplicate_message", h.discardReason())
}

func TestStreamingIgnoresOtherGenerationsAndHonoursCancellation(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newStreamingTestHarness(t)
	txs := transferTransactions(t, 3, 1000)
	other := turbine.NewDetachedStreamGeneration(h.env.exec.block.Slot)
	foreign := turbine.NewDetachedStreamBatch(other, 1, 3, txs, verifiedIdentities(t, txs))
	h.exec.handleEvent(turbine.StreamEvent{Kind: turbine.StreamBatchReady, Slot: foreign.Slot, Generation: other, Batch: foreign})
	require.Empty(t, h.executed(), "another generation's batches are not this stream's")
	require.NotNil(t, h.exec.current)

	h.exec.handleEvent(turbine.StreamEvent{Kind: turbine.StreamCancelled, Slot: foreign.Slot, Generation: other, Reason: "reset"})
	require.NotNil(t, h.exec.current, "another generation's cancellation is ignored")

	h.exec.handleEvent(turbine.StreamEvent{Kind: turbine.StreamCancelled, Slot: h.env.exec.block.Slot, Generation: h.gen, Reason: "reset"})
	require.Nil(t, h.exec.current)
	require.Equal(t, "cancelled:reset", h.discardReason())
}

func TestStreamingTickEnforcesStatusAndAge(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true, MaxOpenAge: 50 * time.Millisecond}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()

	h := newStreamingTestHarness(t)
	h.feed.status[h.gen] = turbine.StreamGone
	h.exec.handleTick()
	require.Nil(t, h.exec.current)
	require.Equal(t, "gone", h.discardReason())

	h = newStreamingTestHarness(t)
	h.exec.current.openedAt = time.Now().Add(-time.Second)
	h.exec.handleTick()
	require.Nil(t, h.exec.current)
	require.Equal(t, "timeout", h.discardReason())

	h = newStreamingTestHarness(t)
	h.feed.status[h.gen] = turbine.StreamDone
	h.exec.current.openedAt = time.Now().Add(-100 * time.Millisecond)
	h.exec.handleTick()
	require.NotNil(t, h.exec.current, "a completed stream outlives MaxOpenAge while its block is emitted")
	require.True(t, h.exec.current.completed)
	h.exec.current.openedAt = time.Now().Add(-time.Minute)
	h.exec.handleTick()
	require.Nil(t, h.exec.current, "but not the hard cap")
	require.Equal(t, "timeout", h.discardReason())
}

func TestStreamingDiscardUndoesGlobalSideEffects(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newStreamingTestHarness(t)
	slot := h.env.exec.block.Slot
	slotCtx := h.env.exec.slotCtx
	slotCtx.DeferVoteCachePublication = true
	voteKey := solana.PublicKey{9}
	putVoteCacheItem(slotCtx, voteKey, &sealevel.VoteStateVersions{})
	markSlotVoteStakeDirty(slotCtx)
	require.Nil(t, global.VoteCacheItem(voteKey), "deferred put stays off the global cache")
	before := len(global.PendingStakeEntriesSnapshot())
	global.EnqueuePendingStakePubkey(slot, solana.PublicKey{8})
	require.Len(t, global.PendingStakeEntriesSnapshot(), before+1)

	h.exec.discard("test")
	require.Nil(t, slotCtx.PendingVoteCache)
	require.False(t, slotCtx.VoteStakeDirty)
	require.Nil(t, global.VoteCacheItem(voteKey))
	require.Len(t, global.PendingStakeEntriesSnapshot(), before, "the stream's stake index entries are dropped")
	require.False(t, h.exec.matches(slot))
	require.Nil(t, h.exec.tick(), "no poll timer while idle")
}

func TestStreamingEligibility(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newStreamingTestHarness(t)
	h.exec.discard("reset for eligibility")
	header := h.header()
	require.Equal(t, "", h.exec.eligibility(header))

	cases := []struct {
		name   string
		mutate func()
		want   string
	}{
		{"generation gone", func() { h.feed.status[h.gen] = turbine.StreamGone }, "generation no longer active"},
		{"generation done", func() { h.feed.status[h.gen] = turbine.StreamDone }, "generation no longer active"},
		{"no parent context", func() { h.lastCtx = nil }, "no executed parent context"},
		{"frontier moved", func() { h.frontier = 42 }, "slot 42 on parent 41 does not extend the executed frontier 42 (parent context 41)"},
		{"parent is not the executed slot", func() { h.lastCtx = &sealevel.SlotCtx{Slot: 40} }, "slot 42 on parent 41 does not extend the executed frontier 41 (parent context 40)"},
		{"parent id mismatch", func() { h.parentID = solana.Hash{1} }, "parent block id does not match the executed parent"},
		{"switch pending", func() { h.exec.deps.switchPending = func() bool { return true } }, "fork switch pending"},
		{"epoch boundary", func() { h.exec.deps.currentEpoch = func() uint64 { return 99 } }, "epoch boundary"},
		{"rewards", func() { h.exec.deps.rewardsInFlight = func() bool { return true } }, "partitioned rewards in flight"},
		{"no features", func() { h.exec.deps.currentFeatures = func() *features.Features { return nil } }, "no feature set"},
		{"no tail", func() { h.exec.deps.tail = nil }, "requires alpenglow rooted-durable replay"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			saved := *h
			savedDeps := h.exec.deps
			savedStatus := h.feed.status[h.gen]
			tc.mutate()
			require.Equal(t, tc.want, h.exec.eligibility(header))
			*h = saved
			h.exec.deps = savedDeps
			h.feed.status[h.gen] = savedStatus
		})
	}
}

func TestStreamingRememberHeaderAndTryOpenBounds(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newStreamingTestHarness(t)
	h.exec.discard("idle")

	stale := turbine.NewDetachedStreamMarker(turbine.NewDetachedStreamGeneration(41), 0, 0, turbine.StreamMarkerHeader, 40, solana.Hash{})
	h.exec.handleEvent(h.event(stale))
	require.Empty(t, h.exec.headers, "headers at or below the frontier are not kept")

	ahead := turbine.NewDetachedStreamMarker(turbine.NewDetachedStreamGeneration(44), 0, 0, turbine.StreamMarkerHeader, 43, solana.Hash{})
	h.exec.handleEvent(h.event(ahead))
	require.Contains(t, h.exec.headers, uint64(44), "a header ahead of the frontier waits for its parent")
	require.Nil(t, h.exec.current)

	// The next slot's header is ineligible (its generation is unknown to the
	// feed), so it is dropped rather than opened; nothing else changes.
	next := turbine.NewDetachedStreamMarker(turbine.NewDetachedStreamGeneration(42), 0, 0, turbine.StreamMarkerHeader, 41, h.parentID)
	h.exec.handleEvent(h.event(next))
	require.Nil(t, h.exec.current)
	require.NotContains(t, h.exec.headers, uint64(42))
	require.Contains(t, h.exec.headers, uint64(44))

	h.frontier = 44
	h.exec.handleTick()
	require.Empty(t, h.exec.headers, "advancing the frontier past a remembered header drops it")

	StreamingExecutionCfg.Enabled = false
	h.frontier = 41
	h.exec.headers[42] = next
	h.exec.tryOpen()
	require.Contains(t, h.exec.headers, uint64(42), "disabled: nothing opens")
}

// A dropped header wake-up is recovered from the assembler by the next
// wake-up for the generation (the pending list is authoritative after a
// drop); the usual open path follows. The lookup is only made for the slot
// within the bounded lookahead, never for a generation this executor retired, and only finds
// a header that is decoded.
func TestStreamingRecoversHeaderFromPendingAfterDroppedWakeup(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newStreamingTestHarness(t)
	txs := transferTransactions(t, 3, 700)
	g := turbine.NewDetachedStreamGeneration(42)
	header := turbine.NewDetachedStreamMarker(g, 0, 0, turbine.StreamMarkerHeader, 41, h.parentID)
	batch := turbine.NewDetachedStreamBatch(g, 1, 3, txs, verifiedIdentities(t, txs))
	h.feed.pending[g] = []*turbine.StreamBatch{batch, header}

	h.exec.recoverHeader(42, g)
	require.Empty(t, h.exec.headers, "the open stream's own slot is not looked up (its successor is; see below)")

	h.exec.discard("idle")
	require.Equal(t, h.gen, h.exec.retired[42], "the discarded generation is retired")
	h.exec.recoverHeader(42, g)
	require.Same(t, header, h.exec.headers[42], "the batch wake-up recovered the decoded header")

	// Declined once (the feed does not know the generation, so it is
	// ineligible), the generation is retired and no later wake-up brings it
	// back; whole-block execution owns the slot.
	h.exec.tryOpen()
	require.Nil(t, h.exec.current)
	require.Empty(t, h.exec.headers)
	require.Equal(t, g, h.exec.retired[42])
	h.exec.recoverHeader(42, g)
	require.Empty(t, h.exec.headers, "a retired generation is never recovered")

	// A new generation of the slot (after a reset) is recoverable again, but
	// only once its header is decoded.
	renewed := turbine.NewDetachedStreamGeneration(42)
	renewedHeader := turbine.NewDetachedStreamMarker(renewed, 0, 0, turbine.StreamMarkerHeader, 41, h.parentID)
	h.feed.pending[renewed] = []*turbine.StreamBatch{turbine.NewDetachedStreamBatch(renewed, 1, 3, txs, verifiedIdentities(t, txs))}
	h.exec.recoverHeader(42, renewed)
	require.Empty(t, h.exec.headers, "a header that is not decoded yet cannot be recovered")
	h.feed.pending[renewed] = append(h.feed.pending[renewed], renewedHeader)
	h.exec.recoverHeader(42, renewed)
	require.Same(t, renewedHeader, h.exec.headers[42])
	delete(h.exec.headers, 42)

	ahead := turbine.NewDetachedStreamGeneration(44)
	h.feed.pending[ahead] = []*turbine.StreamBatch{turbine.NewDetachedStreamMarker(ahead, 0, 0, turbine.StreamMarkerHeader, 43, solana.Hash{})}
	h.exec.recoverHeader(44, ahead)
	require.Contains(t, h.exec.headers, uint64(44), "nearby header can be recovered before its parent is ready")
	delete(h.exec.headers, 44)

	h.exec.recoverHeader(42, turbine.StreamGeneration{})
	require.Empty(t, h.exec.headers, "a zero generation has nothing pending")

	// While a stream is open, its successor is the slot that could open next.
	h.frontier = 42
	h.exec.pruneHeaders(h.frontier)
	require.Empty(t, h.exec.retired, "retirements at or below the frontier are pruned")
	h.frontier = 41
	h.open()
	successor := turbine.NewDetachedStreamGeneration(43)
	successorHeader := turbine.NewDetachedStreamMarker(successor, 0, 0, turbine.StreamMarkerHeader, 42, solana.Hash{1})
	h.feed.pending[successor] = []*turbine.StreamBatch{successorHeader}
	h.exec.recoverHeader(43, successor)
	require.Same(t, successorHeader, h.exec.headers[43], "the open stream's successor is recoverable")
	require.NotNil(t, h.exec.current, "the open stream is untouched")
}

func (h *streamingTestHarness) matchingBlock(t *testing.T, txs []*solana.Transaction) *b.Block {
	t.Helper()
	shell := h.env.exec.block
	block := &b.Block{
		Slot:                      shell.Slot,
		Epoch:                     shell.Epoch,
		ParentSlot:                shell.ParentSlot,
		ParentBankhash:            shell.ParentBankhash,
		Features:                  shell.Features,
		FromLiveStream:            true,
		SourceParentSlot:          41,
		AlpenglowParentBlockID:    h.parentID,
		HasAlpenglowParentBlockID: true,
		Transactions:              txs,
	}
	return block
}

func TestStreamingHandshake(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newStreamingTestHarness(t)
	txs := transferTransactions(t, 4, 1100)
	h.exec.handleEvent(h.event(h.batch(t, 1, 3, txs[:3])))
	sameTransactions(t, txs[:3], h.executed())
	sysvars := h.env.exec.parentBankSysvars

	require.Equal(t, "", h.exec.handshake(h.matchingBlock(t, txs), sysvars), "the block extends the executed prefix")
	require.Equal(t, "", h.exec.handshake(h.matchingBlock(t, txs[:3]), sysvars), "the block may end exactly at the prefix")

	cases := []struct {
		name   string
		mutate func(block *b.Block)
		want   string
	}{
		{"slot", func(block *b.Block) { block.Slot++ }, "slot"},
		{"skipped", func(block *b.Block) { block.IsSkipped = true }, "not a live block"},
		{"rpc block", func(block *b.Block) { block.FromLiveStream = false }, "not a live block"},
		{"parent id", func(block *b.Block) { block.AlpenglowParentBlockID = [32]byte{1} }, "parent"},
		{"parent slot", func(block *b.Block) { block.SourceParentSlot = 40 }, "parent"},
		{"configured parent", func(block *b.Block) { block.ParentBankhash = [32]byte{2} }, "configured parent"},
		{"features", func(block *b.Block) { block.Features = features.NewFeaturesDefault() }, "features"},
		{"epoch", func(block *b.Block) { block.Epoch++ }, "epoch"},
		{"epoch accounts", func(block *b.Block) { block.EpochUpdatedAccts = append(block.EpochUpdatedAccts, nil) }, "epoch account updates"},
		{"shorter", func(block *b.Block) { block.Transactions = block.Transactions[:2] }, "shorter than executed prefix"},
		{"different transaction", func(block *b.Block) {
			replacement := transferTransactions(t, 1, 1101)[0] // same bytes as txs[1], different object
			block.Transactions[1] = replacement
		}, "transaction 1 identity"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			block := h.matchingBlock(t, append([]*solana.Transaction(nil), txs...))
			tc.mutate(block)
			require.Equal(t, tc.want, h.exec.handshake(block, sysvars))
		})
	}
	require.Equal(t, "parent sysvars", h.exec.handshake(h.matchingBlock(t, txs), &sealevel.BankSysvars{}))
	require.Equal(t, "parent sysvars", h.exec.handshake(h.matchingBlock(t, txs), nil))
	h.feed.status[h.gen] = turbine.StreamGone
	require.Equal(t, "generation gone", h.exec.handshake(h.matchingBlock(t, txs), sysvars))
}

func TestStreamingFinalizeFallsBackOnMismatch(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newStreamingTestHarness(t)
	txs := transferTransactions(t, 3, 1200)
	h.exec.handleEvent(h.event(h.batch(t, 1, 3, txs)))
	block := h.matchingBlock(t, txs)
	block.Transactions[0] = transferTransactions(t, 1, 1200)[0]

	slotCtx, ok, err := h.exec.finalize(block, h.env.exec.parentBankSysvars)
	require.NoError(t, err)
	require.False(t, ok, "the caller must execute the block whole")
	require.Nil(t, slotCtx)
	require.Nil(t, h.exec.current)
	require.Equal(t, "prefix_mismatch:transaction 0 identity", h.discardReason())
	require.True(t, h.env.exec.closed)

	_, ok, err = h.exec.finalize(block, nil)
	require.NoError(t, err)
	require.False(t, ok, "no stream, nothing to finalize")
	require.False(t, h.exec.matches(block.Slot))
}

// finalizeFailureHarness prepares a stream whose executed prefix matches the
// block (three of four transfers executed) and instruments every undo hook,
// so a post-handshake failure can be checked for the discard contract.
type finalizeFailureHarness struct {
	*streamingTestHarness
	txs           []*solana.Transaction
	restores      int
	voteKey       solana.PublicKey
	stakeBefore   int
	openedPending int
}

func newFinalizeFailureHarness(t *testing.T) *finalizeFailureHarness {
	t.Helper()
	h := &finalizeFailureHarness{streamingTestHarness: newStreamingTestHarness(t), voteKey: solana.PublicKey{3, 3, 3}}
	h.txs = transferTransactions(t, 4, 1300)
	h.exec.handleEvent(h.event(h.batch(t, 1, 3, h.txs[:3])))
	sameTransactions(t, h.txs[:3], h.executed())
	h.exec.current.restoreSysvarCache = func() { h.restores++ }
	slotCtx := h.env.exec.slotCtx
	slotCtx.DeferVoteCachePublication = true
	slotCtx.TrackProgramCacheAdds = true
	putVoteCacheItem(slotCtx, h.voteKey, &sealevel.VoteStateVersions{})
	slotCtx.RecordProgramCacheAdd(solana.PublicKey{4})
	h.stakeBefore = len(global.PendingStakeEntriesSnapshot())
	global.EnqueuePendingStakePubkey(h.env.exec.block.Slot, solana.PublicKey{5})
	return h
}

func (h *finalizeFailureHarness) assertUndone(t *testing.T, reason string) {
	t.Helper()
	require.Nil(t, h.exec.current, "the stream no longer owns a bank")
	require.True(t, h.env.exec.closed, "the execution is closed")
	require.Equal(t, reason, h.discardReason())
	require.Equal(t, 1, h.restores, "the legacy sysvar cache is restored once")
	require.Nil(t, global.VoteCacheItem(h.voteKey), "unpublished vote-cache entries never reach the global cache")
	require.Nil(t, h.env.exec.slotCtx.PendingVoteCache)
	require.Empty(t, h.env.exec.slotCtx.TakeProgramCacheAdds(), "tracked program-cache adds were consumed by the undo")
	require.Len(t, global.PendingStakeEntriesSnapshot(), h.stakeBefore, "the slot's stake index entries are dropped")
	require.Nil(t, h.exec.tick())
}

func TestStreamingFinalizeSuffixFailureUndoesTheStream(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newFinalizeFailureHarness(t)
	suffixErr := errors.New("suffix exploded")
	calls := 0
	h.exec.executeFn = func(exec *blockExecution, group []*solana.Transaction, identities *b.PreparedTransactionMessageIdentities, shouldVerify bool) error {
		calls++
		sameTransactions(t, h.txs[3:], group)
		require.Equal(t, 1, identities.Len())
		require.True(t, shouldVerify, "an unmarked block's suffix is verified like any whole block")
		return suffixErr
	}
	block := h.matchingBlock(t, h.txs)

	slotCtx, ok, err := h.exec.finalize(block, h.env.exec.parentBankSysvars)
	require.True(t, ok, "the handshake passed: the failure is the block's")
	require.Nil(t, slotCtx)
	require.ErrorIs(t, err, suffixErr)
	var final *streamingFinalizeError
	require.ErrorAs(t, err, &final)
	require.Equal(t, 1, calls)
	h.assertUndone(t, "finalize:suffix")
}

func TestStreamingFinalizeFooterRejectionUndoesTheStream(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newFinalizeFailureHarness(t)
	// An Alpenglow bank requires the footer before it executes the suffix; a
	// live block without one is rejected exactly as ProcessBlock rejects it.
	h.exec.deps.alpenglowClock = true
	h.env.exec.block.Features.EnableFeature(features.AlpenglowDevContext, 0)
	h.exec.executeFn = func(*blockExecution, []*solana.Transaction, *b.PreparedTransactionMessageIdentities, bool) error {
		t.Fatal("the suffix must not execute after a footer rejection")
		return nil
	}
	block := h.matchingBlock(t, h.txs)
	require.False(t, block.HasAlpenglowFooter)

	slotCtx, ok, err := h.exec.finalize(block, h.env.exec.parentBankSysvars)
	require.True(t, ok)
	require.Nil(t, slotCtx)
	require.ErrorContains(t, err, "missing block footer")
	h.assertUndone(t, "finalize:footer_clock")
}

func TestStreamingFinalizeProcessedCountMismatchUndoesTheStream(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newFinalizeFailureHarness(t)
	// A suffix that "succeeds" without recording its transactions leaves the
	// executed counts short of the whole-block plan.
	h.exec.executeFn = func(*blockExecution, []*solana.Transaction, *b.PreparedTransactionMessageIdentities, bool) error {
		return nil
	}
	block := h.matchingBlock(t, h.txs)

	_, ok, err := h.exec.finalize(block, h.env.exec.parentBankSysvars)
	require.True(t, ok)
	require.ErrorContains(t, err, "processed 3 transactions")
	h.assertUndone(t, "finalize:counts")
}

// recordingStreamer is the wait loop's view of an executor.
type recordingStreamer struct {
	ch      chan turbine.StreamEvent
	tickCh  chan time.Time
	handled []turbine.StreamEvent
	ticks   int
}

func (r *recordingStreamer) events() <-chan turbine.StreamEvent { return r.ch }
func (r *recordingStreamer) tick() <-chan time.Time             { return r.tickCh }
func (r *recordingStreamer) handleEvent(e turbine.StreamEvent)  { r.handled = append(r.handled, e) }
func (r *recordingStreamer) handleTick()                        { r.ticks++ }

func TestWaitForReplayInputDispatchesStreamWakeups(t *testing.T) {
	streamer := &recordingStreamer{ch: make(chan turbine.StreamEvent, 4), tickCh: make(chan time.Time, 4)}
	block := &b.Block{Slot: 5}
	script := []blockstream.ReplayInput{
		{StreamEvent: &turbine.StreamEvent{Kind: turbine.StreamBatchReady, Slot: 6}},
		{StreamTick: true},
		{DecisionChanged: true},
		{StreamEvent: &turbine.StreamEvent{Kind: turbine.StreamCompleted, Slot: 6}},
		{Block: block},
	}
	var seenEvents []<-chan turbine.StreamEvent
	var seenTicks []<-chan time.Time
	next := func(ctx context.Context, decisionChanges <-chan struct{}, events <-chan turbine.StreamEvent, tick <-chan time.Time) blockstream.ReplayInput {
		seenEvents = append(seenEvents, events)
		seenTicks = append(seenTicks, tick)
		in := script[0]
		script = script[1:]
		return in
	}
	sweeps := 0
	sweep := func() *CertifiedSwitch { sweeps++; return nil }
	got, parentSwitch, sw := waitForReplayInput(context.Background(), next, sweep, make(chan struct{}), time.Second, streamer)
	require.Same(t, block, got)
	require.Nil(t, parentSwitch)
	require.Nil(t, sw)
	require.Len(t, streamer.handled, 2, "feed wake-ups are handled and never end the wait")
	require.Equal(t, turbine.StreamCompleted, streamer.handled[1].Kind)
	require.Equal(t, 2, streamer.ticks, "one tick on entry, one from the timer")
	require.Equal(t, 5, sweeps, "every wait is preceded by a sweep")
	for _, ch := range seenEvents {
		require.Equal(t, (<-chan turbine.StreamEvent)(streamer.ch), ch)
	}
	for _, ch := range seenTicks {
		require.Equal(t, (<-chan time.Time)(streamer.tickCh), ch)
	}
}

func TestWaitForReplayInputStreamerWithoutSweepBlocksWithoutPolling(t *testing.T) {
	streamer := &recordingStreamer{ch: make(chan turbine.StreamEvent, 1)}
	calls := 0
	next := func(ctx context.Context, decisionChanges <-chan struct{}, events <-chan turbine.StreamEvent, tick <-chan time.Time) blockstream.ReplayInput {
		calls++
		require.Nil(t, decisionChanges, "decision wake-ups stay disabled without a sweep")
		_, hasDeadline := ctx.Deadline()
		require.False(t, hasDeadline, "no poll timeout without a sweep")
		if calls == 1 {
			return blockstream.ReplayInput{StreamTick: true}
		}
		return blockstream.ReplayInput{}
	}
	block, parentSwitch, sw := waitForReplayInput(context.Background(), next, nil, make(chan struct{}), time.Second, streamer)
	require.Nil(t, block)
	require.Nil(t, parentSwitch)
	require.Nil(t, sw)
	require.Equal(t, 2, calls)
	require.Equal(t, 2, streamer.ticks)
}

func TestWaitForReplayInputWithoutStreamerIsUnchanged(t *testing.T) {
	block := &b.Block{Slot: 9}
	next := func(ctx context.Context, decisionChanges <-chan struct{}, events <-chan turbine.StreamEvent, tick <-chan time.Time) blockstream.ReplayInput {
		require.Nil(t, events)
		require.Nil(t, tick)
		require.Nil(t, decisionChanges)
		return blockstream.ReplayInput{Block: block}
	}
	got, _, sw := waitForReplayInput(context.Background(), next, nil, make(chan struct{}), time.Second, nil)
	require.Same(t, block, got)
	require.Nil(t, sw)

	var nilStreamer *streamingExecutor
	require.Nil(t, nilStreamer.tick())
	require.Nil(t, nilStreamer.events())
	require.False(t, nilStreamer.matches(1))
	nilStreamer.handleTick()
	nilStreamer.discard("noop")
	nilStreamer.discardSlot(1, "noop")
	nilStreamer.shutdown()
	_, ok, err := nilStreamer.finalize(block, nil)
	require.False(t, ok)
	require.NoError(t, err)
}

func TestStreamingConfigDefaults(t *testing.T) {
	var cfg StreamingExecutionConfig
	require.Equal(t, defaultStreamingWorkers, cfg.workers(0))
	require.Equal(t, 2, cfg.workers(2))
	require.Equal(t, defaultStreamingWorkers, cfg.workers(64))
	cfg.Workers = 8
	require.Equal(t, 8, cfg.workers(0))
	require.Equal(t, 3, cfg.workers(3))
	require.Equal(t, defaultStreamingMaxAge, cfg.maxOpenAge())
	cfg.MaxOpenAge = time.Second
	require.Equal(t, time.Second, cfg.maxOpenAge())
}

func TestStreamingGapSelectionAndSafety(t *testing.T) {
	h := newStreamingTestHarness(t)
	defer h.exec.shutdown()
	makeHeader := func(slot, parent uint64, id solana.Hash) *turbine.StreamBatch {
		g := turbine.NewDetachedStreamGeneration(slot)
		h.feed.status[g] = turbine.StreamActive
		return turbine.NewDetachedStreamMarker(g, 0, 0, turbine.StreamMarkerHeader, parent, id)
	}
	gap := makeHeader(46, 41, h.parentID)
	h.exec.headers[46] = gap
	h.exec.headers[48] = makeHeader(48, 41, h.parentID)
	h.exec.headers[44] = makeHeader(44, 43, h.parentID)
	require.Same(t, gap, h.exec.nextHeader(h.frontier), "earliest direct child, not a grandchild")
	require.Empty(t, h.exec.eligibility(gap))
	require.NotEmpty(t, h.exec.eligibility(makeHeader(46, 41, solana.Hash{99})))
	require.NotEmpty(t, h.exec.eligibility(makeHeader(46, 40, h.parentID)))
	require.NotEmpty(t, h.exec.eligibility(makeHeader(74, 41, h.parentID)), "lookahead is bounded")
	h.exec.deps.switchPending = func() bool { return true }
	require.Equal(t, "fork switch pending", h.exec.eligibility(gap))
	h.exec.deps.switchPending = func() bool { return false }
	h.exec.headers[42] = makeHeader(42, 41, h.parentID)
	require.Equal(t, uint64(42), h.exec.nextHeader(h.frontier).Slot)

	// A late real bank in the unresolved gap must restore all speculative
	// state before that bank is validated, configured or executed.
	h.exec.current.slot = 46
	cur := h.exec.current
	h.exec.beforeBlock(&b.Block{Slot: 42, IsSkipped: true})
	require.Same(t, cur, h.exec.current)
	h.exec.beforeBlock(&b.Block{Slot: 42})
	require.Nil(t, h.exec.current)
	require.True(t, cur.exec.closed)
	require.Equal(t, h.gen, h.exec.retired[46])
}

// The open timeline attributes every millisecond between the header's decode
// and the open to exactly one of: the parent's arrival (header decoded before
// the parent's last shred), the parent's replay (last shred → replay result),
// or the loop (nothing left to wait for, still not opened).
func TestStreamingTimelineAttributesOpenDelay(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	block := &b.Block{ShredFullNanos: at(300).UnixNano()}
	record := func(cur *streamingSlot) metrics.StreamingExecution {
		var out metrics.StreamingExecution
		cur.recordTimeline(&out, block, at(320))
		return out
	}
	ms := func(timing metrics.Timing) float64 { return float64(timing.SumNanoseconds) / 1e6 }

	// Header decoded 50 ms before the parent's last shred, parent admitted
	// 20 ms after that and replayed 10 ms later, opened 5 ms after; the
	// header wake-up was handled 10 ms after decode.
	cur := &streamingSlot{
		headerAt: at(0), headerSeenAt: at(10), openedAt: at(85),
		parentFullNanos: at(50).UnixNano(), parentAdmittedAt: at(70), parentReplayedAt: at(80),
		groups: []streamingGroup{{startedAt: at(90), finishedAt: at(120), transactions: 3}},
	}
	r := record(cur)
	require.Equal(t, at(0).UnixNano(), r.HeaderReadyNanos)
	require.Equal(t, at(10).UnixNano(), r.HeaderSeenNanos)
	require.Equal(t, at(50).UnixNano(), r.ParentFullNanos)
	require.Equal(t, at(70).UnixNano(), r.ParentAdmittedNanos)
	require.Equal(t, at(80).UnixNano(), r.ParentReplayedNanos)
	require.Equal(t, at(85).UnixNano(), r.OpenedNanos)
	require.Equal(t, at(90).UnixNano(), r.FirstGroupStartNanos)
	require.Equal(t, at(300).UnixNano(), r.FullNanos)
	require.Equal(t, at(320).UnixNano(), r.FinalizeStartNanos)
	require.Equal(t, 50.0, ms(r.OpenWaitParentArrival))
	require.Equal(t, 30.0, ms(r.OpenWaitParentReplay))
	require.Equal(t, 20.0, ms(r.OpenWaitParentQueue), "the parent sat in the source 20 ms after its last shred")
	require.Equal(t, 10.0, ms(r.OpenWaitParentExec))
	require.Equal(t, 5.0, ms(r.OpenWaitLoop))
	require.Equal(t, uint64(1), r.OpenWaitLoop.Count)

	// The parent was admitted before the child's header was decoded (its
	// queueing cannot have held the child): the replay wait is all execution.
	cur = &streamingSlot{headerAt: at(0), headerSeenAt: at(1), openedAt: at(40), parentFullNanos: at(-30).UnixNano(), parentAdmittedAt: at(-5), parentReplayedAt: at(30)}
	r = record(cur)
	require.Zero(t, r.OpenWaitParentQueue.Count)
	require.Equal(t, 30.0, ms(r.OpenWaitParentExec))
	require.Equal(t, 30.0, ms(r.OpenWaitParentReplay))

	// Parent fully received before the header was even decoded: no arrival
	// wait; the parent's replay wait starts at the header.
	cur = &streamingSlot{headerAt: at(0), headerSeenAt: at(1), openedAt: at(40), parentFullNanos: at(-20).UnixNano(), parentReplayedAt: at(30)}
	r = record(cur)
	require.Zero(t, r.OpenWaitParentArrival.Count)
	require.Equal(t, 30.0, ms(r.OpenWaitParentReplay))
	require.Equal(t, 10.0, ms(r.OpenWaitLoop))

	// Header handled only after the parent was replayed (the wake-up sat in
	// the channel): the loop wait runs from the header being seen.
	cur = &streamingSlot{headerAt: at(0), headerSeenAt: at(90), openedAt: at(95), parentFullNanos: at(20).UnixNano(), parentReplayedAt: at(60)}
	r = record(cur)
	require.Equal(t, 20.0, ms(r.OpenWaitParentArrival))
	require.Equal(t, 40.0, ms(r.OpenWaitParentReplay))
	require.Equal(t, 5.0, ms(r.OpenWaitLoop))

	// The loop wait splits at the first wait entry after the parent: a header
	// remembered while the parent executed waited through the parent's
	// post-replay tail (replayed → wait entry) and then its dispatch (wait
	// entry → opened).
	cur = &streamingSlot{headerAt: at(0), headerSeenAt: at(5), openedAt: at(130), parentFullNanos: at(-100).UnixNano(), parentReplayedAt: at(60), waitEnteredAt: at(125)}
	r = record(cur)
	require.Equal(t, at(125).UnixNano(), r.WaitEnteredNanos)
	require.Equal(t, 70.0, ms(r.OpenWaitLoop))
	require.Equal(t, 65.0, ms(r.OpenWaitPostReplay))
	require.Equal(t, 5.0, ms(r.OpenWaitDispatch))

	// A header seen only after the wait entry (it arrived while the loop was
	// already waiting) was not held by the tail: dispatch only, from seen.
	cur = &streamingSlot{headerAt: at(0), headerSeenAt: at(140), openedAt: at(141), parentFullNanos: at(-100).UnixNano(), parentReplayedAt: at(60), waitEnteredAt: at(125)}
	r = record(cur)
	require.Zero(t, r.OpenWaitPostReplay.Count)
	require.Equal(t, 1.0, ms(r.OpenWaitDispatch))
	require.Equal(t, 1.0, ms(r.OpenWaitLoop))
	require.Contains(t, cur.openTimeline(), "wait entered +125.0ms")

	// Unknown parent instants (no mark, or a re-based frontier): only the
	// loop wait, from the header being seen.
	cur = &streamingSlot{headerAt: at(0), headerSeenAt: at(0), openedAt: at(70)}
	r = record(cur)
	require.Zero(t, r.ParentFullNanos)
	require.Zero(t, r.ParentReplayedNanos)
	require.Zero(t, r.OpenWaitParentArrival.Count)
	require.Zero(t, r.OpenWaitParentReplay.Count)
	require.Zero(t, r.OpenWaitParentQueue.Count)
	require.Zero(t, r.OpenWaitParentExec.Count)
	require.Equal(t, 70.0, ms(r.OpenWaitLoop))
	require.Zero(t, r.OpenWaitPostReplay.Count)
	require.Zero(t, r.OpenWaitDispatch.Count)
	require.Zero(t, r.WaitEnteredNanos)
	require.Zero(t, r.FirstGroupStartNanos, "no group ran")
	require.Contains(t, cur.openTimeline(), "parent full ?")
	require.Contains(t, cur.openTimeline(), "opened +70.0ms")
}

// A block executed whole says why no stream opened for it, from the
// executor's per-slot observation of its header.
func TestStreamingNoteWholeBlockReasons(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newStreamingTestHarness(t)
	reset := func() { metrics.GlobalBlockReplay.StreamingExecution = metrics.StreamingExecution{} }
	reason := func() string { return metrics.GlobalBlockReplay.StreamingExecution.NotOpenedReason }

	// Discarded stream, recorded on the observation (the record's own
	// Discarded/DiscardReason may or may not have survived the loop's
	// per-attempt reset; the observation always has it).
	reset()
	h.exec.observed[42] = &streamingObservation{generation: h.gen, parentSlot: 41, readyAt: time.Now(), seenAt: time.Now(), frontierAtSeen: 41}
	h.exec.discard("timeout")
	h.exec.noteWholeBlock(&b.Block{Slot: 42, ShredFullNanos: 123})
	require.Equal(t, "discarded:timeout", reason())
	require.Equal(t, int64(123), metrics.GlobalBlockReplay.StreamingExecution.FullNanos)
	require.Positive(t, metrics.GlobalBlockReplay.StreamingExecution.HeaderReadyNanos)
	require.Positive(t, metrics.GlobalBlockReplay.StreamingExecution.HeaderSeenNanos)

	// Never saw a header.
	reset()
	delete(h.exec.observed, 42)
	h.exec.noteWholeBlock(&b.Block{Slot: 42})
	require.Equal(t, "header_not_seen", reason())

	// Declined: the header's generation is unknown to the feed (gone).
	reset()
	declined := turbine.NewDetachedStreamMarker(turbine.NewDetachedStreamGeneration(42), 0, 0, turbine.StreamMarkerHeader, 41, h.parentID)
	h.exec.handleEvent(h.event(declined))
	require.Nil(t, h.exec.current)
	h.exec.noteWholeBlock(&b.Block{Slot: 42})
	require.Contains(t, reason(), "declined:generation no longer active")

	// Waiting: a header whose parent is not the executed frontier stays
	// remembered, and a whole-block execution of it reports what it waited on.
	reset()
	waiting := turbine.NewDetachedStreamMarker(turbine.NewDetachedStreamGeneration(44), 0, 0, turbine.StreamMarkerHeader, 43, solana.Hash{})
	h.exec.handleEvent(h.event(waiting))
	require.Contains(t, h.exec.headers, uint64(44))
	h.exec.noteWholeBlock(&b.Block{Slot: 44})
	require.Equal(t, "waiting_for_parent:header_on_parent_43_seen_at_frontier_41", reason())

	// Skips never report; a nil executor is a no-op.
	reset()
	h.exec.noteWholeBlock(&b.Block{Slot: 44, IsSkipped: true})
	require.Empty(t, reason())
	var none *streamingExecutor
	none.noteWholeBlock(&b.Block{Slot: 44})
	require.Empty(t, reason())

	// Observations are pruned with the frontier.
	h.frontier = 44
	h.exec.pruneHeaders(h.frontier)
	require.Empty(t, h.exec.observed)
}

// The per-group record splits verification waits and execution at the last
// shred, so the execution FullToReplayed paid for is visible separately
// from the work hidden behind reception.
func TestStreamingGroupRecordSplitsAtFull(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	ms := func(timing metrics.Timing) float64 { return float64(timing.SumNanoseconds) / 1e6 }
	cur := &streamingSlot{slot: 42, groups: []streamingGroup{
		{readyAt: at(90), verifiedAt: at(92), startedAt: at(92), finishedAt: at(120), batches: 3, transactions: 900},     // entirely before full
		{readyAt: at(250), verifiedAt: at(310), startedAt: at(310), finishedAt: at(340), batches: 9, transactions: 5000}, // waited 60 ms for the verifier, 10 of them after full; ran after full
		{readyAt: at(280), verifiedAt: at(281), startedAt: at(281), finishedAt: at(320), batches: 1, transactions: 300},  // straddles full
		{readyAt: at(340), verifiedAt: at(340), startedAt: at(340), finishedAt: at(350), transactions: 40, suffix: true},
	}}
	var r metrics.StreamingExecution
	cur.recordGroups(&r, at(300))
	require.Equal(t, 63.0, ms(r.GroupVerifyWait))
	require.Equal(t, uint64(3), r.GroupVerifyWait.Count, "the suffix has no verifier wait")
	require.Equal(t, 10.0, ms(r.GroupVerifyWaitAfterFull))
	require.Equal(t, uint64(1), r.GroupVerifyWaitAfterFull.Count)
	require.Equal(t, 60.0, ms(r.TxLoopAfterFull), "30 + 20 + 10 ms of execution after the last shred")
	require.Equal(t, uint64(3), r.TxLoopAfterFull.Count)
	require.Equal(t, uint64(1), r.GroupsStraddlingFull)
	require.Equal(t, uint64(5000), r.LargestGroupTransactions)
	require.Equal(t, uint64(9), r.LargestGroupBatches)
	require.Equal(t, at(350).UnixNano(), r.LastGroupEndNanos)
	line := cur.groupTimeline(at(300), 3)
	require.Contains(t, line, "[#0 b=3 tx=900 ready-210.0 verified-208.0 exec-208.0..-180.0]")
	require.Contains(t, line, "…(1 more)")
	require.Contains(t, line, "[#3 suffix b=0 tx=40 ready+40.0 verified+40.0 exec+40.0..+50.0]")
	require.NotContains(t, line, "#2 ")

	// No full instant (a block that did not arrive as shreds): nothing is
	// "after full", waits and sizes still count.
	var whole metrics.StreamingExecution
	cur.recordGroups(&whole, time.Time{})
	require.Equal(t, 63.0, ms(whole.GroupVerifyWait))
	require.Zero(t, whole.GroupVerifyWaitAfterFull.Count)
	require.Zero(t, whole.TxLoopAfterFull.Count)
	require.Zero(t, whole.GroupsStraddlingFull)
	require.Equal(t, uint64(5000), whole.LargestGroupTransactions)
	require.Equal(t, at(350).UnixNano(), whole.LastGroupEndNanos)
}
