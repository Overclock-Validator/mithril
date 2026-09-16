package replay

import (
	"context"
	"net"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/sigverify"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// The real-feed gate. A leader-side BroadcastSession shreds a header, entry
// batches (legacy and v0 transfers), a footer and the ending tick; the
// packets cross a loopback UDP socket into a real UDPReceiver (assembler,
// entry prefetch, signature verifier); the receiver's streaming feed drives
// the real streamingExecutor, which opens a bank on the lifecycle
// environment and executes the prefix while the slot is still incomplete;
// the receiver then emits the complete block, finalize matches it, and the
// result must equal whole-block replay of the same block — enforced twice:
// by the footer's expected bank hash inside finalize (the footer carries the
// whole-block reference hash) and by the explicit outcome comparison.

const (
	realFeedSlot          = lifecycleSlot
	realFeedParentSlot    = lifecycleParentSlot
	realFeedShredVersion  = uint16(7)
	realFeedTickHashByte  = 0xEE
	realFeedDriveDeadline = 20 * time.Second
)

// receiverFeed is the block source's view of the feed, backed by a receiver.
type receiverFeed struct {
	r      *turbine.UDPReceiver
	events chan turbine.StreamEvent
}

func (f *receiverFeed) StreamEvents() <-chan turbine.StreamEvent { return f.events }
func (f *receiverFeed) StreamStatusOf(g turbine.StreamGeneration) turbine.StreamStatus {
	return f.r.StreamStatusOf(g)
}
func (f *receiverFeed) PendingStreamBatches(g turbine.StreamGeneration, from uint32) []*turbine.StreamBatch {
	return f.r.PendingStreamBatches(g, from)
}
func (f *receiverFeed) PrioritizeStreamRepair(uint64) {}

type realFeedRig struct {
	slot        uint64
	t           *testing.T
	env         *lifecycleEnv
	receiver    *turbine.UDPReceiver
	feed        *receiverFeed
	broadcaster *turbine.UDPBroadcaster
	leader      solana.PrivateKey
	lastCtx     *sealevel.SlotCtx
	tail        *lifecycleTail
	statuses    *TransactionStatusCache
	exec        *streamingExecutor
	block       *b.Block
	// the slot's content, as wire bytes, decoded fresh for every use
	legacyWires, v0Wires [][]byte
}

func newRealFeedRig(t *testing.T, dest solana.PublicKey, eventBuffer int) *realFeedRig {
	t.Helper()
	previousOverlap := sigverify.Cfg.DisableShredOverlap
	sigverify.Cfg.DisableShredOverlap = false // the entry prefetch is what streams
	t.Cleanup(func() { sigverify.Cfg.DisableShredOverlap = previousOverlap })
	// The open-age bound is the loop's protection against a stalled slot; a
	// loaded CI host must not trip it between two drive calls.
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true, Workers: 2, MaxOpenAge: realFeedDriveDeadline}
	t.Cleanup(func() { StreamingExecutionCfg = StreamingExecutionConfig{} })
	// The per-block collector, as the loop resets it before every wait.
	previousMetrics := metrics.GlobalBlockReplay
	metrics.GlobalBlockReplay = metrics.BlockReplay{}
	t.Cleanup(func() { metrics.GlobalBlockReplay = previousMetrics })

	env := newLifecycleEnvWithTable(t, dest)
	rig := &realFeedRig{slot: realFeedSlot, t: t, env: env, leader: solana.NewWallet().PrivateKey}
	for i := 0; i < 4; i++ {
		rig.legacyWires = append(rig.legacyWires, txfixture.MustSignedTransferWire(uint64(2000+i)))
	}
	for i := 0; i < 3; i++ {
		rig.v0Wires = append(rig.v0Wires, signedV0TransferViaTableWire(t, uint64(30+i)))
	}

	// The executed parent, as the loop would hold it: its bank hash is the
	// child's ParentBankhash and its sysvar snapshot the child's parent.
	rig.lastCtx = &sealevel.SlotCtx{
		Slot:            realFeedParentSlot,
		Epoch:           0,
		FinalBankhash:   append([]byte{0x88}, make([]byte, 31)...),
		FeeRateGovernor: &sealevel.FeeRateGovernor{TargetLamportsPerSignature: 5_000, LamportsPerSignature: 5_000},
		VoteTimestamps:  map[solana.PublicKey]sealevel.BlockTimestamp{},
	}
	require.NoError(t, rig.lastCtx.PublishBankSysvars(env.parent))

	// A real receiver on a loopback port we pick ourselves (the receiver does
	// not report an ephemeral bind), fed by a real broadcaster.
	probe, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	bindAddr := probe.LocalAddr().String()
	require.NoError(t, probe.Close())
	receiver := turbine.NewUDPReceiver(bindAddr)
	receiver.SetShredVersion(realFeedShredVersion)
	leaderKey := rig.leader.PublicKey()
	receiver.SetLeaderForSlot(func(uint64) (solana.PublicKey, bool) { return leaderKey, true })
	rig.feed = &receiverFeed{r: receiver, events: make(chan turbine.StreamEvent, eventBuffer)}
	receiver.SubscribeStream(rig.feed.events)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { _ = receiver.Run(ctx); close(runDone) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
			t.Error("receiver did not stop")
		}
	})
	select {
	case err := <-receiver.Ready():
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("receiver did not become ready")
	}
	rig.receiver = receiver
	udpAddr, err := net.ResolveUDPAddr("udp", bindAddr)
	require.NoError(t, err)
	rig.broadcaster, err = turbine.NewUDPBroadcaster("127.0.0.1:0")
	require.NoError(t, err)
	rig.broadcaster.AddPeer(udpAddr)
	t.Cleanup(func() { _ = rig.broadcaster.Close() })

	rig.tail = &lifecycleTail{durable: env.durable}
	rig.statuses = NewTransactionStatusCache()
	rig.exec = newStreamingExecutor(streamingDeps{
		acctsDb:             env.acctsDb,
		feed:                rig.feed,
		epochSchedule:       env.epochSchedule,
		txParallelism:       2,
		persistedHashes:     &persistedTracker{},
		tail:                rig.tail,
		transactionStatuses: rig.statuses,
		alpenglowMode:       true,
		unrootedTailUsed:    true,
		lastSlotCtx:         func() *sealevel.SlotCtx { return rig.lastCtx },
		frontier:            func() uint64 { return realFeedParentSlot },
		currentFeatures:     func() *features.Features { return env.feats },
		currentEpoch:        func() uint64 { return 0 },
		rewardsInFlight:     func() bool { return false },
		switchPending:       func() bool { return false },
		executedBlockID:     func(uint64) (solana.Hash, bool) { return lifecycleParentBlockID, true },
	})
	t.Cleanup(rig.exec.shutdown)
	return rig
}

// entries decodes the slot's content into the two entry batches the leader
// broadcasts: legacy transfers, then v0 transfers through the lookup table.
func (rig *realFeedRig) entries() (legacy, v0 []turbine.Entry) {
	values := func(wires [][]byte) []solana.Transaction {
		out := make([]solana.Transaction, len(wires))
		for i, tx := range decodeWires(rig.t, wires) {
			out[i] = *tx
		}
		return out
	}
	return []turbine.Entry{{NumHashes: 1, Hash: solana.Hash{0x11}, Txns: values(rig.legacyWires)}},
		[]turbine.Entry{{NumHashes: 1, Hash: solana.Hash{0x22}, Txns: values(rig.v0Wires)}}
}

func (rig *realFeedRig) allWires() [][]byte {
	return append(append([][]byte(nil), rig.legacyWires...), rig.v0Wires...)
}

func decodeWires(t *testing.T, wires [][]byte) []*solana.Transaction {
	t.Helper()
	txs := make([]*solana.Transaction, len(wires))
	for i, wire := range wires {
		tx, err := solana.TransactionFromBytes(wire)
		require.NoError(t, err)
		txs[i] = tx
	}
	return txs
}

// configure applies what the replay loop applies to every block on the
// executed parent before execution.
func (rig *realFeedRig) configure(block *b.Block) {
	require.NoError(rig.t, configureBlockFromParent(block, rig.lastCtx, rig.env.epochSchedule, false))
	block.Epoch = rig.env.epochSchedule.GetEpoch(block.Slot)
	block.Features = rig.env.feats
}

// reference executes the slot whole, from freshly decoded objects, exactly as
// the loop would: same parent, same content, same last-entry hash.
func (rig *realFeedRig) reference() lifecycleOutcome {
	block := &b.Block{
		Slot:                      rig.slot,
		SourceParentSlot:          realFeedParentSlot,
		FromLiveStream:            true,
		AlpenglowParentBlockID:    lifecycleParentBlockID,
		HasAlpenglowParentBlockID: true,
		Transactions:              decodeWires(rig.t, rig.allWires()),
		Blockhash:                 solana.Hash{realFeedTickHashByte},
	}
	rig.configure(block)
	block.MarkTransactionSignaturesVerified()
	tail := &lifecycleTail{durable: rig.env.durable}
	slotCtx, err := ProcessBlock(rig.env.acctsDb, block, rig.env.epochSchedule, 2, nil, &persistedTracker{}, tail, NewTransactionStatusCache(), false, rig.env.parent)
	require.NoError(rig.t, err)
	return lifecycleOutcomeOf(rig.t, slotCtx, tail)
}

func (rig *realFeedRig) session() *turbine.BroadcastSession {
	return turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
		Leader:                  rig.leader,
		Slot:                    rig.slot,
		ParentSlot:              realFeedParentSlot,
		ParentBlockID:           lifecycleParentBlockID,
		ParentChainedMerkleRoot: solana.Hash{0xBB},
		Broadcaster:             rig.broadcaster,
		Version:                 realFeedShredVersion,
	})
}

// broadcastPrefix sends the header and both entry batches; the slot stays
// incomplete (no footer, no ending tick).
func (rig *realFeedRig) broadcastPrefix(session *turbine.BroadcastSession) {
	legacy, v0 := rig.entries()
	require.NoError(rig.t, session.BroadcastHeader(lifecycleParentBlockID))
	require.NoError(rig.t, session.BroadcastEntryBatch(legacy))
	require.NoError(rig.t, session.BroadcastEntryBatch(v0))
}

// broadcastCompletion sends the footer carrying the expected bank hash and
// the ending tick, which completes the slot.
func (rig *realFeedRig) broadcastCompletion(session *turbine.BroadcastSession, expectedBankhash []byte) {
	require.NoError(rig.t, session.BroadcastFooter(solana.HashFromBytes(expectedBankhash), 1_700_000_000_000_000_000, nil, nil))
	require.NoError(rig.t, session.BroadcastEndingTickLast(solana.Hash{realFeedTickHashByte}))
}

// drive runs the replay loop's wait as the loop would: feed wake-ups and
// poll ticks go to the executor, a complete block ends the wait.
func (rig *realFeedRig) drive(until func() bool, what string) {
	rig.t.Helper()
	deadline := time.After(realFeedDriveDeadline)
	for !until() {
		select {
		case event := <-rig.feed.events:
			rig.exec.handleEvent(event)
		case <-rig.exec.tick():
			rig.exec.handleTick()
		case blk, ok := <-rig.receiver.Blocks():
			require.True(rig.t, ok, "receiver closed its block channel")
			rig.receiver.AcknowledgeBlockDelivery(blk.Slot)
			rig.block = blk
		case <-deadline:
			reason := metrics.GlobalBlockReplay.StreamingExecution.DiscardReason
			rig.t.Fatalf("timed out waiting for %s (stream open: %v, discard reason %q, block: %v)", what, rig.exec.current != nil, reason, rig.block != nil)
		}
	}
}

func (rig *realFeedRig) executedPrefix() int {
	if rig.exec.current == nil {
		return -1
	}
	return len(rig.exec.current.origin)
}

// finalizeAndCompare completes the streamed bank against the emitted block
// and checks it against the whole-block reference.
func (rig *realFeedRig) finalizeAndCompare(reference lifecycleOutcome) {
	rig.t.Helper()
	block := rig.block
	require.NotNil(rig.t, block)
	require.Equal(rig.t, rig.slot, block.Slot)
	require.True(rig.t, block.HasAlpenglowParentBlockID)
	require.Equal(rig.t, lifecycleParentBlockID, solana.Hash(block.AlpenglowParentBlockID))
	require.True(rig.t, block.HasExpectedBankhash, "the footer carried the reference bank hash")
	require.Len(rig.t, block.Transactions, len(rig.allWires()))
	require.Positive(rig.t, block.ShredFullNanos)
	rig.configure(block)

	slotCtx, ok, err := rig.exec.finalize(block, rig.env.parent)
	require.NoError(rig.t, err)
	require.True(rig.t, ok, "the stream must accept its own block (discard reason %q)", metrics.GlobalBlockReplay.StreamingExecution.DiscardReason)
	require.Nil(rig.t, rig.exec.current)
	streamed := lifecycleOutcomeOf(rig.t, slotCtx, rig.tail)
	requireSameLifecycleOutcome(rig.t, reference, streamed)
	require.Equal(rig.t, reference.bankhash, block.ExpectedBankhash[:], "finalize verified the footer hash against the same value")

	record := metrics.GlobalBlockReplay.StreamingExecution
	require.Equal(rig.t, uint64(1), record.Opened)
	require.Equal(rig.t, uint64(len(rig.allWires())), record.Transactions)
	require.Zero(rig.t, record.Discarded, "discard reason %q", record.DiscardReason)
}

func TestStreamingRealFeedExecutesPrefixBeforeCompletionAndMatchesWholeBlock(t *testing.T) {
	dest := solana.PublicKey{0xF1}
	rig := newRealFeedRig(t, dest, 64)
	reference := rig.reference()
	require.Contains(t, reference.delta, dest)
	total := len(rig.allWires())

	session := rig.session()
	rig.broadcastPrefix(session)
	rig.drive(func() bool { return rig.executedPrefix() == total }, "the prefix to execute")
	require.False(t, rig.receiver.SlotCompleted(realFeedSlot), "the slot is still incomplete while the prefix executes")
	require.Nil(t, rig.block)
	prefixDone := time.Now()
	for i, tx := range rig.exec.current.origin[len(rig.legacyWires):] {
		require.Equal(t, solana.MessageVersionV0, tx.Message.GetVersion())
		require.False(t, tx.Message.IsResolved(), "block object %d ran as a stream-owned copy", i)
	}
	sameCopies(t, rig.exec.current.origin, rig.exec.current.exec.transactions)

	rig.broadcastCompletion(session, reference.bankhash)
	rig.drive(func() bool { return rig.block != nil }, "the complete block")
	require.NotNil(t, rig.exec.current, "the stream survives completion")
	require.Equal(t, turbine.StreamDone, rig.receiver.StreamStatusOf(rig.exec.current.generation))
	require.True(t, time.Unix(0, rig.block.ShredFullNanos).After(prefixDone), "the prefix executed before the last shred arrived")
	for i, tx := range rig.exec.current.origin {
		require.Same(t, rig.block.Transactions[i], tx, "the executed prefix is the block, by identity")
	}

	rig.finalizeAndCompare(reference)
	require.Positive(t, metrics.GlobalBlockReplay.StreamingExecution.TxLoopBeforeFull.Count, "transaction work finished before the slot was full")
	require.Zero(t, rig.receiver.StreamDroppedEvents())
}

// Dropped wake-ups: with room for one event, whichever of the three prefix
// wake-ups (header, two batches) the prefetch publishes first is queued and
// the other two are dropped — the prefetch decodes ranges concurrently, so
// the survivor is not fixed. The executor must reach the same state from any
// of them: a surviving header opens the stream and pulls the batches; a
// surviving batch recovers the header from the assembler, then opens and
// pulls the same way. Nothing is read from the feed until every wake-up has
// been published, so exactly two are dropped.
func TestStreamingRealFeedRecoversDroppedWakeups(t *testing.T) {
	dest := solana.PublicKey{0xF2}
	rig := newRealFeedRig(t, dest, 1)
	reference := rig.reference()
	total := len(rig.allWires())

	session := rig.session()
	rig.broadcastPrefix(session)
	require.Eventually(t, func() bool {
		return rig.receiver.StreamDroppedEvents() == 2
	}, realFeedDriveDeadline, 5*time.Millisecond, "one wake-up queued, two dropped")
	var survivor turbine.StreamEvent
	select {
	case survivor = <-rig.feed.events:
	default:
		t.Fatal("the surviving wake-up is not queued")
	}
	require.Zero(t, len(rig.feed.events), "nothing else was published")
	require.Equal(t, turbine.StreamBatchReady, survivor.Kind)
	require.Equal(t, uint64(realFeedSlot), survivor.Slot)
	require.Len(t, rig.receiver.PendingStreamBatches(survivor.Generation, 0), 3, "header and both batches are decoded and discoverable")
	require.Nil(t, rig.exec.current)

	rig.exec.handleEvent(survivor)
	require.NotNil(t, rig.exec.current, "the surviving wake-up (marker %v at shred %d) opened the stream", survivor.Batch.Marker, survivor.Batch.Start)
	require.Equal(t, total, rig.executedPrefix(), "opening pulled every decoded batch from the assembler")
	require.Equal(t, uint64(2), rig.receiver.StreamDroppedEvents(), "recovery reads the assembler, it does not replay wake-ups")
	require.False(t, rig.receiver.SlotCompleted(realFeedSlot))

	rig.broadcastCompletion(session, reference.bankhash)
	rig.drive(func() bool { return rig.block != nil }, "the complete block")
	rig.finalizeAndCompare(reference)
}

// Reset while streaming: the generation is cancelled, the stream discards
// and leaves nothing behind; the slot re-broadcast is a new generation, which
// opens a new stream and finalizes to the same result.
func TestStreamingRealFeedResetDiscardsAndRenews(t *testing.T) {
	dest := solana.PublicKey{0xF3}
	rig := newRealFeedRig(t, dest, 64)
	reference := rig.reference()
	total := len(rig.allWires())

	stakeBefore := len(global.PendingStakeEntriesSnapshot())
	rig.broadcastPrefix(rig.session())
	rig.drive(func() bool { return rig.executedPrefix() == total }, "the first prefix to execute")
	firstGeneration := rig.exec.current.generation

	rig.receiver.ResetSlot(realFeedSlot)
	rig.drive(func() bool { return rig.exec.current == nil }, "the cancellation")
	// The reset reaches the executor either as the feed's cancellation wake-up
	// or, when the poll tick is selected first, as the generation reading gone.
	require.Contains(t, []string{"cancelled:reset", "gone"}, metrics.GlobalBlockReplay.StreamingExecution.DiscardReason)
	require.Equal(t, turbine.StreamGone, rig.receiver.StreamStatusOf(firstGeneration))
	require.Empty(t, rig.tail.added, "a discarded stream commits nothing")
	require.Len(t, global.PendingStakeEntriesSnapshot(), stakeBefore)
	durablePayer, err := rig.env.durable.GetAccountWithoutLock(txfixture.PayerPubkey())
	require.NoError(t, err)
	require.Equal(t, uint64(10_000_000_000), durablePayer.Lamports, "the durable view is untouched")
	// The loop starts the next replay attempt with a fresh collector.
	metrics.GlobalBlockReplay = metrics.BlockReplay{}

	session := rig.session()
	rig.broadcastPrefix(session)
	rig.drive(func() bool { return rig.executedPrefix() == total }, "the renewed prefix to execute")
	// Compared as values (a deep comparison would walk the assembler's live
	// slot state without its lock).
	require.False(t, firstGeneration == rig.exec.current.generation, "a re-assembled slot is a new generation")
	rig.broadcastCompletion(session, reference.bankhash)
	rig.drive(func() bool { return rig.block != nil }, "the complete block")
	rig.finalizeAndCompare(reference)
}

// The child executes on parent 7 while slots 8..11 are unresolved. Consuming
// those skips advances only the frontier; the completed child must still
// produce exactly the whole-block bank hash, accounts, fees and CU.
func TestStreamingRealFeedAcrossSkippedSlots(t *testing.T) {
	rig := newRealFeedRig(t, solana.PublicKey{0xF1}, 64)
	rig.slot += 4
	frontier := uint64(realFeedParentSlot)
	rig.exec.deps.frontier = func() uint64 { return frontier }
	reference := rig.reference()
	session := rig.session()
	rig.broadcastPrefix(session)
	rig.drive(func() bool { return rig.executedPrefix() == len(rig.allWires()) }, "prefix across unresolved skips")
	require.Equal(t, uint64(realFeedParentSlot), frontier, "speculation does not certify skips or advance replay")
	require.False(t, rig.receiver.SlotCompleted(rig.slot))
	cur := rig.exec.current
	for slot := frontier + 1; slot < rig.slot; slot++ {
		rig.exec.beforeBlock(&b.Block{Slot: slot, IsSkipped: true})
		rig.exec.discardSlot(slot, "skipped")
		frontier = slot
		rig.exec.handleTick()
		require.Same(t, cur, rig.exec.current)
	}
	rig.broadcastCompletion(session, reference.bankhash)
	rig.drive(func() bool { return rig.block != nil }, "completed child after skips")
	rig.finalizeAndCompare(reference)
}
