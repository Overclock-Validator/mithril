package replay

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// historyAdmissionContext releases the held worker only when wait evaluates its
// cancellation arm. The poll control never reaches that handshake.
type historyAdmissionContext struct {
	context.Context
	release func()
}

func (ctx historyAdmissionContext) Done() <-chan struct{} {
	ctx.release()
	return ctx.Context.Done()
}

// historyAdmissionExecute reuses the real whole/stream lifecycle fixture. The
// two IDs are independent banks on the same parent, not a consecutive-slot proof.
func historyAdmissionExecute(t *testing.T, streamMode bool, session *TransactionHistorySession, id byte) (lifecycleOutcome, transactionhistory.Block) {
	t.Helper()
	env := newLifecycleEnv(t)
	txs := transferTransactions(t, 4, 999_000)
	block := env.block(txs)
	block.Blockhash = [32]byte{id}
	block.MarkTransactionSignaturesVerified()
	tail := &lifecycleTail{durable: env.durable}
	var slotCtx *sealevel.SlotCtx
	if !streamMode {
		var err error
		slotCtx, err = processBlockWithHistory(env.acctsDb, block, env.epochSchedule, 4, nil,
			&persistedTracker{}, tail, NewTransactionStatusCache(), false, env.parent, session)
		require.NoError(t, err)
	} else {
		parent := &sealevel.SlotCtx{Slot: lifecycleParentSlot, Epoch: 0,
			FinalBankhash: block.ParentBankhash[:], VoteTimestamps: block.VoteTimestamps,
			FeeRateGovernor: block.PrevFeeRateGovernor}
		require.NoError(t, parent.PublishBankSysvars(env.parent))
		feed := newFakeStreamFeed()
		gen := turbine.NewDetachedStreamGeneration(lifecycleSlot)
		feed.status[gen] = turbine.StreamActive
		stream := newStreamingExecutor(streamingDeps{acctsDb: env.acctsDb, feed: feed,
			epochSchedule: env.epochSchedule, txParallelism: 4, persistedHashes: &persistedTracker{},
			tail: tail, transactionStatuses: NewTransactionStatusCache(), transactionHistory: session,
			alpenglowMode: true, unrootedTailUsed: true,
			lastSlotCtx: func() *sealevel.SlotCtx { return parent }, frontier: func() uint64 { return lifecycleParentSlot },
			currentFeatures: func() *features.Features { return env.feats }, currentEpoch: func() uint64 { return 0 },
			rewardsInFlight: func() bool { return false }, switchPending: func() bool { return false },
			executedBlockID: func(uint64) (solana.Hash, bool) { return lifecycleParentBlockID, true },
		})
		defer stream.shutdown()
		header := turbine.NewDetachedStreamMarker(gen, 0, 0, turbine.StreamMarkerHeader, lifecycleParentSlot, lifecycleParentBlockID)
		stream.openStream(header)
		require.NotNil(t, stream.current)
		if session.current == nil {
			require.Nil(t, stream.current.exec.metadata)
		} else {
			require.Same(t, session.current, stream.current.exec.metadata)
		}
		batch := turbine.NewDetachedStreamBatch(gen, 1, 2, txs[:2], verifiedIdentities(t, txs[:2]))
		stream.handleEvent(turbine.StreamEvent{Kind: turbine.StreamBatchReady, Slot: lifecycleSlot, Generation: gen, Batch: batch})
		var ok bool
		var err error
		slotCtx, ok, err = stream.finalize(block, env.parent)
		require.NoError(t, err)
		require.True(t, ok)
	}
	outcome := lifecycleOutcomeOf(t, slotCtx, tail)
	var receipt transactionhistory.Block
	if session.current != nil {
		require.NoError(t, session.current.Err)
		require.NotNil(t, session.current.Record)
		var err error
		receipt, err = session.current.Record.historyBlock()
		require.NoError(t, err)
	}
	session.acceptBlock(block)
	return outcome, receipt
}

func TestHistoryAdmissionBackpressureReceipts(t *testing.T) {
	arm := os.Getenv("MITHRIL_HISTORY_ADMISSION_ARM")
	require.Contains(t, []string{"", "wait", "poll"}, arm)
	previous := StreamingExecutionCfg
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	t.Cleanup(func() { StreamingExecutionCfg = previous })
	manage := global.ManageLeaderSchedule()
	global.SetManageLeaderSchedule(false)
	t.Cleanup(func() { global.SetManageLeaderSchedule(manage) })
	for _, streamMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "whole", true: "stream"}[streamMode], func(t *testing.T) {
			reference := &TransactionHistorySession{write: func(transactionhistory.Block) error { return nil }}
			wantState, want := historyAdmissionExecute(t, streamMode, reference, 0x99)
			requireHistorySessionStopped(t, reference, nil)
			require.Len(t, want.Records, 4)

			store, err := transactionhistory.Open(t.TempDir(), [32]byte{1}, transactionhistory.DefaultLimits())
			require.NoError(t, err)
			defer func() { require.NoError(t, store.Close()) }()
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			session := &TransactionHistorySession{write: func(block transactionhistory.Block) error {
				if block.ID.Blockhash == [32]byte{0x98} {
					close(entered)
					<-release
				}
				return store.WriteBlock(block)
			}}
			defer func() {
				unblock()
				requireHistorySessionStopped(t, session, nil)
			}()
			_, first := historyAdmissionExecute(t, false, session, 0x98)
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("first write did not reach the held store call")
			}
			pending := session.pending
			require.NotNil(t, pending)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if arm == "poll" {
				require.NoError(t, session.poll())
				require.Same(t, pending, session.pending)
			} else {
				require.NoError(t, session.wait(historyAdmissionContext{Context: ctx, release: unblock}))
				require.Nil(t, session.pending)
			}
			gotState, captured := historyAdmissionExecute(t, streamMode, session, 0x99)
			requireSameLifecycleOutcome(t, wantState, gotState)
			unblock()
			requireHistorySessionStopped(t, session, nil)
			view, err := store.Snapshot()
			require.NoError(t, err)
			defer func() { require.NoError(t, view.Close()) }()
			prior, err := view.Block(first.ID)
			require.NoError(t, err)
			require.Equal(t, first, prior)
			got, err := view.Block(want.ID)
			require.NoError(t, err, "accepted next block must retain its receipt after the admission boundary")
			require.Equal(t, want, captured)
			require.Equal(t, want, got)
		})
	}
}

func TestHistoryAdmissionBackpressureCancellation(t *testing.T) {
	var disabled *TransactionHistorySession
	require.NoError(t, disabled.wait(context.Background()))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if err := disabled.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	session := &TransactionHistorySession{write: func(transactionhistory.Block) error {
		close(entered)
		<-release
		return nil
	}}
	defer func() {
		unblock()
		requireHistorySessionStopped(t, session, nil)
	}()
	capture := session.begin()
	capture.Record = &blockMetadata{Slot: 8}
	session.finish(capture, true)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("write did not start")
	}
	pending := session.pending
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, session.wait(ctx), context.Canceled)
	require.Same(t, pending, session.pending, "cancellation must not release worker ownership")
	require.Nil(t, session.err, "replay cancellation must not become a sticky store failure")
	require.False(t, session.stopped)
	require.Nil(t, session.begin(), "a retry cannot admit while the same job is pending")
	joined, err := session.Stop(ctx)
	require.False(t, joined)
	require.ErrorIs(t, err, context.Canceled)
	require.Same(t, pending, session.pending)
	unblock()
	requireHistorySessionStopped(t, session, nil)
	require.NoError(t, session.wait(context.Background()))
	require.Nil(t, session.begin(), "Stop still prevents later admission")
}

func TestHistoryAdmissionBackpressureErrors(t *testing.T) {
	for _, kind := range []string{"completed", "retention", "write", "conversion"} {
		t.Run(kind, func(t *testing.T) {
			failure := errors.New("held history write failed")
			writes := 0
			session := &TransactionHistorySession{write: func(transactionhistory.Block) error {
				writes++
				switch kind {
				case "retention":
					return transactionhistory.ErrRetention
				case "write":
					return failure
				default:
					return nil
				}
			}}
			capture := session.begin()
			capture.Record = &blockMetadata{Slot: 8}
			if kind == "conversion" {
				capture.Record.Transactions = []*transactionMetadata{nil}
			}
			session.finish(capture, true)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := session.wait(ctx)
			require.Nil(t, session.pending)
			if kind == "write" || kind == "conversion" {
				require.Error(t, err)
				if kind == "write" {
					require.ErrorIs(t, err, failure)
					require.Equal(t, 1, writes)
				} else {
					require.ErrorContains(t, err, "missing transaction capture")
					require.Zero(t, writes)
				}
				require.ErrorIs(t, session.wait(context.Background()), err)
				require.Nil(t, session.begin())
				requireHistorySessionStopped(t, session, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, writes)
			require.NotNil(t, session.begin(), "completion and retention pressure must permit another capture")
			requireHistorySessionStopped(t, session, nil)
		})
	}
}
