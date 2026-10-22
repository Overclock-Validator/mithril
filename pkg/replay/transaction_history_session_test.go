package replay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type historySessionEngine struct {
	consensusengine.Engine
	voting bool
}

func (e historySessionEngine) Snapshot() consensusengine.Snapshot {
	var snapshot consensusengine.Snapshot
	if e.voting {
		snapshot.Voting = &consensusengine.VotingStats{}
	}
	return snapshot
}

type historySessionReadFailure struct{ *lifecycleTail }

func (historySessionReadFailure) GetAccountsBatch(context.Context, uint64, []solana.PublicKey) ([]*accounts.Account, error) {
	return nil, errors.New("history session test account read failure")
}

func TestTransactionHistorySessionObserverGate(t *testing.T) {
	store, err := transactionhistory.Open(t.TempDir(), [32]byte{1}, transactionhistory.DefaultLimits())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	for _, mode := range []string{"", "validator", "Verifying", " verifying"} {
		_, err := NewTransactionHistorySession(store, mode)
		require.Error(t, err, mode)
	}
	session, err := NewTransactionHistorySession(store, "verifying")
	require.NoError(t, err)
	defer requireHistorySessionStopped(t, session, nil)
	valid := ConsensusOpts{Alpenglow: true, Engine: historySessionEngine{}, TransactionHistory: session}
	require.NoError(t, session.validateReplay(&valid, nil))
	for _, test := range []struct {
		name  string
		opts  ConsensusOpts
		fetch *BlockFetchOpts
	}{
		{"classic", ConsensusOpts{Engine: historySessionEngine{}, TransactionHistory: session}, nil},
		{"no engine", ConsensusOpts{Alpenglow: true, TransactionHistory: session}, nil},
		{"voting", ConsensusOpts{Alpenglow: true, Engine: historySessionEngine{voting: true}, TransactionHistory: session}, nil},
		{"local channel", valid, &BlockFetchOpts{LocalBlocks: make(chan *b.Block)}},
		{"local leader", valid, &BlockFetchOpts{LocalLeaderForSlot: func(uint64) bool { return false }}},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Missing bootstrap inputs would panic if rejection happened after
			// replay touched accounts, stores, network or the block source.
			result := ReplayBlocks(context.Background(), nil, "", nil, nil, 8, 8,
				nil, "", "", "", "", "", 0, "", nil, "", 0, false, false, false,
				nil, nil, nil, test.fetch, &test.opts, nil)
			require.Error(t, result.Error)
			require.Nil(t, session.current)
			require.Nil(t, session.pending)
		})
	}
}

func TestTransactionHistorySessionWholeBlockAcceptance(t *testing.T) {
	for _, accept := range []bool{false, true} {
		t.Run(map[bool]string{false: "rejected after execution", true: "accepted"}[accept], func(t *testing.T) {
			env := newLifecycleEnv(t)
			txs := transferTransactions(t, 4, 999_000)
			wantState := lifecycleWholeBlock(t, env, txs, 4)
			store, err := transactionhistory.Open(t.TempDir(), [32]byte{1}, transactionhistory.DefaultLimits())
			require.NoError(t, err)
			defer func() { require.NoError(t, store.Close()) }()
			session, err := NewTransactionHistorySession(store, "verifying")
			require.NoError(t, err)
			defer requireHistorySessionStopped(t, session, nil)
			tail := &lifecycleTail{durable: env.durable}
			block := env.block(txs)
			block.MarkTransactionSignaturesVerified()
			ctx, err := processBlockWithHistory(env.acctsDb, block, env.epochSchedule, 4, nil,
				&persistedTracker{}, tail, NewTransactionStatusCache(), false, env.parent, session)
			require.NoError(t, err)
			require.Equal(t, wantState, lifecycleOutcomeOf(t, ctx, tail))
			require.NotNil(t, session.current.Record)
			require.Nil(t, session.pending, "execution success alone must not start persistence")
			want, err := session.current.Record.historyBlock()
			require.NoError(t, err)
			if accept {
				session.acceptBlock(block)
			} else {
				// This models a later summary publisher or OnReplayResult rejection.
				session.discard()
			}
			requireHistorySessionStopped(t, session, nil)
			view, err := store.Snapshot()
			require.NoError(t, err)
			defer func() { require.NoError(t, view.Close()) }()
			got, err := view.Block(want.ID)
			if accept {
				require.NoError(t, err)
				require.Equal(t, want, got)
			} else {
				require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
			}
		})
	}
}

func TestTransactionHistorySessionWholeBlockEarlyExit(t *testing.T) {
	session := &TransactionHistorySession{write: func(transactionhistory.Block) error {
		t.Error("failed execution must not be written")
		return nil
	}}
	_, err := processBlockWithHistory(nil, nil, nil, 0, nil, nil, nil, nil, false, nil, session)
	require.Error(t, err)
	require.Nil(t, session.current)
	env := newLifecycleEnv(t)
	block := env.block(transferTransactions(t, 1, 1))
	block.MarkTransactionSignaturesVerified()
	// A read failure must release capture ownership without publishing history.
	ctx, err := processBlockWithHistory(env.acctsDb, block, env.epochSchedule, 0, nil, &persistedTracker{},
		historySessionReadFailure{&lifecycleTail{durable: env.durable}}, NewTransactionStatusCache(), false, env.parent, session)
	require.Nil(t, ctx)
	require.ErrorContains(t, err, "history session test account read failure")
	require.Nil(t, session.current)
	require.Nil(t, session.pending)
	requireHistorySessionStopped(t, session, nil)
}

func TestTransactionHistorySessionStreamAcceptance(t *testing.T) {
	previous := StreamingExecutionCfg
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	t.Cleanup(func() { StreamingExecutionCfg = previous })
	manage := global.ManageLeaderSchedule()
	global.SetManageLeaderSchedule(false)
	t.Cleanup(func() { global.SetManageLeaderSchedule(manage) })
	for _, accept := range []bool{false, true} {
		t.Run(map[bool]string{false: "discard", true: "accepted"}[accept], func(t *testing.T) {
			env := newLifecycleEnv(t)
			txs := transferTransactions(t, 4, 999_000)
			wantState := lifecycleWholeBlock(t, env, txs, 4)
			var written transactionhistory.Block
			session := &TransactionHistorySession{write: func(block transactionhistory.Block) error {
				written = block
				return nil
			}}
			defer requireHistorySessionStopped(t, session, nil)
			tail := &lifecycleTail{durable: env.durable}
			block := env.block(txs)
			block.MarkTransactionSignaturesVerified()
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
			require.Same(t, session.current, stream.current.exec.metadata, "real stream open acquired the session capture")
			batch := turbine.NewDetachedStreamBatch(gen, 1, 2, txs[:2], verifiedIdentities(t, txs[:2]))
			stream.handleEvent(turbine.StreamEvent{Kind: turbine.StreamBatchReady, Slot: lifecycleSlot, Generation: gen, Batch: batch})
			require.Len(t, session.current.records, 2)
			if !accept {
				stream.discard("test rejection")
				require.Nil(t, session.current)
				require.Nil(t, session.pending)
				require.Empty(t, tail.added)
				return
			}
			ctx, ok, err := stream.finalize(block, env.parent)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, wantState, lifecycleOutcomeOf(t, ctx, tail))
			require.NotNil(t, session.current.Record)
			require.Nil(t, session.pending, "stream success is not the common replay acceptance gate")
			want, err := session.current.Record.historyBlock()
			require.NoError(t, err)
			session.acceptBlock(block)
			requireHistorySessionStopped(t, session, nil)
			require.Equal(t, want, written)
		})
	}
}

func TestTransactionHistorySessionDiscardDoesNotWait(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	session := &TransactionHistorySession{write: func(transactionhistory.Block) error {
		close(entered)
		<-release
		return nil
	}}
	defer func() {
		close(release)
		joined, err := session.Stop(context.Background())
		require.True(t, joined)
		require.ErrorContains(t, err, "unexpected local production")
	}()
	capture := session.begin()
	capture.Record = &blockMetadata{Slot: 8, Blockhash: [32]byte{1}}
	session.acceptBlock(&b.Block{Slot: 8, Blockhash: [32]byte{1}})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not enter")
	}
	session.discard() // ReplayBlocks return or fork unwind: never joins storage.
	require.NotNil(t, session.pending)
	require.Nil(t, session.begin(), "a subsequent replay attempt shares the same one-job ceiling")
	failure := errors.New("unexpected local production")
	session.disable(failure)
	require.ErrorIs(t, session.poll(), failure)
	require.NotNil(t, session.pending)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	joined, err := session.Stop(ctx)
	require.False(t, joined)
	require.ErrorIs(t, err, context.Canceled)
}

func TestTransactionHistorySessionRejectsWrongAcceptedIdentity(t *testing.T) {
	for _, block := range []*b.Block{nil, {Slot: 9, Blockhash: [32]byte{1}}, {Slot: 8, Blockhash: [32]byte{2}}} {
		session := &TransactionHistorySession{write: func(transactionhistory.Block) error {
			t.Error("mismatched block must not be written")
			return nil
		}}
		capture := session.begin()
		capture.Record = &blockMetadata{Slot: 8, Blockhash: [32]byte{1}}
		session.acceptBlock(block)
		require.Nil(t, session.pending)
		require.Nil(t, session.current)
		require.Nil(t, session.begin())
		require.ErrorContains(t, session.poll(), "does not match accepted block")
	}
}
