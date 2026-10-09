package consensus

import (
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Keep the actor unstarted so tests can place finality, replay and durable
// promotion in a deterministic order, using the real engine event queue.
func newOrderingTestVoter(t *testing.T, reserved bool) (*AlpenglowObserverEngine, *alpenglowVoter) {
	t.Helper()
	cfg := reservedTestConfig(t.TempDir())
	cfg.ReservedHistory = reserved
	cfg.InitializeVoteReservation = reserved
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{39}}
	e, err := NewEngine(Config{AlpenglowIdentity: cfg.Identity, AlpenglowShredVersion: 0x1234})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	set := voterTestValidatorSet(t, cfg.Identity, cfg.AuthorizedVoter, cfg.VoteAccount)
	e.SetAlpenglowEpochLookup(cfg.EpochForSlot)
	require.NoError(t, e.SetAlpenglowValidatorSet(set))
	e.SetAlpenglowRoot(root)
	v, err := newAlpenglowVoterUnstarted(e, cfg, root, []alpenglow.ValidatorSet{set})
	require.NoError(t, err)
	e.voter = v
	if reserved {
		reserveThrough(t, v.reservation, 44)
	}
	// Equivalent to startup's trusted-root ParentReady seed.
	require.True(t, v.history.AddParentReady(40, root))
	return e, v
}

func drainOrderingEvents(t *testing.T, v *alpenglowVoter) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		select {
		case event := <-v.events:
			require.NoError(t, v.handle(event))
		default:
			return
		}
	}
	t.Fatal("voter event queue did not drain")
}

func observeOrderingBlock(t *testing.T, e *AlpenglowObserverEngine, slot uint64) alpenglow.BlockID {
	t.Helper()
	id := alpenglow.BlockID{Slot: slot, Hash: solana.Hash{byte(slot)}}
	require.NoError(t, e.ObserveBlock(context.Background(), BlockObservation{Source: "ordering-test", Block: &block.Block{
		Slot: slot, ParentSlot: slot - 1,
		AlpenglowBlockID: [32]byte(id.Hash), HasAlpenglowBlockID: true,
		AlpenglowParentBlockID: [32]byte{byte(slot - 1)}, HasAlpenglowParentBlockID: true,
	}}))
	return id
}

func finalizeOrderingBlock(t *testing.T, e *AlpenglowObserverEngine, v *alpenglowVoter, id alpenglow.BlockID) {
	t.Helper()
	// The two peers supply the 60% slow-finality quorum without our vote;
	// adding our 30% notarization later can produce a fast certificate.
	for _, vote := range []alpenglow.Vote{alpenglow.NewNotarizationVote(id.Slot, id.Hash), alpenglow.NewFinalizationVote(id.Slot)} {
		for rank, key := range []ed25519.PrivateKey{voterTestKey(21), voterTestKey(22)} {
			peer := signedVerifiedVoterPeerVote(t, e, v.sets[7], key, uint16(rank+1), vote)
			_, err := e.acceptVerifiedVoteResult(peer)
			require.NoError(t, err)
		}
	}
	require.Equal(t, id.Slot, e.ensureChain().Snapshot().LatestDirectFinalizedBlock.Slot)
}

func TestAlpenglowVoterReplaysFourBlocksAfterNetworkFinality(t *testing.T) {
	for _, reserved := range []bool{false, true} {
		name := "synchronous"
		if reserved {
			name = "reserved"
		}
		t.Run(name, func(t *testing.T) {
			e, v := newOrderingTestVoter(t, reserved)
			for slot := uint64(40); slot <= 43; slot++ {
				id := observeOrderingBlock(t, e, slot)
				finalizeOrderingBlock(t, e, v, id)
				drainOrderingEvents(t, v)
				require.Equal(t, slot, v.highestFinal)
				require.Less(t, v.admissionFloor(), slot)
				require.False(t, v.history.VotedAt(slot), "network finality does not prove local execution")
				before := v.snapshot()
				require.NoError(t, e.OnReplayResult(context.Background(), SlotReplayResult{Slot: slot, Source: "ordering-test"}))
				drainOrderingEvents(t, v)
				hash, ok := v.history.NotarizedVote(slot)
				require.True(t, ok, "replay must still notarize after slow finality")
				require.Equal(t, id.Hash, hash)
				require.Greater(t, v.snapshot().BroadcastMessagesQueued, before.BroadcastMessagesQueued)
				require.NoError(t, e.AlpenglowSafetyError())
			}
		})
	}
}

func TestAlpenglowVoterNetworkFinalityDuringLocalAdmission(t *testing.T) {
	e, v := newOrderingTestVoter(t, false)
	id := observeOrderingBlock(t, e, 40)
	called := false
	v.beforeLocalVoteInject = func(vote alpenglow.Vote) {
		if called {
			return
		}
		called = true
		require.Equal(t, alpenglow.NewNotarizationVote(id.Slot, id.Hash), vote)
		finalizeOrderingBlock(t, e, v, id)
	}
	require.NoError(t, e.OnReplayResult(context.Background(), SlotReplayResult{Slot: id.Slot}))
	drainOrderingEvents(t, v)
	require.True(t, called)
	require.Positive(t, v.snapshot().VotesCastThisRun)
	message, _, err := v.sign(alpenglow.NewNotarizationVote(id.Slot, id.Hash), false)
	require.NoError(t, err)
	require.True(t, e.ensurePool().HasVerifiedVote(message))
	require.NoError(t, e.AlpenglowSafetyError())
}

func TestAlpenglowVoterDurableRootCannotOvertakeQueuedReplay(t *testing.T) {
	e, v := newOrderingTestVoter(t, true)
	id := observeOrderingBlock(t, e, 40)
	finalizeOrderingBlock(t, e, v, id)
	drainOrderingEvents(t, v)
	require.NoError(t, e.OnReplayResult(context.Background(), SlotReplayResult{Slot: id.Slot}))
	e.PruneAlpenglowBefore(id.Slot)
	require.Equal(t, uint64(39), e.ensurePool().Snapshot().RootSlot)
	require.Contains(t, e.executedReplayBlocks, id, "queued replay must retain its execution proof")
	queuedBefore := v.snapshot().BroadcastMessagesQueued
	drainOrderingEvents(t, v)
	require.Greater(t, v.snapshot().BroadcastMessagesQueued, queuedBefore)
	require.Equal(t, id.Slot, v.history.Root)
	require.Equal(t, id.Slot, e.ensurePool().Snapshot().RootSlot)
	require.NotContains(t, e.executedReplayBlocks, id)
	_, ok := v.history.NotarizedVote(id.Slot)
	require.True(t, ok, "root vote must remain available to its intra-window child")
	child := observeOrderingBlock(t, e, 41)
	finalizeOrderingBlock(t, e, v, child)
	require.NoError(t, e.OnReplayResult(context.Background(), SlotReplayResult{Slot: child.Slot}))
	drainOrderingEvents(t, v)
	hash, ok := v.history.NotarizedVote(child.Slot)
	require.True(t, ok)
	require.Equal(t, child.Hash, hash)
	require.NoError(t, e.AlpenglowSafetyError())
}

func TestAlpenglowVoterFinalityPreservesEarlierSkipDecision(t *testing.T) {
	e, v := newOrderingTestVoter(t, true)
	require.NoError(t, v.history.AddVote(alpenglow.NewSkipVote(40)))
	id := observeOrderingBlock(t, e, 40)
	finalizeOrderingBlock(t, e, v, id)
	drainOrderingEvents(t, v)
	require.NoError(t, e.OnReplayResult(context.Background(), SlotReplayResult{Slot: 40}))
	drainOrderingEvents(t, v)
	require.True(t, v.history.HasSkipped(40))
	_, ok := v.history.NotarizedVote(40)
	require.False(t, ok, "late replay must never replace an earlier round-one decision")
	require.NoError(t, e.AlpenglowSafetyError())
}

func TestReservedRecoveryUsesVerifiedFinalityNotLiveAdmissionFloor(t *testing.T) {
	cfg := reservedTestConfig(t.TempDir())
	v, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	reserveThrough(t, v.reservation, 40)
	h := v.reservation.through.Load()
	crashReservedTestVoter(t, v)
	cfg.InitializeVoteReservation = false
	v, err = openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	_, _, err = v.sign(alpenglow.NewSkipVote(h+1), false)
	require.ErrorIs(t, err, errVoterNotReady)
	id := observeOrderingBlock(t, v.engine, h)
	finalizeOrderingBlock(t, v.engine, v, id)
	require.Less(t, v.admissionFloor(), h)
	require.Equal(t, h, v.engine.alpenglowVerifiedFinalityFloor())
	reserveThrough(t, v.reservation, h+1)
	for _, normal := range []bool{false, true} {
		_, _, err = v.sign(alpenglow.NewSkipVote(h), normal)
		require.ErrorIs(t, err, errVoterNotReady)
		_, _, err = v.sign(alpenglow.NewSkipVote(h+1), normal)
		require.NoError(t, err)
	}
}
