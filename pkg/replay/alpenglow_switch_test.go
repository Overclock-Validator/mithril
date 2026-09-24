package replay

import (
	"context"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeChainQuery is a canned AlpenglowChainQuery.
type fakeChainQuery struct {
	certified      map[uint64]alpenglow.BlockID
	skipped        map[uint64]bool
	finalizedSkips map[uint64]alpenglow.BlockID
	version        uint64
}

type trackerChainQuery struct{ *alpenglow.ChainTracker }

func (q *trackerChainQuery) ChainDecisionVersion() uint64          { return q.DecisionVersion() }
func (q *trackerChainQuery) ChainDecisionChanges() <-chan struct{} { return q.DecisionChanges() }

// Only the optional chain query/notification interfaces are used by the sweeper.
type trackerChainEngine struct {
	consensusengine.Engine
	*trackerChainQuery
}

func (f *fakeChainQuery) CertifiedBlockAt(slot uint64) (alpenglow.BlockID, alpenglow.CertificateType, bool) {
	if b, ok := f.certified[slot]; ok {
		return b, alpenglow.CertificateNotarize, true
	}
	return alpenglow.BlockID{}, "", false
}
func (f *fakeChainQuery) SkipCertifiedAt(slot uint64) bool { return f.skipped[slot] }
func (f *fakeChainQuery) FinalizedSkipAt(slot uint64) (alpenglow.BlockID, bool) {
	via, ok := f.finalizedSkips[slot]
	return via, ok
}
func (f *fakeChainQuery) ChainDecisionVersion() uint64 { return f.version }

func swHash(b byte) solana.Hash { var h solana.Hash; h[0] = b; return h }

func newTestSweeper(q *fakeChainQuery) *alpenglowSwitchSweeper {
	return &alpenglowSwitchSweeper{query: q}
}

// Matching certificates produce no switch.
func TestSweepNoContradiction(t *testing.T) {
	q := &fakeChainQuery{
		certified: map[uint64]alpenglow.BlockID{101: {Slot: 101, Hash: swHash(1)}},
		skipped:   map[uint64]bool{},
		version:   1,
	}
	s := newTestSweeper(q)
	executed := map[uint64]solana.Hash{101: swHash(1), 102: swHash(2)}
	assert.Nil(t, s.sweep(executed, 100, 102))
}

// A decisive cert naming a different sibling switches at that slot.
func TestSweepSiblingMismatch(t *testing.T) {
	q := &fakeChainQuery{
		certified: map[uint64]alpenglow.BlockID{101: {Slot: 101, Hash: swHash(9)}},
		skipped:   map[uint64]bool{},
		version:   1,
	}
	s := newTestSweeper(q)
	executed := map[uint64]solana.Hash{101: swHash(1)}
	sw := s.sweep(executed, 100, 101)
	require.NotNil(t, sw)
	assert.Equal(t, uint64(101), sw.Slot)
	assert.Equal(t, swHash(1), sw.Executed)
	assert.Equal(t, swHash(9), sw.Certified)
	assert.False(t, sw.Skip)
}

// Like Agave's BankForks, execute-on-receipt must retain an already replayed
// block when a skip certificate arrives. A skip makes it legal for a future
// child to omit the block, but does not prove that the block cannot be that
// child's parent. An exact parent link or finalized ancestry selects the fork.
func TestSweepSkipDoesNotInvalidateExecutedPotentialParent(t *testing.T) {
	q := &fakeChainQuery{certified: map[uint64]alpenglow.BlockID{}, skipped: map[uint64]bool{101: true}, version: 1}
	s := newTestSweeper(q)
	executed := map[uint64]solana.Hash{101: swHash(1)}
	assert.Nil(t, s.sweep(executed, 100, 101))
}

func TestSweepFinalizedAncestrySkipInvalidatesExecutedBlock(t *testing.T) {
	q := &fakeChainQuery{
		finalizedSkips: map[uint64]alpenglow.BlockID{
			101: {Slot: 104, Hash: swHash(4)},
		},
		version: 1,
	}
	s := newTestSweeper(q)
	sw := s.sweep(map[uint64]solana.Hash{101: swHash(1)}, 100, 101)
	require.NotNil(t, sw)
	assert.Equal(t, uint64(101), sw.Slot)
	assert.Equal(t, swHash(1), sw.Executed)
	assert.True(t, sw.Skip)
	assert.True(t, sw.Certified.IsZero())
}

func TestSweepRetainsSkipCertifiedParentSelectedByFinalizedChild(t *testing.T) {
	tracker := alpenglow.NewChainTracker()
	parent := alpenglow.BlockID{Slot: 12, Hash: swHash(12)}
	child := alpenglow.BlockID{Slot: 16, Hash: swHash(16)}
	tracker.ObserveReplayBlock(alpenglow.ReplayBlockObservation{Block: parent, ParentSlot: 7, ParentHash: swHash(7)})
	for _, cert := range []alpenglow.Certificate{
		{Type: alpenglow.CertificateNotarizeFallback, Slot: parent.Slot, BlockHash: parent.Hash, SignatureVerified: true},
		{Type: alpenglow.CertificateSkip, Slot: parent.Slot, SignatureVerified: true},
	} {
		_, err := tracker.ObserveCertificate(cert)
		require.NoError(t, err)
	}

	s := &alpenglowSwitchSweeper{query: &trackerChainQuery{ChainTracker: tracker}}
	executed := map[uint64]solana.Hash{parent.Slot: parent.Hash}
	assert.Nil(t, s.sweep(executed, 7, parent.Slot), "skip alone must retain the possible parent")

	tracker.ObserveReplayBlock(alpenglow.ReplayBlockObservation{Block: child, ParentSlot: parent.Slot, ParentHash: parent.Hash})
	_, err := tracker.ObserveCertificate(alpenglow.Certificate{
		Type:              alpenglow.CertificateFinalizeFast,
		Slot:              child.Slot,
		BlockHash:         child.Hash,
		SignatureVerified: true,
	})
	require.NoError(t, err)
	require.NoError(t, tracker.ObserveFinalized(child, alpenglow.CertificateFinalizeFast))

	assert.Nil(t, s.sweep(executed, 7, parent.Slot), "finalized child must retain its matching exact parent")
	executed[parent.Slot] = swHash(99)
	s = &alpenglowSwitchSweeper{query: &trackerChainQuery{ChainTracker: tracker}}
	sw := s.sweep(executed, 7, parent.Slot)
	require.NotNil(t, sw)
	assert.Equal(t, parent.Slot, sw.Slot)
	assert.Equal(t, parent.Hash, sw.Certified)
	assert.False(t, sw.Skip)
}

func TestSweepCertifiedSkipMatchesLocalSkip(t *testing.T) {
	q := &fakeChainQuery{certified: map[uint64]alpenglow.BlockID{}, skipped: map[uint64]bool{101: true}, version: 1}
	s := newTestSweeper(q)
	executed := map[uint64]solana.Hash{101: {}}
	assert.Nil(t, s.sweep(executed, 100, 101))
}

func TestSweepCertifiedBlockContradictsLocalSkip(t *testing.T) {
	q := &fakeChainQuery{
		certified: map[uint64]alpenglow.BlockID{101: {Slot: 101, Hash: swHash(9)}},
		skipped:   map[uint64]bool{},
		version:   1,
	}
	s := newTestSweeper(q)
	executed := map[uint64]solana.Hash{101: {}}
	sw := s.sweep(executed, 100, 101)
	require.NotNil(t, sw)
	assert.True(t, sw.Executed.IsZero())
	assert.Equal(t, swHash(9), sw.Certified)
	assert.False(t, sw.Skip)
	assert.Contains(t, sw.Error(), "treated as skipped locally")
}

// The FIRST contradiction wins (ancestors before descendants).
func TestSweepReportsFirstContradiction(t *testing.T) {
	q := &fakeChainQuery{
		certified: map[uint64]alpenglow.BlockID{
			101: {Slot: 101, Hash: swHash(9)},
			103: {Slot: 103, Hash: swHash(8)},
		},
		skipped: map[uint64]bool{102: true},
		version: 1,
	}
	s := newTestSweeper(q)
	executed := map[uint64]solana.Hash{101: swHash(1), 102: swHash(2), 103: swHash(3)}
	sw := s.sweep(executed, 100, 103)
	require.NotNil(t, sw)
	assert.Equal(t, uint64(101), sw.Slot, "lowest contradicted slot first")
}

// The sweep is gated on the decision version and the replay/root frontiers. The
// decision version advances on ANY decisive change — not only certificates —
// so a contradiction derived from replay observations (parent links, finalized
// ancestry, indirect skips) still re-arms the sweep.
func TestSweepGatingAndBounds(t *testing.T) {
	q := &fakeChainQuery{
		certified: map[uint64]alpenglow.BlockID{101: {Slot: 101, Hash: swHash(9)}},
		skipped:   map[uint64]bool{},
		version:   1,
	}
	s := newTestSweeper(q)
	executed := map[uint64]solana.Hash{101: swHash(1)}

	// Below the rooted floor: never inspected.
	assert.Nil(t, s.sweep(executed, 101, 101), "slot at/below lastRooted is out of window")

	// First sweep at version=1 fires...
	sw := s.sweep(executed, 100, 101)
	require.NotNil(t, sw)
	// ...but with no decision or frontier change the next sweep is a no-op even
	// though the contradiction persists (the caller acts on the first report).
	assert.Nil(t, s.sweep(executed, 100, 101), "no decision change -> no re-sweep")

	// A decision-version bump WITHOUT a new certificate (e.g. replay-derived
	// finalized ancestry or an indirect skip) re-arms the sweep.
	q.version = 2
	require.NotNil(t, s.sweep(executed, 100, 101), "decision-version bump re-arms the sweep")
}

// A nil sweeper (engine without chain query) is inert.
func TestSweepNilSweeper(t *testing.T) {
	var s *alpenglowSwitchSweeper
	assert.Nil(t, s.sweep(map[uint64]solana.Hash{1: swHash(1)}, 0, 1))
}

func TestSweepNewlyConsumedSkipRechecksUnchangedDecision(t *testing.T) {
	q := &fakeChainQuery{
		certified: map[uint64]alpenglow.BlockID{101: {Slot: 101, Hash: swHash(1)}},
		version:   1,
	}
	s := newTestSweeper(q)
	executed := map[uint64]solana.Hash{100: swHash(100)}
	require.Nil(t, s.sweep(executed, 99, 100))

	// A skip already queued by the source can be consumed after the same
	// certificate was checked. Advancing the consumed frontier must recheck it.
	executed[101] = solana.Hash{}
	sw := s.sweep(executed, 99, 101)
	require.NotNil(t, sw)
	require.Equal(t, uint64(101), sw.Slot)
	require.True(t, sw.Executed.IsZero())
	require.Equal(t, swHash(1), sw.Certified)
}

func TestWaitForAlpenglowReplayInputRepairsSkippedParentWhileChildHeld(t *testing.T) {
	for _, tc := range []struct {
		name         string
		lateAncestry bool
		pollOnly     bool
	}{
		{name: "certificate notification"},
		{name: "ancestry notification", lateAncestry: true},
		{name: "polling fallback", pollOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := alpenglow.NewChainTracker()
			parent := alpenglow.BlockID{Slot: 2795461, Hash: swHash(61)}
			child := alpenglow.BlockID{Slot: 2795464, Hash: swHash(64)}
			for slot := uint64(2795461); slot <= 2795463; slot++ {
				_, err := tracker.ObserveCertificate(alpenglow.Certificate{
					Type: alpenglow.CertificateSkip, Slot: slot, SignatureVerified: true,
				})
				require.NoError(t, err)
			}
			observeAncestry := func() {
				tracker.ObserveReplayBlock(alpenglow.ReplayBlockObservation{
					Block: child, ParentSlot: parent.Slot, ParentHash: parent.Hash,
				})
			}
			finalizeChild := func() {
				_, err := tracker.ObserveCertificate(alpenglow.Certificate{
					Type: alpenglow.CertificateFinalizeFast, Slot: child.Slot, BlockHash: child.Hash, SignatureVerified: true,
				})
				require.NoError(t, err)
				require.NoError(t, tracker.ObserveFinalized(child, alpenglow.CertificateFinalizeFast))
			}
			if tc.lateAncestry {
				finalizeChild()
			} else {
				observeAncestry()
			}
			executed := map[uint64]solana.Hash{
				2795460: swHash(60), 2795461: {}, 2795462: {}, 2795463: {},
			}
			s := newAlpenglowSwitchSweeper(&trackerChainEngine{trackerChainQuery: &trackerChainQuery{tracker}})
			// Clear setup notifications before starting replay so only the new
			// decision can wake it. Production must never drain after a sweep.
			select {
			case <-s.decisionChanges:
			default:
			}
			changes := s.decisionChanges
			pollInterval := time.Hour // Only notification can beat the test deadline.
			if tc.pollOnly {
				changes = nil
				pollInterval = 5 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			waiting := make(chan struct{}, 1)
			returned := make(chan *CertifiedSwitch, 1)
			// No block/parent-switch channels: the emitter is holding the child
			// until its missing parent replays. Exercise the real source select.
			source := &blockstream.BlockSource{}
			go func() {
				_, _, sw := waitForAlpenglowReplayInput(ctx,
					func(waitCtx context.Context, changes <-chan struct{}) (*b.Block, *blockstream.AlpenglowParentSwitch, bool) {
						select {
						case waiting <- struct{}{}:
						default:
						}
						return source.NextBlockOrAlpenglowEvent(waitCtx, changes)
					},
					func() *CertifiedSwitch { return s.sweep(executed, 2795459, 2795463) },
					changes, pollInterval)
				returned <- sw
			}()
			select {
			case <-waiting:
			case <-ctx.Done():
				t.Fatal("replay did not start waiting")
			}
			if tc.lateAncestry {
				observeAncestry() // No new certificate accompanies this decision.
			} else {
				finalizeChild()
			}
			select {
			case sw := <-returned:
				require.NotNil(t, sw, "decision correction must wake replay before another block arrives")
				require.Equal(t, parent.Slot, sw.Slot)
				require.Equal(t, parent.Hash, sw.Certified)
				require.True(t, sw.Executed.IsZero())
				require.False(t, parentSwitchNeedsStateUnwind(sw.Slot, 2795460), "the skipped suffix has no account state to unwind")
			case <-ctx.Done():
				t.Fatal("certificate-selected skipped parent left replay stalled")
			}
		})
	}
}

func TestWaitForAlpenglowReplayInputPreservesSourceResults(t *testing.T) {
	block := &b.Block{Slot: 101}
	parentSwitch := &blockstream.AlpenglowParentSwitch{SwitchSlot: 101}
	for _, tc := range []struct {
		name         string
		block        *b.Block
		parentSwitch *blockstream.AlpenglowParentSwitch
	}{
		{name: "block", block: block},
		{name: "parent switch", parentSwitch: parentSwitch},
		{name: "closed source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			gotBlock, gotParent, gotCertified := waitForAlpenglowReplayInput(context.Background(),
				func(context.Context, <-chan struct{}) (*b.Block, *blockstream.AlpenglowParentSwitch, bool) {
					calls++
					return tc.block, tc.parentSwitch, false
				}, func() *CertifiedSwitch { return nil }, nil, time.Second)
			require.Same(t, tc.block, gotBlock)
			require.Same(t, tc.parentSwitch, gotParent)
			require.Nil(t, gotCertified)
			require.Equal(t, 1, calls)
		})
	}
}

func TestWaitForAlpenglowReplayInputDecisionBetweenSweepAndWait(t *testing.T) {
	tracker := alpenglow.NewChainTracker()
	s := newAlpenglowSwitchSweeper(&trackerChainEngine{trackerChainQuery: &trackerChainQuery{tracker}})
	executed := map[uint64]solana.Hash{101: {}}
	source := &blockstream.BlockSource{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	calls := 0
	block, parentSwitch, sw := waitForAlpenglowReplayInput(ctx,
		func(waitCtx context.Context, changes <-chan struct{}) (*b.Block, *blockstream.AlpenglowParentSwitch, bool) {
			calls++
			// The sweep just found no contradiction. Publish one before the
			// source enters select: the buffered notification must survive.
			_, err := tracker.ObserveCertificate(alpenglow.Certificate{
				Type: alpenglow.CertificateNotarize, Slot: 101, BlockHash: swHash(1), SignatureVerified: true,
			})
			require.NoError(t, err)
			return source.NextBlockOrAlpenglowEvent(waitCtx, changes)
		}, func() *CertifiedSwitch { return s.sweep(executed, 100, 101) }, s.decisionChanges, time.Hour)
	require.Nil(t, block)
	require.Nil(t, parentSwitch)
	require.NotNil(t, sw, "change between sweep and wait must not fall back to the timer")
	require.Equal(t, uint64(101), sw.Slot)
	require.Equal(t, swHash(1), sw.Certified)
	require.Equal(t, 1, calls)
}

func TestWaitForAlpenglowReplayInputNotificationWithoutContradiction(t *testing.T) {
	tracker := alpenglow.NewChainTracker()
	s := newAlpenglowSwitchSweeper(&trackerChainEngine{trackerChainQuery: &trackerChainQuery{tracker}})
	executed := map[uint64]solana.Hash{101: swHash(1)}
	source := &blockstream.BlockSource{}
	want := &b.Block{Slot: 102}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	calls, sweeps := 0, 0
	block, parentSwitch, sw := waitForAlpenglowReplayInput(ctx,
		func(waitCtx context.Context, changes <-chan struct{}) (*b.Block, *blockstream.AlpenglowParentSwitch, bool) {
			calls++
			if calls == 1 {
				// Several future decisions coalesce into one notification and
				// do not contradict the consumed frontier. Replay must keep waiting.
				for slot := uint64(103); slot < 110; slot++ {
					_, err := tracker.ObserveCertificate(alpenglow.Certificate{
						Type: alpenglow.CertificateSkip, Slot: slot, SignatureVerified: true,
					})
					require.NoError(t, err)
				}
				return source.NextBlockOrAlpenglowEvent(waitCtx, changes)
			}
			return want, nil, false
		}, func() *CertifiedSwitch {
			sweeps++
			return s.sweep(executed, 100, 101)
		}, s.decisionChanges, time.Hour)
	require.Same(t, want, block, "notification must not be mistaken for a closed source")
	require.Nil(t, parentSwitch)
	require.Nil(t, sw)
	require.Equal(t, 2, calls)
	require.Equal(t, 2, sweeps)
}

func TestWaitForAlpenglowReplayInputWithoutSweepDisablesNotifications(t *testing.T) {
	changes := make(chan struct{}, 1)
	changes <- struct{}{}
	want := &b.Block{Slot: 101}
	block, parentSwitch, sw := waitForAlpenglowReplayInput(context.Background(),
		func(ctx context.Context, wake <-chan struct{}) (*b.Block, *blockstream.AlpenglowParentSwitch, bool) {
			require.Nil(t, wake)
			_, deadline := ctx.Deadline()
			require.False(t, deadline, "ordinary block waits do not need polling")
			return want, nil, false
		}, nil, changes, time.Second)
	require.Same(t, want, block)
	require.Nil(t, parentSwitch)
	require.Nil(t, sw)
	require.Len(t, changes, 1)
}

func TestWaitForAlpenglowReplayInputHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	block, parentSwitch, certifiedSwitch := waitForAlpenglowReplayInput(ctx,
		func(waitCtx context.Context, _ <-chan struct{}) (*b.Block, *blockstream.AlpenglowParentSwitch, bool) {
			cancel()
			<-waitCtx.Done()
			return nil, nil, false
		}, func() *CertifiedSwitch { return nil }, nil, time.Second)
	require.Nil(t, block)
	require.Nil(t, parentSwitch)
	require.Nil(t, certifiedSwitch)
}
