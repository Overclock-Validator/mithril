package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
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

func (q *trackerChainQuery) ChainDecisionVersion() uint64 { return q.DecisionVersion() }

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

// The sweep is gated on the decision version and bounded by the window. The
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
	// ...but with no decision change the next sweep is a no-op even though the
	// contradiction persists (the caller acts on the first report).
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
