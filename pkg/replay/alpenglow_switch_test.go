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
	certified map[uint64]alpenglow.BlockID
	skipped   map[uint64]bool
	version   uint64
}

func (f *fakeChainQuery) CertifiedBlockAt(slot uint64) (alpenglow.BlockID, alpenglow.CertificateType, bool) {
	if b, ok := f.certified[slot]; ok {
		return b, alpenglow.CertificateNotarize, true
	}
	return alpenglow.BlockID{}, "", false
}
func (f *fakeChainQuery) SkipCertifiedAt(slot uint64) bool { return f.skipped[slot] }
func (f *fakeChainQuery) ChainDecisionVersion() uint64     { return f.version }

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

// A skip cert over an executed slot switches with Skip=true.
func TestSweepSkipOverExecuted(t *testing.T) {
	q := &fakeChainQuery{certified: map[uint64]alpenglow.BlockID{}, skipped: map[uint64]bool{101: true}, version: 1}
	s := newTestSweeper(q)
	executed := map[uint64]solana.Hash{101: swHash(1)}
	sw := s.sweep(executed, 100, 101)
	require.NotNil(t, sw)
	assert.True(t, sw.Skip)
	assert.Equal(t, uint64(101), sw.Slot)
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
