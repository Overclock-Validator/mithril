package harmonic

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// uses describes a bundle's accounts: keys prefixed with w are written, the
// rest read.
func uses(keys ...string) accountSet {
	return func(fn func(solana.PublicKey, bool)) {
		for _, k := range keys {
			var key solana.PublicKey
			copy(key[:], k[1:])
			fn(key, k[0] == 'w')
		}
	}
}

// scheduleOf adds bundles 1..n, the ith using accounts[i-1].
func scheduleOf(accounts ...accountSet) (*schedule, []*bundleNode) {
	s := &schedule{}
	nodes := make([]*bundleNode, len(accounts))
	for i, a := range accounts {
		nodes[i] = &bundleNode{seq: uint64(i + 1)}
		s.add(nodes[i], a)
	}
	return s, nodes
}

// drain returns the ready bundles' sequence numbers, in order.
func drain(s *schedule) []uint64 {
	var seqs []uint64
	for n := s.next(); n != nil; n = s.next() {
		seqs = append(seqs, n.seq)
	}
	return seqs
}

func TestScheduleOrdersConflictsTransitively(t *testing.T) {
	// 1 writes x; 2 writes x and y; 3 writes y; 4 shares nothing. 3 has no
	// account in common with 1 but must still wait for it, through 2.
	s, nodes := scheduleOf(uses("wx"), uses("wx", "wy"), uses("wy"), uses("wz"))
	assert.Equal(t, []uint64{1, 4}, drain(s), "4 overtakes the waiting 2 and 3")
	s.finish(nodes[3])
	assert.Empty(t, drain(s))
	s.finish(nodes[0])
	assert.Equal(t, []uint64{2}, drain(s))
	s.finish(nodes[1])
	assert.Equal(t, []uint64{3}, drain(s))
}

func TestScheduleLockRules(t *testing.T) {
	t.Run("readers share", func(t *testing.T) {
		s, _ := scheduleOf(uses("rx"), uses("rx"), uses("rx"))
		assert.Equal(t, []uint64{1, 2, 3}, drain(s))
	})
	t.Run("a write waits for every earlier reader", func(t *testing.T) {
		s, nodes := scheduleOf(uses("rx"), uses("rx"), uses("wx"))
		assert.Equal(t, []uint64{1, 2}, drain(s))
		s.finish(nodes[1])
		assert.Empty(t, drain(s))
		s.finish(nodes[0])
		assert.Equal(t, []uint64{3}, drain(s))
	})
	t.Run("a read waits for the earlier writer, not for other readers", func(t *testing.T) {
		s, nodes := scheduleOf(uses("wx"), uses("rx"), uses("rx"))
		assert.Equal(t, []uint64{1}, drain(s))
		s.finish(nodes[0])
		assert.Equal(t, []uint64{2, 3}, drain(s))
	})
	t.Run("a write after a write", func(t *testing.T) {
		s, nodes := scheduleOf(uses("wx"), uses("wx"))
		assert.Equal(t, []uint64{1}, drain(s))
		s.finish(nodes[0])
		assert.Equal(t, []uint64{2}, drain(s))
	})
	t.Run("a bundle sharing several accounts depends once", func(t *testing.T) {
		s, nodes := scheduleOf(uses("wx", "wy", "rz"), uses("wx", "wy", "wz"))
		assert.Equal(t, []uint64{1}, drain(s))
		assert.Equal(t, 1, nodes[1].deps)
		s.finish(nodes[0])
		assert.Equal(t, []uint64{2}, drain(s))
	})
	t.Run("only unfinished bundles are depended on", func(t *testing.T) {
		s, nodes := scheduleOf(uses("wx"))
		require.Equal(t, []uint64{1}, drain(s))
		s.finish(nodes[0])
		late := &bundleNode{seq: 2}
		s.add(late, uses("wx"))
		assert.Equal(t, []uint64{2}, drain(s))
	})
}

func TestScheduleRetryAndClose(t *testing.T) {
	s, nodes := scheduleOf(uses("wx"), uses("wx"), uses("wy"))
	require.Equal(t, []uint64{1, 3}, drain(s))
	s.retryLater(nodes[0])
	assert.Empty(t, drain(s), "a retried bundle keeps its locks")
	s.finish(nodes[2])
	s.requeueRetries()
	assert.Equal(t, []uint64{1}, drain(s))

	s.retryLater(nodes[0])
	assert.Equal(t, 2, s.close(), "the retry and the waiting bundle are dropped")
	s.finish(nodes[0])
	assert.Nil(t, s.next(), "nothing is scheduled after close")
	assert.Zero(t, s.close())
}
