package scheduler

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func retainedTestEntry(id, reward uint64) *entry {
	e := &entry{tx: &solana.Transaction{}, wire: []byte{1, 2, 3}, seq: id, reward: reward}
	binary.LittleEndian.PutUint64(e.messageHash[:], id)
	return e
}

// Check both membership and backing-array references: shrinking a slice alone
// must not leave transaction payloads reachable outside its visible length.
func assertBufferIndexes(t *testing.T, b *Buffer) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	require.Equal(t, b.alive, len(b.byHash))
	require.Equal(t, b.alive, len(b.max))
	require.Equal(t, b.alive, len(b.min))
	require.LessOrEqual(t, b.alive, b.capacity)
	for i, e := range b.max {
		require.True(t, e.alive)
		require.Equal(t, i, e.maxIndex)
		require.Same(t, e, b.byHash[e.messageHash])
		require.Same(t, e, b.min[e.minIndex])
		if i > 0 {
			require.False(t, b.max.Less(i, (i-1)/2))
		}
	}
	for i, e := range b.min {
		require.Equal(t, i, e.minIndex)
		require.Same(t, e, b.max[e.maxIndex])
		if i > 0 {
			require.False(t, b.min.Less(i, (i-1)/2))
		}
	}
	for _, backing := range [][]*entry{b.max[:cap(b.max)], b.min[:cap(b.min)]} {
		for _, e := range backing[b.alive:] {
			require.Nil(t, e)
		}
	}
}

func TestBufferConsumedEntriesReleaseBothHeapReferences(t *testing.T) {
	b := NewBuffer(256)
	pinned := retainedTestEntry(0, 1)
	b.Insert(pinned)
	for i := uint64(1); i <= 100000; i++ {
		e := retainedTestEntry(i, 2)
		result, _ := b.Insert(e)
		require.Equal(t, InsertAccepted, result)
		require.Same(t, e, b.PopMax())
		// The consumer still owns usable payloads after index removal.
		require.NotNil(t, e.tx)
		require.Equal(t, []byte{1, 2, 3}, e.wire)
		if i%1000 == 0 {
			assertBufferIndexes(t, b)
		}
	}
	b.Cleanup(func(*entry) bool { return false })
	assertBufferIndexes(t, b)
	t.Logf("capacity=%d active=%d max_refs=%d min_refs=%d", b.capacity, b.Len(), len(b.max), len(b.min))
	require.Same(t, pinned, b.PopMax())
	assertBufferIndexes(t, b)
}

func TestBufferEvictionAndCleanupReleaseBothHeapReferences(t *testing.T) {
	b := NewBuffer(2)
	pinned := retainedTestEntry(0, 1000000)
	b.Insert(pinned)
	previous := retainedTestEntry(1, 1)
	b.Insert(previous)
	for i := uint64(2); i < 10000; i++ {
		next := retainedTestEntry(i, i)
		result, evicted := b.Insert(next)
		require.Equal(t, InsertAccepted, result)
		require.Same(t, previous, evicted)
		require.False(t, evicted.alive)
		require.Equal(t, -1, evicted.maxIndex)
		require.Equal(t, -1, evicted.minIndex)
		previous = next
		if i%100 == 0 {
			assertBufferIndexes(t, b)
		}
	}
	require.Equal(t, 1, b.Cleanup(func(e *entry) bool { return e != pinned }))
	assertBufferIndexes(t, b)
	require.Same(t, pinned, b.PopMax())
	assertBufferIndexes(t, b)
}

func TestBufferRepeatedRebufferPreservesNewHigherPriorityArrivals(t *testing.T) {
	s := New(nil)
	s.bankGen = 1
	skipped := retainedTestEntry(1, 10)
	skipped.skipGen = s.bankGen
	s.buffer.Insert(skipped)
	for i := uint64(2); i < 10002; i++ {
		low := retainedTestEntry(i, 1)
		s.buffer.Insert(low)
		picked, retry := s.popSchedulable(s.bankGen)
		require.Same(t, low, picked)
		require.Equal(t, []*entry{skipped}, retry)
		for _, e := range retry {
			s.rebuffer(e)
		}
		if i%100 == 0 {
			assertBufferIndexes(t, s.buffer)
		}
	}
	// Preserve the existing scan/retry policy when a higher-fee packet arrives.
	high := retainedTestEntry(20000, 20)
	s.buffer.Insert(high)
	picked, retry := s.popSchedulable(s.bankGen)
	require.Same(t, high, picked)
	require.Empty(t, retry)
	// A later bank may retry the previously skipped transaction.
	s.bankGen++
	picked, retry = s.popSchedulable(s.bankGen)
	require.Same(t, skipped, picked)
	require.Empty(t, retry)
	assertBufferIndexes(t, s.buffer)
}

func TestBufferConcurrentInsertRemovalAndCleanup(t *testing.T) {
	b := NewBuffer(64)
	var workers sync.WaitGroup
	for worker := uint64(0); worker < 4; worker++ {
		workers.Go(func() {
			for i := uint64(0); i < 2000; i++ {
				id := worker*2000 + i
				b.Insert(retainedTestEntry(id, id%17))
			}
		})
	}
	workers.Go(func() {
		for i := 0; i < 8000; i++ {
			b.PopMax()
		}
	})
	workers.Go(func() {
		for i := 0; i < 100; i++ {
			b.Cleanup(func(e *entry) bool { return e.seq%3 == 0 })
		}
	})
	workers.Wait()
	assertBufferIndexes(t, b)
	for b.PopMax() != nil {
	}
	assertBufferIndexes(t, b)
}
