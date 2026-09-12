package scheduler

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func testEntry(reward, seq uint64, hashByte byte) *entry {
	var mh [32]byte
	mh[0] = hashByte
	return &entry{
		tx:          &solana.Transaction{},
		wireSize:    1,
		messageHash: mh,
		reward:      reward,
		seq:         seq,
	}
}

func TestBufferPopsHighestReward(t *testing.T) {
	b := NewBuffer(8)
	_, _ = b.Insert(testEntry(10, 1, 1))
	_, _ = b.Insert(testEntry(50, 2, 2))
	_, _ = b.Insert(testEntry(20, 3, 3))

	require.Equal(t, uint64(50), b.PopMax().reward)
	require.Equal(t, uint64(20), b.PopMax().reward)
	require.Equal(t, uint64(10), b.PopMax().reward)
	require.Nil(t, b.PopMax())
}

func TestBufferTieBreaksOlderFirst(t *testing.T) {
	b := NewBuffer(8)
	_, _ = b.Insert(testEntry(10, 1, 1))
	_, _ = b.Insert(testEntry(10, 2, 2))
	require.Equal(t, uint64(1), b.PopMax().seq)
	require.Equal(t, uint64(2), b.PopMax().seq)
}

func TestBufferRejectsDuplicate(t *testing.T) {
	b := NewBuffer(8)
	res, _ := b.Insert(testEntry(10, 1, 1))
	require.Equal(t, InsertAccepted, res)
	res, _ = b.Insert(testEntry(99, 2, 1))
	require.Equal(t, InsertDuplicate, res)
	require.Equal(t, 1, b.Len())
}

func TestBufferEvictsLowestRewardAtCapacity(t *testing.T) {
	b := NewBuffer(2)
	res, _ := b.Insert(testEntry(10, 1, 1))
	require.Equal(t, InsertAccepted, res)
	res, _ = b.Insert(testEntry(20, 2, 2))
	require.Equal(t, InsertAccepted, res)

	res, evicted := b.Insert(testEntry(5, 3, 3))
	require.Equal(t, InsertRejectedCapacity, res)
	require.Nil(t, evicted)
	require.Equal(t, 2, b.Len())

	res, evicted = b.Insert(testEntry(30, 4, 4))
	require.Equal(t, InsertAccepted, res)
	require.NotNil(t, evicted)
	require.Equal(t, uint64(10), evicted.reward)
	require.Equal(t, 2, b.Len())

	require.Equal(t, uint64(30), b.PopMax().reward)
	require.Equal(t, uint64(20), b.PopMax().reward)
}

func TestBufferCleanup(t *testing.T) {
	b := NewBuffer(8)
	_, _ = b.Insert(testEntry(10, 1, 1))
	_, _ = b.Insert(testEntry(20, 2, 2))
	dropped := b.Cleanup(func(e *entry) bool { return e.messageHash[0] == 1 })
	require.Equal(t, 1, dropped)
	require.Equal(t, 1, b.Len())
	require.Equal(t, byte(2), b.PopMax().messageHash[0])
}
