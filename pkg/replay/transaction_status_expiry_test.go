package replay

import (
	"encoding/binary"
	"fmt"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestTransactionStatusBatchExpiryMatchesPerKeyRemoval(t *testing.T) {
	for seed := int64(0); seed < 100; seed++ {
		rng := rand.New(rand.NewSource(seed))
		fast, ref := NewTransactionStatusCache(), NewTransactionStatusCache()
		var nodes []*transactionStatusNode
		for slot := 0; slot < 40; slot++ {
			d := make(transactionStatusDelta)
			for j := 0; j < 6; j++ {
				h := solana.Hash{byte(rng.Intn(12))}
				g := &transactionStatusGroup{keyIndex: h[0], keys: make(map[transactionStatusKey]struct{})}
				for k := 0; k < rng.Intn(30); k++ {
					g.keys[transactionStatusKey{byte(rng.Intn(40))}] = struct{}{}
				}
				d[h] = g
			}
			nodes = append(nodes, &transactionStatusNode{slot: uint64(slot), delta: d})
			require.NoError(t, fast.addDeltaVisibleLocked(d))
			require.NoError(t, ref.addDeltaVisibleLocked(d))
		}
		cut := 1 + rng.Intn(len(nodes)-1)
		fast.expireVisibleLocked(nodes[:cut], nodes[cut:])
		for _, n := range nodes[:cut] {
			ref.removeDeltaVisibleLocked(n.delta)
		}
		require.Equal(t, ref.visible, fast.visible, "seed %d", seed)
		for i := len(nodes) - 1; i >= cut; i-- {
			fast.removeDeltaVisibleLocked(nodes[i].delta)
			ref.removeDeltaVisibleLocked(nodes[i].delta)
		}
		require.Equal(t, ref.visible, fast.visible, "unwind seed %d", seed)
	}
}

func TestTransactionStatusBatchExpiryPinnedViewsAndSnapshot(t *testing.T) {
	c := NewTransactionStatusCache()
	old := statusCacheTestTransaction(1, 1, 1)
	keep := statusCacheTestTransaction(2, 2, 2)
	require.NoError(t, c.CommitBlock(statusCacheTestBlock(1, old)))
	for slot := uint64(2); slot <= maxTransactionStatusRoots+1; slot++ {
		blk := statusCacheTestBlock(slot)
		if slot == maxTransactionStatusRoots+1 {
			blk.Transactions = append(blk.Transactions, keep)
		}
		require.NoError(t, c.CommitBlock(blk))
	}
	pinned := c.View()
	snapshot, err := c.CaptureSnapshotThrough(maxTransactionStatusRoots + 1)
	require.NoError(t, err)
	before, err := snapshot.MarshalBinary()
	require.NoError(t, err)
	c.coverageComplete = false // Exercise completion once 300 banks become rooted.
	c.Root(maxTransactionStatusRoots + 1)
	after, err := snapshot.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, before, after)
	found, err := pinned.ContainsTransaction(old)
	require.NoError(t, err)
	require.True(t, found)
	found, err = c.View().ContainsTransaction(old)
	require.NoError(t, err)
	require.False(t, found)
	require.NoError(t, c.ValidateBlock(statusCacheTestBlock(maxTransactionStatusRoots+2, old)))
	require.Error(t, c.ValidateBlock(statusCacheTestBlock(maxTransactionStatusRoots+2, keep)))
	blob, err := c.SnapshotThrough(maxTransactionStatusRoots + 1)
	require.NoError(t, err)
	restored, err := NewTransactionStatusCacheFromSnapshot(blob)
	require.NoError(t, err)
	require.NoError(t, restored.ValidateBlock(statusCacheTestBlock(maxTransactionStatusRoots+2, old)))
	require.Error(t, restored.ValidateBlock(statusCacheTestBlock(maxTransactionStatusRoots+2, keep)))
	require.Error(t, c.Unwind(maxTransactionStatusRoots+1))
}

func BenchmarkTransactionStatusBatchExpiry(b *testing.B) {
	for _, shape := range []string{"four-bank-groups", "one-expired-group", "crossing-group"} {
		for _, legacy := range []bool{true, false} {
			b.Run(fmt.Sprintf("%s/legacy=%t", shape, legacy), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					c := NewTransactionStatusCache()
					var expired, retained []*transactionStatusNode
					for slot := 0; slot < 129; slot++ {
						var h solana.Hash
						if shape == "four-bank-groups" || (shape == "one-expired-group" && slot == 128) {
							binary.LittleEndian.PutUint64(h[:], uint64(slot/4+1))
						}
						g := &transactionStatusGroup{keys: make(map[transactionStatusKey]struct{})}
						for k := 0; k < 33760; k++ {
							var key transactionStatusKey
							binary.LittleEndian.PutUint64(key[:], uint64(slot*33760+k))
							g.keys[key] = struct{}{}
						}
						n := &transactionStatusNode{delta: transactionStatusDelta{h: g}}
						if err := c.addDeltaVisibleLocked(n.delta); err != nil {
							b.Fatal(err)
						}
						if slot < 128 {
							expired = append(expired, n)
						} else {
							retained = append(retained, n)
						}
					}
					b.StartTimer()
					if legacy {
						for _, n := range expired {
							c.removeDeltaVisibleLocked(n.delta)
						}
					} else {
						c.expireVisibleLocked(expired, retained)
					}
					b.StopTimer()
					if len(c.visible) != 1 {
						b.Fatal("retained group missing")
					}
				}
			})
		}
	}
}
