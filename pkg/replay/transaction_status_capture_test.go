package replay

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Keep the pre-split selection/metadata calculation as a differential oracle.
// Use the original uncached wire encoder to check byte-for-byte compatibility.
func legacyStatusSnapshotForTest(c *TransactionStatusCache, through uint64) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	nodes := c.nodesThroughLocked(through)
	if len(nodes) > maxTransactionStatusRoots {
		nodes = nodes[len(nodes)-maxTransactionStatusRoots:]
	}
	rooted := uint32(c.rootedSinceSeed) + uint32(c.countNodesBetweenLocked(c.rootedThrough, through))
	complete := c.coverageComplete || rooted >= maxTransactionStatusRoots
	if rooted > maxTransactionStatusRoots {
		rooted = maxTransactionStatusRoots
	}
	return marshalTransactionStatusNodesUncached(nodes, uint16(rooted), complete, c.coverageFromGenesis)
}

func importedStatusCacheForTest(t *testing.T) *TransactionStatusCache {
	t.Helper()
	roots := make([]txstatus.SnapshotSlotDelta, maxTransactionStatusRoots)
	for i := range roots {
		roots[i] = txstatus.SnapshotSlotDelta{Slot: uint64(i + 1), IsRoot: true}
	}
	c, err := NewTransactionStatusCacheFromAgaveSnapshot(roots, maxTransactionStatusRoots)
	require.NoError(t, err)
	return c
}

func captureTestBlock(slot uint64, branch byte) *b.Block {
	tx := statusCacheTestTransaction(1, 2, branch)
	data := make([]byte, 9)
	binary.LittleEndian.PutUint64(data, slot)
	data[8] = branch
	tx.Message.Instructions[0].Data = data
	return statusCacheTestBlock(slot, tx)
}

func TestTransactionStatusCaptureSurvivesConcurrentPruneAndUnwind(t *testing.T) {
	c := importedStatusCacheForTest(t)
	for slot := uint64(301); slot <= 350; slot++ {
		require.NoError(t, c.CommitBlock(captureTestBlock(slot, 1)))
	}
	want, err := legacyStatusSnapshotForTest(c, 320)
	require.NoError(t, err)
	captured, err := c.CaptureSnapshotThrough(320)
	require.NoError(t, err)
	view := captured.(*transactionStatusSnapshot)
	require.Len(t, view.nodes, maxTransactionStatusRoots)
	require.Equal(t, uint64(21), view.nodes[0].slot)
	require.Equal(t, uint64(320), view.nodes[len(view.nodes)-1].slot)
	for _, node := range view.nodes {
		require.Nil(t, node.parent, "capture retained excluded ancestry")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	errs := make(chan error, 1)
	go func() {
		defer wg.Done()
		for slot := uint64(351); slot <= 750; slot++ {
			if err := c.CommitBlock(captureTestBlock(slot, 1)); err != nil {
				errs <- err
				return
			}
			c.Root(slot - 20)
		}
		if err := c.Unwind(741); err != nil {
			errs <- err
			return
		}
		for slot := uint64(741); slot <= 755; slot++ {
			if err := c.CommitBlock(captureTestBlock(slot, 2)); err != nil {
				errs <- err
				return
			}
		}
	}()
	for i := 0; i < 50; i++ {
		got, err := captured.MarshalBinary()
		if err != nil || string(want) != string(got) {
			t.Errorf("captured bytes changed during replay: %v", err)
			break
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	got, err := captured.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, want, got)
	restored, err := NewTransactionStatusCacheFromSnapshot(got)
	require.NoError(t, err)
	require.Equal(t, uint64(320), restored.RootedThrough())
	require.True(t, restored.CoverageComplete())
	retry := statusCacheTestBlock(321, captureTestBlock(301, 1).Transactions[0])
	require.Error(t, restored.ValidateBlock(retry), "captured ancestor was forgotten")
	// A bank after the capture's through-slot must not leak into recovery.
	future := statusCacheTestBlock(321, captureTestBlock(350, 1).Transactions[0])
	require.NoError(t, restored.ValidateBlock(future))
}

func TestTransactionStatusCapturePreservesCoverageAndOwnedBytes(t *testing.T) {
	for _, complete := range []bool{false, true} {
		c := newTransactionStatusCache(complete)
		// Exercise metadata selection without changing its pre-existing rules.
		for slot := uint64(1); slot <= 310; slot++ {
			c.tip = &transactionStatusNode{slot: slot, parent: c.tip,
				delta: transactionStatusDelta{solana.Hash{1}: {keyIndex: 7, keys: map[transactionStatusKey]struct{}{{byte(slot), byte(slot >> 8)}: {}}}}}
		}
		for _, through := range []uint64{0, 1, 299, 300, 310, 400} {
			want, err := legacyStatusSnapshotForTest(c, through)
			require.NoError(t, err)
			view, err := c.CaptureSnapshotThrough(through)
			require.NoError(t, err)
			got, err := view.MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, want, got)
			got[0] ^= 0xff
			again, err := view.MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, want, again, "caller mutated the captured data through encoded bytes")
		}
	}
	var absent *TransactionStatusCache
	view, err := absent.CaptureSnapshotThrough(1)
	require.NoError(t, err)
	require.Nil(t, view)
}

func TestTransactionStatusCaptureEncodingDoesNotLockLiveCache(t *testing.T) {
	c := importedStatusCacheForTest(t)
	require.NoError(t, c.CommitBlock(captureTestBlock(301, 1)))
	view, err := c.CaptureSnapshotThrough(301)
	require.NoError(t, err)
	c.mu.Lock()
	done := make(chan error, 1)
	go func() { _, err := view.MarshalBinary(); done <- err }()
	select {
	case err := <-done:
		c.mu.Unlock()
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		c.mu.Unlock()
		t.Fatal("checkpoint encoding waited for the live cache lock")
	}
}

func marshalTransactionStatusNodesUncached(nodes []*transactionStatusNode, rootedSinceSeed uint16, complete bool, coverageFromGenesis bool) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(transactionStatusSnapshotMagic[:])
	flags := byte(0)
	if complete {
		flags = 1
	}
	if coverageFromGenesis {
		flags |= 2
	}
	buf.WriteByte(flags)
	_ = binary.Write(&buf, binary.LittleEndian, rootedSinceSeed)
	_ = binary.Write(&buf, binary.LittleEndian, uint16(len(nodes)))
	for _, node := range nodes {
		_ = binary.Write(&buf, binary.LittleEndian, node.slot)
		nodeFlags := byte(0)
		if node.hasBlockID {
			nodeFlags = 1
		}
		buf.WriteByte(nodeFlags)
		if node.hasBlockID {
			buf.Write(node.blockID[:])
		}
		blockhashes := make([]solana.Hash, 0, len(node.delta))
		for blockhash := range node.delta {
			blockhashes = append(blockhashes, blockhash)
		}
		sort.Slice(blockhashes, func(i, j int) bool {
			return bytes.Compare(blockhashes[i][:], blockhashes[j][:]) < 0
		})
		_ = binary.Write(&buf, binary.LittleEndian, uint32(len(blockhashes)))
		for _, blockhash := range blockhashes {
			group := node.delta[blockhash]
			buf.Write(blockhash[:])
			buf.WriteByte(group.keyIndex)
			keys := make([]transactionStatusKey, 0, len(group.keys))
			for key := range group.keys {
				keys = append(keys, key)
			}
			sort.Slice(keys, func(i, j int) bool {
				return bytes.Compare(keys[i][:], keys[j][:]) < 0
			})
			_ = binary.Write(&buf, binary.LittleEndian, uint32(len(keys)))
			for _, key := range keys {
				buf.Write(key[:])
			}
		}
	}
	return buf.Bytes(), nil
}

func TestTransactionStatusEncodingSharedAcrossCaptureAndPrune(t *testing.T) {
	for _, warmBeforePrune := range []bool{false, true} {
		t.Run(fmt.Sprintf("warm=%t", warmBeforePrune), func(t *testing.T) {
			c := importedStatusCacheForTest(t)
			for slot := uint64(301); slot <= 305; slot++ {
				require.NoError(t, c.CommitBlock(captureTestBlock(slot, 1)))
			}
			first, err := c.CaptureSnapshotThrough(304)
			require.NoError(t, err)
			pinned := first.(*transactionStatusSnapshot)
			want, err := legacyStatusSnapshotForTest(c, 304)
			require.NoError(t, err)
			if warmBeforePrune {
				got, err := first.MarshalBinary()
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
			// Force relinking of retained nodes after the snapshot has copied
			// their headers, including the still-unencoded case.
			c.Root(305)
			second, err := c.CaptureSnapshotThrough(305)
			require.NoError(t, err)
			current := second.(*transactionStatusSnapshot)
			caches := make(map[uint64]*transactionStatusNodeEncoding)
			for _, node := range pinned.nodes {
				caches[node.slot] = node.encodingCache()
			}
			for _, node := range current.nodes {
				if prior := caches[node.slot]; prior != nil {
					require.Same(t, prior, node.encodingCache())
				}
			}
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					got, err := first.MarshalBinary()
					assert.NoError(t, err)
					assert.Equal(t, want, got)
					// Mutate the node body as well as the header; neither may
					// alias the memoized node data or another caller's result.
					clear(got)
				}()
			}
			wg.Wait()
			currentWant, err := legacyStatusSnapshotForTest(c, 305)
			require.NoError(t, err)
			got, err := second.MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, currentWant, got)
			restored, err := NewTransactionStatusCacheFromSnapshot(got)
			require.NoError(t, err)
			roundTrip, err := restored.SnapshotThrough(305)
			require.NoError(t, err)
			require.Equal(t, got, roundTrip)
		})
	}
}

func TestTransactionStatusEncodingMatchesOriginalWireFormat(t *testing.T) {
	// Deliberately unsorted groups/keys, nonzero offsets, empty deltas and
	// mixed block-ID presence exercise every independently cached field.
	nodes := []*transactionStatusNode{
		{slot: 3, hasBlockID: true, blockID: solana.Hash{9}, delta: transactionStatusDelta{
			solana.Hash{7}: {keyIndex: 11, keys: map[transactionStatusKey]struct{}{{8}: {}, {1}: {}, {4}: {}}},
			solana.Hash{1}: {keyIndex: 2, keys: map[transactionStatusKey]struct{}{{9}: {}, {2}: {}}},
		}},
		{slot: 5},
		{slot: 8, delta: transactionStatusDelta{solana.Hash{3}: {keyIndex: 0, keys: map[transactionStatusKey]struct{}{}}}},
	}
	for _, complete := range []bool{false, true} {
		for _, genesis := range []bool{false, true} {
			want, err := marshalTransactionStatusNodesUncached(nodes, 3, complete, genesis)
			require.NoError(t, err)
			got, err := marshalTransactionStatusNodes(nodes, 3, complete, genesis)
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
	}
}
