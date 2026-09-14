package replay

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Keep the pre-split selection/metadata calculation as a differential oracle.
// The wire encoder itself did not change.
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
	return marshalTransactionStatusNodes(nodes, uint16(rooted), complete, c.coverageFromGenesis)
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
