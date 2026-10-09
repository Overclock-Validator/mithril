package turbine

import (
	"errors"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
	"time"
)

func childRepairFixture(t *testing.T) (*SlotAssembler, *slotState, *slotState, *prefetchedShredBatch) {
	t.Helper()
	a := NewSlotAssembler()
	a.SubscribeStream(make(chan StreamEvent, 1))
	p := newRepairSelectionSlot(100)
	c := newRepairSelectionSlot(101)
	addCodedSet(c, 0, 32, 32, seq(0, 19), 8)   // deficit4
	addCodedSet(c, 32, 32, 32, seq(32, 56), 6) // cheaper later span
	a.slots[100] = p
	a.slots[101] = c
	a.maxObservedSlot = 101
	done := make(chan struct{})
	close(done)
	b := &prefetchedShredBatch{start: 0, marker: true, parent: &AlpenglowParentInfo{ParentSlot: 100}, ready: done}
	c.prefetch = &slotEntryPrefetch{batches: map[uint32]*prefetchedShredBatch{0: b}}
	a.SetStreamRepairParent(StreamGeneration{slot: 100, state: p})
	return a, p, c, b
}
func TestChildRepairEarlyHeaderAndDroppedEvents(t *testing.T) {
	a, _, c, b := childRepairFixture(t)
	req, ok := a.childRepairRequest(time.Now())
	require.True(t, ok)
	require.Equal(t, []uint32{20, 21, 22, 23}, req.MissingDataShreds)
	require.False(t, req.NeedHighestDataShred)
	a.streamRepairChild = nil
	a.streamSubscriber <- StreamEvent{} // full notification channel
	wake := make(chan struct{}, 1)
	a.streamRepairWake = wake
	a.mu.Lock()
	a.publishStreamBatchReadyLocked(c, b)
	a.mu.Unlock()
	require.Len(t, wake, 1)
	require.Equal(t, uint64(1), a.StreamDroppedEvents())
	_, ok = a.childRepairRequest(time.Now())
	require.True(t, ok)
	// Repeated publication is not a new repair wakeup.
	<-wake
	a.mu.Lock()
	a.publishStreamBatchReadyLocked(c, b)
	a.mu.Unlock()
	require.Empty(t, wake)
}
func TestChildRepairLifecycle(t *testing.T) {
	for _, kind := range []string{"expiry", "clear", "child-reset", "parent-reset", "child-complete", "parent-replaced", "unsubscribe", "update-parent", "wrong-parent"} {
		t.Run(kind, func(t *testing.T) {
			a, p, c, b := childRepairFixture(t)
			switch kind {
			case "expiry":
				a.streamRepairUntil = time.Now().Add(-time.Second)
			case "clear":
				a.SetStreamRepairParent(StreamGeneration{})
			case "child-reset":
				c.prefetch = nil // fixture has no live prefetch workers
				a.ResetSlot(c.slot)
			case "parent-reset":
				a.ResetSlot(p.slot)
			case "child-complete":
				c.completing = true
			case "parent-replaced":
				a.slots[p.slot] = newRepairSelectionSlot(p.slot)
			case "unsubscribe":
				a.SubscribeStream(nil)
			case "update-parent", "wrong-parent":
				changed := prefetchedShredBatch{start: b.start, end: b.end, parent: b.parent, marker: b.marker, ready: b.ready}
				parent := *b.parent
				changed.parent = &parent
				if kind == "update-parent" {
					changed.start = 32
					parent.FromUpdateParent = true
				} else {
					parent.ParentSlot = 99
				}
				a.mu.Lock()
				a.noteChildRepairHeaderLocked(c, &changed)
				a.noteChildRepairHeaderLocked(c, b)
				a.mu.Unlock()
			}
			_, ok := a.childRepairRequest(time.Now())
			require.False(t, ok)
		})
	}
	// Parent assembly can finish while replay is still executing it.
	a, p, _, _ := childRepairFixture(t)
	delete(a.slots, p.slot)
	p.streamCompleted = true
	_, ok := a.childRepairRequest(time.Now())
	require.True(t, ok)
	a.ResetSlot(p.slot)
	_, ok = a.childRepairRequest(time.Now())
	require.False(t, ok)
}
func TestChildRepairRejectsUnrelatedAndInvalidHeader(t *testing.T) {
	a, _, c, b := childRepairFixture(t)
	a.streamRepairChild = nil
	bad := prefetchedShredBatch{start: b.start, end: b.end, parent: b.parent, marker: b.marker, ready: b.ready}
	bad.err = errors.New("invalid header")
	a.mu.Lock()
	a.noteChildRepairHeaderLocked(c, &bad)
	a.mu.Unlock()
	_, ok := a.childRepairRequest(time.Now())
	require.False(t, ok)
	other := newRepairSelectionSlot(102)
	a.mu.Lock()
	a.noteChildRepairHeaderLocked(other, b)
	a.mu.Unlock()
	_, ok = a.childRepairRequest(time.Now())
	require.False(t, ok)
}
func TestChildRepairUsesOnlyRemainingBudget(t *testing.T) {
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer sink.Close()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer conn.Close()
	c := newPacingTestClient(t)
	peers := []gossip.RepairPeer{{Addr: sink.LocalAddr().(*net.UDPAddr)}}
	req := SlotRepairRequest{Slot: 101, MissingDataShreds: []uint32{20, 21, 22, 23, 24, 25}, NeedHighestDataShred: true}
	require.Zero(t, c.sendChildRepair(conn, peers, req, 0, time.Second))
	require.Equal(t, 2, c.sendChildRepair(conn, peers, req, 2, time.Second))
	require.Equal(t, 2, c.sendChildRepair(conn, peers, req, 100, time.Second))
	require.Zero(t, c.sendChildRepair(conn, peers, req, 100, time.Second))
	require.Len(t, c.outstanding, 4)
	for k := range c.outstanding {
		require.Equal(t, repairRequestWindowIndex, k.kind)
		require.Zero(t, k.attempt)
	}
}

func TestChildRepairRejectsStaleParentAnchor(t *testing.T) {
	a, p, _, _ := childRepairFixture(t)
	a.slots[p.slot] = newRepairSelectionSlot(p.slot)
	a.SetStreamRepairParent(StreamGeneration{slot: p.slot, state: p})
	require.Nil(t, a.streamRepairParent)
}

func TestChildRepairParentUpdateClearsLookahead(t *testing.T) {
	a, p, _, b := childRepairFixture(t)
	update := prefetchedShredBatch{start: b.start, end: b.end, parent: b.parent, marker: b.marker, ready: b.ready}
	info := *b.parent
	info.FromUpdateParent = true
	update.parent = &info
	update.start = 32
	a.mu.Lock()
	a.noteChildRepairHeaderLocked(p, &update)
	a.mu.Unlock()
	_, ok := a.childRepairRequest(time.Now())
	require.False(t, ok)
}
