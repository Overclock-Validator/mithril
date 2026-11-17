package turbine

import (
	"reflect"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
)

// The head retry interval is a few observed round trips, clamped to the
// floor and to the tier's own interval; with no latency observed the tier
// stands.
func TestHeadRetryIntervalTracksRoundTrips(t *testing.T) {
	c := &repairClient{}
	if got := c.headRetryInterval(repairRetryHeadNear); got != repairRetryHeadNear {
		t.Fatalf("no latency observed: got %s, want tier %s", got, repairRetryHeadNear)
	}
	for _, tc := range []struct {
		ewma, tier, want time.Duration
	}{
		{28 * time.Millisecond, repairRetryHeadEndgame, 84 * time.Millisecond},
		{5 * time.Millisecond, repairRetryHeadEndgame, repairHeadRetryFloor},
		{200 * time.Millisecond, repairRetryHeadEndgame, repairRetryHeadEndgame},
	} {
		c.respLatencyEWMA = tc.ewma.Seconds()
		if got := c.headRetryInterval(tc.tier); got != tc.want {
			t.Errorf("ewma %s tier %s: got %s, want %s", tc.ewma, tc.tier, got, tc.want)
		}
	}
}

// Head data requests without slot-specific evidence go to the fastest
// reliable responders; slow or unreliable peers are left out of that set, and
// requests for other slots never use it.
func TestFastHeadPeersPreferLowLatencyReliableResponders(t *testing.T) {
	now := time.Now()
	c := &repairClient{perPeer: make(map[repairAddressKey]*peerRecord)}
	fast, slow, flaky := repairTestPeer(1, 8008), repairTestPeer(2, 8008), repairTestPeer(3, 8008)
	peers := []gossip.RepairPeer{slow, flaky, fast}
	record := func(p gossip.RepairPeer, lat time.Duration, score float64) {
		key, _ := repairAddressKeyFromUDP(p.Addr)
		c.perPeer[key] = &peerRecord{latEWMASec: lat.Seconds(), score: score, lastMatched: now}
	}
	record(fast, 12*time.Millisecond, 1)
	record(slow, 90*time.Millisecond, 1)
	record(flaky, 5*time.Millisecond, 0.5)
	c.rebuildRankedLocked(peers, now)
	if want := []gossip.RepairPeer{fast, slow}; !reflect.DeepEqual(c.fastHead, want) {
		t.Fatalf("fast set = %v, want reliable peers by latency %v", c.fastHead, want)
	}

	c.headPeers = headRepairPeers{slot: 50, active: true, records: map[repairAddressKey]*peerRecord{}}
	if _, ok := c.pickFastHeadPeerLocked(peers, 8, shredKey{kind: repairRequestWindowIndex, slot: 50, index: 3}); ok {
		t.Fatal("the fast set served a head that is not near completion")
	}
	c.fastHeadEligible = true
	got, ok := c.pickFastHeadPeerLocked(peers, 8, shredKey{kind: repairRequestWindowIndex, slot: 50, index: 3})
	if !ok || (got.Addr.String() != fast.Addr.String() && got.Addr.String() != slow.Addr.String()) {
		t.Fatalf("head request picked %v (ok=%v), want a fast-set peer", got.Addr, ok)
	}
	if _, ok := c.pickFastHeadPeerLocked(peers, 8, shredKey{kind: repairRequestWindowIndex, slot: 51, index: 3}); ok {
		t.Fatal("a non-head request used the fast head set")
	}
}

// Without slot evidence, a near-complete head goes to the fast set ahead of
// the ranked responders (on the 3 of 4 requests that exploit rather than
// explore); a head missing many shreds keeps the ranked spread.
func TestHeadSelectionOrderUsesFastSetOnlyNearCompletion(t *testing.T) {
	now := time.Now()
	c := newPacingTestClient(t)
	peers := make([]gossip.RepairPeer, 40)
	fast := make(map[string]bool)
	for i := range peers {
		peers[i] = repairTestPeer(byte(i+1), 8008)
		key, _ := repairAddressKeyFromUDP(peers[i].Addr)
		lat := 0.090
		if i >= 24 { // the ranked half is peers 0..19 (equal scores, stable order)
			lat = 0.008
			fast[peers[i].Addr.String()] = true
		}
		c.perPeer[key] = &peerRecord{latEWMASec: lat, score: 1, lastMatched: now}
	}
	c.setRepairHead([]SlotRepairRequest{{Slot: 100}})
	request := shredKey{kind: repairRequestWindowIndex, slot: 100, index: 1}
	countFast := func() int {
		n := 0
		for i := 0; i < 400; i++ {
			if p, ok := c.nextPeerForRequestLocked(peers, request); ok && fast[p.Addr.String()] {
				n++
			}
		}
		return n
	}
	c.fastHeadEligible = false
	if n := countFast(); n > 100 {
		t.Fatalf("far head: fast peers got %d of 400, want only the explore share", n)
	}
	c.fastHeadEligible = true
	if n := countFast(); n < 300 {
		t.Fatalf("near head: fast peers got %d of 400, want at least the exploit share", n)
	}
}

// A head that had no shreds must not be pinned by a late shred once the
// head has moved on.
func TestPendingHeadRepairPinDroppedWhenHeadMoves(t *testing.T) {
	assembler := NewSlotAssembler()
	assembler.prioritizeObservedRepairSlot(100)
	assembler.slots[101] = &slotState{slot: 101, shreds: map[uint32]*Shred{}, fecSets: map[uint32]*fecState{}, firstShredAt: time.Now()}
	assembler.prioritizeObservedRepairSlot(101)
	if assembler.pendingHeadRepairPin != 0 {
		t.Fatalf("pending pin %d survived the head moving to an observed slot", assembler.pendingHeadRepairPin)
	}
	if _, err := assembler.AddShred(&Shred{Variant: legacyDataVariant, Type: ShredTypeData, Slot: 100, Index: 0, Version: 1, ParentOffset: 1, Data: []byte{1}}); err != nil {
		t.Fatalf("AddShred: %v", err)
	}
	if _, pinned := assembler.priorityRepairSlots[100]; pinned {
		t.Fatal("a late shred pinned a slot the head already passed")
	}
}

// An earlier pending retry wake is kept: a later scan must not push it back.
func TestHeadRetryWakeKeepsEarlierDeadline(t *testing.T) {
	c := &repairClient{priorityWake: make(chan struct{}, 1)}
	defer c.stopHeadRetryWake()
	c.armHeadRetryWake(time.Hour)
	first := c.headRetryDue
	c.armHeadRetryWake(2 * time.Hour)
	if !c.headRetryDue.Equal(first) {
		t.Fatal("a later deadline replaced an earlier pending wake")
	}
	c.armHeadRetryWake(time.Minute)
	if !c.headRetryDue.Before(first) {
		t.Fatal("an earlier deadline did not replace the pending wake")
	}
}
