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
		{28 * time.Millisecond, repairRetryHeadNear, 84 * time.Millisecond},
		{5 * time.Millisecond, repairRetryHeadNear, repairHeadRetryFloor},
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
	got, ok := c.pickFastHeadPeerLocked(peers, 8, shredKey{kind: repairRequestWindowIndex, slot: 50, index: 3})
	if !ok || (got.Addr.String() != fast.Addr.String() && got.Addr.String() != slow.Addr.String()) {
		t.Fatalf("head request picked %v (ok=%v), want a fast-set peer", got.Addr, ok)
	}
	if _, ok := c.pickFastHeadPeerLocked(peers, 8, shredKey{kind: repairRequestWindowIndex, slot: 51, index: 3}); ok {
		t.Fatal("a non-head request used the fast head set")
	}
}
