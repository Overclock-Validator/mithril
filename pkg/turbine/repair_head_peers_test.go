package turbine

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
)

func recordHeadTestResponse(t *testing.T, c *repairClient, peer gossip.RepairPeer, slot uint64, kind repairRequestKind, index uint32, late bool) {
	t.Helper()
	addr, _ := repairAddressKeyFromUDP(peer.Addr)
	key := repairRequestKey{kind: kind, slot: slot, index: index}
	nonce := uint32(123)
	sent := time.Now().Add(-20 * time.Millisecond)
	if late {
		sent = time.Now().Add(-3 * time.Second)
	}
	req := outstandingRepairRequest{key: key, nonce: nonce, addr: addr, sentAt: sent, accountAt: sent.Add(time.Second)}
	c.outstanding[key] = req
	c.byResponse[repairResponseKey{addr: addr, nonce: nonce}] = key
	c.addInflightLocked(key.shred(), sent)
	c.notePeerSentLocked(addr)
	if late {
		c.expireOutstanding(time.Now())
	}
	matched, _ := c.matchShredResponse(nonceTrailer(nonce), peer.Addr, &Shred{Type: ShredTypeData, Slot: slot, Index: index})
	if !matched {
		t.Fatal("response did not match")
	}
}

func TestHeadRepairPrefersPeersServingBlockedSlot(t *testing.T) {
	c := newPacingTestClient(t)
	c.setRepairHead([]SlotRepairRequest{{Slot: 100}})
	peers := make([]gossip.RepairPeer, 64)
	for i := range peers {
		peers[i] = repairTestPeer(byte(i+1), 8008)
		// Every peer serves fresh blocks, but only one serves the old hole.
		for j := 0; j < 30; j++ {
			recordHeadTestResponse(t, c, peers[i], 200, repairRequestWindowIndex, uint32(j), false)
		}
		recordHeadTestResponse(t, c, peers[i], 100, repairRequestHighestWindowIndex, 1000, false)
	}
	source := peers[0]
	for i := 0; i < 20; i++ {
		recordHeadTestResponse(t, c, source, 100, repairRequestWindowIndex, uint32(i), false)
	}
	// Another peer has a few old shreds, but repeatedly times out on the rest.
	recordHeadTestResponse(t, c, peers[1], 100, repairRequestWindowIndex, 1, false)
	addr, _ := repairAddressKeyFromUDP(peers[1].Addr)
	for i := 0; i < 20; i++ {
		c.noteHeadTimeoutLocked(outstandingRepairRequest{addr: addr, key: repairRequestKey{slot: 100, kind: repairRequestWindowIndex}})
	}
	hits := map[string]int{}
	for i := 0; i < 400; i++ {
		p, ok := c.nextPeerForRequestLocked(peers, shredKey{slot: 100, kind: repairRequestWindowIndex})
		if !ok {
			t.Fatal("no peer")
		}
		hits[p.Addr.String()]++
	}
	if hits[source.Addr.String()] < 300 {
		t.Fatalf("peer serving blocked slot got %d/400 requests, want >=300", hits[source.Addr.String()])
	}
	if len(hits) != len(peers) {
		t.Fatalf("exploration reached %d/%d peers", len(hits), len(peers))
	}
	// Head knowledge must not redirect the live edge to that single peer.
	n := 0
	for i := 0; i < 400; i++ {
		p, _ := c.nextPeerForRequestLocked(peers, shredKey{slot: 200, kind: repairRequestWindowIndex})
		if p.Addr.String() == source.Addr.String() {
			n++
		}
	}
	if n > 50 {
		t.Fatalf("head preference leaked into live-edge requests: %d/400", n)
	}
}

func TestHeadRepairRespectsPeerCapAndHeadChanges(t *testing.T) {
	c := newPacingTestClient(t)
	peers := []gossip.RepairPeer{repairTestPeer(1, 8008), repairTestPeer(2, 8008)}
	c.setRepairHead([]SlotRepairRequest{{Slot: 100}})
	recordHeadTestResponse(t, c, peers[0], 100, repairRequestWindowIndex, 3, false)
	addr, _ := repairAddressKeyFromUDP(peers[0].Addr)
	c.perPeer[addr].inflight = c.adaptivePerPeerCapLocked(len(peers))
	p, ok := c.nextPeerForRequestLocked(peers, shredKey{slot: 100, kind: repairRequestWindowIndex})
	if !ok || p.Addr.String() != peers[1].Addr.String() {
		t.Fatalf("saturated head source selected: %+v", p)
	}
	c.setRepairHead([]SlotRepairRequest{{Slot: 101}})
	// Old responses (including late answers) cannot populate the new head.
	recordHeadTestResponse(t, c, peers[0], 100, repairRequestWindowIndex, 4, true)
	recordHeadTestResponse(t, c, peers[0], 101, repairRequestHighestWindowIndex, 1000, false)
	if len(c.headPeers.records) != 0 {
		t.Fatal("old-slot or highest-shred response credited to new head")
	}
	recordHeadTestResponse(t, c, peers[1], 101, repairRequestWindowIndex, 5, true)
	if len(c.headPeers.records) != 1 {
		t.Fatal("valid late head response not credited")
	}
	c.setRepairHead(nil)
	if c.headPeers.active || len(c.headPeers.records) != 0 {
		t.Fatal("head state not released")
	}
}
