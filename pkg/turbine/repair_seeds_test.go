package turbine

import (
	"net"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/stretchr/testify/assert"
)

func seedTestPeer(b byte, port int) gossip.RepairPeer {
	var key gossip.Pubkey
	key[0] = b
	return gossip.RepairPeer{Pubkey: key, Addr: &net.UDPAddr{IP: net.IPv4(10, 0, 0, b), Port: port}}
}

func TestSeededRepairPeersUntilGossipCatchesUp(t *testing.T) {
	var gossipPeers []gossip.RepairPeer
	source := func() []gossip.RepairPeer { return gossipPeers }
	seedA, seedB := seedTestPeer(1, 8000), seedTestPeer(2, 8000)

	peers := SeededRepairPeers(source, []gossip.RepairPeer{seedA, seedB}, 3, time.Now().Add(time.Hour))
	assert.ElementsMatch(t, []gossip.Pubkey{seedA.Pubkey, seedB.Pubkey}, pubkeys(peers()), "seeds serve before gossip finds anyone")

	moved := seedTestPeer(1, 9000) // gossip knows seed A at a new address
	gossipPeers = []gossip.RepairPeer{moved}
	got := peers()
	assert.Len(t, got, 2)
	for _, p := range got {
		if p.Pubkey == seedA.Pubkey {
			assert.Equal(t, 9000, p.Addr.Port, "gossip's address replaces the remembered one")
		}
	}

	gossipPeers = []gossip.RepairPeer{moved, seedTestPeer(3, 1), seedTestPeer(4, 1)}
	assert.Len(t, peers(), 3, "with enough gossip peers the seeds drop out")

	expired := SeededRepairPeers(func() []gossip.RepairPeer { return nil }, []gossip.RepairPeer{seedA}, 3, time.Now().Add(-time.Second))
	assert.Empty(t, expired(), "seeds expire after their window")
	assert.Empty(t, SeededRepairPeers(func() []gossip.RepairPeer { return nil }, nil, 3, time.Now().Add(time.Hour))())
}

func TestRepairClientGoodPeersRanksRecentResponders(t *testing.T) {
	best, ok, stale, silent := seedTestPeer(1, 1), seedTestPeer(2, 1), seedTestPeer(3, 1), seedTestPeer(4, 1)
	now := time.Now()
	record := func(p gossip.RepairPeer, score float64, matched time.Time) (repairAddressKey, *peerRecord) {
		key, _ := repairAddressKeyFromUDP(p.Addr)
		return key, &peerRecord{score: score, lastMatched: matched}
	}
	c := &repairClient{
		peerSource: func() []gossip.RepairPeer { return []gossip.RepairPeer{ok, stale, best, silent} },
		perPeer:    map[repairAddressKey]*peerRecord{},
	}
	for _, r := range []struct {
		p       gossip.RepairPeer
		score   float64
		matched time.Time
	}{{best, 0.9, now}, {ok, 0.5, now.Add(-time.Minute)}, {stale, 0.99, now.Add(-time.Hour)}} {
		k, rec := record(r.p, r.score, r.matched)
		c.perPeer[k] = rec
	}
	assert.Equal(t, []gossip.Pubkey{best.Pubkey, ok.Pubkey}, pubkeys(c.goodPeers(10, 10*time.Minute)),
		"recent responders, best score first; stale and never-answering peers are left out")
	assert.Equal(t, []gossip.Pubkey{best.Pubkey}, pubkeys(c.goodPeers(1, 10*time.Minute)))
}

func pubkeys(peers []gossip.RepairPeer) []gossip.Pubkey {
	out := make([]gossip.Pubkey, len(peers))
	for i, p := range peers {
		out[i] = p.Pubkey
	}
	return out
}
