package turbine

import (
	"sort"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
)

// SeededRepairPeers returns a repair peer source that adds seeds, peers that
// served repair in an earlier run, to source's peers until gossip has
// discovered at least enough peers or until passes. A restarted node then
// repairs within a second or two instead of waiting ~20 s for gossip to find
// peers. A seed gossip also lists is dropped (gossip's address wins), and a
// seed that no longer answers falls out of selection through the client's
// per-peer quality score like any unresponsive peer.
func SeededRepairPeers(source RepairPeerSource, seeds []gossip.RepairPeer, enough int, until time.Time) RepairPeerSource {
	if len(seeds) == 0 {
		return source
	}
	seeds = append([]gossip.RepairPeer(nil), seeds...)
	return func() []gossip.RepairPeer {
		peers := source()
		now := time.Now()
		if len(peers) >= enough || now.After(until) {
			return peers
		}
		known := make(map[gossip.Pubkey]struct{}, len(peers))
		for _, peer := range peers {
			known[peer.Pubkey] = struct{}{}
		}
		out := append(make([]gossip.RepairPeer, 0, len(peers)+len(seeds)), peers...)
		for _, seed := range seeds {
			if _, ok := known[seed.Pubkey]; !ok {
				seed.LastSeen = now
				out = append(out, seed)
			}
		}
		return out
	}
}

// goodPeers returns up to maxPeers of the current repair peers that answered
// a request within window, best quality score first.
func (c *repairClient) goodPeers(maxPeers int, window time.Duration) []gossip.RepairPeer {
	now := time.Now()
	peers := c.peerSnapshot(now)
	cutoff := now.Add(-window)
	type scored struct {
		peer  gossip.RepairPeer
		score float64
	}
	good := make([]scored, 0, len(peers))
	c.mu.Lock()
	for _, peer := range peers {
		key, ok := repairAddressKeyFromUDP(peer.Addr)
		if !ok {
			continue
		}
		if rec := c.perPeer[key]; rec != nil && rec.lastMatched.After(cutoff) {
			good = append(good, scored{peer: peer, score: rec.score})
		}
	}
	c.mu.Unlock()
	sort.SliceStable(good, func(i, j int) bool { return good[i].score > good[j].score })
	if maxPeers > 0 && len(good) > maxPeers {
		good = good[:maxPeers]
	}
	out := make([]gossip.RepairPeer, len(good))
	for i, g := range good {
		out[i] = g.peer
	}
	return out
}
