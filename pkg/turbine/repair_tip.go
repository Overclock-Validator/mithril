package turbine

import (
	"sort"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
)

// Tip-latency tuning for repair of the slot replay is waiting on (the head).
//
// Traced on a mainnet full node, the median repair round trip was about
// 27 ms, yet a head request sent to a silent peer was retried only after the
// tier interval (150 to 500 ms) plus up to one 100 ms scan, and the last
// shreds of a block went to effectively random peers (every response under
// 300 ms scores as perfect).
//
//   - The head's retry interval is a few measured round trips (never longer
//     than its tier's, never shorter than a floor), and a timer wakes the
//     scan when that retry falls due instead of waiting for the next tick.
//   - Head requests without slot-specific evidence go to the fastest
//     reliable responders.
const (
	repairHeadRetryRTTs    = 3
	repairHeadRetryFloor   = 60 * time.Millisecond
	repairFastHeadPeers    = 16
	repairFastHeadMinScore = 0.9
)

// headRetryInterval shortens a head tier's retry interval to
// repairHeadRetryRTTs observed round trips, clamped to
// [repairHeadRetryFloor, tier]. With no latency observed yet the tier stands.
func (c *repairClient) headRetryInterval(tier time.Duration) time.Duration {
	c.mu.Lock()
	ewma := c.respLatencyEWMA
	c.mu.Unlock()
	if ewma <= 0 {
		return tier
	}
	d := time.Duration(ewma * repairHeadRetryRTTs * float64(time.Second))
	if d < repairHeadRetryFloor {
		d = repairHeadRetryFloor
	}
	if d > tier {
		d = tier
	}
	return d
}

// armHeadRetryWake schedules one extra scan when attempts sent now become
// eligible for a retry. Only the repair loop calls it, so the timer needs no
// lock; the wake itself is the usual nonblocking, coalesced hint.
func (c *repairClient) armHeadRetryWake(d time.Duration) {
	if d <= 0 {
		return
	}
	if c.headRetryTimer == nil {
		c.headRetryTimer = time.AfterFunc(d, c.wakePriority)
		return
	}
	c.headRetryTimer.Reset(d)
}

// rebuildFastHeadLocked keeps the repairFastHeadPeers lowest-latency
// responders whose score shows they reliably answer. Called with the ranked
// responder rebuild, so it shares its cache lifetime.
func (c *repairClient) rebuildFastHeadLocked(peers []gossip.RepairPeer, now time.Time) {
	type timedPeer struct {
		peer gossip.RepairPeer
		lat  float64
	}
	fast := make([]timedPeer, 0, 64)
	for _, peer := range peers {
		key, ok := repairAddressKeyFromUDP(peer.Addr)
		if !ok {
			continue
		}
		rec := c.perPeer[key]
		if rec == nil || rec.latEWMASec <= 0 || rec.score < repairFastHeadMinScore || now.Sub(rec.lastMatched) > repairResponderWindow {
			continue
		}
		fast = append(fast, timedPeer{peer: peer, lat: rec.latEWMASec})
	}
	sort.Slice(fast, func(i, j int) bool { return fast[i].lat < fast[j].lat })
	c.fastHead = c.fastHead[:0]
	for i := 0; i < len(fast) && i < repairFastHeadPeers; i++ {
		c.fastHead = append(c.fastHead, fast[i].peer)
	}
}

// pickFastHeadPeerLocked round-robins head data requests across the fast
// responder set, skipping saturated peers. ok is false for any other request
// or when no fast peer is free.
func (c *repairClient) pickFastHeadPeerLocked(peers []gossip.RepairPeer, cap int, request shredKey) (gossip.RepairPeer, bool) {
	if !c.isHeadDataRequest(request) {
		return gossip.RepairPeer{}, false
	}
	if now := time.Now(); now.Sub(c.rankedAt) > repairRankedRebuildTTL {
		c.rebuildRankedLocked(peers, now)
	}
	for attempts := 0; attempts < len(c.fastHead); attempts++ {
		c.fastHeadCursor++
		peer := c.fastHead[int(c.fastHeadCursor%uint64(len(c.fastHead)))]
		if key, ok := repairAddressKeyFromUDP(peer.Addr); ok {
			if rec := c.perPeer[key]; rec != nil && rec.inflight >= cap {
				continue
			}
		}
		return peer, true
	}
	return gossip.RepairPeer{}, false
}
