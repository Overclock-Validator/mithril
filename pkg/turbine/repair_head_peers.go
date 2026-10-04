package turbine

import (
	"sort"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
)

// A peer serving fresh shreds may have none of the older shreds blocking
// replay. Keep head-slot service separate from aggregate peer quality.
// This state is bounded to one slot and guarded by repairClient.mu.
type headRepairPeers struct {
	slot     uint64
	active   bool
	records  map[repairAddressKey]*peerRecord
	ranked   []gossip.RepairPeer
	rankedAt time.Time
}

func (c *repairClient) setRepairHead(priority []SlotRepairRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(priority) == 0 {
		c.headPeers = headRepairPeers{}
		return
	}
	if !c.headPeers.active || c.headPeers.slot != priority[0].Slot {
		c.headPeers = headRepairPeers{slot: priority[0].Slot, active: true,
			records: make(map[repairAddressKey]*peerRecord)}
	}
}

func (c *repairClient) isHeadDataRequest(key shredKey) bool {
	return c.headPeers.active && key.slot == c.headPeers.slot && key.kind == repairRequestWindowIndex
}

func (c *repairClient) noteHeadResponseLocked(req outstandingRepairRequest, latency time.Duration, late bool) {
	if !c.isHeadDataRequest(req.key.shred()) {
		return
	}
	rec := c.headPeers.records[req.addr]
	if rec == nil {
		if len(c.headPeers.records) >= repairPeerStatsCap {
			return
		}
		rec = &peerRecord{}
		c.headPeers.records[req.addr] = rec
	}
	quality := 1.0
	if late {
		quality = repairScoreLateReward
	} else if latency > repairScoreFullLatency {
		quality = repairScoreFullLatency.Seconds() / latency.Seconds()
	}
	rec.score = (1-repairScoreAlpha)*rec.score + repairScoreAlpha*quality
	rec.lastMatched = time.Now()
}

func (c *repairClient) noteHeadTimeoutLocked(req outstandingRepairRequest) {
	if c.isHeadDataRequest(req.key.shred()) {
		if rec := c.headPeers.records[req.addr]; rec != nil {
			rec.score *= 1 - repairScoreAlpha
		}
	}
}

func (c *repairClient) pickHeadResponderLocked(peers []gossip.RepairPeer, cap int, request shredKey) (gossip.RepairPeer, bool) {
	if !c.isHeadDataRequest(request) {
		return gossip.RepairPeer{}, false
	}
	h := &c.headPeers
	now := time.Now()
	if now.Sub(h.rankedAt) > repairRankedRebuildTTL {
		h.rankedAt = now
		h.ranked = h.ranked[:0]
		for _, peer := range peers {
			addr, ok := repairAddressKeyFromUDP(peer.Addr)
			if !ok {
				continue
			}
			if rec := h.records[addr]; rec != nil && now.Sub(rec.lastMatched) <= repairResponderWindow {
				h.ranked = append(h.ranked, peer)
			}
		}
		sort.SliceStable(h.ranked, func(i, j int) bool {
			a, _ := repairAddressKeyFromUDP(h.ranked[i].Addr)
			b, _ := repairAddressKeyFromUDP(h.ranked[j].Addr)
			return h.records[a].score > h.records[b].score
		})
	}
	// Prefer the strongest evidence for this slot, bounded by the same
	// per-peer cap as bulk repair. The caller still explores on every fourth
	// request and falls back to global selection when these peers are busy.
	for _, peer := range h.ranked {
		addr, _ := repairAddressKeyFromUDP(peer.Addr)
		if rec := c.perPeer[addr]; rec != nil && rec.inflight >= cap {
			continue
		}
		return peer, true
	}
	return gossip.RepairPeer{}, false
}
