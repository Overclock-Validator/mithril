package turbine

import (
	"net"
	"time"
)

// highestRepairFollowup snapshots current deficits AFTER response admission and
// FEC recovery. It never treats a highest-index response as proof that earlier
// pieces are missing. The returned selection can race with later arrivals, but
// no assembler lock is held during signing, network sends, or repair locking.
func (a *SlotAssembler) highestRepairFollowup(slot uint64) (SlotRepairRequest, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.slotTooOldLocked(slot) {
		return SlotRepairRequest{}, false
	}
	if _, done := a.completedSlots[slot]; done {
		return SlotRepairRequest{}, false
	}
	s := a.slots[slot]
	if s == nil || s.completing {
		return SlotRepairRequest{}, false
	}
	return s.repairRequest(repairMaxFollowupRequests)
}

// Only invoked for a matched highest-index response which passed admission.
// Disk-only catchup slots defer selection until hydration supplies assembler
// state; blindly backfilling those would ignore data already held in the spool.
func (c *repairClient) followupHighestResponse(conn *net.UDPConn, a *SlotAssembler, slot uint64) {
	req, ok := a.highestRepairFollowup(slot)
	if !ok {
		return
	}
	peers := c.peerSnapshot(time.Now())
	if len(peers) == 0 {
		return
	}
	// A matched discovery response can request at most 256 missing pieces plus
	// one highest-index probe. This response-driven burst shares the global
	// token bucket (not the periodic head-share quota); inflight dedup suppresses
	// repeat sends and unused tokens are returned below.
	ask := len(req.MissingDataShreds)
	if req.NeedHighestDataShred {
		ask++
	}
	grant := c.takeRateTokens(ask)
	if grant <= 0 {
		return
	}
	// Reserve discovery capacity as before, even when missing data fills the cap.
	window := grant
	if req.NeedHighestDataShred {
		window--
	}
	pol, acct := bulkPolicy(), c.accountingTimeout()
	sent := 0
	for _, index := range req.MissingDataShreds {
		if sent >= window {
			break
		}
		if c.sendShredAttempt(conn, peers, repairRequestWindowIndex, slot, index, pol, acct) {
			sent++
		}
	}
	if req.NeedHighestDataShred && sent < grant {
		if c.sendShredAttempt(conn, peers, repairRequestHighestWindowIndex, slot, req.HighestDataShredIndex, pol, acct) {
			sent++
		}
	}
	c.returnRateTokens(grant - sent)
}
