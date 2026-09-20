package turbine

import (
	"net"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
)

func repairTestPeer(ip byte, port int) gossip.RepairPeer {
	return gossip.RepairPeer{Addr: &net.UDPAddr{IP: net.IPv4(10, 0, 0, ip), Port: port}}
}

// Success-weighted selection: once one peer has answered, roughly 3 of 4
// requests concentrate on it while the full set keeps being explored — the
// contract that turns "95% of requests wasted on peers without the data"
// into "most requests go where answers come from".
func TestNextPeerPrefersRecentResponders(t *testing.T) {
	client := &repairClient{
		outstanding: make(map[repairRequestKey]outstandingRepairRequest),
		byResponse:  make(map[repairResponseKey]repairRequestKey),
		perPeer:     make(map[repairAddressKey]*peerRecord),
	}
	peers := make([]gossip.RepairPeer, 0, 8)
	for i := byte(1); i <= 8; i++ {
		peers = append(peers, repairTestPeer(i, 8000+int(i)))
	}

	responder := peers[2]
	key, ok := repairAddressKeyFromUDP(responder.Addr)
	if !ok {
		t.Fatalf("premise: responder addr key")
	}
	client.perPeer[key] = &peerRecord{timely: 1, lastMatched: time.Now()}

	hits := 0
	const picks = 400
	client.mu.Lock()
	for i := 0; i < picks; i++ {
		peer, ok := client.nextPeerLocked(peers)
		if !ok {
			t.Fatalf("selection must succeed with live peers")
		}
		if peer.Addr.String() == responder.Addr.String() {
			hits++
		}
	}
	client.mu.Unlock()

	// 3/4 of picks target the sole responder; the remaining exploration
	// quarter round-robins all 8 (occasionally landing on it too).
	if hits < picks/2 {
		t.Fatalf("responder got %d/%d picks; expected concentration (>=%d)", hits, picks, picks/2)
	}
	if hits == picks {
		t.Fatalf("exploration must still reach non-responders")
	}

	// A stale responder record (outside the window) drops back to blind
	// round-robin: no concentration. Invalidate the ranked cache — live code
	// rebuilds it within repairRankedRebuildTTL anyway.
	client.perPeer[key].lastMatched = time.Now().Add(-2 * repairResponderWindow)
	client.rankedAt = time.Time{}
	stale := 0
	client.mu.Lock()
	for i := 0; i < picks; i++ {
		peer, _ := client.nextPeerLocked(peers)
		if peer.Addr.String() == responder.Addr.String() {
			stale++
		}
	}
	client.mu.Unlock()
	if stale > picks/4 {
		t.Fatalf("stale responder still concentrated: %d/%d", stale, picks)
	}
}

func TestRecentResponderYieldsAfterRepeatedTimeouts(t *testing.T) {
	peer := repairTestPeer(1, 8001)
	key, ok := repairAddressKeyFromUDP(peer.Addr)
	if !ok {
		t.Fatal("invalid test peer")
	}
	client := &repairClient{perPeer: map[repairAddressKey]*peerRecord{
		key: {score: 1, lastMatched: time.Now()},
	}}
	peers := []gossip.RepairPeer{peer}
	client.rebuildRankedLocked(peers, time.Now())
	if len(client.ranked) != 1 {
		t.Fatal("answering peer must be preferred")
	}
	for range 2 {
		client.notePeerTimeoutLocked(key)
	}
	client.rebuildRankedLocked(peers, time.Now())
	if len(client.ranked) != 1 {
		t.Fatal("peer must remain preferred before the timeout threshold")
	}
	client.notePeerTimeoutLocked(key)
	client.rebuildRankedLocked(peers, time.Now())
	if len(client.ranked) != 0 {
		t.Fatal("unanswered peer must return to the exploration ring")
	}
	client.notePeerLateLocked(key, 3*time.Second)
	client.rebuildRankedLocked(peers, time.Now())
	if len(client.ranked) != 1 {
		t.Fatal("peer must be preferred again after a late answer")
	}
}

// Outcome scoring under the REAL request lifecycle. A late answer is not a
// standalone outcome: the request first expires (timeout, score touch 0)
// and the answer later matches (late, score touch 0.3) — so `late` is a
// subset of `timeouts`, and an always-late peer settles near
// 0.3*alpha/(1-(1-alpha)^2) ~ 0.16, deliberately between dead and the raw
// late reward.
func TestPeerScoreTracksOutcomes(t *testing.T) {
	client := &repairClient{perPeer: make(map[repairAddressKey]*peerRecord)}
	addr := repairAddressKey{port: 9000}

	client.mu.Lock()
	for i := 0; i < 20; i++ {
		client.notePeerTimelyLocked(addr, 100*time.Millisecond)
	}
	fast := client.perPeer[addr].score
	// Always-late lifecycle: every request expires first, then answers.
	for i := 0; i < 40; i++ {
		client.notePeerTimeoutLocked(addr)
		client.notePeerLateLocked(addr, 3*time.Second)
	}
	lateScore := client.perPeer[addr].score
	for i := 0; i < 40; i++ {
		client.notePeerTimeoutLocked(addr)
	}
	dead := client.perPeer[addr].score
	client.mu.Unlock()

	if fast < 0.8 {
		t.Fatalf("fast responder score = %.2f, want near 1", fast)
	}
	if lateScore < 0.10 || lateScore > 0.25 {
		t.Fatalf("always-late steady state = %.3f, want ~0.16 (timeout touch + late touch per request)", lateScore)
	}
	if dead > 0.1 {
		t.Fatalf("timeout-only score = %.2f, want decayed toward 0", dead)
	}
	rec := client.perPeer[addr]
	// late (40) is a SUBSET of timeouts (40 late-answered + 40 never): the
	// request total is timely+timeouts = 100, not the sum of all three.
	if rec.timely != 20 || rec.late != 40 || rec.timeouts != 80 {
		t.Fatalf("counters = timely %d late %d timeouts %d, want 20/40/80", rec.timely, rec.late, rec.timeouts)
	}
}

// Selection distribution: every ranked peer receives real traffic. Review
// caught (via simulation) that sharing one cursor between the 1-in-4
// exploration gate and the ring indices locked responder picks to indices
// {2,3} mod 4 — the top-2 ranked peers got ZERO requests whenever the
// ranked set's size divided 4, which the min-peers floor made the common
// steady state. Separate cursors per ring are the fix; this pins it.
func TestSelectionCoversAllRankedPeers(t *testing.T) {
	client := &repairClient{perPeer: make(map[repairAddressKey]*peerRecord)}
	peers := make([]gossip.RepairPeer, 0, 12)
	for i := byte(1); i <= 12; i++ {
		peer := repairTestPeer(i, 9100+int(i))
		peers = append(peers, peer)
		key, _ := repairAddressKeyFromUDP(peer.Addr)
		client.perPeer[key] = &peerRecord{
			score:       1.0 - 0.05*float64(i),
			lastMatched: time.Now(),
		}
	}

	const picks = 4000
	hits := make(map[string]int, 12)
	client.mu.Lock()
	for i := 0; i < picks; i++ {
		peer, ok := client.nextPeerLocked(peers)
		if !ok {
			t.Fatal("selection must succeed with live peers")
		}
		hits[peer.Addr.String()]++
	}
	ranked := append([]gossip.RepairPeer(nil), client.ranked...)
	client.mu.Unlock()

	if len(ranked) != repairRankedMinPeers {
		t.Fatalf("premise: ranked size = %d, want %d", len(ranked), repairRankedMinPeers)
	}
	// Each ranked peer must carry a fair share of the 3-in-4 exploit
	// traffic: expected picks*3/4/8 = 375 each; require at least half that.
	for i, peer := range ranked {
		if got := hits[peer.Addr.String()]; got < picks*3/4/repairRankedMinPeers/2 {
			t.Fatalf("ranked peer %d got %d/%d picks — ring aliasing starves it", i, got, picks)
		}
	}
	// Exploration must still reach every peer, ranked or not.
	for _, peer := range peers {
		if hits[peer.Addr.String()] == 0 {
			t.Fatalf("peer %s never picked — exploration ring broken", peer.Addr)
		}
	}
}

// The aggregate compresses per-peer records into the one-clause summary the
// replay summary and catchup heartbeat print. Percentages are over
// RESOLVED requests (timely + expired): late is a refinement of expired,
// so a late-answered request must not be double-counted in the base.
func TestPeerAggregateSummarizes(t *testing.T) {
	client := &repairClient{perPeer: make(map[repairAddressKey]*peerRecord)}
	now := time.Now()
	for i := 0; i < 4; i++ {
		client.perPeer[repairAddressKey{port: 9200 + i}] = &peerRecord{
			timely:      8,
			late:        1, // the one expired request below, answered late
			timeouts:    1,
			score:       0.2 * float64(i+1),
			latEWMASec:  0.1 * float64(i+1),
			lastMatched: now,
		}
	}
	client.perPeer[repairAddressKey{port: 9300}] = &peerRecord{timeouts: 10} // tracked, never answered

	agg := client.peerAggregate()
	if agg.Tracked != 5 || agg.Responding != 4 {
		t.Fatalf("tracked/responding = %d/%d, want 5/4", agg.Tracked, agg.Responding)
	}
	// Resolved requests: timely 32 + timeouts 14 = 46 (late's 4 already
	// inside the 14) -> 69% timely, 8% eventually-late.
	if agg.TimelyPct != 69 || agg.LatePct != 8 {
		t.Fatalf("timely/late = %d%%/%d%%, want 69%%/8%%", agg.TimelyPct, agg.LatePct)
	}
	if agg.ScoreP50 < 0.59 || agg.ScoreP50 > 0.61 {
		t.Fatalf("score p50 = %.4f, want ~0.60", agg.ScoreP50)
	}
	if agg.MedianLatencyMillis != 300 {
		t.Fatalf("median latency = %dms, want 300", agg.MedianLatencyMillis)
	}
}

// Ranking narrows the responder set to the higher-scoring half (with a
// floor so selection never overfits to a handful of peers): with 12
// responders the 4 lowest-scoring are excluded, the floor of 8 kept.
func TestRankedRespondersPreferHighScores(t *testing.T) {
	client := &repairClient{perPeer: make(map[repairAddressKey]*peerRecord)}
	peers := make([]gossip.RepairPeer, 0, 12)
	for i := byte(1); i <= 12; i++ {
		peer := repairTestPeer(i, 9000+int(i))
		peers = append(peers, peer)
		key, _ := repairAddressKeyFromUDP(peer.Addr)
		client.perPeer[key] = &peerRecord{
			score:       1.0 - 0.05*float64(i),
			lastMatched: time.Now(),
		}
	}

	client.mu.Lock()
	client.rebuildRankedLocked(peers, time.Now())
	ranked := append([]gossip.RepairPeer(nil), client.ranked...)
	client.mu.Unlock()

	if len(ranked) != repairRankedMinPeers {
		t.Fatalf("ranked size = %d, want the floor %d", len(ranked), repairRankedMinPeers)
	}
	inRanked := make(map[string]bool, len(ranked))
	for _, p := range ranked {
		inRanked[p.Addr.String()] = true
	}
	for i := 0; i < 8; i++ {
		if !inRanked[peers[i].Addr.String()] {
			t.Fatalf("high-scoring peer %d missing from ranked set", i+1)
		}
	}
	for i := 8; i < 12; i++ {
		if inRanked[peers[i].Addr.String()] {
			t.Fatalf("low-scoring peer %d should be excluded from ranked set", i+1)
		}
	}
}
