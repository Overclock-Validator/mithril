package turbine

import (
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
)

func newPacingTestClient(t *testing.T) *repairClient {
	t.Helper()
	c := &repairClient{
		outstanding: make(map[repairRequestKey]outstandingRepairRequest),
		byResponse:  make(map[repairResponseKey]repairRequestKey),
		inflight:    make(map[shredKey]*shredInflight),
		perPeer:     make(map[repairAddressKey]*peerRecord),
		expiredCur:  make(map[repairResponseKey]outstandingRepairRequest),
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	c.identity = priv
	c.timeoutNanos.Store(int64(repairMinRequestTimeout))
	return c
}

// nonceTrailer builds a packet whose trailing 4 bytes decode to the nonce.
func nonceTrailer(nonce uint32) []byte {
	return []byte{0xAA, 0xBB, byte(nonce), byte(nonce >> 8), byte(nonce >> 16), byte(nonce >> 24)}
}

// Fast responses pin the timeout at the floor; sustained slow responses walk
// it toward factor x latency; the ceiling holds against pathological tails.
func TestAdaptiveTimeoutTracksObservedLatency(t *testing.T) {
	c := newPacingTestClient(t)

	c.mu.Lock()
	for i := 0; i < 50; i++ {
		c.observeLatencyLocked(200 * time.Millisecond)
	}
	c.mu.Unlock()
	if got := time.Duration(c.timeoutNanos.Load()); got != repairMinRequestTimeout {
		t.Fatalf("fast responses: timeout = %s, want floor %s", got, repairMinRequestTimeout)
	}

	c.mu.Lock()
	for i := 0; i < 200; i++ {
		c.observeLatencyLocked(1500 * time.Millisecond)
	}
	c.mu.Unlock()
	got := time.Duration(c.timeoutNanos.Load())
	if got < 4*time.Second || got > 5*time.Second {
		t.Fatalf("1.5s responses: timeout = %s, want ~4.5s (3x EWMA)", got)
	}

	c.mu.Lock()
	for i := 0; i < 200; i++ {
		c.observeLatencyLocked(30 * time.Second)
	}
	c.mu.Unlock()
	if got := time.Duration(c.timeoutNanos.Load()); got != repairMaxRequestTimeout {
		t.Fatalf("pathological responses: timeout = %s, want ceiling %s", got, repairMaxRequestTimeout)
	}
}

// An answer landing after its request expired must be recognized: counted as
// late (not dropped as noise), the peer credited as a responder, and the
// shred attributed as repair-delivered.
func TestLateResponseMatchedAfterExpiry(t *testing.T) {
	c := newPacingTestClient(t)
	from := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 7), Port: 8007}
	addrKey, ok := repairAddressKeyFromUDP(from)
	if !ok {
		t.Fatal("premise: addr key")
	}

	reqKey := repairRequestKey{kind: repairRequestWindowIndex, slot: 42, index: 3}
	sentAt := time.Now().Add(-5 * time.Second)
	c.outstanding[reqKey] = outstandingRepairRequest{key: reqKey, nonce: 777, addr: addrKey, sentAt: sentAt}
	c.byResponse[repairResponseKey{addr: addrKey, nonce: 777}] = reqKey

	c.expireOutstanding(time.Now())
	if c.timeouts.Load() != 1 || len(c.outstanding) != 0 {
		t.Fatalf("expiry: timeouts=%d outstanding=%d, want 1/0", c.timeouts.Load(), len(c.outstanding))
	}

	packet := nonceTrailer(777)
	shred := &Shred{Slot: 42, Index: 3, Type: ShredTypeData}
	if !observeRepairForTest(c, nil, packet, from, shred) {
		t.Fatal("late answer must be attributed as a repair delivery")
	}
	if c.lateResponses.Load() != 1 {
		t.Fatalf("lateResponses = %d, want 1", c.lateResponses.Load())
	}
	if rec := c.perPeer[addrKey]; rec == nil || rec.late != 1 {
		t.Fatal("late-answering peer must earn responder credit (late count)")
	}
	if c.respLatencyEWMA < 4.5 {
		t.Fatalf("late latency %.2fs must feed the EWMA (~5s)", c.respLatencyEWMA)
	}

	// Second delivery of the same nonce: entry consumed, ordinary broadcast.
	if observeRepairForTest(c, nil, packet, from, shred) {
		t.Fatal("expired entry must be single-use")
	}
}

// A late answer carrying a shred for the WRONG slot is rejected exactly like
// a timely mismatch would be — the expired record keeps the request's slot,
// so a peer cannot ride an old nonce to repair-attribute arbitrary data.
func TestLateResponseWrongSlotRejected(t *testing.T) {
	c := newPacingTestClient(t)
	from := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 8), Port: 8008}
	addrKey, _ := repairAddressKeyFromUDP(from)

	reqKey := repairRequestKey{kind: repairRequestWindowIndex, slot: 42, index: 3}
	c.outstanding[reqKey] = outstandingRepairRequest{key: reqKey, nonce: 900, addr: addrKey, sentAt: time.Now().Add(-5 * time.Second)}
	c.byResponse[repairResponseKey{addr: addrKey, nonce: 900}] = reqKey
	c.expireOutstanding(time.Now())

	if observeRepairForTest(c, nil, nonceTrailer(900), from, &Shred{Slot: 43, Index: 3, Type: ShredTypeData}) {
		t.Fatal("wrong-slot late answer must not be attributed as repair")
	}
	if c.lateResponses.Load() != 0 {
		t.Fatalf("lateResponses = %d, want 0 (slot check precedes counting)", c.lateResponses.Load())
	}
}

// A response is credited only if the returned shred actually satisfies the
// request. A WindowIndex answer must carry the EXACT requested data index; a
// coding shred, a wrong index, or (for HWI) an index below the requested floor
// is non-conforming and earns the peer NOTHING — the request stays outstanding
// for a deserved timeout and keeps its in-flight retry slot. Without this a
// peer could echo a valid nonce with a signed-but-wrong shred to farm responder
// credit, poison ranking, and prematurely free the still-missing shred.
func TestNonConformingResponseRejected(t *testing.T) {
	from := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 21), Port: 8021}
	addrKey, _ := repairAddressKeyFromUDP(from)

	cases := []struct {
		name  string
		key   repairRequestKey
		shred *Shred
	}{
		{"window wrong index", repairRequestKey{kind: repairRequestWindowIndex, slot: 80, index: 5},
			&Shred{Slot: 80, Index: 9, Type: ShredTypeData}},
		{"window coding shred", repairRequestKey{kind: repairRequestWindowIndex, slot: 80, index: 5},
			&Shred{Slot: 80, Index: 5, Type: ShredTypeCode}},
		{"highest below floor", repairRequestKey{kind: repairRequestHighestWindowIndex, slot: 80, index: 40},
			&Shred{Slot: 80, Index: 30, Type: ShredTypeData}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newPacingTestClient(t)
			c.outstanding[tc.key] = outstandingRepairRequest{
				key: tc.key, nonce: 111, addr: addrKey,
				sentAt: time.Now(), accountAt: time.Now().Add(time.Second),
			}
			c.byResponse[repairResponseKey{addr: addrKey, nonce: 111}] = tc.key
			c.mu.Lock()
			c.addInflightLocked(tc.key.shred(), time.Now())
			c.mu.Unlock()

			if observeRepairForTest(c, nil, nonceTrailer(111), from, tc.shred) {
				t.Fatal("non-conforming answer must not be attributed as a repair delivery")
			}
			if c.responses.Load() != 0 || c.lateResponses.Load() != 0 {
				t.Fatalf("responses=%d late=%d, want 0/0", c.responses.Load(), c.lateResponses.Load())
			}
			if rec := c.perPeer[addrKey]; rec != nil && (rec.timely != 0 || rec.late != 0) {
				t.Fatalf("peer credited on a bad answer: %+v", rec)
			}
			if len(c.outstanding) != 1 {
				t.Fatalf("outstanding = %d, want 1 (bad answer must not retire the request)", len(c.outstanding))
			}
			c.mu.Lock()
			inf := c.inflight[tc.key.shred()]
			c.mu.Unlock()
			if inf == nil || inf.concurrent != 1 {
				t.Fatalf("inflight = %v, want concurrent 1 (bad answer must not free the retry slot)", inf)
			}
		})
	}
}

// A LATE HighestWindowIndex answer fires the same metered gap followups as a
// timely one — the revealed missing range is the whole point of the probe.
func TestLateHighestResponseFiresFollowups(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer conn.Close()
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("sink listen: %v", err)
	}
	defer sink.Close()

	c := newPacingTestClient(t)
	c.peerCache = []gossip.RepairPeer{{Addr: sink.LocalAddr().(*net.UDPAddr)}}
	c.peerCacheAt = time.Now()
	from := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 9), Port: 8009}
	addrKey, _ := repairAddressKeyFromUDP(from)
	reqKey := repairRequestKey{kind: repairRequestHighestWindowIndex, slot: 50, index: 0}
	c.outstanding[reqKey] = outstandingRepairRequest{key: reqKey, nonce: 6, addr: addrKey, sentAt: time.Now().Add(-5 * time.Second)}
	c.byResponse[repairResponseKey{addr: addrKey, nonce: 6}] = reqKey
	c.expireOutstanding(time.Now())

	if !observeRepairForTest(c, conn, nonceTrailer(6), from, &Shred{Slot: 50, Index: 200, Type: ShredTypeData}) {
		t.Fatal("late HWI answer must match")
	}
	if c.lateResponses.Load() != 1 || c.responses.Load() != 0 {
		t.Fatalf("late=%d responses=%d, want 1/0", c.lateResponses.Load(), c.responses.Load())
	}
	// Single sink peer: under the adaptive per-peer cap a lone peer's share is
	// the whole admission window, so it no longer binds — the followups fill the
	// entire revealed gap (200) plus the chained probe = 201, same as timely.
	want := uint64(201)
	if got := c.requests.Load(); got != want {
		t.Fatalf("late HWI sent %d followups, want %d (full gap + chain probe)", got, want)
	}
}

// The head holds at most its admission SHARE (share = rate x queueSeconds x
// headShare, minus what it already has outstanding), and the fill keeps
// retryable shreds while skipping only those already at the tier's
// concurrency cap — a plain prefix truncation would re-select the covered
// prefix and under-fill; skipping ALL in-flight would starve fast retries.
func TestBoundHeadToAdmissionShare(t *testing.T) {
	share := int(float64(repairMaxRequestsPerSecond) * repairAdmissionQueueSeconds * repairHeadAdmissionShare)

	// room = share - outstanding-for-slot; the listing is capped to it.
	c := newPacingTestClient(t)
	for i := 0; i < 100; i++ {
		key := repairRequestKey{kind: repairRequestWindowIndex, slot: 70, index: uint32(i)}
		c.outstanding[key] = outstandingRepairRequest{key: key}
	}
	// More missing than the room, so the room (share-100) is what binds.
	head := SlotRepairRequest{Slot: 70, MissingDataShreds: seq(100, uint32(100+share+200))}
	c.boundHeadToAdmissionShare(&head, repairMaxAttemptsBulk)
	if got := len(head.MissingDataShreds); got != share-100 {
		t.Fatalf("head listing = %d, want %d (share %d minus 100 outstanding)", got, share-100, share)
	}

	// Saturated shreds (at maxConcurrent) are skipped so the room fills with
	// still-sendable indices, but the fill starts at the first non-saturated
	// index rather than truncating the covered prefix.
	c2 := newPacingTestClient(t)
	const maxC = uint8(2)
	saturated := 50
	for i := 0; i < saturated; i++ {
		c2.inflight[shredKey{kind: repairRequestWindowIndex, slot: 72, index: uint32(i)}] = &shredInflight{concurrent: maxC}
	}
	fill := SlotRepairRequest{Slot: 72, MissingDataShreds: seq(0, uint32(share+500))}
	c2.boundHeadToAdmissionShare(&fill, maxC)
	if got := len(fill.MissingDataShreds); got != share {
		t.Fatalf("fill = %d indices, want the full room %d", got, share)
	}
	if fill.MissingDataShreds[0] != uint32(saturated) {
		t.Fatalf("fill starts at %d, want %d (first non-saturated index)", fill.MissingDataShreds[0], saturated)
	}

	// A shred with room for another attempt (below maxConcurrent) is KEPT —
	// retries must not be starved by the share fill.
	c3 := newPacingTestClient(t)
	c3.inflight[shredKey{kind: repairRequestWindowIndex, slot: 73, index: 5}] = &shredInflight{concurrent: 1}
	retryable := SlotRepairRequest{Slot: 73, MissingDataShreds: []uint32{5}}
	c3.boundHeadToAdmissionShare(&retryable, maxC)
	if len(retryable.MissingDataShreds) != 1 || retryable.MissingDataShreds[0] != 5 {
		t.Fatalf("retryable shred (below maxConcurrent) must be kept, got %v", retryable.MissingDataShreds)
	}
}

// Selection skips peers at their inflight cap: a slow peer's admission
// slots recycle 5-8x slower than a fast one's, so unbounded per-peer
// queueing lets the slow population clog the whole window.
func TestSelectionSkipsSaturatedPeers(t *testing.T) {
	client := &repairClient{perPeer: make(map[repairAddressKey]*peerRecord)}
	peers := []gossip.RepairPeer{
		{Addr: &net.UDPAddr{IP: net.IPv4(10, 0, 0, 41), Port: 9401}},
		{Addr: &net.UDPAddr{IP: net.IPv4(10, 0, 0, 42), Port: 9402}},
	}
	k1, _ := repairAddressKeyFromUDP(peers[0].Addr)
	k2, _ := repairAddressKeyFromUDP(peers[1].Addr)
	// Saturation is now the ADAPTIVE per-peer cap (fair share of the admission
	// window), not a fixed constant.
	cap := client.adaptivePerPeerCapLocked(len(peers))
	client.perPeer[k1] = &peerRecord{lastMatched: time.Now(), inflight: cap}
	client.perPeer[k2] = &peerRecord{lastMatched: time.Now()}

	client.mu.Lock()
	for i := 0; i < 50; i++ {
		peer, ok := client.nextPeerLocked(peers)
		if !ok {
			t.Fatal("pick must succeed while an unsaturated peer exists")
		}
		if peer.Addr.String() == peers[0].Addr.String() {
			t.Fatal("saturated peer must be skipped")
		}
	}
	client.perPeer[k2].inflight = cap
	client.rankedAt = time.Time{} // force ranked rebuild after mutation
	if _, ok := client.nextPeerLocked(peers); ok {
		t.Fatal("all peers saturated must yield no pick (tokens return, retry next scan)")
	}
	client.mu.Unlock()
}

// The inflight counter follows the outstanding entry's lifecycle: one
// decrement when the entry leaves (timely match or expiry) — a late answer
// must NOT decrement again.
func TestPeerInflightLifecycle(t *testing.T) {
	c := &repairClient{perPeer: make(map[repairAddressKey]*peerRecord)}
	addr := repairAddressKey{port: 9500}
	c.mu.Lock()
	c.notePeerSentLocked(addr)
	c.notePeerSentLocked(addr)
	if c.perPeer[addr].inflight != 2 {
		t.Fatalf("inflight after 2 sends = %d, want 2", c.perPeer[addr].inflight)
	}
	c.notePeerTimelyLocked(addr, 100*time.Millisecond)
	c.notePeerTimeoutLocked(addr)
	if c.perPeer[addr].inflight != 0 {
		t.Fatalf("inflight after match+expiry = %d, want 0", c.perPeer[addr].inflight)
	}
	c.notePeerLateLocked(addr, time.Second)
	if c.perPeer[addr].inflight != 0 {
		t.Fatalf("late answer double-decremented inflight: %d", c.perPeer[addr].inflight)
	}
	c.mu.Unlock()
}

// The retry model: an endgame head shred fans out immediately, then adds a
// retry once the newest attempt goes stale — up to the concurrency cap —
// while every attempt keeps the LONG accounting window (a fast retry never
// shortens a peer's grace).
func TestRetryModelEndgameEscalation(t *testing.T) {
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("sink: %v", err)
	}
	defer sink.Close()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	c := newPacingTestClient(t)
	sinkAddr := sink.LocalAddr().(*net.UDPAddr)
	peers := make([]gossip.RepairPeer, 0, 4)
	for i := 0; i < 4; i++ {
		peers = append(peers, gossip.RepairPeer{Addr: &net.UDPAddr{IP: sinkAddr.IP, Port: sinkAddr.Port}})
	}
	c.peerCache = peers
	c.peerCacheAt = time.Now()

	pol := headPolicy(10) // endgame: initial 2, max 3
	const acct = time.Second
	sk := shredKey{kind: repairRequestWindowIndex, slot: 50, index: 7}

	sent := 0
	for c.sendShredAttempt(conn, peers, repairRequestWindowIndex, 50, 7, pol, acct) {
		sent++
	}
	if sent != 2 {
		t.Fatalf("initial endgame fanout sent %d, want 2 (initialConcurrent)", sent)
	}
	c.mu.Lock()
	if inf := c.inflight[sk]; inf == nil || inf.concurrent != 2 || inf.nextAttempt != 2 {
		t.Fatalf("inflight after fanout = %+v, want concurrent 2 / nextAttempt 2", inf)
	}
	for key, o := range c.outstanding {
		if key.shred() == sk && o.accountAt.Sub(o.sentAt) != acct {
			t.Fatalf("attempt accountAt window = %v, want the accounting timeout %v (retry must not shorten it)", o.accountAt.Sub(o.sentAt), acct)
		}
	}
	c.mu.Unlock()

	// Newest attempt still fresh → no retry.
	if c.sendShredAttempt(conn, peers, repairRequestWindowIndex, 50, 7, pol, acct) {
		t.Fatal("attempt within the retry interval must not send")
	}
	// Age it past the retry interval → one retry, to the concurrency cap.
	c.mu.Lock()
	c.inflight[sk].lastSentAt = time.Now().Add(-2 * repairRetryHeadEndgame)
	c.mu.Unlock()
	if !c.sendShredAttempt(conn, peers, repairRequestWindowIndex, 50, 7, pol, acct) {
		t.Fatal("a stale attempt must spawn a retry")
	}
	c.mu.Lock()
	if c.inflight[sk].concurrent != 3 {
		t.Fatalf("concurrent after retry = %d, want 3 (maxConcurrent)", c.inflight[sk].concurrent)
	}
	c.inflight[sk].lastSentAt = time.Now().Add(-time.Second) // stale again
	c.mu.Unlock()
	if c.sendShredAttempt(conn, peers, repairRequestWindowIndex, 50, 7, pol, acct) {
		t.Fatal("must never exceed maxConcurrent, even when stale")
	}
}

// A timely answer to the ORIGINAL endgame attempt (after another fanout went
// out) credits that peer as timely, then retires all sibling attempts because
// the shred is already satisfied.
func TestRepairAnswerCancelsSiblingAttempts(t *testing.T) {
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("sink: %v", err)
	}
	defer sink.Close()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	c := newPacingTestClient(t)
	sinkAddr := sink.LocalAddr().(*net.UDPAddr)
	peers := []gossip.RepairPeer{{Addr: &net.UDPAddr{IP: sinkAddr.IP, Port: sinkAddr.Port}}}
	c.peerCache = peers
	c.peerCacheAt = time.Now()

	pol := headPolicy(1)
	const acct = time.Second
	sk := shredKey{kind: repairRequestWindowIndex, slot: 60, index: 3}

	if !c.sendShredAttempt(conn, peers, repairRequestWindowIndex, 60, 3, pol, acct) {
		t.Fatal("first attempt must send")
	}
	// Endgame sends two attempts immediately; capture attempt 0's nonce.
	c.mu.Lock()
	o0 := c.outstanding[repairRequestKey{kind: repairRequestWindowIndex, slot: 60, index: 3, attempt: 0}]
	c.mu.Unlock()
	if !c.sendShredAttempt(conn, peers, repairRequestWindowIndex, 60, 3, pol, acct) {
		t.Fatal("second endgame fanout attempt must send")
	}
	c.mu.Lock()
	concurrent := c.inflight[sk].concurrent
	c.mu.Unlock()
	if concurrent != 2 {
		t.Fatalf("concurrent = %d, want 2 after endgame fanout", concurrent)
	}

	// The ORIGINAL peer answers timely; the sibling is neutral-cancelled.
	from := &net.UDPAddr{IP: sinkAddr.IP, Port: sinkAddr.Port}
	if !observeRepairForTest(c, conn, nonceTrailer(o0.nonce), from, &Shred{Slot: 60, Index: 3, Type: ShredTypeData}) {
		t.Fatal("original attempt's answer must match")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inflight[sk] != nil || len(c.outstanding) != 0 || len(c.byResponse) != 0 {
		t.Fatalf("satisfied shred retained work: inflight=%v outstanding=%d responses=%d", c.inflight[sk], len(c.outstanding), len(c.byResponse))
	}
	addrKey, _ := repairAddressKeyFromUDP(from)
	if rec := c.perPeer[addrKey]; rec == nil || rec.timely != 1 || rec.late != 0 || rec.timeouts != 0 {
		t.Fatalf("answering peer = %+v, want timely 1 / late 0 / timeouts 0 (retry must not mis-score)", c.perPeer[addrKey])
	}
}

func TestVerifiedBroadcastCancelsOutstandingRepair(t *testing.T) {
	c := newPacingTestClient(t)
	addr := repairAddressKey{port: 8008}
	sk := shredKey{kind: repairRequestWindowIndex, slot: 70, index: 9}
	key := repairRequestKey{kind: sk.kind, slot: sk.slot, index: sk.index}
	req := outstandingRepairRequest{key: key, nonce: 44, addr: addr}
	c.outstanding[key] = req
	c.byResponse[repairResponseKey{addr: addr, nonce: req.nonce}] = key
	c.inflight[sk] = &shredInflight{concurrent: 1, nextAttempt: 1}
	c.perPeer[addr] = &peerRecord{sent: 1, inflight: 1}

	c.satisfyDataShred(&Shred{Slot: sk.slot, Index: sk.index, Type: ShredTypeData})

	if len(c.outstanding) != 0 || len(c.byResponse) != 0 || c.inflight[sk] != nil {
		t.Fatalf("broadcast-satisfied repair was not cancelled")
	}
	if got := c.perPeer[addr].inflight; got != 0 {
		t.Fatalf("peer inflight = %d, want 0 after neutral cancellation", got)
	}
	if c.responses.Load() != 0 || c.timeouts.Load() != 0 {
		t.Fatalf("broadcast cancellation must not score a peer outcome")
	}
}

func TestCompletedSlotCancelsAllRepairWork(t *testing.T) {
	c := newPacingTestClient(t)
	addr := repairAddressKey{port: 8009}
	for index := uint32(0); index < 3; index++ {
		sk := shredKey{kind: repairRequestWindowIndex, slot: 80, index: index}
		key := repairRequestKey{kind: sk.kind, slot: sk.slot, index: sk.index}
		req := outstandingRepairRequest{key: key, nonce: 100 + index, addr: addr}
		c.outstanding[key] = req
		c.byResponse[repairResponseKey{addr: addr, nonce: req.nonce}] = key
		c.inflight[sk] = &shredInflight{concurrent: 1, nextAttempt: 1}
	}
	c.perPeer[addr] = &peerRecord{sent: 3, inflight: 3}

	c.cancelSlotRequests(80)

	if len(c.outstanding) != 0 || len(c.byResponse) != 0 || len(c.inflight) != 0 {
		t.Fatalf("completed slot retained repair state")
	}
	if got := c.perPeer[addr].inflight; got != 0 {
		t.Fatalf("peer inflight = %d, want 0 after slot cancellation", got)
	}
}

// Admission cap: outstanding never exceeds the configured fraction of one
// second of service rate no matter
// what the token bucket would allow — queue DEPTH, not the timeout, bounds
// peer-side queueing (the bufferbloat spiral observed live: outstanding
// ~3000 at 500/s pushed avg latency to 4.4s with zero throughput gain).
func TestAdmissionCapBoundsOutstanding(t *testing.T) {
	c := newPacingTestClient(t)
	admission := int(float64(repairMaxRequestsPerSecond) * repairAdmissionQueueSeconds)
	for i := 0; i < admission-10; i++ {
		key := repairRequestKey{kind: repairRequestWindowIndex, slot: 1, index: uint32(i)}
		c.outstanding[key] = outstandingRepairRequest{key: key}
	}
	if got := c.takeRateTokens(100); got != 10 {
		t.Fatalf("grant = %d, want 10 (admission room, not bucket balance)", got)
	}
	for i := admission - 10; i < admission; i++ {
		key := repairRequestKey{kind: repairRequestWindowIndex, slot: 1, index: uint32(i)}
		c.outstanding[key] = outstandingRepairRequest{key: key}
	}
	if got := c.takeRateTokens(100); got != 0 {
		t.Fatalf("grant at cap = %d, want 0 (full token bucket must not override admission)", got)
	}
}

// Followups draw from the shared token bucket: a drained bucket sends none
// (the scan covers the slot on its own cadence), a full one sends the
// followup cap plus the reserved chained probe and keeps the remainder.
func TestFollowupsAreMeteredByTokenBucket(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer conn.Close()
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("sink listen: %v", err)
	}
	defer sink.Close()

	prime := func() (*repairClient, *net.UDPAddr, []byte, *Shred) {
		c := newPacingTestClient(t)
		c.peerCache = []gossip.RepairPeer{{Addr: sink.LocalAddr().(*net.UDPAddr)}}
		c.peerCacheAt = time.Now()

		from := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 9), Port: 8009}
		addrKey, _ := repairAddressKeyFromUDP(from)
		reqKey := repairRequestKey{kind: repairRequestHighestWindowIndex, slot: 50, index: 0}
		c.outstanding[reqKey] = outstandingRepairRequest{key: reqKey, nonce: 5, addr: addrKey, sentAt: time.Now()}
		c.byResponse[repairResponseKey{addr: addrKey, nonce: 5}] = reqKey

		// Highest response reveals a 200-shred gap, not last-in-slot.
		return c, from, nonceTrailer(5), &Shred{Slot: 50, Index: 200, Type: ShredTypeData}
	}

	c, from, packet, shred := prime()
	// Drain the bucket. Two calls: the admission cap trims the first grant
	// by the primed outstanding entry, leaving a stray token.
	c.takeRateTokens(repairMaxRequestsPerSecond)
	c.takeRateTokens(repairMaxRequestsPerSecond)
	if !observeRepairForTest(c, conn, packet, from, shred) {
		t.Fatal("response itself must match")
	}
	if got := c.requests.Load(); got != 0 {
		t.Fatalf("drained bucket sent %d followups, want 0", got)
	}

	c, from, packet, shred = prime()
	c.mu.Lock()
	c.rateRefillAt = time.Now()
	c.rateTokens = repairMaxRequestsPerSecond
	c.mu.Unlock()
	if !observeRepairForTest(c, conn, packet, from, shred) {
		t.Fatal("response itself must match")
	}
	// A full bucket sends the whole revealed gap: under the adaptive per-peer
	// cap a lone peer no longer saturates at a fixed 16, so the 200-shred gap
	// plus the chained probe (201) all land, metered only by the token budget.
	want := uint64(201)
	if got := c.requests.Load(); got != want {
		t.Fatalf("full bucket sent %d requests, want %d (full gap + chain probe)", got, want)
	}
	c.mu.Lock()
	remaining := c.rateTokens
	c.mu.Unlock()
	wantTokens := float64(repairMaxRequestsPerSecond) - float64(want)
	if remaining < wantTokens-1 || remaining > wantTokens+1 {
		t.Fatalf("bucket holds %.0f tokens, want ~%.0f (unused grant returned)", remaining, wantTokens)
	}
}
