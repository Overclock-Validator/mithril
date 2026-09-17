package turbine

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	repairproto "github.com/Overclock-Validator/mithril/pkg/repair"
)

const (
	repairScanInterval        = 100 * time.Millisecond
	repairPeerRefreshInterval = 2 * time.Second
	// Match blockstream's 64-slot catchup priority window. At high healthy
	// repair rates, scanning only 32 slots left the admission window half empty
	// and replay repeatedly reached the end of its prefetched runway.
	repairMaxSlotsPerScan = 64
	// Max NEW requests one scan may put on the wire, independent of how many
	// tokens the bucket has accumulated. Each send does a random-nonce Ed25519
	// signature (~13us measured, BenchmarkRepairRequestSign); without this cap a
	// full one-second token burst (tens of thousands at the aggressive rate)
	// would make a single scan sign for hundreds of ms, delaying the expiry scan
	// and flooding the socket. 6000/scan x 10 scans/s = 60k/s leaves headroom
	// above the rate ceiling so this only bounds bursts, never steady state, and
	// 6000 signs (~80ms) stays under the 100ms scan interval.
	repairMaxSendsPerScan = 6000
	// Per-slot missing-shred listing cap for NON-head (window/edge) slots. The
	// emission-gating HEAD is capped separately and much higher by the assembler
	// (repairHeadMaxMissing) so it can list a whole monster slot; window slots
	// only prefill what is coming next off leftover budget, so a moderate cap
	// keeps one big window slot from hogging the window's share. (Raising THIS
	// alone was the bug in the first cut — it fed window slots, not the head.)
	repairMaxMissingPerSlot   = 2048
	repairMaxFollowupRequests = 256
	// Outstanding hard ceiling — matched to cavey's kv-block-production
	// (= maxDataShredsPerSlot, "a full slot in flight"). This is only the
	// backstop; the rate x admission window below is the real governor.
	repairMaxOutstanding = 64 * 1024
	// TWO timeouts, deliberately separated (the key to head responsiveness):
	//
	//   RETRY timeout — how long ONE in-flight attempt for a missing shred
	//   may sit before the scan sends ANOTHER attempt to a different peer.
	//   Short for the emission-gating head (a silent peer must not block
	//   replay for seconds), longer for bulk (avoid duplicating thousands
	//   of shreds). See repairRetry* below.
	//
	//   ACCOUNTING timeout — how long the original attempt stays tracked so
	//   a peer that answers LATER is still credited (timely if within this
	//   window, so an aggressive retry never mis-scores a merely-slower peer)
	//   and, if it never answers, debited exactly once as a real timeout.
	//   This is the adaptive latency-driven value; retries do NOT touch it.
	//
	// Before the split, the accounting timeout gated retries too, so the
	// head crawled: a request stretched to seconds before we asked anyone
	// else. Now a stale head attempt spawns a fresh concurrent attempt fast
	// while the original keeps its (correct) latency/scoring accounting.
	repairMinRequestTimeout    = 1500 * time.Millisecond
	repairMaxRequestTimeout    = 6 * time.Second
	repairTimeoutLatencyFactor = 3
	repairLatencyEWMAAlpha     = 0.1
	// Retry intervals by tier and, for the head, by how close it is to
	// completion. Only the endgame permits concurrent attempts: duplicating
	// the whole bulk/prefetch window when the receive queue is seconds deep
	// halves distinct-shred throughput. Bulk attempts retry after their
	// accounting timeout releases the in-flight entry.
	repairRetryHeadEndgame     = 150 * time.Millisecond // head within the fanout window
	repairRetryHeadNear        = 250 * time.Millisecond // head within repairRetryHeadNearMissing
	repairRetryHeadFar         = 500 * time.Millisecond // large head — don't over-duplicate
	repairRetryBulk            = 800 * time.Millisecond // window/edge slots
	repairRetryHeadNearMissing = 128
	// Concurrent in-flight attempts allowed per missing shred, by tier: the
	// head endgame fans a shred across a few distinct peers (fastest wins),
	// while near/far head and bulk work stay one-request-per-distinct-shred.
	repairMaxAttemptsHeadEndgame = 3
	repairMaxAttemptsHead        = 1
	repairMaxAttemptsBulk        = 1
	// Expired-request memory: (responder, nonce) keys of timed-out requests,
	// two rotating generations. A response landing after expiry is matched
	// here instead of vanishing into ignored_old — it counts as a LATE
	// answer, credits the peer as a responder, and feeds its true latency to
	// the adaptive timeout.
	//
	// The generation SIZE is rate-scaled (expiredGenCapLocked) so the two
	// generations retain a roughly fixed WALL-CLOCK window of expirations at the
	// current rate. A fixed small cap was fine at 500/s but at tens of thousands
	// of req/s it rotates out in a fraction of a second, so late answers could
	// no longer be matched during the exact congestion this is meant to observe.
	repairExpiredRetentionSeconds = 4.0
	repairExpiredGenMin           = 8192
	repairExpiredGenMax           = 131072
	// Global request-rate START (the starting point; the AIMD controller ramps
	// it up toward repairAutoRateMax when the peer set absorbs it cleanly and
	// multiplicatively backs it off on a QoS-throttle signal — see
	// repair_catchup.go). It is deliberately NOT the experimental ceiling:
	// pinning default==max would disable the controller (can't ramp, can't back
	// off). This base is already aggressive — 30x the old 500 and well past the
	// ~2000/s that once froze serve-repair — so catchup moves immediately, then
	// the controller finds the peer set's real ceiling and retreats if it trips
	// a ban. Fully overridable via block.repair_max_requests_per_second, which
	// pins a fixed rate and turns the controller OFF (an explicit operator
	// decision). HISTORY: cavey's kv-block-production runs repair unthrottled and
	// does not stall on these clusters; low caps left monster-block catchup
	// crawling at ~80 useful shreds/s, hence the aggressive posture.
	// Live shred-only catchup on the Alpenglow community testnet plateaued at
	// roughly the cluster slot rate from a 10k ceiling: the admission window
	// could only sustain 4-6k useful shreds/s through monster-block stretches.
	// The dedicated repair socket remains healthy at 15k (100/103 peers
	// responding, >90% timely); when its admission window binds below the token
	// ceiling, the controller raises it until replay closes the gap.
	repairMaxRequestsPerSecond = 15000
	// Success-weighted peer selection: a peer that answered within this
	// window counts as a responder, and 3 of 4 requests go to responders.
	// Blind round-robin across a cluster where only a few peers still
	// retain old shreds wastes nearly every request on silence (observed
	// live: 95% timeouts during a resume-gap catchup).
	repairResponderWindow = 60 * time.Second
	repairPeerStatsCap    = 1024
	// Per-peer quality score: an EWMA over request outcomes in [0,1]. A
	// timely answer at/below repairScoreFullLatency earns 1.0 (linearly
	// less when slower), a timeout earns 0 — so the score decays naturally
	// as a peer stops answering. A LATE answer is NOT a replacement
	// outcome: the request already scored 0 when it expired, and the late
	// match adds a second touch at repairScoreLateReward. An always-late
	// peer therefore settles near lateReward*alpha/(1-(1-alpha)^2) ~ 0.16 —
	// deliberately below the raw 0.3: from the requester's side a late
	// answer still burned a timeout (and possibly a retry) before serving.
	// Selection concentrates on the higher-scoring half of the responder
	// set while the 1-in-4 full-set round-robin keeps exploring; the floor
	// stops overfitting to a handful of peers.
	repairScoreAlpha       = 0.1
	repairScoreLateReward  = 0.3
	repairScoreFullLatency = 300 * time.Millisecond
	repairRankedMinPeers   = 8
	repairRankedRebuildTTL = 500 * time.Millisecond
	// Head fanout engages only in the ENDGAME: with a big hole, doubling
	// every head request halves distinct-shred throughput on the exact
	// tokens that gate emission (bulk fill is rate-bound, not tail-latency
	// bound). Once the head is within this many missing shreds of
	// completion, one slow peer IS the completion time — redundancy pays.
	repairHeadFanoutMaxMissing = 32
	// Admission cap (Little's law): at a fixed service rate, in-flight
	// requests beyond rate x latency add ZERO throughput — they just sit in
	// the peers' QoS queues inflating latency. Observed live: outstanding
	// ~3000 at 500/s pushed average response latency to 4.4s and pinned the
	// adaptive timeout at its ceiling (which in turn widened the in-flight
	// window — a positive feedback loop), while the same wire rate at
	// outstanding ~200 ran at ~300ms. The target sits above the healthy
	// median latency (~0.7s observed with the per-peer cap in place) so
	// admission never binds in the good regime, but bounds queue depth the
	// moment service degrades; the per-peer inflight cap keeps the slow
	// population from absorbing the extra room this allows.
	repairAdmissionQueueSeconds = 0.75
	// The head's share of the admission window. 1.0 was tried live and
	// LOWERED emission throughput (6.5s/slot vs 3.8s on easier content):
	// total fill rate is peer-service-bound either way, so a total head
	// monopoly only removed the overlap — zero window prefill ("window
	// blocks 0" on every heartbeat), zero freshness repair (disk-complete
	// frozen), and every small slot re-paid full serial fill latency as it
	// became the head. Half keeps the head strictly first in line while the
	// remainder prefills what is next and patches the live edge.
	repairHeadAdmissionShare = 0.5
	// Per-peer in-flight cap FLOOR. The cap itself is ADAPTIVE — each peer gets
	// a fair share of the admission window, cap = max(floor, window/peerCount)
	// where window = rate x queueSeconds (see adaptivePerPeerCapLocked). This is
	// the fix for a small validator set: a fixed cap times a ~60-80 peer count
	// is the hard ceiling on total outstanding, so a low fixed cap throttled us
	// below the peer set's real serving capacity no matter how high the rate
	// went (at 16 the ceiling was ~1000 outstanding). The fair-share cap tracks
	// the rate, so cranking the rate automatically lets each peer hold more,
	// while still bounding any one peer to its share so a slow peer can't clog
	// the whole window (the 63ms<->4.9s sawtooth that motivated the cap). The
	// old fixed 16 was, not by coincidence, ~= window/peers for the old 500/s
	// rate; this generalizes it. The floor preserves that behavior on very
	// large peer sets.
	repairPerPeerInflightFloor = 16
)

// DefaultRepairRequestsPerSecond is the built-in rate ceiling, exported as
// the floor/starting point for the catchup auto-tuner.
const DefaultRepairRequestsPerSecond = repairMaxRequestsPerSecond

type RepairPeerSource func() []gossip.RepairPeer

type RepairStats struct {
	Requests    uint64
	Responses   uint64
	Timeouts    uint64
	Pings       uint64
	Pongs       uint64
	Errors      uint64
	Outstanding int
	Peers       int
	// Peers that answered a request within repairResponderWindow — the
	// number that separates "cluster serves this range" from "only N nodes
	// still retain it".
	RespondingPeers int
	// Answers that arrived AFTER their request expired (already counted in
	// Timeouts). LateResponses tracking Timeouts means peers serve nearly
	// everything but slower than the timeout — a pacing problem, not loss.
	LateResponses uint64
	// The adaptive request timeout currently in effect, and the smoothed
	// response latency driving it.
	TimeoutMillis     int64
	AvgResponseMillis int64
}

type repairRequestKind uint8

const (
	repairRequestWindowIndex repairRequestKind = iota
	repairRequestHighestWindowIndex
)

type repairRequestKey struct {
	kind  repairRequestKind
	slot  uint64
	index uint32
	// attempt distinguishes concurrent in-flight sends of the SAME shred to
	// different peers — head-slot fanout AND fast-retry (a stale attempt
	// spawns the next one). Monotonic per shred; keyed so each attempt has
	// its own outstanding entry and nonce.
	attempt uint8
}

// shredKey identifies a missing shred independent of which attempt is asking
// for it — the unit the retry logic reasons about.
type shredKey struct {
	kind  repairRequestKind
	slot  uint64
	index uint32
}

func (k repairRequestKey) shred() shredKey {
	return shredKey{kind: k.kind, slot: k.slot, index: k.index}
}

// shredInflight is the per-shred retry bookkeeping: how many attempts are
// live, the next attempt id to hand out, and when the newest attempt went to
// the wire (the retry clock). Kept alongside `outstanding` so the retry
// decision is O(1) instead of an O(outstanding) scan per missing shred.
type shredInflight struct {
	concurrent  uint8
	nextAttempt uint8
	lastSentAt  time.Time
}

type repairAddressKey struct {
	ip   [16]byte
	port int
}

type repairResponseKey struct {
	addr  repairAddressKey
	nonce uint32
}

type outstandingRepairRequest struct {
	key    repairRequestKey
	nonce  uint32
	addr   repairAddressKey
	sentAt time.Time
	// accountAt: when this attempt is forgotten and (if still unanswered)
	// the peer is debited a real timeout. A retry does NOT shorten this —
	// the attempt stays matchable-as-timely until here.
	accountAt time.Time
}

// peerRecord counters follow the request lifecycle: every request resolves
// exactly once as timely OR expired (timeouts). A late answer REFINES an
// expired request — late is a subset of timeouts, not a third disjoint
// outcome — so timely+timeouts is the request total and late/timeouts is
// the "expired but eventually served" share.
type peerRecord struct {
	sent        uint64
	timely      uint64  // responses matched within the timeout
	late        uint64  // expired requests later answered (subset of timeouts)
	timeouts    uint64  // requests that expired, INCLUDING the late-answered ones
	inflight    int     // requests currently outstanding at this peer
	latEWMASec  float64 // smoothed response latency, timely + late
	score       float64 // rolling quality in [0,1]; see notePeerOutcomeLocked
	lastMatched time.Time
}

// RepairPeerReport is one peer's service record, for file-log peer tables.
// Timeouts includes the Late requests (a late answer is an expired request
// that eventually served — see peerRecord).
type RepairPeerReport struct {
	Addr              string
	Sent              uint64
	Timely            uint64
	Late              uint64
	Timeouts          uint64
	AvgResponseMillis int64
	Score             float64
	SecondsSinceHeard int64 // -1: never answered
}

type repairClient struct {
	priorityWake chan struct{} // initialized before the receiver starts; coalesced hints
	identity     ed25519.PrivateKey
	peerSource   RepairPeerSource

	mu          sync.Mutex
	outstanding map[repairRequestKey]outstandingRepairRequest
	byResponse  map[repairResponseKey]repairRequestKey
	inflight    map[shredKey]*shredInflight
	perPeer     map[repairAddressKey]*peerRecord
	// Selection counters. Three SEPARATE counters on purpose: gateCursor
	// only decides exploit-vs-explore, rankedCursor rings the ranked set,
	// exploreCursor rings the full set. Sharing one counter between the %4
	// gate and a ring index locks the reachable indices to fixed residues
	// whenever a ring length divides 4 — review caught the top-2 ranked
	// peers receiving ZERO traffic at len(ranked)=8 via exactly that
	// aliasing.
	gateCursor    uint64
	rankedCursor  uint64
	exploreCursor uint64

	// Quality-ranked responder cache (mu-guarded); see pickResponderLocked.
	ranked   []gossip.RepairPeer
	rankedAt time.Time

	peerCacheMu sync.Mutex
	peerCache   []gossip.RepairPeer
	peerCacheAt time.Time

	// Token bucket for the global request-rate ceiling (guarded by mu).
	// ratePerSecond is configurable (block.repair_max_requests_per_second);
	// zero means the built-in default.
	rateTokens    float64
	rateRefillAt  time.Time
	ratePerSecond float64

	// Adaptive timeout state: EWMA of observed response latency (mu-guarded),
	// effective timeout published atomically for the expiry scan.
	respLatencyEWMA float64 // seconds
	timeoutNanos    atomic.Int64

	// Recently-expired requests keyed like byResponse, for late-answer
	// matching (mu-guarded, rotating generations). The FULL request record
	// is kept so a late answer takes the exact same path as a timely one:
	// slot validation, responder credit, latency feed, and HWI gap
	// followups.
	expiredCur  map[repairResponseKey]outstandingRepairRequest
	expiredPrev map[repairResponseKey]outstandingRepairRequest

	requests      atomic.Uint64
	responses     atomic.Uint64
	timeouts      atomic.Uint64
	lateResponses atomic.Uint64
	pings         atomic.Uint64
	pongs         atomic.Uint64
	errors        atomic.Uint64
}

func newRepairClient(identity ed25519.PrivateKey, peerSource RepairPeerSource) (*repairClient, error) {
	var err error
	if len(identity) == 0 {
		_, identity, err = ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("generate repair identity: %w", err)
		}
	}
	if len(identity) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid repair identity size %d", len(identity))
	}
	if peerSource == nil {
		return nil, fmt.Errorf("repair peer source is required")
	}
	c := &repairClient{
		priorityWake: make(chan struct{}, 1),
		identity:     append(ed25519.PrivateKey(nil), identity...),
		peerSource:   peerSource,
		outstanding:  make(map[repairRequestKey]outstandingRepairRequest),
		byResponse:   make(map[repairResponseKey]repairRequestKey),
		inflight:     make(map[shredKey]*shredInflight),
		perPeer:      make(map[repairAddressKey]*peerRecord),
		expiredCur:   make(map[repairResponseKey]outstandingRepairRequest, repairExpiredGenMin),
	}
	c.timeoutNanos.Store(int64(repairMinRequestTimeout))
	return c, nil
}

// wakePriority does not send requests or mint rate tokens. It only asks the
// single repair loop to reconsider newly prioritized work sooner.
func (c *repairClient) wakePriority() {
	select {
	case c.priorityWake <- struct{}{}:
	default:
	}
}

func (c *repairClient) run(ctx context.Context, conn *net.UDPConn, assembler *SlotAssembler) {
	runRepairSchedule(ctx, c.priorityWake, repairScanInterval, 20*time.Millisecond, func() {
		c.expireOutstanding(time.Now())
		c.repairOnce(conn, assembler)
	})
}

// Keep periodic scans for retries/freshness. Coalesce urgent hints and bound
// scan frequency; all sends still use the existing token bucket, admission,
// retry, fanout and peer budgets. The loop remains the sole sender.
func runRepairSchedule(ctx context.Context, wake <-chan struct{}, interval, minSpacing time.Duration, scan func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var timer *time.Timer
	var urgent <-chan time.Time
	var last time.Time
	stopTimer := func() {
		if timer != nil {
			timer.Stop()
		}
		urgent = nil
	}
	defer stopTimer()
	run := func() {
		stopTimer()
		if ctx.Err() != nil {
			return
		}
		scan()
		last = time.Now()
	}
	schedule := func() {
		if delay := minSpacing - time.Since(last); delay <= 0 {
			run()
		} else if urgent == nil {
			timer = time.NewTimer(delay)
			urgent = timer.C
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			schedule()
		case <-urgent:
			run()
		case <-wake:
			schedule()
		}
	}
}

func (c *repairClient) repairOnce(conn *net.UDPConn, assembler *SlotAssembler) {
	peers := c.peerSnapshot(time.Now())
	if len(peers) == 0 {
		return
	}
	priority, edge := assembler.RepairRequestsTiered(repairMaxSlotsPerScan, repairMaxMissingPerSlot)
	child, haveChild := assembler.childRepairRequest(time.Now())
	if len(priority)+len(edge) == 0 && !haveChild {
		return
	}

	// The head's retry aggression is set by how close the SLOT is to
	// completion (full missing count, before the admission-share truncation
	// caps the listing this scan may send).
	var headPol *retryPolicy
	if len(priority) > 0 {
		p := headPolicy(len(priority[0].MissingDataShreds))
		headPol = &p
		// Head admission share: the head stays strictly first in line, but
		// may hold at most its share of the admission window — the remainder
		// keeps prefilling the window slots and patching the live edge.
		c.boundHeadToAdmissionShare(&priority[0], p.maxConcurrent)
	}

	// Size the token grant by roughly what this scan could put on the wire
	// (the retry policy usually sends far less — covered shreds no-op — and
	// unused grant is returned, so the rate cap still means "requests sent").
	headInitial := uint8(1)
	if headPol != nil {
		headInitial = headPol.initialConcurrent
	}
	edgeDemand := tierSendDemand(edge, 1)
	want := tierSendDemand(priority, headInitial) + edgeDemand
	if haveChild {
		want += len(child.MissingDataShreds)
	}
	if want > repairMaxOutstanding {
		want = repairMaxOutstanding
	}
	// Bound the per-scan burst so a full token bucket can't make one scan sign
	// tens of thousands of requests (see repairMaxSendsPerScan).
	if want > repairMaxSendsPerScan {
		want = repairMaxSendsPerScan
	}
	budget := c.takeRateTokens(want)
	if budget <= 0 {
		return
	}
	acct := c.accountingTimeout()

	// Freshness split: most of the budget unblocks replay (the priority head
	// window), but a reserved slice patches LIVE-EDGE holes while peers still
	// serve those shreds hot — a hole repaired at receipt costs one cheap
	// round trip; the same hole discovered at hydration time, minutes later,
	// competes with the head for budget against colder peers. The reserve is
	// sized by the edge's actual demand, so a blocked head is never capped
	// below what the edge can use; leftover head budget flows to the edge.
	spent := c.sendTier(conn, peers, priority, splitRepairBudget(budget, edgeDemand), headPol, acct)
	spent += c.sendTier(conn, peers, edge, budget-spent, nil, acct)
	// Parent/normal repair and freshness keep first claim on every token.
	if haveChild {
		spent += c.sendChildRepair(conn, peers, child, budget-spent, acct)
	}
	c.returnRateTokens(budget - spent)
}

// tierSendDemand estimates the packets a tier could emit, for token-draw
// sizing; headInitial multiplies the first (head) request's initial fanout.
// The retry policy usually emits far less (covered shreds no-op), so this
// over-estimates and the unused grant is returned.
func tierSendDemand(requests []SlotRepairRequest, headInitial uint8) int {
	demand := 0
	for reqIdx, req := range requests {
		per := len(req.MissingDataShreds)
		if req.NeedHighestDataShred {
			per++
		}
		if reqIdx == 0 {
			per *= int(headInitial)
		}
		demand += per
	}
	return demand
}

// splitRepairBudget returns the head tier's share: everything except a
// freshness reserve of a fifth, bounded by the edge's actual demand.
func splitRepairBudget(budget, edgeDemand int) int {
	reserve := budget / 5
	if reserve > edgeDemand {
		reserve = edgeDemand
	}
	return budget - reserve
}

// sendTier drives one tier's per-shred retry sends within budget and returns
// how many packets went out. When headPol != nil the FIRST request (the
// emission-gating head) uses that policy; every other request in the tier
// (and the whole edge tier) uses the bulk policy.
func (c *repairClient) sendTier(conn *net.UDPConn, peers []gossip.RepairPeer, requests []SlotRepairRequest, budget int, headPol *retryPolicy, acct time.Duration) int {
	sent := 0
	bulk := bulkPolicy()
	for reqIdx, req := range requests {
		pol := bulk
		if headPol != nil && reqIdx == 0 {
			pol = *headPol
		}
		for _, index := range req.MissingDataShreds {
			for sent < budget && c.sendShredAttempt(conn, peers, repairRequestWindowIndex, req.Slot, index, pol, acct) {
				sent++
			}
			if sent >= budget {
				return sent
			}
		}
		if req.NeedHighestDataShred {
			for sent < budget && c.sendShredAttempt(conn, peers, repairRequestHighestWindowIndex, req.Slot, req.HighestDataShredIndex, pol, acct) {
				sent++
			}
			if sent >= budget {
				return sent
			}
		}
	}
	return sent
}

func (c *repairClient) handleRepairPing(conn *net.UDPConn, packet []byte, from *net.UDPAddr) bool {
	ping, ok := repairproto.DecodePing(packet)
	if !ok {
		return false
	}
	c.pings.Add(1)
	pong, err := repairproto.BuildPong(c.identity, ping)
	if err != nil {
		c.errors.Add(1)
		return true
	}
	if _, err := conn.WriteToUDP(pong, from); err != nil {
		c.errors.Add(1)
		return true
	}
	c.pongs.Add(1)
	return true
}

// shredSatisfiesRequest reports whether a returned shred actually answers the
// request it is matched to by (responder, nonce). Serve-repair answers
// WindowIndex/HighestWindowIndex with DATA shreds only: a WindowIndex response
// must carry the exact requested data index; a HighestWindowIndex response
// carries the highest data shred at or beyond the requested index. Anything
// else — wrong slot, a coding shred, a different/short index — is a
// non-conforming answer. Enforcing this BEFORE crediting stops a
// malicious/buggy peer from echoing a valid nonce with a signed-but-wrong shred
// to farm responder credit, poison peer ranking, inflate response stats, and
// prematurely release the still-missing shred's in-flight retry slot.
func shredSatisfiesRequest(key repairRequestKey, shred *Shred) bool {
	if shred == nil || shred.Slot != key.slot || shred.Type != ShredTypeData {
		return false
	}
	switch key.kind {
	case repairRequestWindowIndex:
		return shred.Index == key.index
	case repairRequestHighestWindowIndex:
		return shred.Index >= key.index
	default:
		return false
	}
}

// observeShredResponse matches an incoming packet against outstanding repair
// requests (responder address + nonce). Returns true when the shred was
// delivered BY REPAIR — it answers one of our requests — so the caller can
// attribute it in per-slot repair accounting.
func (c *repairClient) observeShredResponse(conn *net.UDPConn, packet []byte, from *net.UDPAddr, shred *Shred) bool {
	if from == nil || shred == nil {
		return false
	}
	nonce, ok := repairproto.ResponseNonce(packet)
	if !ok {
		return false
	}
	addrKey, ok := repairAddressKeyFromUDP(from)
	if !ok {
		return false
	}
	responseKey := repairResponseKey{addr: addrKey, nonce: nonce}

	// A late answer — one arriving after its request expired — is matched
	// from the expired-request memory and then treated EXACTLY like a
	// timely one: peer credited as a responder, true tail latency fed to
	// the adaptive timeout, and HWI gap followups fired. Without this, a
	// slow-but-serving cluster looks dead and its revealed gaps never get
	// requested.
	//
	// Locate the request this response claims to answer, but DON'T mutate any
	// state until the returned shred is proven to satisfy that exact request:
	// a peer earns credit (and the shred's in-flight slot is released) only for
	// a conforming answer. See shredSatisfiesRequest.
	c.mu.Lock()
	var (
		outstanding outstandingRepairRequest
		reqKey      repairRequestKey
		late        bool
		found       bool
	)
	if k, ok := c.byResponse[responseKey]; ok {
		outstanding, reqKey, found = c.outstanding[k], k, true
	} else if rec, ok := c.expiredCur[responseKey]; ok {
		outstanding, late, found = rec, true, true
	} else if rec, ok := c.expiredPrev[responseKey]; ok {
		outstanding, late, found = rec, true, true
	}
	if !found || !shredSatisfiesRequest(outstanding.key, shred) {
		// Unmatched, or a non-conforming answer: leave the request outstanding
		// so it still expires into a deserved timeout and keeps its in-flight
		// slot for retry. The peer gets nothing.
		c.mu.Unlock()
		return false
	}
	if late {
		// The expired record lives in exactly one generation; deleting from
		// both is a harmless no-op on the other.
		delete(c.expiredCur, responseKey)
		delete(c.expiredPrev, responseKey)
	} else {
		delete(c.byResponse, responseKey)
		delete(c.outstanding, reqKey)
		// A timely answer releases this attempt's inflight slot. Late answers
		// do NOT: their slot was already released at accounting expiry, so
		// decrementing again would underflow the shred's count.
		c.decInflightLocked(reqKey.shred())
	}
	latency := time.Since(outstanding.sentAt)
	if late {
		c.notePeerLateLocked(addrKey, latency)
	} else {
		c.notePeerTimelyLocked(addrKey, latency)
	}
	// The first conforming answer satisfies the shred. Retire every sibling
	// attempt immediately instead of leaving redundant requests to occupy the
	// admission window (and later deliver duplicate packets) until timeout.
	// This is especially important in catchup: with a 2-6s receive backlog,
	// bulk fast-retries otherwise keep the entire window full of obsolete work.
	c.cancelShredRequestsLocked(outstanding.key.shred())
	c.observeLatencyLocked(latency)
	c.mu.Unlock()

	if entryTraceSelected(shred.Slot) {
		traceRepairResponse(outstanding, shred, from.String(), late)
	}
	if late {
		c.lateResponses.Add(1)
	} else {
		c.responses.Add(1)
	}
	// shredSatisfiesRequest already guaranteed slot match and a data shred; the
	// gap-backfill path below is HWI-only.
	if outstanding.key.kind != repairRequestHighestWindowIndex {
		return true
	}

	peers := c.peerSnapshot(time.Now())
	if len(peers) == 0 {
		return true
	}
	start := outstanding.key.index
	gap := 0
	if shred.Index > start {
		gap = int(shred.Index - start)
	}
	ask := gap
	if ask > repairMaxFollowupRequests {
		ask = repairMaxFollowupRequests
	}
	chainProbe := !shred.LastInSlot() && shred.Index < maxDataShredsPerSlot-1
	if chainProbe {
		ask++
	}
	if ask == 0 {
		return true
	}
	// Followups draw from the SAME token bucket as the scan. This path used
	// to be unmetered — with hundreds of probed slots it pushed the total
	// send rate ~70% past the cap, which is exactly the flood the peer-side
	// QoS ban punishes. When the bucket is dry the scan's deficit-aware
	// selection covers the slot on its own cadence.
	grant := c.takeRateTokens(ask)
	if grant <= 0 {
		return true
	}
	windowBudget := grant
	if chainProbe && windowBudget > 0 {
		windowBudget-- // reserve the chained probe's token
	}
	// Discovery followups are bulk-paced and go through the same inflight
	// dedup as the scan, so a window index already being repaired is not
	// re-sent here.
	bulk := bulkPolicy()
	acct := c.accountingTimeout()
	followups := 0
	for index := start; index < shred.Index && followups < windowBudget; index++ {
		if c.sendShredAttempt(conn, peers, repairRequestWindowIndex, shred.Slot, index, bulk, acct) {
			followups++
		}
	}
	if chainProbe && followups < grant {
		if c.sendShredAttempt(conn, peers, repairRequestHighestWindowIndex, shred.Slot, shred.Index+1, bulk, acct) {
			followups++
		}
	}
	c.returnRateTokens(grant - followups)
	return true
}

// observeLatencyLocked folds one request->response latency into the EWMA and
// refreshes the effective timeout: repairTimeoutLatencyFactor x the smoothed
// latency, clamped to [repairMinRequestTimeout, repairMaxRequestTimeout].
func (c *repairClient) observeLatencyLocked(lat time.Duration) {
	sec := lat.Seconds()
	if sec <= 0 {
		return
	}
	if c.respLatencyEWMA == 0 {
		c.respLatencyEWMA = sec
	} else {
		c.respLatencyEWMA = (1-repairLatencyEWMAAlpha)*c.respLatencyEWMA + repairLatencyEWMAAlpha*sec
	}
	eff := time.Duration(c.respLatencyEWMA * repairTimeoutLatencyFactor * float64(time.Second))
	if eff < repairMinRequestTimeout {
		eff = repairMinRequestTimeout
	}
	if eff > repairMaxRequestTimeout {
		eff = repairMaxRequestTimeout
	}
	c.timeoutNanos.Store(int64(eff))
}

// retryPolicy governs how aggressively a tier re-asks for a still-missing
// shred: initialConcurrent attempts go out immediately (fanout), further
// attempts up to maxConcurrent are added once the newest is retryInterval
// stale (a peer went quiet). See the repairRetry*/repairMaxAttempts* consts.
type retryPolicy struct {
	retryInterval     time.Duration
	initialConcurrent uint8
	maxConcurrent     uint8
}

// headPolicy scales the head's aggression by how close it is to completion:
// a big head is fetched steadily (over-duplicating thousands of shreds only
// wastes budget), a nearly-done head is fanned across peers so one slow peer
// never gates emission.
func headPolicy(missing int) retryPolicy {
	switch {
	case missing <= repairHeadFanoutMaxMissing:
		return retryPolicy{repairRetryHeadEndgame, 2, repairMaxAttemptsHeadEndgame}
	case missing <= repairRetryHeadNearMissing:
		return retryPolicy{repairRetryHeadNear, 1, repairMaxAttemptsHead}
	default:
		return retryPolicy{repairRetryHeadFar, 1, repairMaxAttemptsHead}
	}
}

func bulkPolicy() retryPolicy {
	return retryPolicy{repairRetryBulk, 1, repairMaxAttemptsBulk}
}

// satisfyDataShred retires WindowIndex requests satisfied by a verified data
// shred arriving through any path: a matched repair response, Turbine
// broadcast, FEC/spool hydration, or a duplicate response. Request nonce
// matching still happens first in observeShredResponse so the answering peer
// receives its proper timely/late credit.
func (c *repairClient) satisfyDataShred(shred *Shred) {
	if c == nil || shred == nil || shred.Type != ShredTypeData {
		return
	}
	c.mu.Lock()
	c.cancelShredRequestsLocked(shredKey{
		kind:  repairRequestWindowIndex,
		slot:  shred.Slot,
		index: shred.Index,
	})
	c.mu.Unlock()
}

// cancelSlotRequests retires all repair work for a slot that has assembled or
// is being reset. These requests can no longer contribute useful data; keeping
// them until their accounting deadlines steals admission room from the next
// replay head.
func (c *repairClient) cancelSlotRequests(slot uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	for key, req := range c.outstanding {
		if key.slot != slot {
			continue
		}
		delete(c.outstanding, key)
		delete(c.byResponse, repairResponseKey{addr: req.addr, nonce: req.nonce})
		c.notePeerInflightDoneLocked(req.addr)
	}
	for key := range c.inflight {
		if key.slot == slot {
			delete(c.inflight, key)
		}
	}
	c.mu.Unlock()
}

// cancelShredRequestsLocked retires the handful of concurrent attempts for a
// single shred in O(attempts), not O(all outstanding). It must be called with
// c.mu held.
func (c *repairClient) cancelShredRequestsLocked(sk shredKey) {
	inf := c.inflight[sk]
	if inf == nil {
		return
	}
	for attempt := 0; attempt < int(inf.nextAttempt); attempt++ {
		key := repairRequestKey{
			kind:    sk.kind,
			slot:    sk.slot,
			index:   sk.index,
			attempt: uint8(attempt),
		}
		req, ok := c.outstanding[key]
		if !ok {
			continue
		}
		delete(c.outstanding, key)
		delete(c.byResponse, repairResponseKey{addr: req.addr, nonce: req.nonce})
		c.notePeerInflightDoneLocked(req.addr)
	}
	delete(c.inflight, sk)
}

// accountingTimeout is how long an attempt stays matchable-as-timely (and,
// unanswered, counts as a real timeout) — the adaptive latency-driven value,
// NOT the retry interval.
func (c *repairClient) accountingTimeout() time.Duration {
	t := time.Duration(c.timeoutNanos.Load())
	if t <= 0 {
		t = repairMinRequestTimeout
	}
	return t
}

// sendShredAttempt sends at most one NEW attempt for a missing shred, gated
// by the tier's retry policy: covered (enough concurrent attempts, newest
// still fresh) -> no send. The original attempt is left tracked under its
// own accounting timeout, so a fast retry never shortens a peer's grace or
// mis-scores it. Returns true iff a packet went to the wire.
func (c *repairClient) sendShredAttempt(conn *net.UDPConn, peers []gossip.RepairPeer, kind repairRequestKind, slot uint64, index uint32, pol retryPolicy, acct time.Duration) bool {
	if conn == nil || len(peers) == 0 {
		return false
	}
	sk := shredKey{kind: kind, slot: slot, index: index}
	now := time.Now()

	c.mu.Lock()
	if inf := c.inflight[sk]; inf != nil {
		if inf.concurrent >= pol.maxConcurrent {
			c.mu.Unlock()
			return false
		}
		// Beyond the initial fanout, a new attempt only fires once the
		// newest one has gone retryInterval-stale.
		if inf.concurrent >= pol.initialConcurrent && now.Sub(inf.lastSentAt) < pol.retryInterval {
			c.mu.Unlock()
			return false
		}
	}
	if len(c.outstanding) >= repairMaxOutstanding {
		c.mu.Unlock()
		return false
	}
	peer, ok := c.nextPeerLocked(peers)
	if !ok {
		c.mu.Unlock()
		return false
	}
	addrKey, ok := repairAddressKeyFromUDP(peer.Addr)
	if !ok {
		c.mu.Unlock()
		return false
	}
	if kind != repairRequestWindowIndex && kind != repairRequestHighestWindowIndex {
		c.mu.Unlock()
		return false
	}
	attempt := uint8(0)
	if inf := c.inflight[sk]; inf != nil {
		attempt = inf.nextAttempt
	}
	key := repairRequestKey{kind: kind, slot: slot, index: index, attempt: attempt}
	// RESERVE the attempt slot under the lock (bumps inflight + this peer's
	// count), then release BEFORE signing. Ed25519 signing is ~tens of
	// microseconds; at tens of thousands of req/s, holding the lock across it
	// serialized every send against the response-processing path
	// (observeShredResponse needs the same lock) and could stall the UDP
	// receive loop into kernel drops. The reserve is enough for coherence: a
	// response for this attempt cannot arrive until after we WriteToUDP below,
	// which is strictly after we register outstanding/byResponse.
	c.addInflightLocked(sk, now)
	c.notePeerSentLocked(addrKey)
	c.mu.Unlock()

	undoReserve := func() {
		c.mu.Lock()
		c.decInflightLocked(sk)
		c.notePeerSendAbortedLocked(addrKey)
		c.mu.Unlock()
	}

	var (
		packet []byte
		nonce  uint32
		err    error
	)
	switch kind {
	case repairRequestWindowIndex:
		packet, nonce, err = repairproto.NewWindowIndexRequest(c.identity, peer.Pubkey, slot, uint64(index))
	case repairRequestHighestWindowIndex:
		packet, nonce, err = repairproto.NewHighestWindowIndexRequest(c.identity, peer.Pubkey, slot, uint64(index))
	}
	if err != nil {
		undoReserve()
		c.errors.Add(1)
		return false
	}

	responseKey := repairResponseKey{addr: addrKey, nonce: nonce}
	c.mu.Lock()
	c.outstanding[key] = outstandingRepairRequest{
		key:       key,
		nonce:     nonce,
		addr:      addrKey,
		sentAt:    now,
		accountAt: now.Add(acct),
	}
	c.byResponse[responseKey] = key
	c.mu.Unlock()

	var traceStart int64
	var traceBinding entryRepairTrace
	if entryTraceSelected(slot) {
		traceStart = entryTraceNow()
		traceBinding = entryRepairTrace{Peer: peer.Addr.String(), Nonce: nonce}
	}
	if _, err := conn.WriteToUDP(packet, peer.Addr); err != nil {
		traceRepairSend(slot, index, kind, attempt, traceStart, false, traceBinding)
		c.mu.Lock()
		delete(c.outstanding, key)
		delete(c.byResponse, responseKey)
		c.decInflightLocked(sk)
		c.notePeerSendAbortedLocked(addrKey)
		c.mu.Unlock()
		c.errors.Add(1)
		return false
	}

	traceRepairSend(slot, index, kind, attempt, traceStart, true, traceBinding)
	c.requests.Add(1)
	return true
}

func (c *repairClient) addInflightLocked(sk shredKey, now time.Time) {
	inf := c.inflight[sk]
	if inf == nil {
		inf = &shredInflight{}
		c.inflight[sk] = inf
	}
	inf.concurrent++
	inf.nextAttempt++
	inf.lastSentAt = now
}

// decInflightLocked drops one live attempt for a shred; the last one out
// removes the entry so a shred that comes back around later starts fresh.
func (c *repairClient) decInflightLocked(sk shredKey) {
	inf := c.inflight[sk]
	if inf == nil {
		return
	}
	if inf.concurrent > 0 {
		inf.concurrent--
	}
	if inf.concurrent == 0 {
		delete(c.inflight, sk)
	}
}

// rateLimitLocked is the effective requests-per-second ceiling.
func (c *repairClient) rateLimitLocked() float64 {
	if c.ratePerSecond > 0 {
		return c.ratePerSecond
	}
	return repairMaxRequestsPerSecond
}

// setRateLimit overrides the request-rate ceiling (0 restores the default).
// The automatic caller watches timely/late service and backs off when the
// current cluster/identity begins to see a QoS-throttle signature.
func (c *repairClient) setRateLimit(perSecond int) {
	c.mu.Lock()
	c.ratePerSecond = float64(perSecond)
	c.mu.Unlock()
}

// takeRateTokens refills the request-rate bucket and grants up to want
// tokens, returning how many requests this scan may send. The grant is
// additionally bounded by the ADMISSION CAP: outstanding may never exceed
// ~repairAdmissionQueueSeconds of service rate, whatever the rate bucket
// would allow — the timeout bounds how long we wait, admission bounds how
// deep we queue.
func (c *repairClient) takeRateTokens(want int) int {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	limit := c.rateLimitLocked()
	if room := int(limit*repairAdmissionQueueSeconds) - len(c.outstanding); want > room {
		want = room
	}
	if want <= 0 {
		return 0
	}
	if c.rateRefillAt.IsZero() {
		c.rateRefillAt = now
		// Start with one scan interval, not a full second's burst. A cold
		// catchup previously signed and sent ~10k requests in its first two
		// scans, filling the local UDP receive queue before latency feedback
		// existed. Steady-state refill is unchanged.
		c.rateTokens = limit * repairScanInterval.Seconds()
	}
	elapsed := now.Sub(c.rateRefillAt).Seconds()
	c.rateRefillAt = now
	c.rateTokens += elapsed * limit
	if c.rateTokens > limit {
		c.rateTokens = limit // burst cap: one second's worth
	}
	grant := want
	if float64(grant) > c.rateTokens {
		grant = int(c.rateTokens)
	}
	if grant < 0 {
		grant = 0
	}
	c.rateTokens -= float64(grant)
	return grant
}

// returnRateTokens puts the unused portion of a grant back in the bucket
// (bounded by the burst cap) so dedup-suppressed sends don't burn rate.
func (c *repairClient) returnRateTokens(count int) {
	if count <= 0 {
		return
	}
	c.mu.Lock()
	c.rateTokens += float64(count)
	if limit := c.rateLimitLocked(); c.rateTokens > limit {
		c.rateTokens = limit
	}
	c.mu.Unlock()
}

func (c *repairClient) nextPeerLocked(peers []gossip.RepairPeer) (gossip.RepairPeer, bool) {
	if len(peers) == 0 {
		return gossip.RepairPeer{}, false
	}
	// Adaptive per-peer inflight cap: each peer's fair share of the admission
	// window, so the cap tracks the rate and the peer count rather than being
	// a fixed number that throttles a small validator set.
	cap := c.adaptivePerPeerCapLocked(len(peers))
	c.gateCursor++
	// 3 of 4 requests go to the quality-ranked responder set; every 4th
	// round-robins the FULL set so new or newly-caught-up peers keep being
	// discovered and the responder set can grow back after churn. Both
	// rings skip SATURATED peers (per-peer inflight cap): a slow peer's
	// admission slots recycle 5-8x slower than a fast one's, so unbounded
	// per-peer queueing lets the slow population clog the whole window.
	if c.gateCursor%4 != 0 {
		if peer, ok := c.pickResponderLocked(peers, cap); ok {
			return peer, true
		}
	}
	for attempts := 0; attempts < len(peers); attempts++ {
		c.exploreCursor++
		peer := peers[int(c.exploreCursor%uint64(len(peers)))]
		if peer.Addr == nil || peer.Addr.Port == 0 {
			continue
		}
		if key, ok := repairAddressKeyFromUDP(peer.Addr); ok {
			if rec := c.perPeer[key]; rec != nil && rec.inflight >= cap {
				continue
			}
		}
		return peer, true
	}
	return gossip.RepairPeer{}, false
}

// adaptivePerPeerCapLocked is each peer's fair share of the admission window
// (rate x queueSeconds), floored so very large peer sets keep a sane minimum.
// peerCount x cap >= the window, so the per-peer cap never throttles total
// outstanding below what the peers can serve — the whole point on a small
// validator set — while still bounding any one peer to ~its share.
func (c *repairClient) adaptivePerPeerCapLocked(peerCount int) int {
	if peerCount <= 0 {
		return repairPerPeerInflightFloor
	}
	window := int(c.rateLimitLocked() * repairAdmissionQueueSeconds)
	cap := window/peerCount + 1
	if cap < repairPerPeerInflightFloor {
		cap = repairPerPeerInflightFloor
	}
	return cap
}

// pickResponderLocked round-robins among the QUALITY-RANKED responder set:
// peers that answered within repairResponderWindow, narrowed to the
// higher-scoring half (with a floor so selection never overfits to a
// handful of peers). ok is false when no responder is known yet (fresh
// start, or the whole set went quiet) — the caller falls back to the full
// round-robin. The ranking is cached briefly: selection runs once per
// request and must not rebuild-and-sort the set per call.
func (c *repairClient) pickResponderLocked(peers []gossip.RepairPeer, cap int) (gossip.RepairPeer, bool) {
	now := time.Now()
	if now.Sub(c.rankedAt) > repairRankedRebuildTTL {
		c.rebuildRankedLocked(peers, now)
	}
	for attempts := 0; attempts < len(c.ranked); attempts++ {
		c.rankedCursor++
		peer := c.ranked[int(c.rankedCursor%uint64(len(c.ranked)))]
		if key, ok := repairAddressKeyFromUDP(peer.Addr); ok {
			if rec := c.perPeer[key]; rec != nil && rec.inflight >= cap {
				continue // saturated: give the slot to a peer that can use it
			}
		}
		return peer, true
	}
	return gossip.RepairPeer{}, false
}

func (c *repairClient) rebuildRankedLocked(peers []gossip.RepairPeer, now time.Time) {
	c.rankedAt = now
	type scoredPeer struct {
		peer  gossip.RepairPeer
		score float64
	}
	responders := make([]scoredPeer, 0, 32)
	for _, peer := range peers {
		if peer.Addr == nil || peer.Addr.Port == 0 {
			continue
		}
		key, ok := repairAddressKeyFromUDP(peer.Addr)
		if !ok {
			continue
		}
		if rec := c.perPeer[key]; rec != nil && now.Sub(rec.lastMatched) <= repairResponderWindow {
			responders = append(responders, scoredPeer{peer: peer, score: rec.score})
		}
	}
	sort.SliceStable(responders, func(i, j int) bool { return responders[i].score > responders[j].score })
	keep := len(responders)
	if keep > repairRankedMinPeers {
		if half := (len(responders) + 1) / 2; half > repairRankedMinPeers {
			keep = half
		} else {
			keep = repairRankedMinPeers
		}
	}
	c.ranked = c.ranked[:0]
	for _, sp := range responders[:keep] {
		c.ranked = append(c.ranked, sp.peer)
	}
}

// notePeerSentLocked and the outcome noters maintain the per-peer service
// records behind quality-ranked responder selection.
func (c *repairClient) notePeerSentLocked(addr repairAddressKey) {
	rec := c.perPeer[addr]
	if rec == nil {
		if len(c.perPeer) >= repairPeerStatsCap {
			// Bound memory across peer churn: drop stale non-responders.
			cutoff := time.Now().Add(-repairResponderWindow)
			for key, old := range c.perPeer {
				if old.lastMatched.Before(cutoff) {
					delete(c.perPeer, key)
				}
			}
		}
		rec = &peerRecord{}
		c.perPeer[addr] = rec
	}
	rec.sent++
	rec.inflight++
}

// notePeerSendAbortedLocked undoes the inflight increment when a registered
// request never reached the wire (socket write error).
func (c *repairClient) notePeerSendAbortedLocked(addr repairAddressKey) {
	if rec := c.perPeer[addr]; rec != nil && rec.inflight > 0 {
		rec.inflight--
	}
}

func (c *repairClient) notePeerInflightDoneLocked(addr repairAddressKey) {
	if rec := c.perPeer[addr]; rec != nil && rec.inflight > 0 {
		rec.inflight--
	}
}

// notePeerOutcomeLocked folds one request outcome into the peer's rolling
// score and (for answers) latency EWMA. outcome: 1.0-graded for timely
// answers, repairScoreLateReward for late ones, 0 for timeouts.
func (c *repairClient) notePeerOutcomeLocked(addr repairAddressKey, outcome float64, lat time.Duration) *peerRecord {
	rec := c.perPeer[addr]
	if rec == nil {
		rec = &peerRecord{}
		c.perPeer[addr] = rec
	}
	rec.score = (1-repairScoreAlpha)*rec.score + repairScoreAlpha*outcome
	if lat > 0 {
		sec := lat.Seconds()
		if rec.latEWMASec == 0 {
			rec.latEWMASec = sec
		} else {
			rec.latEWMASec = (1-repairLatencyEWMAAlpha)*rec.latEWMASec + repairLatencyEWMAAlpha*sec
		}
	}
	return rec
}

func (c *repairClient) notePeerTimelyLocked(addr repairAddressKey, lat time.Duration) {
	quality := 1.0
	if lat > repairScoreFullLatency {
		quality = repairScoreFullLatency.Seconds() / lat.Seconds()
	}
	rec := c.notePeerOutcomeLocked(addr, quality, lat)
	rec.timely++
	rec.lastMatched = time.Now()
	if rec.inflight > 0 {
		rec.inflight-- // late answers don't decrement: expiry already did
	}
}

func (c *repairClient) notePeerLateLocked(addr repairAddressKey, lat time.Duration) {
	rec := c.notePeerOutcomeLocked(addr, repairScoreLateReward, lat)
	rec.late++
	rec.lastMatched = time.Now()
}

func (c *repairClient) notePeerTimeoutLocked(addr repairAddressKey) {
	rec := c.notePeerOutcomeLocked(addr, 0, 0)
	rec.timeouts++
	if rec.inflight > 0 {
		rec.inflight--
	}
}

// expireOutstanding retires attempts past their per-request ACCOUNTING
// deadline (not the short retry interval): the peer is debited a real
// timeout exactly once here, the attempt moves to late-answer memory, and
// its inflight slot is released. Retries happen in the scan while the
// attempt is still alive; this is only the terminal "never answered" path.
func (c *repairClient) expireOutstanding(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, outstanding := range c.outstanding {
		if now.Before(outstanding.accountAt) {
			continue
		}
		responseKey := repairResponseKey{addr: outstanding.addr, nonce: outstanding.nonce}
		delete(c.outstanding, key)
		delete(c.byResponse, responseKey)
		c.decInflightLocked(key.shred())
		c.rememberExpiredLocked(responseKey, outstanding)
		c.notePeerTimeoutLocked(outstanding.addr)
		c.timeouts.Add(1)
	}
}

// expiredGenCapLocked sizes each expired-memory generation so the two
// generations together retain ~repairExpiredRetentionSeconds of expirations at
// the current rate: cap = rate x retention / 2, clamped. This keeps late-answer
// matching working at high rates instead of rotating out in a fraction of a
// second (see the repairExpired* const block).
func (c *repairClient) expiredGenCapLocked() int {
	genCap := int(c.rateLimitLocked() * repairExpiredRetentionSeconds / 2)
	if genCap < repairExpiredGenMin {
		genCap = repairExpiredGenMin
	}
	if genCap > repairExpiredGenMax {
		genCap = repairExpiredGenMax
	}
	return genCap
}

// rememberExpiredLocked keeps an expired request's full record so a late
// answer can still be recognized and handled like a timely one. Two
// rotating, rate-scaled generations bound memory to a time window.
func (c *repairClient) rememberExpiredLocked(key repairResponseKey, req outstandingRepairRequest) {
	genCap := c.expiredGenCapLocked()
	if c.expiredCur == nil {
		c.expiredCur = make(map[repairResponseKey]outstandingRepairRequest, genCap)
	}
	if len(c.expiredCur) >= genCap {
		c.expiredPrev = c.expiredCur
		c.expiredCur = make(map[repairResponseKey]outstandingRepairRequest, genCap)
	}
	c.expiredCur[key] = req
}

func (c *repairClient) peerSnapshot(now time.Time) []gossip.RepairPeer {
	c.peerCacheMu.Lock()
	if !c.peerCacheAt.IsZero() && now.Sub(c.peerCacheAt) < repairPeerRefreshInterval {
		peers := c.peerCache
		c.peerCacheMu.Unlock()
		return peers
	}
	c.peerCacheMu.Unlock()

	peers := c.peerSource()

	c.peerCacheMu.Lock()
	c.peerCache = peers
	c.peerCacheAt = now
	c.peerCacheMu.Unlock()
	return peers
}

func (c *repairClient) cachedPeerCount() int {
	c.peerCacheMu.Lock()
	count := len(c.peerCache)
	cacheFresh := !c.peerCacheAt.IsZero() && time.Since(c.peerCacheAt) < repairPeerRefreshInterval
	c.peerCacheMu.Unlock()
	if cacheFresh || c.peerSource == nil {
		return count
	}
	return len(c.peerSnapshot(time.Now()))
}

// RepairPeerAggregate is the one-clause peer-quality picture: how many
// peers serve us, how reliably, and how the quality mass is distributed —
// enough to see the trend in a summary line without a table. Percentages
// are over resolved requests (timely + expired); a late answer is an
// expired request that eventually served, so LatePct is the slice of that
// same total, NOT a third bucket (timely% + expired-unanswered% +
// late% = 100).
type RepairPeerAggregate struct {
	Tracked             int
	Responding          int
	TimelyPct           int   // timely / (timely + timeouts)
	LatePct             int   // late / (timely + timeouts); late ⊂ timeouts
	MedianLatencyMillis int64 // median of responding peers' latency EWMAs
	ScoreP50            float64
	ScoreP90            float64
}

func (c *repairClient) peerAggregate() RepairPeerAggregate {
	cutoff := time.Now().Add(-repairResponderWindow)
	c.mu.Lock()
	agg := RepairPeerAggregate{Tracked: len(c.perPeer)}
	var timely, late, timeouts uint64
	scores := make([]float64, 0, len(c.perPeer))
	lats := make([]float64, 0, len(c.perPeer))
	for _, rec := range c.perPeer {
		timely += rec.timely
		late += rec.late
		timeouts += rec.timeouts
		if rec.lastMatched.After(cutoff) {
			agg.Responding++
			scores = append(scores, rec.score)
			if rec.latEWMASec > 0 {
				lats = append(lats, rec.latEWMASec)
			}
		}
	}
	c.mu.Unlock()
	// Every request resolves exactly once as timely or expired; late refines
	// expired. Summing all three would count late-answered requests twice
	// and deflate the timely share.
	if total := timely + timeouts; total > 0 {
		agg.TimelyPct = int(100 * timely / total)
		agg.LatePct = int(100 * late / total)
	}
	sort.Float64s(scores)
	if len(scores) > 0 {
		agg.ScoreP50 = scores[len(scores)/2]
		agg.ScoreP90 = scores[(len(scores)-1)*9/10]
	}
	sort.Float64s(lats)
	if len(lats) > 0 {
		agg.MedianLatencyMillis = int64(lats[len(lats)/2] * 1000)
	}
	return agg
}

// outstandingForSlot counts in-flight requests for one slot — the number
// that proves (or disproves) the head's admission share in the catchup
// heartbeat.
func (c *repairClient) outstandingForSlot(slot uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.outstandingForSlotLocked(slot)
}

func (c *repairClient) outstandingForSlotLocked(slot uint64) int {
	n := 0
	for key := range c.outstanding {
		if key.slot == slot {
			n++
		}
	}
	return n
}

// boundHeadToAdmissionShare fills the head's listing with up to
// share-room indices that are NOT already in flight, and returns the head
// fanout — decided on the FULL listing, because "endgame" is about how
// close the SLOT is to completion, not how much room this particular scan
// has. Filtering (rather than truncating) matters: the listing's prefix is
// exactly the indices sent first, so a plain prefix truncation re-selects
// mostly-outstanding indices and the head under-fills its share whenever
// room is small (observed live as head in-flight drooping to single
// digits between response bursts).
func (c *repairClient) boundHeadToAdmissionShare(head *SlotRepairRequest, maxConcurrent uint8) {
	c.mu.Lock()
	room := int(c.rateLimitLocked()*repairAdmissionQueueSeconds*repairHeadAdmissionShare) - c.outstandingForSlotLocked(head.Slot)
	if room < 0 {
		room = 0
	}
	kept := head.MissingDataShreds[:0]
	for _, index := range head.MissingDataShreds {
		if len(kept) >= room {
			break
		}
		sk := shredKey{kind: repairRequestWindowIndex, slot: head.Slot, index: index}
		// Keep shreds that can still accept an attempt (a fresh send OR a
		// fast retry); skip only those already at the head's concurrency
		// cap. Skipping merely-in-flight shreds would starve retries — the
		// whole point of the split — and re-selecting the fully-covered
		// prefix would under-fill the share (the live-observed droop).
		if inf := c.inflight[sk]; inf != nil && inf.concurrent >= maxConcurrent {
			continue
		}
		kept = append(kept, index)
	}
	c.mu.Unlock()
	head.MissingDataShreds = kept
}

// peerReport snapshots the busiest peers' service records, sorted by sent
// descending, for file-log peer tables.
func (c *repairClient) peerReport(maxPeers int) []RepairPeerReport {
	now := time.Now()
	c.mu.Lock()
	reports := make([]RepairPeerReport, 0, len(c.perPeer))
	for addr, rec := range c.perPeer {
		since := int64(-1)
		if !rec.lastMatched.IsZero() {
			since = int64(now.Sub(rec.lastMatched).Seconds())
		}
		reports = append(reports, RepairPeerReport{
			Addr:              net.JoinHostPort(net.IP(addr.ip[:]).String(), strconv.Itoa(addr.port)),
			Sent:              rec.sent,
			Timely:            rec.timely,
			Late:              rec.late,
			Timeouts:          rec.timeouts,
			AvgResponseMillis: int64(rec.latEWMASec * 1000),
			Score:             rec.score,
			SecondsSinceHeard: since,
		})
	}
	c.mu.Unlock()
	sort.Slice(reports, func(i, j int) bool { return reports[i].Sent > reports[j].Sent })
	if maxPeers > 0 && len(reports) > maxPeers {
		reports = reports[:maxPeers]
	}
	return reports
}

func repairAddressKeyFromUDP(addr *net.UDPAddr) (repairAddressKey, bool) {
	var key repairAddressKey
	if addr == nil {
		return key, false
	}
	ip := addr.IP.To16()
	if ip == nil || addr.Port == 0 {
		return key, false
	}
	copy(key.ip[:], ip)
	key.port = addr.Port
	return key, true
}

func (c *repairClient) stats() RepairStats {
	peers := c.cachedPeerCount()
	cutoff := time.Now().Add(-repairResponderWindow)
	c.mu.Lock()
	outstanding := len(c.outstanding)
	responding := 0
	for _, rec := range c.perPeer {
		if rec.lastMatched.After(cutoff) {
			responding++
		}
	}
	avgResponseMillis := int64(c.respLatencyEWMA * 1000)
	c.mu.Unlock()
	return RepairStats{
		Requests:          c.requests.Load(),
		Responses:         c.responses.Load(),
		Timeouts:          c.timeouts.Load(),
		Pings:             c.pings.Load(),
		Pongs:             c.pongs.Load(),
		Errors:            c.errors.Load(),
		Outstanding:       outstanding,
		Peers:             peers,
		RespondingPeers:   responding,
		LateResponses:     c.lateResponses.Load(),
		TimeoutMillis:     int64(time.Duration(c.timeoutNanos.Load()) / time.Millisecond),
		AvgResponseMillis: avgResponseMillis,
	}
}

// Called after normal priority and freshness work. Existing in-flight child
// requests count against lookahead capacity; no fanout or highest-index probes.
func (c *repairClient) sendChildRepair(conn *net.UDPConn, peers []gossip.RepairPeer, req SlotRepairRequest, budget int, acct time.Duration) int {
	if budget <= 0 {
		return 0
	}
	c.mu.Lock()
	room := childRepairLimit - c.outstandingForSlotLocked(req.Slot)
	c.mu.Unlock()
	if room <= 0 {
		return 0
	}
	req.NeedHighestDataShred = false
	return c.sendTier(conn, peers, []SlotRepairRequest{req}, min(room, budget), nil, acct)
}
