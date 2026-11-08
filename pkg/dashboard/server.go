package dashboard

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/net/netutil"
	"golang.org/x/time/rate"
)

type frameSet struct {
	messages            [][]byte
	publicJSON          []byte
	manifestMessages    [][]byte
	manifestPublicJSON  []byte
	readyMessages       [][]byte
	readyPublicJSON     []byte
	safetyMessages      [][]byte
	safetyPublicJSON    []byte
	advancedMessages    [][]byte
	advancedPublicJSON  []byte
	freshUntil          time.Time
	lease               *statusFrameLease
	safetyUnavailable   bool
	advancedUnavailable bool
	clockOrigin         time.Time
	clockUncertain      *atomic.Bool
	fence               statusFence
}

type statusFrameLeaseValue struct {
	until     time.Time
	wallUntil time.Time
	changed   chan struct{}
}

type statusFrameLease struct {
	value atomic.Pointer[statusFrameLeaseValue]
}

type statusFrameUpdate struct {
	changed chan struct{}
}

func newStatusFrameLease(until time.Time) *statusFrameLease {
	lease := &statusFrameLease{}
	lease.value.Store(&statusFrameLeaseValue{until: until, wallUntil: until.Round(0), changed: make(chan struct{})})
	return lease
}

// Only the serial status collector tightens frame leases.
func (lease *statusFrameLease) tighten(until, wallUntil time.Time) {
	old := lease.value.Load()
	if old == nil {
		return
	}
	if !until.Before(old.until) {
		until = old.until
	}
	if !wallUntil.Before(old.wallUntil) {
		wallUntil = old.wallUntil
	}
	if until.Equal(old.until) && wallUntil.Equal(old.wallUntil) {
		return
	}
	lease.value.Store(&statusFrameLeaseValue{until: until, wallUntil: wallUntil, changed: make(chan struct{})})
	close(old.changed)
}

const manifestProfileQuery = "include=snapshot_manifests"
const readyProfileQuery = "include=snapshot_manifests_v2"
const safetyProfileQuery = "include=snapshot_manifests_v3"
const advancedProfileQuery = "include=advanced_status"

func snapshotProfile(query string) int {
	switch query {
	case manifestProfileQuery:
		return 1
	case readyProfileQuery:
		return 2
	case safetyProfileQuery:
		return 3
	case advancedProfileQuery:
		return 4
	default:
		return 0
	}
}

func (f *frameSet) messagesForProfile(profile int) [][]byte {
	if profile == 4 {
		return f.advancedMessages
	}
	if profile == 3 {
		return f.safetyMessages
	}
	if profile == 2 && len(f.readyMessages) != 0 {
		return f.readyMessages
	}
	if profile == 1 && len(f.manifestMessages) != 0 {
		return f.manifestMessages
	}
	return f.messages
}

// An unavailable v3 projection does not inherit the default profile's lease.
func (f *frameSet) deadlineForProfile(profile int) time.Time {
	if profile == 3 && f.safetyUnavailable || profile == 4 && f.advancedUnavailable {
		return time.Time{}
	}
	return f.freshUntil
}

// statusRemaining checks wall and monotonic clocks at delivery. A detected
// discontinuity remains uncertain until the reader is restarted.
func (f *frameSet) statusRemaining(profile int, now time.Time) (time.Duration, <-chan struct{}, bool) {
	until := f.deadlineForProfile(profile)
	if until.IsZero() {
		return 0, nil, true
	}
	var changed <-chan struct{}
	monoRemaining, wallRemaining := until.Sub(now), until.Round(0).Sub(now.Round(0))
	if f.lease != nil {
		value := f.lease.value.Load()
		if value == nil {
			return 0, nil, false
		}
		monoRemaining = min(monoRemaining, value.until.Sub(now))
		wallRemaining = min(wallRemaining, value.wallUntil.Sub(now.Round(0)))
		changed = value.changed
	}
	if f.clockUncertain == nil || f.clockUncertain.Load() || !clusterClockConsistent(f.clockOrigin, now) {
		if f.clockUncertain != nil {
			f.clockUncertain.Store(true)
		}
		return 0, changed, false
	}
	remaining := min(monoRemaining, wallRemaining)
	return remaining, changed, remaining > 0
}

func (f *frameSet) remainingStatus(profile int, now time.Time) (time.Duration, bool) {
	remaining, _, ok := f.statusRemaining(profile, now)
	return remaining, ok
}

func (f *frameSet) statusWriteDeadline(profile int, now time.Time) (time.Time, bool) {
	remaining, ok := f.remainingStatus(profile, now)
	if !ok {
		return time.Time{}, false
	}
	if f.deadlineForProfile(profile).IsZero() {
		return now.Add(2 * time.Second), true
	}
	return now.Add(min(2*time.Second, remaining)), true
}

type inputLimitedListener struct{ net.Listener }

func (l inputLimitedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &inputLimitedConn{Conn: conn, limit: rate.NewLimiter(4096, 8192)}, nil
}

type inputLimitedConn struct {
	net.Conn
	limit *rate.Limiter
}

func (c *inputLimitedConn) Read(b []byte) (int, error) {
	if len(b) > 8192 {
		b = b[:8192]
	}
	n, err := c.Conn.Read(b)
	// Count framing bytes too: unfinished messages can contain arbitrarily
	// many empty continuation frames without reaching the payload limit.
	if !c.limit.AllowN(time.Now(), n) {
		_ = c.Conn.Close()
		return 0, errors.New("input byte rate exceeded")
	}
	return n, err
}

type collector struct {
	client           *http.Client
	last             *Snapshot // owned by the collection loop
	lastAcceptedRun  string
	lastAcceptedAt   time.Time
	lastAcceptedSum  [sha256.Size]byte
	statusLeaseSeen  bool
	statusLeaseEnd   time.Time
	statusFreshEnd   time.Time
	statusSampleEnd  time.Time
	statusSampleWall time.Time
	statusNodeEpoch  uint64
	statusPriorEnd   time.Time
	statusPriorWall  time.Time
	statusLeases     []*statusFrameLease // owned by the collection loop
	clockOrigin      time.Time
	clockUncertain   atomic.Bool
	latest           atomic.Pointer[frameSet]
	statusUpdate     atomic.Pointer[statusFrameUpdate]
	public           bool
	publicOptions    PublicOptions
	throughput       Throughput // owned by the collection loop, never by viewers
	resourceCPU      resourceCPURate
	history          *historyService
	networkGate      atomic.Pointer[networkDetailGate]
	localPeerGate    atomic.Pointer[localPeerGate]
	machine          *machineDetailReader
}

func newCollector(socket string) *collector {
	dialer := &net.Dialer{Timeout: time.Second}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socket)
		},
		DisableKeepAlives:      true,
		MaxResponseHeaderBytes: 4096,
	}
	c := &collector{clockOrigin: time.Now(), client: &http.Client{Transport: transport, Timeout: time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	c.statusUpdate.Store(&statusFrameUpdate{changed: make(chan struct{})})
	return c
}

func (c *collector) publishStatusFrame(frame *frameSet) {
	c.latest.Store(frame)
	old := c.statusUpdate.Swap(&statusFrameUpdate{changed: make(chan struct{})})
	if old != nil {
		close(old.changed)
	}
}

// ponytail: retain at most 16 live frame leases, expiring the oldest when full;
// increase the bound only with a measured memory budget and revised tests.
func (c *collector) tightenStatusLeases(bound, wallBound, now time.Time) {
	leases, n := c.statusLeases, 0
	for _, lease := range leases {
		lease.tighten(bound, wallBound)
		value := lease.value.Load()
		if statusLeaseContinuous(value.until.Sub(now), value.wallUntil.Sub(now.Round(0))) {
			leases[n], n = lease, n+1
		}
	}
	clear(leases[n:])
	c.statusLeases = leases[:n]
}

// statusLease accepts a missing header only for a legacy private feed.
func statusLease(header http.Header) (time.Duration, bool, bool) {
	values := header.Values(statusRemainingHeader)
	if len(values) == 0 {
		return 0, false, true
	}
	if len(values) != 1 {
		return 0, true, false
	}
	milliseconds, err := strconv.ParseUint(values[0], 10, 64)
	if err != nil || milliseconds > uint64(StaleAfter.Milliseconds()) || strconv.FormatUint(milliseconds, 10) != values[0] {
		return 0, true, false
	}
	return time.Duration(milliseconds) * time.Millisecond, true, true
}

func (c *collector) poll(ctx context.Context, now time.Time) {
	previous := c.last
	collectionStarted := time.Now()
	var receivedAt, observedAgeEnd, observedLeaseEnd time.Time
	var sampleAccepted, repeatedSample bool
	state := "unavailable"
	reason := "source_unavailable"
	if c.last != nil {
		state = "stale"
	}
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://node/snapshot", nil)
	response, err := c.client.Do(request)
	if err == nil {
		defer response.Body.Close()
		b, readErr := io.ReadAll(io.LimitReader(response.Body, MaxPrivateSnapshotBytes+1))
		// Date the result after receipt, not before a concurrently newer node
		// sample. Monotonic elapsed time preserves the caller's clock origin.
		receivedAt = time.Now()
		now = now.Add(receivedAt.Sub(collectionStarted))
		if response.StatusCode == http.StatusOK && readErr == nil {
			reason = "source_invalid"
		}
		var snapshot Snapshot
		remaining, hasLease, validLease := statusLease(response.Header)
		valid := response.StatusCode == http.StatusOK && readErr == nil && len(b) <= MaxPrivateSnapshotBytes &&
			validLease && json.Unmarshal(b, &snapshot) == nil && snapshot.validate() == nil && !snapshot.SampledAt.After(now.Add(time.Second))
		var sum [sha256.Size]byte
		if valid {
			canonical, marshalErr := json.Marshal(snapshot)
			if marshalErr != nil {
				valid = false
			} else {
				sum = sha256.Sum256(canonical)
			}
		}
		sameRun := snapshot.RunID == c.lastAcceptedRun && c.lastAcceptedRun != ""
		// A repeated timestamp identifies the same cached sample, not new data.
		// Compare decoded fields so unknown private extensions remain ignored.
		if valid && sameRun && (snapshot.SampledAt.Before(c.lastAcceptedAt) || c.statusLeaseSeen && !hasLease ||
			snapshot.SampledAt.Equal(c.lastAcceptedAt) && sum != c.lastAcceptedSum) {
			valid = false
		}
		if valid {
			sampleAccepted = true
			repeatedSample = sameRun && snapshot.SampledAt.Equal(c.lastAcceptedAt)
			if !sameRun {
				c.statusLeaseSeen, c.statusLeaseEnd = false, time.Time{}
				c.statusFreshEnd = time.Time{}
			}
			if hasLease {
				observedLeaseEnd = collectionStarted.Add(remaining)
				until := observedLeaseEnd
				if sameRun && snapshot.SampledAt.Equal(c.lastAcceptedAt) && !c.statusLeaseEnd.IsZero() && c.statusLeaseEnd.Before(until) {
					until = c.statusLeaseEnd
				}
				c.statusLeaseSeen, c.statusLeaseEnd = true, until
			}
			// Anchor both bounds to this receipt; an unchanged sample cannot
			// gain more lifetime on a later poll or a wall-clock rollback.
			age := max(time.Duration(0), now.Sub(snapshot.SampledAt))
			observedAgeEnd = receivedAt.Add(StaleAfter - age)
			until := observedAgeEnd
			if hasLease && c.statusLeaseEnd.Before(until) {
				until = c.statusLeaseEnd
			}
			if sameRun && snapshot.SampledAt.Equal(c.lastAcceptedAt) && !c.statusFreshEnd.IsZero() && c.statusFreshEnd.Before(until) {
				until = c.statusFreshEnd
			}
			c.statusFreshEnd = until
			// A new run replaces every field, including previously available
			// values that are now unknown. Old-run data must not fill its gaps.
			c.last = &snapshot
			c.lastAcceptedRun, c.lastAcceptedAt, c.lastAcceptedSum = snapshot.RunID, snapshot.SampledAt, sum
			if receivedAt.Before(until) {
				state, reason = "fresh", ""
			} else {
				state, reason = "stale", "status_stale"
			}
		}
	} else {
		receivedAt = time.Now()
		now = now.Add(receivedAt.Sub(collectionStarted))
	}
	if !clusterClockConsistent(c.clockOrigin, receivedAt) {
		c.clockUncertain.Store(true)
	}
	if state == "fresh" && c.clockUncertain.Load() {
		state, reason = "stale", "status_stale"
	}
	var candidateLease *statusFrameLease
	if sampleAccepted {
		lease := newStatusFrameLease(c.statusFreshEnd)
		lease.tighten(observedAgeEnd, observedAgeEnd.Round(0))
		if !observedLeaseEnd.IsZero() {
			lease.tighten(observedLeaseEnd, observedLeaseEnd.Round(0))
		}
		if repeatedSample && !c.statusSampleEnd.IsZero() && !c.statusSampleWall.IsZero() {
			lease.tighten(c.statusSampleEnd, c.statusSampleWall)
		}
		value := lease.value.Load()
		c.statusSampleEnd, c.statusSampleWall = value.until, value.wallUntil
		if state == "fresh" {
			if statusLeaseContinuous(value.until.Sub(receivedAt), value.wallUntil.Sub(receivedAt.Round(0))) {
				candidateLease = lease
			} else {
				state, reason = "stale", "status_stale"
			}
		}
	}
	if state == "fresh" && sameStatusBinding(previous, c.last) {
		value := candidateLease.value.Load()
		c.tightenStatusLeases(value.until, value.wallUntil, receivedAt)
	} else {
		c.tightenStatusLeases(receivedAt, receivedAt.Round(0), receivedAt)
	}
	encoded, err := c.encode(c.last, state, now)
	if err != nil {
		c.tightenStatusLeases(receivedAt, receivedAt.Round(0), receivedAt)
		// Projection/history growth must not leave an earlier fresh frame
		// cached indefinitely. Drop this observation and its rate baseline,
		// but keep the timestamp guard against older same-run samples.
		c.last = nil
		state, reason = "unavailable", "source_invalid"
		encoded, err = c.encode(nil, "unavailable", now)
	}
	if err == nil && state == "fresh" {
		encoded.freshUntil = c.statusFreshEnd
		encoded.clockOrigin, encoded.clockUncertain = c.clockOrigin, &c.clockUncertain
		encoded.lease = candidateLease
	}
	if err == nil && state == "fresh" {
		if len(c.statusLeases) == 16 {
			c.statusLeases[0].tighten(receivedAt, receivedAt.Round(0))
			copy(c.statusLeases, c.statusLeases[1:])
			c.statusLeases = c.statusLeases[:15]
		}
		c.statusLeases = append(c.statusLeases, encoded.lease)
	}
	if c.publicOptions.NetworkDetails || c.publicOptions.MachineDetails || c.publicOptions.LocalPeers {
		c.statusNodeEpoch, _ = nextNodeEpoch(c.statusNodeEpoch, previous, c.last, c.statusPriorEnd, c.statusPriorWall, state, receivedAt)
		if c.publicOptions.NetworkDetails {
			c.observeNetworkDetailStatus(state, reason, receivedAt, c.statusNodeEpoch)
		}
		if c.publicOptions.LocalPeers {
			peerReason := reason
			if state != "fresh" && peerReason == "" {
				peerReason = "status_stale"
			}
			gate := advanceLocalPeerStatusGate(c.localPeerGate.Load(), c.last, peerReason, receivedAt, c.statusNodeEpoch)
			c.localPeerGate.Store(&gate)
		}
		var machineStamp statusStamp
		if c.machine != nil {
			machineStamp = c.machine.observeStatus(c.last, state, reason, receivedAt, c.statusNodeEpoch)
		}
		if err == nil {
			encoded.fence.nodeEpoch = c.statusNodeEpoch
			if c.publicOptions.NetworkDetails {
				gate := c.networkGate.Load()
				encoded.fence.families[statusNetwork] = gate.stamp.familyEpoch
				encoded.fence.families[statusNetworkNode] = gate.nodeStamp.familyEpoch
			}
			if c.publicOptions.LocalPeers {
				encoded.fence.families[statusPeers] = c.localPeerGate.Load().revision
			}
			if c.machine != nil {
				encoded.fence.families[statusMachine] = machineStamp.familyEpoch
			}
		}
	}
	c.statusPriorEnd, c.statusPriorWall = time.Time{}, time.Time{}
	if candidateLease != nil && state == "fresh" {
		value := candidateLease.value.Load()
		c.statusPriorEnd, c.statusPriorWall = value.until, value.wallUntil
	}
	if err == nil {
		c.publishStatusFrame(encoded)
		if c.history != nil {
			c.history.offer(encoded.publicJSON, now)
		}
	} else {
		c.publishStatusFrame(nil)
	}
}

// observeNetworkDetailStatus publishes detached status context for the reader.
func (c *collector) observeNetworkDetailStatus(state, reason string, receivedAt time.Time, nodeEpoch uint64) {
	g := networkDetailGate{observedAt: receivedAt, reason: "source_unavailable", nodeReason: "source_unavailable"}
	if c.last != nil {
		g = networkGate(*c.last, receivedAt)
	}
	if state != "fresh" {
		if reason == "" {
			reason = "status_stale"
		}
		g.reason = networkDetailReason(reason)
		g.nodeReason = g.reason
	}
	var oldNode, oldReceiver statusStamp
	nodeSame, receiverSame := false, false
	if old := c.networkGate.Load(); old != nil {
		oldNode, oldReceiver = old.nodeStamp, old.stamp
		nodeSame = sameNodeNetworkBinding(old.nodeBinding(), g.nodeBinding()) && old.nodeReason == g.nodeReason
		receiverSame = nodeSame && old.matches(g.binding) && g.matches(old.binding) && old.reason == g.reason
	}
	var nodeOK, receiverOK bool
	g.nodeStamp, nodeOK = advanceStatusStamp(oldNode, nodeEpoch, nodeSame)
	g.stamp, receiverOK = advanceStatusStamp(oldReceiver, nodeEpoch, receiverSame && oldNode == g.nodeStamp)
	if !nodeOK {
		g.nodeReason, g.reason = "source_unavailable", "source_unavailable"
	}
	if !receiverOK {
		g.reason = "source_unavailable"
	}
	c.networkGate.Store(&g)
}

func (c *collector) encode(s *Snapshot, state string, now time.Time) (*frameSet, error) {
	if !c.public {
		messages, err := frames(s, state, now)
		return &frameSet{messages: messages}, err
	}
	throughput := c.throughput.Observe(s, state, now)
	var resources *PublicResources
	if c.publicOptions.Resources {
		resources = publicResources(s, state, now)
		resources.CPU = c.resourceCPU.observe(s, resources)
	}
	var peers *PublicPeers
	if c.publicOptions.Peers {
		peers = publicPeers(s, state, now)
	}
	b, err := encodePublicSnapshot(s, state, now, &throughput, resources, peers)
	if err != nil {
		return nil, err
	}
	frame, err := json.Marshal(message{Topic: "mithril", Key: "public", Value: json.RawMessage(b)})
	if err != nil {
		return nil, err
	}
	result := &frameSet{messages: [][]byte{frame}, publicJSON: b}
	advanced, advancedErr := encodePublicSnapshotProfile(s, state, now, &throughput, resources, peers, 4)
	if advancedErr == nil {
		advancedFrame, frameErr := json.Marshal(message{Topic: "mithril", Key: "public", Value: json.RawMessage(advanced)})
		if frameErr == nil {
			result.advancedPublicJSON, result.advancedMessages = advanced, [][]byte{advancedFrame}
		}
	}
	result.advancedUnavailable = result.advancedPublicJSON == nil
	if s != nil && s.SnapshotManifests != nil {
		enhanced, err := encodePublicSnapshotProfile(s, state, now, &throughput, resources, peers, 1)
		if err == nil {
			enhancedFrame, frameErr := json.Marshal(message{Topic: "mithril", Key: "public", Value: json.RawMessage(enhanced)})
			if frameErr == nil {
				result.manifestPublicJSON, result.manifestMessages = enhanced, [][]byte{enhancedFrame}
			}
		}
		if s.SnapshotManifests.ReadyMarker == nil {
			result.readyPublicJSON, result.readyMessages = result.manifestPublicJSON, result.manifestMessages
		} else {
			ready, err := encodePublicSnapshotProfile(s, state, now, &throughput, resources, peers, 2)
			if err == nil {
				readyFrame, frameErr := json.Marshal(message{Topic: "mithril", Key: "public", Value: json.RawMessage(ready)})
				if frameErr == nil {
					result.readyPublicJSON, result.readyMessages = ready, [][]byte{readyFrame}
				}
			}
		}
	}
	safety, err := encodePublicSnapshotProfile(s, state, now, &throughput, resources, peers, 3)
	if err != nil {
		// Never replace a failed v3 projection with a seemingly fresh older
		// profile that omits its independent safety-fault observation.
		result.safetyUnavailable = true
		safety, err = encodePublicSnapshotProfile(nil, "unavailable", now, nil, nil, nil, 3)
		if err != nil {
			return nil, err
		}
	}
	safetyFrame, err := json.Marshal(message{Topic: "mithril", Key: "public", Value: json.RawMessage(safety)})
	if err != nil {
		return nil, err
	}
	result.safetyPublicJSON, result.safetyMessages = safety, [][]byte{safetyFrame}
	return result, nil
}

// Serve connects to one fixed local node feed. The browser cannot select a
// source, filesystem path, RPC method or node operation. Sharing requires a
// separately configured authenticated proxy; this listener is loopback-only.
func Serve(ctx context.Context, socket, address string) error {
	return listen(ctx, socket, address, "", PublicOptions{})
}

// PublicOptions enables additional disclosure. All fields default off.
type PublicOptions struct {
	Resources      bool
	Peers          bool
	NetworkDetails bool
	MachineDetails bool
	LocalPeers     bool
	HistoryDir     string
	ClusterFile    string
}

// ServePublic serves an explicitly enabled public projection on loopback.
// An HTTPS reverse proxy must preserve the configured Host and Origin.
func ServePublic(ctx context.Context, socket, address, origin string) error {
	return ServePublicWithOptions(ctx, socket, address, origin, PublicOptions{})
}

// ServePublicWithOptions adds only the explicitly enabled public data groups.
func ServePublicWithOptions(ctx context.Context, socket, address, origin string, options PublicOptions) error {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || strings.Contains(origin, "#") || strings.ContainsAny(u.Host, "\\ \t\r\n") {
		return errors.New("dashboard public origin must be an HTTPS origin")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("dashboard public origin has an invalid port")
		}
	} else if strings.HasSuffix(u.Host, ":") {
		return errors.New("dashboard public origin has an invalid port")
	}
	return listen(ctx, socket, address, origin, options)
}

func listen(ctx context.Context, socket, address, origin string, options PublicOptions) error {
	if !filepath.IsAbs(socket) {
		return errors.New("dashboard node socket must be an absolute path")
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return errors.New("dashboard address must be a loopback IP and port")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	return serveProfile(ctx, socket, listener, origin, options)
}

func serve(ctx context.Context, socket string, listener net.Listener) error {
	return serveProfile(ctx, socket, listener, "", PublicOptions{})
}

func serveProfile(ctx context.Context, socket string, listener net.Listener, origin string, options PublicOptions) error {
	defer listener.Close()
	if options.NetworkDetails && (origin == "" || !options.Resources) {
		return errors.New("dashboard network details require the public profile and resource disclosure")
	}
	if options.MachineDetails && (origin == "" || !options.Resources) {
		return errors.New("dashboard machine details require the public profile and resource disclosure")
	}
	if options.LocalPeers && origin == "" {
		return errors.New("dashboard local peers require the public profile")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := newCollector(socket)
	c.public = origin != ""
	c.publicOptions = options
	defer c.client.CloseIdleConnections()
	if (options.HistoryDir != "" || options.ClusterFile != "") && origin == "" {
		return errors.New("dashboard history and cluster data require a public profile")
	}
	if options.HistoryDir != "" {
		var err error
		c.history, err = newHistoryService(options.HistoryDir, options.Resources)
		if err != nil {
			return err
		}
		go c.history.run(ctx)
		defer func() { cancel(); <-c.history.done }()
	}
	var external *clusterReader
	if options.ClusterFile != "" {
		var err error
		external, err = newClusterReader(ctx, options.ClusterFile)
		if err != nil {
			return err
		}
		defer external.close()
	}
	var network *networkDetailReader
	if options.NetworkDetails {
		var err error
		network, err = newNetworkDetailReader(c.client, &c.networkGate)
		if err != nil {
			return err
		}
		network.admit = func(reply networkDetailReply) (time.Duration, bool) {
			return c.admitNetworkDetail(network, reply, statusNetworkNode)
		}
	}
	if options.MachineDetails {
		var err error
		c.machine, err = newMachineDetailReader(c.client)
		if err != nil {
			return err
		}
	}
	var localPeers *localPeerReader
	if options.LocalPeers {
		var err error
		localPeers, err = newLocalPeerReader(c.client, &c.localPeerGate)
		if err != nil {
			return err
		}
		localPeers.admit = func(reply localPeerReply) (time.Duration, bool) {
			return c.admitLocalPeers(localPeers, reply)
		}
	}
	initial, err := c.encode(nil, "unavailable", time.Now().UTC())
	if err != nil {
		return err
	}
	c.latest.Store(initial)
	collected := make(chan struct{})
	go func() {
		defer close(collected)
		c.poll(ctx, time.Now().UTC())
		if network != nil {
			go network.run(ctx)
		}
		if c.machine != nil {
			go c.machine.run(ctx)
		}
		if localPeers != nil {
			go localPeers.run(ctx)
		}
		ticker := time.NewTicker(SampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.poll(ctx, time.Now().UTC())
			}
		}
	}()
	if network != nil {
		defer func() { cancel(); <-network.done }()
	}
	if c.machine != nil {
		defer func() { cancel(); <-c.machine.done }()
	}
	if localPeers != nil {
		defer func() { cancel(); <-localPeers.done }()
	}
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	allowed := func(r *http.Request) bool {
		if origin == "" {
			return allowedRequest(r, port)
		}
		u, _ := url.Parse(origin)
		origins := r.Header.Values("Origin")
		return r.Host == u.Host && (len(origins) == 0 || len(origins) == 1 && origins[0] == origin)
	}
	peers := make(chan struct{}, 16)
	var sessions sync.WaitGroup
	var sessionMu sync.Mutex
	closing := false
	upgrader := websocket.Upgrader{HandshakeTimeout: 2 * time.Second, ReadBufferSize: 1024, WriteBufferSize: 4096,
		CheckOrigin: allowed}
	mux := http.NewServeMux()
	var machineHandler http.HandlerFunc
	var localPeerHandler http.HandlerFunc
	if c.public {
		features, _ := json.Marshal(struct {
			SchemaVersion int  `json:"schema_version"`
			History       bool `json:"history"`
			Cluster       bool `json:"cluster"`
			NodeDisplay   bool `json:"node_display,omitempty"`
		}{1, c.history != nil, external != nil, false})
		advancedFeatures, _ := json.Marshal(struct {
			SchemaVersion  int  `json:"schema_version"`
			History        bool `json:"history"`
			Cluster        bool `json:"cluster"`
			NetworkDetails bool `json:"network_details,omitempty"`
			MachineDetails bool `json:"machine_details,omitempty"`
			LocalPeers     bool `json:"local_peers,omitempty"`
		}{1, c.history != nil, external != nil, network != nil, c.machine != nil, localPeers != nil})
		mux.HandleFunc("/api/v1/features", func(w http.ResponseWriter, r *http.Request) {
			if !allowed(r) {
				http.Error(w, "host or origin not allowed", http.StatusForbidden)
				return
			}
			if r.Method != http.MethodGet || r.URL.RawQuery != "" && r.URL.RawQuery != advancedProfileQuery || r.URL.ForceQuery || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
				http.Error(w, "unsupported request", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if r.URL.RawQuery == advancedProfileQuery {
				_, _ = w.Write(advancedFeatures)
			} else {
				_, _ = w.Write(features)
			}
		})
		if external != nil {
			mux.HandleFunc("/api/v1/cluster", external.handler(allowed, clusterDirectoryRoute))
			mux.HandleFunc("/api/v1/cluster-map", external.handler(allowed, clusterMapRoute))
			mux.HandleFunc("/api/v1/cluster-versions", external.handler(allowed, clusterVersionsRoute))
			mux.HandleFunc("/api/v1/cluster-performance-history", external.handler(allowed, clusterPerformanceHistoryRoute))
		}
		if c.history != nil {
			mux.HandleFunc("/api/v1/history", c.history.handler(allowed))
		}
		if localPeers != nil {
			localPeerHandler = func(w http.ResponseWriter, r *http.Request) {
				if !allowed(r) {
					http.Error(w, "host or origin not allowed", http.StatusForbidden)
					return
				}
				localPeers.serveHTTP(w, r)
			}
			mux.HandleFunc(localPeerPublicPath, localPeerHandler)
		}
		if network != nil {
			mux.HandleFunc("/api/v1/details/network", func(w http.ResponseWriter, r *http.Request) {
				if !allowed(r) {
					http.Error(w, "host or origin not allowed", http.StatusForbidden)
					return
				}
				network.serveHTTP(w, r)
			})
		}
		if c.machine != nil {
			machineHandler = func(w http.ResponseWriter, r *http.Request) {
				if !allowed(r) {
					http.Error(w, "host or origin not allowed", http.StatusForbidden)
					return
				}
				c.machine.serveHTTP(w, r, func(reply machineReaderReply) (time.Duration, bool) {
					return c.admitMachineDetail(c.machine, reply)
				})
			}
			mux.HandleFunc(machineDetailPublicPath, machineHandler)
		}
		mux.HandleFunc("/api/v1/status", func(w http.ResponseWriter, r *http.Request) {
			if !allowed(r) {
				http.Error(w, "host or origin not allowed", http.StatusForbidden)
				return
			}
			if r.Method != http.MethodGet || (r.URL.RawQuery != "" && snapshotProfile(r.URL.RawQuery) == 0) || r.URL.ForceQuery || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
				http.Error(w, "unsupported request", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			latest := c.latest.Load()
			if latest == nil || r.URL.RawQuery == safetyProfileQuery && latest.safetyPublicJSON == nil || r.URL.RawQuery == advancedProfileQuery && latest.advancedPublicJSON == nil {
				http.Error(w, "status unavailable", http.StatusServiceUnavailable)
				return
			}
			body := latest.publicJSON
			if r.URL.RawQuery == advancedProfileQuery {
				body = latest.advancedPublicJSON
			} else if r.URL.RawQuery == safetyProfileQuery {
				body = latest.safetyPublicJSON
			} else if r.URL.RawQuery == manifestProfileQuery && latest.manifestPublicJSON != nil {
				body = latest.manifestPublicJSON
			} else if r.URL.RawQuery == readyProfileQuery && latest.readyPublicJSON != nil {
				body = latest.readyPublicJSON
			}
			profile := snapshotProfile(r.URL.RawQuery)
			if until := latest.deadlineForProfile(profile); !until.IsZero() {
				remaining, valid := latest.remainingStatus(profile, time.Now())
				if !valid || remaining.Milliseconds() < 1 {
					http.Error(w, "status unavailable", http.StatusServiceUnavailable)
					return
				}
				w.Header().Set(statusRemainingHeader, strconv.FormatInt(remaining.Milliseconds(), 10))
			}
			_, _ = w.Write(body)
		})
	}
	mux.HandleFunc("/websocket", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || (r.URL.RawQuery != "" && (!c.public || snapshotProfile(r.URL.RawQuery) == 0)) || c.public && r.URL.ForceQuery || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			http.Error(w, "unsupported request", http.StatusBadRequest)
			return
		}
		if !allowed(r) || c.public && len(r.Header.Values("Origin")) != 1 {
			http.Error(w, "host or origin not allowed", http.StatusForbidden)
			return
		}
		select {
		case peers <- struct{}{}:
			defer func() { <-peers }()
		default:
			http.Error(w, "viewer limit reached", http.StatusServiceUnavailable)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		sessionMu.Lock()
		if closing {
			sessionMu.Unlock()
			conn.Close()
			return
		}
		sessions.Add(1)
		sessionMu.Unlock()
		defer sessions.Done()
		streamProfile(ctx, conn, c, snapshotProfile(r.URL.RawQuery))
	})
	var handler http.Handler = mux
	if c.public {
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			if machineHandler != nil && (r.URL.Path == machineDetailPublicPath || strings.HasPrefix(r.URL.Path, machineDetailPublicPath+"/") || path.Clean(r.URL.Path) == machineDetailPublicPath) {
				machineHandler(w, r)
				return
			}
			if localPeerHandler != nil && (r.URL.Path == localPeerPublicPath || strings.HasPrefix(r.URL.Path, localPeerPublicPath+"/") || path.Clean(r.URL.Path) == localPeerPublicPath) {
				localPeerHandler(w, r)
				return
			}
			mux.ServeHTTP(w, r)
		})
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 4096}
	stopped := make(chan struct{})
	go func() { <-ctx.Done(); _ = server.Close(); close(stopped) }()
	err = server.Serve(netutil.LimitListener(inputLimitedListener{listener}, 32))
	sessionMu.Lock()
	closing = true
	sessionMu.Unlock()
	cancel()
	<-stopped
	<-collected
	// HTTP Close does not join hijacked handlers. Closing the admission gate
	// before waiting prevents a concurrent upgrade from adding a late session.
	sessions.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// proxyHeaders mark a request relayed by a proxy. A proxy's default upstream
// Host (for example nginx's $proxy_host) is the loopback listener itself, so
// the Host check alone would admit Origin-less remote clients.
var proxyHeaders = [...]string{"Forwarded", "Via", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Real-Ip"}

// Checking Host as well as Origin prevents a rebinding hostname from reaching
// a loopback listener. Forwarded headers are never trusted, and the private
// profile refuses proxied requests: only the public profile is for proxies.
func allowedRequest(r *http.Request, port string) bool {
	for _, name := range proxyHeaders {
		if _, ok := r.Header[name]; ok {
			return false
		}
	}
	host, p, err := net.SplitHostPort(r.Host)
	if err != nil || p != port {
		return false
	}
	ip := net.ParseIP(host)
	if !strings.EqualFold(host, "localhost") && (ip == nil || !ip.IsLoopback()) {
		return false
	}
	origins := r.Header.Values("Origin")
	if len(origins) == 0 {
		return true
	}
	if len(origins) != 1 {
		return false
	}
	origin, err := url.Parse(origins[0])
	return err == nil && (origin.Scheme == "http" || origin.Scheme == "https") && origin.User == nil && origin.Path == "" && origin.RawQuery == "" && origin.Fragment == "" && strings.EqualFold(origin.Host, r.Host)
}

var optionalPrivateFrames = [...]struct{ prefix, clear []byte }{
	{[]byte(`{"topic":"mithril","key":"snapshot_archive",`), []byte(`{"topic":"mithril","key":"snapshot_archive","value":null}`)},
	{[]byte(`{"topic":"mithril","key":"incremental_archive",`), []byte(`{"topic":"mithril","key":"incremental_archive","value":null}`)},
}

func hasPrivateFrame(messages [][]byte, prefix []byte) bool {
	for _, body := range messages {
		if bytes.HasPrefix(body, prefix) {
			return true
		}
	}
	return false
}

func stream(ctx context.Context, conn *websocket.Conn, c *collector) {
	streamProfile(ctx, conn, c, 0)
}

func watchStatusFrames(conn *websocket.Conn, profile int, active, writing *atomic.Pointer[frameSet], transition *sync.Mutex, wake, stop <-chan struct{}) {
	for {
		frames := [2]*frameSet{active.Load(), writing.Load()}
		var changes [2]<-chan struct{}
		var earliest time.Duration
		retry := false
		for i, frame := range frames {
			if frame == nil || frame.deadlineForProfile(profile).IsZero() {
				continue
			}
			remaining, changed, ok := frame.statusRemaining(profile, time.Now())
			if !ok {
				transition.Lock()
				if active.Load() == frame || writing.Load() == frame {
					if _, _, stillValid := frame.statusRemaining(profile, time.Now()); !stillValid {
						_ = conn.Close()
						transition.Unlock()
						return
					}
				}
				transition.Unlock()
				retry = true
				break
			}
			changes[i] = changed
			if earliest == 0 || remaining < earliest {
				earliest = remaining
			}
		}
		if retry {
			continue
		}
		var timer *time.Timer
		var timerC <-chan time.Time
		if earliest > 0 {
			timer = time.NewTimer(earliest)
			timerC = timer.C
		}
		select {
		case <-stop:
			if timer != nil {
				timer.Stop()
			}
			return
		case <-wake:
		case <-changes[0]:
		case <-changes[1]:
		case <-timerC:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

func streamProfile(ctx context.Context, conn *websocket.Conn, c *collector, profile int) {
	defer conn.Close()
	conn.SetReadLimit(1024)
	_ = conn.SetReadDeadline(time.Now().Add(time.Minute))
	var active atomic.Pointer[frameSet]
	active.Store(c.latest.Load())
	var writing atomic.Pointer[frameSet]
	var transition sync.Mutex
	watchWake := make(chan struct{}, 1)
	watchStop, watchDone := make(chan struct{}), make(chan struct{})
	wakeWatch := func() {
		select {
		case watchWake <- struct{}{}:
		default:
		}
	}
	setActive := func(frame *frameSet) {
		transition.Lock()
		active.Store(frame)
		transition.Unlock()
		wakeWatch()
	}
	setWriting := func(frame *frameSet) {
		transition.Lock()
		writing.Store(frame)
		transition.Unlock()
		wakeWatch()
	}
	go func() {
		defer close(watchDone)
		watchStatusFrames(conn, profile, &active, &writing, &transition, watchWake, watchStop)
	}()
	defer func() { close(watchStop); <-watchDone }()
	// Control frames bypass ReadMessage's data-message limit. Bound them too;
	// these handlers run only on the reader goroutine.
	controls := rate.NewLimiter(2, 4)
	conn.SetPongHandler(func(string) error {
		if !controls.Allow() {
			return errors.New("control frame rate exceeded")
		}
		return conn.SetReadDeadline(time.Now().Add(time.Minute))
	})
	conn.SetPingHandler(func(payload string) error {
		if !controls.Allow() {
			return errors.New("control frame rate exceeded")
		}
		now := time.Now()
		deadline := now.Add(time.Second)
		if f := active.Load(); f != nil {
			limit, ok := f.statusWriteDeadline(profile, now)
			if !ok {
				return errors.New("status expired")
			}
			if limit.Before(deadline) {
				deadline = limit
			}
		}
		return conn.WriteControl(websocket.PongMessage, []byte(payload), deadline)
	})
	finished := make(chan struct{})
	responses := make(chan []byte, 1)
	go func() {
		defer close(finished)
		limiter := rate.NewLimiter(2, 2)
		for {
			kind, b, err := conn.ReadMessage()
			if err != nil || kind != websocket.TextMessage || !limiter.Allow() {
				return
			}
			if c.public {
				return
			}
			var query struct {
				Topic string  `json:"topic"`
				Key   string  `json:"key"`
				ID    *uint64 `json:"id"`
			}
			decoder := json.NewDecoder(bytes.NewReader(b))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&query) != nil || decoder.Decode(new(any)) != io.EOF || query.Topic != "summary" || query.Key != "ping" || query.ID == nil {
				return
			}
			reply, _ := json.Marshal(message{Topic: "summary", Key: "ping", ID: query.ID, Value: nil})
			select {
			case responses <- reply:
			default:
				return
			}
		}
	}()
	defer func() { _ = conn.Close(); <-finished }()
	var previous *frameSet
	write := func(kind int, b []byte) bool {
		now := time.Now()
		deadline := now.Add(2 * time.Second)
		if previous != nil {
			var ok bool
			deadline, ok = previous.statusWriteDeadline(profile, now)
			if !ok {
				return false
			}
		}
		_ = conn.SetWriteDeadline(deadline)
		return conn.WriteMessage(kind, b) == nil
	}
	writeStatus := func(f *frameSet, b []byte) bool {
		deadline, ok := f.statusWriteDeadline(profile, time.Now())
		if !ok {
			return false
		}
		_ = conn.SetWriteDeadline(deadline)
		return conn.WriteMessage(websocket.TextMessage, b) == nil
	}
	var expiry *time.Timer
	var expiryC <-chan time.Time
	var leaseChanged <-chan struct{}
	var statusChanged <-chan struct{}
	defer func() {
		if expiry != nil {
			expiry.Stop()
		}
	}()
	setExpiry := func(f *frameSet) bool {
		if expiry != nil {
			expiry.Stop()
		}
		if f.deadlineForProfile(profile).IsZero() {
			expiryC = nil
			leaseChanged = nil
			return true
		}
		remaining, changed, ok := f.statusRemaining(profile, time.Now())
		if !ok {
			return false
		}
		leaseChanged = changed
		if expiry == nil {
			expiry = time.NewTimer(remaining)
		} else {
			expiry.Reset(remaining)
		}
		expiryC = expiry.C
		return true
	}
	send := func() bool {
		if update := c.statusUpdate.Load(); update != nil {
			statusChanged = update.changed
		}
		next := c.latest.Load()
		if next == nil || profile == 3 && len(next.safetyMessages) == 0 || profile == 4 && len(next.advancedMessages) == 0 {
			return false
		}
		if _, ok := next.remainingStatus(profile, time.Now()); !ok {
			return false
		}
		if previous == nil {
			setActive(next)
		}
		if next == previous {
			return setExpiry(next)
		}
		setWriting(next)
		defer setWriting(nil)
		// Clear only values this connection previously received. Initial frames
		// retain the legacy absent-topic shape; public output is whole-status.
		if previous != nil && !c.public {
			for _, topic := range optionalPrivateFrames {
				if hasPrivateFrame(previous.messages, topic.prefix) && !hasPrivateFrame(next.messages, topic.prefix) {
					if !writeStatus(next, topic.clear) {
						return false
					}
				}
			}
		}
		messages := next.messagesForProfile(profile)
		var oldMessages [][]byte
		if previous != nil {
			oldMessages = previous.messagesForProfile(profile)
		}
		for i, b := range messages {
			if i < len(oldMessages) && bytes.Equal(b, oldMessages[i]) {
				continue
			}
			if !writeStatus(next, b) {
				return false
			}
		}
		previous = next
		setActive(next)
		return setExpiry(next)
	}
	if !send() {
		return
	}
	tick := time.NewTicker(SampleInterval)
	defer tick.Stop()
	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case reply := <-responses:
			if !write(websocket.TextMessage, reply) {
				return
			}
		case <-expiryC:
			if !send() {
				return
			}
		case <-leaseChanged:
			if !send() {
				return
			}
		case <-statusChanged:
			if !send() {
				return
			}
		case <-tick.C:
			if !send() {
				return
			}
		case <-ping.C:
			if !write(websocket.PingMessage, nil) {
				return
			}
		}
	}
}
