package gossip

import (
	"net"
	"sort"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

// MonitoringSnapshot is one cached aggregate observation and bounded contact sample.
// StartedAt identifies the client instance's counter lifetime, including identity
// swaps. SampledAt retains the original monotonic clock; DTOs convert it to UTC.
// Stats counters are cumulative; peer counts are known contacts, not a
// verified live cluster population. Its fields are not sampled atomically.
type MonitoringSnapshot struct {
	SampledAt            time.Time
	StartedAt            time.Time
	Stats                Stats
	Peers                [8]PeerObservation
	PeerCount            int
	SocketObserved       bool
	SocketGeneration     uint64
	SocketMemory         metrics.SocketObservation
	SocketDrops          metrics.SocketDropDelta
	LocalShredVersion    uint16
	HasLocalShredVersion bool
	PayloadObserved      bool
	Payload              UDPPayloadObservation
	DecodeObserved       bool
	Decode               DecodeMonitoringObservation
}

// PeerObservation records an accepted signed advert, not peer health. Versions
// and service flags are self-reported. Network endpoints are never copied.
type PeerObservation struct {
	Pubkey                 Pubkey
	Version                Version
	ShredVersion           uint16
	ReceivedAt             time.Time
	Repair, TVU, Alpenglow bool
}

type monitoredPeer struct {
	PeerObservation
	outset, wallclock uint64
}

// observeMonitoringPeer is called only after the packet's signature and the
// contact's shred version have passed the existing checks. It does not route.
func (c *Client) observeMonitoringPeer(r contactRecord, now time.Time) {
	if c == nil || !c.monitoringEnabled.Load() || r.Pubkey == (Pubkey{}) || now.Before(c.startedAt) {
		return
	}
	c.monitoringMu.Lock()
	defer c.monitoringMu.Unlock()
	index := 0
	for i, p := range c.monitoringPeers {
		if p.Pubkey == r.Pubkey && !p.ReceivedAt.IsZero() {
			if r.Outset < p.outset || r.Outset == p.outset && r.Wallclock <= p.wallclock {
				return // Repeated or older adverts must not refresh their age.
			}
			index = i
			break
		}
		if p.ReceivedAt.Before(c.monitoringPeers[index].ReceivedAt) {
			index = i
		}
	}
	c.monitoringPeers[index] = monitoredPeer{PeerObservation: PeerObservation{
		Pubkey: r.Pubkey, Version: r.Version, ShredVersion: r.ShredVer, ReceivedAt: now.UTC(),
		Repair:    r.ServeRepairAddr.ok && r.ServeRepairAddr.port != 0,
		TVU:       r.TVUAddr.ok && r.TVUAddr.port != 0,
		Alpenglow: r.Sockets[socketTagAlpenglow].ok && r.Sockets[socketTagAlpenglow].port != 0,
	}, outset: r.Outset, wallclock: r.Wallclock}
}

// EnableMonitoring opts into the existing ten-second diagnostics publication.
// It is idempotent and safe while Run is active; it does not sample or reset.
func (c *Client) EnableMonitoring() {
	if c != nil {
		c.monitoringEnabled.Store(true)
	}
}

// EnableSocketMemoryMonitoring opts into low-rate owner socket observations.
func (c *Client) EnableSocketMemoryMonitoring() {
	if c != nil {
		c.socketMemoryEnabled.Store(true)
	}
}

func (c *Client) nextSocketGeneration() uint64 {
	for {
		before := c.socketGeneration.Load()
		if before == ^uint64(0) {
			return 0
		}
		if c.socketGeneration.CompareAndSwap(before, before+1) {
			return before + 1
		}
	}
}

// MonitoringSnapshot returns a value copy without locks, I/O or peer-map access.
// It is unavailable before the first enabled diagnostic cycle and after Run
// returns. Readers must expire old samples if the owner loop stops advancing.
func (c *Client) MonitoringSnapshot() (MonitoringSnapshot, bool) {
	if c != nil {
		if s := c.monitoring.Load(); s != nil {
			return *s, true
		}
	}
	return MonitoringSnapshot{}, false
}

// publishMonitoring reuses the owner loop's existing Stats read.
func (c *Client) publishMonitoring(now time.Time, stats Stats) {
	c.publishMonitoringWithSocket(now, stats, nil, 0)
}

func (c *Client) publishMonitoringWithSocket(now time.Time, stats Stats, conn *net.UDPConn, generation uint64) {
	if c == nil || !c.monitoringEnabled.Load() {
		return
	}
	if session := c.peerDataset.Load(); session != nil && session.conn == conn && session.dataset.generation == generation {
		session.dataset.publish(now)
	}
	s := &MonitoringSnapshot{SampledAt: now, StartedAt: c.startedAt.UTC(), Stats: stats}
	// A configured fallback before contact initialization is not an applied
	// local Gossip contact/filter observation.
	c.contactMu.RLock()
	if c.contact != nil {
		s.LocalShredVersion, s.HasLocalShredVersion = c.contact.ShredVer, true
	}
	c.contactMu.RUnlock()
	if c.socketMemoryEnabled.Load() && conn != nil && generation != 0 {
		s.SocketObserved, s.SocketGeneration = true, generation
		s.SocketMemory = metrics.ObserveUDPSocketMemory(conn)
		if before := c.monitoring.Load(); before != nil && before.SocketObserved && before.SocketGeneration == generation {
			s.SocketDrops = metrics.ObservedSocketDrops(before.SocketMemory, s.SocketMemory)
		}
	}
	if payload := c.udpPayload.Load(); payload != nil && payload.conn == conn && payload.generation == generation {
		s.PayloadObserved, s.Payload = true, payload.snapshot()
		if before := c.monitoring.Load(); before != nil && before.PayloadObserved {
			s.Payload.Window = observedUDPPayloadWindow(before.Payload, s.Payload)
		}
	}
	if recorder := c.decodeMonitoring.Load(); recorder != nil && recorder.conn == conn && recorder.generation == generation {
		s.DecodeObserved, s.Decode = true, recorder.snapshot(now)
	}
	c.monitoringMu.Lock()
	for _, p := range c.monitoringPeers {
		if !p.ReceivedAt.IsZero() && !p.ReceivedAt.After(now) && now.Sub(p.ReceivedAt) <= peerExpirationWindow {
			s.Peers[s.PeerCount] = p.PeerObservation
			s.PeerCount++
		}
	}
	c.monitoringMu.Unlock()
	sort.Slice(s.Peers[:s.PeerCount], func(i, j int) bool { return s.Peers[i].ReceivedAt.After(s.Peers[j].ReceivedAt) })
	c.monitoring.Store(s)
}
