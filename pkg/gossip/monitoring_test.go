package gossip

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"sync"
	"testing"
	"time"
)

func TestMonitoringReusesImmutableAggregateSample(t *testing.T) {
	var absent *Client
	absent.EnableMonitoring()
	if _, ok := absent.MonitoringSnapshot(); ok {
		t.Fatal("nil client reported a sample")
	}
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.FixedZone("offset", 19800))
	c := &Client{startedAt: now.Add(-time.Minute)}
	stats := Stats{RxPackets: 1<<53 + 1, TxPackets: 2, RxDecodeErrors: 3, TxErrors: 4,
		RxPings: 5, RxPongs: 6, RxContacts: 7, AcceptedContacts: 8, RxPullRequests: 9,
		TxPushMessages: 10, TxPingMessages: 11, TxPongMessages: 12, TxPullResponses: 13,
		LastRxUnix: now.Unix(), LastTxUnix: now.Unix() - 1, Peers: 14, RepairPeers: 15, AlpenglowPeers: 16}
	if allocations := testing.AllocsPerRun(100, func() { c.publishMonitoring(now, stats) }); allocations != 0 {
		t.Fatalf("disabled monitoring allocated: %v", allocations)
	}
	if _, ok := c.MonitoringSnapshot(); ok {
		t.Fatal("disabled monitoring published")
	}
	c.EnableMonitoring()
	c.EnableMonitoring()
	if _, ok := c.MonitoringSnapshot(); ok {
		t.Fatal("enabling fabricated a diagnostic sample")
	}
	c.publishMonitoring(now, stats)
	s, ok := c.MonitoringSnapshot()
	want := MonitoringSnapshot{SampledAt: now, StartedAt: c.startedAt.UTC(), Stats: stats}
	if !ok || s != want {
		t.Fatalf("sample = %+v, %t; want %+v", s, ok, want)
	}
	s.Stats.RxPackets = 0
	stats.RxPackets = 0
	// Cached readers must not acquire the live peer-map locks or drain counters.
	c.peerMu.Lock()
	c.repairPeerMu.Lock()
	c.alpenglowPeerMu.Lock()
	defer c.peerMu.Unlock()
	defer c.repairPeerMu.Unlock()
	defer c.alpenglowPeerMu.Unlock()
	var next MonitoringSnapshot
	if allocations := testing.AllocsPerRun(100, func() { next, _ = c.MonitoringSnapshot() }); allocations != 0 {
		t.Fatalf("cached read allocated: %v", allocations)
	}
	if next != want {
		t.Fatalf("read mutated or consumed sample: got %+v, want %+v", next, want)
	}
	c.publishMonitoring(now.Add(time.Second), Stats{})
	if last, _ := c.MonitoringSnapshot(); last.Stats != (Stats{}) || !last.StartedAt.Equal(want.StartedAt) {
		t.Fatalf("publication did not replace aggregates within instance: %+v", last)
	}
}

func TestMonitoringLocalShredVersionUsesInitializedContact(t *testing.T) {
	now := time.Now()
	c := &Client{startedAt: now, cfg: Config{ShredVersion: 17}}
	c.EnableMonitoring()
	c.publishMonitoring(now, Stats{})
	if s, _ := c.MonitoringSnapshot(); s.HasLocalShredVersion {
		t.Fatal("configured fallback became an applied local contact version")
	}
	for _, version := range []uint16{0, ^uint16(0)} {
		c.contactMu.Lock()
		c.contact = &ContactInfo{ShredVer: version}
		c.contactMu.Unlock()
		c.publishMonitoring(now, Stats{})
		s, known := c.MonitoringSnapshot()
		if !known || !s.HasLocalShredVersion || s.LocalShredVersion != version || c.ShredVersion() != version {
			t.Fatal("cached version differs from the initialized local contact/filter")
		}
	}
}

func TestMonitoringClientInstanceAndRunExit(t *testing.T) {
	c, err := NewClient(Config{Entrypoint: "127.0.0.1:8000", TVUAddr: "127.0.0.1:8001"})
	if err != nil {
		t.Fatal(err)
	}
	if c.startedAt.IsZero() || c.startedAt.Location() != time.UTC {
		t.Fatalf("client lacks UTC counter lifetime: %v", c.startedAt)
	}
	c.EnableMonitoring()
	c.publishMonitoring(time.Now(), Stats{RxPackets: 42})
	replacement := &Client{startedAt: c.startedAt.Add(time.Second)}
	replacement.EnableMonitoring()
	if _, ok := replacement.MonitoringSnapshot(); ok {
		t.Fatal("new client inherited the old sample")
	}
	replacement.publishMonitoring(time.Now(), Stats{})
	if s, _ := replacement.MonitoringSnapshot(); s.StartedAt == c.startedAt || s.Stats != (Stats{}) {
		t.Fatalf("replacement retained the old counter lifetime: %+v", s)
	}
	// Invalid port avoids a listening service and still exercises Run cleanup.
	c.bindAddr = &net.UDPAddr{Port: -1}
	if err := c.Run(context.Background()); err == nil {
		t.Fatal("invalid bind unexpectedly succeeded")
	}
	if _, ok := c.MonitoringSnapshot(); ok {
		t.Fatal("Run exit retained an available sample")
	}
}

func TestMonitoringPreservesOriginalMonotonicSampleClock(t *testing.T) {
	now := time.Now()
	c := &Client{startedAt: now.Add(-time.Second).UTC()}
	c.EnableMonitoring()
	c.publishMonitoring(now, Stats{})
	s, ok := c.MonitoringSnapshot()
	if !ok || s.SampledAt != now || s.SampledAt == now.UTC() {
		t.Fatal("cached sample lost its original monotonic clock")
	}
}

func TestMonitoringConcurrentReadersAndPublisher(t *testing.T) {
	c := &Client{startedAt: time.Now().UTC()}
	c.EnableMonitoring()
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for range 1000 {
				c.EnableMonitoring()
				if s, ok := c.MonitoringSnapshot(); ok && s.Stats.TxPackets != s.Stats.RxPackets*2 {
					t.Errorf("torn aggregate snapshot: %+v", s)
					return
				}
			}
		})
	}
	for i := range uint64(1000) {
		c.observeMonitoringPeer(contactRecord{Pubkey: Pubkey{byte(i%16 + 1)}, Wallclock: i}, time.Now())
		c.publishMonitoring(time.Now(), Stats{RxPackets: i, TxPackets: i * 2})
	}
	readers.Wait()
}

func TestMonitoringPeerBoundsAgeAndOrdering(t *testing.T) {
	at := time.Now().UTC()
	c := &Client{startedAt: at.Add(-time.Minute)}
	r := contactRecord{Pubkey: Pubkey{1}, Outset: 10, Wallclock: 20, Version: Version{Major: 3, Minor: 2, Patch: 1, Client: 15}, ShredVer: 1234,
		TVUAddr: contactEndpoint{ok: true, port: 8001}, Sockets: map[uint8]contactEndpoint{socketTagAlpenglow: {ok: true, port: 8002}}}
	if n := testing.AllocsPerRun(100, func() { c.observeMonitoringPeer(r, at) }); n != 0 {
		t.Fatalf("disabled peer monitoring allocated: %v", n)
	}
	c.EnableMonitoring()
	c.publishMonitoring(at, Stats{})
	if s, _ := c.MonitoringSnapshot(); s.PeerCount != 0 {
		t.Fatal("disabled observation was retained")
	}
	c.observeMonitoringPeer(r, at)
	c.publishMonitoring(at, Stats{})
	original, _ := c.MonitoringSnapshot()
	if original.PeerCount != 1 || original.Peers[0].Version != r.Version || !original.Peers[0].TVU || !original.Peers[0].Alpenglow || original.Peers[0].Repair {
		t.Fatalf("wrong advertised peer metadata: %+v", original)
	}
	for _, older := range []contactRecord{r, {Pubkey: r.Pubkey, Outset: 9, Wallclock: 99}, {Pubkey: r.Pubkey, Outset: 10, Wallclock: 19}} {
		c.observeMonitoringPeer(older, at.Add(time.Second))
	}
	c.publishMonitoring(at.Add(time.Second), Stats{})
	if s, _ := c.MonitoringSnapshot(); s.Peers[0] != original.Peers[0] {
		t.Fatal("old/repeated advert refreshed peer metadata or age")
	}
	r.Outset, r.Wallclock, r.Version.Patch = 11, 1, 2
	c.observeMonitoringPeer(r, at.Add(2*time.Second))
	for i := byte(2); i <= 10; i++ {
		r.Pubkey = Pubkey{i}
		c.observeMonitoringPeer(r, at.Add(time.Duration(i+2)*time.Second))
	}
	c.publishMonitoring(at.Add(12*time.Second), Stats{})
	s, _ := c.MonitoringSnapshot()
	if s.PeerCount != 8 || s.Peers[0].Pubkey != (Pubkey{10}) || s.Peers[7].Pubkey != (Pubkey{3}) || original.Peers[0].Version.Patch != 1 {
		t.Fatalf("peer capacity/order or detached sample failed: %+v", s)
	}
	c.publishMonitoring(at.Add(12*time.Second+peerExpirationWindow+time.Nanosecond), Stats{})
	if s, _ := c.MonitoringSnapshot(); s.PeerCount != 0 {
		t.Fatal("expired adverts survived the contact retention window")
	}
}

func TestMonitoringPeersRequireVerifiedMatchingContact(t *testing.T) {
	c, err := NewClient(Config{Entrypoint: "127.0.0.1:8000", TVUAddr: "127.0.0.1:8001", ShredVersion: 4321})
	if err != nil {
		t.Fatal(err)
	}
	c.EnableMonitoring()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	info, err := NewContactInfo(Pubkey(pub), 4321, &net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 8000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	info.Version = Version{Major: 3, Minor: 1, Patch: 2, Client: 15}
	record := contactRecordFromInfo(t, info, priv)
	if record.Outset != info.Outset || record.Version != info.Version || !record.Verify() {
		t.Fatal("wire metadata changed signed record")
	}
	for _, bad := range []bool{true, false} {
		value, err := signCrdsContactInfo(info, priv)
		if err != nil {
			t.Fatal(err)
		}
		if bad {
			value.Signature[0] ^= 1
		}
		packet, err := encodePushMessage(Pubkey(pub), []CrdsValue{value})
		if err != nil {
			t.Fatal(err)
		}
		_, err = decodePacketWithContactHandler(packet, func(r contactRecord) { c.handleContactRecord(r, 4321) })
		if err != nil {
			t.Fatal(err)
		}
		c.publishMonitoring(time.Now(), Stats{})
		s, _ := c.MonitoringSnapshot()
		if bad && s.PeerCount != 0 || !bad && (s.PeerCount != 1 || s.Peers[0].Version != info.Version) {
			t.Fatalf("signature gate failed: %+v", s)
		}
	}
	record.ShredVer++
	record.Pubkey = Pubkey{3}
	c.handleContactRecord(record, 4321)
	c.publishMonitoring(time.Now(), Stats{})
	if s, _ := c.MonitoringSnapshot(); s.PeerCount != 1 {
		t.Fatal("wrong-shred peer included")
	}
}
