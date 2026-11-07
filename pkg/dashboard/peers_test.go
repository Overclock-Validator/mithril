package dashboard

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func peerFixture(at time.Time) Snapshot {
	s := publicTestSnapshot(at)
	s.BlockSource.SampledAt = at
	s.BlockSource.Gossip = &Gossip{SampledAt: at, StartedAt: s.StartedAt}
	for i := range 8 {
		s.BlockSource.Gossip.RecentPeers = append(s.BlockSource.Gossip.RecentPeers, Peer{
			Identity: strings.Repeat("z", 43) + string(rune('A'+i)), Version: "65535.65535.65535", Client: 65535, ShredVersion: 65535,
			ReceivedAt: at.Add(-time.Duration(i) * time.Second), Repair: true, TVU: true, Alpenglow: true,
		})
	}
	return s
}

func TestPublicPeersDefaultAbsenceAndDetachedProjection(t *testing.T) {
	at := time.Now().UTC()
	s := peerFixture(at)
	public, raw := publicValue(t, &s, "fresh", at)
	if public.Peers != nil || strings.Contains(string(raw), s.BlockSource.Gossip.RecentPeers[0].Identity) {
		t.Fatal("default public profile disclosed peer identities")
	}
	p := publicPeers(&s, "fresh", at)
	if p.State != "fresh" || len(p.Contacts) != 8 || p.Contacts[0].Version != "65535.65535.65535" || p.Contacts[0].Client != 65535 || !p.Contacts[0].Alpenglow {
		t.Fatalf("missing bounded peer metadata: %+v", p)
	}
	original := p.Contacts[0]
	s.BlockSource.Gossip.RecentPeers[0].Identity = "changed"
	s.BlockSource.Gossip.SampledAt = at.Add(time.Second)
	if p.Contacts[0] != original || !p.SampledAt.Equal(at) {
		t.Fatal("projection aliases source")
	}
}

func TestPublicPeersUnavailableExpiryAndEmpty(t *testing.T) {
	at := time.Now().UTC()
	for _, change := range []func(*Snapshot){
		func(s *Snapshot) { s.BlockSource = nil },
		func(s *Snapshot) { s.BlockSource.LiveConnected = false },
		func(s *Snapshot) { s.BlockSource.Gossip = nil },
		func(s *Snapshot) { s.BlockSource.SampledAt = at.Add(2 * time.Second) },
		func(s *Snapshot) { s.BlockSource.Gossip.SampledAt = at.Add(2 * time.Second) },
		func(s *Snapshot) { s.BlockSource.Gossip.StartedAt = s.StartedAt.Add(-time.Second) },
		func(s *Snapshot) {
			s.BlockSource.Gossip.RecentPeers = append(s.BlockSource.Gossip.RecentPeers, s.BlockSource.Gossip.RecentPeers[0])
		},
		func(s *Snapshot) { s.BlockSource.Gossip.RecentPeers[1] = s.BlockSource.Gossip.RecentPeers[0] },
		func(s *Snapshot) { s.BlockSource.Gossip.RecentPeers[0].ReceivedAt = at.Add(time.Nanosecond) },
		func(s *Snapshot) {
			s.BlockSource.Gossip.RecentPeers[0].ReceivedAt = at.Add(-15*time.Minute - time.Nanosecond)
		},
		func(s *Snapshot) { s.BlockSource.Gossip.RecentPeers[0].Identity = "http://private-host/secret" },
		func(s *Snapshot) { s.BlockSource.Gossip.RecentPeers[0].Version = "private-tag" },
		func(s *Snapshot) { s.BlockSource.Gossip.RecentPeers[0].Version = "01.0.0" },
		func(s *Snapshot) { s.BlockSource.Gossip.RecentPeers[0].Version = "65536.0.0" },
	} {
		s := peerFixture(at)
		change(&s)
		if p := publicPeers(&s, "fresh", at); !reflect.DeepEqual(p, &PublicPeers{State: "unavailable"}) {
			t.Fatalf("invalid peer input published: %+v", p)
		}
	}
	for _, s := range []*Snapshot{nil, new(Snapshot)} {
		if p := publicPeers(s, "fresh", at); p.State != "unavailable" {
			t.Fatal("missing source fabricated contacts")
		}
	}
	s := peerFixture(at)
	if p := publicPeers(&s, "unavailable", at); p.State != "unavailable" || p.Contacts != nil {
		t.Fatal("unavailable source disclosed contacts")
	}
	if p := publicPeers(&s, "stale", at); p.State != "stale" || len(p.Contacts) != 8 {
		t.Fatal("lost stale/unknown distinction")
	}
	if p := publicPeers(&s, "fresh", at.Add(31*time.Second)); p.State != "stale" {
		t.Fatal("frozen producer appeared fresh")
	}
	s.BlockSource.Gossip.RecentPeers = nil
	if p := publicPeers(&s, "fresh", at); p.State != "fresh" || p.Contacts == nil || len(p.Contacts) != 0 {
		t.Fatal("known empty sample confused with unavailable")
	}
}

func TestPublicPeersCollectorOptIn(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public, c.publicOptions.Peers = true, true
	defer c.client.CloseIdleConnections()
	at := time.Now().UTC()
	s := peerFixture(at)
	b, err := json.Marshal(s)
	if err != nil || len(b) > MaxSnapshotBytes || s.validate() != nil {
		t.Fatalf("invalid collector input: %d bytes, marshal %v, validation %v", len(b), err, s.validate())
	}
	setFixture(t, body, s)
	c.poll(context.Background(), at)
	read := func() PublicSnapshot {
		t.Helper()
		set := c.latest.Load()
		var p PublicSnapshot
		if set == nil || json.Unmarshal(set.publicJSON, &p) != nil {
			t.Fatal("missing peer frame")
		}
		var frame struct{ Value json.RawMessage }
		if err := json.Unmarshal(set.messages[0], &frame); err != nil || string(frame.Value) != string(set.publicJSON) {
			t.Fatal("HTTP/WebSocket peer values differ")
		}
		return p
	}
	if p := read(); p.Peers == nil || p.Peers.State != "fresh" || len(p.Peers.Contacts) != 8 {
		t.Fatalf("opt-in peers unavailable: %+v", p.Peers)
	}
	body.Store(nil)
	c.poll(context.Background(), at.Add(time.Second))
	if p := read(); p.Peers.State != "stale" {
		t.Fatal("source loss left fresh peer sample")
	}
	c.last = nil
	c.poll(context.Background(), at.Add(2*time.Second))
	if p := read(); p.Peers.State != "unavailable" || p.Peers.Contacts != nil {
		t.Fatal("missing source retained contacts")
	}
}
