package gossip

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
)

func relayFixture(t testing.TB) (*ContactInfo, ed25519.PrivateKey) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	info, err := NewContactInfo(Pubkey(pub), 4321,
		&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9000},
		&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9001})
	if err != nil {
		t.Fatal(err)
	}
	if err := info.SetSocket(socketTagServeRepair, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9002}); err != nil {
		t.Fatal(err)
	}
	if err := info.SetAlpenglowAddr(&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9003}); err != nil {
		t.Fatal(err)
	}
	if err := info.SetTPUQUIC(&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9004}); err != nil {
		t.Fatal(err)
	}
	return info, key
}

func TestContactRelayPreservesSignedBytesAndOrdersUpdates(t *testing.T) {
	info, key := relayFixture(t)
	now := info.Wallclock
	var relay contactRelay
	record := contactRecordFromInfo(t, info, key)
	original := bytes.Clone(record.data)
	if !relay.accept(record, now) {
		t.Fatal("first contact rejected")
	}
	clear(record.data)
	values := relay.values(now)
	if len(values) != 1 || !bytes.Equal(values[0].Data, original) {
		t.Fatal("relay did not retain the original receive bytes")
	}
	packet, err := encodePushMessage(Pubkey{}, values)
	if err != nil {
		t.Fatal(err)
	}
	var forwarded contactRecord
	decoded, err := decodePacketWithContactHandler(packet, func(record contactRecord) { forwarded = record })
	if err != nil || decoded.ContactCount != 1 {
		t.Fatalf("relayed signature/decoding: %v, %+v", err, decoded)
	}
	if endpointString(forwarded.ServeRepairAddr) != "127.0.0.1:9002" || endpointString(forwarded.Sockets[socketTagAlpenglow]) != "127.0.0.1:9003" || endpointString(forwarded.Sockets[socketTagTPUQUIC]) != "127.0.0.1:9004" {
		t.Fatalf("lost relayed service endpoints: %+v", forwarded)
	}
	if relay.accept(contactRecordFromInfo(t, info, key), now) {
		t.Fatal("duplicate accepted as update")
	}
	if relay.accept(contactRecordFromInfo(t, info.CloneWithWallclock(now-1), key), now) {
		t.Fatal("older wallclock accepted")
	}
	newer := info.CloneWithWallclock(now + 1)
	if !relay.accept(contactRecordFromInfo(t, newer, key), now) {
		t.Fatal("newer wallclock rejected")
	}
	restarted := info.CloneWithWallclock(now - 1)
	restarted.Outset++
	if !relay.accept(contactRecordFromInfo(t, restarted, key), now) {
		t.Fatal("newer process outset rejected")
	}
	if relay.accept(contactRecordFromInfo(t, newer.CloneWithWallclock(now+2), key), now) {
		t.Fatal("old process overwrote restarted process")
	}
	if got := relay.values(now + contactPushWindow + 1); len(got) != 0 {
		t.Fatal("expired push value relayed")
	}
	if got := relay.values(now + uint64(peerExpirationWindow/time.Millisecond) + 1); len(got) != 0 || len(relay.contacts) != 0 {
		t.Fatal("expired cache entry retained")
	}
}

func TestContactRelayDeterministicTies(t *testing.T) {
	info, key := relayFixture(t)
	a := contactRecordFromInfo(t, info, key)
	if err := info.SetSocket(socketTagTVU, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9101}); err != nil {
		t.Fatal(err)
	}
	b := contactRecordFromInfo(t, info, key)
	var left, right contactRelay
	left.accept(a, info.Wallclock)
	left.accept(b, info.Wallclock)
	right.accept(b, info.Wallclock)
	right.accept(a, info.Wallclock)
	if !bytes.Equal(left.values(info.Wallclock)[0].Data, right.values(info.Wallclock)[0].Data) {
		t.Fatal("equal timestamps did not converge")
	}
}

func TestContactRelayRejectsInvalidContacts(t *testing.T) {
	for _, name := range []string{"signature", "wrong cluster", "expired", "future", "zero port", "multicast", "unspecified"} {
		t.Run(name, func(t *testing.T) {
			client, err := NewClient(Config{Entrypoint: "127.0.0.1:8000", ShredVersion: 4321})
			if err != nil {
				t.Fatal(err)
			}
			info, key := relayFixture(t)
			switch name {
			case "wrong cluster":
				info.ShredVer++
			case "expired":
				info.Wallclock -= uint64(peerExpirationWindow/time.Millisecond) + 1000
			case "future":
				info.Wallclock += contactPushWindow + 1000
			case "zero port":
				info.Sockets[0].Port = 0
			case "multicast":
				info.Addrs[0] = net.ParseIP("224.0.0.1")
			case "unspecified":
				info.Addrs[0] = net.IPv4zero
			}
			value, err := signCrdsContactInfo(info, key)
			if err != nil {
				t.Fatal(err)
			}
			if name == "signature" {
				value.Signature[0] ^= 1
			}
			packet, err := encodePushMessage(info.Pubkey, []CrdsValue{value})
			if err != nil {
				t.Fatal(err)
			}
			if err := client.handlePacket(nil, packet, &net.UDPAddr{}); err != nil {
				t.Fatal(err)
			}
			if len(client.relay.contacts) != 0 || len(client.TVUPeers()) != 0 || client.Stats().AcceptedContacts != 0 {
				t.Fatal("invalid contact entered relay or routing tables")
			}
		})
	}
}

func TestContactRelayPacketAndStorageBounds(t *testing.T) {
	var relay contactRelay
	now := wallclockMillis()
	for i := 0; i < maxKnownGossipPeers+10; i++ {
		info, key := relayFixture(t)
		if !relay.accept(contactRecordFromInfo(t, info, key), now) {
			t.Fatal("valid contact rejected")
		}
		if len(relay.contacts) > maxKnownGossipPeers {
			t.Fatal("relay exceeded capacity")
		}
	}
	seen := make(map[Pubkey]bool)
	for i := 0; i < maxKnownGossipPeers; i++ {
		packet, err := encodePushMessage(Pubkey{}, relay.values(now))
		if err != nil {
			t.Fatal(err)
		}
		if len(packet) > packetDataSize {
			t.Fatal("relay packet exceeded wire limit")
		}
		decoded, err := decodePacket(packet)
		if err != nil {
			t.Fatal(err)
		}
		for _, contact := range decoded.Contacts {
			seen[contact.Pubkey] = true
		}
		if len(seen) == maxKnownGossipPeers {
			break
		}
	}
	if len(seen) != maxKnownGossipPeers {
		t.Fatalf("rotation reached %d of %d contacts", len(seen), maxKnownGossipPeers)
	}
}

func TestContactUpdateRemovesWithdrawnEndpoints(t *testing.T) {
	client, err := NewClient(Config{Entrypoint: "127.0.0.1:8000", ShredVersion: 4321})
	if err != nil {
		t.Fatal(err)
	}
	info, key := relayFixture(t)
	client.handleContactRecord(contactRecordFromInfo(t, info, key), 4321)
	if len(client.RepairPeers()) != 1 || len(client.AlpenglowPeers()) != 1 {
		t.Fatal("initial endpoints missing")
	}
	updated, err := NewContactInfo(info.Pubkey, 4321, info.GossipAddr, nil)
	if err != nil {
		t.Fatal(err)
	}
	updated.Outset = info.Outset + 1
	client.handleContactRecord(contactRecordFromInfo(t, updated, key), 4321)
	if len(client.RepairPeers()) != 0 || len(client.AlpenglowPeers()) != 0 || len(client.TVUPeers()) != 0 {
		t.Fatal("withdrawn endpoints retained")
	}
	client.handleContactRecord(contactRecordFromInfo(t, info, key), 4321)
	if len(client.RepairPeers()) != 0 {
		t.Fatal("old relayed contact restored withdrawn endpoint")
	}
}

func waitForGossip(t *testing.T, label string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out: " + label)
}

func runRelayClient(t *testing.T, seed string, key ed25519.PrivateKey) (*Client, func()) {
	t.Helper()
	socket, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	addr := socket.LocalAddr().String()
	if err := socket.Close(); err != nil {
		t.Fatal(err)
	}
	if seed == "" {
		seed = addr
	}
	client, err := NewClient(Config{Entrypoint: seed, BindAddr: addr, TVUAddr: addr, AlpenglowAddr: addr,
		AdvertisedIP: "127.0.0.1", ShredVersion: 4321, Identity: key, PushInterval: 50 * time.Millisecond, PingInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("gossip Run: %v", err)
				}
			case <-time.After(time.Second):
				t.Error("gossip did not stop")
			}
		})
	}
	t.Cleanup(stop)
	waitForGossip(t, "client startup", func() bool { return client.AdvertisedGossipAddr() != nil })
	return client, stop
}

func TestGossipDiscoverySurvivesSeedExitAndPeerRestart(t *testing.T) {
	seed, stopSeed := runRelayClient(t, "", nil)
	left, stopLeft := runRelayClient(t, seed.AdvertisedGossipAddr().String(), nil)
	right, _ := runRelayClient(t, seed.AdvertisedGossipAddr().String(), nil)
	leftKey, rightKey := solana.PublicKey(left.Pubkey()), solana.PublicKey(right.Pubkey())
	waitForGossip(t, "joiners discover each other through seed", func() bool {
		_, a := left.LookupAlpenglow(rightKey)
		_, b := right.LookupAlpenglow(leftKey)
		return a && b
	})
	stopSeed()
	time.Sleep(100 * time.Millisecond)
	lrx, rrx := left.Stats().RxPackets, right.Stats().RxPackets
	waitForGossip(t, "survivors exchange new packets", func() bool { return left.Stats().RxPackets > lrx && right.Stats().RxPackets > rrx })
	identity := left.Identity()
	stopLeft()
	restarted, _ := runRelayClient(t, right.AdvertisedGossipAddr().String(), identity)
	waitForGossip(t, "restart replaces old endpoint", func() bool {
		addr, ok := right.LookupAlpenglow(leftKey)
		return ok && addr.String() == restarted.AdvertisedGossipAddr().String()
	})
}

func TestGossipPushContinuesAfterUnreachablePeer(t *testing.T) {
	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	receiver, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	client, err := NewClient(Config{Entrypoint: receiver.LocalAddr().String(), TVUAddr: sender.LocalAddr().String(), AdvertisedIP: "127.0.0.1", ShredVersion: 4321})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.initializeContact(sender.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	client.recordPeer(receiver.LocalAddr().(*net.UDPAddr))
	// An IPv4-only socket cannot send to this IPv6 destination.
	client.recordPeer(&net.UDPAddr{IP: net.ParseIP("::1"), Port: 9000})
	if err := client.pushContact(sender); err != nil {
		t.Fatal(err)
	}
	if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var packet [packetDataSize]byte
	if _, _, err := receiver.ReadFromUDP(packet[:]); err != nil {
		t.Fatal(err)
	}
	if client.Stats().TxErrors != 1 || client.Stats().TxPushMessages != 1 {
		t.Fatalf("unexpected send counters: %+v", client.Stats())
	}
}

func FuzzContactRelayPackets(f *testing.F) {
	info, key := relayFixture(f)
	_, localKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		f.Fatal(err)
	}
	value, err := signCrdsContactInfo(info, key)
	if err != nil {
		f.Fatal(err)
	}
	packet, err := encodePushMessage(info.Pubkey, []CrdsValue{value})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(packet)
	f.Add([]byte{0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, packet []byte) {
		if len(packet) > packetDataSize {
			return
		}
		client, err := NewClient(Config{Entrypoint: "127.0.0.1:8000", Identity: localKey, ShredVersion: 4321})
		if err != nil {
			t.Fatal(err)
		}
		// Decode contact messages only; this check must never send a packet.
		_, _ = decodePacketWithContactHandler(packet, func(record contactRecord) { client.handleContactRecord(record, 4321) })
		values := client.relay.values(info.Wallclock)
		encoded, err := encodePushMessage(client.Pubkey(), values)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodePacket(encoded)
		if err != nil || decoded.ContactCount != len(values) {
			t.Fatalf("relay produced invalid signed values: %v", err)
		}
	})
}
