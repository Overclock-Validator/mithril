package gossip

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
)

func TestVarintAndShortLenEncoding(t *testing.T) {
	var e encoder
	if err := e.shortLen(0xffff); err != nil {
		t.Fatalf("shortLen returned error: %v", err)
	}
	if got, want := e.bytes(), []byte{0xff, 0xff, 0x03}; string(got) != string(want) {
		t.Fatalf("shortLen bytes = %x, want %x", got, want)
	}

	d := newDecoder(e.bytes())
	got, err := d.shortLen()
	if err != nil {
		t.Fatalf("shortLen decode returned error: %v", err)
	}
	if got != 0xffff {
		t.Fatalf("shortLen decode = %d, want %d", got, 0xffff)
	}
}

func TestContactInfoRoundTripAndSignature(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey returned error: %v", err)
	}
	pubkey, err := pubkeyFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("pubkeyFromPrivateKey returned error: %v", err)
	}
	contact, err := NewContactInfo(
		pubkey,
		1234,
		&net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 65400},
		&net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 8001},
	)
	if err != nil {
		t.Fatalf("NewContactInfo returned error: %v", err)
	}
	if err := contact.SetSocket(socketTagServeRepair, &net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 8008}); err != nil {
		t.Fatalf("SetSocket serve repair returned error: %v", err)
	}
	if err := contact.SetAlpenglowAddr(&net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 8002}); err != nil {
		t.Fatalf("SetAlpenglowAddr returned error: %v", err)
	}
	if err := contact.SetTPUVoteAddr(&net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 8002}); err != nil {
		t.Fatalf("SetTPUVoteAddr returned error: %v", err)
	}
	if err := contact.SetTPUVoteQuicAddr(&net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 8002}); err != nil {
		t.Fatalf("SetTPUVoteQuicAddr returned error: %v", err)
	}

	value, err := signCrdsContactInfo(contact, priv)
	if err != nil {
		t.Fatalf("signCrdsContactInfo returned error: %v", err)
	}
	if !value.Verify() {
		t.Fatalf("signed CRDS value did not verify")
	}

	var e encoder
	value.encode(&e)
	decoded, err := decodeCrdsValue(newDecoder(e.bytes()))
	if err != nil {
		t.Fatalf("decodeCrdsValue returned error: %v", err)
	}
	if !decoded.Verify() {
		t.Fatalf("decoded CRDS value did not verify")
	}
	if decoded.ContactInfo.ShredVer != 1234 {
		t.Fatalf("shred version = %d, want 1234", decoded.ContactInfo.ShredVer)
	}
	if decoded.ContactInfo.GossipAddr.String() != "203.0.113.10:65400" {
		t.Fatalf("gossip addr = %s", decoded.ContactInfo.GossipAddr.String())
	}
	if decoded.ContactInfo.TVUAddr.String() != "203.0.113.10:8001" {
		t.Fatalf("tvu addr = %s", decoded.ContactInfo.TVUAddr.String())
	}
	if decoded.ContactInfo.ServeRepairAddr.String() != "203.0.113.10:8008" {
		t.Fatalf("serve repair addr = %s", decoded.ContactInfo.ServeRepairAddr.String())
	}
	if decoded.ContactInfo.AlpenglowAddr.String() != "203.0.113.10:8002" {
		t.Fatalf("alpenglow addr = %s", decoded.ContactInfo.AlpenglowAddr.String())
	}
	if decoded.ContactInfo.TPUVoteAddr.String() != "203.0.113.10:8002" {
		t.Fatalf("tpu vote addr = %s", decoded.ContactInfo.TPUVoteAddr.String())
	}
	if decoded.ContactInfo.TPUVoteQuicAddr.String() != "203.0.113.10:8002" {
		t.Fatalf("tpu vote quic addr = %s", decoded.ContactInfo.TPUVoteQuicAddr.String())
	}
}

func TestClientAdvertisesAlpenglowSocket(t *testing.T) {
	client, err := NewClient(Config{
		Entrypoint:    "127.0.0.1:8000",
		BindAddr:      "0.0.0.0:0",
		TVUAddr:       "0.0.0.0:8001",
		AlpenglowAddr: "0.0.0.0:8002",
		AdvertisedIP:  "203.0.113.10",
		ShredVersion:  4321,
	})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	if err := client.initializeContact(&net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 65400}); err != nil {
		t.Fatalf("initializeContact returned error: %v", err)
	}

	client.contactMu.RLock()
	contact := client.contact
	client.contactMu.RUnlock()
	if contact == nil {
		t.Fatalf("expected contact info")
	}
	if contact.AlpenglowAddr == nil {
		t.Fatalf("expected alpenglow socket to be advertised")
	}
	if got, want := contact.AlpenglowAddr.String(), "203.0.113.10:8002"; got != want {
		t.Fatalf("alpenglow addr = %s, want %s", got, want)
	}
	if contact.TPUVoteAddr != nil || contact.TPUVoteQuicAddr != nil {
		t.Fatalf("Alpenglow tag 13 must not be duplicated into TPU vote tags 9/12: udp=%v quic=%v", contact.TPUVoteAddr, contact.TPUVoteQuicAddr)
	}
}

func TestClientConfirmsLiveDuplicateIdentityBeforeSignaling(t *testing.T) {
	_, identity, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey returned error: %v", err)
	}
	client, err := NewClient(Config{
		Entrypoint:    "127.0.0.1:8000",
		BindAddr:      "0.0.0.0:0",
		TVUAddr:       "0.0.0.0:8001",
		AlpenglowAddr: "0.0.0.0:8002",
		AdvertisedIP:  "203.0.113.10",
		ShredVersion:  4321,
		Identity:      identity,
	})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	if err := client.initializeContact(&net.UDPAddr{IP: net.IPv4zero, Port: 65400}); err != nil {
		t.Fatalf("initializeContact returned error: %v", err)
	}

	client.contactMu.RLock()
	localWallclock := client.contact.Wallclock
	client.contactMu.RUnlock()
	foreign, err := NewContactInfo(
		client.pubkey,
		4321,
		&net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 9000},
		&net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 9001},
	)
	if err != nil {
		t.Fatalf("NewContactInfo foreign returned error: %v", err)
	}
	if err := foreign.SetAlpenglowAddr(&net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 9002}); err != nil {
		t.Fatalf("SetAlpenglowAddr foreign returned error: %v", err)
	}

	client.handleContactRecord(contactRecordFromInfo(t, foreign.CloneWithWallclock(localWallclock+1), identity), 4321)
	select {
	case err := <-client.identityConflictCh:
		t.Fatalf("first foreign record must not signal a live conflict: %v", err)
	default:
	}
	// Repeating the same CRDS value through another peer is not proof that its
	// publisher is still alive.
	client.handleContactRecord(contactRecordFromInfo(t, foreign.CloneWithWallclock(localWallclock+1), identity), 4321)
	select {
	case err := <-client.identityConflictCh:
		t.Fatalf("echoed foreign record must not signal a live conflict: %v", err)
	default:
	}

	client.handleContactRecord(contactRecordFromInfo(t, foreign.CloneWithWallclock(localWallclock+2), identity), 4321)
	select {
	case err := <-client.identityConflictCh:
		if !errors.Is(err, ErrIdentityInUse) {
			t.Fatalf("identity conflict error = %v, want ErrIdentityInUse", err)
		}
	default:
		t.Fatal("newer foreign ContactInfo did not signal a live identity conflict")
	}
}

func TestClientLearnsAlpenglowEndpointFromSignedContact(t *testing.T) {
	_, localIdentity, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey local: %v", err)
	}
	client, err := NewClient(Config{
		Entrypoint:   "127.0.0.1:8000",
		BindAddr:     "0.0.0.0:0",
		ShredVersion: 4321,
		Identity:     localIdentity,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, foreignIdentity, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey foreign: %v", err)
	}
	foreignPubkey := Pubkey(foreignIdentity.Public().(ed25519.PublicKey))
	foreign, err := NewContactInfo(
		foreignPubkey,
		4321,
		&net.UDPAddr{IP: net.ParseIP("203.0.113.20"), Port: 9000},
		&net.UDPAddr{IP: net.ParseIP("203.0.113.20"), Port: 9001},
	)
	if err != nil {
		t.Fatalf("NewContactInfo: %v", err)
	}
	want := &net.UDPAddr{IP: net.ParseIP("203.0.113.20"), Port: 9002}
	if err := foreign.SetAlpenglowAddr(want); err != nil {
		t.Fatalf("SetAlpenglowAddr: %v", err)
	}
	client.handleContactRecord(contactRecordFromInfo(t, foreign, foreignIdentity), 4321)
	got, ok := client.LookupAlpenglow(solana.PublicKey(foreignPubkey))
	if !ok || got.String() != want.String() {
		t.Fatalf("LookupAlpenglow = %v, %t; want %v", got, ok, want)
	}
}

func TestClientLearnsServiceEndpointsFromEntrypointContactWithoutPullPeer(t *testing.T) {
	_, localIdentity, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey local: %v", err)
	}
	entrypoint := &net.UDPAddr{IP: net.ParseIP("203.0.113.20"), Port: 9000}
	client, err := NewClient(Config{
		Entrypoint:   entrypoint.String(),
		BindAddr:     "0.0.0.0:0",
		ShredVersion: 4321,
		Identity:     localIdentity,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	_, entrypointIdentity, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey entrypoint: %v", err)
	}
	entrypointPubkey := Pubkey(entrypointIdentity.Public().(ed25519.PublicKey))
	tvu := &net.UDPAddr{IP: net.ParseIP("203.0.113.20"), Port: 9001}
	repair := &net.UDPAddr{IP: net.ParseIP("203.0.113.20"), Port: 9008}
	alpenglow := &net.UDPAddr{IP: net.ParseIP("203.0.113.20"), Port: 9002}
	contact, err := NewContactInfo(entrypointPubkey, 4321, entrypoint, tvu)
	if err != nil {
		t.Fatalf("NewContactInfo: %v", err)
	}
	if err := contact.SetSocket(socketTagServeRepair, repair); err != nil {
		t.Fatalf("SetSocket serve repair: %v", err)
	}
	if err := contact.SetAlpenglowAddr(alpenglow); err != nil {
		t.Fatalf("SetAlpenglowAddr: %v", err)
	}

	client.handleContactRecord(contactRecordFromInfo(t, contact, entrypointIdentity), 4321)

	if got := len(client.currentPeers()); got != 0 {
		t.Fatalf("entrypoint entered pull-peer table: %d", got)
	}
	gotTVU, ok := client.LookupTVU(solana.PublicKey(entrypointPubkey))
	if !ok || gotTVU.String() != tvu.String() {
		t.Fatalf("LookupTVU = %v, %t; want %v", gotTVU, ok, tvu)
	}
	gotAlpenglow, ok := client.LookupAlpenglow(solana.PublicKey(entrypointPubkey))
	if !ok || gotAlpenglow.String() != alpenglow.String() {
		t.Fatalf("LookupAlpenglow = %v, %t; want %v", gotAlpenglow, ok, alpenglow)
	}
	repairPeers := client.RepairPeers()
	if len(repairPeers) != 1 || repairPeers[0].Pubkey != entrypointPubkey || repairPeers[0].Addr.String() != repair.String() {
		t.Fatalf("RepairPeers = %+v; want %s at %s", repairPeers, solana.PublicKey(entrypointPubkey), repair)
	}
}

func TestClientIgnoresEchoedAndStaleOwnContactRecords(t *testing.T) {
	_, identity, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey returned error: %v", err)
	}
	client, err := NewClient(Config{
		Entrypoint:    "127.0.0.1:8000",
		BindAddr:      "0.0.0.0:0",
		TVUAddr:       "0.0.0.0:8001",
		AlpenglowAddr: "0.0.0.0:8002",
		AdvertisedIP:  "203.0.113.10",
		ShredVersion:  4321,
		Identity:      identity,
	})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	if err := client.initializeContact(&net.UDPAddr{IP: net.IPv4zero, Port: 65400}); err != nil {
		t.Fatalf("initializeContact returned error: %v", err)
	}
	client.contactMu.RLock()
	local := client.contact.CloneWithWallclock(client.contact.Wallclock + 10)
	localWallclock := client.contact.Wallclock
	client.contactMu.RUnlock()
	client.handleContactRecord(contactRecordFromInfo(t, local, identity), 4321)

	stale, err := NewContactInfo(client.pubkey, 4321,
		&net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 9000},
		&net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 9001})
	if err != nil {
		t.Fatalf("NewContactInfo stale returned error: %v", err)
	}
	client.handleContactRecord(contactRecordFromInfo(t, stale.CloneWithWallclock(localWallclock), identity), 4321)
	select {
	case err := <-client.identityConflictCh:
		t.Fatalf("echoed/current or stale own contact signaled conflict: %v", err)
	default:
	}
	if got := len(client.currentPeers()); got != 0 {
		t.Fatalf("own contact records entered peer table: %d", got)
	}
}

func contactRecordFromInfo(t *testing.T, info *ContactInfo, identity ed25519.PrivateKey) contactRecord {
	t.Helper()
	value, err := signCrdsContactInfo(info, identity)
	if err != nil {
		t.Fatalf("signCrdsContactInfo returned error: %v", err)
	}
	var encoded encoder
	value.encode(&encoded)
	record, err := decodeCrdsContactRecord(newDecoder(encoded.bytes()))
	if err != nil {
		t.Fatalf("decodeCrdsContactRecord returned error: %v", err)
	}
	return record
}

func TestPingPongRoundTrip(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey returned error: %v", err)
	}
	ping, err := newPing(priv)
	if err != nil {
		t.Fatalf("newPing returned error: %v", err)
	}
	if !ping.Verify() {
		t.Fatalf("ping did not verify")
	}
	pong, err := newPong(ping, priv)
	if err != nil {
		t.Fatalf("newPong returned error: %v", err)
	}
	if !pong.Verify() {
		t.Fatalf("pong did not verify")
	}

	decoded, err := decodePacket(encodePingMessage(ping))
	if err != nil {
		t.Fatalf("decodePacket ping returned error: %v", err)
	}
	if decoded.Kind != packetPing || !decoded.Ping.Verify() {
		t.Fatalf("decoded ping did not verify")
	}

	decoded, err = decodePacket(encodePongMessage(pong))
	if err != nil {
		t.Fatalf("decodePacket pong returned error: %v", err)
	}
	if decoded.Kind != packetPong || !decoded.Pong.Verify() {
		t.Fatalf("decoded pong did not verify")
	}
}

func TestQueryEntrypointParsesEchoResponse(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		buf := make([]byte, ipEchoHeaderLength+16+1)
		_, _ = conn.Read(buf)
		var e encoder
		e.fixed([]byte{0, 0, 0, 0})
		if err := encodeIP(&e, net.ParseIP("198.51.100.7")); err != nil {
			t.Errorf("encodeIP returned error: %v", err)
			return
		}
		e.bool(true)
		e.u16(4321)
		_, _ = conn.Write(e.bytes())
	}()

	addr, err := net.ResolveUDPAddr("udp", listener.Addr().String())
	if err != nil {
		t.Fatalf("ResolveUDPAddr returned error: %v", err)
	}
	resp, err := QueryEntrypoint(addr, time.Second)
	if err != nil {
		t.Fatalf("QueryEntrypoint returned error: %v", err)
	}
	if resp.ShredVersion != 4321 {
		t.Fatalf("shred version = %d, want 4321", resp.ShredVersion)
	}
	if !resp.Address.Equal(net.ParseIP("198.51.100.7")) {
		t.Fatalf("address = %s", resp.Address.String())
	}
}
