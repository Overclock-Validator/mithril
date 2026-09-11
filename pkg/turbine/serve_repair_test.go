package turbine

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	repairproto "github.com/Overclock-Validator/mithril/pkg/repair"
	"github.com/gagliardetto/solana-go"
)

func TestServeRepairPeerRateLimit(t *testing.T) {
	var limit serveRepairPeerLimit
	now := time.Unix(100, 0)
	for i := 0; i < serveRepairMaxRequestsPerSecond; i++ {
		if !limit.allow(now) {
			t.Fatalf("request %d was limited before the ceiling", i)
		}
	}
	if limit.allow(now) {
		t.Fatal("request above the per-second ceiling was admitted")
	}
	if !limit.allow(now.Add(serveRepairRateLimitWindow)) {
		t.Fatal("new rate-limit window did not admit a request")
	}
}

func TestServeRepairRateLimitTableEvictsOldestAtCapacity(t *testing.T) {
	now := time.Unix(100, 0)
	limits := newServeRepairRateLimits(2, now)
	a := netip.MustParseAddrPort("192.0.2.1:1001")
	b := netip.MustParseAddrPort("192.0.2.2:1002")
	c := netip.MustParseAddrPort("192.0.2.3:1003")

	if !limits.allow(a, now) || !limits.allow(b, now.Add(time.Nanosecond)) {
		t.Fatal("fresh peers were rate limited")
	}
	if !limits.allow(c, now.Add(2*time.Nanosecond)) {
		t.Fatal("new peer was rejected at the table capacity")
	}
	if len(limits.peers) != 2 {
		t.Fatalf("tracked peers = %d, want 2", len(limits.peers))
	}
	if limits.peers[a] != nil || limits.peers[b] == nil || limits.peers[c] == nil {
		t.Fatal("capacity did not evict the least-recently-used peer")
	}
	if !limits.allow(a, now.Add(3*time.Nanosecond)) {
		t.Fatal("an evicted peer could not re-enter the bounded table")
	}
}

func TestServeRepairInvalidPacketsDoNotConsumePeerLimits(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	server := &ServeRepairServer{
		self:   gossip.Pubkey{1},
		limits: newServeRepairRateLimits(2, now),
		now:    func() time.Time { return now },
	}
	work := make(chan serveRepairWork, 1)
	for i := 0; i < 100; i++ {
		addr := netip.AddrPortFrom(netip.MustParseAddr("192.0.2.1"), uint16(1000+i))
		server.handlePacket([]byte{0xff}, addr, work)
	}
	if len(server.limits.peers) != 0 {
		t.Fatalf("malformed packets created %d peer limit entries", len(server.limits.peers))
	}

	_, clientIdentity, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey client: %v", err)
	}
	wrongRecipient, err := repairproto.BuildWindowIndexRequest(clientIdentity, gossip.Pubkey{}, 1, 0, 1)
	if err != nil {
		t.Fatalf("BuildWindowIndexRequest: %v", err)
	}
	for i := 0; i < 100; i++ {
		addr := netip.AddrPortFrom(netip.MustParseAddr("198.51.100.1"), uint16(1000+i))
		server.handlePacket(wrongRecipient, addr, work)
	}
	if len(server.limits.peers) != 0 {
		t.Fatalf("header-invalid packets created %d peer limit entries", len(server.limits.peers))
	}
	stats := server.Stats()
	if stats.DropMalformed != 100 || stats.DropHeaderInvalid != 100 {
		t.Fatalf("invalid packet stats = malformed %d header %d", stats.DropMalformed, stats.DropHeaderInvalid)
	}
}

func TestServeRepairHeaderValidation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	server := &ServeRepairServer{}
	for i := range server.self {
		server.self[i] = byte(i + 1)
	}
	request := repairproto.Request{
		Sender:    gossip.Pubkey{0xff},
		Recipient: server.self,
		Timestamp: uint64(now.UnixMilli()),
	}
	if !server.validHeader(request, now) {
		t.Fatal("current request header was rejected")
	}

	toleranceMillis := uint64(serveRepairTimestampTolerance / time.Millisecond)
	request.Timestamp += toleranceMillis
	if !server.validHeader(request, now) {
		t.Fatal("request at future timestamp tolerance was rejected")
	}
	request.Timestamp++
	if server.validHeader(request, now) {
		t.Fatal("request beyond future timestamp tolerance was accepted")
	}
	request.Timestamp = uint64(now.UnixMilli()) - toleranceMillis - 1
	if server.validHeader(request, now) {
		t.Fatal("stale request was accepted")
	}
	request.Timestamp = uint64(now.UnixMilli())
	request.Sender = server.self
	if server.validHeader(request, now) {
		t.Fatal("request sent by the serving identity was accepted")
	}
	request.Sender = gossip.Pubkey{0xff}
	request.Recipient = gossip.Pubkey{}
	if server.validHeader(request, now) {
		t.Fatal("request for another recipient was accepted")
	}
}

func TestSetServeRepairRequiresVerifiedShredSpool(t *testing.T) {
	_, identity, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	spool, err := OpenShredSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("OpenShredSpool: %v", err)
	}
	defer spool.Close()

	receiver := NewUDPReceiver("127.0.0.1:0")
	receiver.SetShredSpool(spool)
	if err := receiver.SetServeRepair("127.0.0.1:0", identity); err == nil {
		t.Fatal("SetServeRepair accepted a receiver without leader verification")
	}
	receiver.SetLeaderForSlot(func(uint64) (solana.PublicKey, bool) {
		return solana.PublicKey{}, true
	})
	if err := receiver.SetServeRepair("127.0.0.1:0", identity); err != nil {
		t.Fatalf("SetServeRepair with verified spool: %v", err)
	}
}

func TestServeRepairWindowHighestAndPing(t *testing.T) {
	serverPub, serverIdentity, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey server: %v", err)
	}
	clientPub, clientIdentity, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey client: %v", err)
	}
	var serverKey gossip.Pubkey
	copy(serverKey[:], serverPub)

	spool, err := OpenShredSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("OpenShredSpool: %v", err)
	}
	defer spool.Close()
	shreds := generatedAlpenglowDataShreds(t)
	if len(shreds) < 2 {
		t.Fatalf("generated %d data shreds, want at least two", len(shreds))
	}
	for _, shred := range shreds {
		if !spool.AppendShred(shred, shred.Payload) {
			t.Fatalf("AppendShred(%d) returned false", shred.Index)
		}
	}

	server, err := NewServeRepairServer(ServeRepairConfig{
		Addr:     "127.0.0.1:0",
		Identity: serverIdentity,
		Store:    spool,
	})
	if err != nil {
		t.Fatalf("NewServeRepairServer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("ServeRepairServer.Run: %v", err)
		}
	}()

	client, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP client: %v", err)
	}
	defer client.Close()

	first := shreds[0]
	last := shreds[len(shreds)-1]
	window, err := repairproto.BuildWindowIndexRequest(clientIdentity, serverKey, first.Slot, uint64(first.Index), 0x11223344)
	if err != nil {
		t.Fatalf("BuildWindowIndexRequest: %v", err)
	}
	response := sendRepairRequest(t, client, server.Addr(), window)
	assertRepairResponse(t, response, first.Payload, 0x11223344)

	highest, err := repairproto.BuildHighestWindowIndexRequest(clientIdentity, serverKey, first.Slot, 0, 0x55667788)
	if err != nil {
		t.Fatalf("BuildHighestWindowIndexRequest: %v", err)
	}
	response = sendRepairRequest(t, client, server.Addr(), highest)
	assertRepairResponse(t, response, last.Payload, 0x55667788)

	// Solana peers can challenge the repair source with a signed ping before
	// accepting responses. The same socket must answer with our identity.
	var token [32]byte
	for i := range token {
		token[i] = byte(i + 1)
	}
	ping := buildRepairPing(clientPub, clientIdentity, token)
	pong := sendRepairRequest(t, client, server.Addr(), ping)
	if len(pong) != 132 || binary.LittleEndian.Uint32(pong[:4]) != 7 {
		t.Fatalf("pong shape = %d bytes variant %d", len(pong), binary.LittleEndian.Uint32(pong[:4]))
	}
	if string(pong[4:36]) != string(serverPub) {
		t.Fatal("pong used the wrong sender identity")
	}
	hash := sha256.Sum256(append([]byte("SOLANA_PING_PONG"), token[:]...))
	if string(pong[36:68]) != string(hash[:]) || !ed25519.Verify(serverPub, hash[:], pong[68:]) {
		t.Fatal("pong hash/signature did not verify")
	}

	// Header rejection happens before signature verification and never reflects
	// a shred to a packet addressed to another validator.
	wrongRecipient, err := repairproto.BuildWindowIndexRequest(clientIdentity, gossip.Pubkey{}, first.Slot, uint64(first.Index), 9)
	if err != nil {
		t.Fatalf("BuildWindowIndexRequest wrong recipient: %v", err)
	}
	assertNoRepairResponse(t, client, server.Addr(), wrongRecipient)

	badSignature := append([]byte(nil), window...)
	badSignature[4] ^= 0x80
	assertNoRepairResponse(t, client, server.Addr(), badSignature)

	waitForServeRepairStats(t, server, func(stats ServeRepairStats) bool {
		return stats.Served == 2 && stats.Pings == 1 && stats.Pongs == 1 &&
			stats.DropHeaderInvalid == 1 && stats.DropSignature == 1
	})
	stats := server.Stats()
	if stats.WindowIndex != 3 || stats.HighestWindowIndex != 1 {
		t.Fatalf("request-kind stats = window %d highest %d", stats.WindowIndex, stats.HighestWindowIndex)
	}
}

func TestServeRepairPingBypassesRequestRateLimit(t *testing.T) {
	serverPub, serverIdentity, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey server: %v", err)
	}
	clientPub, clientIdentity, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey client: %v", err)
	}
	var serverKey gossip.Pubkey
	copy(serverKey[:], serverPub)

	spool, err := OpenShredSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("OpenShredSpool: %v", err)
	}
	defer spool.Close()
	server, err := NewServeRepairServer(ServeRepairConfig{
		Addr:     "127.0.0.1:0",
		Identity: serverIdentity,
		Store:    spool,
	})
	if err != nil {
		t.Fatalf("NewServeRepairServer: %v", err)
	}
	defer server.Close()

	client, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP client: %v", err)
	}
	defer client.Close()
	clientAddr := client.LocalAddr().(*net.UDPAddr).AddrPort()

	request, err := repairproto.BuildWindowIndexRequest(clientIdentity, serverKey, 1, 0, 1)
	if err != nil {
		t.Fatalf("BuildWindowIndexRequest: %v", err)
	}
	work := make(chan serveRepairWork, serveRepairMaxRequestsPerSecond)
	for i := 0; i < serveRepairMaxRequestsPerSecond; i++ {
		server.handlePacket(request, clientAddr, work)
	}
	if len(work) != serveRepairMaxRequestsPerSecond {
		t.Fatalf("queued requests = %d, want %d", len(work), serveRepairMaxRequestsPerSecond)
	}

	var token [32]byte
	for i := range token {
		token[i] = byte(i + 1)
	}
	server.handlePacket(buildRepairPing(clientPub, clientIdentity, token), clientAddr, work)
	if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, packetDataSize)
	n, _, err := client.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("ReadFromUDP pong: %v", err)
	}
	if n != 132 || binary.LittleEndian.Uint32(buf[:4]) != 7 {
		t.Fatalf("pong shape = %d bytes variant %d", n, binary.LittleEndian.Uint32(buf[:4]))
	}
	stats := server.Stats()
	if stats.Pings != 1 || stats.Pongs != 1 || stats.RateLimited != 0 {
		t.Fatalf("ping stats after saturated request limit: %+v", stats)
	}
}

func buildRepairPing(pub ed25519.PublicKey, identity ed25519.PrivateKey, token [32]byte) []byte {
	packet := binary.LittleEndian.AppendUint32(nil, 0)
	packet = append(packet, pub...)
	packet = append(packet, token[:]...)
	return append(packet, ed25519.Sign(identity, token[:])...)
}

func sendRepairRequest(t *testing.T, conn *net.UDPConn, addr *net.UDPAddr, packet []byte) []byte {
	t.Helper()
	if _, err := conn.WriteToUDP(packet, addr); err != nil {
		t.Fatalf("WriteToUDP: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, packetDataSize)
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("ReadFromUDP: %v", err)
	}
	return append([]byte(nil), buf[:n]...)
}

func assertNoRepairResponse(t *testing.T, conn *net.UDPConn, addr *net.UDPAddr, packet []byte) {
	t.Helper()
	if _, err := conn.WriteToUDP(packet, addr); err != nil {
		t.Fatalf("WriteToUDP: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, packetDataSize)
	if n, _, err := conn.ReadFromUDP(buf); err == nil {
		t.Fatalf("unexpected repair response (%d bytes)", n)
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("ReadFromUDP: %v", err)
	}
}

func assertRepairResponse(t *testing.T, response, shred []byte, nonce uint32) {
	t.Helper()
	if len(response) != len(shred)+4 {
		t.Fatalf("response length = %d, want %d", len(response), len(shred)+4)
	}
	if string(response[:len(shred)]) != string(shred) {
		t.Fatal("response shred payload changed")
	}
	if got := binary.LittleEndian.Uint32(response[len(shred):]); got != nonce {
		t.Fatalf("response nonce = %#x, want %#x", got, nonce)
	}
}

func waitForServeRepairStats(t *testing.T, server *ServeRepairServer, ready func(ServeRepairStats) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ready(server.Stats()) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("serve repair stats did not converge: %+v", server.Stats())
}
