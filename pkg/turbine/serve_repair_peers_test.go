package turbine

import (
	"crypto/ed25519"
	"net/netip"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	repairproto "github.com/Overclock-Validator/mithril/pkg/repair"
)

func TestServeRepairSourceChallengeIdentityAddressExpiryAndBound(t *testing.T) {
	_, server, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	pub, client, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	var sender gossip.Pubkey
	copy(sender[:], pub)
	now := time.Unix(1700000000, 0)
	key := serveRepairPeerKey{identity: sender, addr: netip.MustParseAddrPort("127.0.0.1:1234")}
	peers := newServeRepairPeers(2)
	verified, packet, err := peers.check(key, server, now)
	if err != nil || verified {
		t.Fatalf("initial check: %v %v", verified, err)
	}
	ping, ok := repairproto.DecodePing(packet)
	if !ok {
		t.Fatal("invalid challenge signature")
	}
	if verified, retry, err := peers.check(key, server, now.Add(time.Millisecond)); err != nil || verified || len(retry) != 0 {
		t.Fatal("challenge retries were not throttled")
	}
	encoded, err := repairproto.BuildPong(client, ping)
	if err != nil {
		t.Fatal(err)
	}
	pong, ok := repairproto.DecodePong(encoded)
	if !ok {
		t.Fatal("invalid pong signature")
	}
	wrongHash := pong
	wrongHash.Hash[0] ^= 1
	if peers.accept(wrongHash, key.addr, now) {
		t.Fatal("wrong challenge accepted")
	}
	wrongIdentity := pong
	wrongIdentity.From[0] ^= 1
	if peers.accept(wrongIdentity, key.addr, now) {
		t.Fatal("wrong identity accepted")
	}
	otherAddr := netip.MustParseAddrPort("127.0.0.1:1235")
	if peers.accept(pong, otherAddr, now) {
		t.Fatal("pong from another UDP source accepted")
	}
	if !peers.accept(pong, key.addr, now) {
		t.Fatal("matching pong rejected")
	}
	if peers.accept(pong, key.addr, now.Add(time.Second)) {
		t.Fatal("replayed pong renewed authorization")
	}
	if verified, packet, err := peers.check(key, server, now.Add(time.Second)); err != nil || !verified || len(packet) != 0 {
		t.Fatal("verified peer rejected")
	}
	verified, packet, err = peers.check(key, server, now.Add(serveRepairPeerTTL))
	if err != nil || verified || len(packet) == 0 {
		t.Fatal("expired peer was not rechallenged")
	}
	// A newer challenge supersedes the old hash.
	if peers.accept(pong, key.addr, now.Add(serveRepairPeerTTL)) {
		t.Fatal("old challenge accepted after expiry")
	}
	newPing, ok := repairproto.DecodePing(packet)
	if !ok {
		t.Fatal("invalid replacement challenge")
	}
	newEncoded, err := repairproto.BuildPong(client, newPing)
	if err != nil {
		t.Fatal(err)
	}
	newPong, ok := repairproto.DecodePong(newEncoded)
	if !ok {
		t.Fatal("invalid replacement pong")
	}
	if peers.accept(newPong, key.addr, now.Add(serveRepairPeerTTL+serveRepairChallengeTTL+time.Nanosecond)) {
		t.Fatal("expired challenge accepted")
	}
	for i := uint16(1235); i < 1245; i++ {
		other := serveRepairPeerKey{identity: sender, addr: netip.AddrPortFrom(key.addr.Addr(), i)}
		if _, _, err := peers.check(other, server, now); err != nil {
			t.Fatal(err)
		}
	}
	if len(peers.peers) != 2 || peers.order.Len() != 2 {
		t.Fatal("source challenge cache exceeded its bound")
	}
	if peers.peers[key] != nil || peers.accept(newPong, key.addr, now) {
		t.Fatal("evicted challenge remained authorized")
	}
}
