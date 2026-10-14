package turbine

import (
	"container/list"
	"crypto/ed25519"
	"net/netip"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	repairproto "github.com/Overclock-Validator/mithril/pkg/repair"
)

const (
	serveRepairPeerTTL      = 20 * time.Minute
	serveRepairPingInterval = 2 * time.Second
	serveRepairChallengeTTL = time.Minute
)

type serveRepairPeerKey struct {
	identity gossip.Pubkey
	addr     netip.AddrPort
}

type serveRepairPeer struct {
	key           serveRepairPeerKey
	hash          gossip.Hash
	lastPing      time.Time
	verifiedUntil time.Time
	pending       bool
}

// Both the identity and UDP source must answer a fresh random challenge before
// we read or return shreds. Signatures alone do not prevent UDP reflection.
// Workers and the packet reader share this bounded cache.
type serveRepairPeers struct {
	mu       sync.Mutex
	maxPeers int
	peers    map[serveRepairPeerKey]*list.Element
	order    list.List
}

func newServeRepairPeers(maxPeers int) *serveRepairPeers {
	if maxPeers < 1 {
		maxPeers = 1
	}
	return &serveRepairPeers{maxPeers: maxPeers, peers: make(map[serveRepairPeerKey]*list.Element)}
}

func (p *serveRepairPeers) check(key serveRepairPeerKey, identity ed25519.PrivateKey, now time.Time) (bool, []byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	element := p.peers[key]
	if element != nil {
		p.order.MoveToBack(element)
		peer := element.Value.(*serveRepairPeer)
		if now.Before(peer.verifiedUntil) && !now.Before(peer.lastPing) {
			return true, nil, nil
		}
		if !now.Before(peer.lastPing) && now.Sub(peer.lastPing) < serveRepairPingInterval {
			return false, nil, nil
		}
	}
	packet, hash, err := repairproto.BuildPing(identity)
	if err != nil {
		return false, nil, err
	}
	if element == nil {
		if len(p.peers) >= p.maxPeers {
			oldest := p.order.Front()
			delete(p.peers, oldest.Value.(*serveRepairPeer).key)
			p.order.Remove(oldest)
		}
		element = p.order.PushBack(&serveRepairPeer{key: key})
		p.peers[key] = element
	}
	peer := element.Value.(*serveRepairPeer)
	peer.hash, peer.lastPing, peer.pending = hash, now, true
	peer.verifiedUntil = time.Time{}
	return false, packet, nil
}

func (p *serveRepairPeers) accept(pong repairproto.Pong, addr netip.AddrPort, now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	element := p.peers[serveRepairPeerKey{identity: pong.From, addr: addr}]
	if element == nil {
		return false
	}
	peer := element.Value.(*serveRepairPeer)
	if !peer.pending || peer.hash != pong.Hash || now.Before(peer.lastPing) || now.Sub(peer.lastPing) > serveRepairChallengeTTL {
		return false
	}
	peer.pending = false
	peer.verifiedUntil = now.Add(serveRepairPeerTTL)
	p.order.MoveToBack(element)
	return true
}
