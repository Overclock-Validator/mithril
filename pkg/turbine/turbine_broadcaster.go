package turbine

import (
	"fmt"
	"net"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/gagliardetto/solana-go"
)

const defaultBroadcastLeaderSlots = 4 // protocol default consecutive leader window

// TVUPeerSource supplies gossip TVU peers for turbine routing.
type TVUPeerSource interface {
	TVUPeers() []gossip.TVUPeer
	SelfTVUAddr() *net.UDPAddr
	LookupTVU(pubkey solana.PublicKey) (*net.UDPAddr, bool)
}

// TurbineBroadcaster sends each shred to the turbine root peer for the slot.
type TurbineBroadcaster struct {
	mu sync.Mutex

	conn   *net.UDPConn
	self   func() solana.PublicKey
	peers  TVUPeerSource
	stakes func(uint64) map[solana.PublicKey]uint64
	epoch  func(uint64) uint64

	leaderForSlot func(slot uint64) (solana.PublicKey, bool)
	leaderSlots   uint64

	useChaCha8 bool
	dedupAddrs bool
	receivers  []*net.UDPAddr

	cacheSlot  uint64
	cacheSelf  solana.PublicKey
	cacheValid bool
	cacheNodes *ClusterNodes
}

type TurbineBroadcasterConfig struct {
	BindAddr      string
	Self          func() solana.PublicKey // this node's identity, read per shred so set-identity moves the broadcast tree
	Peers         TVUPeerSource
	Stakes        func(slot uint64) map[solana.PublicKey]uint64
	EpochForSlot  func(slot uint64) uint64
	LeaderForSlot func(slot uint64) (solana.PublicKey, bool)
	LeaderSlots   uint64
	UseChaCha8    bool
	DedupAddrs    bool
	// ShredReceivers get a copy of every shred this node broadcasts, in
	// addition to the Turbine root, such as a block builder on the same host.
	ShredReceivers []*net.UDPAddr
}

func NewTurbineBroadcaster(cfg TurbineBroadcasterConfig) (*TurbineBroadcaster, error) {
	var conn *net.UDPConn
	if cfg.BindAddr != "" {
		addr, err := net.ResolveUDPAddr("udp", cfg.BindAddr)
		if err != nil {
			return nil, fmt.Errorf("resolve turbine broadcast bind %q: %w", cfg.BindAddr, err)
		}
		conn, err = net.ListenUDP("udp", addr)
		if err != nil {
			return nil, fmt.Errorf("listen turbine broadcast udp %q: %w", cfg.BindAddr, err)
		}
	} else {
		var err error
		conn, err = net.ListenUDP("udp", nil)
		if err != nil {
			return nil, fmt.Errorf("open turbine broadcast udp socket: %w", err)
		}
	}
	if cfg.Self == nil {
		return nil, fmt.Errorf("turbine broadcast: self identity is required")
	}
	leaderSlots := cfg.LeaderSlots
	if leaderSlots == 0 {
		leaderSlots = defaultBroadcastLeaderSlots
	}
	return &TurbineBroadcaster{
		conn:          conn,
		self:          cfg.Self,
		peers:         cfg.Peers,
		stakes:        cfg.Stakes,
		epoch:         cfg.EpochForSlot,
		leaderForSlot: cfg.LeaderForSlot,
		leaderSlots:   leaderSlots,
		useChaCha8:    cfg.UseChaCha8,
		dedupAddrs:    cfg.DedupAddrs,
		receivers:     cfg.ShredReceivers,
	}, nil
}

func (b *TurbineBroadcaster) Close() error {
	if b.conn == nil {
		return nil
	}
	return b.conn.Close()
}

func (b *TurbineBroadcaster) Broadcast(packets [][]byte) error {
	for _, packet := range packets {
		if err := b.broadcastPacket(packet); err != nil {
			return err
		}
	}
	return nil
}

func (b *TurbineBroadcaster) broadcastPacket(packet []byte) error {
	shredID, err := ShredIDFromPacket(packet)
	if err != nil {
		return fmt.Errorf("turbine broadcast: parse shred id: %w", err)
	}
	nodes := b.clusterNodesForSlot(shredID.Slot)
	dests := b.broadcastDestinations(shredID, nodes)
	// The deterministic root can be a staked identity whose contact info is
	// absent. Match Agave by dropping that route and relying on the FEC set.
	// With no external route whatsoever, however, fail the local slot closed.
	if len(dests) == 0 && !nodes.HasRoutablePeer() {
		return fmt.Errorf("turbine broadcast: no routable TVU destination for slot %d shred %d", shredID.Slot, shredID.Index)
	}
	for _, peer := range dests {
		if err := b.sendTo(packet, peer); err != nil {
			return err
		}
	}
	// After Turbine, so receivers never delay it. A receiver that cannot be
	// reached does not fail the slot.
	for _, receiver := range b.receivers {
		_ = b.sendTo(packet, receiver)
	}
	return nil
}

// broadcastDestinations selects turbine broadcast peers: weighted root plus
// optional next-leader TVU when distinct.
func (b *TurbineBroadcaster) broadcastDestinations(shredID ShredID, nodes *ClusterNodes) []*net.UDPAddr {
	var dests []*net.UDPAddr
	if addr, ok := nodes.BroadcastPeer(shredID); ok {
		dests = append(dests, addr)
	}
	if b.leaderForSlot != nil && b.peers != nil {
		nextSlot := shredID.Slot + b.leaderSlots
		nextLeader, ok := b.leaderForSlot(nextSlot)
		if ok && nextLeader != b.self() {
			if addr, ok := b.peers.LookupTVU(nextLeader); ok {
				if addr, ok := broadcastTVUUDP(addr); ok {
					if len(dests) == 0 || dests[0].String() != addr.String() {
						dests = append(dests, addr)
					}
				}
			}
		}
	}
	return dests
}

func (b *TurbineBroadcaster) clusterNodesForSlot(slot uint64) *ClusterNodes {
	b.mu.Lock()
	defer b.mu.Unlock()
	self := b.self()
	if b.cacheNodes != nil && b.cacheValid && b.cacheSlot == slot && b.cacheSelf == self {
		return b.cacheNodes
	}
	stakes := map[solana.PublicKey]uint64{}
	if b.stakes != nil {
		stakes = b.stakes(slot)
	}
	var tvuPeers []gossip.TVUPeer
	var selfTVU *net.UDPAddr
	if b.peers != nil {
		tvuPeers = b.peers.TVUPeers()
		selfTVU = b.peers.SelfTVUAddr()
	}
	b.cacheNodes = NewBroadcastClusterNodes(ClusterNodesConfig{
		Self:         self,
		SelfTVU:      selfTVU,
		TVUPeers:     tvuPeers,
		Stakes:       stakes,
		DedupTVUAddr: b.dedupAddrs,
		UseChaCha8:   b.useChaCha8,
	})
	b.cacheSlot = slot
	b.cacheSelf = self
	b.cacheValid = true
	return b.cacheNodes
}

func (b *TurbineBroadcaster) sendTo(packet []byte, peer *net.UDPAddr) error {
	if peer == nil {
		return fmt.Errorf("turbine broadcast: nil peer")
	}
	if b.conn == nil {
		return fmt.Errorf("turbine broadcast: closed UDP socket")
	}
	_, err := b.conn.WriteToUDP(packet, peer)
	return err
}
