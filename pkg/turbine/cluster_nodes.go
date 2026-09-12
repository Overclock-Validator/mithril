package turbine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sort"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/gagliardetto/solana-go"
	chacha "github.com/nixberg/chacha-rng-go"
)

const maxNodesPerIPAddress = 1

const (
	// dataPlaneFanout is Agave's DATA_PLANE_FANOUT. Alpenglow initially keeps
	// Turbine for block dissemination, so the same 200-wide tree is used.
	dataPlaneFanout = 200
	maxTurbineHops  = 4
)

var ErrRetransmitLoopback = errors.New("slot leader cannot retransmit its own shred")

type clusterNode struct {
	pubkey     solana.PublicKey
	stake      uint64
	tvuAddr    *net.UDPAddr
	hasContact bool // gossip ContactInfo present (Agave NodeId::ContactInfo)
}

// ClusterNodes implements Agave's broadcast-stage turbine peer selection.
type ClusterNodes struct {
	selfPubkey  solana.PublicKey
	nodes       []clusterNode
	selfIndex   int
	shuffle     *WeightedShuffle
	shufflePool sync.Pool
	useChaCha8  bool
}

type ClusterNodesConfig struct {
	Self         solana.PublicKey
	SelfTVU      *net.UDPAddr
	TVUPeers     []gossip.TVUPeer
	Stakes       map[solana.PublicKey]uint64
	DedupTVUAddr bool
	UseChaCha8   bool
}

func NewBroadcastClusterNodes(cfg ClusterNodesConfig) *ClusterNodes {
	return newClusterNodes(cfg, true)
}

func NewRetransmitClusterNodes(cfg ClusterNodesConfig) *ClusterNodes {
	return newClusterNodes(cfg, false)
}

func newClusterNodes(cfg ClusterNodesConfig, broadcast bool) *ClusterNodes {
	nodes := buildClusterNodes(cfg)
	selfIndex := -1
	for i, node := range nodes {
		if node.pubkey == cfg.Self {
			selfIndex = i
			break
		}
	}
	weights := make([]uint64, len(nodes))
	for i, node := range nodes {
		weights[i] = node.stake
	}
	shuffle := NewWeightedShuffle(weights)
	if broadcast && selfIndex >= 0 {
		shuffle.RemoveIndex(selfIndex)
	}
	return &ClusterNodes{
		selfPubkey: cfg.Self,
		nodes:      nodes,
		selfIndex:  selfIndex,
		shuffle:    shuffle,
		useChaCha8: cfg.UseChaCha8,
	}
}

// RetransmitPeers returns this node's distance from the Turbine root and its
// downstream peers for a shred. The leader is excluded before the weighted
// shuffle because it already owns the shred. Tree placement and fanout match
// Agave ClusterNodes<RetransmitStage>::get_retransmit_addrs.
func (c *ClusterNodes) RetransmitPeers(leader solana.PublicKey, shred ShredID, fanout int) (uint8, []*net.UDPAddr, error) {
	if c == nil || fanout <= 0 {
		return maxTurbineHops - 1, nil, nil
	}
	if leader == c.selfPubkey {
		return 0, nil, ErrRetransmitLoopback
	}
	shuffle := c.borrowRetransmitShuffle(leader)
	defer c.releaseRetransmitShuffle(shuffle)
	rng := newTurbineRNG(shred.Seed(leader), c.useChaCha8)
	selfPos := -1
	for position := 0; ; position++ {
		index, ok := shuffle.Next(rng)
		if !ok {
			break
		}
		if c.nodes[index].pubkey == c.selfPubkey {
			selfPos = position
			break
		}
	}
	if selfPos < 0 {
		return maxTurbineHops - 1, nil, nil
	}

	offset := 0
	if selfPos > 0 {
		offset = (selfPos - 1) % fanout
	}
	anchor := selfPos - offset
	step := fanout
	if selfPos == 0 {
		step = 1
	}
	position := anchor*fanout + offset + 1
	peers := make([]*net.UDPAddr, 0, fanout)
	shufflePosition := selfPos
	for range fanout {
		var index int
		for shufflePosition < position {
			var ok bool
			index, ok = shuffle.Next(rng)
			if !ok {
				return turbineRootDistance(selfPos, fanout), peers, nil
			}
			shufflePosition++
		}
		node := c.nodes[index]
		if node.hasContact {
			if addr, ok := broadcastTVUUDP(node.tvuAddr); ok {
				peers = append(peers, addr)
			}
		}
		position += step
	}
	return turbineRootDistance(selfPos, fanout), peers, nil
}

// RetransmitParent returns the deterministic upstream node whose signature is
// expected on a retransmitter-signed Merkle shred. Unstaked nodes have no
// stable parent because their tree position depends on gossip convergence.
func (c *ClusterNodes) RetransmitParent(leader solana.PublicKey, shred ShredID, fanout int) (solana.PublicKey, bool, error) {
	if c == nil || fanout <= 0 {
		return solana.PublicKey{}, false, nil
	}
	if leader == c.selfPubkey {
		return solana.PublicKey{}, false, ErrRetransmitLoopback
	}
	if c.selfIndex < 0 || c.nodes[c.selfIndex].stake == 0 {
		return solana.PublicKey{}, false, nil
	}
	shuffle := c.borrowRetransmitShuffle(leader)
	defer c.releaseRetransmitShuffle(shuffle)
	rng := newTurbineRNG(shred.Seed(leader), c.useChaCha8)
	beforeSelf := make([]int, 0, len(c.nodes)/2)
	for {
		index, ok := shuffle.Next(rng)
		if !ok {
			return solana.PublicKey{}, false, nil
		}
		if c.nodes[index].pubkey == c.selfPubkey {
			break
		}
		beforeSelf = append(beforeSelf, index)
	}
	selfPos := len(beforeSelf)
	if selfPos <= 0 {
		return solana.PublicKey{}, false, nil
	}

	offset := (selfPos - 1) % fanout
	parentPos := (selfPos - 1) / fanout
	if parentPos > 0 {
		parentPos -= (parentPos - 1) % fanout
		parentPos += offset
	}
	if parentPos < 0 || parentPos >= len(beforeSelf) {
		return solana.PublicKey{}, false, nil
	}
	return c.nodes[beforeSelf[parentPos]].pubkey, true, nil
}

func (c *ClusterNodes) retransmitShuffle(leader solana.PublicKey, shred ShredID) []int {
	shuffle := c.borrowRetransmitShuffle(leader)
	defer c.releaseRetransmitShuffle(shuffle)
	rng := newTurbineRNG(shred.Seed(leader), c.useChaCha8)
	indices := make([]int, 0, len(c.nodes))
	for {
		index, ok := shuffle.Next(rng)
		if !ok {
			break
		}
		indices = append(indices, index)
	}
	return indices
}

func (c *ClusterNodes) borrowRetransmitShuffle(leader solana.PublicKey) *WeightedShuffle {
	shuffle, _ := c.shufflePool.Get().(*WeightedShuffle)
	if shuffle == nil {
		shuffle = &WeightedShuffle{}
	}
	copyWeightedShuffle(shuffle, c.shuffle)
	for i, node := range c.nodes {
		if node.pubkey == leader {
			shuffle.RemoveIndex(i)
			break
		}
	}
	return shuffle
}

func (c *ClusterNodes) releaseRetransmitShuffle(shuffle *WeightedShuffle) {
	c.shufflePool.Put(shuffle)
}

func turbineRootDistance(index, fanout int) uint8 {
	switch {
	case index <= 0:
		return 0
	case index <= fanout:
		return 1
	case index <= (fanout+1)*fanout:
		return 2
	default:
		return maxTurbineHops - 1
	}
}

// BroadcastPeer returns the TVU root for a shred, matching get_broadcast_peer +
// tvu(Protocol::UDP) filtering used by Agave broadcast_shreds.
func (c *ClusterNodes) BroadcastPeer(shred ShredID) (*net.UDPAddr, bool) {
	if c == nil || len(c.nodes) == 0 {
		return nil, false
	}
	rng := newTurbineRNG(shred.Seed(c.selfPubkey), c.useChaCha8)
	shuffle := cloneWeightedShuffle(c.shuffle)
	index, ok := shuffle.First(rng)
	if !ok {
		return nil, false
	}
	node := c.nodes[index]
	if !node.hasContact {
		return nil, false
	}
	return broadcastTVUUDP(node.tvuAddr)
}

// HasRoutablePeer reports whether the current gossip snapshot contains any
// external IPv4 TVU route. Individual shreds can still select a deterministic
// staked identity whose contact info is missing; Agave drops that one route
// while relying on FEC, which is distinct from having no network route at all.
func (c *ClusterNodes) HasRoutablePeer() bool {
	if c == nil {
		return false
	}
	for _, node := range c.nodes {
		if node.pubkey == c.selfPubkey || !node.hasContact {
			continue
		}
		if _, ok := broadcastTVUUDP(node.tvuAddr); ok {
			return true
		}
	}
	return false
}

func broadcastTVUUDP(addr *net.UDPAddr) (*net.UDPAddr, bool) {
	if addr == nil || addr.IP == nil || addr.IP.To4() == nil {
		return nil, false
	}
	return addr, true
}

// BroadcastPeerRawIndex returns the weighted-shuffle index from get_broadcast_peer
// before TVU/contact filtering.
func (c *ClusterNodes) BroadcastPeerRawIndex(shred ShredID) (int, bool) {
	if c == nil || len(c.nodes) == 0 {
		return 0, false
	}
	rng := newTurbineRNG(shred.Seed(c.selfPubkey), c.useChaCha8)
	shuffle := cloneWeightedShuffle(c.shuffle)
	return shuffle.First(rng)
}

func buildClusterNodes(cfg ClusterNodesConfig) []clusterNode {
	type keyed struct {
		node clusterNode
	}
	byPubkey := make(map[solana.PublicKey]keyed)

	put := func(pubkey solana.PublicKey, stake uint64, tvu *net.UDPAddr, hasContact bool) {
		existing, ok := byPubkey[pubkey]
		if !ok {
			byPubkey[pubkey] = keyed{node: clusterNode{
				pubkey:     pubkey,
				stake:      stake,
				tvuAddr:    tvu,
				hasContact: hasContact,
			}}
			return
		}
		node := existing.node
		if stake > node.stake {
			node.stake = stake
		}
		if tvu != nil {
			node.tvuAddr = tvu
		}
		if hasContact {
			node.hasContact = true
		}
		byPubkey[pubkey] = keyed{node: node}
	}

	put(cfg.Self, cfg.Stakes[cfg.Self], cfg.SelfTVU, cfg.SelfTVU != nil)
	for _, peer := range cfg.TVUPeers {
		pubkey := solana.PublicKey(peer.Pubkey)
		put(pubkey, cfg.Stakes[pubkey], peer.TVUAddr, true)
	}
	for pubkey, stake := range cfg.Stakes {
		if stake == 0 {
			continue
		}
		put(pubkey, stake, nil, false)
	}

	nodes := make([]clusterNode, 0, len(byPubkey))
	for _, entry := range byPubkey {
		nodes = append(nodes, entry.node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].stake != nodes[j].stake {
			return nodes[i].stake > nodes[j].stake
		}
		if cmp := bytes.Compare(nodes[i].pubkey[:], nodes[j].pubkey[:]); cmp != 0 {
			// Agave orders (stake, pubkey) descending. Equal-stake identities
			// must therefore use descending pubkey order as well: node indices
			// feed the deterministic weighted shuffle and select the Turbine root.
			return cmp > 0
		}
		// Agave: NodeId::ContactInfo > NodeId::Pubkey for the same pubkey.
		return nodes[i].hasContact && !nodes[j].hasContact
	})
	nodes = dedupClusterNodePubkeys(nodes)
	if cfg.DedupTVUAddr {
		nodes = dedupTVUAddrs(nodes)
	}
	return nodes
}

func dedupClusterNodePubkeys(nodes []clusterNode) []clusterNode {
	if len(nodes) <= 1 {
		return nodes
	}
	out := nodes[:1]
	for _, node := range nodes[1:] {
		if node.pubkey == out[len(out)-1].pubkey {
			prev := out[len(out)-1]
			if node.stake > prev.stake {
				prev.stake = node.stake
			}
			if node.tvuAddr != nil {
				prev.tvuAddr = node.tvuAddr
			}
			out[len(out)-1] = prev
			continue
		}
		out = append(out, node)
	}
	return out
}

func dedupTVUAddrs(nodes []clusterNode) []clusterNode {
	type addrKey struct {
		ip   string
		port int
	}
	seen := make(map[addrKey]struct{})
	ipCounts := make(map[string]int)
	out := make([]clusterNode, 0, len(nodes))
	for _, node := range nodes {
		node := node
		if !node.hasContact {
			if node.stake > 0 {
				out = append(out, node)
			}
			continue
		}
		if node.tvuAddr != nil {
			ip := node.tvuAddr.IP.String()
			key := addrKey{ip: ip, port: node.tvuAddr.Port}
			ipCounts[ip]++
			if _, dup := seen[key]; dup || ipCounts[ip] > maxNodesPerIPAddress {
				node.tvuAddr = nil
			} else {
				seen[key] = struct{}{}
			}
		}
		if node.stake > 0 || node.tvuAddr != nil {
			out = append(out, node)
		}
	}
	return out
}

func newTurbineRNG(seed [32]byte, useChaCha8 bool) rngSource {
	var words [8]uint32
	for i := range words {
		words[i] = binary.LittleEndian.Uint32(seed[i*4:])
	}
	if useChaCha8 {
		return chacha.Seeded8(words, 0)
	}
	return chacha.Seeded20(words, 0)
}

func cloneWeightedShuffle(src *WeightedShuffle) *WeightedShuffle {
	if src == nil {
		return NewWeightedShuffle(nil)
	}
	cp := *src
	cp.zeros = append([]int(nil), src.zeros...)
	cp.tree = append([][weightedShuffleFanout]uint64(nil), src.tree...)
	return &cp
}

func copyWeightedShuffle(dst, src *WeightedShuffle) {
	if src == nil {
		*dst = *NewWeightedShuffle(nil)
		return
	}
	dst.numNodes = src.numNodes
	dst.weight = src.weight
	dst.zeros = append(dst.zeros[:0], src.zeros...)
	dst.tree = append(dst.tree[:0], src.tree...)
}

// StakedNodes maps vote-account stakes onto validator identities, matching
// Agave VoteAccounts::staked_nodes / Bank::epoch_staked_nodes.
func StakedNodes(voteStakes map[solana.PublicKey]uint64, nodeForVote func(vote solana.PublicKey) (solana.PublicKey, bool)) map[solana.PublicKey]uint64 {
	out := make(map[solana.PublicKey]uint64)
	for votePubkey, stake := range voteStakes {
		if nodeForVote == nil {
			continue
		}
		identity, ok := nodeForVote(votePubkey)
		if !ok {
			continue
		}
		out[identity] += stake
	}
	return out
}
