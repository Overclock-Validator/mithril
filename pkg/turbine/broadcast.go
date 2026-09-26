package turbine

import (
	"fmt"
	"net"
	"sync"

	"github.com/gagliardetto/solana-go"
)

// PacketBroadcaster sends shred packets to turbine peers.
type PacketBroadcaster interface {
	Broadcast(packets [][]byte) error
}

// UDPBroadcaster sends packets to configured UDP destinations.
type UDPBroadcaster struct {
	mu    sync.Mutex
	conn  *net.UDPConn
	peers []*net.UDPAddr
}

func NewUDPBroadcaster(bindAddr string) (*UDPBroadcaster, error) {
	var conn *net.UDPConn
	if bindAddr != "" {
		addr, err := net.ResolveUDPAddr("udp", bindAddr)
		if err != nil {
			return nil, fmt.Errorf("resolve broadcast bind %q: %w", bindAddr, err)
		}
		conn, err = net.ListenUDP("udp", addr)
		if err != nil {
			return nil, fmt.Errorf("listen broadcast udp %q: %w", bindAddr, err)
		}
	}
	return &UDPBroadcaster{conn: conn}, nil
}

func (b *UDPBroadcaster) SetPeers(peers []*net.UDPAddr) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.peers = append([]*net.UDPAddr(nil), peers...)
}

func (b *UDPBroadcaster) AddPeer(addr *net.UDPAddr) {
	if addr == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.peers = append(b.peers, addr)
}

func (b *UDPBroadcaster) Broadcast(packets [][]byte) error {
	b.mu.Lock()
	peers := append([]*net.UDPAddr(nil), b.peers...)
	conn := b.conn
	b.mu.Unlock()

	if len(peers) == 0 {
		return nil
	}
	if conn == nil {
		var d net.Dialer
		for _, packet := range packets {
			for _, peer := range peers {
				c, err := d.Dial("udp", peer.String())
				if err != nil {
					return err
				}
				if _, err := c.Write(packet); err != nil {
					_ = c.Close()
					return err
				}
				_ = c.Close()
			}
		}
		return nil
	}
	for _, packet := range packets {
		for _, peer := range peers {
			if _, err := conn.WriteToUDP(packet, peer); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *UDPBroadcaster) Close() error {
	if b.conn == nil {
		return nil
	}
	return b.conn.Close()
}

// BroadcastSession shreds and broadcasts Alpenglow block components for one leader slot.
type BroadcastSession struct {
	leader      solana.PrivateKey
	shredder    Shredder
	broadcaster PacketBroadcaster
	userAgent   []byte

	chainedMerkleRoot solana.Hash
	nextDataIndex     uint32
	nextCodeIndex     uint32
	fecSetRoots       []solana.Hash
}

type BroadcastSessionConfig struct {
	Leader        solana.PrivateKey
	Slot          uint64
	ParentSlot    uint64
	ParentBlockID solana.Hash
	// ParentChainedMerkleRoot is the last data-shred merkle root of the parent slot.
	// It seeds the chained merkle root embedded in this slot's first FEC batch.
	ParentChainedMerkleRoot solana.Hash
	Broadcaster             PacketBroadcaster
	UserAgent               []byte
	Version                 uint16
}

func NewBroadcastSession(cfg BroadcastSessionConfig) *BroadcastSession {
	chainedRoot := solana.Hash{}
	if cfg.Slot > cfg.ParentSlot {
		chainedRoot = cfg.ParentChainedMerkleRoot
	}
	return &BroadcastSession{
		leader: cfg.Leader,
		shredder: Shredder{
			Slot:          cfg.Slot,
			ParentSlot:    cfg.ParentSlot,
			Version:       cfg.Version,
			ReferenceTick: 0,
		},
		broadcaster:       cfg.Broadcaster,
		userAgent:         cfg.UserAgent,
		chainedMerkleRoot: chainedRoot,
	}
}

func (s *BroadcastSession) BroadcastHeader(parentBlockID solana.Hash) error {
	component := NewBlockHeader(s.shredder.ParentSlot, parentBlockID)
	return s.broadcastComponent(component, false)
}

func (s *BroadcastSession) BroadcastComponent(component BlockComponent, isLastInSlot bool) error {
	return s.broadcastComponent(component, isLastInSlot)
}

func (s *BroadcastSession) BroadcastEntryBatch(entries []Entry) error {
	component, err := NewEntryBatch(entries)
	if err != nil {
		return err
	}
	return s.broadcastComponent(component, false)
}

func (s *BroadcastSession) BroadcastEndingTick(hash solana.Hash) error {
	return s.BroadcastEndingTickLast(hash)
}

func (s *BroadcastSession) BroadcastFooter(bankHash solana.Hash, producerTimeNanos uint64, skipRewardCert, notarRewardCert []byte) error {
	component := NewBlockFooter(BlockFooter{
		BankHash:               bankHash,
		BlockProducerTimeNanos: producerTimeNanos,
		BlockUserAgent:         append([]byte(nil), s.userAgent...),
		SkipRewardCert:         append([]byte(nil), skipRewardCert...),
		NotarRewardCert:        append([]byte(nil), notarRewardCert...),
	})
	return s.broadcastComponent(component, false)
}

func (s *BroadcastSession) BroadcastEndingTickLast(hash solana.Hash) error {
	component, err := NewEntryBatch([]Entry{{NumHashes: 1, Hash: hash}})
	if err != nil {
		return err
	}
	return s.broadcastComponent(component, true)
}

func (s *BroadcastSession) BlockID(parentSlot uint64, parentBlockID solana.Hash) solana.Hash {
	return DoubleMerkleBlockID(parentSlot, parentBlockID, s.fecSetRoots)
}

// ChainedMerkleRoot returns the running chained merkle root after the last broadcast batch.
func (s *BroadcastSession) ChainedMerkleRoot() solana.Hash {
	return s.chainedMerkleRoot
}

func (s *BroadcastSession) broadcastComponent(component BlockComponent, isLastInSlot bool) error {
	if s.broadcaster == nil {
		return nil
	}
	batch, nextData, nextCode, err := s.shredder.makeMerklePacketsFromComponent(
		s.leader,
		component,
		isLastInSlot,
		s.chainedMerkleRoot,
		s.nextDataIndex,
		s.nextCodeIndex,
	)
	if err != nil {
		return err
	}
	s.chainedMerkleRoot = batch.chainedMerkleRoot
	s.nextDataIndex = nextData
	s.nextCodeIndex = nextCode
	s.fecSetRoots = append(s.fecSetRoots, batch.fecSetRoots...)
	return s.broadcaster.Broadcast(batch.packets)
}
