package alpenglow

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
	"github.com/quic-go/quic-go"
)

const (
	defaultVotorBroadcastQueue      = 1024
	defaultVotorSendWorkers         = 32
	defaultVotorPeerJobQueue        = 16384
	defaultVotorPeerRefreshInterval = time.Second
)

type VotorPeer struct {
	Identity solana.PublicKey
	Addr     *net.UDPAddr
}

type VotorPeerSource func() []VotorPeer

type VotorBroadcasterConfig struct {
	Identity     ed25519.PrivateKey
	ShredVersion uint16
	Peers        VotorPeerSource
	QueueSize    int
	Workers      int
}

type VotorBroadcasterStats struct {
	MessagesQueued        uint64
	MessagesDropped       uint64
	PeerSends             uint64
	PeerSendsSkipped      uint64
	PeerSendErrors        uint64
	DesiredPeers          int
	Connections           int
	PendingConnections    int
	ConnectionAttempts    uint64
	ConnectionErrors      uint64
	ConnectionJobsDropped uint64
	LastPeerSendError     string
	LastPeerSendErrorAt   time.Time
	LastConnectionError   string
	LastConnectionErrorAt time.Time
}

type votorPeerJobKind uint8

const (
	votorPeerJobConnect votorPeerJobKind = iota
	votorPeerJobSend
)

type votorPeerJob struct {
	kind    votorPeerJobKind
	peer    VotorPeer
	payload []byte
}

type votorConnection struct {
	addr string
	conn *quic.Conn
}

type votorDial struct {
	done chan struct{}
}

type VotorBroadcaster struct {
	shredVersion          uint16
	peers                 VotorPeerSource
	tlsConfig             *tls.Config
	quicConfig            *quic.Config
	ctx                   context.Context
	cancel                context.CancelFunc
	queue                 chan Message
	jobs                  chan votorPeerJob
	done                  chan struct{}
	closeOnce             sync.Once
	wg                    sync.WaitGroup
	closed                atomic.Bool
	connMu                sync.Mutex
	desired               map[solana.PublicKey]VotorPeer
	conns                 map[solana.PublicKey]votorConnection
	dialing               map[solana.PublicKey]*votorDial
	connectQueued         map[solana.PublicKey]struct{}
	errorMu               sync.Mutex
	lastSendError         string
	lastSendErrorAt       time.Time
	lastConnectionError   string
	lastConnectionErrorAt time.Time
	queued                atomic.Uint64
	dropped               atomic.Uint64
	sends                 atomic.Uint64
	sendsSkipped          atomic.Uint64
	sendErrors            atomic.Uint64
	connectionAttempts    atomic.Uint64
	connectionErrors      atomic.Uint64
	connectionJobsDropped atomic.Uint64
}

func NewVotorBroadcaster(cfg VotorBroadcasterConfig) (*VotorBroadcaster, error) {
	if len(cfg.Identity) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("Votor broadcaster: validator identity is required")
	}
	if cfg.Peers == nil {
		return nil, fmt.Errorf("Votor broadcaster: peer source is required")
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultVotorBroadcastQueue
	}
	if cfg.Workers <= 0 {
		cfg.Workers = defaultVotorSendWorkers
	}
	certificate, err := newVotorQUICCertificate(cfg.Identity)
	if err != nil {
		return nil, fmt.Errorf("Votor broadcaster certificate: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &VotorBroadcaster{
		shredVersion: cfg.ShredVersion,
		peers:        cfg.Peers,
		tlsConfig: &tls.Config{
			Certificates:       []tls.Certificate{certificate},
			NextProtos:         []string{VotorQUICALPN},
			MinVersion:         tls.VersionTLS13,
			InsecureSkipVerify: true, // The validator SPKI is checked explicitly after the handshake.
		},
		quicConfig:    newVotorQUICConfig(),
		ctx:           ctx,
		cancel:        cancel,
		queue:         make(chan Message, cfg.QueueSize),
		jobs:          make(chan votorPeerJob, defaultVotorPeerJobQueue),
		done:          make(chan struct{}),
		desired:       make(map[solana.PublicKey]VotorPeer),
		conns:         make(map[solana.PublicKey]votorConnection),
		dialing:       make(map[solana.PublicKey]*votorDial),
		connectQueued: make(map[solana.PublicKey]struct{}),
	}
	b.wg.Add(2 + cfg.Workers)
	for range cfg.Workers {
		go b.sendLoop()
	}
	go b.broadcastLoop()
	// Populate the desired set and queue the first bounded preconnects before
	// returning. Consensus messages therefore never drive peer discovery.
	b.reconcilePeers()
	go b.peerReconcileLoop()
	return b, nil
}

// Enqueue broadcasts a vote or certificate to every currently desired Votor
// peer. A full local queue is surfaced to the caller; the voting engine treats
// dropping a newly signed vote as fatal.
func (b *VotorBroadcaster) Enqueue(message Message) error {
	if b == nil {
		return fmt.Errorf("Votor broadcaster is nil")
	}
	if b.closed.Load() {
		return fmt.Errorf("Votor broadcaster is closed")
	}
	message.ShredVersion = b.shredVersion
	if err := message.ValidateBasic(); err != nil {
		return err
	}
	select {
	case <-b.done:
		return fmt.Errorf("Votor broadcaster is closed")
	case b.queue <- message:
		b.queued.Add(1)
		return nil
	default:
		b.dropped.Add(1)
		return fmt.Errorf("Votor broadcaster queue is full")
	}
}

func (b *VotorBroadcaster) broadcastLoop() {
	defer b.wg.Done()
	for {
		select {
		case <-b.done:
			return
		case message := <-b.queue:
			payload, err := EncodeMessage(message)
			if err != nil {
				b.recordSendError(VotorPeer{}, fmt.Errorf("encode Votor message: %w", err))
				continue
			}
			peers, skipped := b.connectedPeers()
			b.sendsSkipped.Add(uint64(skipped))
			for _, peer := range peers {
				job := votorPeerJob{kind: votorPeerJobSend, peer: peer, payload: payload}
				select {
				case b.jobs <- job:
				case <-b.done:
					return
				default:
					b.dropped.Add(1)
				}
			}
		}
	}
}

func (b *VotorBroadcaster) sendLoop() {
	defer b.wg.Done()
	for {
		select {
		case <-b.done:
			return
		case job := <-b.jobs:
			switch job.kind {
			case votorPeerJobConnect:
				b.connectPeer(job.peer.Identity)
			case votorPeerJobSend:
				if err := b.send(job.peer, job.payload); err != nil {
					b.recordSendError(job.peer, err)
				} else {
					b.sends.Add(1)
				}
			}
		}
	}
}

func (b *VotorBroadcaster) send(peer VotorPeer, payload []byte) error {
	conn, ok := b.establishedConnection(peer)
	if !ok {
		b.queueConnect(peer.Identity)
		return fmt.Errorf("send Votor datagram to %s (%s): no established connection", peer.Identity, peer.Addr)
	}
	err := conn.SendDatagram(payload)
	if err == nil {
		return nil
	}
	var tooLarge *quic.DatagramTooLargeError
	if !errors.As(err, &tooLarge) {
		b.dropConnection(peer.Identity, conn)
		b.queueConnect(peer.Identity)
	}
	return fmt.Errorf("send Votor datagram to %s (%s): %w", peer.Identity, peer.Addr, err)
}

func (b *VotorBroadcaster) peerReconcileLoop() {
	defer b.wg.Done()
	ticker := time.NewTicker(defaultVotorPeerRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.done:
			return
		case <-ticker.C:
			b.reconcilePeers()
		}
	}
}

func (b *VotorBroadcaster) reconcilePeers() {
	next := make(map[solana.PublicKey]VotorPeer)
	for _, peer := range b.peers() {
		if !validVotorPeer(peer) {
			continue
		}
		if _, duplicate := next[peer.Identity]; duplicate {
			continue
		}
		peer.Addr = cloneUDPAddr(peer.Addr)
		next[peer.Identity] = peer
	}

	var stale []*quic.Conn
	b.connMu.Lock()
	if b.closed.Load() {
		b.connMu.Unlock()
		return
	}
	b.desired = next
	for identity, existing := range b.conns {
		peer, desired := next[identity]
		if desired && peer.Addr.String() == existing.addr && existing.conn.Context().Err() == nil {
			continue
		}
		delete(b.conns, identity)
		stale = append(stale, existing.conn)
	}
	for identity := range next {
		b.queueConnectLocked(identity)
	}
	b.connMu.Unlock()
	for _, conn := range stale {
		_ = conn.CloseWithError(0, "Votor peer departed or changed address")
	}
}

func (b *VotorBroadcaster) connectedPeers() ([]VotorPeer, int) {
	b.connMu.Lock()
	peers := make([]VotorPeer, 0, len(b.desired))
	skipped := 0
	for identity, peer := range b.desired {
		existing, connected := b.conns[identity]
		if !connected || existing.addr != peer.Addr.String() || existing.conn.Context().Err() != nil {
			skipped++
			continue
		}
		peer.Addr = cloneUDPAddr(peer.Addr)
		peers = append(peers, peer)
	}
	b.connMu.Unlock()
	return peers, skipped
}

func (b *VotorBroadcaster) desiredPeer(identity solana.PublicKey) (VotorPeer, bool) {
	b.connMu.Lock()
	peer, ok := b.desired[identity]
	if ok {
		peer.Addr = cloneUDPAddr(peer.Addr)
	}
	b.connMu.Unlock()
	return peer, ok
}

func (b *VotorBroadcaster) queueConnect(identity solana.PublicKey) {
	b.connMu.Lock()
	b.queueConnectLocked(identity)
	b.connMu.Unlock()
}

func (b *VotorBroadcaster) queueConnectLocked(identity solana.PublicKey) {
	peer, desired := b.desired[identity]
	if !desired || b.closed.Load() {
		return
	}
	if existing, ok := b.conns[identity]; ok && existing.addr == peer.Addr.String() && existing.conn.Context().Err() == nil {
		return
	}
	if _, queued := b.connectQueued[identity]; queued || b.dialing[identity] != nil {
		return
	}
	job := votorPeerJob{kind: votorPeerJobConnect, peer: peer}
	select {
	case b.jobs <- job:
		b.connectQueued[identity] = struct{}{}
	default:
		b.connectionJobsDropped.Add(1)
	}
}

func (b *VotorBroadcaster) connectPeer(identity solana.PublicKey) {
	defer func() {
		b.connMu.Lock()
		delete(b.connectQueued, identity)
		b.connMu.Unlock()
	}()
	peer, desired := b.desiredPeer(identity)
	if !desired || b.closed.Load() {
		return
	}
	if _, err := b.connection(peer); err != nil && !errors.Is(err, errVotorPeerNotDesired) && !b.closed.Load() {
		b.recordConnectionError(peer, err)
	}
}

func (b *VotorBroadcaster) establishedConnection(peer VotorPeer) (*quic.Conn, bool) {
	b.connMu.Lock()
	current, desired := b.desired[peer.Identity]
	if !desired || current.Addr.String() != peer.Addr.String() {
		b.connMu.Unlock()
		return nil, false
	}
	existing, ok := b.conns[peer.Identity]
	if ok && (existing.addr != current.Addr.String() || existing.conn.Context().Err() != nil) {
		delete(b.conns, peer.Identity)
		ok = false
	}
	b.connMu.Unlock()
	return existing.conn, ok
}

var errVotorPeerNotDesired = errors.New("Votor peer is no longer desired")

func (b *VotorBroadcaster) connection(peer VotorPeer) (*quic.Conn, error) {
	addr := peer.Addr.String()
	ctx, cancel := context.WithTimeout(b.ctx, 2*time.Second)
	defer cancel()
	for {
		if b.closed.Load() {
			return nil, fmt.Errorf("Votor broadcaster is closed")
		}
		b.connMu.Lock()
		current, desired := b.desired[peer.Identity]
		if !desired || current.Addr.String() != addr {
			b.connMu.Unlock()
			return nil, errVotorPeerNotDesired
		}
		if existing, ok := b.conns[peer.Identity]; ok && existing.addr == addr && existing.conn.Context().Err() == nil {
			b.connMu.Unlock()
			return existing.conn, nil
		}
		if inFlight := b.dialing[peer.Identity]; inFlight != nil {
			done := inFlight.done
			b.connMu.Unlock()
			select {
			case <-done:
				continue
			case <-b.done:
				return nil, fmt.Errorf("Votor broadcaster is closed")
			case <-ctx.Done():
				return nil, fmt.Errorf("wait for Votor peer %s (%s) connection: %w", peer.Identity, peer.Addr, ctx.Err())
			}
		}
		inFlight := &votorDial{done: make(chan struct{})}
		b.dialing[peer.Identity] = inFlight
		b.connMu.Unlock()
		defer func() {
			b.connMu.Lock()
			if b.dialing[peer.Identity] == inFlight {
				delete(b.dialing, peer.Identity)
				close(inFlight.done)
			}
			b.connMu.Unlock()
		}()
		break
	}

	b.connectionAttempts.Add(1)
	conn, err := quic.DialAddr(ctx, addr, b.tlsConfig.Clone(), b.quicConfig.Clone())
	if err != nil {
		return nil, fmt.Errorf("connect Votor peer %s (%s): %w", peer.Identity, peer.Addr, err)
	}
	remoteIdentity, err := votorPeerIdentity(conn.ConnectionState().TLS)
	if err != nil {
		_ = conn.CloseWithError(0, "invalid Votor server identity")
		return nil, fmt.Errorf("authenticate Votor peer %s (%s): %w", peer.Identity, peer.Addr, err)
	}
	if remoteIdentity != peer.Identity {
		_ = conn.CloseWithError(0, "unexpected Votor server identity")
		return nil, fmt.Errorf("authenticate Votor peer %s (%s): certificate is for %s", peer.Identity, peer.Addr, remoteIdentity)
	}
	state := conn.ConnectionState()
	if !state.SupportsDatagrams.Local || !state.SupportsDatagrams.Remote {
		_ = conn.CloseWithError(0, "Votor peer does not support QUIC datagrams")
		return nil, fmt.Errorf("Votor peer %s (%s) does not support QUIC datagrams", peer.Identity, peer.Addr)
	}
	if b.closed.Load() {
		_ = conn.CloseWithError(0, "Votor broadcaster closed")
		return nil, fmt.Errorf("Votor broadcaster is closed")
	}
	b.connMu.Lock()
	if b.closed.Load() {
		b.connMu.Unlock()
		_ = conn.CloseWithError(0, "Votor broadcaster closed")
		return nil, fmt.Errorf("Votor broadcaster is closed")
	}
	current, desired := b.desired[peer.Identity]
	if !desired || current.Addr.String() != addr {
		b.connMu.Unlock()
		_ = conn.CloseWithError(0, "Votor peer no longer desired")
		return nil, errVotorPeerNotDesired
	}
	if existing, ok := b.conns[peer.Identity]; ok && existing.addr == addr && existing.conn.Context().Err() == nil {
		b.connMu.Unlock()
		_ = conn.CloseWithError(0, "duplicate Votor connection")
		return existing.conn, nil
	}
	stale := b.conns[peer.Identity].conn
	b.conns[peer.Identity] = votorConnection{addr: addr, conn: conn}
	b.connMu.Unlock()
	if stale != nil {
		_ = stale.CloseWithError(0, "Votor peer address changed")
	}
	return conn, nil
}

func (b *VotorBroadcaster) dropConnection(identity solana.PublicKey, conn *quic.Conn) {
	b.connMu.Lock()
	if existing, ok := b.conns[identity]; ok && existing.conn == conn {
		delete(b.conns, identity)
	}
	b.connMu.Unlock()
	_ = conn.CloseWithError(0, "Votor send failed")
}

func (b *VotorBroadcaster) Stats() VotorBroadcasterStats {
	if b == nil {
		return VotorBroadcasterStats{}
	}
	b.connMu.Lock()
	connections := 0
	for identity, existing := range b.conns {
		if existing.conn.Context().Err() != nil {
			delete(b.conns, identity)
			continue
		}
		connections++
	}
	desiredPeers := len(b.desired)
	pendingConnections := len(b.connectQueued)
	b.connMu.Unlock()
	b.errorMu.Lock()
	lastSendError, lastSendErrorAt := b.lastSendError, b.lastSendErrorAt
	lastConnectionError, lastConnectionErrorAt := b.lastConnectionError, b.lastConnectionErrorAt
	b.errorMu.Unlock()
	return VotorBroadcasterStats{
		MessagesQueued:        b.queued.Load(),
		MessagesDropped:       b.dropped.Load(),
		PeerSends:             b.sends.Load(),
		PeerSendsSkipped:      b.sendsSkipped.Load(),
		PeerSendErrors:        b.sendErrors.Load(),
		DesiredPeers:          desiredPeers,
		Connections:           connections,
		PendingConnections:    pendingConnections,
		ConnectionAttempts:    b.connectionAttempts.Load(),
		ConnectionErrors:      b.connectionErrors.Load(),
		ConnectionJobsDropped: b.connectionJobsDropped.Load(),
		LastPeerSendError:     lastSendError,
		LastPeerSendErrorAt:   lastSendErrorAt,
		LastConnectionError:   lastConnectionError,
		LastConnectionErrorAt: lastConnectionErrorAt,
	}
}

func (b *VotorBroadcaster) Close() error {
	if b == nil {
		return nil
	}
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		b.cancel()
		close(b.done)
		b.connMu.Lock()
		clear(b.desired)
		for identity, existing := range b.conns {
			_ = existing.conn.CloseWithError(0, "Votor broadcaster closed")
			delete(b.conns, identity)
		}
		b.connMu.Unlock()
		b.wg.Wait()
	})
	return nil
}

func validVotorPeer(peer VotorPeer) bool {
	return peer.Identity != (solana.PublicKey{}) && peer.Addr != nil && peer.Addr.Port != 0 &&
		peer.Addr.IP != nil && peer.Addr.IP.To4() != nil && !peer.Addr.IP.IsUnspecified() && !peer.Addr.IP.IsMulticast()
}

func (b *VotorBroadcaster) recordSendError(peer VotorPeer, err error) {
	if err == nil {
		return
	}
	detail := err.Error()
	if peer.Identity != (solana.PublicKey{}) {
		detail = fmt.Sprintf("peer=%s addr=%s: %v", peer.Identity, peer.Addr, err)
	}
	now := time.Now()
	b.errorMu.Lock()
	b.lastSendError = detail
	b.lastSendErrorAt = now
	b.errorMu.Unlock()

	count := b.sendErrors.Add(1)
	// Preserve the actionable error without flooding the log during a cluster-
	// wide outage: log the first failure and then exponentially sparse samples.
	if count == 1 || count&(count-1) == 0 {
		mlog.Log.FileOnlyf("ALPENGLOW Votor broadcaster send error (count=%d): %s", count, detail)
	}
}

func (b *VotorBroadcaster) recordConnectionError(peer VotorPeer, err error) {
	if err == nil {
		return
	}
	detail := fmt.Sprintf("peer=%s addr=%s: %v", peer.Identity, peer.Addr, err)
	now := time.Now()
	b.errorMu.Lock()
	b.lastConnectionError = detail
	b.lastConnectionErrorAt = now
	b.errorMu.Unlock()

	count := b.connectionErrors.Add(1)
	if count == 1 || count&(count-1) == 0 {
		mlog.Log.FileOnlyf("ALPENGLOW Votor broadcaster connection error (count=%d): %s", count, detail)
	}
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	ip := append(net.IP(nil), addr.IP...)
	return &net.UDPAddr{IP: ip, Port: addr.Port, Zone: addr.Zone}
}
