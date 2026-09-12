package alpenglow

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
	"github.com/quic-go/quic-go"
)

const (
	VotorQUICALPN                  = "alpenglow-v1"
	VotorQUICInitialPacketSize     = uint16(1280)
	DefaultReceiverBindAddr        = "0.0.0.0:0"
	DefaultReceiverMaxMessageSize  = int64(VotorQUICInitialPacketSize)
	DefaultReceiverLogInterval     = 10 * time.Second
	DefaultReceiverMaxConnections  = 512
	DefaultReceiverMaxConnsPerIP   = 32
	DefaultReceiverMaxConnsPerPeer = 2
	DefaultReceiverDatagramsPerSec = 50
)

type ReceiverConfig struct {
	BindAddr        string
	MaxMessageBytes int64
	ShredVersion    uint16
	// Identity binds the QUIC certificate's Ed25519 public key to the validator
	// identity, allowing Agave's staked A2A admission to recognize this node.
	// Empty retains an ephemeral certificate for passive observer mode.
	Identity              ed25519.PrivateKey
	Decode                DecodeOptions
	LogInterval           time.Duration
	MaxConnections        int
	MaxConnsPerIP         int
	MaxConnsPerPeer       int
	MaxDatagramsPerSecond int
	// AdmitPeer is called with the Ed25519 identity authenticated by TLS before
	// the connection is allowed to deliver datagrams. A nil callback admits any
	// correctly encoded single-certificate Ed25519 identity.
	AdmitPeer func(solana.PublicKey) bool
	// AdmitMessage runs after wire decoding/version checks but before the
	// observer. It may authenticate/normalize a message or reject a duplicate.
	// This boundary keeps unauthenticated certificates out of trusted observer
	// state and prevents rebroadcast duplicates from reaching expensive users.
	AdmitMessage func(solana.PublicKey, Message) (Message, bool)
	OnMessage    func(Message)
}

func DefaultReceiverConfig() ReceiverConfig {
	return ReceiverConfig{
		BindAddr:              DefaultReceiverBindAddr,
		MaxMessageBytes:       DefaultReceiverMaxMessageSize,
		Decode:                DefaultDecodeOptions(),
		LogInterval:           DefaultReceiverLogInterval,
		MaxConnections:        DefaultReceiverMaxConnections,
		MaxConnsPerIP:         DefaultReceiverMaxConnsPerIP,
		MaxConnsPerPeer:       DefaultReceiverMaxConnsPerPeer,
		MaxDatagramsPerSecond: DefaultReceiverDatagramsPerSec,
	}
}

type ReceiverStats struct {
	ConnectionsAccepted    uint64    `json:"connections_accepted"`
	DatagramsReceived      uint64    `json:"datagrams_received"`
	MessagesDecoded        uint64    `json:"messages_decoded"`
	VotesDecoded           uint64    `json:"votes_decoded"`
	CertificatesDecoded    uint64    `json:"certificates_decoded"`
	DecodeErrors           uint64    `json:"decode_errors"`
	ShredVersionMismatches uint64    `json:"shred_version_mismatches"`
	ReadErrors             uint64    `json:"read_errors"`
	AcceptErrors           uint64    `json:"accept_errors"`
	OversizeMessages       uint64    `json:"oversize_messages"`
	ConnectionsRejected    uint64    `json:"connections_rejected"`
	RateLimitedDatagrams   uint64    `json:"rate_limited_datagrams"`
	LastMessageAt          time.Time `json:"last_message_at,omitempty"`
	LatestVoteSlot         uint64    `json:"latest_vote_slot,omitempty"`
	LatestCertSlot         uint64    `json:"latest_certificate_slot,omitempty"`
}

type Receiver struct {
	cfg      ReceiverConfig
	observer *Observer
	listener *quic.Listener

	mu          sync.Mutex
	closed      bool
	conns       map[*quic.Conn]receiverConnection
	connsByIP   map[string]int
	connsByPeer map[solana.PublicKey]int
	stats       ReceiverStats
}

type receiverConnection struct {
	remoteIP string
	peer     solana.PublicKey
}

func NewReceiver(cfg ReceiverConfig, observer *Observer) (*Receiver, error) {
	cfg = normalizeReceiverConfig(cfg)
	if observer == nil {
		observer = NewObserver()
	}

	cert, err := newVotorQUICCertificate(cfg.Identity)
	if err != nil {
		return nil, fmt.Errorf("create Alpenglow Votor QUIC certificate: %w", err)
	}

	listener, err := quic.ListenAddr(cfg.BindAddr, &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAnyClientCert,
		NextProtos:   []string{VotorQUICALPN},
		MinVersion:   tls.VersionTLS13,
	}, newVotorQUICConfig())
	if err != nil {
		return nil, fmt.Errorf("listen for Alpenglow Votor QUIC on %s: %w", cfg.BindAddr, err)
	}

	return &Receiver{
		cfg:         cfg,
		observer:    observer,
		listener:    listener,
		conns:       make(map[*quic.Conn]receiverConnection),
		connsByIP:   make(map[string]int),
		connsByPeer: make(map[solana.PublicKey]int),
	}, nil
}

func (r *Receiver) Addr() net.Addr {
	if r == nil || r.listener == nil {
		return nil
	}
	return r.listener.Addr()
}

func (r *Receiver) Run(ctx context.Context) error {
	if r == nil || r.listener == nil {
		return fmt.Errorf("nil Alpenglow Votor receiver")
	}
	defer r.Close()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go r.logStatsLoop(runCtx)

	mlog.Log.FileOnlyf("ALPENGLOW Votor receiver listening on %s (transport=quic-datagram alpn=%s packet_mtu=%d app_max_datagram_bytes=%d shred_version=%d)",
		r.listener.Addr(), VotorQUICALPN, VotorQUICInitialPacketSize, r.cfg.MaxMessageBytes, r.cfg.ShredVersion)

	for {
		conn, err := r.listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, quic.ErrServerClosed) || r.isClosed() {
				return nil
			}
			r.recordAcceptError()
			return fmt.Errorf("accept Alpenglow Votor QUIC connection: %w", err)
		}

		if _, ok := receiverRemoteIPv4(conn.RemoteAddr()); !ok {
			r.rejectConn(conn, "alpenglow receiver requires an IPv4 unicast peer")
			continue
		}
		peer, err := votorPeerIdentity(conn.ConnectionState().TLS)
		if err != nil || (r.cfg.AdmitPeer != nil && !r.cfg.AdmitPeer(peer)) {
			r.rejectConn(conn, "alpenglow receiver peer identity rejected")
			continue
		}
		if !r.addConn(conn, peer) {
			continue
		}
		r.recordConnection()
		go r.handleConn(runCtx, conn, peer)
	}
}

func (r *Receiver) Close() error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	listener := r.listener
	conns := make([]*quic.Conn, 0, len(r.conns))
	for conn := range r.conns {
		conns = append(conns, conn)
	}
	r.mu.Unlock()

	var err error
	if listener != nil {
		err = listener.Close()
	}
	for _, conn := range conns {
		_ = conn.CloseWithError(0, "alpenglow receiver closed")
	}
	if errors.Is(err, quic.ErrServerClosed) {
		return nil
	}
	return err
}

func (r *Receiver) Stats() ReceiverStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats
}

func (r *Receiver) handleConn(ctx context.Context, conn *quic.Conn, peer solana.PublicKey) {
	defer r.removeConn(conn)
	defer conn.CloseWithError(0, "alpenglow receiver done")

	windowStart := time.Now()
	datagramsThisWindow := 0
	for {
		payload, err := conn.ReceiveDatagram(ctx)
		if err != nil {
			if ctx.Err() != nil || conn.Context().Err() != nil || r.isClosed() {
				return
			}
			r.recordReadError()
			return
		}
		now := time.Now()
		if now.Sub(windowStart) >= time.Second {
			windowStart = now
			datagramsThisWindow = 0
		}
		if datagramsThisWindow >= r.cfg.MaxDatagramsPerSecond {
			r.recordRateLimitedDatagram()
			continue
		}
		datagramsThisWindow++
		r.handleDatagram(peer, payload)
	}
}

func (r *Receiver) handleDatagram(peer solana.PublicKey, payload []byte) {
	r.recordDatagram()
	if int64(len(payload)) > r.cfg.MaxMessageBytes {
		r.recordOversizeMessage()
		return
	}

	msg, err := DecodeMessageWithOptions(payload, r.cfg.Decode)
	if err != nil {
		r.recordDecodeError()
		return
	}
	if r.cfg.ShredVersion != 0 && msg.ShredVersion != r.cfg.ShredVersion {
		r.recordShredVersionMismatch()
		return
	}
	// Count decoded traffic independently of admission. In particular, a
	// duplicate storm must remain visible even though admission collapses it to
	// one cryptographic verification and one observer update.
	r.recordMessage(msg)
	if r.cfg.AdmitMessage != nil {
		var admitted bool
		msg, admitted = r.cfg.AdmitMessage(peer, msg)
		if !admitted {
			return
		}
	}
	if _, err := r.observer.ObserveMessage(msg); err != nil {
		r.recordDecodeError()
		return
	}
	if r.cfg.OnMessage != nil {
		r.cfg.OnMessage(msg)
	}
}

func (r *Receiver) logStatsLoop(ctx context.Context) {
	if r.cfg.LogInterval <= 0 {
		return
	}

	ticker := time.NewTicker(r.cfg.LogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := r.Stats()
			mlog.Log.FileOnlyf("alpenglow Votor receiver stats: connections=%d datagrams=%d messages=%d votes=%d certificates=%d decode_errors=%d shred_version_mismatches=%d read_errors=%d accept_errors=%d oversize=%d rejected_connections=%d rate_limited_datagrams=%d latest_vote_slot=%d latest_certificate_slot=%d last_message=%s",
				stats.ConnectionsAccepted, stats.DatagramsReceived, stats.MessagesDecoded, stats.VotesDecoded, stats.CertificatesDecoded,
				stats.DecodeErrors, stats.ShredVersionMismatches, stats.ReadErrors, stats.AcceptErrors, stats.OversizeMessages, stats.ConnectionsRejected, stats.RateLimitedDatagrams, stats.LatestVoteSlot, stats.LatestCertSlot,
				formatStatsTime(stats.LastMessageAt))
		}
	}
}

func (r *Receiver) recordConnection() {
	r.mu.Lock()
	r.stats.ConnectionsAccepted++
	r.mu.Unlock()
}

func (r *Receiver) recordDatagram() {
	r.mu.Lock()
	r.stats.DatagramsReceived++
	r.mu.Unlock()
}

func (r *Receiver) recordMessage(msg Message) {
	r.mu.Lock()
	r.stats.MessagesDecoded++
	r.stats.LastMessageAt = time.Now()
	if msg.Vote != nil {
		r.stats.VotesDecoded++
		if msg.Vote.Vote.Slot > r.stats.LatestVoteSlot {
			r.stats.LatestVoteSlot = msg.Vote.Vote.Slot
		}
	}
	if msg.Certificate != nil {
		r.stats.CertificatesDecoded++
		if msg.Certificate.Slot > r.stats.LatestCertSlot {
			r.stats.LatestCertSlot = msg.Certificate.Slot
		}
	}
	first := r.stats.MessagesDecoded == 1
	stats := r.stats
	r.mu.Unlock()

	if first {
		mlog.Log.Infof("ALPENGLOW Votor receiver decoded first message (votes=%d certificates=%d latest_vote_slot=%d latest_certificate_slot=%d)",
			stats.VotesDecoded, stats.CertificatesDecoded, stats.LatestVoteSlot, stats.LatestCertSlot)
	}
}

func (r *Receiver) recordDecodeError() {
	r.mu.Lock()
	r.stats.DecodeErrors++
	r.mu.Unlock()
}

func (r *Receiver) recordShredVersionMismatch() {
	r.mu.Lock()
	r.stats.ShredVersionMismatches++
	r.mu.Unlock()
}

func (r *Receiver) recordReadError() {
	r.mu.Lock()
	r.stats.ReadErrors++
	r.mu.Unlock()
}

func (r *Receiver) recordAcceptError() {
	r.mu.Lock()
	r.stats.AcceptErrors++
	r.mu.Unlock()
}

func (r *Receiver) recordOversizeMessage() {
	r.mu.Lock()
	r.stats.OversizeMessages++
	r.mu.Unlock()
}

func (r *Receiver) recordRateLimitedDatagram() {
	r.mu.Lock()
	r.stats.RateLimitedDatagrams++
	r.mu.Unlock()
}

func (r *Receiver) rejectConn(conn *quic.Conn, reason string) {
	r.mu.Lock()
	r.stats.ConnectionsRejected++
	r.mu.Unlock()
	_ = conn.CloseWithError(0, reason)
}

func (r *Receiver) addConn(conn *quic.Conn, peer solana.PublicKey) bool {
	remoteIP, ok := receiverRemoteIPv4(conn.RemoteAddr())
	if !ok {
		r.rejectConn(conn, "alpenglow receiver requires an IPv4 unicast peer")
		return false
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		_ = conn.CloseWithError(0, "alpenglow receiver closed")
		return false
	}
	if len(r.conns) >= r.cfg.MaxConnections || r.connsByIP[remoteIP] >= r.cfg.MaxConnsPerIP ||
		r.connsByPeer[peer] >= r.cfg.MaxConnsPerPeer {
		r.stats.ConnectionsRejected++
		r.mu.Unlock()
		_ = conn.CloseWithError(0, "alpenglow receiver connection limit")
		return false
	}
	r.conns[conn] = receiverConnection{remoteIP: remoteIP, peer: peer}
	r.connsByIP[remoteIP]++
	r.connsByPeer[peer]++
	r.mu.Unlock()
	return true
}

func (r *Receiver) removeConn(conn *quic.Conn) {
	r.mu.Lock()
	tracked, exists := r.conns[conn]
	delete(r.conns, conn)
	if exists {
		r.connsByIP[tracked.remoteIP]--
		if r.connsByIP[tracked.remoteIP] <= 0 {
			delete(r.connsByIP, tracked.remoteIP)
		}
		r.connsByPeer[tracked.peer]--
		if r.connsByPeer[tracked.peer] <= 0 {
			delete(r.connsByPeer, tracked.peer)
		}
	}
	r.mu.Unlock()
}

func receiverRemoteIPv4(addr net.Addr) (string, bool) {
	udp, ok := addr.(*net.UDPAddr)
	if !ok || udp.IP == nil || udp.IP.IsUnspecified() || udp.IP.IsMulticast() {
		return "", false
	}
	ipv4 := udp.IP.To4()
	if ipv4 == nil {
		return "", false
	}
	return ipv4.String(), true
}

func (r *Receiver) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func normalizeReceiverConfig(cfg ReceiverConfig) ReceiverConfig {
	defaults := DefaultReceiverConfig()
	if cfg.BindAddr == "" {
		cfg.BindAddr = defaults.BindAddr
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = defaults.MaxMessageBytes
	}
	if cfg.Decode.MaxBitmapSize == 0 {
		cfg.Decode = defaults.Decode
	}
	if cfg.LogInterval == 0 {
		cfg.LogInterval = defaults.LogInterval
	}
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = defaults.MaxConnections
	}
	if cfg.MaxConnsPerIP <= 0 {
		cfg.MaxConnsPerIP = defaults.MaxConnsPerIP
	}
	if cfg.MaxConnsPerPeer <= 0 {
		cfg.MaxConnsPerPeer = defaults.MaxConnsPerPeer
	}
	if cfg.MaxDatagramsPerSecond <= 0 {
		cfg.MaxDatagramsPerSecond = defaults.MaxDatagramsPerSecond
	}
	return cfg
}

func formatStatsTime(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return time.Since(t).Truncate(time.Second).String() + " ago"
}
