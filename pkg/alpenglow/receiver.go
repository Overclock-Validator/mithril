package alpenglow

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/quic-go/quic-go"
)

const (
	VotorQUICALPN                 = "solana-tpu"
	DefaultReceiverBindAddr       = "0.0.0.0:0"
	DefaultReceiverMaxMessageSize = int64(DefaultMaxBitmapSize + 4096)
	DefaultReceiverLogInterval    = 10 * time.Second
)

type ReceiverConfig struct {
	BindAddr        string
	MaxMessageBytes int64
	Decode          DecodeOptions
	LogInterval     time.Duration
	OnMessage       func(Message)
	// SkipObserver prevents decoded-but-unverified network messages from being
	// retained by Observer. Consensus ingress sets this and observes only after
	// BLS/stake verification; standalone diagnostic receivers may leave it off.
	SkipObserver bool
}

func DefaultReceiverConfig() ReceiverConfig {
	return ReceiverConfig{
		BindAddr:        DefaultReceiverBindAddr,
		MaxMessageBytes: DefaultReceiverMaxMessageSize,
		Decode:          DefaultDecodeOptions(),
		LogInterval:     DefaultReceiverLogInterval,
	}
}

type ReceiverStats struct {
	ConnectionsAccepted uint64    `json:"connections_accepted"`
	StreamsReceived     uint64    `json:"streams_received"`
	MessagesDecoded     uint64    `json:"messages_decoded"`
	VotesDecoded        uint64    `json:"votes_decoded"`
	CertificatesDecoded uint64    `json:"certificates_decoded"`
	DecodeErrors        uint64    `json:"decode_errors"`
	ReadErrors          uint64    `json:"read_errors"`
	AcceptErrors        uint64    `json:"accept_errors"`
	OversizeMessages    uint64    `json:"oversize_messages"`
	LastMessageAt       time.Time `json:"last_message_at,omitempty"`
	LatestVoteSlot      uint64    `json:"latest_vote_slot,omitempty"`
	LatestCertSlot      uint64    `json:"latest_certificate_slot,omitempty"`
}

type Receiver struct {
	cfg      ReceiverConfig
	observer *Observer
	listener *quic.Listener

	mu     sync.Mutex
	closed bool
	conns  map[*quic.Conn]struct{}
	stats  ReceiverStats
}

func NewReceiver(cfg ReceiverConfig, observer *Observer) (*Receiver, error) {
	cfg = normalizeReceiverConfig(cfg)
	if observer == nil {
		observer = NewObserver()
	}

	cert, err := newVotorQUICCertificate()
	if err != nil {
		return nil, fmt.Errorf("create Alpenglow Votor QUIC certificate: %w", err)
	}

	listener, err := quic.ListenAddr(cfg.BindAddr, &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAnyClientCert,
		NextProtos:   []string{VotorQUICALPN},
		MinVersion:   tls.VersionTLS13,
	}, &quic.Config{
		HandshakeIdleTimeout: 3 * time.Second,
		MaxIdleTimeout:       30 * time.Second,
		KeepAlivePeriod:      time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("listen for Alpenglow Votor QUIC on %s: %w", cfg.BindAddr, err)
	}

	return &Receiver{
		cfg:      cfg,
		observer: observer,
		listener: listener,
		conns:    make(map[*quic.Conn]struct{}),
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

	mlog.Log.Infof("ALPENGLOW Votor receiver listening on %s (transport=quic alpn=%s max_message=%d)",
		r.listener.Addr(), VotorQUICALPN, r.cfg.MaxMessageBytes)

	for {
		conn, err := r.listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, quic.ErrServerClosed) || r.isClosed() {
				return nil
			}
			r.recordAcceptError()
			return fmt.Errorf("accept Alpenglow Votor QUIC connection: %w", err)
		}

		r.addConn(conn)
		r.recordConnection()
		go r.handleConn(runCtx, conn)
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

func (r *Receiver) handleConn(ctx context.Context, conn *quic.Conn) {
	defer r.removeConn(conn)
	defer conn.CloseWithError(0, "alpenglow receiver done")

	for {
		stream, err := conn.AcceptUniStream(ctx)
		if err != nil {
			if ctx.Err() != nil || conn.Context().Err() != nil || r.isClosed() {
				return
			}
			r.recordReadError()
			return
		}
		r.handleStream(stream)
	}
}

func (r *Receiver) handleStream(stream *quic.ReceiveStream) {
	r.recordStream()

	payload, err := io.ReadAll(io.LimitReader(stream, r.cfg.MaxMessageBytes+1))
	if err != nil {
		r.recordReadError()
		return
	}
	if int64(len(payload)) > r.cfg.MaxMessageBytes {
		r.recordOversizeMessage()
		return
	}

	msg, err := DecodeMessageWithOptions(payload, r.cfg.Decode)
	if err != nil {
		r.recordDecodeError()
		return
	}
	if !r.cfg.SkipObserver {
		if _, err := r.observer.ObserveMessage(msg); err != nil {
			r.recordDecodeError()
			return
		}
	}
	if r.cfg.OnMessage != nil {
		r.cfg.OnMessage(msg)
	}
	r.recordMessage(msg)
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
			mlog.Log.FileOnlyf("alpenglow Votor receiver stats: connections=%d streams=%d messages=%d votes=%d certificates=%d decode_errors=%d read_errors=%d accept_errors=%d oversize=%d latest_vote_slot=%d latest_certificate_slot=%d last_message=%s",
				stats.ConnectionsAccepted, stats.StreamsReceived, stats.MessagesDecoded, stats.VotesDecoded, stats.CertificatesDecoded,
				stats.DecodeErrors, stats.ReadErrors, stats.AcceptErrors, stats.OversizeMessages, stats.LatestVoteSlot, stats.LatestCertSlot,
				formatStatsTime(stats.LastMessageAt))
		}
	}
}

func (r *Receiver) recordConnection() {
	r.mu.Lock()
	r.stats.ConnectionsAccepted++
	r.mu.Unlock()
}

func (r *Receiver) recordStream() {
	r.mu.Lock()
	r.stats.StreamsReceived++
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

func (r *Receiver) addConn(conn *quic.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		_ = conn.CloseWithError(0, "alpenglow receiver closed")
		return
	}
	r.conns[conn] = struct{}{}
}

func (r *Receiver) removeConn(conn *quic.Conn) {
	r.mu.Lock()
	delete(r.conns, conn)
	r.mu.Unlock()
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
	return cfg
}

func newVotorQUICCertificate() (tls.Certificate, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "Mithril Alpenglow observer",
		},
		NotBefore:             time.Unix(0, 0),
		NotAfter:              time.Date(4096, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"localhost"},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		return tls.Certificate{}, err
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  priv,
		Leaf:        cert,
	}, nil
}

func formatStatsTime(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return time.Since(t).Truncate(time.Second).String() + " ago"
}
