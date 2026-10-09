package quicserver

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/pipeline"
	"github.com/quic-go/quic-go"
)

// ServerError is returned when the QUIC server fails to start.
type ServerError struct {
	Op  string
	Err error
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("tpu server %s: %v", e.Op, e.Err)
}

func (e *ServerError) Unwrap() error {
	return e.Err
}

// SpawnServerResult mirrors agave's streamer::quic::SpawnServerResult.
type SpawnServerResult struct {
	Listeners  []*quic.Listener
	Stats      *ServerStats
	Done       <-chan struct{}
	KeyUpdater KeyUpdater
	cancel     context.CancelFunc
}

// KeyUpdater mirrors agave's streamer::quic::EndpointKeyUpdater.
type KeyUpdater interface {
	UpdateKey(identity ed25519.PrivateKey) error
}

type endpointKeyUpdater struct {
	cfg ServerConfig
}

func (u *endpointKeyUpdater) UpdateKey(identity ed25519.PrivateKey) error {
	// quic-go listeners do not support in-place TLS rotation; validate the new
	// identity and require callers to re-spawn the server to apply it.
	_, err := newServerTLSConfig(identity)
	return err
}

// Spawn starts a bounded QUIC TPU server on the provided sockets.
func Spawn(
	ctx context.Context,
	name string,
	sockets []QuicSocket,
	identity ed25519.PrivateKey,
	cfg ServerConfig,
) (*SpawnServerResult, error) {
	if len(sockets) == 0 {
		return nil, &ServerError{Op: "spawn", Err: errors.New("no sockets provided")}
	}

	cfg = cfg.normalized()
	if cfg.MaxStreamDataBytes > packet.DataSize {
		return nil, &ServerError{Op: "configure transport", Err: fmt.Errorf("max stream data bytes %d exceeds packet buffer size %d", cfg.MaxStreamDataBytes, packet.DataSize)}
	}
	if cfg.StreamReceiveWindowSize < cfg.MaxStreamDataBytes {
		return nil, &ServerError{Op: "configure transport", Err: fmt.Errorf("stream receive window %d is smaller than max stream data bytes %d", cfg.StreamReceiveWindowSize, cfg.MaxStreamDataBytes)}
	}
	tlsConf, err := newServerTLSConfig(identity)
	if err != nil {
		return nil, &ServerError{Op: "configure tls", Err: err}
	}

	stats := &ServerStats{}
	stats.QuicEndpointsCount.Store(uint64(len(sockets)))

	quicConf := quicConfig(cfg)
	gate := newConnectionGate(cfg.MaxConnections)
	perIPGate := newPerIPGate(cfg.MaxConnectionsPerIP)
	pool := packet.NewPool(cfg.maxStreamSlots())

	listeners := make([]*quic.Listener, 0, len(sockets))
	for _, sock := range sockets {
		ln, err := quic.Listen(sock.UDPConn(), tlsConf, quicConf)
		if err != nil {
			for _, existing := range listeners {
				_ = existing.Close()
			}
			return nil, &ServerError{Op: "listen", Err: err}
		}
		listeners = append(listeners, ln)
	}

	updater := &endpointKeyUpdater{cfg: cfg}

	done := make(chan struct{})
	srv := &runtimeServer{
		name:      name,
		listeners: listeners,
		stats:     stats,
		cfg:       cfg,
		gate:      gate,
		perIPGate: perIPGate,
		pool:      pool,
		done:      done,
	}

	runCtx, cancel := context.WithCancel(ctx)
	go srv.run(runCtx)

	return &SpawnServerResult{
		Listeners:  listeners,
		Stats:      stats,
		Done:       done,
		KeyUpdater: updater,
		cancel:     cancel,
	}, nil
}

func quicConfig(cfg ServerConfig) *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout:           DefaultHandshakeTimeout,
		MaxIdleTimeout:                 QuicMaxTimeout,
		MaxIncomingStreams:             0,
		MaxIncomingUniStreams:          int64(cfg.MaxStreamsPerConnection),
		InitialStreamReceiveWindow:     uint64(cfg.StreamReceiveWindowSize),
		MaxStreamReceiveWindow:         uint64(cfg.StreamReceiveWindowSize),
		InitialConnectionReceiveWindow: DefaultConnectionRecvWindow,
		MaxConnectionReceiveWindow:     DefaultConnectionRecvWindow,
	}
}

type runtimeServer struct {
	name      string
	listeners []*quic.Listener
	stats     *ServerStats
	cfg       ServerConfig
	gate      *connectionGate
	perIPGate *perIPGate
	pool      *packet.Pool
	done      chan struct{}
}

func (s *runtimeServer) run(ctx context.Context) {
	defer close(s.done)

	var acceptWG sync.WaitGroup
	var connectionWG sync.WaitGroup
	for _, ln := range s.listeners {
		acceptWG.Add(1)
		go func(listener *quic.Listener) {
			defer acceptWG.Done()
			s.acceptLoop(ctx, listener, &connectionWG)
		}(ln)
	}

	<-ctx.Done()
	for _, ln := range s.listeners {
		_ = ln.Close()
	}
	acceptWG.Wait()
	connectionWG.Wait()
}

func (s *runtimeServer) acceptLoop(ctx context.Context, listener *quic.Listener, connectionWG *sync.WaitGroup) {
	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, quic.ErrServerClosed) {
				return
			}
			s.stats.ConnectionSetupError.Add(1)
			continue
		}

		if !s.gate.tryAcquire() {
			s.stats.RefusedTooManyOpen.Add(1)
			_ = conn.CloseWithError(0, "too many connections")
			continue
		}

		remote, ok := netipFromAddr(conn.RemoteAddr())
		if !ok || !s.perIPGate.tryAcquire(remote.Addr()) {
			s.gate.release()
			s.stats.RefusedPerIP.Add(1)
			_ = conn.CloseWithError(0, "too many connections per ip")
			continue
		}

		s.stats.TotalNewConnections.Add(1)
		connectionWG.Add(1)
		go func() {
			defer connectionWG.Done()
			s.handleConnection(ctx, conn, remote.Addr())
		}()
	}
}

func (s *runtimeServer) handleConnection(ctx context.Context, conn *quic.Conn, remoteIP netip.Addr) {
	defer func() {
		s.perIPGate.release(remoteIP)
		s.gate.release()
		s.stats.ConnectionRemoved.Add(1)
		s.stats.OpenConnections.Add(^uint64(0))
		_ = conn.CloseWithError(0, "closed")
	}()

	s.stats.TotalConnections.Add(1)
	s.stats.OpenConnections.Add(1)

	for {
		if ctx.Err() != nil {
			return
		}

		stream, err := conn.AcceptUniStream(ctx)
		if err != nil {
			if ctx.Err() == nil {
				s.stats.ConnectionSetupError.Add(1)
			}
			return
		}

		s.stats.ActiveStreams.Add(1)
		s.stats.TotalNewStreams.Add(1)

		buf, idx, ok := s.pool.Acquire()
		if !ok {
			s.stats.ConnectionAddFailed.Add(1)
			stream.CancelRead(0)
			s.stats.ActiveStreams.Add(^uint64(0))
			continue
		}

		n, readErr := readUniStream(stream, buf, s.cfg.WaitForChunkTimeout, s.cfg.MaxStreamDataBytes)
		if readErr != nil {
			switch {
			case errors.Is(readErr, errStreamTooLarge):
				s.stats.InvalidStreamSize.Add(1)
			case errors.Is(readErr, errReadTimeout):
				s.stats.TotalStreamReadTimeout.Add(1)
			default:
				s.stats.TotalStreamReadErrors.Add(1)
			}
			stream.CancelRead(0)
			s.pool.Release(idx)
			s.stats.ActiveStreams.Add(^uint64(0))
			continue
		}

		pkt := packet.FromPool(s.pool, buf, n, idx)
		if s.cfg.Ingress != nil {
			pipeline.TryEnqueue(s.cfg.Ingress, pkt, s.cfg.IngressStats)
		} else {
			pkt.Release()
		}
		s.stats.ActiveStreams.Add(^uint64(0))
	}
}

var (
	errStreamTooLarge = errors.New("stream exceeds max data bytes")
	errReadTimeout    = errors.New("stream read timeout")
)

func readUniStream(stream *quic.ReceiveStream, buf []byte, timeout time.Duration, maxBytes uint32) (int, error) {
	deadline := time.Now().Add(timeout)
	total := 0
	limit := int(maxBytes)
	if limit > len(buf) {
		limit = len(buf)
	}

	for total < limit {
		if err := stream.SetReadDeadline(deadline); err != nil {
			return total, err
		}
		n, err := stream.Read(buf[total:limit])
		total += n
		if err != nil {
			if err == io.EOF {
				return total, nil
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return total, errReadTimeout
			}
			return total, err
		}
		if n == 0 {
			return total, errReadTimeout
		}
	}

	if total >= limit {
		// Drain one byte to detect overflow.
		var scratch [1]byte
		if err := stream.SetReadDeadline(deadline); err != nil {
			return total, err
		}
		n, err := stream.Read(scratch[:])
		if n > 0 {
			return total, errStreamTooLarge
		}
		if err != nil && err != io.EOF {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return total, errReadTimeout
			}
			return total, err
		}
	}

	return total, nil
}

func netipFromAddr(addr net.Addr) (netip.AddrPort, bool) {
	host, portStr, err := net.SplitHostPort(addr.String())
	if err != nil {
		return netip.AddrPort{}, false
	}
	addrPort, err := netip.ParseAddrPort(net.JoinHostPort(host, portStr))
	if err != nil {
		return netip.AddrPort{}, false
	}
	return addrPort, true
}

// Close shuts down listeners and waits for the server goroutines to exit.
func (r *SpawnServerResult) Close(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	for _, ln := range r.Listeners {
		_ = ln.Close()
	}
	select {
	case <-r.Done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
