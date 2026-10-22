package tpu

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net"
	"runtime"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/pipeline"
	"github.com/Overclock-Validator/mithril/pkg/tpu/quicserver"
	"github.com/Overclock-Validator/mithril/pkg/tpu/sink"
)

// Config wires the TPU ingress stack: QUIC -> dedup/sanitize -> sigverify -> sink.
// Additional downstream stages can be attached via Sink or composed around TPU later.
type Config struct {
	Name         string
	ListenAddr   string
	AdvertisedIP string
	Identity     ed25519.PrivateKey
	QUIC         quicserver.ServerConfig
	Pipeline     pipeline.Config
}

// Stats aggregates QUIC server and pipeline counters.
type Stats struct {
	QUIC     *quicserver.ServerStats
	Pipeline pipeline.Stats
}

// TPU owns the QUIC ingress server and ingress processing pipeline.
type TPU struct {
	cfg      Config
	conn     *net.UDPConn
	pipeline *pipeline.Pipeline
	server   *quicserver.SpawnServerResult
	cancel   context.CancelFunc
}

// DefaultConfig returns defaults suitable for a validator TPU listener.
func DefaultConfig() Config {
	return Config{
		Name:       "mithril-tpu",
		ListenAddr: "0.0.0.0:0",
		QUIC:       quicserver.DefaultServerConfig(),
		Pipeline: pipeline.Config{
			SigverifyWorkers: runtime.GOMAXPROCS(0),
			Sink:             &sink.Noop{},
		},
	}
}

func (c Config) normalized() Config {
	out := c
	if out.Name == "" {
		out.Name = "mithril-tpu"
	}
	if out.ListenAddr == "" {
		out.ListenAddr = "0.0.0.0:0"
	}
	if out.Pipeline.Sink == nil {
		out.Pipeline.Sink = &sink.Noop{}
	}
	return out
}

// Start launches the TPU QUIC server wired into the ingress pipeline.
func Start(ctx context.Context, cfg Config) (*TPU, error) {
	cfg = cfg.normalized()
	if len(cfg.Identity) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid TPU identity key size %d", len(cfg.Identity))
	}

	udpAddr, err := net.ResolveUDPAddr("udp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve TPU listen address %q: %w", cfg.ListenAddr, err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("listen TPU udp %q: %w", cfg.ListenAddr, err)
	}

	pl, ingress := pipeline.Start(ctx, cfg.Pipeline)
	serverCfg := cfg.QUIC
	serverCfg.Ingress = ingress
	serverCfg.IngressStats = pl.IngressStats()

	runCtx, cancel := context.WithCancel(ctx)
	result, err := quicserver.Spawn(runCtx, cfg.Name, []quicserver.QuicSocket{quicserver.QuicSocketFromUDP(conn)}, cfg.Identity, serverCfg)
	if err != nil {
		cancel()
		pl.Stop()
		conn.Close()
		return nil, err
	}

	return &TPU{
		cfg:      cfg,
		conn:     conn,
		pipeline: pl,
		server:   result,
		cancel:   cancel,
	}, nil
}

func (t *TPU) Identity() ed25519.PrivateKey {
	return append(ed25519.PrivateKey(nil), t.cfg.Identity...)
}

func (t *TPU) ListenAddr() *net.UDPAddr {
	if t.conn == nil {
		return nil
	}
	return t.conn.LocalAddr().(*net.UDPAddr)
}

// AdvertisedQUICAddr returns the UDP endpoint to publish in gossip for TPU QUIC.
func (t *TPU) AdvertisedQUICAddr() (*net.UDPAddr, error) {
	local := t.ListenAddr()
	if local == nil {
		return nil, fmt.Errorf("TPU is not listening")
	}
	ip := local.IP
	if t.cfg.AdvertisedIP != "" {
		parsed := net.ParseIP(t.cfg.AdvertisedIP)
		if parsed == nil {
			return nil, fmt.Errorf("invalid advertised IP %q", t.cfg.AdvertisedIP)
		}
		ip = parsed
	}
	if ip == nil || ip.IsUnspecified() {
		return nil, fmt.Errorf("could not determine advertised TPU IP")
	}
	return &net.UDPAddr{IP: ip, Port: local.Port}, nil
}

// Submit copies a transaction from outside the QUIC listener, such as a
// relayer, into the ingress pipeline. It never blocks, and returns false if
// the packet was dropped because ingress is full or the TPU has stopped.
func (t *TPU) Submit(wire []byte, src packet.Source) bool {
	if t == nil || t.pipeline == nil {
		return false
	}
	return t.pipeline.Submit(packet.OwnedFrom(wire, src))
}

func (t *TPU) Stats() Stats {
	return Stats{
		QUIC:     t.server.Stats,
		Pipeline: t.pipeline.Stats(),
	}
}

// Stop shuts down the ingress pipeline and QUIC listeners.
func (t *TPU) Stop(ctx context.Context) error {
	if t == nil {
		return nil
	}
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}
	if t.cancel != nil {
		t.cancel()
	}
	var closeErr error
	if t.server != nil {
		closeErr = t.server.Close(ctx)
	}
	// The server must stop producing before ingress is closed. Closing the
	// pipeline first lets a final QUIC handler send on a closed channel.
	if t.pipeline != nil {
		t.pipeline.Stop()
	}
	if t.conn != nil {
		_ = t.conn.Close()
	}
	return closeErr
}
