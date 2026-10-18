package harmonic

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	authpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/auth"
	relayerpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/relayer"
	sharedpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/shared"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	tpupacket "github.com/Overclock-Validator/mithril/pkg/tpu/packet"
)

const (
	unaryRPCTimeout         = 10 * time.Second
	streamHeaderTimeout     = 10 * time.Second
	defaultConfigInterval   = 60 * time.Second
	defaultHeartbeatTimeout = 400 * time.Millisecond
	defaultMaxBackoff       = 5 * time.Second
	// quicPortOffset is where a relayer's QUIC sockets sit relative to the UDP
	// ports it reports.
	quicPortOffset = 6
)

var errHeartbeatTimeout = errors.New("relayer stream went quiet")

// PacketSink takes packets from a Harmonic stream into the TPU. It must not
// block.
type PacketSink interface {
	Submit(wire []byte, src tpupacket.Source) bool
}

// TPUConfig is a relayer's validated TPU UDP sockets.
type TPUConfig struct {
	TPU        netip.AddrPort
	TPUForward netip.AddrPort
}

// Sockets returns the gossip sockets that advertise the relayer: the UDP
// sockets as reported and the QUIC sockets 6 ports above them.
func (c TPUConfig) Sockets() gossip.TPUSockets {
	udp := func(ap netip.AddrPort, offset uint16) *net.UDPAddr {
		return net.UDPAddrFromAddrPort(netip.AddrPortFrom(ap.Addr(), ap.Port()+offset))
	}
	return gossip.TPUSockets{
		UDP:          udp(c.TPU, 0),
		QUIC:         udp(c.TPU, quicPortOffset),
		ForwardsUDP:  udp(c.TPUForward, 0),
		ForwardsQUIC: udp(c.TPUForward, quicPortOffset),
	}
}

// ParseTPUConfig validates a GetTpuConfigs response.
func ParseTPUConfig(resp *relayerpb.GetTpuConfigsResponse) (TPUConfig, error) {
	tpu, err := parseRelayerSocket(resp.GetTpu(), "tpu")
	if err != nil {
		return TPUConfig{}, err
	}
	forward, err := parseRelayerSocket(resp.GetTpuForward(), "tpu_forward")
	if err != nil {
		return TPUConfig{}, err
	}
	return TPUConfig{TPU: tpu, TPUForward: forward}, nil
}

func parseRelayerSocket(socket *sharedpb.Socket, name string) (netip.AddrPort, error) {
	if socket == nil {
		return netip.AddrPort{}, fmt.Errorf("relayer %s socket missing", name)
	}
	addr, err := netip.ParseAddr(socket.GetIp())
	if err != nil || !addr.Is4() || addr.IsUnspecified() {
		return netip.AddrPort{}, fmt.Errorf("relayer %s address %q is not a usable IPv4 address", name, socket.GetIp())
	}
	// The QUIC socket at port+6 must be a valid port too.
	if port := socket.GetPort(); port < 1 || port > 0xffff-quicPortOffset {
		return netip.AddrPort{}, fmt.Errorf("relayer %s port %d out of range", name, port)
	}
	return netip.AddrPortFrom(addr, uint16(socket.GetPort())), nil
}

// RelayerConfig configures a relayer session.
type RelayerConfig struct {
	Endpoint           Endpoint
	InsecureSkipVerify bool
	Identity           ed25519.PrivateKey
	Packets            PacketSink
	// OnStatus is called from the session goroutine with the relayer's TPU
	// config whenever it becomes usable or changes, and with nil whenever it
	// stops being usable.
	OnStatus func(*TPUConfig)
	// Zero values use the defaults: 60s, 400ms, 5s.
	ConfigInterval   time.Duration
	HeartbeatTimeout time.Duration
	MaxBackoff       time.Duration
	// Dialer replaces the TCP dialer, for tests.
	Dialer func(ctx context.Context, addr string) (net.Conn, error)
}

// RelayerStats counts relayer session activity.
type RelayerStats struct {
	Sessions          uint64
	Resets            uint64
	Heartbeats        uint64
	Batches           uint64
	Packets           uint64
	DroppedSize       uint64
	DroppedFull       uint64
	InvalidTPUConfigs uint64
}

// RelayerSession keeps an authenticated packet subscription to a relayer and
// feeds its packets into the TPU.
type RelayerSession struct {
	cfg   RelayerConfig
	stats struct {
		sessions, resets, heartbeats, batches, packets, droppedSize, droppedFull, invalidConfigs atomic.Uint64
	}
	statusMu sync.Mutex
	status   *TPUConfig
}

func NewRelayerSession(cfg RelayerConfig) *RelayerSession {
	if cfg.ConfigInterval <= 0 {
		cfg.ConfigInterval = defaultConfigInterval
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = defaultHeartbeatTimeout
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultMaxBackoff
	}
	return &RelayerSession{cfg: cfg}
}

func (r *RelayerSession) Stats() RelayerStats {
	return RelayerStats{
		Sessions:          r.stats.sessions.Load(),
		Resets:            r.stats.resets.Load(),
		Heartbeats:        r.stats.heartbeats.Load(),
		Batches:           r.stats.batches.Load(),
		Packets:           r.stats.packets.Load(),
		DroppedSize:       r.stats.droppedSize.Load(),
		DroppedFull:       r.stats.droppedFull.Load(),
		InvalidTPUConfigs: r.stats.invalidConfigs.Load(),
	}
}

// Status returns the relayer's TPU config while it is usable, else nil.
func (r *RelayerSession) Status() *TPUConfig {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()
	return r.status
}

// Run keeps a session open until ctx ends, reconnecting after jittered
// backoff whenever it fails.
func (r *RelayerSession) Run(ctx context.Context) {
	for ctx.Err() == nil {
		err := r.runSession(ctx)
		r.setStatus(nil)
		if ctx.Err() != nil {
			return
		}
		r.stats.resets.Add(1)
		mlog.Log.Infof("Harmonic relayer %s session reset: %v", r.cfg.Endpoint.Target, err)
		select {
		case <-ctx.Done():
		case <-time.After(jitteredBackoff(r.cfg.MaxBackoff)):
		}
	}
}

// jitteredBackoff waits between half of max and max, so reconnects spread
// out but never exceed max.
func jitteredBackoff(max time.Duration) time.Duration {
	return max/2 + rand.N(max/2+1)
}

func (r *RelayerSession) setStatus(cfg *TPUConfig) {
	r.statusMu.Lock()
	changed := (r.status == nil) != (cfg == nil) || (cfg != nil && *r.status != *cfg)
	r.status = cfg
	r.statusMu.Unlock()
	if changed && r.cfg.OnStatus != nil {
		r.cfg.OnStatus(cfg)
	}
}

func (r *RelayerSession) runSession(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	tokens := NewTokenSource(nil, r.cfg.Identity)
	conn, err := Dial(ctx, DialConfig{
		Endpoint:           r.cfg.Endpoint,
		InsecureSkipVerify: r.cfg.InsecureSkipVerify,
		Credentials:        tokens.Bearer(),
		Dialer:             r.cfg.Dialer,
	})
	if err != nil {
		return err
	}
	defer conn.Close()
	tokens.client = authpb.NewAuthServiceClient(conn)
	if err := tokens.Authenticate(ctx); err != nil {
		return err
	}
	r.stats.sessions.Add(1)
	errs := make(chan error, 2)
	go func() { errs <- tokens.Run(ctx) }()

	client := relayerpb.NewRelayerClient(conn)
	config, configErr := r.fetchTPUConfig(ctx, client)
	if IsAuthError(configErr) {
		return configErr
	}
	stream, err := subscribePackets(ctx, client)
	if err != nil {
		return err
	}
	beats := make(chan struct{}, 1)
	go func() { errs <- r.readPackets(stream, beats) }()

	if configErr == nil {
		r.setStatus(&config)
	}
	watchdog := time.NewTimer(r.cfg.HeartbeatTimeout)
	defer watchdog.Stop()
	refresh := time.NewTicker(r.cfg.ConfigInterval)
	defer refresh.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errs:
			return err
		case <-beats:
			watchdog.Reset(r.cfg.HeartbeatTimeout)
		case <-watchdog.C:
			return errHeartbeatTimeout
		case <-refresh.C:
			config, err := r.fetchTPUConfig(ctx, client)
			if IsAuthError(err) {
				return err
			}
			if err != nil {
				r.setStatus(nil)
				continue
			}
			r.setStatus(&config)
		}
	}
}

// fetchTPUConfig returns the relayer's TPU config. An invalid config makes
// the relayer unusable for advertisement but does not end the session.
func (r *RelayerSession) fetchTPUConfig(ctx context.Context, client relayerpb.RelayerClient) (TPUConfig, error) {
	ctx, cancel := context.WithTimeout(ctx, unaryRPCTimeout)
	defer cancel()
	resp, err := client.GetTpuConfigs(ctx, &relayerpb.GetTpuConfigsRequest{})
	if err != nil {
		return TPUConfig{}, fmt.Errorf("get TPU configs: %w", err)
	}
	config, err := ParseTPUConfig(resp)
	if err != nil {
		r.stats.invalidConfigs.Add(1)
		mlog.Log.Infof("Harmonic relayer %s: %v", r.cfg.Endpoint.Target, err)
	}
	return config, err
}

// subscribePackets opens the packet stream and waits, for at most 10s, for
// its response headers; the stream itself has no deadline.
func subscribePackets(ctx context.Context, client relayerpb.RelayerClient) (relayerpb.Relayer_SubscribePacketsClient, error) {
	stream, err := client.SubscribePackets(ctx, &relayerpb.SubscribePacketsRequest{})
	if err != nil {
		return nil, fmt.Errorf("subscribe packets: %w", err)
	}
	headers := make(chan error, 1)
	go func() {
		_, err := stream.Header()
		headers <- err
	}()
	select {
	case err := <-headers:
		if err != nil {
			return nil, fmt.Errorf("subscribe packets: %w", err)
		}
		return stream, nil
	case <-time.After(streamHeaderTimeout):
		return nil, errors.New("subscribe packets: no response headers within 10s")
	}
}

func (r *RelayerSession) readPackets(stream relayerpb.Relayer_SubscribePacketsClient, beats chan<- struct{}) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("packet stream: %w", err)
		}
		select {
		case beats <- struct{}{}:
		default:
		}
		switch m := msg.GetMsg().(type) {
		case *relayerpb.SubscribePacketsResponse_Heartbeat:
			r.stats.heartbeats.Add(1)
		case *relayerpb.SubscribePacketsResponse_Batch:
			r.stats.batches.Add(1)
			for _, pkt := range m.Batch.GetPackets() {
				data := pkt.GetData()
				if len(data) == 0 || len(data) > tpupacket.DataSize {
					r.stats.droppedSize.Add(1)
					continue
				}
				r.stats.packets.Add(1)
				if !r.cfg.Packets.Submit(data, tpupacket.SourceRelayer) {
					r.stats.droppedFull.Add(1)
				}
			}
		}
	}
}

// TPUAdvertiser publishes the TPU sockets in gossip.
type TPUAdvertiser interface {
	SetTPUSockets(gossip.TPUSockets) error
}

// TPUAdvertisement chooses the TPU sockets gossip advertises: the relayer's
// while it is usable and the block engine session is healthy, since relayed
// packets are only worth attracting while the validator can schedule them,
// and this validator's own otherwise.
type TPUAdvertisement struct {
	gossip TPUAdvertiser
	local  gossip.TPUSockets

	mu                sync.Mutex
	relayer           *TPUConfig
	blockEngineHealth bool
	advertised        *TPUConfig
	initialized       bool
}

func NewTPUAdvertisement(g TPUAdvertiser, local gossip.TPUSockets) *TPUAdvertisement {
	return &TPUAdvertisement{gossip: g, local: local}
}

// SetRelayer records the relayer's usable TPU config, or nil.
func (a *TPUAdvertisement) SetRelayer(cfg *TPUConfig) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.relayer = cfg
	return a.applyLocked()
}

// SetBlockEngineHealthy records whether the block engine session is healthy.
func (a *TPUAdvertisement) SetBlockEngineHealthy(healthy bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.blockEngineHealth = healthy
	return a.applyLocked()
}

func (a *TPUAdvertisement) applyLocked() error {
	var want *TPUConfig
	if a.relayer != nil && a.blockEngineHealth {
		want = a.relayer
	}
	if a.initialized && (want == nil) == (a.advertised == nil) && (want == nil || *want == *a.advertised) {
		return nil
	}
	sockets := a.local
	if want != nil {
		sockets = want.Sockets()
	}
	if err := a.gossip.SetTPUSockets(sockets); err != nil {
		return err
	}
	a.advertised, a.initialized = want, true
	return nil
}
