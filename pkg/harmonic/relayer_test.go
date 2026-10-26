package harmonic

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	authpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/auth"
	packetpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/packet"
	relayerpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/relayer"
	sharedpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/shared"
	tpupacket "github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func tpuConfigResponse(ip string, tpuPort, forwardPort int64) *relayerpb.GetTpuConfigsResponse {
	return &relayerpb.GetTpuConfigsResponse{
		Tpu:        &sharedpb.Socket{Ip: ip, Port: tpuPort},
		TpuForward: &sharedpb.Socket{Ip: ip, Port: forwardPort},
	}
}

func TestParseTPUConfig(t *testing.T) {
	got, err := ParseTPUConfig(tpuConfigResponse("198.51.100.7", 11222, 11223))
	require.NoError(t, err)
	assert.Equal(t, netip.MustParseAddrPort("198.51.100.7:11222"), got.TPU)
	assert.Equal(t, netip.MustParseAddrPort("198.51.100.7:11223"), got.TPUForward)
	sockets := got.Sockets()
	assert.Equal(t, "198.51.100.7:11222", sockets.UDP.String())
	assert.Equal(t, "198.51.100.7:11228", sockets.QUIC.String(), "QUIC is 6 ports above UDP")
	assert.Equal(t, "198.51.100.7:11223", sockets.ForwardsUDP.String())
	assert.Equal(t, "198.51.100.7:11229", sockets.ForwardsQUIC.String())

	_, err = ParseTPUConfig(tpuConfigResponse("198.51.100.7", 65529, 1))
	assert.NoError(t, err, "the highest port whose QUIC socket still fits")

	for name, resp := range map[string]*relayerpb.GetTpuConfigsResponse{
		"empty":            {},
		"missing forward":  {Tpu: &sharedpb.Socket{Ip: "198.51.100.7", Port: 11222}},
		"unspecified":      tpuConfigResponse("0.0.0.0", 11222, 11223),
		"IPv6":             tpuConfigResponse("2001:db8::1", 11222, 11223),
		"IPv4-mapped IPv6": tpuConfigResponse("::ffff:198.51.100.7", 11222, 11223),
		"not an address":   tpuConfigResponse("relayer.example.com", 11222, 11223),
		"zero port":        tpuConfigResponse("198.51.100.7", 0, 11223),
		"negative port":    tpuConfigResponse("198.51.100.7", -1, 11223),
		"QUIC overflow":    tpuConfigResponse("198.51.100.7", 65530, 11223),
	} {
		_, err := ParseTPUConfig(resp)
		assert.Errorf(t, err, "%s", name)
	}
}

func TestJitteredBackoffStaysWithinBound(t *testing.T) {
	for range 1000 {
		d := jitteredBackoff(5 * time.Second)
		require.GreaterOrEqual(t, d, 2500*time.Millisecond)
		require.LessOrEqual(t, d, 5*time.Second)
	}
}

type packetRecorder struct {
	mu      sync.Mutex
	packets [][]byte
	sources []tpupacket.Source
	refuse  bool
}

func (p *packetRecorder) Submit(wire []byte, src tpupacket.Source) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.packets = append(p.packets, append([]byte(nil), wire...))
	p.sources = append(p.sources, src)
	return !p.refuse
}

func (p *packetRecorder) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.packets)
}

type statusRecorder struct {
	mu       sync.Mutex
	statuses []*TPUConfig
}

func (s *statusRecorder) record(cfg *TPUConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses = append(s.statuses, cfg)
}

func (s *statusRecorder) all() []*TPUConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*TPUConfig(nil), s.statuses...)
}

func (s *statusRecorder) last() *TPUConfig {
	all := s.all()
	if len(all) == 0 {
		return nil
	}
	return all[len(all)-1]
}

type relayerHarness struct {
	relayer  *mockRelayer
	session  *RelayerSession
	packets  *packetRecorder
	statuses *statusRecorder
}

func startRelayerSession(t *testing.T, config *relayerpb.GetTpuConfigsResponse, configInterval time.Duration) *relayerHarness {
	t.Helper()
	h := &relayerHarness{relayer: newMockRelayer(config), packets: &packetRecorder{}, statuses: &statusRecorder{}}
	dial := serveBufconn(t, func(s *grpc.Server) {
		authpb.RegisterAuthServiceServer(s, newMockAuth(time.Now))
		relayerpb.RegisterRelayerServer(s, h.relayer)
	})
	h.session = NewRelayerSession(RelayerConfig{
		Endpoint:         dial.Endpoint,
		Dialer:           dial.Dialer,
		Identity:         identityHolder(t, rfc8032Key(t)),
		Packets:          h.packets,
		OnStatus:         h.statuses.record,
		ConfigInterval:   configInterval,
		HeartbeatTimeout: 150 * time.Millisecond,
		MaxBackoff:       20 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.session.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return h
}

func batch(packets ...[]byte) *relayerpb.SubscribePacketsResponse {
	pb := &packetpb.PacketBatch{}
	for _, data := range packets {
		pb.Packets = append(pb.Packets, &packetpb.Packet{Data: data})
	}
	return &relayerpb.SubscribePacketsResponse{Msg: &relayerpb.SubscribePacketsResponse_Batch{Batch: pb}}
}

func TestRelayerSessionDeliversPacketsAndReportsConfig(t *testing.T) {
	h := startRelayerSession(t, tpuConfigResponse("198.51.100.7", 11222, 11223), time.Hour)
	require.Eventually(t, func() bool { return h.session.Status() != nil }, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, netip.MustParseAddrPort("198.51.100.7:11222"), h.session.Status().TPU)
	assert.Equal(t, "Bearer access-1", h.relayer.last().Get("authorization")[0])

	h.relayer.send <- batch([]byte{1, 2, 3}, nil, make([]byte, tpupacket.DataSize+1), make([]byte, tpupacket.DataSize))
	require.Eventually(t, func() bool { return h.packets.count() == 2 }, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, []byte{1, 2, 3}, h.packets.packets[0])
	assert.Equal(t, []tpupacket.Source{tpupacket.SourceRelayer, tpupacket.SourceRelayer}, h.packets.sources)

	// Heartbeats keep a quiet stream alive across several watchdog periods.
	time.Sleep(600 * time.Millisecond)
	stats := h.session.Stats()
	assert.Equal(t, uint64(1), stats.Sessions)
	assert.Zero(t, stats.Resets)
	assert.Equal(t, uint64(1), stats.Batches)
	assert.Equal(t, uint64(2), stats.Packets)
	assert.Equal(t, uint64(2), stats.DroppedSize, "empty and oversized packets are dropped one by one")
	assert.NotZero(t, stats.Heartbeats)
	assert.Len(t, h.statuses.all(), 1, "the status only changes once")
}

func TestRelayerSessionResetsWhenTheStreamGoesQuiet(t *testing.T) {
	h := startRelayerSession(t, tpuConfigResponse("198.51.100.7", 11222, 11223), time.Hour)
	require.Eventually(t, func() bool { return h.session.Status() != nil }, 2*time.Second, 5*time.Millisecond)

	h.relayer.stall.Store(true)
	require.Eventually(t, func() bool { return h.session.Status() == nil }, 2*time.Second, 5*time.Millisecond)
	assert.NotZero(t, h.session.Stats().Resets)
	h.relayer.stall.Store(false)
	require.Eventually(t, func() bool { return h.session.Status() != nil }, 2*time.Second, 5*time.Millisecond)
	assert.GreaterOrEqual(t, h.relayer.subscriptions.Load(), int32(2), "a new session subscribed again")
	statuses := h.statuses.all()
	require.GreaterOrEqual(t, len(statuses), 3)
	assert.NotNil(t, statuses[0])
	assert.Nil(t, statuses[1], "the relayer stops being advertised while it is down")
}

func TestRelayerSessionResetsWhenTheStreamFails(t *testing.T) {
	h := startRelayerSession(t, tpuConfigResponse("198.51.100.7", 11222, 11223), time.Hour)
	require.Eventually(t, func() bool { return h.session.Status() != nil }, 2*time.Second, 5*time.Millisecond)
	h.relayer.endStream <- status.Error(codes.Unavailable, "relayer restarting")
	require.Eventually(t, func() bool { return h.session.Stats().Sessions == 2 }, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, uint64(1), h.session.Stats().Resets)
}

func TestRelayerSessionNeverReportsAnInvalidConfig(t *testing.T) {
	h := startRelayerSession(t, tpuConfigResponse("0.0.0.0", 11222, 11223), 50*time.Millisecond)
	require.Eventually(t, func() bool { return h.session.Stats().InvalidTPUConfigs >= 2 }, 2*time.Second, 5*time.Millisecond)
	assert.Nil(t, h.session.Status())
	assert.Empty(t, h.statuses.all())

	// Packets still flow while the config is unusable.
	h.relayer.send <- batch([]byte{9})
	require.Eventually(t, func() bool { return h.packets.count() == 1 }, 2*time.Second, 5*time.Millisecond)

	h.relayer.setConfig(tpuConfigResponse("198.51.100.8", 12000, 12001))
	require.Eventually(t, func() bool { return h.session.Status() != nil }, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, netip.MustParseAddrPort("198.51.100.8:12000"), h.statuses.last().TPU)

	h.relayer.setConfig(tpuConfigResponse("198.51.100.9", 12000, 12001))
	require.Eventually(t, func() bool {
		last := h.statuses.last()
		return last != nil && last.TPU.Addr() == netip.MustParseAddr("198.51.100.9")
	}, 2*time.Second, 5*time.Millisecond, "an address change is reported")
	assert.Zero(t, h.session.Stats().Resets, "a config change does not reset the session")
}

type advertiserRecorder struct {
	calls []gossip.TPUSockets
	err   error
}

func (a *advertiserRecorder) SetTPUSockets(s gossip.TPUSockets) error {
	if a.err != nil {
		return a.err
	}
	a.calls = append(a.calls, s)
	return nil
}

func TestTPUAdvertisementAdvertisesTheRelayerOnlyWhileUsable(t *testing.T) {
	local := gossip.TPUSockets{QUIC: netipUDP("203.0.113.10:8009"), ForwardsQUIC: netipUDP("203.0.113.10:8009")}
	relayerA, err := ParseTPUConfig(tpuConfigResponse("198.51.100.7", 11222, 11223))
	require.NoError(t, err)
	relayerB, err := ParseTPUConfig(tpuConfigResponse("198.51.100.8", 11222, 11223))
	require.NoError(t, err)
	gossipRec := &advertiserRecorder{}
	ad := NewTPUAdvertisement(gossipRec, local)

	require.NoError(t, ad.SetBlockEngineHealthy(false))
	require.NoError(t, ad.SetRelayer(&relayerA))
	require.Len(t, gossipRec.calls, 1, "without a healthy block engine the local TPU stays advertised")
	assert.Equal(t, local, gossipRec.calls[0])

	require.NoError(t, ad.SetBlockEngineHealthy(true))
	require.Len(t, gossipRec.calls, 2)
	assert.Equal(t, relayerA.Sockets(), gossipRec.calls[1])
	require.NoError(t, ad.SetBlockEngineHealthy(true))
	require.NoError(t, ad.SetRelayer(&relayerA))
	assert.Len(t, gossipRec.calls, 2, "an unchanged choice is not re-published")

	require.NoError(t, ad.SetRelayer(&relayerB))
	require.Len(t, gossipRec.calls, 3)
	assert.Equal(t, relayerB.Sockets(), gossipRec.calls[2])

	require.NoError(t, ad.SetRelayer(nil))
	require.Len(t, gossipRec.calls, 4)
	assert.Equal(t, local, gossipRec.calls[3], "a lost relayer reverts to the local TPU")
	require.NoError(t, ad.SetBlockEngineHealthy(false))
	assert.Len(t, gossipRec.calls, 4)

	// A failed publish is retried on the next update.
	gossipRec.err = errors.New("gossip not running")
	require.NoError(t, ad.SetBlockEngineHealthy(true))
	assert.Error(t, ad.SetRelayer(&relayerA))
	gossipRec.err = nil
	require.NoError(t, ad.SetRelayer(&relayerA))
	require.Len(t, gossipRec.calls, 5)
	assert.Equal(t, relayerA.Sockets(), gossipRec.calls[4])
}

func netipUDP(s string) *net.UDPAddr {
	return net.UDPAddrFromAddrPort(netip.MustParseAddrPort(s))
}
