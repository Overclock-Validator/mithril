package harmonic

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	authpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/auth"
	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
	relayerpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/relayer"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// gossipRecorder records what the service advertises.
type gossipRecorder struct {
	mu        sync.Mutex
	sockets   []string
	clientIDs []uint16
}

func socketsString(s gossip.TPUSockets) string {
	return fmt.Sprintf("udp=%v quic=%v fwd_udp=%v fwd_quic=%v", s.UDP, s.QUIC, s.ForwardsUDP, s.ForwardsQUIC)
}

func (g *gossipRecorder) SetTPUSockets(s gossip.TPUSockets) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sockets = append(g.sockets, socketsString(s))
	return nil
}

func (g *gossipRecorder) SetClientID(id uint16) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.clientIDs = append(g.clientIDs, id)
	return nil
}

func (g *gossipRecorder) advertisedSockets() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.sockets) == 0 {
		return ""
	}
	return g.sockets[len(g.sockets)-1]
}

func (g *gossipRecorder) clientID() uint16 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.clientIDs) == 0 {
		return gossip.ClientIDMithril
	}
	return g.clientIDs[len(g.clientIDs)-1]
}

var serviceLocalTPU = gossip.TPUSockets{QUIC: netipUDP("203.0.113.10:8009"), ForwardsQUIC: netipUDP("203.0.113.10:8009")}

type serviceHarness struct {
	be      *mockBlockEngine
	gossip  *gossipRecorder
	service *Service
	crank   *TipCrank
	stop    func()
}

// startService runs a service against a mock block engine and relayer.
func startService(t *testing.T) *serviceHarness {
	t.Helper()
	h := &serviceHarness{be: newMockBlockEngine(), gossip: &gossipRecorder{}}
	dial := serveBufconn(t, func(s *grpc.Server) {
		authpb.RegisterAuthServiceServer(s, newMockAuth(time.Now))
		blockenginepb.RegisterBlockEngineValidatorServer(s, h.be)
		relayerpb.RegisterRelayerServer(s, newMockRelayer(tpuConfigResponse("198.51.100.7", 11222, 11223)))
	})
	tips := TipConfig{
		TipPaymentProgram:         mainnetTipPayment,
		TipDistributionProgram:    mainnetTipDistribution,
		MerkleRootUploadAuthority: majorMerkleAuthority,
		VoteAccount:               majorVote,
		Identity:                  solana.PrivateKey(rfc8032Key(t)),
	}
	var err error
	h.crank, err = NewTipCrank(tips)
	require.NoError(t, err)
	h.service, err = NewService(ServiceConfig{
		BlockEngine: dial.Endpoint,
		Relayer:     dial.Endpoint,
		Tips:        tips,
		TPU:         &packetRecorder{},
		Gossip:      h.gossip,
		LocalTPU:    serviceLocalTPU,
		Dialer:      dial.Dialer,
	})
	require.NoError(t, err)
	h.service.harmonicClientIDAfter, h.service.baseClientIDAfter = 100*time.Millisecond, 50*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.service.Run(ctx)
		close(done)
	}()
	var once sync.Once
	h.stop = func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	t.Cleanup(h.stop)
	return h
}

// routeTipsHere sets the bank's tip state to route tips here already, with
// the mock block engine's block builder, so the slot needs no crank.
func (h *serviceHarness) routeTipsHere(t *testing.T, env *blockprod.TestEnv) {
	t.Helper()
	tda, _, err := h.crank.TipDistributionAccount(env.Bank.Epoch())
	require.NoError(t, err)
	builder := solana.MustPublicKeyFromBase58(h.be.feeInfo.GetPubkey())
	require.NoError(t, env.SlotCtx.SetAccount(h.crank.TipPaymentConfigAddress(), owned(mainnetTipPayment, tipConfigData(tda, builder, h.be.feeInfo.GetCommission()))))
	require.NoError(t, env.SlotCtx.SetAccount(tda, owned(mainnetTipDistribution, nil)))
}

func TestServiceAdvertisesHarmonicWhileTheBlockEngineIsHealthy(t *testing.T) {
	h := startService(t)
	relayer, err := ParseTPUConfig(tpuConfigResponse("198.51.100.7", 11222, 11223))
	require.NoError(t, err)
	local, relayed := socketsString(serviceLocalTPU), socketsString(relayer.Sockets())

	eventually(t, func() bool { return h.gossip.advertisedSockets() == relayed }, "the relayer's TPU is advertised once both sessions are up")
	eventually(t, func() bool { return h.gossip.clientID() == gossip.ClientIDMithrilHarmonic }, "then the Harmonic client ID")

	// The session reconnects only after a backoff of seconds.
	h.be.endBlocks <- errors.New("block engine restarting")
	eventually(t, func() bool { return h.gossip.advertisedSockets() == local }, "a lost block engine restores the local TPU")
	eventually(t, func() bool { return h.gossip.clientID() == gossip.ClientIDMithril }, "and mithril's own client ID")

	h.stop()
	assert.Equal(t, local, h.gossip.advertisedSockets())
	assert.Equal(t, gossip.ClientIDMithril, h.gossip.clientID())
}

func TestServiceRestoresGossipOnShutdown(t *testing.T) {
	h := startService(t)
	eventually(t, func() bool { return h.gossip.clientID() == gossip.ClientIDMithrilHarmonic }, "Harmonic advertised")
	h.stop()
	assert.Equal(t, socketsString(serviceLocalTPU), h.gossip.advertisedSockets())
	assert.Equal(t, gossip.ClientIDMithril, h.gossip.clientID())
}

func TestServiceRunsTheBlockAndFallsBackWhenTheSessionDrops(t *testing.T) {
	h := startService(t)
	eventually(t, h.service.blockEngine.Healthy, "block engine session up")
	hooks := h.service.Hooks()
	startSlot := func(slot uint64) *blockprod.TestEnv {
		env := gatedBank(t, costmodel.Limits{})
		h.routeTipsHere(t, env)
		now := time.Now()
		hooks.SlotStarted(blockprod.LeaderSlotInfo{Slot: slot, Bank: env.Bank, Start: now, End: now.Add(time.Hour)})
		eventually(t, func() bool { return slices.Contains(h.be.noticeSlots(), slot) }, "the block engine is told the slot")
		return env
	}

	env := startSlot(10)
	wire, err := transfer(t, 1).MarshalBinary()
	require.NoError(t, err)
	h.be.blocks <- blockMsg(10, blockTx(wire, 0))
	eventually(t, func() bool { return len(env.Bank.ForgedTransactions()) == 1 }, "the block's transaction executes")
	assert.False(t, env.Bank.TPUAllowed(), "the TPU stays closed while the block runs")
	hooks.SlotEnding(10)

	env = startSlot(11)
	h.be.endBlocks <- errors.New("block engine restarting")
	eventually(t, env.Bank.TPUAllowed, "a lost session falls back at once, not at the threshold")
	assert.Empty(t, env.Bank.ForgedTransactions())
	hooks.SlotEnding(11)

	// The session is down, so the next slot opens the TPU before it starts.
	env = gatedBank(t, costmodel.Limits{})
	now := time.Now()
	hooks.SlotStarted(blockprod.LeaderSlotInfo{Slot: 12, Bank: env.Bank, Start: now, End: now.Add(time.Hour)})
	assert.True(t, env.Bank.TPUAllowed())
}
