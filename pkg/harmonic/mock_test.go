package harmonic

import (
	"context"
	"crypto/ed25519"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	authpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/auth"
	blockpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/block"
	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
	relayerpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/relayer"
	sharedpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/shared"
	"github.com/gagliardetto/solana-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockAuth is an AuthService that checks the handshake the way the block
// engine does and issues numbered tokens.
type mockAuth struct {
	authpb.UnimplementedAuthServiceServer
	now                   func() time.Time
	challenge             string
	accessTTL, refreshTTL time.Duration
	// tokens, if set, replaces the GenerateAuthTokens response.
	tokens func() *authpb.GenerateAuthTokensResponse
	// refreshGate, if set, holds RefreshAccessToken until it is closed.
	refreshGate chan struct{}

	mu          sync.Mutex
	refreshErr  error
	issued      int
	calls       map[string]int
	authHeaders []string
}

func newMockAuth(now func() time.Time) *mockAuth {
	return &mockAuth{now: now, challenge: "abcdefghi", accessTTL: 10 * time.Minute, refreshTTL: time.Hour, calls: map[string]int{}}
}

func (m *mockAuth) record(ctx context.Context, method string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls[method]++
	md, _ := metadata.FromIncomingContext(ctx)
	m.authHeaders = append(m.authHeaders, md.Get("authorization")...)
}

func (m *mockAuth) callCount(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[method]
}

func (m *mockAuth) token(prefix string, ttl time.Duration) *authpb.Token {
	m.issued++
	return &authpb.Token{Value: prefix + "-" + string(rune('0'+m.issued)), ExpiresAtUtc: timestamppb.New(m.now().Add(ttl))}
}

func (m *mockAuth) GenerateAuthChallenge(ctx context.Context, req *authpb.GenerateAuthChallengeRequest) (*authpb.GenerateAuthChallengeResponse, error) {
	m.record(ctx, "challenge")
	if req.GetRole() != authpb.Role_VALIDATOR || len(req.GetPubkey()) != ed25519.PublicKeySize {
		return nil, status.Error(codes.InvalidArgument, "bad challenge request")
	}
	return &authpb.GenerateAuthChallengeResponse{Challenge: m.challenge}, nil
}

func (m *mockAuth) GenerateAuthTokens(ctx context.Context, req *authpb.GenerateAuthTokensRequest) (*authpb.GenerateAuthTokensResponse, error) {
	m.record(ctx, "tokens")
	want := solana.PublicKeyFromBytes(req.GetClientPubkey()).String() + "-" + m.challenge
	if req.GetChallenge() != want || !ed25519.Verify(req.GetClientPubkey(), []byte(req.GetChallenge()), req.GetSignedChallenge()) {
		return nil, status.Error(codes.Unauthenticated, "bad signature")
	}
	if m.tokens != nil {
		return m.tokens(), nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return &authpb.GenerateAuthTokensResponse{
		AccessToken:  m.token("access", m.accessTTL),
		RefreshToken: m.token("refresh", m.refreshTTL),
	}, nil
}

func (m *mockAuth) RefreshAccessToken(ctx context.Context, req *authpb.RefreshAccessTokenRequest) (*authpb.RefreshAccessTokenResponse, error) {
	m.record(ctx, "refresh")
	if m.refreshGate != nil {
		<-m.refreshGate
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refreshErr != nil {
		return nil, m.refreshErr
	}
	return &authpb.RefreshAccessTokenResponse{AccessToken: m.token("access", m.accessTTL)}, nil
}

// mockRelayer serves a scripted relayer: it records each GetTpuConfigs
// call's metadata, returns the configured TPU sockets, and streams heartbeats
// every heartbeatEvery (unless stalled) plus whatever the test sends.
type mockRelayer struct {
	relayerpb.UnimplementedRelayerServer
	heartbeatEvery time.Duration
	send           chan *relayerpb.SubscribePacketsResponse
	endStream      chan error
	stall          atomic.Bool
	subscriptions  atomic.Int32

	mu     sync.Mutex
	md     []metadata.MD
	config *relayerpb.GetTpuConfigsResponse
}

func newMockRelayer(config *relayerpb.GetTpuConfigsResponse) *mockRelayer {
	return &mockRelayer{
		heartbeatEvery: 20 * time.Millisecond,
		send:           make(chan *relayerpb.SubscribePacketsResponse, 16),
		endStream:      make(chan error, 1),
		config:         config,
	}
}

func (r *mockRelayer) setConfig(config *relayerpb.GetTpuConfigsResponse) {
	r.mu.Lock()
	r.config = config
	r.mu.Unlock()
}

func (r *mockRelayer) GetTpuConfigs(ctx context.Context, _ *relayerpb.GetTpuConfigsRequest) (*relayerpb.GetTpuConfigsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.md = append(r.md, md)
	if r.config == nil {
		return &relayerpb.GetTpuConfigsResponse{}, nil
	}
	return r.config, nil
}

func (r *mockRelayer) SubscribePackets(_ *relayerpb.SubscribePacketsRequest, stream relayerpb.Relayer_SubscribePacketsServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	if auth := md.Get("authorization"); len(auth) != 1 || !strings.HasPrefix(auth[0], "Bearer access-") {
		return status.Error(codes.Unauthenticated, "missing bearer")
	}
	r.subscriptions.Add(1)
	if err := stream.SendHeader(metadata.MD{}); err != nil {
		return err
	}
	var heartbeats <-chan time.Time
	if r.heartbeatEvery > 0 {
		ticker := time.NewTicker(r.heartbeatEvery)
		defer ticker.Stop()
		heartbeats = ticker.C
	}
	var count uint64
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case err := <-r.endStream:
			return err
		case msg := <-r.send:
			if err := stream.Send(msg); err != nil {
				return err
			}
		case <-heartbeats:
			if r.stall.Load() {
				continue
			}
			count++
			if err := stream.Send(&relayerpb.SubscribePacketsResponse{
				Msg: &relayerpb.SubscribePacketsResponse_Heartbeat{Heartbeat: &sharedpb.Heartbeat{Count: count}},
			}); err != nil {
				return err
			}
		}
	}
}

func (r *mockRelayer) last() metadata.MD {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.md[len(r.md)-1]
}

// serveBufconn starts a gRPC server on an in-memory listener and returns a
// dial config that reaches it.
func serveBufconn(t *testing.T, register func(*grpc.Server)) DialConfig {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return DialConfig{
		Endpoint: Endpoint{Target: "bufnet"},
		Dialer:   func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) },
	}
}

// mockBlockEngine serves a scripted block engine. SubmitLeaderWindowInfo
// fails with FAILED_PRECONDITION while no block stream is live, as the real
// one does.
type mockBlockEngine struct {
	blockenginepb.UnimplementedBlockEngineValidatorServer
	packets    chan *blockenginepb.SubscribePacketsResponse
	blocks     chan *blockpb.Block
	endBlocks  chan error
	blocksLive atomic.Int32
	blockSubs  atomic.Int32

	mu            sync.Mutex
	strategyErrs  []error
	noticeErr     error
	feeInfo       *blockenginepb.BlockBuilderFeeInfoResponse
	strategies    []blockenginepb.SchedulingStrategy
	notices       []*blockenginepb.SubmitLeaderWindowInfoRequest
	blockRequests []*blockenginepb.SubscribeBlocksRequest
}

func newMockBlockEngine() *mockBlockEngine {
	return &mockBlockEngine{
		packets:   make(chan *blockenginepb.SubscribePacketsResponse, 16),
		blocks:    make(chan *blockpb.Block, 16),
		endBlocks: make(chan error, 1),
		feeInfo:   &blockenginepb.BlockBuilderFeeInfoResponse{Pubkey: "11111111111111111111111111111112", Commission: 5},
	}
}

func requireBearer(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	if auth := md.Get("authorization"); len(auth) != 1 || !strings.HasPrefix(auth[0], "Bearer access-") {
		return status.Error(codes.Unauthenticated, "missing bearer")
	}
	return nil
}

func (m *mockBlockEngine) SetStrategy(ctx context.Context, req *blockenginepb.SetStrategyRequest) (*blockenginepb.SetStrategyResponse, error) {
	if err := requireBearer(ctx); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.strategies = append(m.strategies, req.GetStrategy())
	if len(m.strategyErrs) > 0 {
		err := m.strategyErrs[0]
		m.strategyErrs = m.strategyErrs[1:]
		return nil, err
	}
	return &blockenginepb.SetStrategyResponse{}, nil
}

func (m *mockBlockEngine) GetBlockBuilderFeeInfo(ctx context.Context, _ *blockenginepb.BlockBuilderFeeInfoRequest) (*blockenginepb.BlockBuilderFeeInfoResponse, error) {
	if err := requireBearer(ctx); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.feeInfo, nil
}

func (m *mockBlockEngine) SubmitLeaderWindowInfo(ctx context.Context, req *blockenginepb.SubmitLeaderWindowInfoRequest) (*blockenginepb.SubmitLeaderWindowInfoResponse, error) {
	if err := requireBearer(ctx); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notices = append(m.notices, req)
	if m.noticeErr != nil {
		return nil, m.noticeErr
	}
	if m.blocksLive.Load() == 0 {
		return nil, status.Error(codes.FailedPrecondition, "no block subscription")
	}
	return &blockenginepb.SubmitLeaderWindowInfoResponse{}, nil
}

func (m *mockBlockEngine) SubscribePackets(_ *blockenginepb.SubscribePacketsRequest, stream blockenginepb.BlockEngineValidator_SubscribePacketsServer) error {
	if err := requireBearer(stream.Context()); err != nil {
		return err
	}
	if err := stream.SendHeader(metadata.MD{}); err != nil {
		return err
	}
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case msg := <-m.packets:
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

func (m *mockBlockEngine) SubscribeBlocks2(req *blockenginepb.SubscribeBlocksRequest, stream blockenginepb.BlockEngineValidator_SubscribeBlocks2Server) error {
	if err := requireBearer(stream.Context()); err != nil {
		return err
	}
	m.mu.Lock()
	m.blockRequests = append(m.blockRequests, req)
	m.mu.Unlock()
	m.blockSubs.Add(1)
	m.blocksLive.Add(1)
	defer m.blocksLive.Add(-1)
	if err := stream.SendHeader(metadata.MD{}); err != nil {
		return err
	}
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case err := <-m.endBlocks:
			return err
		case block := <-m.blocks:
			if err := stream.Send(block); err != nil {
				return err
			}
		}
	}
}

func (m *mockBlockEngine) noticeSlots() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var slots []uint64
	for _, n := range m.notices {
		slots = append(slots, n.GetSlot())
	}
	return slots
}
