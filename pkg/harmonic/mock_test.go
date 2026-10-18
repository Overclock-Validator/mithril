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
