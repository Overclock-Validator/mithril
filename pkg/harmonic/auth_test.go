package harmonic

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"
	"time"

	authpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/auth"
	relayerpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/relayer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// base58 is a plain big-integer Bitcoin-alphabet encoder, independent of the
// one the code under test uses.
func base58(b []byte) string {
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	n := new(big.Int).SetBytes(b)
	var out []byte
	for radix, mod := big.NewInt(58), new(big.Int); n.Sign() > 0; {
		n.DivMod(n, radix, mod)
		out = append([]byte{alphabet[mod.Int64()]}, out...)
	}
	for _, c := range b {
		if c != 0 {
			break
		}
		out = append([]byte{'1'}, out...)
	}
	return string(out)
}

// rfc8032Key is the RFC 8032 section 7.1 test 1 key.
func rfc8032Key(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	seed, err := hex.DecodeString("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
	require.NoError(t, err)
	key := ed25519.NewKeyFromSeed(seed)
	require.Equal(t, "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a", hex.EncodeToString(key.Public().(ed25519.PublicKey)))
	return key
}

func TestAuthMessageIsBase58PubkeyDashChallenge(t *testing.T) {
	pubkey := rfc8032Key(t).Public().(ed25519.PublicKey)
	message, err := AuthMessage(pubkey, "abcdefghi")
	require.NoError(t, err)
	// The same bytes the reference client signs: format!("{}-{}", pubkey, challenge).
	assert.Equal(t, base58(pubkey)+"-abcdefghi", string(message))
	// The 9 challenge bytes are used verbatim, even when not printable.
	message, err = AuthMessage(pubkey, "\x00\xff-é\n123")
	require.NoError(t, err)
	assert.Equal(t, base58(pubkey)+"-\x00\xff-é\n123", string(message))

	for _, challenge := range []string{"", "abcdefgh", "abcdefghij", "ééééé"} {
		_, err := AuthMessage(pubkey, challenge)
		assert.Errorf(t, err, "challenge of %d bytes", len(challenge))
	}
	_, err = AuthMessage(pubkey[:31], "abcdefghi")
	assert.Error(t, err)
}

func TestAuthenticateHandshakeAndBearer(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	auth := newMockAuth(func() time.Time { return now })
	relayer := newMockRelayer(nil)
	relayer.heartbeatEvery = 0
	cfg := serveBufconn(t, func(s *grpc.Server) {
		authpb.RegisterAuthServiceServer(s, auth)
		relayerpb.RegisterRelayerServer(s, relayer)
	})
	identity := rfc8032Key(t)
	tokens := NewTokenSource(nil, identityHolder(t, identity))
	cfg.Credentials = tokens.Bearer()
	conn, err := Dial(context.Background(), cfg)
	require.NoError(t, err)
	defer conn.Close()
	tokens.client = authpb.NewAuthServiceClient(conn)
	relayerClient := relayerpb.NewRelayerClient(conn)

	_, err = relayerClient.GetTpuConfigs(context.Background(), &relayerpb.GetTpuConfigsRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err), "no RPC goes out without a token")

	require.NoError(t, tokens.Authenticate(context.Background()))
	got := tokens.Tokens()
	assert.Equal(t, "access-1", got.Access.Value)
	assert.True(t, now.Add(10*time.Minute).Equal(got.Access.Expiry))
	assert.Equal(t, "refresh-2", got.Refresh.Value)
	assert.Empty(t, auth.authHeaders, "auth RPCs carry no authorization header")

	_, err = relayerClient.GetTpuConfigs(context.Background(), &relayerpb.GetTpuConfigsRequest{})
	require.NoError(t, err)
	assert.Equal(t, []string{"Bearer access-1"}, relayer.last().Get("authorization"))
	assert.True(t, strings.HasPrefix(relayer.last().Get("user-agent")[0], "mithril/"))

	tokens.Invalidate()
	_, err = relayerClient.GetTpuConfigs(context.Background(), &relayerpb.GetTpuConfigsRequest{})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.True(t, IsAuthError(err))
	assert.True(t, IsAuthError(status.Error(codes.PermissionDenied, "")))
	assert.False(t, IsAuthError(status.Error(codes.Unavailable, "")))
}

func TestAuthenticateRejectsBadResponses(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	valid := func() *authpb.Token {
		return &authpb.Token{Value: "token", ExpiresAtUtc: timestamppb.New(now.Add(time.Hour))}
	}
	cases := map[string]func(*mockAuth){
		"short challenge": func(m *mockAuth) { m.challenge = "abcdefgh" },
		"long challenge":  func(m *mockAuth) { m.challenge = "abcdefghij" },
		"missing access token": func(m *mockAuth) {
			m.tokens = func() *authpb.GenerateAuthTokensResponse {
				return &authpb.GenerateAuthTokensResponse{RefreshToken: valid()}
			}
		},
		"missing refresh token": func(m *mockAuth) {
			m.tokens = func() *authpb.GenerateAuthTokensResponse {
				return &authpb.GenerateAuthTokensResponse{AccessToken: valid()}
			}
		},
		"empty token": func(m *mockAuth) {
			m.tokens = func() *authpb.GenerateAuthTokensResponse {
				return &authpb.GenerateAuthTokensResponse{AccessToken: &authpb.Token{ExpiresAtUtc: timestamppb.New(now.Add(time.Hour))}, RefreshToken: valid()}
			}
		},
		"oversized token": func(m *mockAuth) {
			m.tokens = func() *authpb.GenerateAuthTokensResponse {
				big := valid()
				big.Value = strings.Repeat("x", maxTokenBytes+1)
				return &authpb.GenerateAuthTokensResponse{AccessToken: valid(), RefreshToken: big}
			}
		},
		"no expiry": func(m *mockAuth) {
			m.tokens = func() *authpb.GenerateAuthTokensResponse {
				return &authpb.GenerateAuthTokensResponse{AccessToken: &authpb.Token{Value: "token"}, RefreshToken: valid()}
			}
		},
		"already expired": func(m *mockAuth) {
			m.tokens = func() *authpb.GenerateAuthTokensResponse {
				expired := valid()
				expired.ExpiresAtUtc = timestamppb.New(now)
				return &authpb.GenerateAuthTokensResponse{AccessToken: expired, RefreshToken: valid()}
			}
		},
	}
	for name, configure := range cases {
		t.Run(name, func(t *testing.T) {
			auth := newMockAuth(func() time.Time { return now })
			configure(auth)
			cfg := serveBufconn(t, func(s *grpc.Server) { authpb.RegisterAuthServiceServer(s, auth) })
			conn, err := Dial(context.Background(), cfg)
			require.NoError(t, err)
			defer conn.Close()
			tokens := NewTokenSource(authpb.NewAuthServiceClient(conn), identityHolder(t, rfc8032Key(t)))
			tokens.now = func() time.Time { return now }
			tokens.bearer.Set("stale")

			require.Error(t, tokens.Authenticate(context.Background()))
			_, err = tokens.Bearer().GetRequestMetadata(context.Background())
			assert.Equal(t, codes.Unauthenticated, status.Code(err), "a failed handshake leaves no token")
			if strings.Contains(name, "challenge") {
				assert.Zero(t, auth.callCount("tokens"), "a bad challenge is never signed")
			}
		})
	}
}

// fakeTimers hands the test every timer TokenSource.Run starts.
type fakeTimers struct {
	waits chan time.Duration
	fires chan chan time.Time
}

func newFakeTimers() *fakeTimers {
	return &fakeTimers{waits: make(chan time.Duration, 8), fires: make(chan chan time.Time, 8)}
}

func (f *fakeTimers) after(d time.Duration) <-chan time.Time {
	c := make(chan time.Time, 1)
	f.waits <- d
	f.fires <- c
	return c
}

// next returns the next timer's duration and a function that fires it.
func (f *fakeTimers) next(t *testing.T) (time.Duration, func()) {
	t.Helper()
	select {
	case d := <-f.waits:
		c := <-f.fires
		return d, func() { c <- time.Time{} }
	case <-time.After(2 * time.Second):
		t.Fatal("no timer started")
		return 0, nil
	}
}

func startTokenSource(t *testing.T, auth *mockAuth, now time.Time) (*TokenSource, *fakeTimers, <-chan error) {
	t.Helper()
	cfg := serveBufconn(t, func(s *grpc.Server) { authpb.RegisterAuthServiceServer(s, auth) })
	conn, err := Dial(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	tokens := NewTokenSource(authpb.NewAuthServiceClient(conn), identityHolder(t, rfc8032Key(t)))
	timers := newFakeTimers()
	tokens.now = func() time.Time { return now }
	tokens.after = timers.after
	require.NoError(t, tokens.Authenticate(context.Background()))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tokens.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return tokens, timers, done
}

func bearerValue(t *testing.T, tokens *TokenSource) string {
	t.Helper()
	md, err := tokens.Bearer().GetRequestMetadata(context.Background())
	require.NoError(t, err)
	return md["authorization"]
}

func TestTokenSourceRefreshesAtHalfLifeAndFallsBackToReauth(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	auth := newMockAuth(func() time.Time { return now })
	tokens, timers, _ := startTokenSource(t, auth, now)

	wait, fire := timers.next(t)
	assert.Equal(t, 5*time.Minute, wait, "half of the access token's 10 minutes")
	fire()
	wait, fire = timers.next(t)
	assert.Equal(t, 1, auth.callCount("refresh"))
	assert.Equal(t, "Bearer access-3", bearerValue(t, tokens))
	assert.Equal(t, 5*time.Minute, wait)

	auth.mu.Lock()
	auth.refreshErr = status.Error(codes.Unauthenticated, "refresh token revoked")
	auth.mu.Unlock()
	fire()
	timers.next(t)
	assert.Equal(t, 2, auth.callCount("refresh"))
	assert.Equal(t, 2, auth.callCount("tokens"), "a failed refresh authenticates again")
	assert.Equal(t, "Bearer access-4", bearerValue(t, tokens))

	tokens.Invalidate()
	timers.next(t)
	assert.Equal(t, 3, auth.callCount("tokens"), "an auth error authenticates again at once")
	assert.Equal(t, "Bearer access-6", bearerValue(t, tokens))
}

func TestTokenSourceReauthenticatesAtRefreshTokenHalfLife(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	auth := newMockAuth(func() time.Time { return now })
	auth.accessTTL, auth.refreshTTL = time.Hour, 20*time.Minute
	_, timers, _ := startTokenSource(t, auth, now)

	wait, fire := timers.next(t)
	assert.Equal(t, 10*time.Minute, wait, "half of the refresh token's 20 minutes comes first")
	fire()
	timers.next(t)
	assert.Zero(t, auth.callCount("refresh"))
	assert.Equal(t, 2, auth.callCount("tokens"))
}

func TestTokenSourceDiscardsRefreshThatOutlivesInvalidation(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	auth := newMockAuth(func() time.Time { return now })
	auth.refreshGate = make(chan struct{})
	tokens, timers, _ := startTokenSource(t, auth, now)

	_, fire := timers.next(t)
	fire()
	require.Eventually(t, func() bool { return auth.callCount("refresh") == 1 }, 2*time.Second, time.Millisecond)
	tokens.Invalidate()
	close(auth.refreshGate)
	timers.next(t)
	assert.Equal(t, 2, auth.callCount("tokens"), "the stale refresh falls back to authenticating again")
	assert.Equal(t, "Bearer access-4", bearerValue(t, tokens), "the refreshed token (access-3) is never used")
}
