package harmonic

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	authpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/auth"
	"github.com/gagliardetto/solana-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	authChallengeBytes = 9
	maxTokenBytes      = 1024
	authRPCTimeout     = 5 * time.Second
	authServicePrefix  = "/auth.AuthService/"
)

// Token is a validated auth token.
type Token struct {
	Value  string
	Expiry time.Time
}

// Tokens are one endpoint's access and refresh tokens.
type Tokens struct {
	Access  Token
	Refresh Token
}

// AuthMessage returns the exact bytes a validator signs to authenticate:
// base58(pubkey), "-", then the 9-byte challenge verbatim. The whole string
// is also what GenerateAuthTokens takes as its challenge.
func AuthMessage(pubkey ed25519.PublicKey, challenge string) ([]byte, error) {
	if len(pubkey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("auth pubkey has %d bytes", len(pubkey))
	}
	if len(challenge) != authChallengeBytes {
		return nil, fmt.Errorf("auth challenge has %d bytes, want %d", len(challenge), authChallengeBytes)
	}
	return []byte(solana.PublicKeyFromBytes(pubkey).String() + "-" + challenge), nil
}

// Authenticate runs the challenge-response handshake and returns validated
// tokens.
func Authenticate(ctx context.Context, client authpb.AuthServiceClient, identity ed25519.PrivateKey) (Tokens, error) {
	pubkey, ok := identity.Public().(ed25519.PublicKey)
	if !ok || len(identity) != ed25519.PrivateKeySize {
		return Tokens{}, errors.New("invalid identity key")
	}
	challengeCtx, cancel := context.WithTimeout(ctx, authRPCTimeout)
	defer cancel()
	challenge, err := client.GenerateAuthChallenge(challengeCtx, &authpb.GenerateAuthChallengeRequest{
		Role:   authpb.Role_VALIDATOR,
		Pubkey: pubkey,
	})
	if err != nil {
		return Tokens{}, fmt.Errorf("generate auth challenge: %w", err)
	}
	message, err := AuthMessage(pubkey, challenge.GetChallenge())
	if err != nil {
		return Tokens{}, err
	}
	tokensCtx, cancel := context.WithTimeout(ctx, authRPCTimeout)
	defer cancel()
	resp, err := client.GenerateAuthTokens(tokensCtx, &authpb.GenerateAuthTokensRequest{
		Challenge:       string(message),
		ClientPubkey:    pubkey,
		SignedChallenge: ed25519.Sign(identity, message),
	})
	if err != nil {
		return Tokens{}, fmt.Errorf("generate auth tokens: %w", err)
	}
	access, err := validateToken(resp.GetAccessToken(), "access")
	if err != nil {
		return Tokens{}, err
	}
	refresh, err := validateToken(resp.GetRefreshToken(), "refresh")
	if err != nil {
		return Tokens{}, err
	}
	return Tokens{Access: access, Refresh: refresh}, nil
}

// RefreshAccess exchanges a refresh token for a new access token.
func RefreshAccess(ctx context.Context, client authpb.AuthServiceClient, refresh string) (Token, error) {
	ctx, cancel := context.WithTimeout(ctx, authRPCTimeout)
	defer cancel()
	resp, err := client.RefreshAccessToken(ctx, &authpb.RefreshAccessTokenRequest{RefreshToken: refresh})
	if err != nil {
		return Token{}, fmt.Errorf("refresh access token: %w", err)
	}
	return validateToken(resp.GetAccessToken(), "access")
}

func validateToken(token *authpb.Token, name string) (Token, error) {
	switch {
	case token == nil:
		return Token{}, fmt.Errorf("missing %s token", name)
	case token.GetValue() == "":
		return Token{}, fmt.Errorf("empty %s token", name)
	case len(token.GetValue()) > maxTokenBytes:
		return Token{}, fmt.Errorf("%s token has %d bytes, limit %d", name, len(token.GetValue()), maxTokenBytes)
	case token.GetExpiresAtUtc() == nil:
		return Token{}, fmt.Errorf("%s token has no expiry", name)
	}
	return Token{Value: token.GetValue(), Expiry: token.GetExpiresAtUtc().AsTime()}, nil
}

// Bearer attaches the current access token to every RPC except the auth
// service's own. With no token, RPCs fail with UNAUTHENTICATED rather than
// going out without one.
type Bearer struct {
	value atomic.Pointer[string]
}

// Set rotates the token. RPCs started afterwards use it; open streams keep
// the token they started with.
func (b *Bearer) Set(token string) {
	header := "Bearer " + token
	b.value.Store(&header)
}

// Clear drops the token.
func (b *Bearer) Clear() {
	b.value.Store(nil)
}

func (b *Bearer) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	if info, ok := credentials.RequestInfoFromContext(ctx); ok && strings.HasPrefix(info.Method, authServicePrefix) {
		return nil, nil
	}
	header := b.value.Load()
	if header == nil {
		return nil, status.Error(codes.Unauthenticated, "no Harmonic access token")
	}
	return map[string]string{"authorization": *header}, nil
}

// RequireTransportSecurity is false so plaintext development endpoints work.
func (b *Bearer) RequireTransportSecurity() bool {
	return false
}

// IsAuthError reports a status that requires discarding tokens and
// authenticating again.
func IsAuthError(err error) bool {
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		return true
	}
	return false
}
