package harmonic

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	authpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/auth"
	"github.com/Overclock-Validator/mithril/pkg/identity"
)

// TokenSource keeps one endpoint's tokens fresh and publishes the access
// token through its Bearer.
type TokenSource struct {
	client   authpb.AuthServiceClient
	identity *identity.Holder
	bearer   Bearer
	now      func() time.Time
	after    func(time.Duration) <-chan time.Time
	reauth   chan struct{}
	// generation changes whenever the tokens are invalidated, so a refresh
	// that was in flight across an invalidation is discarded.
	generation atomic.Uint64

	mu     sync.Mutex
	tokens Tokens
}

// NewTokenSource authenticates as holder's current identity, and again after
// every set-identity swap while Run is running.
func NewTokenSource(client authpb.AuthServiceClient, holder *identity.Holder) *TokenSource {
	return &TokenSource{
		client:   client,
		identity: holder,
		now:      time.Now,
		after:    time.After,
		reauth:   make(chan struct{}, 1),
	}
}

// Bearer returns the credentials that carry this source's access token.
func (s *TokenSource) Bearer() *Bearer {
	return &s.bearer
}

// Tokens returns the current tokens.
func (s *TokenSource) Tokens() Tokens {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokens
}

// Authenticate replaces the tokens with a fresh pair.
func (s *TokenSource) Authenticate(ctx context.Context) error {
	tokens, err := Authenticate(ctx, s.client, s.identity.Current().PrivateKey())
	if err == nil {
		err = s.checkUnexpired(tokens.Access, tokens.Refresh)
	}
	if err != nil {
		s.bearer.Clear()
		return err
	}
	s.mu.Lock()
	s.tokens = tokens
	s.mu.Unlock()
	s.bearer.Set(tokens.Access.Value)
	return nil
}

// Invalidate discards the access token after an UNAUTHENTICATED or
// PERMISSION_DENIED status. Run then authenticates again.
func (s *TokenSource) Invalidate() {
	s.generation.Add(1)
	s.bearer.Clear()
	select {
	case s.reauth <- struct{}{}:
	default:
	}
}

// Run refreshes the access token halfway to its expiry and authenticates
// again halfway to the refresh token's expiry, until ctx ends or
// authentication fails. A failed refresh falls back to authenticating again.
// Call Authenticate first.
func (s *TokenSource) Run(ctx context.Context) error {
	// Tokens are bound to the pubkey that authenticated; a swap needs new ones.
	defer s.identity.OnSwap(func(_, _ *identity.Keypair) { s.Invalidate() })()
	for {
		tokens := s.Tokens()
		now := s.now()
		refreshAt := halfway(now, tokens.Access.Expiry)
		reauthAt := halfway(now, tokens.Refresh.Expiry)
		wait, reauth := refreshAt.Sub(now), false
		if !reauthAt.After(refreshAt) {
			wait, reauth = reauthAt.Sub(now), true
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.reauth:
			reauth = true
		case <-s.after(wait):
		}
		if !reauth && s.refresh(ctx, tokens.Refresh.Value) {
			continue
		}
		if err := s.Authenticate(ctx); err != nil {
			return fmt.Errorf("authenticate: %w", err)
		}
	}
}

func (s *TokenSource) refresh(ctx context.Context, refreshToken string) bool {
	generation := s.generation.Load()
	access, err := RefreshAccess(ctx, s.client, refreshToken)
	if err == nil {
		err = s.checkUnexpired(access)
	}
	if err != nil || s.generation.Load() != generation {
		return false
	}
	s.mu.Lock()
	s.tokens.Access = access
	s.mu.Unlock()
	s.bearer.Set(access.Value)
	return true
}

func (s *TokenSource) checkUnexpired(tokens ...Token) error {
	now := s.now()
	for _, token := range tokens {
		if !token.Expiry.After(now) {
			return fmt.Errorf("token expired at %s", token.Expiry.Format(time.RFC3339))
		}
	}
	return nil
}

func halfway(now, expiry time.Time) time.Time {
	if !expiry.After(now) {
		return now
	}
	return now.Add(expiry.Sub(now) / 2)
}
