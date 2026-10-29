package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func newKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return key
}

func pubkey(key ed25519.PrivateKey) solana.PublicKey {
	return solana.PublicKeyFromBytes(key.Public().(ed25519.PublicKey))
}

func TestHolderSwapNotifiesListenerWithNewKey(t *testing.T) {
	first, second := newKey(t), newKey(t)
	h, err := NewHolder(first)
	require.NoError(t, err)
	require.Equal(t, pubkey(first), h.PublicKey())

	var observed []solana.PublicKey
	h.OnSwap(func(prev, next *Keypair) {
		require.Equal(t, pubkey(first), prev.PublicKey())
		// Listeners run after the holder publishes next.
		require.Equal(t, next, h.Current())
		observed = append(observed, next.PublicKey())
	})

	prev, err := h.Swap(second)
	require.NoError(t, err)
	require.Equal(t, pubkey(first), prev.PublicKey())
	require.Equal(t, pubkey(second), h.PublicKey())
	require.Equal(t, []solana.PublicKey{pubkey(second)}, observed)

	message := []byte("vote")
	require.True(t, ed25519.Verify(pubkey(second).Bytes(), message, h.Current().Sign(message)))
}

func TestHolderDoesNotShareKeyBytes(t *testing.T) {
	key := newKey(t)
	want := pubkey(key)
	h, err := NewHolder(key)
	require.NoError(t, err)
	clear(key)
	copied := h.Current().PrivateKey()
	clear(copied)

	message := []byte("shred")
	require.Equal(t, want, h.PublicKey())
	require.True(t, ed25519.Verify(want.Bytes(), message, h.Current().Sign(message)))
}

func TestHolderSwapSameKeyIsNoop(t *testing.T) {
	key := newKey(t)
	h, err := NewHolder(key)
	require.NoError(t, err)
	before := h.Current()
	calls := 0
	h.RegisterUpdater("counter", func(*Keypair) error { calls++; return nil })
	h.OnSwap(func(*Keypair, *Keypair) { calls++ })

	_, err = h.Swap(append(ed25519.PrivateKey(nil), key...))
	require.NoError(t, err)
	require.Same(t, before, h.Current())
	require.Zero(t, calls)
}

func TestHolderFailedUpdaterKeepsOldIdentity(t *testing.T) {
	first, second := newKey(t), newKey(t)
	h, err := NewHolder(first)
	require.NoError(t, err)

	var applied []solana.PublicKey
	h.RegisterUpdater("tls", func(next *Keypair) error {
		applied = append(applied, next.PublicKey())
		return nil
	})
	h.RegisterUpdater("history", func(*Keypair) error { return errors.New("history locked") })
	notified := false
	h.OnSwap(func(*Keypair, *Keypair) { notified = true })

	_, err = h.Swap(second)
	require.ErrorContains(t, err, "apply identity "+pubkey(second).String()+" to history: history locked")
	require.Equal(t, pubkey(first), h.PublicKey())
	require.False(t, notified)
	// The updater that adopted the new key was handed the old one back.
	require.Equal(t, []solana.PublicKey{pubkey(second), pubkey(first)}, applied)
}

func TestHolderUnregisteredUpdaterIsSkipped(t *testing.T) {
	h, err := NewHolder(newKey(t))
	require.NoError(t, err)
	unregister := h.RegisterUpdater("closed", func(*Keypair) error { return errors.New("closed") })
	unregister()
	_, err = h.Swap(newKey(t))
	require.NoError(t, err)
}

func TestHolderRejectsBadKeys(t *testing.T) {
	_, err := NewHolder(make([]byte, 32))
	require.ErrorContains(t, err, "has 32 bytes, want 64")

	key := newKey(t)
	h, err := NewHolder(key)
	require.NoError(t, err)
	mismatched := append(ed25519.PrivateKey(nil), key...)
	mismatched[63] ^= 1
	_, err = h.Swap(mismatched)
	require.ErrorContains(t, err, "public key does not match")
	require.Equal(t, pubkey(key), h.PublicKey())
}

func TestDerivedFollowsRegisteredRefresh(t *testing.T) {
	h, err := NewHolder(newKey(t))
	require.NoError(t, err)
	derived, err := NewDerived(h, func(kp *Keypair) (solana.PublicKey, error) { return kp.PublicKey(), nil })
	require.NoError(t, err)
	h.RegisterUpdater("derived", derived.Refresh)
	require.Equal(t, h.PublicKey(), derived.Get())

	next := newKey(t)
	_, err = h.Swap(next)
	require.NoError(t, err)
	require.Equal(t, pubkey(next), derived.Get())
	require.Same(t, h.Current(), derived.Keypair())
}

func TestEphemeralHolder(t *testing.T) {
	h, err := HolderOrEphemeral(nil)
	require.NoError(t, err)
	require.False(t, h.PublicKey().IsZero())
	same, err := HolderOrEphemeral(h)
	require.NoError(t, err)
	require.Same(t, h, same)
}
