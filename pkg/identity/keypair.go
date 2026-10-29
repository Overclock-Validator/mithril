// Package identity owns the validator identity keypair. A Holder is the one
// place that keypair lives: subsystems read the current Keypair when they sign
// or compare pubkeys, and a set-identity swap replaces it in that one place.
package identity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"

	"github.com/gagliardetto/solana-go"
)

// Keypair is one immutable identity. Its key bytes are never shared: callers
// sign through it, or take a fresh copy with PrivateKey.
type Keypair struct {
	priv ed25519.PrivateKey
	pub  solana.PublicKey
}

// NewKeypair copies a 64-byte Solana keypair (seed followed by public key).
// The public half must match the seed, as Agave's Keypair::from_bytes checks.
func NewKeypair(key []byte) (*Keypair, error) {
	if len(key) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("identity keypair has %d bytes, want %d", len(key), ed25519.PrivateKeySize)
	}
	priv := ed25519.NewKeyFromSeed(key[:ed25519.SeedSize])
	if !bytes.Equal(priv[ed25519.SeedSize:], key[ed25519.SeedSize:]) {
		return nil, fmt.Errorf("identity keypair public key does not match its secret key")
	}
	return &Keypair{priv: priv, pub: solana.PublicKeyFromBytes(priv[ed25519.SeedSize:])}, nil
}

// GenerateKeypair returns a fresh random identity.
func GenerateKeypair() (*Keypair, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate identity keypair: %w", err)
	}
	return &Keypair{priv: priv, pub: solana.PublicKeyFromBytes(priv[ed25519.SeedSize:])}, nil
}

func (k *Keypair) PublicKey() solana.PublicKey {
	return k.pub
}

// PrivateKey returns a copy of the 64-byte keypair.
func (k *Keypair) PrivateKey() ed25519.PrivateKey {
	return append(ed25519.PrivateKey(nil), k.priv...)
}

func (k *Keypair) Sign(message []byte) []byte {
	return ed25519.Sign(k.priv, message)
}
