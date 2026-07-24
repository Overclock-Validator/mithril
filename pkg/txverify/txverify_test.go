package txverify

import (
	"bytes"
	"crypto/ed25519"
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestMessageBytesExactLegacyAndV0(t *testing.T) {
	seed := [ed25519.SeedSize]byte{1, 3, 5, 7, 9, 11, 13, 15}
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	var publicKey solana.PublicKey
	copy(publicKey[:], privateKey.Public().(ed25519.PublicKey))
	blockhash := solana.Hash{0: 0x11, 1: 0x22, 30: 0x33, 31: 0x44}

	tests := []struct {
		name    string
		version solana.MessageVersion
		prefix  []byte
		suffix  []byte
	}{
		{name: "legacy", version: solana.MessageVersionLegacy, prefix: []byte{1, 0, 0, 1}, suffix: []byte{0}},
		{name: "v0", version: solana.MessageVersionV0, prefix: []byte{0x80, 1, 0, 0, 1}, suffix: []byte{0, 0}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message := solana.Message{
				Header:          solana.MessageHeader{NumRequiredSignatures: 1},
				AccountKeys:     []solana.PublicKey{publicKey},
				RecentBlockhash: blockhash,
			}
			message.SetVersion(test.version)
			tx := &solana.Transaction{Message: message}

			got, err := MessageBytes(tx)
			if err != nil {
				t.Fatalf("MessageBytes: %v", err)
			}
			want := append([]byte(nil), test.prefix...)
			want = append(want, publicKey[:]...)
			want = append(want, blockhash[:]...)
			want = append(want, test.suffix...)
			if !bytes.Equal(got, want) {
				t.Fatalf("MessageBytes = %x, want %x", got, want)
			}

			signed := ed25519.Sign(privateKey, want)
			var signature solana.Signature
			copy(signature[:], signed)
			tx.Signatures = []solana.Signature{signature}
			if err := VerifyTransaction(tx); err != nil {
				t.Fatalf("VerifyTransaction: %v", err)
			}
		})
	}
}
