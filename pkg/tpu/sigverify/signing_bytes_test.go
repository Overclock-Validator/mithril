package sigverify

import (
	"crypto/ed25519"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

func TestVerifyTransactionUsesCanonicalV0SigningBytes(t *testing.T) {
	seed := [ed25519.SeedSize]byte{2, 4, 6, 8, 10, 12, 14, 16}
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	var publicKey solana.PublicKey
	copy(publicKey[:], privateKey.Public().(ed25519.PublicKey))

	message := solana.Message{
		Header:          solana.MessageHeader{NumRequiredSignatures: 1},
		AccountKeys:     []solana.PublicKey{publicKey},
		RecentBlockhash: solana.Hash{0: 0xaa, 31: 0x55},
	}
	message.SetVersion(solana.MessageVersionV0)
	tx := &solana.Transaction{Message: message}
	signingBytes, err := txverify.MessageBytes(tx)
	if err != nil {
		t.Fatalf("MessageBytes: %v", err)
	}
	if len(signingBytes) == 0 || signingBytes[0] != 0x80 {
		t.Fatalf("V0 signing prefix = %x, want 80", signingBytes)
	}

	var signature solana.Signature
	copy(signature[:], ed25519.Sign(privateKey, signingBytes))
	tx.Signatures = []solana.Signature{signature}
	if !VerifyTransaction(tx) {
		t.Fatal("VerifyTransaction rejected a V0 signature over canonical signing bytes")
	}

	tx.Message.RecentBlockhash[0] ^= 1
	if VerifyTransaction(tx) {
		t.Fatal("VerifyTransaction accepted the signature after the signed V0 message changed")
	}
}
