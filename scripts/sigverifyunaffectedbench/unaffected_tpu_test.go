// Package sigverifyunaffectedbench_test is an external, public-API-only
// benchmark harness copied unchanged into the frozen Mithril baseline. Keeping
// it outside the measured packages prevents current-tree test helpers or
// unexported implementation details from leaking into the comparison.
package sigverifyunaffectedbench_test

import (
	"crypto/ed25519"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/tpu"
	tpusigverify "github.com/Overclock-Validator/mithril/pkg/tpu/sigverify"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

const unaffectedBaselineRevision = "9f43c94203a5f705c8bff09033fa5995448132cc"

var unaffectedVerdictSink bool

type transactionFixture struct {
	transaction *solana.Transaction
	wire        []byte
}

func makeTransactionFixture(tb testing.TB) transactionFixture {
	tb.Helper()
	seed := [ed25519.SeedSize]byte{
		0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
		0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
		0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
		0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xf0, 0x0f,
	}
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	var publicKey solana.PublicKey
	copy(publicKey[:], privateKey.Public().(ed25519.PublicKey))

	message := solana.Message{
		Header: solana.MessageHeader{
			NumRequiredSignatures:       1,
			NumReadonlySignedAccounts:   0,
			NumReadonlyUnsignedAccounts: 0,
		},
		AccountKeys: []solana.PublicKey{publicKey},
		RecentBlockhash: solana.Hash{
			0: 0xa1, 1: 0xb2, 2: 0xc3, 3: 0xd4,
			28: 0x1d, 29: 0x2e, 30: 0x3f, 31: 0x40,
		},
	}
	message.SetVersion(solana.MessageVersionLegacy)
	signingBytes, err := message.MarshalBinary()
	if err != nil {
		tb.Fatalf("marshal signing bytes: %v", err)
	}
	var signature solana.Signature
	copy(signature[:], ed25519.Sign(privateKey, signingBytes))
	transaction := &solana.Transaction{
		Signatures: []solana.Signature{signature},
		Message:    message,
	}
	wire, err := transaction.MarshalBinary()
	if err != nil {
		tb.Fatalf("marshal transaction: %v", err)
	}
	return transactionFixture{
		transaction: transaction,
		wire:        append([]byte(nil), wire...),
	}
}

func TestUnaffectedFixturePassesEveryPublicPath(t *testing.T) {
	fixture := makeTransactionFixture(t)
	checks := []struct {
		name string
		ok   bool
	}{
		{name: "tpu-root-transaction", ok: tpu.VerifyTxSig(fixture.transaction)},
		{name: "tpu-root-packet", ok: tpu.VerifyPacket(fixture.wire)},
		{name: "tpu-sigverify-transaction", ok: tpusigverify.VerifyTransaction(fixture.transaction)},
		{name: "tpu-sigverify-packet", ok: tpusigverify.VerifyPacket(fixture.wire)},
		{name: "txverify-transaction", ok: txverify.VerifyTransaction(fixture.transaction) == nil},
	}
	for _, check := range checks {
		if !check.ok {
			t.Fatalf("%s rejected deterministic valid fixture", check.name)
		}
	}
}

// BenchmarkDisabledTelemetryPublicVerification measures only public verification
// APIs that exist in both the frozen baseline and current source tree. The gate
// runner removes every sigverify-telemetry environment variable before process
// startup, so the current tree exercises its ordinary disabled path.
func BenchmarkDisabledTelemetryPublicVerification(b *testing.B) {
	fixture := makeTransactionFixture(b)

	b.Run("path=tpu-root-transaction", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			unaffectedVerdictSink = tpu.VerifyTxSig(fixture.transaction)
		}
		if !unaffectedVerdictSink {
			b.Fatal("verification failed")
		}
	})

	b.Run("path=tpu-root-packet", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			unaffectedVerdictSink = tpu.VerifyPacket(fixture.wire)
		}
		if !unaffectedVerdictSink {
			b.Fatal("verification failed")
		}
	})

	b.Run("path=tpu-sigverify-transaction", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			unaffectedVerdictSink = tpusigverify.VerifyTransaction(fixture.transaction)
		}
		if !unaffectedVerdictSink {
			b.Fatal("verification failed")
		}
	})

	b.Run("path=tpu-sigverify-packet", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			unaffectedVerdictSink = tpusigverify.VerifyPacket(fixture.wire)
		}
		if !unaffectedVerdictSink {
			b.Fatal("verification failed")
		}
	})

	b.Run("path=txverify-transaction", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			unaffectedVerdictSink = txverify.VerifyTransaction(fixture.transaction) == nil
		}
		if !unaffectedVerdictSink {
			b.Fatal("verification failed")
		}
	})
}
