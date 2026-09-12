package sigverify

import (
	stded25519 "crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

// signedV0Transaction builds a versioned (v0) transaction and signs the exact
// canonical message bytes, including the 0x80 version prefix.
func signedV0Transaction(t *testing.T) *solana.Transaction {
	t.Helper()

	payer, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)

	tx, err := solana.NewTransaction(
		[]solana.Instruction{
			system.NewTransferInstruction(1, payer.PublicKey(), payer.PublicKey()).Build(),
		},
		solana.Hash{},
		solana.TransactionPayer(payer.PublicKey()),
	)
	require.NoError(t, err)
	tx.Message.SetVersion(solana.MessageVersionV0)

	msg, err := txverify.MessageBytes(tx)
	require.NoError(t, err)
	require.Equal(t, byte(0x80), msg[0], "a v0 message is signed over a 0x80 prefix")

	tx.Signatures = []solana.Signature{
		solana.SignatureFromBytes(stded25519.Sign(stded25519.PrivateKey(payer), msg)),
	}
	return tx
}

// Regression: the version prefix is part of the signed message and must reach
// the signature backend unchanged.
func TestVersionedTransactionIsAccepted(t *testing.T) {
	tx := signedV0Transaction(t)
	require.True(t, VerifyTransaction(tx),
		"a correctly signed v0 transaction must be admitted")
	raw, err := tx.MarshalBinary()
	require.NoError(t, err)
	require.True(t, VerifyPacket(raw),
		"a correctly signed v0 wire transaction must survive TPU sanitation")
}

func TestLegacyTransactionIsAccepted(t *testing.T) {
	require.True(t, VerifyPacket(txfixture.MustSignedTransferWire(1)))
}

func TestV1AdmissionFollowsFeatureCallback(t *testing.T) {
	wire := txfixture.MustSignedV1Wire(1, 32)

	// Parsing is feature-independent, but admission fails closed when the
	// caller has no current-bank feature source.
	tx, err := ParseTx(wire)
	require.NoError(t, err)
	require.Equal(t, solana.MessageVersionV1, tx.Message.GetVersion())
	require.False(t, VerifyPacket(wire))
	require.False(t, VerifyPacketWithTxV1(wire, func() bool { return false }))
	require.True(t, VerifyPacketWithTxV1(wire, func() bool { return true }))
	require.False(t, VerifyTransaction(tx))
	require.True(t, VerifyTransactionWithTxV1(tx, func() bool { return true }))
}

func TestV1SignatureTailIsVerified(t *testing.T) {
	wire := txfixture.MustSignedV1Wire(2, 16)
	require.True(t, VerifyPacketWithTxV1(wire, func() bool { return true }))

	corrupt := append([]byte(nil), wire...)
	corrupt[len(corrupt)-64] ^= 0xff
	require.False(t, VerifyPacketWithTxV1(corrupt, func() bool { return true }))
}

func TestParseTxRejectsTrailingBytesForEveryVersion(t *testing.T) {
	legacy := txfixture.MustSignedTransferWire(3)
	_, err := ParseTx(append(append([]byte(nil), legacy...), 0))
	require.Error(t, err)

	v1 := txfixture.MustSignedV1Wire(3, 16)
	_, err = ParseTx(append(append([]byte(nil), v1...), 0))
	require.Error(t, err)
}

func TestBatchV1AdmissionFollowsFeatureCallback(t *testing.T) {
	packets := [][]byte{
		txfixture.MustSignedTransferWire(4),
		txfixture.MustSignedV1Wire(4, 16),
	}

	var verifier BatchVerifier
	got := make([]bool, len(packets))
	verifier.Verify(packets, got)
	require.Equal(t, []bool{true, false}, got)

	verifier.SetTxV1Enabled(func() bool { return true })
	verifier.Verify(packets, got)
	require.Equal(t, []bool{true, true}, got)
}

func TestGarbageAndCorruptionAreRejected(t *testing.T) {
	require.False(t, VerifyPacket([]byte("not a transaction")), "unparseable packet")
	require.False(t, VerifyPacket(nil), "empty packet")

	wire := txfixture.MustSignedTransferWire(2)
	corrupt := make([]byte, len(wire))
	copy(corrupt, wire)
	corrupt[3] ^= 0xFF // inside the first signature
	require.False(t, VerifyPacket(corrupt), "corrupted signature")
}

// Batching must not change any verdict, and an unparseable packet in the middle
// of a group must not shift the verdicts of its neighbours: it contributes no
// signature lanes, so a naive index mapping would slide every later result.
func TestBatchVerdictsMatchSingleVerdictsWithGaps(t *testing.T) {
	const width = 24
	packets := make([][]byte, 0, width)
	want := make([]bool, 0, width)

	for i := 0; i < width; i++ {
		switch i % 4 {
		case 0: // unparseable — contributes no lane
			packets = append(packets, []byte{0x01, 0x02, 0x03})
			want = append(want, false)
		case 1: // corrupted signature — contributes a lane that fails
			wire := txfixture.MustSignedTransferWire(uint64(i))
			corrupt := make([]byte, len(wire))
			copy(corrupt, wire)
			corrupt[5] ^= 0xFF
			packets = append(packets, corrupt)
			want = append(want, false)
		default: // honest
			packets = append(packets, txfixture.MustSignedTransferWire(uint64(i)))
			want = append(want, true)
		}
	}

	var verifier BatchVerifier
	got := make([]bool, width)
	verifier.Verify(packets, got)

	for i := range packets {
		require.Equal(t, want[i], got[i], "batch verdict for packet %d", i)
		require.Equal(t, VerifyPacket(packets[i]), got[i],
			"batch and single-packet verdicts disagree at %d", i)
	}
}

func TestBatchVerifierIsReusable(t *testing.T) {
	var verifier BatchVerifier
	for round := 0; round < 3; round++ {
		packets := [][]byte{
			txfixture.MustSignedTransferWire(uint64(round*2 + 1)),
			[]byte("garbage"),
		}
		got := make([]bool, 2)
		verifier.Verify(packets, got)
		require.True(t, got[0], "round %d: honest packet", round)
		require.False(t, got[1], "round %d: garbage packet", round)
	}
}

func TestInadmissibleShapesAreRejected(t *testing.T) {
	tx := signedV0Transaction(t)
	require.True(t, VerifyTransaction(tx))

	noSignatures := *tx
	noSignatures.Signatures = nil
	require.False(t, VerifyTransaction(&noSignatures),
		"a transaction with no signatures must not be admitted")

	extraSignature := *tx
	extraSignature.Signatures = append(append([]solana.Signature{}, tx.Signatures...), solana.Signature{})
	require.False(t, VerifyTransaction(&extraSignature),
		"signature count must match the header")

	require.False(t, VerifyTransaction(nil), "nil transaction")
}

func TestRandomKeyDoesNotVerify(t *testing.T) {
	tx := signedV0Transaction(t)
	var scratch [64]byte
	_, err := rand.Read(scratch[:])
	require.NoError(t, err)
	tx.Signatures[0] = solana.SignatureFromBytes(scratch[:])
	require.False(t, VerifyTransaction(tx))
}
