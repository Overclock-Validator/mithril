package wire

import (
	"encoding/hex"
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Rust SDK SIMD-0385 fixture (empty config, one signer), copied from
// gagliardetto/solana-go's Apache-2.0 conformance tests.
const rustV1Golden = "8101000200000000abababababababababababababababababababababababababababababababab01048a88e3dd7409" +
	"f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c02020202020202020202020202020202020202020202" +
	"020202020202020202020303030303030303030303030303030303030303030303030303030303030303101010101010" +
	"101010101010101010101010101010101010101010101010101003030400000102deadbeef4e72ba5d7f993321339fd3" +
	"511742c1c21f5b80ca529337413f766a32485b86ddc124f38ebe5f3a907ed3ede52331e746c7e55fddefae148c5aea7a" +
	"780af4b604"

func TestSanitizeRustV1Golden(t *testing.T) {
	raw, err := hex.DecodeString(rustV1Golden)
	require.NoError(t, err)
	view, err := Sanitize(raw)
	require.NoError(t, err)
	require.Equal(t, VersionV1, view.Version)
	require.Equal(t, len(raw)-64, view.MessageEnd)

	tx, err := solana.TransactionFromBytes(raw)
	require.NoError(t, err)
	require.NoError(t, tx.VerifySignatures())
	message, err := tx.Message.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, message, view.Message())
	require.Equal(t, tx.Signatures[0][:], view.FirstSignature())
}

func TestSanitizeV1SignatureTailAndMessageView(t *testing.T) {
	raw := txfixture.MustSignedV1Wire(7, 32)
	tx, err := solana.TransactionFromBytes(raw)
	require.NoError(t, err)
	message, err := tx.Message.MarshalBinary()
	require.NoError(t, err)

	view, err := Sanitize(raw)
	require.NoError(t, err)
	require.Equal(t, VersionV1, view.Version)
	require.Equal(t, 1, view.NumSignatures)
	require.Equal(t, 0, view.MessageOffset)
	require.Equal(t, len(message), view.MessageEnd)
	require.Equal(t, len(message), view.SigsOffset)
	require.Equal(t, message, view.Message())
	require.Equal(t, tx.Signatures[0][:], view.FirstSignature())

	// Both slices must remain zero-copy views of the caller's wire buffer.
	raw[8] ^= 0xff
	require.Equal(t, raw[8], view.Message()[8])
	raw[view.SigsOffset] ^= 0xff
	require.Equal(t, raw[view.SigsOffset], view.FirstSignature()[0])
}

func TestSanitizeLegacyAndV0Views(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version solana.MessageVersion
	}{
		{name: "legacy", version: solana.MessageVersionLegacy},
		{name: "v0", version: solana.MessageVersionV0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			version := tc.version
			raw := structuralWire(t, version, 8)
			view, err := Sanitize(raw)
			require.NoError(t, err)
			if version == solana.MessageVersionV0 {
				require.Equal(t, VersionV0, view.Version)
				require.Equal(t, byte(0x80), view.Message()[0])
			} else {
				require.Equal(t, VersionLegacy, view.Version)
			}
			require.Equal(t, 1, view.SigsOffset)
			require.Equal(t, 65, view.MessageOffset)
			require.Equal(t, len(raw), view.MessageEnd)
			require.Equal(t, raw[1:65], view.FirstSignature())
		})
	}
}

func TestLegacyAndV0UniversalSignatureLimit(t *testing.T) {
	for _, version := range []solana.MessageVersion{solana.MessageVersionLegacy, solana.MessageVersionV0} {
		t.Run(legacyOrV0Name(version), func(t *testing.T) {
			raw := legacyOrV0WireWithLimits(t, version, MaxSignaturesPerTransaction, 0, 0)
			_, err := Sanitize(raw)
			require.NoError(t, err)

			// A structurally complete 13-signature legacy/v0 transaction cannot
			// fit in the 1232-byte packet bound. Keep this malformed packet short
			// so the test specifically proves admission rejects the signature
			// count, before attempting to locate or decode its message.
			tooMany := make([]byte, 1+(MaxSignaturesPerTransaction+1)*64+1)
			tooMany[0] = MaxSignaturesPerTransaction + 1
			if version == solana.MessageVersionV0 {
				tooMany[len(tooMany)-1] = 0x80
			}
			_, err = Sanitize(tooMany)
			require.ErrorIs(t, err, ErrInvalidSigCount)
		})
	}
}

func TestLegacyAndV0UniversalInstructionLimit(t *testing.T) {
	for _, version := range []solana.MessageVersion{solana.MessageVersionLegacy, solana.MessageVersionV0} {
		t.Run(legacyOrV0Name(version), func(t *testing.T) {
			raw := legacyOrV0WireWithLimits(t, version, 1, MaxInstructionsPerMessage, 0)
			_, err := Sanitize(raw)
			require.NoError(t, err)

			raw = legacyOrV0WireWithLimits(t, version, 1, MaxInstructionsPerMessage+1, 0)
			_, err = Sanitize(raw)
			require.ErrorIs(t, err, ErrInvalidMessage)
		})
	}
}

func TestLegacyAndV0UniversalAccountsPerInstructionLimit(t *testing.T) {
	for _, version := range []solana.MessageVersion{solana.MessageVersionLegacy, solana.MessageVersionV0} {
		t.Run(legacyOrV0Name(version), func(t *testing.T) {
			raw := legacyOrV0WireWithLimits(t, version, 1, 1, MaxAccountsPerInstruction)
			_, err := Sanitize(raw)
			require.NoError(t, err)

			raw = legacyOrV0WireWithLimits(t, version, 1, 1, MaxAccountsPerInstruction+1)
			_, err = Sanitize(raw)
			require.ErrorIs(t, err, ErrInvalidMessage)
		})
	}
}

func TestVersionSpecificSizeLimits(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version solana.MessageVersion
		limit   int
	}{
		{name: "legacy", version: solana.MessageVersionLegacy, limit: LegacyPacketDataSize},
		{name: "v0", version: solana.MessageVersionV0, limit: LegacyPacketDataSize},
		{name: "v1", version: solana.MessageVersionV1, limit: PacketDataSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exact := structuralWireAtSize(t, tc.version, tc.limit)
			require.Len(t, exact, tc.limit)
			_, err := Sanitize(exact)
			require.NoError(t, err)

			over := structuralWireAtSize(t, tc.version, tc.limit+1)
			require.Len(t, over, tc.limit+1)
			_, err = Sanitize(over)
			require.ErrorIs(t, err, ErrTooLarge)
		})
	}

	// The original packet ceiling must not accidentally remain as a global
	// transport limit: a structurally valid v1 transaction can exceed it.
	v1 := structuralWireAtSize(t, solana.MessageVersionV1, LegacyPacketDataSize+1)
	_, err := Sanitize(v1)
	require.NoError(t, err)
}

func TestV1RejectsMalformedTailMaskAndKeys(t *testing.T) {
	valid := txfixture.MustSignedV1Wire(9, 16)

	trailing := append(append([]byte(nil), valid...), 0)
	_, err := Sanitize(trailing)
	require.Error(t, err)

	truncated := append([]byte(nil), valid[:len(valid)-1]...)
	_, err = Sanitize(truncated)
	require.Error(t, err)

	badMask := append([]byte(nil), valid...)
	badMask[4] = 0x01 // priority-fee bits must be present as a pair
	_, err = Sanitize(badMask)
	require.ErrorIs(t, err, ErrInvalidMessage)

	duplicateKey := append([]byte(nil), valid...)
	copy(duplicateKey[74:106], duplicateKey[42:74])
	_, err = Sanitize(duplicateKey)
	require.ErrorIs(t, err, ErrInvalidMessage)

	_, err = Sanitize([]byte{0x82})
	require.ErrorIs(t, err, ErrInvalidMessage)
}

func TestSanitizeV1DoesNotAllocate(t *testing.T) {
	raw := txfixture.MustSignedV1Wire(12, 64)
	allocs := testing.AllocsPerRun(1_000, func() {
		if _, err := Sanitize(raw); err != nil {
			t.Fatal(err)
		}
	})
	require.Zero(t, allocs)
}

func TestLegacyTrailingBytesAreRejected(t *testing.T) {
	valid := structuralWire(t, solana.MessageVersionLegacy, 8)
	_, err := Sanitize(append(valid, 0))
	require.ErrorIs(t, err, ErrInvalidMessage)
}

func FuzzSanitizeNeverPanics(f *testing.F) {
	f.Add(txfixture.MustSignedTransferWire(1))
	f.Add(txfixture.MustSignedV1Wire(2, 16))
	f.Add([]byte{0x81})
	f.Fuzz(func(t *testing.T, raw []byte) {
		_, _ = Sanitize(raw)
	})
}

func structuralWireAtSize(t *testing.T, version solana.MessageVersion, target int) []byte {
	t.Helper()
	dataLen := target - 200
	if dataLen < 0 {
		dataLen = 0
	}
	for i := 0; i < 4; i++ {
		raw := structuralWire(t, version, dataLen)
		if len(raw) == target {
			return raw
		}
		dataLen += target - len(raw)
		require.GreaterOrEqual(t, dataLen, 0)
	}
	t.Fatalf("could not construct message version %d transaction of size %d", version, target)
	return nil
}

func structuralWire(t *testing.T, version solana.MessageVersion, dataLen int) []byte {
	t.Helper()
	payer := solana.NewWallet().PublicKey()
	msg := solana.Message{
		Header: solana.MessageHeader{
			NumRequiredSignatures:       1,
			NumReadonlyUnsignedAccounts: 1,
		},
		AccountKeys:     solana.PublicKeySlice{payer, solana.SystemProgramID},
		RecentBlockhash: solana.Hash{},
		Instructions: []solana.CompiledInstruction{{
			ProgramIDIndex: 1,
			Accounts:       []uint16{0},
			Data:           make([]byte, dataLen),
		}},
	}
	if version != solana.MessageVersionLegacy {
		_, err := msg.SetVersion(version)
		require.NoError(t, err)
	}
	tx := &solana.Transaction{Message: msg, Signatures: make([]solana.Signature, 1)}
	raw, err := tx.MarshalBinary()
	require.NoError(t, err)
	if version == solana.MessageVersionV1 && len(raw) <= PacketDataSize {
		require.NoError(t, tx.Sanitize())
	}
	return raw
}

func legacyOrV0WireWithLimits(
	t *testing.T,
	version solana.MessageVersion,
	numSignatures int,
	numInstructions int,
	accountsPerInstruction int,
) []byte {
	t.Helper()
	require.Contains(t, []solana.MessageVersion{solana.MessageVersionLegacy, solana.MessageVersionV0}, version)
	require.Greater(t, numSignatures, 0)

	numKeys := numSignatures
	if numInstructions > 0 && numKeys < 2 {
		numKeys = 2
	}
	keys := make(solana.PublicKeySlice, numKeys)
	for i := range keys {
		keys[i][0] = byte(i + 1)
	}
	header := solana.MessageHeader{
		NumRequiredSignatures:     uint8(numSignatures),
		NumReadonlySignedAccounts: uint8(numSignatures - 1),
	}
	if numKeys > numSignatures {
		header.NumReadonlyUnsignedAccounts = uint8(numKeys - numSignatures)
	}

	instructions := make([]solana.CompiledInstruction, numInstructions)
	for i := range instructions {
		instructions[i].ProgramIDIndex = uint16(numKeys - 1)
		instructions[i].Accounts = make([]uint16, accountsPerInstruction)
	}
	msg := solana.Message{
		Header:          header,
		AccountKeys:     keys,
		RecentBlockhash: solana.Hash{},
		Instructions:    instructions,
	}
	if version == solana.MessageVersionV0 {
		_, err := msg.SetVersion(version)
		require.NoError(t, err)
	}
	tx := &solana.Transaction{
		Message:    msg,
		Signatures: make([]solana.Signature, numSignatures),
	}
	raw, err := tx.MarshalBinary()
	require.NoError(t, err)
	require.LessOrEqual(t, len(raw), LegacyPacketDataSize)
	return raw
}

func legacyOrV0Name(version solana.MessageVersion) string {
	if version == solana.MessageVersionV0 {
		return "v0"
	}
	return "legacy"
}

func TestErrorsRemainClassifiable(t *testing.T) {
	_, err := Sanitize(nil)
	require.True(t, errors.Is(err, ErrEmpty))
}
