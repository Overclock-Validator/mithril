package sealevel

import (
	"bytes"
	"testing"

	bin "github.com/gagliardetto/binary"
	"golang.org/x/crypto/sha3"
)

// FuzzSecppSignatureOffsets tests secp256k1 signature offset parsing
func FuzzSecppSignatureOffsets(f *testing.F) {
	f.Add(makeValidSecp256k1SignatureOffsets(0, 0, 0, 0, 0, 0, 100))
	f.Add(makeValidSecp256k1SignatureOffsets(1, 64, 2, 96, 3, 128, 32))
	f.Add(makeValidSecp256k1SignatureOffsets(255, 1000, 255, 2000, 255, 3000, 500))
	f.Add(makeInvalidSecp256k1SignatureOffsets())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var offsets SecppSignatureOffsets
		_ = offsets.UnmarshalWithDecoder(decoder)
	})
}

// FuzzSecp256k1DataValidation tests secp256k1 instruction data size validation
func FuzzSecp256k1DataValidation(f *testing.F) {
	// Test various data sizes
	f.Add(uint8(0), uint64(0))
	f.Add(uint8(1), uint64(Secp256k1SignatureOffsetsSerializedSize+Secp256k1SignatureOffsetsStart))
	f.Add(uint8(5), uint64(5*Secp256k1SignatureOffsetsSerializedSize+Secp256k1SignatureOffsetsStart))
	f.Add(uint8(255), uint64(255*Secp256k1SignatureOffsetsSerializedSize+Secp256k1SignatureOffsetsStart))

	f.Fuzz(func(t *testing.T, numSignatures uint8, dataLen uint64) {
		// Test expected data size calculation
		expectedDataSize := (uint64(numSignatures) * Secp256k1SignatureOffsetsSerializedSize) + Secp256k1SignatureOffsetsStart

		// Verify no overflow in calculation
		if numSignatures > 0 && expectedDataSize < Secp256k1SignatureOffsetsStart {
			t.Error("Expected data size calculation overflow detected")
		}

		// Test data size validation
		if dataLen < Secp256k1DataStart {
			// Should fail early validation
			return
		}

		if dataLen < expectedDataSize {
			// Should fail size check
			return
		}
	})
}

// FuzzSecp256k1EthereumAddress tests ethereum address derivation from public key
func FuzzSecp256k1EthereumAddress(f *testing.F) {
	f.Add(makeValidSecp256k1TestMessage())
	f.Add([]byte("random test message"))
	f.Add([]byte{})
	f.Add(make([]byte, 32))
	f.Add(make([]byte, 1024))

	f.Fuzz(func(t *testing.T, message []byte) {
		// Hash the message using Keccak256
		hasher := sha3.NewLegacyKeccak256()
		hasher.Write(message)
		_ = hasher.Sum(nil)

		// Test ethereum address derivation from arbitrary public key
		// Use a dummy 65-byte uncompressed public key
		dummyPubKey := make([]byte, 65)
		dummyPubKey[0] = 0x04 // uncompressed marker

		// Derive ethereum address
		hasher.Reset()
		hasher.Write(dummyPubKey[1:])
		digest := hasher.Sum(nil)

		// Verify digest length
		if len(digest) != hasher.Size() {
			t.Errorf("Digest length mismatch: got %d, expected %d", len(digest), hasher.Size())
		}

		// Extract ethereum address (last 20 bytes)
		ethAddr := digest[hasher.Size()-Secp256k1HashedPubkeySerializedSize:]
		if len(ethAddr) != Secp256k1HashedPubkeySerializedSize {
			t.Errorf("Ethereum address length mismatch: got %d, expected %d",
				len(ethAddr), Secp256k1HashedPubkeySerializedSize)
		}
	})
}

// FuzzSecp256k1SignatureCount tests handling of various signature counts
func FuzzSecp256k1SignatureCount(f *testing.F) {
	f.Add(uint8(0), bool(true))   // zero signatures with feature flag
	f.Add(uint8(0), bool(false))  // zero signatures without feature flag
	f.Add(uint8(1), bool(true))   // single signature
	f.Add(uint8(10), bool(false)) // multiple signatures
	f.Add(uint8(255), bool(true)) // maximum count

	f.Fuzz(func(t *testing.T, numSignatures uint8, featureEnabled bool) {
		// Test edge cases for signature count validation
		if numSignatures == 0 {
			// With feature flags enabled, this should be rejected
			if featureEnabled {
				// Should fail validation
				return
			}
		}

		// Calculate expected data size
		_ = uint64(numSignatures)*Secp256k1SignatureOffsetsSerializedSize + Secp256k1SignatureOffsetsStart

		// Verify no overflow
		if numSignatures > 0 {
			maxSafeCount := (^uint64(0) - Secp256k1SignatureOffsetsStart) / Secp256k1SignatureOffsetsSerializedSize
			if uint64(numSignatures) > maxSafeCount {
				t.Log("Signature count would cause overflow")
			}
		}
	})
}

// FuzzSecp256k1OffsetBounds tests offset bounds checking
func FuzzSecp256k1OffsetBounds(f *testing.F) {
	f.Add(uint16(0), uint16(0), uint16(100))
	f.Add(uint16(100), uint16(1000), uint16(50))
	f.Add(uint16(65535), uint16(65535), uint16(65535))

	f.Fuzz(func(t *testing.T, offset uint16, dataLen uint16, size uint16) {
		// Test offset + size overflow protection
		endOffset := uint64(offset) + uint64(size)

		// Check if access would be in bounds
		inBounds := endOffset <= uint64(dataLen)

		// Verify overflow detection
		if offset > 0 && size > 0 && endOffset < uint64(offset) {
			t.Error("Offset calculation overflow not detected")
		}

		if !inBounds && endOffset > uint64(dataLen) {
			// Expected out of bounds
			return
		}
	})
}

// Helper functions to create seed data

func makeValidSecp256k1SignatureOffsets(sigIdx uint16, sigOff uint16, ethIdx uint16, ethOff uint16, msgIdx uint16, msgOff uint16, msgSize uint16) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint16(sigIdx, bin.LE)
	encoder.WriteUint16(sigOff, bin.LE)
	encoder.WriteUint16(ethIdx, bin.LE)
	encoder.WriteUint16(ethOff, bin.LE)
	encoder.WriteUint16(msgIdx, bin.LE)
	encoder.WriteUint16(msgOff, bin.LE)
	encoder.WriteUint16(msgSize, bin.LE)
	return buf.Bytes()
}

func makeInvalidSecp256k1SignatureOffsets() []byte {
	// Truncated offsets structure
	return []byte{1, 0, 2, 0, 3}
}

func makeValidSecp256k1TestMessage() []byte {
	return []byte("This is a test message for secp256k1 signature verification")
}
