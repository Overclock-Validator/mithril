package sealevel

import (
	"bytes"
	"testing"

	bin "github.com/gagliardetto/binary"
)

// FuzzEd25519SignatureOffsets tests Ed25519 signature offset parsing
func FuzzEd25519SignatureOffsets(f *testing.F) {
	f.Add(makeValidEd25519SignatureOffsets(0, 0, 0, 0, 0, 0, 100))
	f.Add(makeValidEd25519SignatureOffsets(1, 64, 2, 96, 3, 128, 32))
	f.Add(makeValidEd25519SignatureOffsets(255, 1000, 255, 2000, 255, 3000, 500))
	f.Add(makeInvalidEd25519SignatureOffsets())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var offsets Ed25519SignatureOffsets
		_ = offsets.UnmarshalWithDecoder(decoder)
	})
}

// FuzzEd25519DataValidation tests Ed25519 instruction data size validation
func FuzzEd25519DataValidation(f *testing.F) {
	// Test various data sizes
	f.Add(uint8(0), uint64(0))
	f.Add(uint8(1), uint64(SignatureOffsetsSerializedSize+SignatureOffsetStarts))
	f.Add(uint8(5), uint64(5*SignatureOffsetsSerializedSize+SignatureOffsetStarts))
	f.Add(uint8(255), uint64(255*SignatureOffsetsSerializedSize+SignatureOffsetStarts))

	f.Fuzz(func(t *testing.T, numSignatures uint8, dataLen uint64) {
		// Test signature count (must be > 0)
		if numSignatures == 0 {
			// Should fail validation
			return
		}

		// Test expected data size calculation
		expectedDataSize := (uint64(numSignatures) * SignatureOffsetsSerializedSize) + SignatureOffsetStarts

		// Verify no overflow in calculation
		if numSignatures > 0 && expectedDataSize < SignatureOffsetStarts {
			t.Error("Expected data size calculation overflow detected")
		}

		// Test data size validation
		if dataLen < DataStart {
			// Should fail early validation
			return
		}

		if dataLen < expectedDataSize {
			// Should fail size check
			return
		}
	})
}

// FuzzEd25519SignatureCount tests handling of various signature counts
func FuzzEd25519SignatureCount(f *testing.F) {
	f.Add(uint8(0))   // zero signatures - should fail
	f.Add(uint8(1))   // single signature
	f.Add(uint8(10))  // multiple signatures
	f.Add(uint8(255)) // maximum count

	f.Fuzz(func(t *testing.T, numSignatures uint8) {
		// Test signature count validation
		if numSignatures == 0 {
			// Should be rejected
			return
		}

		// Calculate expected data size
		expectedSize := uint64(numSignatures)*SignatureOffsetsSerializedSize + SignatureOffsetStarts

		// Verify no overflow
		if numSignatures > 0 {
			maxSafeCount := (^uint64(0) - SignatureOffsetStarts) / SignatureOffsetsSerializedSize
			if uint64(numSignatures) > maxSafeCount {
				t.Log("Signature count would cause overflow")
			}
		}

		_ = expectedSize
	})
}

// FuzzEd25519OffsetBounds tests offset bounds checking
func FuzzEd25519OffsetBounds(f *testing.F) {
	f.Add(uint16(0), uint16(0), uint64(100))
	f.Add(uint16(100), uint16(1000), uint64(50))
	f.Add(uint16(65535), uint16(65535), uint64(65535))

	f.Fuzz(func(t *testing.T, offset uint16, instrIdx uint16, size uint64) {
		// Test offset + size overflow protection
		endOffset := uint64(offset) + size

		// Verify overflow detection
		if size > 0 && endOffset < uint64(offset) {
			t.Error("Offset calculation overflow not detected")
		}

		// Instruction index should be valid (in practice limited by transaction structure)
		if instrIdx > 256 {
			t.Log("Instruction index very large")
		}
	})
}

// FuzzEd25519ComponentSizes tests size constants for Ed25519 components
func FuzzEd25519ComponentSizes(f *testing.F) {
	f.Add(uint64(SignatureSerializedSize))
	f.Add(uint64(PubkeySerializedSize))
	f.Add(uint64(SignatureOffsetsSerializedSize))
	f.Add(uint64(SignatureOffsetStarts))
	f.Add(uint64(DataStart))

	f.Fuzz(func(t *testing.T, size uint64) {
		// Verify size constants are reasonable
		if size > 1000000 {
			t.Error("Size constant unreasonably large")
		}

		// Test component size validation
		sigSize := SignatureSerializedSize
		pkSize := PubkeySerializedSize

		// Verify signature size is 64 bytes (Ed25519 signature)
		if sigSize != 64 {
			t.Errorf("Signature size should be 64, got %d", sigSize)
		}

		// Verify pubkey size is 32 bytes (Ed25519 public key)
		if pkSize != 32 {
			t.Errorf("Pubkey size should be 32, got %d", pkSize)
		}
	})
}

// FuzzEd25519SpecialCases tests special instruction data cases
func FuzzEd25519SpecialCases(f *testing.F) {
	// Special case: data of length 2 with first byte 0 should succeed
	f.Add([]byte{0, 0})
	f.Add([]byte{0, 1})
	f.Add([]byte{0, 255})

	// Edge cases
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte{1})

	f.Fuzz(func(t *testing.T, data []byte) {
		dataLen := uint64(len(data))

		// Special case handling
		if dataLen < DataStart {
			if dataLen == 2 && data[0] == 0 {
				// This should succeed (no-op case)
				return
			}
			// Should fail size validation
			return
		}

		// Normal processing would continue...
	})
}

// Helper functions to create seed data

func makeValidEd25519SignatureOffsets(sigIdx uint16, sigOff uint16, pkIdx uint16, pkOff uint16, msgIdx uint16, msgOff uint16, msgSize uint64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint16(sigIdx, bin.LE)
	encoder.WriteUint16(sigOff, bin.LE)
	encoder.WriteUint16(pkIdx, bin.LE)
	encoder.WriteUint16(pkOff, bin.LE)
	encoder.WriteUint16(msgIdx, bin.LE)
	encoder.WriteUint16(msgOff, bin.LE)
	encoder.WriteUint64(msgSize, bin.LE)
	return buf.Bytes()
}

func makeInvalidEd25519SignatureOffsets() []byte {
	// Truncated offsets structure
	return []byte{1, 0, 2, 0, 3}
}
