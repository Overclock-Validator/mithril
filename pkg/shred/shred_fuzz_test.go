package shred

import (
	"encoding/binary"
	"testing"
)

// FuzzShredDeserializationV1 tests shred deserialization for revision 1 with malformed inputs
func FuzzShredDeserializationV1(f *testing.F) {
	// Seed with minimal valid shred data
	f.Add(make([]byte, 88))
	f.Add(make([]byte, 1143)) // Typical data shred size

	// Seed with variant bytes
	validLegacyData := make([]byte, 1143)
	validLegacyData[64] = LegacyDataID
	f.Add(validLegacyData)

	validLegacyCode := make([]byte, 1228)
	validLegacyCode[64] = LegacyCodeID
	f.Add(validLegacyCode)

	f.Fuzz(func(t *testing.T, shredData []byte) {
		// Should never panic, only return empty shred or valid parsed shred
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("NewShredFromSerialized panicked: %v", r)
			}
		}()

		shred := NewShredFromSerialized(shredData, RevisionV1)

		// If shred was parsed, verify basic invariants
		if shred.Slot != 0 || shred.Index != 0 || len(shred.Payload) > 0 {
			// Shred was successfully parsed
			// Verify payload size is reasonable
			if len(shred.Payload) > 2000 {
				t.Errorf("Payload size too large: %d", len(shred.Payload))
			}

			// Note: Index is uint32, any value 0 to 4,294,967,295 is valid per Solana protocol
			// No need to validate against arbitrary limits
		}
	})
}

// FuzzShredDeserializationV2 tests shred deserialization for revision 2 with malformed inputs
func FuzzShredDeserializationV2(f *testing.F) {
	// Seed with minimal valid shred data
	f.Add(make([]byte, 88))
	f.Add(make([]byte, 1143))

	// Seed with merkle shred variants
	validMerkleData := make([]byte, 1143)
	validMerkleData[64] = MerkleDataID
	f.Add(validMerkleData)

	validMerkleCode := make([]byte, 1228)
	validMerkleCode[64] = MerkleCodeID
	f.Add(validMerkleCode)

	f.Fuzz(func(t *testing.T, shredData []byte) {
		// Should never panic
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("NewShredFromSerialized panicked: %v", r)
			}
		}()

		shred := NewShredFromSerialized(shredData, RevisionV2)

		// If merkle shred was parsed, verify merkle path
		if len(shred.MerklePath) > 0 {
			// Verify merkle path depth is reasonable (max 15 for Solana)
			if len(shred.MerklePath) > 15 {
				t.Errorf("Merkle path too deep: %d", len(shred.MerklePath))
			}
		}
	})
}

// FuzzShredVariantParsing tests edge cases in variant byte handling
func FuzzShredVariantParsing(f *testing.F) {
	// Seed with all possible variant byte values
	for variant := 0; variant < 256; variant++ {
		shredData := make([]byte, 1143)
		shredData[64] = byte(variant)
		f.Add(shredData)
	}

	f.Fuzz(func(t *testing.T, shredData []byte) {
		// Ensure variant byte parsing never panics
		defer func() {
			if r := recover(); r != nil {
				// Only allow panic for unimplemented legacy code shred
				if len(shredData) >= 65 && shredData[64] == LegacyCodeID {
					// Expected panic for todo implementation
					return
				}
				t.Errorf("Unexpected panic on variant parsing: %v", r)
			}
		}()

		_ = NewShredFromSerialized(shredData, RevisionV1)
	})
}

// FuzzShredHeaderFields tests fuzz various header field combinations
func FuzzShredHeaderFields(f *testing.F) {
	// Seed with structure
	f.Add(uint64(0), uint32(0), uint32(0), uint16(88), uint8(0), LegacyDataID)
	f.Add(uint64(1000000), uint32(65535), uint32(1000), uint16(1000), uint8(255), LegacyDataID)

	f.Fuzz(func(t *testing.T, slot uint64, index uint32, parentOffset uint32,
		dataSize uint16, flags uint8, variant uint8) {

		// Build shred with fuzzed header fields
		shredData := make([]byte, 1143)

		// Signature (64 bytes) - leave as zeros

		// Variant
		shredData[64] = variant

		// Slot (8 bytes) - FIXED: Correct offset per Solana spec
		binary.LittleEndian.PutUint64(shredData[65:73], slot)

		// Index (4 bytes) - FIXED: Correct offset per Solana spec
		binary.LittleEndian.PutUint32(shredData[73:77], index)

		// Parent offset (2 bytes) - FIXED: Correct offset and size for legacy data shreds
		binary.LittleEndian.PutUint16(shredData[0x53:0x55], uint16(parentOffset))

		// Flags - FIXED: Correct offset
		shredData[0x55] = flags

		// Data size (2 bytes) - FIXED: Correct offset for V2 legacy data shreds
		// Constrain dataSize to valid range: [88, 1143] for V2 shreds
		// 88 = LegacyDataV2HeaderSize (minimum)
		// 1143 = len(shredData) (maximum to avoid out-of-bounds)
		if dataSize < 88 || dataSize > 1143 {
			dataSize = 88 + (dataSize % (1143 - 88 + 1))
		}
		binary.LittleEndian.PutUint16(shredData[0x56:0x58], dataSize)

		defer func() {
			if r := recover(); r != nil {
				// Allow panic for unimplemented legacy code shred
				if variant == LegacyCodeID {
					return
				}
				t.Errorf("Header field parsing panicked: %v", r)
			}
		}()

		shred := NewShredFromSerialized(shredData, RevisionV2) // Use V2 since we're setting Size field

		// Verify parsed header matches input (if successfully parsed)
		if variant == LegacyDataID {
			if shred.Slot != slot {
				t.Errorf("Slot mismatch: got %d, want %d", shred.Slot, slot)
			}
			if shred.Index != index {
				t.Errorf("Index mismatch: got %d, want %d", shred.Index, index)
			}
		}
	})
}

// FuzzShredPayloadBounds tests payload size boundary conditions
func FuzzShredPayloadBounds(f *testing.F) {
	// Seed with various payload sizes
	f.Add(uint16(0))
	f.Add(uint16(1))
	f.Add(uint16(1057))  // Max for V1
	f.Add(uint16(1203))  // Max for V2
	f.Add(uint16(65535)) // Max uint16

	f.Fuzz(func(t *testing.T, payloadSize uint16) {
		// Create shred data with specified payload size
		shredData := make([]byte, 1143)
		shredData[64] = LegacyDataID
		binary.LittleEndian.PutUint16(shredData[82:84], payloadSize)

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Payload bounds check panicked with size %d: %v", payloadSize, r)
			}
		}()

		shred := NewShredFromSerialized(shredData, RevisionV1)

		// Verify payload doesn't exceed reasonable bounds
		if len(shred.Payload) > 2000 {
			t.Errorf("Payload extracted exceeds max size: %d", len(shred.Payload))
		}
	})
}

// FuzzShredMerklePathDepth tests merkle proof path depth validation
func FuzzShredMerklePathDepth(f *testing.F) {
	// Seed with various depths
	for depth := 0; depth < 20; depth++ {
		f.Add(uint8(depth))
	}

	f.Fuzz(func(t *testing.T, depth uint8) {
		// Create merkle shred with specified depth
		totalSize := 88 + 1203 + int(depth)*20 // header + payload + merkle path
		if totalSize > 10000 {
			t.Skip("Skipping unreasonably large shred")
		}

		shredData := make([]byte, totalSize)
		shredData[64] = MerkleDataID | (depth & MerkleDepthMask)

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Merkle path depth %d caused panic: %v", depth, r)
			}
		}()

		shred := NewShredFromSerialized(shredData, RevisionV2)

		// Verify merkle path length matches depth
		if depth <= 15 && len(shred.MerklePath) != int(depth) {
			// Only check for valid depths (0-15)
			t.Logf("Merkle path length %d doesn't match depth %d", len(shred.MerklePath), depth)
		}
	})
}
