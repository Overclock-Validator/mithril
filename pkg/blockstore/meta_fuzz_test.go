package blockstore

import (
	"testing"
)

// FuzzParseSlotKey tests slot key parsing with malformed input
func FuzzParseSlotKey(f *testing.F) {
	// Seed corpus with valid and edge cases
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Add([]byte{0x01, 0x02, 0x03}) // wrong length

	f.Fuzz(func(t *testing.T, keyData []byte) {
		// Should handle any input without panicking
		slot, ok := ParseSlotKey(keyData)

		if len(keyData) != 8 {
			// Expect failure for wrong length
			if ok {
				t.Errorf("Expected failure for %d byte key, got success with slot %d", len(keyData), slot)
			}
			return
		}

		// Should succeed for 8-byte input
		if !ok {
			t.Errorf("Expected success for 8-byte key, got failure")
		}
	})
}

// FuzzParseShredKey tests shred key parsing with malformed input
func FuzzParseShredKey(f *testing.F) {
	// Seed corpus
	f.Add([]byte{})
	validKey := make([]byte, 16)
	f.Add(validKey)
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, keyData []byte) {
		// Should handle any input gracefully
		slot, index, ok := ParseShredKey(keyData)

		// Check expectations based on input
		if !ok && len(keyData) >= 16 {
			// If key is long enough, parse might still fail for other reasons
			t.Logf("Parse failed for %d byte key: slot=%d, index=%d", len(keyData), slot, index)
		}
	})
}

// FuzzMakeSlotKey tests slot key creation with various slot values
func FuzzMakeSlotKey(f *testing.F) {
	// Seed corpus with different slot values
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(1000000))
	f.Add(uint64(^uint64(0))) // max uint64

	f.Fuzz(func(t *testing.T, slot uint64) {
		// Create key
		key := MakeSlotKey(slot)

		// Verify roundtrip
		parsedSlot, ok := ParseSlotKey(key[:])
		if !ok {
			t.Errorf("Failed to parse key created from slot %d", slot)
			return
		}

		if parsedSlot != slot {
			t.Errorf("Roundtrip failed: got %d, want %d", parsedSlot, slot)
		}
	})
}
