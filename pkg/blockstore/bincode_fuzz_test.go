package blockstore

import (
	"testing"

	bin "github.com/gagliardetto/binary"
)

// FuzzParseBincode tests generic bincode parsing with malformed data
func FuzzParseBincode(f *testing.F) {
	// Seed corpus with various data
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add(make([]byte, 100))
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Limit size to prevent excessive memory usage
		if len(data) > 10000 {
			return
		}

		// Try parsing as SlotMeta (common structure)
		result, err := ParseBincode[SlotMeta](data)

		// Error expected for most random data
		if err != nil {
			// Expected for malformed data
			return
		}

		// If successful, verify result is non-nil
		if result == nil {
			t.Error("ParseBincode returned nil result with nil error")
		}
	})
}

// FuzzSubEntriesUnmarshal tests SubEntries deserialization with malformed data
func FuzzSubEntriesUnmarshal(f *testing.F) {
	// Seed corpus
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})
	f.Add(make([]byte, 50))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Limit size
		if len(data) > 5000 {
			return
		}

		decoder := bin.NewBinDecoder(data)
		subEntries := &SubEntries{}

		// Should handle malformed data gracefully
		err := subEntries.UnmarshalWithDecoder(decoder)

		// Error expected for most random data
		if err != nil {
			// Expected
			return
		}

		// If successful, basic verification
		_ = subEntries
	})
}
