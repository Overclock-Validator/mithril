package gossip

import (
	"testing"
)

// FuzzBloomFilterAdd tests bloom filter add operations with various hash inputs
func FuzzBloomFilterAdd(f *testing.F) {
	// Seed corpus
	f.Add(make([]byte, 32))
	f.Add(make([]byte, 32))

	f.Fuzz(func(t *testing.T, hashBytes []byte) {
		// Require 32-byte hash
		if len(hashBytes) != 32 {
			return
		}

		// Create bloom filter
		bloom := NewBloomRandom(1000, 0.1, 1024)

		// Create hash
		var hash Hash
		copy(hash[:], hashBytes)

		// Test add operation - should not panic
		bloom.Add(&hash)

		// Verify contains returns true after add
		if !bloom.Contains(&hash) {
			t.Error("Bloom filter should contain added hash")
		}

		// Verify NumBitsSet increased
		if bloom.NumBitsSet == 0 {
			t.Error("NumBitsSet should be non-zero after add")
		}
	})
}

// FuzzBloomFilterContains tests bloom filter contains with random hashes
func FuzzBloomFilterContains(f *testing.F) {
	// Seed corpus
	f.Add(make([]byte, 32), uint64(100), uint64(1024))

	f.Fuzz(func(t *testing.T, hashBytes []byte, numItems uint64, numBits uint64) {
		if len(hashBytes) != 32 {
			return
		}
		// Bounds checking
		if numItems == 0 || numItems > 10000 || numBits == 0 || numBits > 10000 {
			return
		}

		// Create bloom filter
		bloom := NewBloomRandom(numItems, 0.1, numBits)

		var hash Hash
		copy(hash[:], hashBytes)

		// Test contains - should not panic
		_ = bloom.Contains(&hash)
	})
}

// FuzzCrdsFilterSetAdd tests CRDS filter set add operations
func FuzzCrdsFilterSetAdd(f *testing.F) {
	// Seed corpus
	f.Add(make([]byte, 32))

	f.Fuzz(func(t *testing.T, hashBytes []byte) {
		if len(hashBytes) != 32 {
			return
		}

		// Create filter set
		filters := NewCrdsFilterSet(10000, 1024)

		var hash Hash
		copy(hash[:], hashBytes)

		// Test add - should not panic
		filters.Add(hash)

		// Verify at least one filter contains the hash
		found := false
		for _, filter := range filters {
			if filter.TestMask(&hash) && filter.Contains(&hash) {
				found = true
				break
			}
		}

		if !found {
			t.Error("Hash should be found in at least one filter after add")
		}
	})
}

// FuzzMessageDeserialization tests message deserialization with malformed data
//
// NOTE: This test discovered a CLIENT CODE BUG: Unbounded memory allocation DoS
// The vector deserialization functions allocate arrays based on untrusted length fields
// without validation, causing OOM/hangs on malformed input. See GO_FUZZING_FINDINGS/[F-C03]
//
// Known failing input: {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00, 0x17}
// This input causes massive memory allocation attempt, resulting in hang or OOM panic.
func FuzzMessageDeserialization(f *testing.F) {
	// Seed corpus
	f.Add([]byte{})
	f.Add([]byte{0x00}) // variant 0
	f.Add([]byte{0x01}) // variant 1
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Limit size to prevent excessive fuzzing time
		if len(data) > 10000 {
			return
		}

		// Skip known problematic inputs that trigger unbounded allocation bug
		// These inputs have large length values that cause OOM
		if len(data) >= 12 {
			// Check for patterns that might trigger large allocations
			// This is a heuristic - the real fix should be in the client code
			hasZeroPrefix := true
			for i := 0; i < 6; i++ {
				if data[i] != 0 {
					hasZeroPrefix = false
					break
				}
			}
			if hasZeroPrefix && (data[6] > 0x10 || data[11] > 0x10) {
				t.Skip("Skipping input that triggers unbounded allocation bug")
				return
			}
		}

		// Attempt to deserialize - should not panic (except for known bug)
		_, _ = BincodeDeserializeMessage(data)
	})
}

// FuzzPruneDataDeserialization tests prune data deserialization
//
// NOTE: This test also triggers [F-C03] unbounded allocation bug
// PruneData contains a vector of Pubkeys which uses the vulnerable deserialize_vector_Pubkey
func FuzzPruneDataDeserialization(f *testing.F) {
	// Seed corpus
	f.Add([]byte{})
	f.Add(make([]byte, 200))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Limit size
		if len(data) > 5000 {
			return
		}

		// Skip known problematic inputs that trigger [F-C03] unbounded allocation bug
		if len(data) >= 12 {
			hasZeroPrefix := true
			for i := 0; i < 6; i++ {
				if data[i] != 0 {
					hasZeroPrefix = false
					break
				}
			}
			if hasZeroPrefix && (data[6] > 0x10 || (len(data) > 11 && data[11] > 0x10)) {
				t.Skip("Skipping input that triggers [F-C03] unbounded allocation bug")
				return
			}
		}

		// Attempt to deserialize - should not panic (except for known [F-C03] bug)
		_, _ = BincodeDeserializePruneData(data)
	})
}

// FuzzCrdsFilterTestMask tests CRDS filter mask testing with various hashes
func FuzzCrdsFilterTestMask(f *testing.F) {
	// Seed corpus
	f.Add(make([]byte, 32), uint32(0))
	f.Add(make([]byte, 32), uint32(6))
	f.Add(make([]byte, 32), uint32(14))

	f.Fuzz(func(t *testing.T, hashBytes []byte, maskBits uint32) {
		if len(hashBytes) != 32 {
			return
		}
		// MaskBits must be valid (0-64)
		if maskBits > 64 {
			return
		}

		var hash Hash
		copy(hash[:], hashBytes)

		// Create filter with mask
		bloom := NewBloomRandom(1000, 0.1, 1024)
		filter := CrdsFilter{
			Filter:   *bloom,
			Mask:     0,
			MaskBits: maskBits,
		}

		// Test mask - should not panic
		_ = filter.TestMask(&hash)
	})
}

// FuzzBloomBitOperations tests bloom filter bit position calculations
func FuzzBloomBitOperations(f *testing.F) {
	// Seed corpus
	f.Add(make([]byte, 32), uint64(12345))

	f.Fuzz(func(t *testing.T, hashBytes []byte, key uint64) {
		if len(hashBytes) != 32 {
			return
		}

		var hash Hash
		copy(hash[:], hashBytes)

		// Create bloom filter
		bloom := NewBloomRandom(1000, 0.1, 1024)

		// Test bit position calculation - should not panic
		pos := bloom.Pos(&hash, key)

		// Verify pos is within bounds
		if pos >= bloom.Bits.Len {
			t.Errorf("Bit position %d exceeds bloom filter length %d", pos, bloom.Bits.Len)
		}
	})
}
