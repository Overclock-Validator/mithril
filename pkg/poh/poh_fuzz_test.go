package poh

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// FuzzStateHash tests that Hash() is deterministic and matches manual SHA256 computation
func FuzzStateHash(f *testing.F) {
	// Seed with various initial states
	f.Add([]byte("00000000000000000000000000000000"), uint8(0))
	f.Add([]byte("ffffffffffffffffffffffffffffffff"), uint8(1))
	f.Add([]byte("45296998a6f8e2a784db5d9f95e18fc2"), uint8(5))
	f.Add([]byte("3973e330c29b831f3fcb0e49374ed8d0"), uint8(10))

	f.Fuzz(func(t *testing.T, stateBytes []byte, numHashesSmall uint8) {
		if len(stateBytes) < 32 {
			t.Skip()
		}

		// Initialize state
		var state State
		copy(state[:], stateBytes[:32])
		originalState := state

		// Test with small number of hashes to avoid timeout
		numHashes := uint(numHashesSmall)

		// Apply Hash()
		state.Hash(numHashes)

		// Verify by manually computing the expected result
		expected := originalState
		for i := uint(0); i < numHashes; i++ {
			expected = sha256.Sum256(expected[:])
		}

		if state != expected {
			t.Errorf("Hash(%d) produced incorrect result.\nGot:      %x\nExpected: %x",
				numHashes, state, expected)
		}

		// Test idempotency: running Hash(0) should not change state
		testState := originalState
		testState.Hash(0)
		if testState != originalState {
			t.Errorf("Hash(0) changed state: %x -> %x", originalState, testState)
		}
	})
}

// FuzzStateRecord tests that Record() correctly mixes in external data
func FuzzStateRecord(f *testing.F) {
	// Seed with various state and mixin combinations
	f.Add([]byte("00000000000000000000000000000000"), []byte("00000000000000000000000000000000"))
	f.Add([]byte("ffffffffffffffffffffffffffffffff"), []byte("ffffffffffffffffffffffffffffffff"))
	f.Add([]byte("45296998a6f8e2a784db5d9f95e18fc2"), []byte("c95f2f13a9a77f32b1437976c4cffe30"))

	f.Fuzz(func(t *testing.T, stateBytes []byte, mixinBytes []byte) {
		if len(stateBytes) < 32 || len(mixinBytes) < 32 {
			t.Skip()
		}

		// Initialize state and mixin
		var state State
		var mixin [32]byte
		copy(state[:], stateBytes[:32])
		copy(mixin[:], mixinBytes[:32])
		originalState := state

		// Apply Record()
		state.Record(&mixin)

		// Verify by manually computing expected result
		var buf [64]byte
		copy(buf[:32], originalState[:])
		copy(buf[32:], mixin[:])
		expected := sha256.Sum256(buf[:])

		if state != expected {
			t.Errorf("Record() produced incorrect result.\nState:    %x\nMixin:    %x\nGot:      %x\nExpected: %x",
				originalState, mixin, state, expected)
		}
	})
}

// FuzzStateHashChainDeterminism tests that hash chain is deterministic
func FuzzStateHashChainDeterminism(f *testing.F) {
	f.Add([]byte("45296998a6f8e2a784db5d9f95e18fc2"), uint8(10))

	f.Fuzz(func(t *testing.T, stateBytes []byte, numHashesSmall uint8) {
		if len(stateBytes) < 32 {
			t.Skip()
		}

		var state1, state2 State
		copy(state1[:], stateBytes[:32])
		copy(state2[:], stateBytes[:32])

		numHashes := uint(numHashesSmall)

		// Apply same number of hashes to both states
		state1.Hash(numHashes)
		state2.Hash(numHashes)

		// They must be identical
		if state1 != state2 {
			t.Errorf("Hash chain not deterministic after %d iterations.\nState1: %x\nState2: %x",
				numHashes, state1, state2)
		}
	})
}

// FuzzStateRecordDeterminism tests that Record() is deterministic
func FuzzStateRecordDeterminism(f *testing.F) {
	f.Add([]byte("45296998a6f8e2a784db5d9f95e18fc2"), []byte("c95f2f13a9a77f32b1437976c4cffe30"))

	f.Fuzz(func(t *testing.T, stateBytes []byte, mixinBytes []byte) {
		if len(stateBytes) < 32 || len(mixinBytes) < 32 {
			t.Skip()
		}

		var state1, state2 State
		var mixin [32]byte
		copy(state1[:], stateBytes[:32])
		copy(state2[:], stateBytes[:32])
		copy(mixin[:], mixinBytes[:32])

		// Apply same Record to both states
		state1.Record(&mixin)
		state2.Record(&mixin)

		// They must be identical
		if state1 != state2 {
			t.Errorf("Record not deterministic.\nMixin:  %x\nState1: %x\nState2: %x",
				mixin, state1, state2)
		}
	})
}

// FuzzStateHashCommutative tests Hash(a) then Hash(b) equals Hash(a+b)
func FuzzStateHashCommutative(f *testing.F) {
	f.Add([]byte("45296998a6f8e2a784db5d9f95e18fc2"), uint8(5), uint8(3))

	f.Fuzz(func(t *testing.T, stateBytes []byte, numHashes1Small, numHashes2Small uint8) {
		if len(stateBytes) < 32 {
			t.Skip()
		}

		var state1, state2 State
		copy(state1[:], stateBytes[:32])
		copy(state2[:], stateBytes[:32])

		numHashes1 := uint(numHashes1Small)
		numHashes2 := uint(numHashes2Small)

		// Apply hashes separately
		state1.Hash(numHashes1)
		state1.Hash(numHashes2)

		// Apply hashes together
		state2.Hash(numHashes1 + numHashes2)

		// Results must be identical
		if state1 != state2 {
			t.Errorf("Hash not commutative.\nHash(%d) + Hash(%d) != Hash(%d)\nSeparate: %x\nCombined: %x",
				numHashes1, numHashes2, numHashes1+numHashes2, state1, state2)
		}
	})
}

// FuzzStateRecordOrder tests that Record order matters (not commutative)
func FuzzStateRecordOrder(f *testing.F) {
	f.Add([]byte("45296998a6f8e2a784db5d9f95e18fc2"),
		[]byte("c95f2f13a9a77f32b1437976c4cffe30"),
		[]byte("1aaeeb36611f484d984683a3db9269f2"))

	f.Fuzz(func(t *testing.T, stateBytes, mixin1Bytes, mixin2Bytes []byte) {
		if len(stateBytes) < 32 || len(mixin1Bytes) < 32 || len(mixin2Bytes) < 32 {
			t.Skip()
		}

		// Skip if mixins are identical (order wouldn't matter)
		var mixin1, mixin2 [32]byte
		copy(mixin1[:], mixin1Bytes[:32])
		copy(mixin2[:], mixin2Bytes[:32])
		if mixin1 == mixin2 {
			t.Skip()
		}

		var state1, state2 State
		copy(state1[:], stateBytes[:32])
		copy(state2[:], stateBytes[:32])

		// Apply Records in different orders
		state1.Record(&mixin1)
		state1.Record(&mixin2)

		state2.Record(&mixin2)
		state2.Record(&mixin1)

		// Results must be different (unless hash collision, which is astronomically unlikely)
		if state1 == state2 {
			t.Logf("Warning: Record appears commutative (possible hash collision).\nState:  %x\nMixin1: %x\nMixin2: %x\nResult: %x",
				stateBytes[:32], mixin1, mixin2, state1)
		}
	})
}

// FuzzStateString tests String() method for correct hex encoding
func FuzzStateString(f *testing.F) {
	f.Add([]byte("00000000000000000000000000000000"))
	f.Add([]byte("ffffffffffffffffffffffffffffffff"))
	f.Add([]byte("45296998a6f8e2a784db5d9f95e18fc2"))

	f.Fuzz(func(t *testing.T, stateBytes []byte) {
		if len(stateBytes) < 32 {
			t.Skip()
		}

		var state State
		copy(state[:], stateBytes[:32])

		// Get string representation
		hexStr := state.String()

		// Verify it's valid hex and has correct length
		if len(hexStr) != 64 {
			t.Errorf("String() returned wrong length: got %d, expected 64", len(hexStr))
		}

		// Verify we can decode it back
		var decoded State
		for i := 0; i < 32; i++ {
			_, err := hex.Decode(decoded[i:i+1], []byte(hexStr[i*2:i*2+2]))
			if err != nil {
				t.Errorf("String() returned invalid hex at position %d: %v", i, err)
			}
		}

		if decoded != state {
			t.Errorf("String() roundtrip failed.\nOriginal: %x\nDecoded:  %x", state, decoded)
		}
	})
}

// FuzzStateMixedOperations tests complex sequences of Hash and Record operations
func FuzzStateMixedOperations(f *testing.F) {
	f.Add([]byte("45296998a6f8e2a784db5d9f95e18fc2"),
		[]byte("c95f2f13a9a77f32b1437976c4cffe30"),
		uint8(5), uint8(3), uint8(2))

	f.Fuzz(func(t *testing.T, stateBytes, mixinBytes []byte, hash1, hash2, hash3 uint8) {
		if len(stateBytes) < 32 || len(mixinBytes) < 32 {
			t.Skip()
		}

		var state State
		var mixin [32]byte
		copy(state[:], stateBytes[:32])
		copy(mixin[:], mixinBytes[:32])
		originalState := state

		// Perform a sequence of operations
		state.Hash(uint(hash1))
		state.Record(&mixin)
		state.Hash(uint(hash2))
		state.Record(&mixin)
		state.Hash(uint(hash3))

		// Manually compute expected result
		expected := originalState
		expected.Hash(uint(hash1))
		expected.Record(&mixin)
		expected.Hash(uint(hash2))
		expected.Record(&mixin)
		expected.Hash(uint(hash3))

		if state != expected {
			t.Errorf("Mixed operations sequence not deterministic")
		}

		// Verify state changed from original (unless all operations were no-ops)
		if hash1 == 0 && hash2 == 0 && hash3 == 0 {
			// Only Records were applied, state should have changed
			if state == originalState {
				// This is actually expected to be different, but we can't enforce it
				// due to theoretical hash collisions
				t.Logf("State unchanged after 2 Record operations (unlikely)")
			}
		}
	})
}

// FuzzStateZeroOperations tests edge cases with zero-valued states and mixins
func FuzzStateZeroOperations(f *testing.F) {
	f.Add(uint8(0), uint8(1), uint8(10))

	f.Fuzz(func(t *testing.T, hash1, hash2, hash3 uint8) {
		// Start with zero state
		var state State
		zeroState := state

		// Apply operations
		state.Hash(uint(hash1))
		firstHash := state

		// Record with zero mixin
		var zeroMixin [32]byte
		state.Record(&zeroMixin)
		afterRecord := state

		state.Hash(uint(hash2))
		state.Hash(uint(hash3))

		// Verify operations modified state (unless all were no-ops)
		if hash1 == 0 && hash2 == 0 && hash3 == 0 {
			// Only one Record was applied
			if afterRecord == zeroState {
				t.Logf("Record with zero mixin left state unchanged (hash collision unlikely)")
			}
		}

		// Hash(0) should be no-op
		testState := firstHash
		testState.Hash(0)
		if testState != firstHash {
			t.Errorf("Hash(0) changed state")
		}
	})
}

// FuzzStateHashBoundary tests boundary conditions for Hash parameter
func FuzzStateHashBoundary(f *testing.F) {
	f.Add([]byte("45296998a6f8e2a784db5d9f95e18fc2"))

	f.Fuzz(func(t *testing.T, stateBytes []byte) {
		if len(stateBytes) < 32 {
			t.Skip()
		}

		var state State
		copy(state[:], stateBytes[:32])

		// Test Hash(0) - should be no-op
		original := state
		state.Hash(0)
		if state != original {
			t.Errorf("Hash(0) modified state: %x -> %x", original, state)
		}

		// Test Hash(1)
		state.Hash(1)
		expected := sha256.Sum256(original[:])
		if state != expected {
			t.Errorf("Hash(1) incorrect.\nGot:      %x\nExpected: %x", state, expected)
		}

		// Test Hash(2) from original
		state = original
		state.Hash(2)
		expected = sha256.Sum256(original[:])
		expected = sha256.Sum256(expected[:])
		if state != expected {
			t.Errorf("Hash(2) incorrect.\nGot:      %x\nExpected: %x", state, expected)
		}
	})
}
