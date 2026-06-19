package sealevel

import (
	"bytes"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/solana"
)

// FuzzCreateProgramAddress tests Program Derived Address creation
func FuzzCreateProgramAddress(f *testing.F) {
	// Seed with various seed patterns
	f.Add([]byte("test"), make([]byte, 32))
	f.Add([]byte{}, make([]byte, 32))
	f.Add(bytes.Repeat([]byte{0xff}, 32), make([]byte, 32))
	f.Add([]byte("seed1"), bytes.Repeat([]byte{0x01}, 32))

	f.Fuzz(func(t *testing.T, seed []byte, programId []byte) {
		// Program ID must be 32 bytes
		if len(programId) != 32 {
			t.Skip("programId must be 32 bytes")
		}

		// Seed must not exceed MaxSeedLen
		if len(seed) > MaxSeedLen {
			t.Skip("seed too long")
		}

		// Create PDA with single seed
		seeds := [][]byte{seed}

		// Test CreateProgramAddress
		address, err := solana.CreateProgramAddressBytes(seeds, programId)

		if err != nil {
			// Expected error: address is on curve (not a valid PDA)
			return
		}

		// Verify address is 32 bytes
		if len(address) != 32 {
			t.Errorf("Address length is %d, want 32", len(address))
		}

		// Verify determinism - same inputs produce same address
		address2, err2 := solana.CreateProgramAddressBytes(seeds, programId)
		if err2 != nil {
			t.Errorf("Second CreateProgramAddress failed: %v", err2)
			return
		}

		if !bytes.Equal(address, address2) {
			t.Errorf("CreateProgramAddress not deterministic")
		}
	})
}

// FuzzCreateProgramAddressMultiSeed tests PDA creation with multiple seeds
func FuzzCreateProgramAddressMultiSeed(f *testing.F) {
	// Seed with various patterns
	f.Add(uint8(1), make([]byte, 32))
	f.Add(uint8(2), make([]byte, 32))
	f.Add(uint8(5), make([]byte, 32))
	f.Add(uint8(16), make([]byte, 32)) // Maximum seeds

	f.Fuzz(func(t *testing.T, numSeeds uint8, programId []byte) {
		// Program ID must be 32 bytes
		if len(programId) != 32 {
			t.Skip("programId must be 32 bytes")
		}

		// Number of seeds must not exceed MaxSeeds
		if numSeeds == 0 || numSeeds > MaxSeeds {
			t.Skip("invalid number of seeds")
		}

		// Create multiple seeds
		seeds := make([][]byte, numSeeds)
		for i := uint8(0); i < numSeeds; i++ {
			// Create varied seed data (small to avoid MaxSeedLen issues)
			seedData := make([]byte, 8)
			for j := range seedData {
				seedData[j] = byte(i*7 + uint8(j))
			}
			seeds[i] = seedData
		}

		// Test CreateProgramAddress
		address, err := solana.CreateProgramAddressBytes(seeds, programId)

		if err != nil {
			// Expected error: address is on curve
			return
		}

		// Verify address is 32 bytes
		if len(address) != 32 {
			t.Errorf("Address length is %d, want 32", len(address))
		}

		// Verify determinism
		address2, err2 := solana.CreateProgramAddressBytes(seeds, programId)
		if err2 != nil {
			t.Errorf("Second CreateProgramAddress failed: %v", err2)
			return
		}

		if !bytes.Equal(address, address2) {
			t.Errorf("CreateProgramAddress not deterministic")
		}

		// Verify changing seed order produces different address
		if numSeeds >= 2 {
			swappedSeeds := make([][]byte, numSeeds)
			copy(swappedSeeds, seeds)
			// Swap first two seeds
			swappedSeeds[0], swappedSeeds[1] = swappedSeeds[1], swappedSeeds[0]

			if !bytes.Equal(seeds[0], seeds[1]) {
				// Only test if seeds are different
				address3, err3 := solana.CreateProgramAddressBytes(swappedSeeds, programId)
				if err3 == nil && bytes.Equal(address, address3) {
					t.Errorf("Swapped seeds produced same address")
				}
			}
		}
	})
}

// FuzzCreateProgramAddressSeedLength tests various seed lengths
func FuzzCreateProgramAddressSeedLength(f *testing.F) {
	// Seed with various lengths
	f.Add(uint8(0), make([]byte, 32))
	f.Add(uint8(1), make([]byte, 32))
	f.Add(uint8(16), make([]byte, 32))
	f.Add(uint8(32), make([]byte, 32)) // MaxSeedLen

	f.Fuzz(func(t *testing.T, seedLen uint8, programId []byte) {
		// Program ID must be 32 bytes
		if len(programId) != 32 {
			t.Skip("programId must be 32 bytes")
		}

		// Seed length must not exceed MaxSeedLen
		if seedLen > MaxSeedLen {
			t.Skip("seed length exceeds maximum")
		}

		// Create seed of specified length
		seed := make([]byte, seedLen)
		for i := range seed {
			seed[i] = byte(i % 256)
		}

		seeds := [][]byte{seed}

		// Test CreateProgramAddress
		address, err := solana.CreateProgramAddressBytes(seeds, programId)

		if err != nil {
			// Expected error: address is on curve
			return
		}

		// Verify address is 32 bytes
		if len(address) != 32 {
			t.Errorf("Address length is %d, want 32", len(address))
		}

		// Empty seed should still work
		if seedLen == 0 {
			// Verify we can create PDA with empty seed
			if len(address) != 32 {
				t.Errorf("Empty seed produced invalid address")
			}
		}
	})
}

// FuzzFindProgramAddressLogic tests the bump seed search logic
func FuzzFindProgramAddressLogic(f *testing.F) {
	// Seed with various patterns
	f.Add([]byte("test"), make([]byte, 32))
	f.Add([]byte{0x01}, make([]byte, 32))
	f.Add([]byte("solana"), bytes.Repeat([]byte{0xff}, 32))

	f.Fuzz(func(t *testing.T, seed []byte, programId []byte) {
		// Program ID must be 32 bytes
		if len(programId) != 32 {
			t.Skip("programId must be 32 bytes")
		}

		// Seed must not exceed MaxSeedLen
		if len(seed) > MaxSeedLen {
			t.Skip("seed too long")
		}

		// Manually search for valid PDA (similar to TryFindProgramAddress)
		foundValid := false
		var validBump uint8
		var validAddress []byte

		for bumpSeed := uint8(255); bumpSeed > 0; bumpSeed-- {
			seedsWithBump := make([][]byte, 0)
			seedsWithBump = append(seedsWithBump, seed)
			seedsWithBump = append(seedsWithBump, []byte{bumpSeed})

			address, err := solana.CreateProgramAddressBytes(seedsWithBump, programId)
			if err == nil {
				// Found valid PDA
				foundValid = true
				validBump = bumpSeed
				validAddress = address
				break
			}
		}

		if !foundValid {
			// No valid PDA found in range [255, 1]
			// This is possible but rare
			return
		}

		// Verify the found address is consistent
		seedsWithFoundBump := make([][]byte, 0)
		seedsWithFoundBump = append(seedsWithFoundBump, seed)
		seedsWithFoundBump = append(seedsWithFoundBump, []byte{validBump})

		verifyAddress, err := solana.CreateProgramAddressBytes(seedsWithFoundBump, programId)
		if err != nil {
			t.Errorf("Failed to verify found PDA: %v", err)
			return
		}

		if !bytes.Equal(validAddress, verifyAddress) {
			t.Errorf("Found PDA verification failed")
		}

		// Verify bump seed is the canonical one (highest valid bump)
		// Try higher bump seeds to ensure none are valid
		for testBump := uint8(255); testBump > validBump; testBump-- {
			testSeeds := make([][]byte, 0)
			testSeeds = append(testSeeds, seed)
			testSeeds = append(testSeeds, []byte{testBump})

			testAddress, err := solana.CreateProgramAddressBytes(testSeeds, programId)
			if err == nil {
				t.Errorf("Found higher valid bump %d than canonical %d", testBump, validBump)
				t.Logf("Higher bump address: %x", testAddress)
			}
		}
	})
}

// FuzzProgramAddressConsistency tests hash collision behavior in PDA generation
// Note: Solana PDA hashing intentionally allows collisions - seeds are concatenated
// before hashing, so ["a", "b"] == ["ab"] and ["", "x"] == ["x", ""]
func FuzzProgramAddressConsistency(f *testing.F) {
	// Seed with patterns
	f.Add([]byte("a"), []byte("b"), make([]byte, 32))
	f.Add([]byte("test"), []byte("data"), make([]byte, 32))

	f.Fuzz(func(t *testing.T, seed1 []byte, seed2 []byte, programId []byte) {
		// Program ID must be 32 bytes
		if len(programId) != 32 {
			t.Skip("programId must be 32 bytes")
		}

		// Seeds must not exceed MaxSeedLen
		if len(seed1) > MaxSeedLen || len(seed2) > MaxSeedLen {
			t.Skip("seed too long")
		}

		// Test with seeds in order [seed1, seed2]
		seeds1 := [][]byte{seed1, seed2}
		address1, err1 := solana.CreateProgramAddressBytes(seeds1, programId)

		// Test with seeds in reverse order [seed2, seed1]
		seeds2 := [][]byte{seed2, seed1}
		address2, err2 := solana.CreateProgramAddressBytes(seeds2, programId)

		// Both should succeed or both should fail
		if (err1 == nil) != (err2 == nil) {
			// Different error status is fine - they're different seeds
			return
		}

		if err1 == nil && err2 == nil {
			// Both succeeded
			// Note: We CANNOT assume different seed orders produce different addresses
			// due to hash collisions. For example:
			// - ["", "abc"] and ["abc", ""] both hash to "abc" + programID
			// - ["a", "b"] and ["ab", ""] both hash to "ab" + programID
			// This is documented Solana behavior and not a bug.

			// Only check: if seeds are identical, addresses must be identical
			if bytes.Equal(seed1, seed2) {
				if !bytes.Equal(address1, address2) {
					t.Errorf("Equal seeds produced different addresses")
				}
			}
			// We do NOT check the reverse case (different seeds → different addresses)
			// because that invariant doesn't hold due to intentional hash collisions
		}

		// Test that concatenated seeds produce SAME address as separate seeds
		// This is expected Solana behavior: seeds are hashed sequentially
		// so ["a", "b"] produces the same hash as ["ab"]
		// See: https://docs.solana.com/developing/programming-model/calling-between-programs#hash-collisions
		if len(seed1)+len(seed2) <= MaxSeedLen {
			concatenated := append([]byte{}, seed1...)
			concatenated = append(concatenated, seed2...)
			seeds3 := [][]byte{concatenated}
			address3, err3 := solana.CreateProgramAddressBytes(seeds3, programId)

			if err1 == nil && err3 == nil {
				// Concatenated seed SHOULD produce same address as separate seeds
				// This is documented Solana behavior (hash collision by design)
				if !bytes.Equal(address1, address3) {
					t.Errorf("Concatenated seed produced different address than separate seeds (expected same)")
				}
			}
		}
	})
}

// FuzzMaxSeedsAndLength tests edge cases for seed limits
func FuzzMaxSeedsAndLength(f *testing.F) {
	// Test maximum constraints
	f.Add(make([]byte, 32))

	f.Fuzz(func(t *testing.T, programId []byte) {
		// Program ID must be 32 bytes
		if len(programId) != 32 {
			t.Skip("programId must be 32 bytes")
		}

		// Test with exactly MaxSeeds seeds
		seeds := make([][]byte, MaxSeeds)
		for i := 0; i < MaxSeeds; i++ {
			// Use small seeds to avoid total size issues
			seeds[i] = []byte{byte(i)}
		}

		address, err := solana.CreateProgramAddressBytes(seeds, programId)
		if err != nil {
			// Address on curve is acceptable
			return
		}

		if len(address) != 32 {
			t.Errorf("Address length is %d, want 32", len(address))
		}

		// Test with exactly MaxSeedLen seed
		maxLenSeed := make([]byte, MaxSeedLen)
		for i := range maxLenSeed {
			maxLenSeed[i] = byte(i % 256)
		}

		address2, err2 := solana.CreateProgramAddressBytes([][]byte{maxLenSeed}, programId)
		if err2 != nil {
			// Address on curve is acceptable
			return
		}

		if len(address2) != 32 {
			t.Errorf("Address length is %d, want 32", len(address2))
		}
	})
}
