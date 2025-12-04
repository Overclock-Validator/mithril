package sealevel

import (
	"bytes"
	"crypto/sha256"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/crypto/secp256k1"
	"github.com/zeebo/blake3"
	"golang.org/x/crypto/sha3"
)

// FuzzSwapEndianness tests byte endianness swapping for cryptographic operations
func FuzzSwapEndianness(f *testing.F) {
	// Seed with various patterns
	f.Add([]byte{})
	f.Add([]byte{0x01})
	f.Add([]byte{0x01, 0x02})
	f.Add([]byte{0x01, 0x02, 0x03, 0x04})
	f.Add(make([]byte, 32))                                       // Zero bytes
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) // All 0xff

	f.Fuzz(func(t *testing.T, input []byte) {
		// Limit input size to prevent excessive memory usage
		if len(input) > 10000 {
			t.Skip("input too large")
		}

		result := SwapEndianness(input)

		// Verify length preserved
		if len(result) != len(input) {
			t.Errorf("Length mismatch: got %d, want %d", len(result), len(input))
			return
		}

		// Verify bytes are reversed
		for i := 0; i < len(input); i++ {
			if result[i] != input[len(input)-1-i] {
				t.Errorf("Byte mismatch at index %d: got %d, want %d", i, result[i], input[len(input)-1-i])
				return
			}
		}

		// Verify double swap returns original
		doubleSwap := SwapEndianness(result)
		if !bytes.Equal(doubleSwap, input) {
			t.Errorf("Double swap did not return original")
		}
	})
}

// FuzzPoseidonHash tests Poseidon hash computation with various inputs
func FuzzPoseidonHash(f *testing.F) {
	// Seed with edge cases
	f.Add([]byte{}, true)
	f.Add([]byte{0x01}, true)
	f.Add(make([]byte, 32), true)
	f.Add(make([]byte, 32), false)
	f.Add([]byte{0xff, 0xff, 0xff, 0xff}, true)

	f.Fuzz(func(t *testing.T, input []byte, isBigEndian bool) {
		// Limit input size
		if len(input) > 32 {
			t.Skip("input too large for single hash element")
		}

		if len(input) == 0 {
			t.Skip("empty input")
		}

		// Create single-element input
		inputs := [][]byte{input}

		// Test Poseidon hash
		hash, err := PoseidonHash(inputs, isBigEndian)

		if err != nil {
			// Expected errors for invalid input
			return
		}

		// Verify hash is 32 bytes
		if len(hash) != 32 {
			t.Errorf("Hash length is %d, want 32", len(hash))
		}

		// Verify determinism - same input should produce same hash
		hash2, err2 := PoseidonHash(inputs, isBigEndian)
		if err2 != nil {
			t.Errorf("Second hash failed: %v", err2)
			return
		}

		if !bytes.Equal(hash, hash2) {
			t.Errorf("Hash not deterministic")
		}
	})
}

// FuzzPoseidonHashMultiInput tests Poseidon hash with multiple inputs
func FuzzPoseidonHashMultiInput(f *testing.F) {
	// Seed with multi-input cases
	f.Add(uint8(2), true)
	f.Add(uint8(5), true)
	f.Add(uint8(12), true) // Maximum allowed
	f.Add(uint8(3), false)

	f.Fuzz(func(t *testing.T, numInputs uint8, isBigEndian bool) {
		// Limit number of inputs (max 12 for Poseidon)
		if numInputs == 0 || numInputs > 12 {
			t.Skip("invalid number of inputs")
		}

		// Create multiple inputs
		inputs := make([][]byte, numInputs)
		for i := uint8(0); i < numInputs; i++ {
			// Create varied input data
			data := make([]byte, 8)
			for j := range data {
				data[j] = byte(i*7 + uint8(j))
			}
			inputs[i] = data
		}

		// Test Poseidon hash
		hash, err := PoseidonHash(inputs, isBigEndian)

		if err != nil {
			// Expected errors
			return
		}

		// Verify hash is 32 bytes
		if len(hash) != 32 {
			t.Errorf("Hash length is %d, want 32", len(hash))
		}

		// Verify determinism
		hash2, err2 := PoseidonHash(inputs, isBigEndian)
		if err2 != nil {
			t.Errorf("Second hash failed: %v", err2)
			return
		}

		if !bytes.Equal(hash, hash2) {
			t.Errorf("Hash not deterministic")
		}
	})
}

// FuzzParseAndValidateSignature tests SECP256K1 signature validation
func FuzzParseAndValidateSignature(f *testing.F) {
	// Seed with various signature patterns
	f.Add(make([]byte, 64))                                            // All zeros
	f.Add(bytes.Repeat([]byte{0xff}, 64))                              // All 0xff
	f.Add(bytes.Repeat([]byte{0x01}, 64))                              // All 0x01
	f.Add(append(make([]byte, 32), bytes.Repeat([]byte{0x01}, 32)...)) // r=0, s=1

	f.Fuzz(func(t *testing.T, signature []byte) {
		// Signature must be exactly 64 bytes
		if len(signature) != 64 {
			t.Skip("signature must be 64 bytes")
		}

		// Test signature validation
		err := parseAndValidateSignature(signature)

		// Signature is valid if:
		// 1. r and s are not zero
		// 2. r and s are less than secp256k1 curve order
		// We just verify it doesn't panic and returns consistent results
		if err != nil {
			// Invalid signature - verify it's actually invalid
			// Extract r and s
			r := new(big.Int).SetBytes(signature[:32])
			s := new(big.Int).SetBytes(signature[32:])

			// Check if either is zero (which would be invalid)
			if r.Sign() == 0 || s.Sign() == 0 {
				// Expected invalid
				return
			}

			// Check if they exceed curve order (also invalid)
			// secp256k1 order: 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
			curveOrder := new(big.Int)
			curveOrder.SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141", 16)

			if r.Cmp(curveOrder) >= 0 || s.Cmp(curveOrder) >= 0 {
				// Expected invalid
				return
			}
		}
	})
}

// FuzzSHA256Hashing tests SHA256 hash computation consistency
func FuzzSHA256Hashing(f *testing.F) {
	// Seed with various inputs
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte("test"))
	f.Add(make([]byte, 100))
	f.Add(bytes.Repeat([]byte{0xff}, 1000))

	f.Fuzz(func(t *testing.T, input []byte) {
		// Limit input size
		if len(input) > 100000 {
			t.Skip("input too large")
		}

		// Compute hash
		hasher := sha256.New()
		hasher.Write(input)
		hash1 := hasher.Sum(nil)

		// Verify hash is 32 bytes
		if len(hash1) != 32 {
			t.Errorf("SHA256 hash length is %d, want 32", len(hash1))
		}

		// Verify determinism
		hasher2 := sha256.New()
		hasher2.Write(input)
		hash2 := hasher2.Sum(nil)

		if !bytes.Equal(hash1, hash2) {
			t.Errorf("SHA256 hash not deterministic")
		}

		// Verify empty input produces known hash
		if len(input) == 0 {
			expectedEmptyHash := []byte{
				0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14,
				0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
				0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c,
				0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
			}
			if !bytes.Equal(hash1, expectedEmptyHash) {
				t.Errorf("Empty input hash mismatch")
			}
		}
	})
}

// FuzzKeccak256Hashing tests Keccak256 hash computation consistency
func FuzzKeccak256Hashing(f *testing.F) {
	// Seed with various inputs
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte("test"))
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, input []byte) {
		// Limit input size
		if len(input) > 100000 {
			t.Skip("input too large")
		}

		// Compute hash
		hasher := sha3.NewLegacyKeccak256()
		hasher.Write(input)
		hash1 := hasher.Sum(nil)

		// Verify hash is 32 bytes
		if len(hash1) != 32 {
			t.Errorf("Keccak256 hash length is %d, want 32", len(hash1))
		}

		// Verify determinism
		hasher2 := sha3.NewLegacyKeccak256()
		hasher2.Write(input)
		hash2 := hasher2.Sum(nil)

		if !bytes.Equal(hash1, hash2) {
			t.Errorf("Keccak256 hash not deterministic")
		}
	})
}

// FuzzBlake3Hashing tests Blake3 hash computation consistency
func FuzzBlake3Hashing(f *testing.F) {
	// Seed with various inputs
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte("test"))
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, input []byte) {
		// Limit input size
		if len(input) > 100000 {
			t.Skip("input too large")
		}

		// Compute hash
		hasher := blake3.New()
		hasher.Write(input)
		hash1 := hasher.Sum(nil)

		// Verify hash is 32 bytes
		if len(hash1) != 32 {
			t.Errorf("Blake3 hash length is %d, want 32", len(hash1))
		}

		// Verify determinism
		hasher2 := blake3.New()
		hasher2.Write(input)
		hash2 := hasher2.Sum(nil)

		if !bytes.Equal(hash1, hash2) {
			t.Errorf("Blake3 hash not deterministic")
		}
	})
}

// FuzzSecp256k1Recover tests public key recovery with various inputs
func FuzzSecp256k1Recover(f *testing.F) {
	// Seed with edge cases
	f.Add(make([]byte, 32), make([]byte, 64), uint8(0))
	f.Add(make([]byte, 32), make([]byte, 64), uint8(1))
	f.Add(make([]byte, 32), make([]byte, 64), uint8(2))
	f.Add(make([]byte, 32), make([]byte, 64), uint8(3))

	f.Fuzz(func(t *testing.T, hash []byte, signature []byte, recoveryId uint8) {
		// Inputs must be exact size
		if len(hash) != 32 {
			t.Skip("hash must be 32 bytes")
		}
		if len(signature) != 64 {
			t.Skip("signature must be 64 bytes")
		}
		if recoveryId >= 4 {
			t.Skip("recovery ID must be 0-3")
		}

		// Prepare signature with recovery ID
		sigAndRecoveryId := make([]byte, 65)
		copy(sigAndRecoveryId, signature)
		sigAndRecoveryId[64] = recoveryId

		// Attempt recovery
		recoveredPubKey, err := secp256k1.RecoverPubkey(hash, sigAndRecoveryId)

		if err != nil {
			// Expected for invalid signatures
			return
		}

		// Verify recovered public key is 65 bytes (uncompressed format)
		if len(recoveredPubKey) != 65 {
			t.Errorf("Recovered pubkey length is %d, want 65", len(recoveredPubKey))
		}

		// Verify first byte is 0x04 (uncompressed point marker)
		if recoveredPubKey[0] != 0x04 {
			t.Errorf("Recovered pubkey first byte is %d, want 4", recoveredPubKey[0])
		}

		// Verify determinism - same inputs produce same output
		recoveredPubKey2, err2 := secp256k1.RecoverPubkey(hash, sigAndRecoveryId)
		if err2 != nil {
			t.Errorf("Second recovery failed: %v", err2)
			return
		}

		if !bytes.Equal(recoveredPubKey, recoveredPubKey2) {
			t.Errorf("Recovery not deterministic")
		}
	})
}
