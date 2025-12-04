package base58

import (
	"testing"
)

// FuzzEncode32 tests the Encode32 function with random 32-byte inputs
// This fuzzer ensures that encoding never panics and always produces valid output
func FuzzEncode32(f *testing.F) {
	// Seed corpus with interesting test cases
	f.Add(make([]byte, 32)) // All zeros
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) // All 0xff
	f.Add([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20}) // Sequential

	f.Fuzz(func(t *testing.T, input []byte) {
		// Only test 32-byte inputs
		if len(input) != 32 {
			t.Skip("Input must be exactly 32 bytes")
		}

		var in [32]byte
		copy(in[:], input)

		var out [44]byte

		// Test that encoding doesn't panic
		outLen := Encode32(&out, in)

		// Verify output length is within expected bounds
		if outLen < 32 || outLen > 44 {
			t.Errorf("Invalid output length: %d, expected between 32 and 44", outLen)
		}

		// Verify all output characters are valid base58
		for i := uint(0); i < outLen; i++ {
			found := false
			for j := 0; j < len(alphabet); j++ {
				if out[i] == alphabet[j] {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Invalid base58 character at position %d: %c", i, out[i])
			}
		}
	})
}

// FuzzDecode32 tests the Decode32 function with random inputs
// This fuzzer ensures that decoding handles invalid input gracefully
func FuzzDecode32(f *testing.F) {
	// Seed corpus with valid and edge-case inputs
	f.Add([]byte("11111111111111111111111111111111"))             // Min length valid
	f.Add([]byte("5Q5F5F5F5F5F5F5F5F5F5F5F5F5F5F5F5F5F5F5F5F5F")) // Near max length
	f.Add([]byte("11111111111111111111111111111112"))             // Simple variation
	f.Add([]byte("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"))             // All 'z'

	f.Fuzz(func(t *testing.T, encoded []byte) {
		var out [32]byte

		// Test that decoding doesn't panic
		// If it does panic, the fuzzer will catch it and save the failing input
		ok := Decode32(&out, encoded)

		// If decoding succeeded, verify we can encode it back
		if ok {
			var reencoded [44]byte
			outLen := Encode32(&reencoded, out)

			// The re-encoded version should decode to the same value
			var out2 [32]byte
			ok2 := Decode32(&out2, reencoded[:outLen])
			if !ok2 {
				t.Errorf("Re-encoding failed for input: %s", string(encoded))
			}

			// The decoded values should match
			if out != out2 {
				t.Errorf("Round-trip encode/decode mismatch")
			}
		}
	})
}

// FuzzEncodeDecodeRoundTrip tests that encoding and decoding are inverses
func FuzzEncodeDecodeRoundTrip(f *testing.F) {
	// Seed with various patterns
	f.Add(make([]byte, 32))
	f.Add([]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	f.Add([]byte{0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

	f.Fuzz(func(t *testing.T, input []byte) {
		// Only test 32-byte inputs
		if len(input) != 32 {
			t.Skip("Input must be exactly 32 bytes")
		}

		var in [32]byte
		copy(in[:], input)

		// Encode the input
		var encoded [44]byte
		encLen := Encode32(&encoded, in)

		// Decode it back
		var decoded [32]byte
		ok := Decode32(&decoded, encoded[:encLen])

		if !ok {
			t.Errorf("Failed to decode encoded value")
			return
		}

		// Verify round-trip
		if in != decoded {
			t.Errorf("Round-trip failed: input != decoded")
		}
	})
}

// FuzzDecodeFromString tests the wrapper function
func FuzzDecodeFromString(f *testing.F) {
	// Seed with various string inputs
	f.Add("11111111111111111111111111111111")
	f.Add("5Q5F5F5F5F5F5F5F5F5F5F5F5F5F5F5F")
	f.Add("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
	f.Add("")    // Empty string
	f.Add("abc") // Too short

	f.Fuzz(func(t *testing.T, input string) {
		// Test that DecodeFromString handles input gracefully
		// If it panics, the fuzzer will catch it and save the failing input
		result, err := DecodeFromString(input)

		// If successful, verify we can encode it back
		if err == nil {
			encoded := Encode(result[:])

			// Decode the encoded version
			result2, err2 := DecodeFromString(encoded)
			if err2 != nil {
				t.Errorf("Re-decoding failed: %v", err2)
			}

			// Should match
			if result != result2 {
				t.Errorf("Round-trip mismatch")
			}
		}
	})
}

// FuzzEncode tests the generic Encode function
func FuzzEncode(f *testing.F) {
	// Seed with 32-byte inputs (currently the only supported length)
	f.Add(make([]byte, 32))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, input []byte) {
		// Only test 32-byte inputs
		if len(input) != 32 {
			t.Skip("Input must be exactly 32 bytes")
		}

		// Test that encoding doesn't panic
		encoded := Encode(input)

		// Verify output is not empty
		if len(encoded) == 0 {
			t.Errorf("Encode returned empty string")
		}

		// Verify output length is reasonable
		if len(encoded) < 32 || len(encoded) > 44 {
			t.Errorf("Invalid encoded length: %d", len(encoded))
		}

		// Verify all characters are valid base58
		for i, c := range encoded {
			found := false
			for j := 0; j < len(alphabet); j++ {
				if byte(c) == alphabet[j] {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Invalid base58 character at position %d: %c", i, c)
			}
		}
	})
}
