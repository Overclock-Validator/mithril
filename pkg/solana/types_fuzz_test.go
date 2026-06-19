package solana

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/base58"
)

// FuzzHashUnmarshalText tests Hash.UnmarshalText with various inputs
func FuzzHashUnmarshalText(f *testing.F) {
	// Seed with valid and invalid base58 strings
	f.Add([]byte("11111111111111111111111111111111"))
	f.Add([]byte("5Q5F5F5F5F5F5F5F5F5F5F5F5F5F5F5F"))
	f.Add([]byte(""))
	f.Add([]byte("invalid"))
	f.Add([]byte("0000000000000000000000000000000")) // Contains invalid '0'

	f.Fuzz(func(t *testing.T, input []byte) {
		var h Hash
		err := h.UnmarshalText(input)

		// If unmarshaling succeeded, verify we can marshal it back
		if err == nil {
			str := h.String()

			// Try to unmarshal the string representation
			var h2 Hash
			err2 := h2.UnmarshalText([]byte(str))
			if err2 != nil {
				t.Errorf("Failed to unmarshal string representation: %v", err2)
			}

			// The hashes should match
			if h != h2 {
				t.Errorf("Round-trip failed: original != re-unmarshaled")
			}
		}
	})
}

// FuzzHashString tests that Hash.String always produces valid base58
func FuzzHashString(f *testing.F) {
	// Seed with various byte patterns
	f.Add(make([]byte, 32))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20})

	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) != 32 {
			t.Skip("Input must be exactly 32 bytes")
		}

		var h Hash
		copy(h[:], input)

		// Convert to string
		str := h.String()

		// Verify it's valid base58
		if len(str) < 32 || len(str) > 44 {
			t.Errorf("Invalid string length: %d", len(str))
		}

		// Verify all characters are valid base58
		const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
		for i, c := range str {
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

		// Verify round-trip
		var h2 Hash
		if err := h2.UnmarshalText([]byte(str)); err != nil {
			t.Errorf("Failed to unmarshal string representation: %v", err)
		}
		if h != h2 {
			t.Errorf("Round-trip failed")
		}
	})
}

// FuzzAddressUnmarshalText tests Address.UnmarshalText
func FuzzAddressUnmarshalText(f *testing.F) {
	// Seed with valid Solana addresses
	f.Add([]byte("11111111111111111111111111111111"))
	f.Add([]byte("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"))
	f.Add([]byte("Vote111111111111111111111111111111111111111"))
	f.Add([]byte("invalid"))

	f.Fuzz(func(t *testing.T, input []byte) {
		var addr Address
		err := addr.UnmarshalText(input)

		// If unmarshaling succeeded, verify we can marshal it back
		if err == nil {
			str := addr.String()

			// Try to unmarshal the string representation
			var addr2 Address
			err2 := addr2.UnmarshalText([]byte(str))
			if err2 != nil {
				t.Errorf("Failed to unmarshal string representation: %v", err2)
			}

			// The addresses should match
			if addr != addr2 {
				t.Errorf("Round-trip failed: original != re-unmarshaled")
			}
		}
	})
}

// FuzzAddressString tests Address.String
func FuzzAddressString(f *testing.F) {
	// Seed with various byte patterns
	f.Add(make([]byte, 32))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) != 32 {
			t.Skip("Input must be exactly 32 bytes")
		}

		var addr Address
		copy(addr[:], input)

		// Convert to string
		str := addr.String()

		// Verify it's valid base58
		if len(str) < 32 || len(str) > 44 {
			t.Errorf("Invalid string length: %d", len(str))
		}

		// Verify round-trip
		var addr2 Address
		if err := addr2.UnmarshalText([]byte(str)); err != nil {
			t.Errorf("Failed to unmarshal string representation: %v", err)
		}
		if addr != addr2 {
			t.Errorf("Round-trip failed")
		}
	})
}

// FuzzAddressHashConsistency verifies Address and Hash behave consistently
func FuzzAddressHashConsistency(f *testing.F) {
	f.Add(make([]byte, 32))
	f.Add([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20})

	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) != 32 {
			t.Skip("Input must be exactly 32 bytes")
		}

		var addr Address
		var hash Hash
		copy(addr[:], input)
		copy(hash[:], input)

		// String representations should be identical
		if addr.String() != hash.String() {
			t.Errorf("Address and Hash string representations differ for same bytes")
		}

		// UnmarshalText should behave the same
		testStr := []byte("5Q5F5F5F5F5F5F5F5F5F5F5F5F5F5F5F")

		var addr2 Address
		var hash2 Hash

		err1 := addr2.UnmarshalText(testStr)
		err2 := hash2.UnmarshalText(testStr)

		if (err1 == nil) != (err2 == nil) {
			t.Errorf("Address and Hash UnmarshalText have different error behavior")
		}

		if err1 == nil && err2 == nil {
			// Compare the bytes
			if addr2 != Address(hash2) {
				t.Errorf("Address and Hash UnmarshalText produce different results")
			}
		}
	})
}

// FuzzMustAddress tests that MustAddress only panics on invalid input
func FuzzMustAddress(f *testing.F) {
	// Seed with valid and invalid addresses
	f.Add("11111111111111111111111111111111")
	f.Add("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
	f.Add("invalid")
	f.Add("")

	f.Fuzz(func(t *testing.T, input string) {
		// First check if it would succeed
		var testAddr Address
		err := testAddr.UnmarshalText([]byte(input))

		// Track whether MustAddress panics
		var didPanic bool
		var addr Address

		func() {
			defer func() {
				if r := recover(); r != nil {
					didPanic = true
				}
			}()
			addr = MustAddress(input)
		}()

		if err != nil {
			// Should panic
			if !didPanic {
				t.Errorf("MustAddress(%q) should panic but didn't", input)
			}
		} else {
			// Should not panic
			if didPanic {
				t.Errorf("MustAddress(%q) panicked but shouldn't", input)
			}

			// Verify the result matches
			if addr != testAddr {
				t.Errorf("MustAddress result doesn't match UnmarshalText result")
			}
		}
	})
}

// FuzzBase58RoundTrip verifies base58 encoding/decoding consistency
func FuzzBase58RoundTrip(f *testing.F) {
	f.Add(make([]byte, 32))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) != 32 {
			t.Skip("Input must be exactly 32 bytes")
		}

		// Encode using Hash
		var h Hash
		copy(h[:], input)
		encoded := h.String()

		// Decode back
		var h2 Hash
		if err := h2.UnmarshalText([]byte(encoded)); err != nil {
			t.Errorf("Failed to decode: %v", err)
			return
		}

		// Should match original
		if h != h2 {
			t.Errorf("Round-trip failed")
		}

		// Also verify with base58 package directly
		var arr [32]byte
		copy(arr[:], input)

		var out [44]byte
		outLen := base58.Encode32(&out, arr)

		var decoded [32]byte
		if !base58.Decode32(&decoded, out[:outLen]) {
			t.Errorf("base58.Decode32 failed")
			return
		}

		if arr != decoded {
			t.Errorf("base58 direct round-trip failed")
		}
	})
}
