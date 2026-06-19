package accountsdb

import (
	"testing"

	"github.com/gagliardetto/solana-go"
)

// FuzzAccountIndexEntryUnmarshalData tests index entry unmarshaling with malformed data
func FuzzAccountIndexEntryUnmarshalData(f *testing.F) {
	// Seed corpus with valid and edge case data
	f.Add([]byte{})
	validEntry := []byte{
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Slot
		0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // FileId
		0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Offset
	}
	f.Add(validEntry)

	f.Fuzz(func(t *testing.T, data []byte) {
		entry := &AccountIndexEntry{}

		// Copy into fixed-size array expected by Unmarshal
		var arr [24]byte
		if len(data) >= 24 {
			copy(arr[:], data[:24])
		} else {
			// If data shorter than 24, copy what's available (rest stays zero)
			copy(arr[:], data)
		}

		// Should not panic on any input
		entry.Unmarshal(&arr)

		// Validate that unmarshaling produces reasonable values
		if entry.FileId > 1<<48 {
			t.Logf("FileId suspiciously large: %d", entry.FileId)
		}
	})
}

// FuzzUnmarshalAcctIdxEntry tests the standalone index entry unmarshaling function
func FuzzUnmarshalAcctIdxEntry(f *testing.F) {
	// Seed corpus
	f.Add([]byte{})
	f.Add(make([]byte, 24))
	f.Add([]byte{0x01, 0x02, 0x03})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Should handle any input size gracefully
		entry, err := unmarshalAcctIdxEntry(data)

		if len(data) < 24 {
			// Expect error for undersized data
			if err == nil {
				t.Errorf("Expected error for data length %d, got nil", len(data))
			}
			return
		}

		// Should succeed for valid length
		if err != nil {
			t.Errorf("Unexpected error for valid length data: %v", err)
			return
		}

		// Entry should be non-nil on success
		if entry == nil {
			t.Errorf("Got nil entry with nil error")
		}
	})
}

// FuzzGetAccount tests account retrieval with various slot/pubkey combinations
func FuzzGetAccount(f *testing.F) {
	// Seed corpus
	var zeroPubkey solana.PublicKey
	var testPubkey solana.PublicKey
	copy(testPubkey[:], []byte{0x01, 0x02, 0x03, 0x04})

	f.Add(uint64(0), zeroPubkey[:])
	f.Add(uint64(100), testPubkey[:])
	f.Add(uint64(1000000), make([]byte, 32))

	f.Fuzz(func(t *testing.T, slot uint64, pubkeyBytes []byte) {
		// Bounds checking
		if slot > 1000000000 {
			return
		}
		if len(pubkeyBytes) != 32 {
			return
		}

		var pubkey solana.PublicKey
		copy(pubkey[:], pubkeyBytes)

		// NOTE: Due to client bug [F-C01], GetAccount panics if Index is nil
		// This test verifies the bug exists - GetAccount should return error, not panic
		// InitCaches() only initializes caches, not the Index field
		db := &AccountsDb{}
		db.InitCaches()

		// Expect panic due to nil Index dereference at accountsdb.go:333
		// This documents the bug - GetAccount should check if Index is initialized
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("Expected panic due to nil Index, but GetAccount succeeded")
			}
			// Panic is expected - this confirms the bug exists
		}()

		_, _ = db.GetAccount(slot, pubkey)
	})
}
