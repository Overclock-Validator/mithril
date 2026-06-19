package accountsdb

import (
	"bytes"
	"testing"

	"github.com/gagliardetto/solana-go"
)

// FuzzAppendVecAccountUnmarshal tests AppendVecAccount deserialization with malformed data
func FuzzAppendVecAccountUnmarshal(f *testing.F) {
	// Seed with various binary patterns
	f.Add([]byte{})
	f.Add(make([]byte, 136)) // header size
	f.Add(make([]byte, 200))
	f.Add(make([]byte, 1024))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Limit size to prevent OOM
		if len(data) > 10*1024 {
			data = data[:10*1024]
		}

		buf := bytes.NewReader(data)
		var acct AppendVecAccount

		// Test deserialization - should not panic
		err := acct.Unmarshal(buf)

		// Expect errors for malformed data, but no panics
		if err == nil {
			// If successfully unmarshaled, verify fields are reasonable
			if acct.DataLen > 10*1024*1024 {
				// DataLen too large
				t.Skip("DataLen too large, skip verification")
			}

			// Verify DataLen matches actual Data length
			if uint64(len(acct.Data)) != acct.DataLen {
				t.Errorf("DataLen mismatch: field=%d, actual=%d", acct.DataLen, len(acct.Data))
			}

			// Executable should be 0 or 1
			// (already validated by hdrBytes[96] != 0 conversion)
		}
	})
}

// FuzzAccountIndexEntryUnmarshal tests AccountIndexEntry deserialization
func FuzzAccountIndexEntryUnmarshal(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 24))
	f.Add(make([]byte, 8))
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Test unmarshalAcctIdxEntry with bounds checking
		entry, err := unmarshalAcctIdxEntry(data)

		if len(data) < 24 {
			// Should return error for insufficient data
			if err == nil {
				t.Error("Expected error for data < 24 bytes")
			}
		} else {
			// Should succeed for valid length
			if err != nil {
				t.Errorf("Unexpected error for valid length: %v", err)
			}

			if entry == nil {
				t.Error("Entry should not be nil for valid data")
			}
		}
	})
}

// FuzzAccountIndexEntryRoundtrip tests index entry marshal/unmarshal
func FuzzAccountIndexEntryRoundtrip(f *testing.F) {
	f.Add(uint64(1000), uint64(5), uint64(256))
	f.Add(uint64(0), uint64(0), uint64(0))
	f.Add(uint64(^uint64(0)), uint64(^uint64(0)), uint64(^uint64(0)))

	f.Fuzz(func(t *testing.T, slot uint64, fileId uint64, offset uint64) {
		// Create original entry
		original := AccountIndexEntry{
			Slot:   slot,
			FileId: fileId,
			Offset: offset,
		}

		// Marshal to bytes
		var data [24]byte
		original.Marshal(&data)

		// Unmarshal back
		var decoded AccountIndexEntry
		decoded.Unmarshal(&data)

		// Verify roundtrip
		if decoded.Slot != original.Slot {
			t.Errorf("Slot mismatch: expected %d, got %d", original.Slot, decoded.Slot)
		}
		if decoded.FileId != original.FileId {
			t.Errorf("FileId mismatch: expected %d, got %d", original.FileId, decoded.FileId)
		}
		if decoded.Offset != original.Offset {
			t.Errorf("Offset mismatch: expected %d, got %d", original.Offset, decoded.Offset)
		}
	})
}

// FuzzParseNextAcct tests account parsing from append vector
func FuzzParseNextAcct(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 200))
	f.Add(make([]byte, 1024))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Limit size
		if len(data) > 10*1024 {
			data = data[:10*1024]
		}

		// Create parser
		parser := &appendVecParser{
			Buf:      data,
			FileSize: uint64(len(data)),
			Offset:   0,
			FileId:   1,
			Slot:     1000,
		}

		var pk solana.PublicKey
		var entry AccountIndexEntry

		// Test parsing - should not panic
		err := parser.ParseNextAcct(&pk, &entry)

		// Expect errors for malformed data, but no panics
		if err == nil {
			// Successfully parsed - verify entry is reasonable
			if entry.Offset > 10*1024*1024 {
				// Offset too large
				t.Skip("Offset too large")
			}
		}
	})
}
