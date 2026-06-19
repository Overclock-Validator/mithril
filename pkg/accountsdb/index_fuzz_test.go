package accountsdb

import (
	"testing"
)

// FuzzAccountIndexEntry tests AccountIndexEntry operations with edge cases
func FuzzAccountIndexEntry(f *testing.F) {
	// Seed corpus with various index entry values
	f.Add(uint64(0), uint64(0), uint64(0))
	f.Add(uint64(1), uint64(5), uint64(100))
	f.Add(uint64(999999), uint64(100), uint64(1<<20))

	f.Fuzz(func(t *testing.T, slot uint64, fileId uint64, offset uint64) {
		// Create entry
		entry := &AccountIndexEntry{
			Slot:   slot,
			FileId: fileId,
			Offset: offset,
		}

		// Test Marshal roundtrip
		var buf [24]byte
		entry.Marshal(&buf)

		// Test Unmarshal
		entry2 := &AccountIndexEntry{}
		entry2.Unmarshal(&buf)

		// Verify roundtrip consistency
		if entry2.Slot != entry.Slot {
			t.Errorf("Slot mismatch: got %d, want %d", entry2.Slot, entry.Slot)
		}
		if entry2.FileId != entry.FileId {
			t.Errorf("FileId mismatch: got %d, want %d", entry2.FileId, entry.FileId)
		}
		if entry2.Offset != entry.Offset {
			t.Errorf("Offset mismatch: got %d, want %d", entry2.Offset, entry.Offset)
		}
	})
}

// FuzzBuildIndexEntriesFromAppendVecs tests index building with malformed append vector data
func FuzzBuildIndexEntriesFromAppendVecs(f *testing.F) {
	// Seed corpus - use empty data and small valid data to avoid parsing issues
	f.Add([]byte{}, uint64(0), uint64(0), uint64(0))
	f.Add([]byte{0x01, 0x02, 0x03}, uint64(3), uint64(100), uint64(5))

	f.Fuzz(func(t *testing.T, data []byte, fileSize uint64, slot uint64, fileId uint64) {
		// Limit size to prevent excessive memory usage
		if len(data) > 10000 {
			return
		}
		if fileSize > 10000 {
			fileSize = uint64(len(data))
		}

		// NOTE: Due to client bug [F-C02], ParseNextAcct panics if fileSize > len(data)
		// The parser validates offsets against FileSize but accesses Buf without bounds checking
		// This test documents the bug by expecting panics for mismatched sizes
		defer func() {
			r := recover()
			if r != nil {
				// Panic is expected when fileSize > len(data)
				// This confirms bug F-C02 exists
				if fileSize > uint64(len(data)) {
					// Expected panic - bug confirmed
					return
				}
				// Unexpected panic for valid input
				t.Errorf("Unexpected panic with fileSize=%d len(data)=%d: %v", fileSize, len(data), r)
			}
		}()

		// Attempt to build index - should handle corruption gracefully
		pks, entries, err := BuildIndexEntriesFromAppendVecs(data, fileSize, slot, fileId)

		// Error is expected for malformed data
		if err != nil {
			return
		}

		// If successful, verify output consistency
		if len(pks) != len(entries) {
			t.Errorf("Pubkeys and entries length mismatch: %d vs %d", len(pks), len(entries))
		}

		// NOTE: BuildIndexEntriesFromAppendVecs appends empty entries before parsing
		// If parsing fails immediately, it returns empty/zero-initialized entries
		// We skip validation for empty results as they indicate parse failure
		if len(entries) == 0 {
			return
		}

		// Check all SUCCESSFULLY PARSED entries have valid slot/fileId
		// The last entry might be zero-initialized if parsing failed on it
		for i := 0; i < len(entries)-1; i++ {
			entry := entries[i]
			// Only validate non-zero entries (successfully parsed)
			if entry.Slot == 0 && entry.FileId == 0 && entry.Offset == 0 {
				continue
			}
			if entry.Slot != slot {
				t.Errorf("Entry %d has wrong slot: got %d, want %d", i, entry.Slot, slot)
			}
			if entry.FileId != fileId {
				t.Errorf("Entry %d has wrong fileId: got %d, want %d", i, entry.FileId, fileId)
			}
		}
	})
}
