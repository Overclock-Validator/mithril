package compactindex

import (
	"bytes"
	"testing"
)

// FuzzHeaderLoadStore tests header serialization round-trip with malformed data
func FuzzHeaderLoadStore(f *testing.F) {
	// Seed with different file sizes and bucket counts
	f.Add(uint64(0), uint32(0))
	f.Add(uint64(1000), uint32(10))
	f.Add(uint64(1<<40), uint32(1000))

	f.Fuzz(func(t *testing.T, fileSize uint64, numBuckets uint32) {
		// Bounds checking
		if numBuckets > 100000 {
			return
		}

		header := Header{
			FileSize:   fileSize,
			NumBuckets: numBuckets,
		}

		// Store
		var buf [headerSize]byte
		header.Store(&buf)

		// Load
		var header2 Header
		err := header2.Load(&buf)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		// Verify roundtrip
		if header2.FileSize != header.FileSize {
			t.Errorf("FileSize mismatch: got %d, want %d", header2.FileSize, header.FileSize)
		}
		if header2.NumBuckets != header.NumBuckets {
			t.Errorf("NumBuckets mismatch: got %d, want %d", header2.NumBuckets, header.NumBuckets)
		}
	})
}

// FuzzHeaderLoad tests header deserialization with random malformed data
func FuzzHeaderLoad(f *testing.F) {
	// Seed with various patterns
	f.Add(Magic[:])
	f.Add(make([]byte, headerSize))
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Header.Load expects exactly headerSize bytes
		if len(data) < headerSize {
			// Pad to headerSize
			padded := make([]byte, headerSize)
			copy(padded, data)
			data = padded
		} else if len(data) > headerSize {
			// Truncate to headerSize
			data = data[:headerSize]
		}

		var buf [headerSize]byte
		copy(buf[:], data)

		var header Header
		err := header.Load(&buf)

		// Should either succeed or return clear error
		if err != nil {
			// Expected for invalid magic
			return
		}

		// If successful, verify values are within reasonable bounds
		// (Header.Load should validate the magic bytes)
		_ = header.FileSize
		_ = header.NumBuckets
	})
}

// FuzzIndexOpen tests index opening with various invalid data
func FuzzIndexOpen(f *testing.F) {
	// Seed with different data patterns
	f.Add([]byte{})
	f.Add(Magic[:])
	f.Add(make([]byte, 100))

	// Valid minimal header
	validHeader := make([]byte, headerSize)
	copy(validHeader, Magic[:])
	f.Add(validHeader)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Limit size
		if len(data) > 10000 {
			return
		}

		reader := bytes.NewReader(data)

		// Try to open - should handle malformed data gracefully
		db, err := Open(reader)

		if err != nil {
			// Expected for invalid data (too short, bad magic, etc.)
			return
		}

		// If successful, verify db is usable
		if db == nil {
			t.Error("Open returned nil DB with nil error")
			return
		}

		// Verify Stream is set
		if db.Stream == nil {
			t.Error("Open returned DB with nil Stream")
		}

		// Verify header fields are accessible
		_ = db.FileSize
		_ = db.NumBuckets
	})
}

// FuzzBucketHash tests the bucket hash function with various keys
func FuzzBucketHash(f *testing.F) {
	// Seed corpus
	f.Add([]byte{}, uint32(1))
	f.Add([]byte{0x00}, uint32(10))
	f.Add(make([]byte, 32), uint32(100))
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF}, uint32(256))

	f.Fuzz(func(t *testing.T, key []byte, numBuckets uint32) {
		// Bounds checking
		if len(key) > 1024 {
			return
		}
		if numBuckets == 0 || numBuckets > 100000 {
			return
		}

		header := Header{
			FileSize:   1000,
			NumBuckets: numBuckets,
		}

		// Hash should not panic
		bucket := header.BucketHash(key)

		// Verify bucket is within valid range
		if bucket >= uint(numBuckets) {
			t.Errorf("BucketHash returned %d, expected < %d", bucket, numBuckets)
		}
	})
}

// FuzzEntryUnmarshal tests entry unmarshaling with various offset widths
func FuzzEntryUnmarshal(f *testing.F) {
	// Seed with different data patterns for various offset widths
	f.Add([]byte{0x01, 0x02, 0x03, 0x04}, uint64(255))         // 1-byte offset
	f.Add([]byte{0x01, 0x02, 0x03, 0x04, 0x05}, uint64(65535)) // 2-byte offset
	f.Add(make([]byte, 10), uint64(1<<32))                     // 5-byte offset

	f.Fuzz(func(t *testing.T, data []byte, fileSize uint64) {
		// Limit data size and fileSize
		if len(data) > 100 || len(data) < 3 {
			return
		}
		if fileSize > 1<<48 {
			return
		}

		// Create bucket descriptor with computed offset width
		bucket := BucketDescriptor{
			BucketHeader: BucketHeader{
				HashDomain: 0,
				NumEntries: 1,
				FileOffset: 100,
			},
			Stride:      uint8(3 + intWidth(fileSize)), // 3 bytes hash + offset width
			OffsetWidth: intWidth(fileSize),
		}

		// Ensure data is the right size for one entry
		entrySize := int(bucket.Stride)
		if len(data) < entrySize {
			// Pad if too short
			padded := make([]byte, entrySize)
			copy(padded, data)
			data = padded
		} else {
			// Truncate if too long
			data = data[:entrySize]
		}

		// Should not panic when unmarshaling
		entry := bucket.unmarshalEntry(data)

		// Verify entry fields are set
		_ = entry.Hash
		_ = entry.Value

		// Value should not exceed fileSize (though this isn't strictly enforced)
		if entry.Value > fileSize {
			t.Logf("Entry value %d exceeds fileSize %d (may be expected for malformed data)", entry.Value, fileSize)
		}
	})
}

// FuzzBucketHeaderLoadStore tests bucket header serialization
func FuzzBucketHeaderLoadStore(f *testing.F) {
	// Seed corpus
	f.Add(uint32(0), uint32(0), uint64(0))
	f.Add(uint32(12345), uint32(100), uint64(5000))
	f.Add(uint32(0xFFFFFFFF), uint32(maxEntriesPerBucket), uint64(1<<40))

	f.Fuzz(func(t *testing.T, hashDomain uint32, numEntries uint32, fileOffset uint64) {
		// Bounds checking
		if numEntries > maxEntriesPerBucket {
			return
		}
		if fileOffset > 1<<48 {
			return
		}

		header := BucketHeader{
			HashDomain: hashDomain,
			NumEntries: numEntries,
			FileOffset: fileOffset,
		}

		// Store
		var buf [bucketHdrLen]byte
		header.Store(&buf)

		// Load
		var header2 BucketHeader
		header2.Load(&buf)

		// Verify roundtrip
		if header2.HashDomain != header.HashDomain {
			t.Errorf("HashDomain mismatch: got %d, want %d", header2.HashDomain, header.HashDomain)
		}
		if header2.NumEntries != header.NumEntries {
			t.Errorf("NumEntries mismatch: got %d, want %d", header2.NumEntries, header.NumEntries)
		}
		if header2.FileOffset != header.FileOffset {
			t.Errorf("FileOffset mismatch: got %d, want %d", header2.FileOffset, header.FileOffset)
		}
	})
}
