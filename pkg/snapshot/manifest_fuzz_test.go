package snapshot

import (
	"bytes"
	"encoding/binary"
	"testing"

	bin "github.com/gagliardetto/binary"
)

// FuzzSnapshotManifestDeserialization tests manifest deserialization with malformed data
func FuzzSnapshotManifestDeserialization(f *testing.F) {
	// Seed with various binary patterns
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add(make([]byte, 1024))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Limit size to prevent timeouts
		if len(data) > 10*1024 {
			data = data[:10*1024]
		}

		decoder := bin.NewBinDecoder(data)

		// Test BankHashInfo deserialization
		var bankHashInfo BankHashInfo
		err := bankHashInfo.UnmarshalWithDecoder(decoder)
		_ = err // Expect errors for malformed data

		// Reset decoder for next test
		decoder = bin.NewBinDecoder(data)

		// Test AccountsDbFields deserialization
		var acctFields AccountsDbFields
		err = acctFields.UnmarshalWithDecoder(decoder)
		_ = err
	})
}

// FuzzBankHashInfoDeserialization tests BankHashInfo structure parsing
func FuzzBankHashInfoDeserialization(f *testing.F) {
	// Seed with valid structure patterns
	validData := make([]byte, 40) // hash (32) + signature (8)
	f.Add(validData)
	f.Add([]byte{})
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1024 {
			data = data[:1024]
		}

		decoder := bin.NewBinDecoder(data)
		var info BankHashInfo
		err := info.UnmarshalWithDecoder(decoder)

		// Should handle errors gracefully
		if err == nil {
			// If successfully decoded, verify structure
			if len(info.Hash) != 32 {
				t.Errorf("BankHashInfo hash should be 32 bytes, got %d", len(info.Hash))
			}
		}
	})
}

// FuzzAccountsDbFieldsDeserialization tests accounts DB metadata parsing
func FuzzAccountsDbFieldsDeserialization(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 100))
	f.Add(make([]byte, 1000))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 2048 {
			data = data[:2048]
		}

		decoder := bin.NewBinDecoder(data)
		var acctFields AccountsDbFields
		err := acctFields.UnmarshalWithDecoder(decoder)

		// Should handle any input gracefully
		_ = err
	})
}

// FuzzBankIncrementalSnapshotPersistence tests incremental snapshot metadata
func FuzzBankIncrementalSnapshotPersistence(f *testing.F) {
	f.Add(uint64(12345))
	f.Add(uint64(0))
	f.Add(uint64(100))

	f.Fuzz(func(t *testing.T, fullSlot uint64) {
		buf := new(bytes.Buffer)

		// Write full incremental snapshot data
		binary.Write(buf, binary.LittleEndian, fullSlot)

		// Full hash
		fullHash := make([]byte, 32)
		for i := range fullHash {
			fullHash[i] = byte(i)
		}
		buf.Write(fullHash)

		// Full capitalization
		binary.Write(buf, binary.LittleEndian, uint64(1000000000))

		// Incremental hash
		incrHash := make([]byte, 32)
		for i := range incrHash {
			incrHash[i] = byte(i + 32)
		}
		buf.Write(incrHash)

		// Incremental capitalization
		binary.Write(buf, binary.LittleEndian, uint64(500000000))

		decoder := bin.NewBinDecoder(buf.Bytes())
		var persistence BankIncrementalSnapshotPersistence
		err := persistence.UnmarshalWithDecoder(decoder)

		if err == nil {
			if persistence.FullSlot != fullSlot {
				t.Errorf("FullSlot mismatch: expected %d, got %d", fullSlot, persistence.FullSlot)
			}
		}
	})
}

// FuzzBlockHashVecDeserialization tests blockhash queue deserialization
func FuzzBlockHashVecDeserialization(f *testing.F) {
	f.Add(uint64(0))   // empty queue
	f.Add(uint64(1))   // single entry
	f.Add(uint64(10))  // multiple entries
	f.Add(uint64(100)) // larger set

	f.Fuzz(func(t *testing.T, count uint64) {
		// Limit count to prevent OOM
		if count > 100 {
			count = count % 100
		}

		// Create buffer with count field
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, count)

		// Add some hash entries
		for i := uint64(0); i < count && i < 10; i++ {
			// Hash (32 bytes) + fee calculator fields
			hash := make([]byte, 32)
			for j := range hash {
				hash[j] = byte(i + uint64(j))
			}
			buf.Write(hash)
			binary.Write(buf, binary.LittleEndian, uint64(5000)) // lamports_per_signature
		}

		decoder := bin.NewBinDecoder(buf.Bytes())
		var bhVec BlockHashVec
		err := bhVec.UnmarshalWithDecoder(decoder)

		// Should not panic
		_ = err
	})
}

// FuzzVersionedEpochStakesDeserialization tests epoch stakes parsing
func FuzzVersionedEpochStakesDeserialization(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 2048 {
			data = data[:2048]
		}

		decoder := bin.NewBinDecoder(data)
		var epochStakes VersionedEpochStakes
		err := epochStakes.UnmarshalWithDecoder(decoder)

		// Should handle gracefully
		_ = err
	})
}

// FuzzStakesDeserialization tests Stakes structure parsing
func FuzzStakesDeserialization(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 256))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 2048 {
			data = data[:2048]
		}

		decoder := bin.NewBinDecoder(data)
		var stakes Stakes
		err := stakes.UnmarshalWithDecoder(decoder)

		// Should not panic
		_ = err
	})
}

// FuzzVoteAccountDeserialization tests VoteAccount parsing
func FuzzVoteAccountDeserialization(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1024 {
			data = data[:1024]
		}

		decoder := bin.NewBinDecoder(data)
		var voteAcct VoteAccount
		err := voteAcct.UnmarshalWithDecoder(decoder)

		// Should handle errors without panicking
		_ = err
	})
}

// FuzzHashAgeDeserialization tests HashAge structure
func FuzzHashAgeDeserialization(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 50))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 256 {
			data = data[:256]
		}

		decoder := bin.NewBinDecoder(data)
		var hashAge HashAge
		err := hashAge.UnmarshalWithDecoder(decoder)

		// Should not panic
		_ = err
	})
}

// FuzzDelegationDeserialization tests Delegation parsing
func FuzzDelegationDeserialization(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 512 {
			data = data[:512]
		}

		decoder := bin.NewBinDecoder(data)
		var delegation Delegation
		err := delegation.UnmarshalWithDecoder(decoder)

		_ = err
	})
}
