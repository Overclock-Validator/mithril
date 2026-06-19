package genesis

import (
	"testing"

	bin "github.com/gagliardetto/binary"
)

// FuzzGenesisUnmarshal tests genesis config deserialization with malformed data
func FuzzGenesisUnmarshal(f *testing.F) {
	// Seed corpus
	f.Add([]byte{})
	f.Add(make([]byte, 100))
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Limit size to prevent excessive memory usage
		if len(data) > 10000 {
			return
		}

		decoder := bin.NewBinDecoder(data)
		genesis := &Genesis{}

		// Should handle malformed data gracefully
		err := genesis.UnmarshalWithDecoder(decoder)

		// Error expected for most random data
		if err != nil {
			// Expected
			return
		}

		// Verify structure access - should not panic
		_ = genesis.CreationTime
		_ = len(genesis.Accounts)
	})
}

// FuzzAccountEntryUnmarshal tests genesis account entry deserialization
func FuzzAccountEntryUnmarshal(f *testing.F) {
	// Seed corpus
	f.Add([]byte{})
	f.Add(make([]byte, 50))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Limit size
		if len(data) > 5000 {
			return
		}

		decoder := bin.NewBinDecoder(data)
		entry := &AccountEntry{}

		// Should handle malformed data gracefully
		err := entry.UnmarshalWithDecoder(decoder)

		// Error expected for most random data
		if err != nil {
			// Expected
			return
		}

		// Basic validation - should not panic
		_ = entry.Pubkey
		_ = entry.Account
	})
}

// FuzzBuiltinProgramUnmarshal tests builtin program deserialization
func FuzzBuiltinProgramUnmarshal(f *testing.F) {
	// Seed corpus
	f.Add([]byte{})
	f.Add(make([]byte, 50))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Limit size
		if len(data) > 1000 {
			return
		}

		decoder := bin.NewBinDecoder(data)
		program := &BuiltinProgram{}

		// Should handle malformed data gracefully
		err := program.UnmarshalWithDecoder(decoder)

		// Error expected for most random data
		if err != nil {
			// Expected
			return
		}

		// Verify structure - should not panic
		_ = program.Pubkey
	})
}
