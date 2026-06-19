package sealevel

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/gagliardetto/solana-go"
)

// Fuzzes Instruction structure creation and validation
func FuzzInstructionCreation(f *testing.F) {
	// Seed with minimal valid instruction
	validPubkey, _ := solana.PublicKeyFromBase58("11111111111111111111111111111111")
	f.Add(uint8(1), true, false, []byte{0x00})

	f.Fuzz(func(t *testing.T, numAccounts uint8, isSigner, isWritable bool, data []byte) {
		// Limit account count to prevent OOM
		if numAccounts > 64 {
			numAccounts = numAccounts % 64
		}

		// Build instruction with fuzzed parameters
		accounts := make([]AccountMeta, numAccounts)
		for i := range accounts {
			accounts[i] = AccountMeta{
				Pubkey:     validPubkey,
				IsSigner:   isSigner && (i == 0), // Only first can be signer for simplicity
				IsWritable: isWritable,
			}
		}

		instr := Instruction{
			Accounts:  accounts,
			Data:      data,
			ProgramId: validPubkey,
		}

		// Verify basic structure
		if len(instr.Accounts) != int(numAccounts) {
			t.Errorf("Account count mismatch: got %d, want %d", len(instr.Accounts), numAccounts)
		}

		if !bytes.Equal(instr.Data, data) {
			t.Error("Instruction data mismatch")
		}
	})
}

// Fuzzes Account MetaC serialization and deserialization for VM compatibility
func FuzzAccountMetaCSerialize(f *testing.F) {
	// Seed with various address and flag combinations
	f.Add(uint64(0x100000), byte(0), byte(0))           // Read-only, non-signer
	f.Add(uint64(0x200000), byte(1), byte(0))           // Signer, read-only
	f.Add(uint64(0x300000), byte(0), byte(1))           // Non-signer, writable
	f.Add(uint64(0x400000), byte(1), byte(1))           // Signer, writable
	f.Add(uint64(0xFFFFFFFFFFFFFFFF), byte(0), byte(0)) // Max address

	f.Fuzz(func(t *testing.T, pubkeyAddr uint64, isSigner, isWritable byte) {
		meta := SolAccountMetaC{
			PubkeyAddr: pubkeyAddr,
			IsSigner:   isSigner,
			IsWritable: isWritable,
		}

		// Serialize to bytes with proper padding for 16-byte alignment
		var buf bytes.Buffer
		if err := binary.Write(&buf, binary.LittleEndian, meta.PubkeyAddr); err != nil {
			t.Fatalf("Failed to write PubkeyAddr: %v", err)
		}
		buf.WriteByte(meta.IsWritable)
		buf.WriteByte(meta.IsSigner)
		// Add 6 bytes of padding to match SolAccountMetaCSize (16 bytes total)
		buf.Write(make([]byte, 6))

		serialized := buf.Bytes()

		// Verify size
		if len(serialized) != SolAccountMetaCSize {
			t.Errorf("Serialized size mismatch: got %d, want %d", len(serialized), SolAccountMetaCSize)
		}

		// Deserialize and verify round-trip (skip padding bytes)
		reader := bytes.NewReader(serialized)
		var deserialized SolAccountMetaC
		if err := binary.Read(reader, binary.LittleEndian, &deserialized.PubkeyAddr); err != nil {
			t.Fatalf("Failed to read PubkeyAddr: %v", err)
		}
		var err error
		deserialized.IsWritable, err = reader.ReadByte()
		if err != nil {
			t.Fatalf("Failed to read IsWritable: %v", err)
		}
		deserialized.IsSigner, err = reader.ReadByte()
		if err != nil {
			t.Fatalf("Failed to read IsSigner: %v", err)
		}
		// Skip 6 bytes of padding (reader will have 6 bytes remaining)

		if deserialized.PubkeyAddr != meta.PubkeyAddr {
			t.Errorf("PubkeyAddr mismatch: got %d, want %d", deserialized.PubkeyAddr, meta.PubkeyAddr)
		}
		if deserialized.IsSigner != meta.IsSigner {
			t.Errorf("IsSigner mismatch: got %d, want %d", deserialized.IsSigner, meta.IsSigner)
		}
		if deserialized.IsWritable != meta.IsWritable {
			t.Errorf("IsWritable mismatch: got %d, want %d", deserialized.IsWritable, meta.IsWritable)
		}
	})
}

// Fuzzes AccountMetaRust serialization for Rust program compatibility
func FuzzAccountMetaRustSerialize(f *testing.F) {
	validPubkey, _ := solana.PublicKeyFromBase58("11111111111111111111111111111111")

	f.Add(validPubkey[:], byte(0), byte(0))
	f.Add(validPubkey[:], byte(1), byte(1))
	f.Add(validPubkey[:], byte(255), byte(255)) // Invalid flag values

	f.Fuzz(func(t *testing.T, pubkey []byte, isSigner, isWritable byte) {
		// Ensure pubkey is exactly 32 bytes
		if len(pubkey) != 32 {
			if len(pubkey) < 32 {
				pubkey = append(pubkey, make([]byte, 32-len(pubkey))...)
			} else {
				pubkey = pubkey[:32]
			}
		}

		var pk solana.PublicKey
		copy(pk[:], pubkey)

		meta := SolAccountMetaRust{
			Pubkey:     pk,
			IsSigner:   isSigner,
			IsWritable: isWritable,
		}

		// Serialize
		var buf bytes.Buffer
		buf.Write(meta.Pubkey[:])
		buf.WriteByte(meta.IsSigner)
		buf.WriteByte(meta.IsWritable)

		serialized := buf.Bytes()

		// Verify size
		if len(serialized) != SolAccountMetaRustSize {
			t.Errorf("Serialized size mismatch: got %d, want %d", len(serialized), SolAccountMetaRustSize)
		}

		// Deserialize and verify
		reader := bytes.NewReader(serialized)
		var deserialized SolAccountMetaRust
		if _, err := reader.Read(deserialized.Pubkey[:]); err != nil {
			t.Fatalf("Failed to read Pubkey: %v", err)
		}
		var err error
		deserialized.IsSigner, err = reader.ReadByte()
		if err != nil {
			t.Fatalf("Failed to read IsSigner: %v", err)
		}
		deserialized.IsWritable, err = reader.ReadByte()
		if err != nil {
			t.Fatalf("Failed to read IsWritable: %v", err)
		}

		if deserialized.Pubkey != meta.Pubkey {
			t.Error("Pubkey mismatch after round-trip")
		}
		if deserialized.IsSigner != meta.IsSigner {
			t.Errorf("IsSigner mismatch: got %d, want %d", deserialized.IsSigner, meta.IsSigner)
		}
		if deserialized.IsWritable != meta.IsWritable {
			t.Errorf("IsWritable mismatch: got %d, want %d", deserialized.IsWritable, meta.IsWritable)
		}
	})
}

// Fuzzes InstructionCtx account indexing and resolution
func FuzzInstructionCtxAccountIndexing(f *testing.F) {
	validPubkey, _ := solana.PublicKeyFromBase58("11111111111111111111111111111111")

	// Seed with various indexing scenarios
	f.Add(uint8(3), uint64(0))   // Valid index
	f.Add(uint8(3), uint64(2))   // Last valid index
	f.Add(uint8(3), uint64(3))   // Out of bounds
	f.Add(uint8(0), uint64(0))   // Empty program accounts
	f.Add(uint8(10), uint64(15)) // Large out of bounds

	f.Fuzz(func(t *testing.T, numAccounts uint8, queryIndex uint64) {
		// Limit to prevent OOM
		if numAccounts > 64 {
			numAccounts = numAccounts % 64
		}

		// Build instruction context
		programAccts := make([]uint64, numAccounts)
		for i := range programAccts {
			programAccts[i] = uint64(i * 100) // Some transaction indices
		}

		instrCtx := &InstructionCtx{
			programId:       validPubkey,
			ProgramAccounts: programAccts,
		}

		// Test index resolution
		txnIdx, err := instrCtx.IndexOfProgramAccountInTransaction(queryIndex)

		// Verify bounds checking
		if queryIndex >= uint64(len(programAccts)) || numAccounts == 0 {
			// Should return error for out-of-bounds
			if err == nil {
				t.Errorf("Expected error for out-of-bounds index %d (numAccounts=%d), got nil", queryIndex, numAccounts)
			}
			if err != InstrErrNotEnoughAccountKeys {
				t.Errorf("Expected InstrErrNotEnoughAccountKeys, got %v", err)
			}
		} else {
			// Should succeed for in-bounds
			if err != nil {
				t.Errorf("Unexpected error for valid index %d: %v", queryIndex, err)
			}
			// Verify correct mapping
			if txnIdx != programAccts[queryIndex] {
				t.Errorf("Index mismatch: got %d, want %d", txnIdx, programAccts[queryIndex])
			}
		}

		// Verify NumberOfProgramAccounts
		if instrCtx.NumberOfProgramAccounts() != uint64(numAccounts) {
			t.Errorf("NumberOfProgramAccounts mismatch: got %d, want %d",
				instrCtx.NumberOfProgramAccounts(), numAccounts)
		}
	})
}
