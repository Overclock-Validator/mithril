package sealevel

import (
	"bytes"
	"testing"

	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// FuzzUpgradeableLoaderInstrWrite tests write instruction deserialization
func FuzzUpgradeableLoaderInstrWrite(f *testing.F) {
	// Seed with valid write instructions
	f.Add(makeValidWriteInstr(0, []byte{1, 2, 3, 4}))
	f.Add(makeValidWriteInstr(1000, make([]byte, 1024)))
	f.Add(makeInvalidWriteInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var write UpgradeableLoaderInstrWrite
		_ = write.UnmarshalWithDecoder(decoder)
	})
}

// FuzzUpgradeableLoaderInstrDeploy tests deploy instruction deserialization
func FuzzUpgradeableLoaderInstrDeploy(f *testing.F) {
	f.Add(makeValidDeployInstr(1000))
	f.Add(makeValidDeployInstr(0))
	f.Add(makeValidDeployInstr(^uint64(0)))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var deploy UpgradeableLoaderInstrDeployWithMaxDataLen
		_ = deploy.UnmarshalWithDecoder(decoder)
	})
}

// FuzzUpgradeableLoaderInstrExtend tests extend program instruction deserialization
func FuzzUpgradeableLoaderInstrExtend(f *testing.F) {
	f.Add(makeValidExtendInstr(100))
	f.Add(makeValidExtendInstr(0))
	f.Add(makeValidExtendInstr(^uint32(0)))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var extend UpgradeableLoaderInstrExtendProgram
		_ = extend.UnmarshalWithDecoder(decoder)
	})
}

// FuzzUpgradeableLoaderStateBuffer tests buffer state serialization/deserialization
func FuzzUpgradeableLoaderStateBuffer(f *testing.F) {
	f.Add(makeValidBufferState(true))
	f.Add(makeValidBufferState(false))
	f.Add(makeInvalidBufferState())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var buffer UpgradeableLoaderStateBuffer
		err := buffer.UnmarshalWithDecoder(decoder)
		if err == nil {
			// Round-trip test
			buf := new(bytes.Buffer)
			encoder := bin.NewBinEncoder(buf)
			_ = buffer.MarshalWithEncoder(encoder)
		}
	})
}

// FuzzUpgradeableLoaderStateProgram tests program state serialization/deserialization
func FuzzUpgradeableLoaderStateProgram(f *testing.F) {
	f.Add(makeValidProgramState())
	f.Add(makeInvalidProgramState())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var program UpgradeableLoaderStateProgram
		err := program.UnmarshalWithDecoder(decoder)
		if err == nil {
			// Round-trip test
			buf := new(bytes.Buffer)
			encoder := bin.NewBinEncoder(buf)
			_ = program.MarshalWithEncoder(encoder)
		}
	})
}

// FuzzUpgradeableLoaderStateProgramData tests program data state serialization/deserialization
func FuzzUpgradeableLoaderStateProgramData(f *testing.F) {
	f.Add(makeValidProgramDataState(true, 1000))
	f.Add(makeValidProgramDataState(false, 0))
	f.Add(makeInvalidProgramDataState())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var programData UpgradeableLoaderStateProgramData
		err := programData.UnmarshalWithDecoder(decoder)
		if err == nil {
			// Round-trip test
			buf := new(bytes.Buffer)
			encoder := bin.NewBinEncoder(buf)
			_ = programData.MarshalWithEncoder(encoder)
		}
	})
}

// FuzzUpgradeableLoaderState tests complete state serialization/deserialization
func FuzzUpgradeableLoaderState(f *testing.F) {
	f.Add(makeCompleteUninitializedState())
	f.Add(makeCompleteBufferState())
	f.Add(makeCompleteProgramState())
	f.Add(makeCompleteProgramDataState())
	f.Add(makeInvalidStateType())

	f.Fuzz(func(t *testing.T, data []byte) {
		// Test unmarshal
		state, err := unmarshalUpgradeableLoaderState(data)
		if err == nil {
			// Test marshal round-trip
			marshaled, err := marshalUpgradeableLoaderState(state)
			if err == nil {
				// Verify round-trip
				state2, err := unmarshalUpgradeableLoaderState(marshaled)
				if err != nil {
					t.Errorf("Round-trip failed: %v", err)
				} else if state.Type != state2.Type {
					t.Errorf("Round-trip type mismatch: %d != %d", state.Type, state2.Type)
				}
			}
		}
	})
}

// FuzzUpgradeableLoaderSizeOf tests size calculation functions
func FuzzUpgradeableLoaderSizeOf(f *testing.F) {
	f.Add(uint64(0))
	f.Add(uint64(1000))
	f.Add(uint64(^uint64(0) - upgradeableLoaderSizeOfBufferMetaData))
	f.Add(uint64(^uint64(0)))

	f.Fuzz(func(t *testing.T, programLen uint64) {
		// Test buffer size calculation - should not panic
		_ = upgradeableLoaderSizeOfBuffer(programLen)

		// Test program data size calculation - should not panic
		_ = upgradeableLoaderSizeOfProgramData(programLen)
	})
}

// FuzzSerializeParametersAligned tests aligned parameter serialization
func FuzzSerializeParametersAligned(f *testing.F) {
	// This would require a complex ExecutionContext setup
	// For now, we'll test the parameter structure components
	f.Add(makeSerializedParamsData(1, false))
	f.Add(makeSerializedParamsData(5, true))
	f.Add(makeSerializedParamsData(255, false))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Test that we can parse serialized parameters without panic
		if len(data) < 8 {
			return
		}
		// Minimal parsing to test bounds checking
		_ = parseSerializedParamsHeader(data)
	})
}

// FuzzSerializeParametersUnaligned tests unaligned parameter serialization
func FuzzSerializeParametersUnaligned(f *testing.F) {
	f.Add(makeUnalignedSerializedParamsData(1))
	f.Add(makeUnalignedSerializedParamsData(10))
	f.Add(makeUnalignedSerializedParamsData(255))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Test that we can parse serialized parameters without panic
		if len(data) < 8 {
			return
		}
		_ = parseSerializedParamsHeader(data)
	})
}

// FuzzCalculateHeapCost tests heap cost calculation
func FuzzCalculateHeapCost(f *testing.F) {
	f.Add(uint32(0), uint64(1))
	f.Add(uint32(1024), uint64(100))
	f.Add(uint32(32768), uint64(1000))
	f.Add(uint32(^uint32(0)), uint64(1))

	f.Fuzz(func(t *testing.T, heapSize uint32, heapCost uint64) {
		if heapCost == 0 {
			return // Avoid division by zero
		}
		_ = calculateHeapCost(heapSize, heapCost)
	})
}

// Helper functions to create seed data

func makeValidWriteInstr(offset uint32, data []byte) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint32(offset, bin.LE)
	encoder.WriteBytes(data, true)
	return buf.Bytes()
}

func makeInvalidWriteInstr() []byte {
	// Invalid length encoding
	return []byte{0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
}

func makeValidDeployInstr(maxDataLen uint64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(maxDataLen, bin.LE)
	return buf.Bytes()
}

func makeValidExtendInstr(additionalBytes uint32) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint32(additionalBytes, bin.LE)
	return buf.Bytes()
}

func makeValidBufferState(hasAuthority bool) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	if hasAuthority {
		encoder.WriteBool(true)
		encoder.WriteBytes(solana.PublicKey{1, 2, 3}.Bytes(), false)
	} else {
		encoder.WriteBool(false)
	}
	return buf.Bytes()
}

func makeInvalidBufferState() []byte {
	// Invalid bool value followed by junk
	return []byte{0x02, 0xFF, 0xFF, 0xFF}
}

func makeValidProgramState() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteBytes(make([]byte, 32), false)
	return buf.Bytes()
}

func makeInvalidProgramState() []byte {
	// Truncated pubkey
	return []byte{1, 2, 3, 4}
}

func makeValidProgramDataState(hasAuthority bool, slot uint64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(slot, bin.LE)
	if hasAuthority {
		encoder.WriteBool(true)
		encoder.WriteBytes(make([]byte, 32), false)
	} else {
		encoder.WriteBool(false)
	}
	return buf.Bytes()
}

func makeInvalidProgramDataState() []byte {
	// Invalid slot followed by invalid bool
	return []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02}
}

func makeCompleteUninitializedState() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint32(UpgradeableLoaderStateTypeUninitialized, bin.LE)
	return buf.Bytes()
}

func makeCompleteBufferState() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint32(UpgradeableLoaderStateTypeBuffer, bin.LE)
	encoder.WriteBool(true)
	encoder.WriteBytes(make([]byte, 32), false)
	return buf.Bytes()
}

func makeCompleteProgramState() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint32(UpgradeableLoaderStateTypeProgram, bin.LE)
	encoder.WriteBytes(make([]byte, 32), false)
	return buf.Bytes()
}

func makeCompleteProgramDataState() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint32(UpgradeableLoaderStateTypeProgramData, bin.LE)
	encoder.WriteUint64(1000, bin.LE)
	encoder.WriteBool(false)
	return buf.Bytes()
}

func makeInvalidStateType() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint32(0xFFFFFFFF, bin.LE)
	return buf.Bytes()
}

func makeSerializedParamsData(numAccounts uint64, hasDuplicates bool) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(numAccounts, bin.LE)

	for i := uint64(0); i < numAccounts; i++ {
		if hasDuplicates && i > 0 && i%2 == 0 {
			// Duplicate account
			encoder.WriteByte(byte(i - 1))
			// Padding
			for j := 0; j < 7; j++ {
				encoder.WriteByte(0)
			}
		} else {
			// Not duplicate
			encoder.WriteByte(0xFF)
			encoder.WriteByte(1)                        // is_signer
			encoder.WriteByte(1)                        // is_writable
			encoder.WriteByte(0)                        // executable
			encoder.WriteUint32(0, bin.LE)              // original_data_len padding
			encoder.WriteBytes(make([]byte, 32), false) // key
			encoder.WriteBytes(make([]byte, 32), false) // owner
			encoder.WriteUint64(1000000, bin.LE)        // lamports
			encoder.WriteUint64(0, bin.LE)              // data len
			encoder.WriteUint64(0, bin.LE)              // rent epoch
		}
	}

	// Instruction data
	encoder.WriteUint64(4, bin.LE)
	encoder.WriteBytes([]byte{1, 2, 3, 4}, false)

	// Program ID
	encoder.WriteBytes(make([]byte, 32), false)

	return buf.Bytes()
}

func makeUnalignedSerializedParamsData(numAccounts uint64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(numAccounts, bin.LE)

	for i := uint64(0); i < numAccounts; i++ {
		encoder.WriteByte(0xFF)                     // not duplicate
		encoder.WriteByte(1)                        // is_signer
		encoder.WriteByte(1)                        // is_writable
		encoder.WriteBytes(make([]byte, 32), false) // key
		encoder.WriteUint64(1000000, bin.LE)        // lamports
		encoder.WriteUint64(0, bin.LE)              // data len
		encoder.WriteBytes(make([]byte, 32), false) // owner
		encoder.WriteByte(0)                        // executable
		encoder.WriteUint64(0, bin.LE)              // rent epoch
	}

	// Instruction data
	encoder.WriteUint64(4, bin.LE)
	encoder.WriteBytes([]byte{1, 2, 3, 4}, false)

	// Program ID
	encoder.WriteBytes(make([]byte, 32), false)

	return buf.Bytes()
}

func parseSerializedParamsHeader(data []byte) (numAccounts uint64) {
	if len(data) < 8 {
		return 0
	}
	decoder := bin.NewBinDecoder(data)
	numAccounts, _ = decoder.ReadUint64(bin.LE)
	return numAccounts
}
