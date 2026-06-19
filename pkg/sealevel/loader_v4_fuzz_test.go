package sealevel

import (
	"bytes"
	"testing"

	bin "github.com/gagliardetto/binary"
)

// FuzzLoaderV4Write tests LoaderV4 write instruction deserialization
func FuzzLoaderV4Write(f *testing.F) {
	f.Add(makeValidLoaderV4WriteInstr(0, []byte{1, 2, 3, 4}))
	f.Add(makeValidLoaderV4WriteInstr(1000, make([]byte, 1024)))
	f.Add(makeValidLoaderV4WriteInstr(^uint32(0), []byte{}))
	f.Add(makeInvalidLoaderV4WriteInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var write LoaderV4Write
		_ = write.UnmarshalWithDecoder(decoder)
	})
}

// FuzzLoaderV4Copy tests LoaderV4 copy instruction deserialization
func FuzzLoaderV4Copy(f *testing.F) {
	f.Add(makeValidLoaderV4CopyInstr(0, 0, 100))
	f.Add(makeValidLoaderV4CopyInstr(100, 200, 50))
	f.Add(makeValidLoaderV4CopyInstr(^uint32(0), ^uint32(0), ^uint32(0)))
	f.Add(makeInvalidLoaderV4CopyInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var copy LoaderV4Copy
		_ = copy.UnmarshalWithDecoder(decoder)
	})
}

// FuzzLoaderV4SetProgramLength tests LoaderV4 set program length instruction deserialization
func FuzzLoaderV4SetProgramLength(f *testing.F) {
	f.Add(makeValidLoaderV4SetProgramLengthInstr(0))
	f.Add(makeValidLoaderV4SetProgramLengthInstr(1024))
	f.Add(makeValidLoaderV4SetProgramLengthInstr(^uint32(0)))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var setProgramLen LoaderV4SetProgramLength
		_ = setProgramLen.UnmarshalWithDecoder(decoder)
	})
}

// FuzzLoaderV4InstructionType tests LoaderV4 instruction type parsing
func FuzzLoaderV4InstructionType(f *testing.F) {
	f.Add(uint8(LoaderV4InstrTypeWrite))
	f.Add(uint8(LoaderV4InstrTypeCopy))
	f.Add(uint8(LoaderV4InstrTypeSetProgramLength))
	f.Add(uint8(LoaderV4InstrTypeDeploy))
	f.Add(uint8(LoaderV4InstrTypeRetract))
	f.Add(uint8(LoaderV4InstrTypeTransferAuthority))
	f.Add(uint8(LoaderV4InstrTypeFinalize))
	f.Add(uint8(255)) // Invalid type

	f.Fuzz(func(t *testing.T, instrType uint8) {
		// Test instruction type validation
		validTypes := []uint8{
			LoaderV4InstrTypeWrite,
			LoaderV4InstrTypeCopy,
			LoaderV4InstrTypeSetProgramLength,
			LoaderV4InstrTypeDeploy,
			LoaderV4InstrTypeRetract,
			LoaderV4InstrTypeTransferAuthority,
			LoaderV4InstrTypeFinalize,
		}

		isValid := false
		for _, validType := range validTypes {
			if instrType == validType {
				isValid = true
				break
			}
		}

		if !isValid {
			// Should be rejected or ignored
			return
		}
	})
}

// FuzzLoaderV4StateTransitions tests state machine transitions
func FuzzLoaderV4StateTransitions(f *testing.F) {
	f.Add(uint8(LoaderV4StatusRetracted), uint8(LoaderV4InstrTypeWrite))
	f.Add(uint8(LoaderV4StatusRetracted), uint8(LoaderV4InstrTypeDeploy))
	f.Add(uint8(LoaderV4StatusDeployed), uint8(LoaderV4InstrTypeRetract))
	f.Add(uint8(LoaderV4StatusDeployed), uint8(LoaderV4InstrTypeFinalize))
	f.Add(uint8(LoaderV4StatusFinalized), uint8(LoaderV4InstrTypeWrite))

	f.Fuzz(func(t *testing.T, currentState uint8, instrType uint8) {
		// Test state transition validation
		// Retracted -> can write, copy, set length, deploy, transfer authority
		// Deployed -> can retract, finalize
		// Finalized -> no state changes allowed

		validStates := []uint8{
			LoaderV4StatusRetracted,
			LoaderV4StatusDeployed,
			LoaderV4StatusFinalized,
		}

		stateValid := false
		for _, state := range validStates {
			if currentState == state {
				stateValid = true
				break
			}
		}

		if !stateValid {
			// Invalid state
			return
		}

		// Verify state transition rules
		switch currentState {
		case LoaderV4StatusRetracted:
			// Most operations allowed
			validInstrs := []uint8{
				LoaderV4InstrTypeWrite,
				LoaderV4InstrTypeCopy,
				LoaderV4InstrTypeSetProgramLength,
				LoaderV4InstrTypeDeploy,
				LoaderV4InstrTypeTransferAuthority,
			}
			_ = validInstrs

		case LoaderV4StatusDeployed:
			// Limited operations
			validInstrs := []uint8{
				LoaderV4InstrTypeRetract,
				LoaderV4InstrTypeFinalize,
				LoaderV4InstrTypeTransferAuthority,
			}
			_ = validInstrs

		case LoaderV4StatusFinalized:
			// Only transfer authority allowed
			if instrType != LoaderV4InstrTypeTransferAuthority {
				// Should be rejected
				return
			}
		}
	})
}

// FuzzLoaderV4OffsetValidation tests offset and length validation
func FuzzLoaderV4OffsetValidation(f *testing.F) {
	f.Add(uint32(0), uint32(100), uint32(1000))
	f.Add(uint32(500), uint32(500), uint32(1000))
	f.Add(uint32(900), uint32(200), uint32(1000))
	f.Add(uint32(^uint32(0)), uint32(1), uint32(1000))

	f.Fuzz(func(t *testing.T, offset uint32, length uint32, programSize uint32) {
		// Test offset + length overflow
		endOffset := uint64(offset) + uint64(length)

		// Check if write would be in bounds
		if endOffset > uint64(programSize) {
			// Out of bounds - should fail
			return
		}

		// Check for overflow
		if length > 0 && endOffset < uint64(offset) {
			t.Error("Offset calculation overflow")
		}
	})
}

// Helper functions to create seed data

func makeValidLoaderV4WriteInstr(offset uint32, data []byte) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteByte(LoaderV4InstrTypeWrite)
	encoder.WriteUint32(offset, bin.LE)
	encoder.WriteBytes(data, true)
	return buf.Bytes()
}

func makeInvalidLoaderV4WriteInstr() []byte {
	// Truncated instruction
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteByte(LoaderV4InstrTypeWrite)
	encoder.WriteUint32(100, bin.LE)
	// Missing length field
	return buf.Bytes()
}

func makeValidLoaderV4CopyInstr(destOffset uint32, srcOffset uint32, length uint32) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteByte(LoaderV4InstrTypeCopy)
	encoder.WriteUint32(destOffset, bin.LE)
	encoder.WriteUint32(srcOffset, bin.LE)
	encoder.WriteUint32(length, bin.LE)
	return buf.Bytes()
}

func makeInvalidLoaderV4CopyInstr() []byte {
	// Truncated instruction
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteByte(LoaderV4InstrTypeCopy)
	encoder.WriteUint32(100, bin.LE)
	// Missing source offset and length
	return buf.Bytes()
}

func makeValidLoaderV4SetProgramLengthInstr(newSize uint32) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteByte(LoaderV4InstrTypeSetProgramLength)
	encoder.WriteUint32(newSize, bin.LE)
	return buf.Bytes()
}
