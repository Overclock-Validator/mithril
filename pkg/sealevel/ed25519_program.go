package sealevel

import (
	stded25519 "crypto/ed25519"

	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sigverify"
)

const DataStart = (SignatureOffsetsSerializedSize + SignatureOffsetStarts)
const SignatureOffsetStarts = 2
const SignatureOffsetsSerializedSize = 14

const SignatureSerializedSize = 64
const PubkeySerializedSize = 32

const Ed25519SignatureOffsetsSize = 14

type Ed25519SignatureOffsets struct {
	SignatureOffset           uint16
	SignatureInstructionIndex uint16
	PublicKeyOffset           uint16
	PublicKeyInstructionIndex uint16
	MessageDataOffset         uint16
	MessageDataSize           uint16
	MessageInstructionIndex   uint16
}

func (offsets *Ed25519SignatureOffsets) UnmarshalWithDecoder(buf io.Reader) error {
	var structBytes [Ed25519SignatureOffsetsSize]byte

	_, err := buf.Read(structBytes[:])
	if err != nil {
		return err
	}

	offsets.SignatureOffset = binary.LittleEndian.Uint16(structBytes[:2])
	offsets.SignatureInstructionIndex = binary.LittleEndian.Uint16(structBytes[2:4])
	offsets.PublicKeyOffset = binary.LittleEndian.Uint16(structBytes[4:6])
	offsets.PublicKeyInstructionIndex = binary.LittleEndian.Uint16(structBytes[6:8])
	offsets.MessageDataOffset = binary.LittleEndian.Uint16(structBytes[8:10])
	offsets.MessageDataSize = binary.LittleEndian.Uint16(structBytes[10:12])
	offsets.MessageInstructionIndex = binary.LittleEndian.Uint16(structBytes[12:14])

	return nil
}

func Ed25519GetDataSlice(txCtx *TransactionCtx, index uint16, offset uint16, size uint16) ([]byte, error) {

	var data []byte
	var dataSize uint64

	// data from current instruction
	if index == math.MaxUint16 {
		instrCtx, _ := txCtx.CurrentInstructionCtx()
		data = instrCtx.Data
		dataSize = uint64(len(data))
	} else {
		if int(index) >= len(txCtx.AllInstructions) {
			return nil, PrecompileErrDataOffset
		}
		data = txCtx.AllInstructions[index].Data
		dataSize = uint64(len(data))
	}

	if uint64(offset)+uint64(size) > dataSize {
		return nil, PrecompileErrSignature
	}

	return data[offset : offset+size], nil
}

func Ed25519ProgramExecute(execCtx *ExecutionCtx) error {

	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	data := instrCtx.Data
	dataLen := uint64(len(data))

	if dataLen < DataStart {
		if dataLen == 2 && data[0] == 0 {
			return nil
		}
		return PrecompileErrInstrDataSize
	}

	numSignatures := data[0]

	if numSignatures == 0 {
		return PrecompileErrInstrDataSize
	}

	expectedDataSize := (uint64(numSignatures) * SignatureOffsetsSerializedSize) + SignatureOffsetStarts
	if dataLen < expectedDataSize {
		return PrecompileErrInstrDataSize
	}

	off := SignatureOffsetStarts
	for count := uint64(0); count < uint64(numSignatures); count++ {
		var offsets Ed25519SignatureOffsets
		err := offsets.UnmarshalWithDecoder(bytes.NewReader(data[off:]))
		if err != nil {
			panic("shouldn't happen")
		}

		off += SignatureOffsetsSerializedSize

		signature, err := Ed25519GetDataSlice(txCtx, offsets.SignatureInstructionIndex, offsets.SignatureOffset, SignatureSerializedSize)
		if err != nil {
			return PrecompileErrDataOffset
		}

		pubkey, err := Ed25519GetDataSlice(txCtx, offsets.PublicKeyInstructionIndex, offsets.PublicKeyOffset, PubkeySerializedSize)
		if err != nil {
			return PrecompileErrDataOffset
		}

		msg, err := Ed25519GetDataSlice(txCtx, offsets.MessageInstructionIndex, offsets.MessageDataOffset, offsets.MessageDataSize)
		if err != nil {
			return PrecompileErrDataOffset
		}

		if len(pubkey) != PubkeySerializedSize {
			return PrecompileErrDataOffset
		}

		// Signatures are verified one at a time on purpose. The reference
		// walks entries in order and returns the FIRST error, so batching
		// these would let a later entry's offset error preempt an earlier
		// entry's signature error. That error code reaches the ledger, so the
		// ordering is consensus-visible and is not worth trading for the
		// throughput of a batch that is usually one or two signatures deep.
		if execCtx.Features.IsActive(features.Ed25519PrecompileVerifyStrict) {
			// DalekStrict: reject small-order A and R, accept a non-canonical
			// A and hash its original bytes.
			if !sigverify.VerifyOne((*[32]byte)(pubkey), msg[:offsets.MessageDataSize], signature[:64]) {
				return PrecompileErrSignature
			}
		} else {
			// Before the feature gate the reference used plain (non-strict)
			// verification, which is exactly crypto/ed25519.Verify: cofactorless,
			// no small-order rejection, non-canonical A accepted, R compared as
			// bytes. narya exposes no StdlibCompat entry point, and the stdlib
			// is definitionally correct here, so this path uses it directly.
			if !stded25519.Verify(stded25519.PublicKey(pubkey), msg[:offsets.MessageDataSize], signature[:64]) {
				return PrecompileErrSignature
			}
		}
	}

	return nil
}
