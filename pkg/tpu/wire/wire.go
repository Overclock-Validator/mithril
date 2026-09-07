package wire

import (
	"bytes"
	stdbinary "encoding/binary"
	"errors"
	"fmt"

	bin "github.com/gagliardetto/binary"
)

const (
	// LegacyPacketDataSize is the transaction-size limit for legacy and v0
	// transactions. Those formats retain Solana's original packet bound.
	LegacyPacketDataSize = 1232
	// PacketDataSize is the largest transaction the TPU transport can carry.
	// SIMD-0385 v1 transactions are admitted up to this size over QUIC.
	PacketDataSize = 4096

	// These limits apply to every supported transaction version. Although the
	// legacy and v0 encodings can represent larger values, Agave rejects them
	// during transaction-view sanitization.
	MaxSignaturesPerTransaction = 12
	MaxInstructionsPerMessage   = 64
	MaxAccountsPerInstruction   = 255
)

var (
	ErrEmpty            = errors.New("empty transaction")
	ErrTooLarge         = errors.New("transaction exceeds packet size")
	ErrInvalidEncoding  = errors.New("invalid compact-u16 encoding")
	ErrInvalidSigCount  = errors.New("invalid signature count")
	ErrInvalidMessage   = errors.New("invalid message encoding")
	ErrSigCountMismatch = errors.New("signature count mismatch")
	ErrInsufficientData = errors.New("insufficient transaction data")
)

// Version is the transaction message version observed on the wire.
type Version uint8

const (
	VersionLegacy Version = iota
	VersionV0
	VersionV1
)

// View is a zero-copy parsed transaction wire layout. Legacy/v0 signatures
// precede the message; v1 signatures follow it.
type View struct {
	Wire          []byte
	Version       Version
	NumSignatures int
	SigsOffset    int
	MessageOffset int
	MessageEnd    int
}

// Parse locates the message and signature regions without copying them.
// Structural message validation is performed by Sanitize.
func Parse(wire []byte) (View, error) {
	if len(wire) == 0 {
		return View{}, ErrEmpty
	}
	if wire[0] == 0x81 {
		return parseV1(wire)
	}
	if wire[0]&0x80 != 0 {
		return View{}, ErrInvalidMessage
	}
	return ParseLegacy(wire)
}

// ParseLegacy parses the signature-prefixed envelope shared by legacy and v0
// transactions without copying wire bytes.
func ParseLegacy(wire []byte) (View, error) {
	if len(wire) == 0 {
		return View{}, ErrEmpty
	}
	if len(wire) > LegacyPacketDataSize {
		return View{}, ErrTooLarge
	}

	numSigs, size, err := bin.DecodeCompactU16(wire)
	if err != nil {
		return View{}, fmt.Errorf("%w: %v", ErrInvalidEncoding, err)
	}
	if numSigs <= 0 || numSigs > MaxSignaturesPerTransaction {
		return View{}, ErrInvalidSigCount
	}

	sigsOffset := size
	sigsLen := numSigs * 64
	if sigsLen > len(wire)-sigsOffset {
		return View{}, ErrInsufficientData
	}

	msgOffset := sigsOffset + sigsLen
	if msgOffset >= len(wire) {
		return View{}, ErrInsufficientData
	}

	version := VersionLegacy
	if wire[msgOffset]&0x80 != 0 {
		if wire[msgOffset] != 0x80 {
			// A v1 message cannot be wrapped in the old signature-prefixed
			// transaction envelope, and no other message version is supported.
			return View{}, ErrInvalidMessage
		}
		version = VersionV0
	}

	return View{
		Wire:          wire,
		Version:       version,
		NumSignatures: numSigs,
		SigsOffset:    sigsOffset,
		MessageOffset: msgOffset,
		MessageEnd:    len(wire),
	}, nil
}

func parseV1(wire []byte) (View, error) {
	if len(wire) > PacketDataSize {
		return View{}, ErrTooLarge
	}
	if len(wire) < 2 {
		return View{}, ErrInsufficientData
	}

	numSigs := int(wire[1])
	if numSigs <= 0 || numSigs > MaxSignaturesPerTransaction {
		return View{}, ErrInvalidSigCount
	}
	sigsLen := numSigs * 64
	if sigsLen > len(wire) {
		return View{}, ErrInsufficientData
	}
	messageEnd := len(wire) - sigsLen
	if messageEnd <= 1 {
		return View{}, ErrInsufficientData
	}

	return View{
		Wire:          wire,
		Version:       VersionV1,
		NumSignatures: numSigs,
		SigsOffset:    messageEnd,
		MessageOffset: 0,
		MessageEnd:    messageEnd,
	}, nil
}

func (v View) FirstSignature() []byte {
	if v.NumSignatures == 0 || v.SigsOffset < 0 || v.SigsOffset+64 > len(v.Wire) {
		return nil
	}
	return v.Wire[v.SigsOffset : v.SigsOffset+64]
}

func (v View) Message() []byte {
	if v.MessageOffset < 0 || v.MessageEnd < v.MessageOffset || v.MessageEnd > len(v.Wire) {
		return nil
	}
	return v.Wire[v.MessageOffset:v.MessageEnd]
}

// Sanitize performs allocation-free structural checks before sigverify.
func Sanitize(wire []byte) (View, error) {
	v, err := Parse(wire)
	if err != nil {
		return View{}, err
	}

	if v.Version == VersionV1 {
		err = sanitizeV1(v)
	} else {
		err = sanitizeLegacyOrV0(v)
	}
	if err != nil {
		return View{}, err
	}
	return v, nil
}

func sanitizeLegacyOrV0(v View) error {
	msg := v.Message()
	pos := 0
	if v.Version == VersionV0 {
		pos++ // 0x80 version prefix
	}
	if !canRead(msg, pos, 3) {
		return ErrInsufficientData
	}
	required := int(msg[pos])
	readonlySigned := int(msg[pos+1])
	readonlyUnsigned := int(msg[pos+2])
	pos += 3

	numAccounts, err := readCompact(msg, &pos)
	if err != nil {
		return err
	}
	if numAccounts <= 0 || numAccounts > 256 {
		return ErrInvalidMessage
	}
	if err := sanitizeHeader(required, readonlySigned, readonlyUnsigned, numAccounts, v.NumSignatures); err != nil {
		return err
	}
	if !skip(msg, &pos, numAccounts*32+32) { // static keys + recent blockhash
		return ErrInsufficientData
	}

	numInstructions, err := readCompact(msg, &pos)
	if err != nil {
		return err
	}
	if numInstructions > MaxInstructionsPerMessage {
		return ErrInvalidMessage
	}
	maxAccountIndex := -1
	for i := 0; i < numInstructions; i++ {
		if !canRead(msg, pos, 1) {
			return ErrInsufficientData
		}
		programIndex := int(msg[pos])
		pos++
		if programIndex == 0 || programIndex >= numAccounts {
			return ErrInvalidMessage
		}

		numIxAccounts, err := readCompact(msg, &pos)
		if err != nil {
			return err
		}
		if numIxAccounts > MaxAccountsPerInstruction {
			return ErrInvalidMessage
		}
		if !canRead(msg, pos, numIxAccounts) {
			return ErrInsufficientData
		}
		for _, index := range msg[pos : pos+numIxAccounts] {
			if int(index) > maxAccountIndex {
				maxAccountIndex = int(index)
			}
		}
		pos += numIxAccounts

		dataLen, err := readCompact(msg, &pos)
		if err != nil {
			return err
		}
		if !skip(msg, &pos, dataLen) {
			return ErrInsufficientData
		}
	}

	totalAccounts := numAccounts
	if v.Version == VersionV0 {
		numLookups, err := readCompact(msg, &pos)
		if err != nil {
			return err
		}
		for i := 0; i < numLookups; i++ {
			if !skip(msg, &pos, 32) {
				return ErrInsufficientData
			}
			writable, err := readCompact(msg, &pos)
			if err != nil {
				return err
			}
			if !skip(msg, &pos, writable) {
				return ErrInsufficientData
			}
			readonly, err := readCompact(msg, &pos)
			if err != nil {
				return err
			}
			if !skip(msg, &pos, readonly) {
				return ErrInsufficientData
			}
			if writable+readonly == 0 {
				return ErrInvalidMessage
			}
			totalAccounts += writable + readonly
			if totalAccounts > 256 {
				return ErrInvalidMessage
			}
		}
	}

	if pos != len(msg) || maxAccountIndex >= totalAccounts {
		return ErrInvalidMessage
	}
	return nil
}

func sanitizeV1(v View) error {
	msg := v.Message()
	// prefix(1) + header(3) + config mask(4) + lifetime(32) +
	// instruction count(1) + address count(1)
	const fixedSize = 42
	if len(msg) < fixedSize || msg[0] != 0x81 {
		return ErrInsufficientData
	}

	required := int(msg[1])
	readonlySigned := int(msg[2])
	readonlyUnsigned := int(msg[3])
	if required > MaxSignaturesPerTransaction {
		return ErrInvalidSigCount
	}
	if required != v.NumSignatures {
		return ErrSigCountMismatch
	}

	mask := stdbinary.LittleEndian.Uint32(msg[4:8])
	if mask&^uint32(0x1f) != 0 || (mask&0x3 != 0 && mask&0x3 != 0x3) {
		return ErrInvalidMessage
	}
	numInstructions := int(msg[40])
	numAccounts := int(msg[41])
	if numInstructions > MaxInstructionsPerMessage || numAccounts > 64 {
		return ErrInvalidMessage
	}
	if err := sanitizeHeader(required, readonlySigned, readonlyUnsigned, numAccounts, v.NumSignatures); err != nil {
		return err
	}

	pos := fixedSize
	keysStart := pos
	if !skip(msg, &pos, numAccounts*32) {
		return ErrInsufficientData
	}
	for i := 0; i < numAccounts; i++ {
		left := msg[keysStart+i*32 : keysStart+(i+1)*32]
		for j := i + 1; j < numAccounts; j++ {
			right := msg[keysStart+j*32 : keysStart+(j+1)*32]
			if bytes.Equal(left, right) {
				return ErrInvalidMessage
			}
		}
	}

	if mask&0x3 == 0x3 && !skip(msg, &pos, 8) {
		return ErrInsufficientData
	}
	if mask&0x4 != 0 && !skip(msg, &pos, 4) {
		return ErrInsufficientData
	}
	if mask&0x8 != 0 && !skip(msg, &pos, 4) {
		return ErrInsufficientData
	}
	if mask&0x10 != 0 {
		if !canRead(msg, pos, 4) {
			return ErrInsufficientData
		}
		heapSize := stdbinary.LittleEndian.Uint32(msg[pos : pos+4])
		if heapSize < 32*1024 || heapSize > 256*1024 || heapSize%1024 != 0 {
			return ErrInvalidMessage
		}
		pos += 4
	}

	var instructionAccounts [64]uint8
	var instructionDataLens [64]uint16
	for i := 0; i < numInstructions; i++ {
		if !canRead(msg, pos, 4) {
			return ErrInsufficientData
		}
		programIndex := int(msg[pos])
		if programIndex == 0 || programIndex >= numAccounts {
			return ErrInvalidMessage
		}
		instructionAccounts[i] = msg[pos+1]
		instructionDataLens[i] = stdbinary.LittleEndian.Uint16(msg[pos+2 : pos+4])
		pos += 4
	}

	for i := 0; i < numInstructions; i++ {
		numIxAccounts := int(instructionAccounts[i])
		dataLen := int(instructionDataLens[i])
		if !canRead(msg, pos, numIxAccounts) {
			return ErrInsufficientData
		}
		for _, index := range msg[pos : pos+numIxAccounts] {
			if int(index) >= numAccounts {
				return ErrInvalidMessage
			}
		}
		pos += numIxAccounts
		if !skip(msg, &pos, dataLen) {
			return ErrInsufficientData
		}
	}

	if pos != len(msg) {
		return ErrInvalidMessage
	}
	return nil
}

func sanitizeHeader(required, readonlySigned, readonlyUnsigned, numAccounts, numSignatures int) error {
	if required <= 0 || required != numSignatures {
		return ErrSigCountMismatch
	}
	if readonlySigned >= required || required+readonlyUnsigned > numAccounts {
		return ErrInvalidMessage
	}
	return nil
}

func readCompact(data []byte, pos *int) (int, error) {
	if *pos < 0 || *pos > len(data) {
		return 0, ErrInsufficientData
	}
	value, size, err := bin.DecodeCompactU16(data[*pos:])
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidEncoding, err)
	}
	*pos += size
	return value, nil
}

func canRead(data []byte, pos, size int) bool {
	return pos >= 0 && size >= 0 && pos <= len(data) && size <= len(data)-pos
}

func skip(data []byte, pos *int, size int) bool {
	if !canRead(data, *pos, size) {
		return false
	}
	*pos += size
	return true
}
