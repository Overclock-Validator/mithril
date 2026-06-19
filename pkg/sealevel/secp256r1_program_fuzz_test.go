package sealevel

import (
	"bytes"
	"testing"

	bin "github.com/gagliardetto/binary"
)

// FuzzSecp256r1SignatureOffsets tests secp256r1 signature offset parsing
func FuzzSecp256r1SignatureOffsets(f *testing.F) {
	f.Add(makeValidSecp256r1SignatureOffsets(0, 0, 0, 0, 0, 0, 100))
	f.Add(makeValidSecp256r1SignatureOffsets(1, 64, 2, 96, 3, 128, 32))
	f.Add(makeValidSecp256r1SignatureOffsets(255, 1000, 255, 2000, 255, 3000, 500))
	f.Add(makeInvalidSecp256r1SignatureOffsets())

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < Secp256r1SignatureOffsetsSerializedSize {
			return
		}
		_ = parseSecp256r1SignatureOffsets(data)
	})
}

// Helper functions to create seed data

func makeValidSecp256r1SignatureOffsets(sigIdx uint16, sigOff uint16, pkIdx uint16, pkOff uint16, msgIdx uint16, msgOff uint16, msgSize uint16) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint16(sigIdx, bin.LE)
	encoder.WriteUint16(sigOff, bin.LE)
	encoder.WriteUint16(pkIdx, bin.LE)
	encoder.WriteUint16(pkOff, bin.LE)
	encoder.WriteUint16(msgIdx, bin.LE)
	encoder.WriteUint16(msgOff, bin.LE)
	encoder.WriteUint16(msgSize, bin.LE)
	return buf.Bytes()
}

func makeInvalidSecp256r1SignatureOffsets() []byte {
	// Truncated offsets structure
	return []byte{1, 0, 2, 0, 3}
}
