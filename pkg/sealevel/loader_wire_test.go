package sealevel

import (
	"bytes"
	"encoding/binary"
	bin "github.com/gagliardetto/binary"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUpgradeableLoaderWriteWireLayout(t *testing.T) {
	instruction := UpgradeableLoaderInstrWrite{Offset: 0x12345678, Bytes: []byte{0xAB, 0xCD}}
	var buf bytes.Buffer
	require.NoError(t, instruction.MarshalWithEncoder(bin.NewBinEncoder(&buf)))
	require.Equal(t, []byte{1, 0, 0, 0, 0x78, 0x56, 0x34, 0x12, 2, 0, 0, 0, 0, 0, 0, 0, 0xAB, 0xCD}, buf.Bytes())
	var decoded UpgradeableLoaderInstrWrite
	require.NoError(t, decoded.UnmarshalWithDecoder(bin.NewBinDecoder(buf.Bytes()[4:])))
	require.Equal(t, instruction, decoded)
}

func TestSolAccountMetaCWireLayout(t *testing.T) {
	for _, bits := range [][2]byte{{0, 0}, {1, 0}, {0, 1}, {1, 1}} {
		meta := SolAccountMetaC{PubkeyAddr: 0x12345678, IsWritable: bits[0], IsSigner: bits[1]}
		wire, err := meta.Marshal()
		require.NoError(t, err)
		want := binary.LittleEndian.AppendUint64(nil, meta.PubkeyAddr)
		want = append(want, bits[:]...)
		require.Equal(t, want, wire)
	}
}
