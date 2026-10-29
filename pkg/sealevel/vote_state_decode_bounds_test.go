package sealevel

import (
	"bytes"
	"encoding/binary"
	"testing"

	bin "github.com/gagliardetto/binary"
	"github.com/stretchr/testify/require"
)

func TestVoteStateDecodeRejectsCountsBeyondPayload(t *testing.T) {
	type codec interface {
		MarshalWithEncoder(*bin.Encoder) error
		UnmarshalWithDecoder(*bin.Decoder) error
	}
	for _, test := range []struct {
		name          string
		newState      func() codec
		lockoutOffset int
	}{
		{"0.23.5", func() codec { return &VoteState0_23_5{} }, 1905},
		{"1.14.11", func() codec { return &VoteState1_14_11{} }, 65},
		{"current", func() codec { return &VoteState{} }, 65},
		{"v4", func() codec { return &VoteState4{} }, 141},
	} {
		t.Run(test.name, func(t *testing.T) {
			var encoded bytes.Buffer
			require.NoError(t, test.newState().MarshalWithEncoder(bin.NewBinEncoder(&encoded)))
			require.NoError(t, test.newState().UnmarshalWithDecoder(bin.NewBinDecoder(encoded.Bytes())))
			for name, offset := range map[string]int{"lockouts": test.lockoutOffset, "credits": encoded.Len() - 24} {
				t.Run(name, func(t *testing.T) {
					raw := bytes.Clone(encoded.Bytes())
					require.Zero(t, binary.LittleEndian.Uint64(raw[offset:]))
					// A modest impossible count keeps the regression safe on the old decoder.
					binary.LittleEndian.PutUint64(raw[offset:], 65536)
					require.ErrorIs(t, test.newState().UnmarshalWithDecoder(bin.NewBinDecoder(raw)), InstrErrInvalidAccountData)
				})
			}
		})
	}
}
