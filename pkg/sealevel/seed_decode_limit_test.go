package sealevel

import (
	"bytes"
	"encoding/binary"
	"testing"

	bin "github.com/gagliardetto/binary"
	"github.com/stretchr/testify/require"
)

func TestSeedAuthorizationDeserializationLimits(t *testing.T) {
	for _, tt := range []struct {
		name      string
		kind      uint32
		headerLen int
		build     func([]byte) []byte
		decode    func(*bin.Decoder) error
	}{
		{
			name: "vote", kind: VoteProgramInstrTypeAuthorizeWithSeed, headerLen: 80,
			build: func(seed []byte) []byte {
				data := make([]byte, 4+32) // authorization kind and seed owner
				data = binary.LittleEndian.AppendUint64(data, uint64(len(seed)))
				data = append(data, seed...)
				return append(data, make([]byte, 32)...) // new authority
			},
			decode: func(d *bin.Decoder) error { return new(VoteInstrAuthorizeWithSeed).UnmarshalWithDecoder(d) },
		},
		{
			name: "vote checked", kind: VoteProgramInstrTypeAuthorizeCheckedWithSeed, headerLen: 48,
			build: func(seed []byte) []byte {
				data := make([]byte, 4+32)
				data = binary.LittleEndian.AppendUint64(data, uint64(len(seed)))
				return append(data, seed...)
			},
			decode: func(d *bin.Decoder) error { return new(VoteInstrAuthorizeCheckedWithSeed).UnmarshalWithDecoder(d) },
		},
		{
			name: "stake", kind: StakeProgramInstrTypeAuthorizeWithSeed, headerLen: 80,
			build: func(seed []byte) []byte {
				data := make([]byte, 32+4) // new authority and authorization kind
				data = binary.LittleEndian.AppendUint64(data, uint64(len(seed)))
				data = append(data, seed...)
				return append(data, make([]byte, 32)...) // seed owner
			},
			decode: func(d *bin.Decoder) error { return new(StakeInstrAuthorizeWithSeed).UnmarshalWithDecoder(d) },
		},
		{
			name: "stake checked", kind: StakeProgramInstrTypeAuthorizeCheckedWithSeed, headerLen: 48,
			build: func(seed []byte) []byte {
				data := make([]byte, 4)
				data = binary.LittleEndian.AppendUint64(data, uint64(len(seed)))
				data = append(data, seed...)
				return append(data, make([]byte, 32)...)
			},
			decode: func(d *bin.Decoder) error { return new(StakeInstrAuthorizeCheckedWithSeed).UnmarshalWithDecoder(d) },
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				seedLen  int
				trailing int
				wantErr  bool
			}{
				{name: "valid seed", seedLen: 32},
				{name: "exact consumed limit", seedLen: 1232 - tt.headerLen},
				{name: "over consumed limit", seedLen: 1233 - tt.headerLen, wantErr: true},
				{name: "ignored trailing bytes", seedLen: 32, trailing: 4096},
				{name: "exact limit with ignored trailing bytes", seedLen: 1232 - tt.headerLen, trailing: 4096},
			} {
				t.Run(tc.name, func(t *testing.T) {
					raw := binary.LittleEndian.AppendUint32(nil, tt.kind)
					raw = append(raw, tt.build(bytes.Repeat([]byte{'a'}, tc.seedLen))...)
					raw = append(raw, make([]byte, tc.trailing)...)
					d := bin.NewBinDecoder(raw)
					_, err := d.ReadUint32(bin.LE)
					require.NoError(t, err)
					err = tt.decode(d)
					if tc.wantErr {
						require.ErrorIs(t, err, InstrErrInvalidInstructionData)
					} else {
						require.NoError(t, err)
						require.Equal(t, uint(tt.headerLen+tc.seedLen), d.Position())
					}
				})
			}
			for _, seed := range [][]byte{{0xFF}, []byte("valid")} {
				raw := binary.LittleEndian.AppendUint32(nil, tt.kind)
				raw = append(raw, tt.build(seed)...)
				if string(seed) == "valid" {
					raw = raw[:len(raw)-1] // truncated owner/new authority or seed
				}
				d := bin.NewBinDecoder(raw)
				_, err := d.ReadUint32(bin.LE)
				require.NoError(t, err)
				require.Error(t, tt.decode(d), "malformed strings must still be rejected")
			}
		})
	}
}
