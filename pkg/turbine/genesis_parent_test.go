package turbine

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestAssembledGenesisParentPresence(t *testing.T) {
	for _, tc := range []struct {
		name                string
		parentSlot          uint64
		parentID            solana.Hash
		header, wantPresent bool
	}{
		{name: "explicit genesis", header: true, wantPresent: true},
		{name: "missing genesis header"},
		{name: "ordinary zero parent", parentSlot: 1, header: true},
		{name: "ordinary parent", parentSlot: 1, parentID: solana.Hash{7}, header: true, wantPresent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shredder := Shredder{Slot: tc.parentSlot + 1, ParentSlot: tc.parentSlot, Version: 42}
			assembler := NewSlotAssembler()
			var packets [][]byte
			var dataIndex, codeIndex uint32
			var chain solana.Hash
			if tc.header {
				batch, data, code, err := shredder.MakeMerkleShredsFromComponent(testShredLeader(t),
					NewBlockHeader(tc.parentSlot, tc.parentID), false, chain, 0, 0)
				require.NoError(t, err)
				packets, dataIndex, codeIndex, chain = batch.Packets, data, code, batch.ChainedMerkleRoot
			}
			ending, err := NewEntryBatch([]Entry{{Hash: solana.Hash{9}}})
			require.NoError(t, err)
			batch, _, _, err := shredder.MakeMerkleShredsFromComponent(testShredLeader(t), ending, true, chain, dataIndex, codeIndex)
			require.NoError(t, err)
			packets = append(packets, batch.Packets...)
			var got *block.Block
			for _, packet := range packets {
				complete, err := assembler.AddPacket(packet)
				require.NoError(t, err)
				if complete != nil {
					require.Nil(t, got)
					got = complete
				}
			}
			require.NotNil(t, got)
			require.Equal(t, tc.wantPresent, got.HasAlpenglowParentBlockID)
			if tc.wantPresent {
				require.Equal(t, tc.parentID, solana.Hash(got.AlpenglowParentBlockID))
			}
		})
	}
}

func TestGenesisShredVersionNeverZero(t *testing.T) {
	require.Equal(t, uint16(1), ShredVersionFromGenesisHash(solana.Hash{}))
	require.Equal(t, uint16(65535), ShredVersionFromGenesisHash(solana.Hash{255, 255}))
}
