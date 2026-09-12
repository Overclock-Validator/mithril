package turbine

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Some cluster producers include the unused FEC capacity in shred.Data.
// Agave parses one component per DATA_COMPLETE range and ignores its suffix.
func TestDataShredDecodersAcceptComponentPadding(t *testing.T) {
	for _, suffix := range []byte{0, 0xa5} {
		name := "zero"
		if suffix != 0 {
			name = "nonzero"
		}
		t.Run(name, func(t *testing.T) {
			optimistic, err := NewEntryBatch([]Entry{{NumHashes: 1, Hash: solana.Hash{1}}})
			require.NoError(t, err)
			selected, err := NewEntryBatch([]Entry{{NumHashes: 1, Hash: solana.Hash{2}, Txns: []solana.Transaction{mustParseTransferTx(t, 21)}}})
			require.NoError(t, err)
			tick, err := NewEntryBatch([]Entry{{NumHashes: 1, Hash: solana.Hash{3}}})
			require.NoError(t, err)
			footer := BlockFooter{BankHash: solana.Hash{4}, BlockProducerTimeNanos: 42, BlockUserAgent: []byte("padding-test")}
			components := []BlockComponent{
				NewBlockHeader(99, solana.Hash{9}),
				optimistic,
				NewUpdateParent(98, solana.Hash{8}),
				selected,
				NewBlockFooter(footer),
				tick,
			}
			var shreds []*Shred
			for _, component := range components {
				raw, err := MarshalBlockComponent(component)
				require.NoError(t, err)
				shreds = append(shreds, paddedComponentShreds(t, raw, uint32(len(shreds)), suffix)...)
			}
			shreds[len(shreds)-1].Flags = shredFlagLastShredInSlot

			decoded, err := DecodeComponentsFromDataShreds(shreds)
			require.NoError(t, err)
			require.Len(t, decoded, len(components))
			for i, component := range decoded {
				want, err := MarshalBlockComponent(components[i])
				require.NoError(t, err)
				got, err := MarshalBlockComponent(component)
				require.NoError(t, err)
				require.Equal(t, want, got)
			}

			entries, parent, gotFooter, err := DecodeEntriesAndAlpenglowMarkersFromDataShreds(shreds)
			require.NoError(t, err)
			require.Equal(t, &AlpenglowParentInfo{
				ParentSlot: 98, ParentBlockID: solana.Hash{8},
				ReplayFECSetIndex: 64, FromUpdateParent: true,
			}, parent)
			require.Equal(t, footer.BankHash, gotFooter.BankHash)
			require.Equal(t, footer.BlockProducerTimeNanos, gotFooter.BlockProducerTimeNanos)
			require.Equal(t, footer.BlockUserAgent, gotFooter.BlockUserAgent)
			// Padding must not change the UpdateParent replay boundary or add entries.
			require.Len(t, entries, 2)
			require.Equal(t, solana.Hash{2}, entries[0].Hash)
			require.Equal(t, solana.Hash{3}, entries[1].Hash)
			wantTx, err := selected.EntryBatch[0].Txns[0].MarshalBinary()
			require.NoError(t, err)
			require.Len(t, entries[0].Txns, 1)
			gotTx, err := entries[0].Txns[0].MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, wantTx, gotTx)
		})
	}
}

func TestDataShredDecodersRejectMalformedPaddedMarkers(t *testing.T) {
	for _, test := range []struct {
		name   string
		length uint16
	}{
		{name: "short_inner", length: 40},
		{name: "extra_inner", length: 42},
		{name: "truncated_envelope", length: 65535},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, err := MarshalBlockComponent(NewBlockHeader(99, solana.Hash{9}))
			require.NoError(t, err)
			binary.LittleEndian.PutUint16(raw[11:13], test.length)
			shreds := paddedComponentShreds(t, raw, 0, 0)
			_, err = DecodeComponentsFromDataShreds(shreds)
			require.Error(t, err)
			_, _, _, err = DecodeEntriesAndAlpenglowMarkersFromDataShreds(shreds)
			require.Error(t, err)
		})
	}
}

func TestDataShredReplayRejectsUnrecognizedPaddedMarkers(t *testing.T) {
	for _, test := range []struct {
		name   string
		start  uint32
		mutate func([]byte)
	}{
		{name: "unknown_version", mutate: func(raw []byte) { raw[8] = 2 }},
		{name: "unknown_kind", mutate: func(raw []byte) { raw[10] = 255 }},
		{name: "misplaced_header", start: 32},
		{name: "misplaced_update", mutate: func(raw []byte) { raw[10] = blockMarkerVariantUpdateParent }},
		{name: "padded_abort", mutate: func(raw []byte) { clear(raw) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, err := MarshalBlockComponent(NewBlockHeader(99, solana.Hash{9}))
			require.NoError(t, err)
			if test.mutate != nil {
				test.mutate(raw)
			}
			shreds := paddedComponentShreds(t, raw, test.start, 0)
			_, _, _, err = DecodeEntriesAndAlpenglowMarkersFromDataShreds(shreds)
			require.Error(t, err, "an unrecognized marker must not become an empty entry batch")
		})
	}
}

func paddedComponentShreds(t *testing.T, raw []byte, start uint32, suffix byte) []*Shred {
	t.Helper()
	const capacity = 963
	require.Less(t, len(raw), dataShredsPerFECBlock*capacity)
	padded := bytes.Repeat([]byte{suffix}, dataShredsPerFECBlock*capacity)
	copy(padded, raw)
	shreds := make([]*Shred, dataShredsPerFECBlock)
	for i := range shreds {
		shreds[i] = &Shred{
			Type: ShredTypeData, Slot: 100, Index: start + uint32(i),
			FECSetIndex: start, ParentOffset: 1,
			Data: padded[i*capacity : (i+1)*capacity],
		}
	}
	shreds[len(shreds)-1].Flags = shredFlagDataComplete
	return shreds
}
