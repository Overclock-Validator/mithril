package turbine

import (
	"cmp"
	"slices"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// generatedAlpenglowSlotPackets returns every packet (data and coding) of a
// valid Alpenglow slot 100, and its data shred payloads by index.
func generatedAlpenglowSlotPackets(t *testing.T) ([][]byte, [][]byte) {
	t.Helper()
	leader := testShredLeader(t)
	gen := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7, ReferenceTick: 63}
	chainedRoot := solana.Hash{5}
	var nextData, nextCode uint32
	var all, data [][]byte
	for _, component := range buildAlpenglowSlot(t) {
		packets, root, newData, newCode, err := gen.MakeShredsFromData(
			leader, component.payload, component.isLastInSlot, chainedRoot, nextData, nextCode,
		)
		require.NoError(t, err)
		chainedRoot, nextData, nextCode = root, newData, newCode
		for _, packet := range packets {
			shred, err := ParseShred(packet)
			require.NoError(t, err)
			all = append(all, packet)
			if shred.Type == ShredTypeData {
				require.Equal(t, len(data), int(shred.Index))
				data = append(data, shred.Payload)
			}
		}
	}
	return all, data
}

func TestRecoverSlotDataShredsRebuildsMissingDataShreds(t *testing.T) {
	all, want := generatedAlpenglowSlotPackets(t)

	got, err := RecoverSlotDataShreds(100, all)
	require.NoError(t, err)
	require.Equal(t, want, got, "a complete slot comes back as its data shreds in index order")

	// Drop the first data shred of every FEC set; coding shreds cover it.
	var partial [][]byte
	dropped := 0
	for _, packet := range all {
		shred, err := ParseShred(packet)
		require.NoError(t, err)
		if shred.Type == ShredTypeData && shred.Index == shred.FECSetIndex {
			dropped++
			continue
		}
		partial = append(partial, packet, packet) // duplicates are harmless
	}
	// Coding shreds first: enough shards for recovery arrive before most data
	// shreds, which must still be returned as received.
	slices.SortStableFunc(partial, func(x, y []byte) int {
		sx, _ := ParseShred(x)
		sy, _ := ParseShred(y)
		return cmp.Compare(sy.Type, sx.Type)
	})
	require.Positive(t, dropped)
	got, err = RecoverSlotDataShreds(100, partial)
	require.NoError(t, err)
	require.Equal(t, want, got, "recovered shreds are byte-identical, Merkle proofs included")

	// With every coding shred gone, nothing can be recovered.
	var dataOnly [][]byte
	for _, packet := range partial {
		if shred, _ := ParseShred(packet); shred.Type == ShredTypeData {
			dataOnly = append(dataOnly, packet)
		}
	}
	_, err = RecoverSlotDataShreds(100, dataOnly)
	require.Error(t, err)

	_, err = RecoverSlotDataShreds(101, all)
	require.Error(t, err, "packets of another slot are ignored")
}
