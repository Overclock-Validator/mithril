package sealevel

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSlotHistoryEncodingParity(t *testing.T) {
	const maxWords = slotHistoryMaxEntries / bitsPerBlock
	words := make([]uint64, maxWords+1)
	for i := range words {
		words[i] = uint64(i)<<32 | 0x01234567
	}
	for _, count := range []uint64{0, 1, 65, maxWords, maxWords + 1} {
		history := SysvarSlotHistory{
			Bits: SlotHistoryBitvec{
				Bits: SlotHistoryInner{BlocksLen: count, Blocks: words},
				Len:  count * bitsPerBlock,
			},
			NextSlot: 0x1122334455667788,
		}
		want := history.mustMarshalSlotHistoryBuffer()
		got := history.MustMarshal()
		require.Equal(t, want, got, "block count %d", count)
		got[0] ^= 1
		require.Equal(t, want, history.MustMarshal(), "output must be independently owned")
	}

	var history SysvarSlotHistory
	history.Bits.Bits.BlocksLen = 1
	require.Panics(t, func() { history.MustMarshal() })
}
