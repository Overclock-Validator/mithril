package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/require"
)

func TestEncodeHashCachedMatchesBase58AcrossGenerations(t *testing.T) {
	// More than two generations of distinct hashes, then the early ones again:
	// every encoding must equal a fresh one whether cached or evicted.
	for round := 0; round < 2; round++ {
		for i := 0; i < 3*hashEncodeCacheGeneration; i++ {
			var h [32]byte
			h[0], h[1], h[2] = byte(i), byte(i>>8), byte(round)
			require.Equal(t, base58.Encode(h[:]), encodeHashCached(h))
		}
	}
	require.LessOrEqual(t, len(hashEncodeCache.cur), hashEncodeCacheGeneration)
}

func TestEncodeSysvarsUnchangedByCache(t *testing.T) {
	var recent sealevel.SysvarRecentBlockhashes
	var slots sealevel.SysvarSlotHashes
	for i := 0; i < 150; i++ {
		var e sealevel.RecentBlockHashesEntry
		e.Blockhash[0], e.Blockhash[1] = byte(i), 7
		e.FeeCalculator.LamportsPerSignature = uint64(i)
		recent = append(recent, e)
	}
	for i := 0; i < 512; i++ {
		var e sealevel.SlotHash
		e.Slot = uint64(1000 + i)
		e.Hash[0], e.Hash[1] = byte(i), byte(i>>8)
		slots = append(slots, e)
	}
	for pass := 0; pass < 2; pass++ { // cold, then cached
		gotRecent := EncodeRecentBlockhashes(&recent)
		require.Len(t, gotRecent, 150)
		for i, e := range recent {
			require.Equal(t, base58.Encode(e.Blockhash[:]), gotRecent[i].Blockhash)
			require.Equal(t, e.FeeCalculator.LamportsPerSignature, gotRecent[i].LamportsPerSignature)
		}
		gotSlots := EncodeSlotHashes(&slots)
		require.Len(t, gotSlots, 512)
		for i, e := range slots {
			require.Equal(t, base58.Encode(e.Hash[:]), gotSlots[i].Hash)
			require.Equal(t, e.Slot, gotSlots[i].Slot)
		}
	}
}

func BenchmarkEncodeResumeContextHashes(b *testing.B) {
	var recent sealevel.SysvarRecentBlockhashes
	var slots sealevel.SysvarSlotHashes
	for i := 0; i < 150; i++ {
		var e sealevel.RecentBlockHashesEntry
		e.Blockhash[0], e.Blockhash[1] = byte(i), 9
		recent = append(recent, e)
	}
	for i := 0; i < 512; i++ {
		var e sealevel.SlotHash
		e.Hash[0], e.Hash[1] = byte(i), byte(i>>8)
		slots = append(slots, e)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EncodeRecentBlockhashes(&recent)
		_ = EncodeSlotHashes(&slots)
	}
}

// BenchmarkEncodeResumeContextHashesUncached is the encoding before the cache.
func BenchmarkEncodeResumeContextHashesUncached(b *testing.B) {
	var hashes [][32]byte
	for i := 0; i < 662; i++ {
		var h [32]byte
		h[0], h[1], h[2] = byte(i), byte(i>>8), 9
		hashes = append(hashes, h)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, h := range hashes {
			_ = base58.Encode(h[:])
		}
	}
}
