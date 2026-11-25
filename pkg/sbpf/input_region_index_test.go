package sbpf

import (
	"math/rand"
	"slices"
	"testing"
)

// referenceInputRegionIndex is the lookup inputRegionIndex replaced.
func referenceInputRegionIndex(regions []InputRegion, offset uint64) int {
	idx, found := slices.BinarySearchFunc(regions, offset, func(region InputRegion, target uint64) int {
		if target < region.Offset {
			return 1
		}
		if target >= region.Offset+region.AddressSpaceReserved {
			return -1
		}
		return 0
	})
	if !found {
		return -1
	}
	return idx
}

// randomInputRegions builds sorted, non-overlapping regions with gaps between
// some of them and an occasional region that reserves nothing.
func randomInputRegions(rng *rand.Rand, n int) []InputRegion {
	regions := make([]InputRegion, 0, n)
	var at uint64
	for range n {
		at += uint64(rng.Intn(3)) * uint64(rng.Intn(64))
		reserved := uint64(1 + rng.Intn(20000))
		if rng.Intn(12) == 0 {
			reserved = 0
		}
		regions = append(regions, InputRegion{Offset: at, AddressSpaceReserved: reserved, RegionSize: reserved})
		at += reserved
	}
	return regions
}

func TestInputRegionIndexMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, n := range []int{0, 1, 2, 3, 7, 64, 200} {
		for range 50 {
			ip := &Interpreter{inputRegions: randomInputRegions(rng, n)}
			var offsets []uint64
			for _, region := range ip.inputRegions {
				end := region.Offset + region.AddressSpaceReserved
				offsets = append(offsets, region.Offset, region.Offset+1, end, end+1, region.Offset+region.AddressSpaceReserved/2)
				if region.Offset > 0 {
					offsets = append(offsets, region.Offset-1)
				}
				if end > 0 {
					offsets = append(offsets, end-1)
				}
			}
			limit := uint64(1)
			if n > 0 {
				last := ip.inputRegions[n-1]
				limit = last.Offset + last.AddressSpaceReserved + 100
			}
			for range 400 {
				offsets = append(offsets, uint64(rng.Int63n(int64(limit))))
			}
			offsets = append(offsets, 0, limit, ^uint64(0))
			// Shuffled, and each offset twice in a row, so the remembered
			// index is right, wrong and stale in turn.
			rng.Shuffle(len(offsets), func(i, j int) { offsets[i], offsets[j] = offsets[j], offsets[i] })
			for _, offset := range offsets {
				want := referenceInputRegionIndex(ip.inputRegions, offset)
				for range 2 {
					if got := ip.inputRegionIndex(offset); got != want {
						t.Fatalf("%d regions, offset %d: index %d, reference %d", n, offset, got, want)
					}
				}
			}
			// A remembered index beyond a shorter region list is ignored.
			ip.inputRegionHint = n + 5
			if got, want := ip.inputRegionIndex(0), referenceInputRegionIndex(ip.inputRegions, 0); got != want {
				t.Fatalf("%d regions, stale index: %d, reference %d", n, got, want)
			}
		}
	}
}

func BenchmarkInputRegionIndex(b *testing.B) {
	rng := rand.New(rand.NewSource(2))
	regions := randomInputRegions(rng, 48)
	last := regions[len(regions)-1]
	offsets := make([]uint64, 4096)
	for i := range offsets {
		offsets[i] = uint64(rng.Int63n(int64(last.Offset + last.AddressSpaceReserved)))
	}
	b.Run("reference", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			referenceInputRegionIndex(regions, offsets[i%len(offsets)])
		}
	})
	b.Run("lookup", func(b *testing.B) {
		ip := &Interpreter{inputRegions: regions}
		for i := 0; b.Loop(); i++ {
			ip.inputRegionIndex(offsets[i%len(offsets)])
		}
	})
}
