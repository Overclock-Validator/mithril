package turbine

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestShredIndexBitsBoundaries(t *testing.T) {
	var b shredIndexBits
	indexes := []uint32{0, 1, 63, 64, 65, 4095, 4096, 4097, 65534, 65535}
	for _, i := range indexes {
		b.set(i)
	}
	for i := uint32(0); i <= 65536; i++ {
		var wantNext, wantPrev uint32
		var hasNext, hasPrev bool
		for _, j := range indexes {
			if j >= i && !hasNext {
				wantNext, hasNext = j, true
			}
			if j <= i {
				wantPrev, hasPrev = j, true
			}
		}
		got, ok := b.next(i)
		require.Equal(t, hasNext, ok)
		if ok {
			require.Equal(t, wantNext, got)
		}
		got, ok = b.previous(i)
		require.Equal(t, hasPrev, ok)
		if ok {
			require.Equal(t, wantPrev, got)
		}
	}
	for _, i := range indexes {
		b.clear(i)
	}
	_, ok := b.next(0)
	require.False(t, ok)
	_, ok = b.previous(65535)
	require.False(t, ok)
	require.Zero(t, b.top)
}

func TestEntryBatchIndexRandomArrivalMatchesOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 100; trial++ {
		const size = 257
		var ends, present [size]bool
		for i := range ends {
			ends[i] = rng.Intn(8) == 0
		}
		ends[size-1] = true
		index := newEntryBatchIndex()
		emitted := map[shredBatchRange]bool{}
		for _, i := range rng.Perm(size) {
			present[i] = true
			got, n := index.add(uint32(i), ends[i])
			want := map[shredBatchRange]bool{}
			start, complete := 0, true
			for j := 0; j < size; j++ {
				complete = complete && present[j]
				if ends[j] && present[j] {
					r := shredBatchRange{uint32(start), uint32(j)}
					if complete && !emitted[r] {
						want[r] = true
					}
					start, complete = j+1, true
				}
			}
			require.Len(t, want, n, "trial %d index %d", trial, i)
			for _, r := range got[:n] {
				require.True(t, want[r])
				require.False(t, emitted[r])
				emitted[r] = true
			}
			_, n = index.add(uint32(i), ends[i])
			require.Zero(t, n, "duplicate emitted")
		}
	}
}

func BenchmarkEntryBatchIndex(b *testing.B) {
	for _, reverse := range []bool{false, true} {
		name := "ordered"
		if reverse {
			name = "reverse"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				idx := newEntryBatchIndex()
				for k := uint32(0); k < 65536; k++ {
					i := k
					if reverse {
						i = 65535 - k
					}
					idx.add(i, i%64 == 63)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/65536, "ns/shred")
		})
	}
}
