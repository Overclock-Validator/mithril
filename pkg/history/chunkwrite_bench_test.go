package history

import (
	"math/rand"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
)

// BenchmarkChunkWrite writes one fold's chunk of 20,000 accounts, at each
// block size.
func BenchmarkChunkWrite(b *testing.B) {
	rng := rand.New(rand.NewSource(8))
	recs := make([]*Record, 20000)
	for i := range recs {
		r := &Record{}
		rng.Read(r.Key[:])
		v := randomVersion(rng)
		if i%3 == 0 {
			r.Full = v
		}
		n := mutate(rng, v)
		r.Diffs = []Diff{{Slot: 10 + uint64(rng.Intn(100)), Bytes: AppendXor(nil, v, n)}}
		recs[i] = r
	}
	sort.Slice(recs, func(i, j int) bool { return lessKey(recs[i].Key, recs[j].Key) })
	for _, size := range []int{64 << 10, 16 << 10} {
		b.Run(strconv.Itoa(size>>10)+"KiB", func(b *testing.B) {
			old := targetBlockSize
			targetBlockSize = size
			defer func() { targetBlockSize = old }()
			dir := b.TempDir()
			for i := 0; i < b.N; i++ {
				if _, err := writeChunk(filepath.Join(dir, "c.hst"), 0, 127, recs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
