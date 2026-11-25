package lthash

import (
	"bytes"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/zeebo/blake3"
)

// bytesInput is a hash tree input held in one slice.
type bytesInput []byte

func (in bytesInput) chunkCount() int { return (len(in) + treeChunkLen - 1) / treeChunkLen }

func (in bytesInput) chunk(index int, _ *[treeChunkLen]byte) []byte {
	return in[index*treeChunkLen : min((index+1)*treeChunkLen, len(in))]
}

func (in bytesInput) fullChunkRun(index int) (*byte, int) {
	if (index+1)*treeChunkLen > len(in) {
		return nil, 0
	}
	return &in[index*treeChunkLen], (len(in) - index*treeChunkLen) / treeChunkLen
}

func libraryXOF(data []byte, n int) []byte {
	h := blake3.New()
	_, _ = h.Write(data)
	out := make([]byte, n)
	_, _ = h.Digest().Read(out)
	return out
}

func treeXOF(t *hashTree, n int) []byte {
	out := make([]byte, n)
	t.output(out)
	return out
}

func randomBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	rng.Read(b)
	return b
}

// The tree's output must equal the BLAKE3 library's for every input shape:
// each side of every chunk boundary, chunk counts on each side of every power
// of two, and partial final blocks.
func TestHashTreeMatchesLibrary(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var sizes []int
	for chunks := 1; chunks <= 40; chunks++ {
		for _, delta := range []int{-65, -64, -63, -1, 0, 1, 63, 64, 65} {
			sizes = append(sizes, chunks*treeChunkLen+delta)
		}
	}
	for _, chunks := range []int{63, 64, 65, 127, 128, 129, 255, 256, 257, 1000, 1023, 1024, 1025, 4097, 8107} {
		sizes = append(sizes, chunks*treeChunkLen, chunks*treeChunkLen-1, chunks*treeChunkLen+1, chunks*treeChunkLen-511)
	}
	for i := 0; i < 200; i++ {
		sizes = append(sizes, treeChunkLen+1+rng.Intn(300_000))
	}
	for _, size := range sizes {
		if size <= treeChunkLen {
			continue
		}
		data := randomBytes(rng, size)
		for _, workers := range []int{1, 4} {
			tree := buildHashTree(bytesInput(data), workers)
			for _, n := range []int{32, 64, 100, 2048} {
				if got, want := treeXOF(tree, n), libraryXOF(data, n); !bytes.Equal(got, want) {
					t.Fatalf("size %d workers %d output %d bytes: tree differs from library", size, workers, n)
				}
			}
		}
	}
}

// After any set of chunks changes, updating only those chunks must give the
// library's hash of the new input, and must leave no stale node behind.
func TestHashTreeUpdateMatchesLibrary(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for trial := 0; trial < 400; trial++ {
		chunks := 2 + rng.Intn(300)
		size := (chunks-1)*treeChunkLen + 1 + rng.Intn(treeChunkLen)
		data := randomBytes(rng, size)
		tree := buildHashTree(bytesInput(data), 1)
		for round := 0; round < 4; round++ {
			seen := map[int]bool{}
			count := 1 + rng.Intn(1+min(chunks, []int{1, 3, 20, chunks}[rng.Intn(4)]))
			for len(seen) < min(count, chunks) {
				seen[rng.Intn(chunks)] = true
			}
			if rng.Intn(3) == 0 {
				seen[0], seen[chunks-1] = true, true
			}
			var dirty []int
			for i := range seen {
				dirty = append(dirty, i)
				lo, hi := i*treeChunkLen, min((i+1)*treeChunkLen, size)
				data[lo+rng.Intn(hi-lo)] ^= byte(1 + rng.Intn(255))
			}
			sort.Ints(dirty)
			tree.update(bytesInput(data), dirty)
			if got, want := treeXOF(tree, 2048), libraryXOF(data, 2048); !bytes.Equal(got, want) {
				t.Fatalf("trial %d round %d: %d chunks, %d dirty: updated tree differs from library", trial, round, chunks, len(dirty))
			}
			fresh := buildHashTree(bytesInput(data), 1)
			if !bytes.Equal(treeXOF(fresh, 64), treeXOF(tree, 64)) || len(fresh.nodes) != len(tree.nodes) {
				t.Fatalf("trial %d: updated tree differs from a rebuilt one", trial)
			}
			for i := 1; i < len(fresh.nodes); i++ {
				if fresh.nodes[i] != tree.nodes[i] {
					t.Fatalf("trial %d: node %d is stale after update", trial, i)
				}
			}
		}
	}
}

// Skipping a changed chunk must change the result: the test above would pass
// trivially if update rehashed everything.
func TestHashTreeUpdateOnlyRehashesListedChunks(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	data := randomBytes(rng, 50*treeChunkLen+17)
	tree := buildHashTree(bytesInput(data), 1)
	data[7*treeChunkLen+5] ^= 1
	data[31*treeChunkLen+9] ^= 1
	tree.update(bytesInput(data), []int{7})
	if bytes.Equal(treeXOF(tree, 64), libraryXOF(data, 64)) {
		t.Fatal("update rehashed a chunk it was not told about")
	}
	tree.update(bytesInput(data), []int{31})
	if !bytes.Equal(treeXOF(tree, 64), libraryXOF(data, 64)) {
		t.Fatal("tree differs from library after both chunks were updated")
	}
}

func FuzzHashTreeMatchesLibrary(f *testing.F) {
	f.Add([]byte("seed"), uint16(1500), uint16(3), uint8(1))
	f.Fuzz(func(t *testing.T, seed []byte, size uint16, touch uint16, flips uint8) {
		n := treeChunkLen + 1 + int(size)*8
		data := make([]byte, n)
		for i := range data {
			data[i] = byte(i * 131)
			if len(seed) > 0 {
				data[i] ^= seed[i%len(seed)]
			}
		}
		tree := buildHashTree(bytesInput(data), 2)
		if !bytes.Equal(treeXOF(tree, 2048), libraryXOF(data, 2048)) {
			t.Fatalf("size %d: tree differs from library", n)
		}
		chunks := bytesInput(data).chunkCount()
		seen := map[int]bool{}
		for i := 0; i <= int(flips%16); i++ {
			c := (int(touch) + i*7919) % chunks
			seen[c] = true
			data[c*treeChunkLen] ^= byte(i + 1)
		}
		var dirty []int
		for c := range seen {
			dirty = append(dirty, c)
		}
		sort.Ints(dirty)
		tree.update(bytesInput(data), dirty)
		if !bytes.Equal(treeXOF(tree, 2048), libraryXOF(data, 2048)) {
			t.Fatalf("size %d: updated tree differs from library", n)
		}
	})
}

func BenchmarkHashTree(b *testing.B) {
	rng := rand.New(rand.NewSource(4))
	data := randomBytes(rng, 8_300_000)
	b.Run("library full 8.3MB", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		for i := 0; i < b.N; i++ {
			libraryXOF(data, 2048)
		}
	})
	b.Run("tree build 1 worker", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		for i := 0; i < b.N; i++ {
			buildHashTree(bytesInput(data), 1)
		}
	})
	b.Run("tree build 8 workers", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		for i := 0; i < b.N; i++ {
			buildHashTree(bytesInput(data), 8)
		}
	})
	tree := buildHashTree(bytesInput(data), 8)
	out := make([]byte, 2048)
	for _, dirtyCount := range []int{1, 10, 100, 1000} {
		dirty := make([]int, dirtyCount)
		for i := range dirty {
			dirty[i] = i * (len(tree.leaves) / dirtyCount)
		}
		b.Run("update dirty="+strconv.Itoa(dirtyCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				tree.update(bytesInput(data), dirty)
				tree.output(out)
			}
		})
	}
}
