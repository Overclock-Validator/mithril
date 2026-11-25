//go:build amd64 && !purego

package lthash

import (
	"math/rand"
	"testing"

	"golang.org/x/sys/cpu"
)

func withTreeKernel(t *testing.T, batched bool, f func()) {
	t.Helper()
	previous := useTreeAVX512
	useTreeAVX512 = batched
	defer func() { useTreeAVX512 = previous }()
	f()
}

// The 16-lane kernel must reproduce the portable compression function, and the
// whole tree must match the BLAKE3 library through both code paths on this
// machine.
func TestHashTreeBatchedAndPortablePaths(t *testing.T) {
	if !cpu.X86.HasAVX512F {
		t.Skip("no AVX-512F")
	}
	if !compress16MatchesPortable() {
		t.Fatal("compress16 differs from the portable compression function")
	}
	for _, batched := range []bool{true, false} {
		withTreeKernel(t, batched, func() {
			TestHashTreeMatchesLibrary(t)
			TestHashTreeUpdateMatchesLibrary(t)
			TestHashTreeUpdateOnlyRehashesListedChunks(t)
			TestAccountTreeMatchesAccountHasher(t)
			TestAccountTreeIncrementalUpdateMatchesAccountHasher(t)
		})
	}
}

// Every chunk value and every node must be identical whichever path built or
// updated the tree, not only the final output.
func TestHashTreeBatchedNodesEqualPortableNodes(t *testing.T) {
	if !cpu.X86.HasAVX512F {
		t.Skip("no AVX-512F")
	}
	rng := rand.New(rand.NewSource(21))
	for _, size := range []int{treeChunkLen + 1, 2 * treeChunkLen, 17*treeChunkLen + 5, 33 * treeChunkLen, 1000*treeChunkLen - 3, 8107*treeChunkLen + 900} {
		data := randomBytes(rng, size)
		var batched, portable *hashTree
		withTreeKernel(t, true, func() { batched = buildHashTree(bytesInput(data), 4) })
		withTreeKernel(t, false, func() { portable = buildHashTree(bytesInput(data), 1) })
		compare := func(stage string) {
			for i := range portable.leaves {
				if batched.leaves[i] != portable.leaves[i] {
					t.Fatalf("size %d %s: chunk %d differs", size, stage, i)
				}
			}
			for i := 1; i < len(portable.nodes); i++ {
				if batched.nodes[i] != portable.nodes[i] {
					t.Fatalf("size %d %s: node %d differs", size, stage, i)
				}
			}
		}
		compare("build")
		chunks := bytesInput(data).chunkCount()
		seen := map[int]bool{0: true, chunks - 1: true}
		for len(seen) < min(chunks, 40) {
			seen[rng.Intn(chunks)] = true
		}
		var dirty []int
		for i := 0; i < chunks; i++ {
			if seen[i] {
				dirty = append(dirty, i)
				data[i*treeChunkLen] ^= 0x55
			}
		}
		withTreeKernel(t, true, func() { batched.update(bytesInput(data), dirty) })
		withTreeKernel(t, false, func() { portable.update(bytesInput(data), dirty) })
		compare("update")
	}
}

func BenchmarkHashTreePortable(b *testing.B) {
	previous := useTreeAVX512
	useTreeAVX512 = false
	defer func() { useTreeAVX512 = previous }()
	BenchmarkHashTree(b)
}
