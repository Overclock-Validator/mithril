package merkletree

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"
)

// Independent construction: retain separate levels and use one-shot SHA256
// over explicit domain-prefixed bytes, without the production hash helpers.
func referenceRoot(leaves [][]byte) [32]byte {
	if len(leaves) == 0 {
		return [32]byte{}
	}
	level := make([][32]byte, len(leaves))
	for i, leaf := range leaves {
		level[i] = sha256.Sum256(append([]byte{0}, leaf...))
	}
	for len(level) > 1 {
		next := make([][32]byte, (len(level)+1)/2)
		for i := range next {
			left, right := level[2*i], level[min(2*i+1, len(level)-1)]
			data := append([]byte{1}, left[:]...)
			data = append(data, right[:]...)
			next[i] = sha256.Sum256(data)
		}
		level = next
	}
	return level[0]
}

func TestHashRootMatchesCanonicalTree(t *testing.T) {
	counts := []int{0, 1, 2, 3, 7, 8, 9, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257, 311, 511, 512, 513, 1023, 1024, 1025}
	for _, size := range []int{0, 31, 32, 55, 56, 63, 64, 65, 1232} {
		for _, count := range counts {
			t.Run(fmt.Sprintf("%dx%d", count, size), func(t *testing.T) {
				leaves := make([][]byte, count)
				for i := range leaves {
					leaves[i] = make([]byte, size)
					for j := range leaves[i] {
						leaves[i][j] = byte(i*71 + i/256 + j*17)
					}
				}
				before := bytes.Join(leaves, nil)
				want := referenceRoot(leaves)
				if got := HashRoot(leaves); got != want {
					t.Fatalf("root %x, want %x", got, want)
				}
				nodes := HashNodes(leaves)
				if got := nodes.GetRoot(); got != nil && *got != want {
					t.Fatalf("proof root %x, want %x", *got, want)
				}
				if !bytes.Equal(before, bytes.Join(leaves, nil)) {
					t.Fatal("hashing modified input leaves")
				}
			})
		}
	}
}
