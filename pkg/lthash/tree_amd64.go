//go:build amd64 && !purego

package lthash

import (
	"os"

	"golang.org/x/sys/cpu"
)

//go:generate go run tree_amd64_gen.go

//go:noescape
func compress16AVX512(cv *[8][16]uint32, msg *byte, ctr *[32]uint32, blen, flags uint32)

// useTreeAVX512 hashes tree chunks and nodes 16 at a time. It is set once,
// before any hashing, and only if the kernel reproduces the portable
// compression function on this machine. Tests flip it to run both paths.
// MITHRIL_LTHASH_TREE_AVX512=0 turns it off.
var useTreeAVX512 = cpu.X86.HasAVX512F && os.Getenv("MITHRIL_LTHASH_TREE_AVX512") != "0" && compress16MatchesPortable()

func compress16(cv *[8][16]uint32, msg *byte, ctr *[32]uint32, blen, flags uint32) {
	compress16AVX512(cv, msg, ctr, blen, flags)
}

// treeRootOutput writes a root parent node's 2 KiB extendable output with the
// 16-lane output kernel, when that kernel is in use.
func treeRootOutput(m *[16]uint32, dst []byte) bool {
	if !useAVX512XOF || len(dst) != 2*numElements {
		return false
	}
	var ctr [32]uint32
	for half := 0; half < 2; half++ {
		for i := 0; i < 16; i++ {
			ctr[i] = uint32(16*half + i)
		}
		xof16AVX512((*[8]uint32)(&blake3IV), m, &ctr, treeBlockLen, flagParent|flagRoot, (*[1024]byte)(dst[1024*half:]))
	}
	return true
}
