//go:build amd64 && !purego

package lthash

//go:generate go run xof_amd64_gen.go

import (
	"os"
	"unsafe"

	"github.com/zeebo/blake3"
	"golang.org/x/sys/cpu"
)

// useAVX512XOF computes the 2 KiB account hash output 16 blocks at a time.
// cpu.X86.HasAVX512F includes the operating-system ZMM-state check. Tests flip
// it to compare against blake3.Digest.Read on the same machine.
// MITHRIL_LTHASH_XOF_AVX512=0 turns it off.
var useAVX512XOF = cpu.X86.HasAVX512F && os.Getenv("MITHRIL_LTHASH_XOF_AVX512") != "0"

// digestState mirrors blake3.Digest (github.com/zeebo/blake3 v0.2.3), whose
// fields are unexported: the root node's chaining value, block, length and
// flags, and the counter of the next output block.
type digestState struct {
	counter uint64
	chain   [8]uint32
	block   [16]uint32
	blen    uint32
	flags   uint32
	buf     [16]uint32
	bufn    int
}

// Fail to compile if blake3.Digest's size changes.
var (
	_ [unsafe.Sizeof(blake3.Digest{}) - unsafe.Sizeof(digestState{})]struct{}
	_ [unsafe.Sizeof(digestState{}) - unsafe.Sizeof(blake3.Digest{})]struct{}
)

// readXOF fills out with d's output stream from its current position, exactly
// as d.Read(out[:]) does for a digest that has not been read yet.
func readXOF(d *blake3.Digest, out *[2 * numElements]byte) {
	if !useAVX512XOF {
		_, _ = d.Read(out[:])
		return
	}
	s := (*digestState)(unsafe.Pointer(d))
	if s.bufn != 0 {
		_, _ = d.Read(out[:])
		return
	}
	var ctr [32]uint32
	for half := uint64(0); half < 2; half++ {
		for i := uint64(0); i < 16; i++ {
			c := s.counter + 16*half + i
			ctr[i], ctr[16+i] = uint32(c), uint32(c>>32)
		}
		xof16AVX512(&s.chain, &s.block, &ctr, s.blen, s.flags, (*[1024]byte)(out[1024*half:]))
	}
	s.counter += 32
}

//go:noescape
func xof16AVX512(chain *[8]uint32, block *[16]uint32, ctr *[32]uint32, blen, flags uint32, out *[1024]byte)
