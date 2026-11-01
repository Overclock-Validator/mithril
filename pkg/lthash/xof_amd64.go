//go:build amd64 && !purego

package lthash

//go:generate go run xof_amd64_gen.go

import (
	"bytes"
	"os"
	"reflect"
	"unsafe"

	"github.com/zeebo/blake3"
	"golang.org/x/sys/cpu"
)

// useAVX512XOF computes the 2 KiB account hash output 16 blocks at a time.
// cpu.X86.HasAVX512F includes the operating-system ZMM-state check. It is set
// once, before any hashing: the path is used only if blake3.Digest still has
// the layout digestState mirrors and its output matches blake3.Digest.Read on
// this machine (xofFallbackReason says why it is not). Tests flip it to
// compare against blake3.Digest.Read on the same machine.
// MITHRIL_LTHASH_XOF_AVX512=0 turns it off.
var useAVX512XOF = cpu.X86.HasAVX512F && os.Getenv("MITHRIL_LTHASH_XOF_AVX512") != "0" && avx512XOFUsable()

// xofFallbackReason is why a machine with AVX-512F does not use the AVX-512
// output path despite it being enabled, or empty.
var xofFallbackReason string

// XOFFallbackReason reports why account hashing is not using the AVX-512
// output path on a machine that supports it, or "" when it is in use, is
// turned off, or the CPU lacks AVX-512F.
func XOFFallbackReason() string { return xofFallbackReason }

func avx512XOFUsable() bool {
	switch {
	case !digestLayoutMatches(reflect.TypeFor[blake3.Digest]()):
		xofFallbackReason = "blake3.Digest no longer has the field layout the AVX-512 path reads"
	case !xofMatchesDigestRead(readXOFAVX512):
		xofFallbackReason = "AVX-512 output differs from blake3.Digest.Read"
	default:
		return true
	}
	return false
}

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

// digestLayoutMatches reports whether digest has exactly digestState's fields:
// the same names, types and offsets, in the same order. The size assertions
// above miss a rearrangement of equally sized fields; this does not.
func digestLayoutMatches(digest reflect.Type) bool {
	mirror := reflect.TypeFor[digestState]()
	if digest.Kind() != reflect.Struct || digest.Size() != mirror.Size() || digest.NumField() != mirror.NumField() {
		return false
	}
	for i := range mirror.NumField() {
		want, got := mirror.Field(i), digest.Field(i)
		if got.Name != want.Name || got.Type != want.Type || got.Offset != want.Offset {
			return false
		}
	}
	return true
}

// xofMatchesDigestRead reports whether read produces blake3.Digest.Read's
// output for a single-block, a block-boundary, a chunk-boundary and a
// multi-chunk (parent root) input, each from a fresh digest, and leaves the
// digest where Read would.
func xofMatchesDigestRead(read func(*blake3.Digest, *[2 * numElements]byte)) bool {
	for _, n := range []int{0, 64, 65, 1024, 1025, 4097} {
		in := make([]byte, n)
		for i := range in {
			in[i] = byte(i*7 + n)
		}
		h := blake3.New()
		_, _ = h.Write(in)
		var want [2*numElements + 64]byte
		_, _ = h.Digest().Read(want[:])

		d := h.Digest()
		var got [2 * numElements]byte
		read(d, &got)
		var next [64]byte
		_, _ = d.Read(next[:])
		if !bytes.Equal(got[:], want[:2*numElements]) || !bytes.Equal(next[:], want[2*numElements:]) {
			return false
		}
	}
	return true
}

// readXOF fills out with d's output stream from its current position, exactly
// as d.Read(out[:]) does for a digest that has not been read yet.
func readXOF(d *blake3.Digest, out *[2 * numElements]byte) {
	if !useAVX512XOF {
		_, _ = d.Read(out[:])
		return
	}
	readXOFAVX512(d, out)
}

func readXOFAVX512(d *blake3.Digest, out *[2 * numElements]byte) {
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
