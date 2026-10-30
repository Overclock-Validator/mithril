//go:build amd64 && !purego

package lthash

import (
	"bytes"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/zeebo/blake3"
)

func requireAVX512XOF(t testing.TB) {
	if !useAVX512XOF {
		t.Skip("no AVX-512F on this machine")
	}
}

// The 16-lane output must equal blake3.Digest.Read for every input length,
// including single-block, single-chunk and multi-chunk (parent root) inputs,
// and the digest must continue exactly where Read would have left it.
func TestXOFAVX512MatchesDigestRead(t *testing.T) {
	requireAVX512XOF(t)
	rng := rand.New(rand.NewSource(7))
	lengths := []int{0, 1, 63, 64, 65, 127, 128, 1023, 1024, 1025, 2047, 2048, 2049, 3072, 4096, 8191, 10240, 65536}
	for i := 0; i < 300; i++ {
		lengths = append(lengths, rng.Intn(12000))
	}
	for _, n := range lengths {
		in := make([]byte, n)
		rng.Read(in)
		h := blake3.New()
		_, _ = h.Write(in)

		var want [2*numElements + 64]byte
		_, _ = h.Digest().Read(want[:])

		d := h.Digest()
		var got [2 * numElements]byte
		readXOF(d, &got)
		if !bytes.Equal(got[:], want[:2*numElements]) {
			t.Fatalf("output differs for input length %d", n)
		}
		var next [64]byte
		_, _ = d.Read(next[:])
		if !bytes.Equal(next[:], want[2*numElements:]) {
			t.Fatalf("digest position differs after readXOF for input length %d", n)
		}
	}
}

// Counters whose low word wraps inside a 16-lane group must carry into the
// high word exactly as the scalar compression does.
func TestXOFAVX512CounterCarry(t *testing.T) {
	requireAVX512XOF(t)
	h := blake3.New()
	_, _ = h.Write([]byte("counter carry"))
	for _, start := range []uint64{1<<32 - 8, 1<<32 - 1, 1 << 32, 1<<40 + 5, 1<<64 - 16} {
		ref := h.Digest()
		(*digestState)(unsafe.Pointer(ref)).counter = start
		var want [1024]byte
		_, _ = ref.Read(want[:])

		s := *(*digestState)(unsafe.Pointer(h.Digest()))
		var ctr [32]uint32
		for i := uint64(0); i < 16; i++ {
			c := start + i
			ctr[i], ctr[16+i] = uint32(c), uint32(c>>32)
		}
		var got [1024]byte
		xof16AVX512(&s.chain, &s.block, &ctr, s.blen, s.flags, &got)
		if got != want {
			t.Fatalf("output differs for counters starting at %#x", start)
		}
	}
}

func TestAccountHasherAVX512XOFMatchesReference(t *testing.T) {
	requireAVX512XOF(t)
	var hasher AccountHasher
	var got LtHash
	for seed := byte(0); seed < 40; seed++ {
		for _, n := range []int{0, 1, 82, 165, 200, 1000, 1024, 3000, 10240} {
			acct := accountHashTestAccount(seed, n)
			acct.Executable = seed%3 == 0
			hasher.HashInto(&got, acct)
			if !bytes.Equal(got.bytes(), referenceAccountHash(acct)) {
				t.Fatalf("account hash differs (seed %d, data length %d)", seed, n)
			}
		}
	}
}

func BenchmarkAccountHashInto(b *testing.B) {
	for _, n := range []int{0, 165, 1024, 10240} {
		acct := accountHashTestAccount(3, n)
		for _, avx := range []bool{false, true} {
			if avx && !useAVX512XOF {
				continue
			}
			name := "digest"
			if avx {
				name = "avx512"
			}
			b.Run(name+"/"+itoa(n), func(b *testing.B) {
				prev := useAVX512XOF
				useAVX512XOF = avx
				defer func() { useAVX512XOF = prev }()
				var hasher AccountHasher
				var dst LtHash
				b.SetBytes(int64(n))
				for i := 0; i < b.N; i++ {
					hasher.HashInto(&dst, acct)
				}
			})
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}
