package solana

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math/big"
	"math/rand"
	"testing"

	"filippo.io/edwards25519"
)

// isOnCurveDecompress is the check isOnCurve replaced: decompress the point
// and report whether that worked.
func isOnCurveDecompress(b []byte) bool {
	_, err := new(edwards25519.Point).SetBytes(b)
	return err == nil
}

var fieldPrime = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 255), big.NewInt(19))

// fieldBytes is v mod p as 32 little-endian bytes.
func fieldBytes(v *big.Int) []byte {
	out := make([]byte, 32)
	be := new(big.Int).Mod(v, fieldPrime).Bytes()
	for i, c := range be {
		out[len(be)-1-i] = c
	}
	return out
}

// fieldValues are values chosen to exercise the GCD: small, near p, near
// powers of two, long runs of zero bits (whole zero words included), every bit
// length, and sparse.
func fieldValues(rng *rand.Rand) []*big.Int {
	var out []*big.Int
	add := func(v *big.Int) {
		out = append(out, new(big.Int).Mod(v, fieldPrime), new(big.Int).Mod(new(big.Int).Neg(v), fieldPrime))
	}
	for k := int64(0); k <= 1024; k++ {
		add(big.NewInt(k))
	}
	for j := uint(0); j < 256; j++ {
		pow := new(big.Int).Lsh(big.NewInt(1), j)
		for _, base := range []*big.Int{pow, new(big.Int).Mul(pow, big.NewInt(3)), new(big.Int).Rsh(fieldPrime, j)} {
			for k := int64(-19); k <= 19; k++ {
				add(new(big.Int).Add(base, big.NewInt(k)))
			}
		}
		for _, i := range []uint{0, 1, j / 2, 63, 64, 65, 127, 128, 129, 191, 192, 193} {
			low := new(big.Int).Lsh(big.NewInt(1), i)
			add(new(big.Int).Add(pow, low))
			add(new(big.Int).Sub(pow, low))
		}
	}
	for bitLen := 1; bitLen <= 255; bitLen++ {
		for range 8 {
			v := new(big.Int).Rand(rng, new(big.Int).Lsh(big.NewInt(1), uint(bitLen-1)))
			add(v.SetBit(v, bitLen-1, 1))
		}
	}
	for range 4000 {
		v := new(big.Int)
		for range 2 + rng.Intn(7) {
			v.SetBit(v, rng.Intn(255), 1)
		}
		add(v)
	}
	for _, word := range []uint64{0x5555555555555555, 0xaaaaaaaaaaaaaaaa, 0x00000000ffffffff, 0xffffffff00000000, 0x8000000000000001, 0xffffffffffffffff, 0x0123456789abcdef} {
		for mask := 1; mask < 16; mask++ {
			v := new(big.Int)
			for limb := 3; limb >= 0; limb-- {
				v.Lsh(v, 64)
				if mask>>limb&1 == 1 {
					v.Or(v, new(big.Int).SetUint64(word))
				}
			}
			add(v)
		}
	}
	return out
}

// The symbol must agree with math/big's Jacobi symbol, which shares no code
// with it.
func TestIsSquareVartimeMatchesBigJacobi(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	values := fieldValues(rng)
	for range 200_000 {
		values = append(values, new(big.Int).Rand(rng, fieldPrime))
	}
	var squares, nonSquares int
	for _, v := range values {
		want := big.Jacobi(v, fieldPrime) >= 0
		if got := isSquareVartime(fieldBytes(v)); got != want {
			t.Fatalf("isSquareVartime(%x) = %v, Jacobi symbol %d", v, got, big.Jacobi(v, fieldPrime))
		}
		if want {
			squares++
		} else {
			nonSquares++
		}
	}
	if squares < 50_000 || nonSquares < 50_000 {
		t.Fatalf("lopsided inputs: %d squares, %d non-squares", squares, nonSquares)
	}
}

// encodings are 32-byte inputs chosen for the decoding rules: every small y,
// y around p (canonical and not), y around 2^255, the sign bit, and y values
// that make the tested value one of fieldValues.
func checkOnCurveEncoding(t *testing.T, b []byte) {
	t.Helper()
	if got, want := isOnCurve(b), isOnCurveDecompress(b); got != want {
		t.Fatalf("isOnCurve(%x) = %v, decompression says %v", b, got, want)
	}
}

func TestIsOnCurveMatchesDecompression(t *testing.T) {
	check := func(b []byte) {
		t.Helper()
		checkOnCurveEncoding(t, b)
		flipped := append([]byte(nil), b...)
		flipped[31] ^= 0x80
		checkOnCurveEncoding(t, flipped)
	}
	// y = k, p - k, p + k (not canonical for k < 19 once bit 255 is dropped)
	// and 2^255 - 1 - k.
	top := new(big.Int).Lsh(big.NewInt(1), 255)
	for k := int64(0); k < 4096; k++ {
		for _, v := range []*big.Int{
			big.NewInt(k),
			new(big.Int).Sub(fieldPrime, big.NewInt(k)),
			new(big.Int).Add(fieldPrime, big.NewInt(k%19)),
			new(big.Int).Sub(top, big.NewInt(k+1)),
		} {
			b := make([]byte, 32)
			be := v.Bytes()
			for i, c := range be {
				b[len(be)-1-i] = c
			}
			check(b)
		}
	}
	// Two structured encodings from the tests of anza-xyz/cryptography pull
	// request 209: one whose tested value is 2^254 + 2^250 and one that takes
	// 253 of the at most 255 GCD steps.
	for _, h := range []string{
		"7379fce5984ae7b06649cb0a9134e7439357b9c2fdeb244b527e77755233217f",
		"c8ae8bc6688e483d1384672a8861c60da024e06fd83a337a1d6871018978244d",
	} {
		b, err := hex.DecodeString(h)
		if err != nil {
			t.Fatal(err)
		}
		check(b)
	}
	rng := rand.New(rand.NewSource(2))
	for _, v := range fieldValues(rng) {
		check(fieldBytes(v)) // as y
	}
	var on, off int
	for i := range 2_000_000 {
		var seed [8]byte
		binary.LittleEndian.PutUint64(seed[:], uint64(i))
		h := sha256.Sum256(seed[:])
		check(h[:])
		if isOnCurve(h[:]) {
			on++
		} else {
			off++
		}
	}
	if on < 900_000 || off < 900_000 {
		t.Fatalf("lopsided inputs: %d on the curve, %d off", on, off)
	}
	for _, b := range [][]byte{nil, {}, {1}, make([]byte, 31), make([]byte, 33), make([]byte, 64)} {
		checkOnCurveEncoding(t, b)
	}
}

func FuzzIsOnCurveMatchesDecompression(f *testing.F) {
	f.Add(make([]byte, 32))
	f.Add([]byte{1})
	f.Add(fieldBytes(big.NewInt(1)))
	f.Add(fieldBytes(big.NewInt(-1)))
	one := sha256.Sum256([]byte("seed"))
	f.Add(one[:])
	f.Fuzz(func(t *testing.T, b []byte) {
		if got, want := isOnCurve(b), isOnCurveDecompress(b); got != want {
			t.Fatalf("isOnCurve(%x) = %v, decompression says %v", b, got, want)
		}
	})
}

func BenchmarkIsOnCurve(b *testing.B) {
	keys := make([][32]byte, 4096)
	for i := range keys {
		keys[i] = sha256.Sum256([]byte{byte(i), byte(i >> 8)})
	}
	b.Run("decompress", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			isOnCurveDecompress(keys[i%len(keys)][:])
		}
	})
	b.Run("jacobi", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			isOnCurve(keys[i%len(keys)][:])
		}
	})
}

// A program address search hashes and tests bump seeds from 255 downward.
func BenchmarkCreateProgramAddress(b *testing.B) {
	program := sha256.Sum256([]byte("program"))
	seeds := [][]byte{[]byte("metadata"), program[:], {255}}
	for i := 0; b.Loop(); i++ {
		seeds[2][0] = byte(i)
		binary.LittleEndian.PutUint32(seeds[0], uint32(i>>8)) // a new address each time: no memo hit
		_, _ = CreateProgramAddressBytes(seeds, program[:])
	}
}
