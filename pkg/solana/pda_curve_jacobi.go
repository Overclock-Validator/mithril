package solana

import (
	"encoding/binary"
	"math/bits"

	"filippo.io/edwards25519/field"
)

var fieldOne = new(field.Element).One()

// isOnCurve reports whether b is the encoding of a point on the ed25519 curve,
// with the same answer as decompressing it: bit 255 (the sign of x) is ignored,
// y at or above p is reduced, and any other length is not a point.
//
// A point with that y exists exactly when u/v is a square or zero, for
// u = y^2 - 1 and v = d*y^2 + 1 (v is never zero). Decompression finds the
// square root; a derived address only needs to know whether one exists, which
// the Jacobi symbol answers several times faster. u/v is a square exactly when
// u*v is, and 121666*u*v = u - 121665*u^2 with 121666 a nonzero square, so
// that is the value tested. The addresses are public, so the test is free to
// take time that depends on them.
//
// This follows is_valid_vartime in anza-xyz/cryptography (pull request 209).
func isOnCurve(b []byte) bool {
	var y, u, t field.Element
	if _, err := y.SetBytes(b); err != nil {
		return false
	}
	u.Subtract(u.Square(&y), fieldOne)
	t.Mult32(t.Square(&u), 121665)
	return isSquareVartime(u.Subtract(&u, &t).Bytes())
}

// isSquareVartime reports whether x, the 32-byte little-endian encoding of a
// value below p = 2^255 - 19, is a square modulo p or zero.
//
// It computes the Jacobi symbol (x|p) with the binary GCD (Stein's algorithm,
// Algorithm 1 of https://eprint.iacr.org/2020/972 without the inversion
// coefficients). a and b stay positive with b odd, and three rules change the
// symbol's sign: (2|b) is -1 when b is 3 or 5 mod 8, exchanging a and b flips
// it when both are 3 mod 4, and (a|b) = (a-b|b). Only the parity of the number
// of sign changes is kept, in bit 0 of flips.
func isSquareVartime(x []byte) bool {
	a0 := binary.LittleEndian.Uint64(x[0:])
	a1 := binary.LittleEndian.Uint64(x[8:])
	a2 := binary.LittleEndian.Uint64(x[16:])
	a3 := binary.LittleEndian.Uint64(x[24:])
	if a0|a1|a2|a3 == 0 {
		return true
	}
	b0, b1, b2, b3 := uint64(0xffffffffffffffed), uint64(0xffffffffffffffff), uint64(0xffffffffffffffff), uint64(0x7fffffffffffffff)
	var flips uint64

	// Make a odd. p is 5 mod 8, so each halving flips the sign.
	for a0 == 0 {
		a0, a1, a2, a3 = a1, a2, a3, 0 // 64 halvings: no flip
	}
	if z := uint(bits.TrailingZeros64(a0)); z != 0 {
		a0 = a0>>z | a1<<(64-z)
		a1 = a1>>z | a2<<(64-z)
		a2 = a2>>z | a3<<(64-z)
		a3 >>= z
		flips = uint64(z)
	}

	// Both odd. While either needs more than one word: replace the larger by
	// the difference, which is even, and halve it until it is odd again.
	// a = b happens only at a = b = 1, so the difference here is never zero.
	for a1|a2|a3|b1|b2|b3 != 0 {
		d0, borrow := bits.Sub64(a0, b0, 0)
		d1, borrow := bits.Sub64(a1, b1, borrow)
		d2, borrow := bits.Sub64(a2, b2, borrow)
		d3, borrow := bits.Sub64(a3, b3, borrow)
		// Which is larger is unpredictable, so the exchange is a selection
		// and not a branch: when a < b, b becomes a and a becomes b - a,
		// which is the negation of the difference just computed.
		swap := -borrow
		flips ^= swap & ((a0 & b0) >> 1)
		b0 ^= swap & (a0 ^ b0)
		b1 ^= swap & (a1 ^ b1)
		b2 ^= swap & (a2 ^ b2)
		b3 ^= swap & (a3 ^ b3)
		var carry uint64
		a0, carry = bits.Add64(d0^swap, borrow, 0)
		a1, carry = bits.Add64(d1^swap, 0, carry)
		a2, carry = bits.Add64(d2^swap, 0, carry)
		a3, _ = bits.Add64(d3^swap, 0, carry)

		for a0 == 0 {
			a0, a1, a2, a3 = a1, a2, a3, 0
		}
		z := uint(bits.TrailingZeros64(a0)) // 1 to 63 unless whole words were dropped
		if z != 0 {
			a0 = a0>>z | a1<<(64-z)
			a1 = a1>>z | a2<<(64-z)
			a2 = a2>>z | a3<<(64-z)
			a3 >>= z
			flips ^= uint64(z) & (b0>>1 ^ b0>>2)
		}
	}

	// The same on one word, to a = b = 1.
	for a0 != b0 {
		if a0 < b0 {
			flips ^= (a0 & b0) >> 1
			a0, b0 = b0, a0
		}
		a0 -= b0
		z := uint(bits.TrailingZeros64(a0))
		a0 >>= z
		flips ^= uint64(z) & (b0>>1 ^ b0>>2)
	}
	return flips&1 == 0
}
