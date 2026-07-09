// Fixed-base comb evaluation for arbitrary points. This file is part
// of mithril, not the vendored upstream code.

package edwards25519

import "github.com/Overclock-Validator/mithril/pkg/ed25519fast/internal/edwards25519/field"

// PubkeyTable is a fixed-base comb table for an arbitrary point,
// mirroring the generator's precomputed table: table i holds multiples
// of 256^i * P, so a signed radix-16 scalar multiplication needs no
// doubling chain. It costs a few scalar multiplications to build and
// 30 KiB to hold, paying off after a handful of uses of a key.
type PubkeyTable [32]affineLookupTable

func NewPubkeyTable(p *Point) *PubkeyTable {
	// Generate all 256 multiples (j+1) * 256^i * P in extended
	// coordinates, then convert to affine with a single shared
	// inversion; the upstream affineLookupTable.FromP3 inverts per
	// point, which is ~50x slower to build.
	var pts [32 * 8]Point
	q := (&Point{}).Set(p)
	for i := 0; i < 32; i++ {
		pts[i*8].Set(q)
		for j := 1; j < 8; j++ {
			pts[i*8+j].Add(&pts[i*8+j-1], q)
		}
		for j := 0; j < 8; j++ {
			q.Add(q, q)
		}
	}

	// Montgomery batch inversion of every Z.
	var acc [32 * 8]field.Element
	prod := new(field.Element).One()
	for i := range pts {
		acc[i].Set(prod)
		prod.Multiply(prod, &pts[i].z)
	}
	inv := new(field.Element).Invert(prod)
	var zInv field.Element
	t := new(PubkeyTable)
	for i := len(pts) - 1; i >= 0; i-- {
		zInv.Multiply(inv, &acc[i])
		inv.Multiply(inv, &pts[i].z)

		dst := &t[i/8].points[i%8]
		dst.YplusX.Add(&pts[i].y, &pts[i].x)
		dst.YminusX.Subtract(&pts[i].y, &pts[i].x)
		dst.T2d.Multiply(&pts[i].t, d2)
		dst.YplusX.Multiply(&dst.YplusX, &zInv)
		dst.YminusX.Multiply(&dst.YminusX, &zInv)
		dst.T2d.Multiply(&dst.T2d, &zInv)
	}
	return t
}

// selectVartime sets dest to x*Q from the table in variable time,
// where -8 <= x <= 8, reporting whether x was nonzero.
func (v *affineLookupTable) selectVartime(dest *affineCached, x int8) bool {
	switch {
	case x == 0:
		return false
	case x > 0:
		*dest = v.points[x-1]
	default:
		*dest = v.points[-x-1]
		dest.CondNeg(1)
	}
	return true
}

// VarTimeDoubleCombMult sets v = a*A + b*B, where aTable is A's comb
// table and B is the canonical generator. It follows ScalarBaseMult's
// digit split (odd digits, one multiplication by 16, even digits) for
// both points at once, in variable time.
func (v *Point) VarTimeDoubleCombMult(a *Scalar, aTable *PubkeyTable, b *Scalar) *Point {
	bTable := basepointTable()
	aDigits := a.signedRadix16()
	bDigits := b.signedRadix16()

	multiple := &affineCached{}
	tmp1 := &projP1xP1{}
	tmp2 := &projP2{}

	v.Set(NewIdentityPoint())
	for i := 1; i < 64; i += 2 {
		if aTable[i/2].selectVartime(multiple, aDigits[i]) {
			tmp1.AddAffine(v, multiple)
			v.fromP1xP1(tmp1)
		}
		if bTable[i/2].selectVartime(multiple, bDigits[i]) {
			tmp1.AddAffine(v, multiple)
			v.fromP1xP1(tmp1)
		}
	}

	tmp2.FromP3(v)
	tmp1.Double(tmp2)
	tmp2.FromP1xP1(tmp1)
	tmp1.Double(tmp2)
	tmp2.FromP1xP1(tmp1)
	tmp1.Double(tmp2)
	tmp2.FromP1xP1(tmp1)
	tmp1.Double(tmp2)
	v.fromP1xP1(tmp1)

	for i := 0; i < 64; i += 2 {
		if aTable[i/2].selectVartime(multiple, aDigits[i]) {
			tmp1.AddAffine(v, multiple)
			v.fromP1xP1(tmp1)
		}
		if bTable[i/2].selectVartime(multiple, bDigits[i]) {
			tmp1.AddAffine(v, multiple)
			v.fromP1xP1(tmp1)
		}
	}

	return v
}
