package alpenglow

import (
	"math/big"
	"math/rand"
	"testing"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
)

// affineChain is the previous aggregation: one affine addition (and one field
// inversion) per signer.
func affineChain(points []bls12381.G1Affine) bls12381.G1Affine {
	var aggregate bls12381.G1Affine
	aggregate.SetInfinity()
	for i := range points {
		aggregate.Add(&aggregate, &points[i])
	}
	return aggregate
}

// jacobianChain mirrors aggregateVerificationTerm: accumulate in Jacobian
// coordinates and convert to affine once.
func jacobianChain(points []bls12381.G1Affine) bls12381.G1Affine {
	var acc bls12381.G1Jac
	for i := range points {
		acc.AddMixed(&points[i])
	}
	var aggregate bls12381.G1Affine
	aggregate.FromJacobian(&acc)
	return aggregate
}

func randomG1(rng *rand.Rand, n int) []bls12381.G1Affine {
	_, _, g1, _ := bls12381.Generators()
	out := make([]bls12381.G1Affine, n)
	for i := range out {
		s := new(big.Int).Rand(rng, new(big.Int).Lsh(big.NewInt(1), 255))
		out[i].ScalarMultiplication(&g1, s)
	}
	return out
}

func TestJacobianAggregationMatchesAffine(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	base := randomG1(rng, 64)
	var neg0 bls12381.G1Affine
	neg0.Neg(&base[0])
	var inf bls12381.G1Affine
	inf.SetInfinity()
	cases := map[string][]bls12381.G1Affine{
		"empty":               {},
		"single":              base[:1],
		"random":              base,
		"duplicate (double)":  {base[1], base[1], base[2]},
		"cancel to infinity":  {base[0], neg0},
		"cancel then add":     {base[0], neg0, base[3], base[4]},
		"infinity members":    {inf, base[5], inf, base[6], inf},
		"only infinity":       {inf, inf},
		"repeated many times": {base[7], base[7], base[7], base[7], base[8], base[8]},
	}
	for i := 0; i < 50; i++ {
		var pts []bls12381.G1Affine
		for j := 0; j < 1+rng.Intn(40); j++ {
			p := base[rng.Intn(len(base))]
			switch rng.Intn(5) {
			case 0:
				p.Neg(&p)
			case 1:
				p = inf
			}
			pts = append(pts, p)
		}
		cases["mixed-"+string(rune('a'+i%26))+string(rune('a'+i/26))] = pts
	}
	for name, pts := range cases {
		want, got := affineChain(pts), jacobianChain(pts)
		if !want.Equal(&got) || want.Bytes() != got.Bytes() {
			t.Fatalf("%s: affine %v != jacobian %v", name, want, got)
		}
	}
}

func randomG2(rng *rand.Rand, n int) []bls12381.G2Affine {
	_, _, _, g2 := bls12381.Generators()
	out := make([]bls12381.G2Affine, n)
	for i := range out {
		s := new(big.Int).Rand(rng, new(big.Int).Lsh(big.NewInt(1), 255))
		out[i].ScalarMultiplication(&g2, s)
	}
	return out
}

func affineChainG2(points []bls12381.G2Affine) bls12381.G2Affine {
	var aggregate bls12381.G2Affine
	aggregate.SetInfinity()
	for i := range points {
		aggregate.Add(&aggregate, &points[i])
	}
	return aggregate
}

// jacobianSplitG2 mirrors the vote pool: two tallies accumulated in Jacobian
// form (AddMixed per vote), combined with AddAssign at assembly, converted once.
func jacobianSplitG2(points []bls12381.G2Affine, split int) bls12381.G2Affine {
	var base, fallback, acc bls12381.G2Jac
	for i := range points {
		if i < split {
			base.AddMixed(&points[i])
		} else {
			fallback.AddMixed(&points[i])
		}
	}
	acc.AddAssign(&base)
	acc.AddAssign(&fallback)
	var aggregate bls12381.G2Affine
	aggregate.FromJacobian(&acc)
	return aggregate
}

func TestJacobianSignatureAggregationMatchesAffine(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	base := randomG2(rng, 24)
	var neg0 bls12381.G2Affine
	neg0.Neg(&base[0])
	var inf bls12381.G2Affine
	inf.SetInfinity()
	cases := [][]bls12381.G2Affine{
		{}, base[:1], base, {base[1], base[1]}, {base[0], neg0}, {base[0], base[2], neg0},
		{inf, base[3], inf}, {inf}, {base[4], base[4], base[4], base[5]},
	}
	for i := 0; i < 30; i++ {
		var pts []bls12381.G2Affine
		for j := 0; j < 1+rng.Intn(20); j++ {
			p := base[rng.Intn(len(base))]
			switch rng.Intn(5) {
			case 0:
				p.Neg(&p)
			case 1:
				p = inf
			}
			pts = append(pts, p)
		}
		cases = append(cases, pts)
	}
	for i, pts := range cases {
		want := affineChainG2(pts)
		// Every split point: base-only, fallback-only and mixed tallies,
		// including a base tally that cancels against the fallback one.
		for split := 0; split <= len(pts); split++ {
			got := jacobianSplitG2(pts, split)
			if !want.Equal(&got) || want.RawBytes() != got.RawBytes() {
				t.Fatalf("case %d split %d: affine %v != jacobian %v", i, split, want, got)
			}
		}
	}
}

func BenchmarkSignatureAggregation(b *testing.B) {
	pts := randomG2(rand.New(rand.NewSource(1)), 1000)
	b.Run("affine", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			affineChainG2(pts)
		}
	})
	b.Run("jacobian", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			jacobianSplitG2(pts, len(pts)/2)
		}
	})
}

func BenchmarkPubkeyAggregation(b *testing.B) {
	pts := randomG1(rand.New(rand.NewSource(1)), 1000)
	b.Run("affine", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			affineChain(pts)
		}
	})
	b.Run("jacobian", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			jacobianChain(pts)
		}
	})
}
