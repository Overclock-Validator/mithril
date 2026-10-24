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
