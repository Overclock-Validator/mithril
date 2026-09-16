package lthash

import (
	"math/rand"
	"testing"
)

func randomLanes(rng *rand.Rand) *[numElements]uint16 {
	var lanes [numElements]uint16
	for i := range lanes {
		switch rng.Intn(8) {
		case 0:
			lanes[i] = 0
		case 1:
			lanes[i] = 0xffff
		case 2:
			lanes[i] = 0x8000
		default:
			lanes[i] = uint16(rng.Uint32())
		}
	}
	return &lanes
}

// TestMixMatchesGeneric checks the platform mixIn/mixOut against the
// portable loops, including wrap-around lanes, and that MixOut inverts MixIn.
func TestMixMatchesGeneric(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for iter := 0; iter < 2000; iter++ {
		dst := randomLanes(rng)
		src := randomLanes(rng)
		wantIn := *dst
		mixInGeneric(&wantIn, src)
		gotIn := *dst
		mixIn(&gotIn, src)
		if gotIn != wantIn {
			t.Fatalf("mixIn diverges from the generic loop (iteration %d)", iter)
		}
		wantOut := *dst
		mixOutGeneric(&wantOut, src)
		gotOut := *dst
		mixOut(&gotOut, src)
		if gotOut != wantOut {
			t.Fatalf("mixOut diverges from the generic loop (iteration %d)", iter)
		}
		roundTrip := gotIn
		mixOut(&roundTrip, src)
		if roundTrip != *dst {
			t.Fatalf("mixOut does not invert mixIn (iteration %d)", iter)
		}
	}
	// In-place: mixing a value into itself doubles every lane.
	dst := randomLanes(rng)
	want := *dst
	for i := range want {
		want[i] *= 2
	}
	mixIn(dst, dst)
	if *dst != want {
		t.Fatal("mixIn with aliased operands must double every lane")
	}
	mixOut(dst, dst)
	if *dst != [numElements]uint16{} {
		t.Fatal("mixOut with aliased operands must clear every lane")
	}
}

func TestLtHashMixInMixOutAndEquals(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	var a, b, c LtHash
	a.value = *randomLanes(rng)
	b.value = *randomLanes(rng)
	c = *a.Clone()
	c.MixIn(&b)
	if c.Equals(&a) {
		t.Fatal("mixing in a random value must change the hash")
	}
	c.MixOut(&b)
	if !c.Equals(&a) {
		t.Fatal("MixOut must undo MixIn")
	}
	c.value[numElements-1]++
	if c.Equals(&a) {
		t.Fatal("Equals must see a last-lane difference")
	}
}

func BenchmarkMixIn(b *testing.B) {
	rng := rand.New(rand.NewSource(5))
	dst := randomLanes(rng)
	src := randomLanes(rng)
	b.SetBytes(numElements * 2)
	for i := 0; i < b.N; i++ {
		mixIn(dst, src)
	}
}

func BenchmarkMixInGeneric(b *testing.B) {
	rng := rand.New(rand.NewSource(5))
	dst := randomLanes(rng)
	src := randomLanes(rng)
	b.SetBytes(numElements * 2)
	for i := 0; i < b.N; i++ {
		mixInGeneric(dst, src)
	}
}

func BenchmarkMixOut(b *testing.B) {
	rng := rand.New(rand.NewSource(5))
	dst := randomLanes(rng)
	src := randomLanes(rng)
	b.SetBytes(numElements * 2)
	for i := 0; i < b.N; i++ {
		mixOut(dst, src)
	}
}
