//go:build amd64 && !purego

package lthash

import (
	"math/rand"
	"testing"
	"unsafe"
)

// TestMixAVX2AgainstGeneric runs the assembly directly (when the CPU has
// AVX2) against the portable loops so the comparison does not depend on the
// dispatch variable.
func TestMixAVX2AgainstGeneric(t *testing.T) {
	if !useAVX2 {
		t.Skip("no AVX2 on this machine")
	}
	rng := rand.New(rand.NewSource(6))
	for iter := 0; iter < 2000; iter++ {
		dst := randomLanes(rng)
		src := randomLanes(rng)
		want, got := *dst, *dst
		mixInGeneric(&want, src)
		mixInAVX2(&got, src)
		if got != want {
			t.Fatalf("mixInAVX2 diverges (iteration %d)", iter)
		}
		want, got = *dst, *dst
		mixOutGeneric(&want, src)
		mixOutAVX2(&got, src)
		if got != want {
			t.Fatalf("mixOutAVX2 diverges (iteration %d)", iter)
		}
	}
}

// TestMixGenericFallbackSelectable makes sure the dispatch honours the flag,
// so a machine without AVX2 takes the portable path.
func TestMixGenericFallbackSelectable(t *testing.T) {
	saved := useAVX2
	defer func() { useAVX2 = saved }()
	useAVX2 = false
	rng := rand.New(rand.NewSource(8))
	dst := randomLanes(rng)
	src := randomLanes(rng)
	want := *dst
	mixInGeneric(&want, src)
	mixIn(dst, src)
	if *dst != want {
		t.Fatal("generic fallback must be used when AVX2 is disabled")
	}
}

func TestMixAVX2UnalignedAndAliased(t *testing.T) {
	if !useAVX2 {
		t.Skip("AVX2 unavailable")
	}
	rng := rand.New(rand.NewSource(19))
	for off := 0; off < 32; off += 2 {
		storage := make([]byte, numElements*2+32)
		dst := (*[numElements]uint16)(unsafe.Pointer(&storage[off]))
		*dst = *randomLanes(rng)
		want := *dst
		mixInGeneric(&want, &want)
		mixInAVX2(dst, dst)
		if *dst != want {
			t.Fatalf("aliased addition offset %d", off)
		}
		mixOutAVX2(dst, dst)
		if *dst != ([numElements]uint16{}) {
			t.Fatalf("aliased subtraction offset %d", off)
		}
	}
}
