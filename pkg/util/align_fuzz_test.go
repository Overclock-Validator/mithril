package util_test

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/util"
)

// FuzzAlignUint64 exercises the uint64 instantiation (appendvec/account padding
// callers). align is derived as a power of two so the helpers' precondition holds.
func FuzzAlignUint64(f *testing.F) {
	seeds := []struct {
		n     uint64
		shift uint
	}{
		{0, 0}, {1, 0}, {1, 3}, {7, 3}, {8, 3}, {9, 3},
		{4096, 12}, {4097, 12}, {8191, 12}, {8192, 12},
		{math.MaxUint64, 12}, {math.MaxUint64 - 5, 12}, {math.MaxUint64, 0},
	}
	for _, s := range seeds {
		f.Add(s.n, s.shift)
	}
	f.Fuzz(func(t *testing.T, n uint64, shiftRaw uint) {
		shift := shiftRaw % 64
		align := uint64(1) << shift

		down := util.AlignDown(n, align)
		checkAlignDownU64(t, n, align, down)

		// Round-up can overflow past MaxUint64; only check AlignUp where it can't.
		if n <= math.MaxUint64-(align-1) {
			up := util.AlignUp(n, align)
			checkAlignUpU64(t, n, align, up)

			if n%align == 0 {
				if up != n || down != n {
					t.Fatalf("aligned n=%d align=%d: up=%d down=%d (want both n)", n, align, up, down)
				}
			} else if up-down != align {
				t.Fatalf("unaligned n=%d align=%d: up=%d down=%d, up-down=%d want align", n, align, up, down, up-down)
			}
		}
	})
}

func checkAlignDownU64(t *testing.T, n, align, down uint64) {
	t.Helper()
	if down%align != 0 {
		t.Fatalf("AlignDown(%d,%d)=%d not a multiple of align", n, align, down)
	}
	if down > n {
		t.Fatalf("AlignDown(%d,%d)=%d > n", n, align, down)
	}
	if n-down >= align {
		t.Fatalf("AlignDown(%d,%d)=%d not the greatest multiple <= n (gap %d >= align)", n, align, down, n-down)
	}
	if got := util.AlignDown(down, align); got != down {
		t.Fatalf("AlignDown not idempotent: AlignDown(%d,%d)=%d", down, align, got)
	}
}

func checkAlignUpU64(t *testing.T, n, align, up uint64) {
	t.Helper()
	if up%align != 0 {
		t.Fatalf("AlignUp(%d,%d)=%d not a multiple of align", n, align, up)
	}
	if up < n {
		t.Fatalf("AlignUp(%d,%d)=%d < n", n, align, up)
	}
	if up-n >= align {
		t.Fatalf("AlignUp(%d,%d)=%d not the least multiple >= n (gap %d >= align)", n, align, up, up-n)
	}
	if got := util.AlignUp(up, align); got != up {
		t.Fatalf("AlignUp not idempotent: AlignUp(%d,%d)=%d", up, align, got)
	}
}

// FuzzAlignInt exercises the int instantiation (O_DIRECT page alignment). The
// helpers are documented for non-negative values, so negatives are folded away.
func FuzzAlignInt(f *testing.F) {
	seeds := []struct {
		n     int
		shift uint
	}{
		{0, 0}, {1, 0}, {1, 3}, {7, 3}, {8, 3}, {9, 3},
		{4096, 12}, {4097, 12}, {8192, 12}, {math.MaxInt, 12}, {math.MaxInt - 5, 12},
	}
	for _, s := range seeds {
		f.Add(s.n, s.shift)
	}
	f.Fuzz(func(t *testing.T, n int, shiftRaw uint) {
		if n < 0 {
			n = -n
		}
		if n < 0 { // n == math.MinInt: -n overflows back to negative
			t.Skip()
		}
		shift := shiftRaw % 63 // keep 1<<shift a positive int64
		align := 1 << shift

		down := util.AlignDown(n, align)
		if down%align != 0 || down > n || n-down >= align {
			t.Fatalf("AlignDown(%d,%d)=%d violates bounds", n, align, down)
		}
		if got := util.AlignDown(down, align); got != down {
			t.Fatalf("AlignDown not idempotent at %d", down)
		}

		if n <= math.MaxInt-(align-1) {
			up := util.AlignUp(n, align)
			if up%align != 0 || up < n || up-n >= align {
				t.Fatalf("AlignUp(%d,%d)=%d violates bounds", n, align, up)
			}
			if got := util.AlignUp(up, align); got != up {
				t.Fatalf("AlignUp not idempotent at %d", up)
			}
			if n%align == 0 {
				if up != n || down != n {
					t.Fatalf("aligned n=%d align=%d: up=%d down=%d", n, align, up, down)
				}
			} else if up-down != align {
				t.Fatalf("unaligned n=%d align=%d: up=%d down=%d", n, align, up, down)
			}
		}
	})
}
