package sealevel

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
)

// FuzzMemOpConsume tests compute unit consumption for memory operations
func FuzzMemOpConsume(f *testing.F) {
	// Seed with edge cases
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(cu.CUMemOpBaseCost))
	f.Add(uint64(cu.CUCpiBytesPerUnit))
	f.Add(uint64(1000000))

	f.Fuzz(func(t *testing.T, n uint64) {
		// Create execution context with sufficient compute units
		execCtx := &ExecutionCtx{
			ComputeMeter: cu.NewComputeMeterDefault(),
		}

		initialUnits := execCtx.ComputeMeter.Remaining()
		err := MemOpConsume(execCtx, n)

		// Verify error handling
		if err != nil {
			// Error only occurs when compute units exhausted
			return
		}

		// Verify correct cost calculation: max(base_cost, n/bytes_per_unit)
		expectedCost := max(cu.CUMemOpBaseCost, n/cu.CUCpiBytesPerUnit)
		actualCost := initialUnits - execCtx.ComputeMeter.Remaining()

		if actualCost != expectedCost {
			t.Errorf("MemOpConsume(%d) consumed %d units, want %d", n, actualCost, expectedCost)
		}
	})
}

// FuzzIsNonOverlapping tests memory region overlap detection
func FuzzIsNonOverlapping(f *testing.F) {
	// Seed with edge cases
	f.Add(uint64(0), uint64(100), uint64(100), uint64(100))
	f.Add(uint64(0), uint64(100), uint64(50), uint64(100))
	f.Add(uint64(100), uint64(100), uint64(0), uint64(100))
	f.Add(uint64(1000), uint64(100), uint64(2000), uint64(100))

	f.Fuzz(func(t *testing.T, src, srcLen, dst, dstLen uint64) {
		result := isNonOverlapping(src, srcLen, dst, dstLen)

		// Manually verify overlap detection
		srcEnd := src + srcLen
		dstEnd := dst + dstLen

		// Check for overflow in additions
		if srcEnd < src || dstEnd < dst {
			// Overflow occurred, skip
			return
		}

		// Regions don't overlap if one ends before the other starts
		expectedNonOverlap := (srcEnd <= dst) || (dstEnd <= src)

		if result != expectedNonOverlap {
			t.Errorf("isNonOverlapping(src=%d, srcLen=%d, dst=%d, dstLen=%d) = %v, want %v",
				src, srcLen, dst, dstLen, result, expectedNonOverlap)
		}
	})
}
