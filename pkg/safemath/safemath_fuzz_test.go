package safemath

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/wide"
)

// FuzzCheckedAddU8 tests CheckedAddU8 for overflow detection
func FuzzCheckedAddU8(f *testing.F) {
	// Seed with edge cases
	f.Add(uint8(0), uint8(0))
	f.Add(uint8(255), uint8(0))
	f.Add(uint8(255), uint8(1))
	f.Add(uint8(128), uint8(128))

	f.Fuzz(func(t *testing.T, a, b uint8) {
		result, err := CheckedAddU8(a, b)

		// Verify consistency with Go's overflow behavior
		goResult := a + b

		if goResult >= a && goResult >= b {
			// No overflow
			if err != nil {
				t.Errorf("CheckedAddU8(%d, %d) returned error but should succeed", a, b)
			}
			if result != goResult {
				t.Errorf("CheckedAddU8(%d, %d) = %d, want %d", a, b, result, goResult)
			}
		} else {
			// Overflow occurred
			if err == nil {
				t.Errorf("CheckedAddU8(%d, %d) should detect overflow", a, b)
			}
		}
	})
}

// FuzzCheckedMulU8 tests CheckedMulU8 for overflow detection
func FuzzCheckedMulU8(f *testing.F) {
	f.Add(uint8(0), uint8(0))
	f.Add(uint8(255), uint8(1))
	f.Add(uint8(16), uint8(16))
	f.Add(uint8(255), uint8(255))

	f.Fuzz(func(t *testing.T, a, b uint8) {
		result, err := CheckedMulU8(a, b)

		// Calculate expected result using wider type
		expected := uint16(a) * uint16(b)

		if expected <= math.MaxUint8 {
			// No overflow
			if err != nil {
				t.Errorf("CheckedMulU8(%d, %d) returned error but should succeed", a, b)
			}
			if result != uint8(expected) {
				t.Errorf("CheckedMulU8(%d, %d) = %d, want %d", a, b, result, uint8(expected))
			}
		} else {
			// Overflow
			if err == nil {
				t.Errorf("CheckedMulU8(%d, %d) should detect overflow (result would be %d)", a, b, expected)
			}
		}
	})
}

// FuzzCheckedSubU8 tests CheckedSubU8 for underflow detection
func FuzzCheckedSubU8(f *testing.F) {
	f.Add(uint8(0), uint8(0))
	f.Add(uint8(255), uint8(0))
	f.Add(uint8(0), uint8(1))
	f.Add(uint8(100), uint8(50))

	f.Fuzz(func(t *testing.T, a, b uint8) {
		result, err := CheckedSubU8(a, b)

		if a >= b {
			// No underflow
			if err != nil {
				t.Errorf("CheckedSubU8(%d, %d) returned error but should succeed", a, b)
			}
			if result != a-b {
				t.Errorf("CheckedSubU8(%d, %d) = %d, want %d", a, b, result, a-b)
			}
		} else {
			// Underflow
			if err == nil {
				t.Errorf("CheckedSubU8(%d, %d) should detect underflow", a, b)
			}
		}
	})
}

// FuzzCheckedDivU8 tests CheckedDivU8 for division by zero
func FuzzCheckedDivU8(f *testing.F) {
	f.Add(uint8(0), uint8(1))
	f.Add(uint8(255), uint8(1))
	f.Add(uint8(100), uint8(0))
	f.Add(uint8(100), uint8(3))

	f.Fuzz(func(t *testing.T, a, b uint8) {
		result, err := CheckedDivU8(a, b)

		if b == 0 {
			// Division by zero
			if err == nil {
				t.Errorf("CheckedDivU8(%d, %d) should detect division by zero", a, b)
			}
		} else {
			// Valid division
			if err != nil {
				t.Errorf("CheckedDivU8(%d, %d) returned error but should succeed", a, b)
			}
			if result != a/b {
				t.Errorf("CheckedDivU8(%d, %d) = %d, want %d", a, b, result, a/b)
			}
		}
	})
}

// FuzzCheckedAddU16 tests CheckedAddU16 for overflow detection
func FuzzCheckedAddU16(f *testing.F) {
	f.Add(uint16(0), uint16(0))
	f.Add(uint16(65535), uint16(0))
	f.Add(uint16(65535), uint16(1))
	f.Add(uint16(32768), uint16(32768))

	f.Fuzz(func(t *testing.T, a, b uint16) {
		result, err := CheckedAddU16(a, b)

		// Calculate using wider type
		expected := uint32(a) + uint32(b)

		if expected <= math.MaxUint16 {
			// No overflow
			if err != nil {
				t.Errorf("CheckedAddU16(%d, %d) returned error but should succeed", a, b)
			}
			if result != uint16(expected) {
				t.Errorf("CheckedAddU16(%d, %d) = %d, want %d", a, b, result, uint16(expected))
			}
		} else {
			// Overflow
			if err == nil {
				t.Errorf("CheckedAddU16(%d, %d) should detect overflow", a, b)
			}
		}
	})
}

// FuzzCheckedMulU16 tests CheckedMulU16 for overflow detection
func FuzzCheckedMulU16(f *testing.F) {
	f.Add(uint16(0), uint16(0))
	f.Add(uint16(65535), uint16(1))
	f.Add(uint16(256), uint16(256))
	f.Add(uint16(65535), uint16(65535))

	f.Fuzz(func(t *testing.T, a, b uint16) {
		result, err := CheckedMulU16(a, b)

		// Calculate expected result using wider type
		expected := uint32(a) * uint32(b)

		if expected <= math.MaxUint16 {
			// No overflow
			if err != nil {
				t.Errorf("CheckedMulU16(%d, %d) returned error but should succeed", a, b)
			}
			if result != uint16(expected) {
				t.Errorf("CheckedMulU16(%d, %d) = %d, want %d", a, b, result, uint16(expected))
			}
		} else {
			// Overflow
			if err == nil {
				t.Errorf("CheckedMulU16(%d, %d) should detect overflow (result would be %d)", a, b, expected)
			}
		}
	})
}

// FuzzCheckedSubU16 tests CheckedSubU16 for underflow detection
func FuzzCheckedSubU16(f *testing.F) {
	f.Add(uint16(0), uint16(0))
	f.Add(uint16(65535), uint16(0))
	f.Add(uint16(0), uint16(1))
	f.Add(uint16(1000), uint16(500))

	f.Fuzz(func(t *testing.T, a, b uint16) {
		result, err := CheckedSubU16(a, b)

		if a >= b {
			// No underflow
			if err != nil {
				t.Errorf("CheckedSubU16(%d, %d) returned error but should succeed", a, b)
			}
			if result != a-b {
				t.Errorf("CheckedSubU16(%d, %d) = %d, want %d", a, b, result, a-b)
			}
		} else {
			// Underflow
			if err == nil {
				t.Errorf("CheckedSubU16(%d, %d) should detect underflow", a, b)
			}
		}
	})
}

// FuzzCheckedDivU16 tests CheckedDivU16 for division by zero
func FuzzCheckedDivU16(f *testing.F) {
	f.Add(uint16(0), uint16(1))
	f.Add(uint16(65535), uint16(1))
	f.Add(uint16(1000), uint16(0))
	f.Add(uint16(1000), uint16(3))

	f.Fuzz(func(t *testing.T, a, b uint16) {
		result, err := CheckedDivU16(a, b)

		if b == 0 {
			// Division by zero
			if err == nil {
				t.Errorf("CheckedDivU16(%d, %d) should detect division by zero", a, b)
			}
		} else {
			// Valid division
			if err != nil {
				t.Errorf("CheckedDivU16(%d, %d) returned error but should succeed", a, b)
			}
			if result != a/b {
				t.Errorf("CheckedDivU16(%d, %d) = %d, want %d", a, b, result, a/b)
			}
		}
	})
}

// FuzzCheckedAddU32 tests CheckedAddU32 with hardware carry detection
func FuzzCheckedAddU32(f *testing.F) {
	f.Add(uint32(0), uint32(0))
	f.Add(uint32(math.MaxUint32), uint32(0))
	f.Add(uint32(math.MaxUint32), uint32(1))
	f.Add(uint32(1<<31), uint32(1<<31))

	f.Fuzz(func(t *testing.T, a, b uint32) {
		result, err := CheckedAddU32(a, b)

		// Calculate using wider type
		expected := uint64(a) + uint64(b)

		if expected <= math.MaxUint32 {
			// No overflow
			if err != nil {
				t.Errorf("CheckedAddU32(%d, %d) returned error but should succeed", a, b)
			}
			if result != uint32(expected) {
				t.Errorf("CheckedAddU32(%d, %d) = %d, want %d", a, b, result, uint32(expected))
			}
		} else {
			// Overflow
			if err == nil {
				t.Errorf("CheckedAddU32(%d, %d) should detect overflow", a, b)
			}
		}
	})
}

// FuzzCheckedMulU32 tests CheckedMulU32 with hardware overflow detection
func FuzzCheckedMulU32(f *testing.F) {
	f.Add(uint32(0), uint32(0))
	f.Add(uint32(math.MaxUint32), uint32(1))
	f.Add(uint32(65536), uint32(65536))
	f.Add(uint32(1000000), uint32(1000000))

	f.Fuzz(func(t *testing.T, a, b uint32) {
		result, err := CheckedMulU32(a, b)

		// Calculate using wider type
		expected := uint64(a) * uint64(b)

		if expected <= math.MaxUint32 {
			// No overflow
			if err != nil {
				t.Errorf("CheckedMulU32(%d, %d) returned error but should succeed", a, b)
			}
			if result != uint32(expected) {
				t.Errorf("CheckedMulU32(%d, %d) = %d, want %d", a, b, result, uint32(expected))
			}
		} else {
			// Overflow
			if err == nil {
				t.Errorf("CheckedMulU32(%d, %d) should detect overflow (result would be %d)", a, b, expected)
			}
		}
	})
}

// FuzzCheckedDivU32 tests CheckedDivU32 for division by zero
func FuzzCheckedDivU32(f *testing.F) {
	f.Add(uint32(0), uint32(1))
	f.Add(uint32(math.MaxUint32), uint32(1))
	f.Add(uint32(1000000), uint32(0))
	f.Add(uint32(1000000), uint32(3))

	f.Fuzz(func(t *testing.T, a, b uint32) {
		result, err := CheckedDivU32(a, b)

		if b == 0 {
			// Division by zero
			if err == nil {
				t.Errorf("CheckedDivU32(%d, %d) should detect division by zero", a, b)
			}
		} else {
			// Valid division
			if err != nil {
				t.Errorf("CheckedDivU32(%d, %d) returned error but should succeed", a, b)
			}
			if result != a/b {
				t.Errorf("CheckedDivU32(%d, %d) = %d, want %d", a, b, result, a/b)
			}
		}
	})
}

// FuzzCheckedAddU64 tests CheckedAddU64 with hardware carry detection
func FuzzCheckedAddU64(f *testing.F) {
	f.Add(uint64(0), uint64(0))
	f.Add(uint64(math.MaxUint64), uint64(0))
	f.Add(uint64(math.MaxUint64), uint64(1))
	f.Add(uint64(1<<63), uint64(1<<63))

	f.Fuzz(func(t *testing.T, a, b uint64) {
		result, err := CheckedAddU64(a, b)

		// Check for overflow by comparing with max value
		if a > math.MaxUint64-b {
			// Overflow expected
			if err == nil {
				t.Errorf("CheckedAddU64(%d, %d) should detect overflow", a, b)
			}
		} else {
			// No overflow
			if err != nil {
				t.Errorf("CheckedAddU64(%d, %d) returned error but should succeed", a, b)
			}
			if result != a+b {
				t.Errorf("CheckedAddU64(%d, %d) = %d, want %d", a, b, result, a+b)
			}
		}
	})
}

// FuzzCheckedMulU64 tests CheckedMulU64 with hardware overflow detection
func FuzzCheckedMulU64(f *testing.F) {
	f.Add(uint64(0), uint64(0))
	f.Add(uint64(math.MaxUint64), uint64(1))
	f.Add(uint64(4294967296), uint64(4294967296))
	f.Add(uint64(1000000000), uint64(1000000000))

	f.Fuzz(func(t *testing.T, a, b uint64) {
		result, err := CheckedMulU64(a, b)

		// Check for overflow
		if a != 0 && b > math.MaxUint64/a {
			// Overflow expected
			if err == nil {
				t.Errorf("CheckedMulU64(%d, %d) should detect overflow", a, b)
			}
		} else {
			// No overflow
			if err != nil {
				t.Errorf("CheckedMulU64(%d, %d) returned error but should succeed", a, b)
			}
			if result != a*b {
				t.Errorf("CheckedMulU64(%d, %d) = %d, want %d", a, b, result, a*b)
			}
		}
	})
}

// FuzzCheckedSubU64 tests CheckedSubU64 with hardware borrow detection
func FuzzCheckedSubU64(f *testing.F) {
	f.Add(uint64(0), uint64(0))
	f.Add(uint64(math.MaxUint64), uint64(0))
	f.Add(uint64(0), uint64(1))
	f.Add(uint64(1000000), uint64(500000))

	f.Fuzz(func(t *testing.T, a, b uint64) {
		result, err := CheckedSubU64(a, b)

		if a >= b {
			// No underflow
			if err != nil {
				t.Errorf("CheckedSubU64(%d, %d) returned error but should succeed", a, b)
			}
			if result != a-b {
				t.Errorf("CheckedSubU64(%d, %d) = %d, want %d", a, b, result, a-b)
			}
		} else {
			// Underflow
			if err == nil {
				t.Errorf("CheckedSubU64(%d, %d) should detect underflow", a, b)
			}
		}
	})
}

// FuzzCheckedDivU64 tests CheckedDivU64 for division by zero
func FuzzCheckedDivU64(f *testing.F) {
	f.Add(uint64(0), uint64(1))
	f.Add(uint64(math.MaxUint64), uint64(1))
	f.Add(uint64(1000000), uint64(0))
	f.Add(uint64(1000000), uint64(3))

	f.Fuzz(func(t *testing.T, a, b uint64) {
		result, err := CheckedDivU64(a, b)

		if b == 0 {
			// Division by zero
			if err == nil {
				t.Errorf("CheckedDivU64(%d, %d) should detect division by zero", a, b)
			}
		} else {
			// Valid division
			if err != nil {
				t.Errorf("CheckedDivU64(%d, %d) returned error but should succeed", a, b)
			}
			if result != a/b {
				t.Errorf("CheckedDivU64(%d, %d) = %d, want %d", a, b, result, a/b)
			}
		}
	})
}

// FuzzSaturatingAddU8 tests saturating addition for uint8
func FuzzSaturatingAddU8(f *testing.F) {
	f.Add(uint8(0), uint8(0))
	f.Add(uint8(255), uint8(1))
	f.Add(uint8(128), uint8(128))
	f.Add(uint8(100), uint8(100))

	f.Fuzz(func(t *testing.T, a, b uint8) {
		result := SaturatingAddU8(a, b)

		// Calculate expected result
		expected := uint16(a) + uint16(b)

		if expected > math.MaxUint8 {
			// Should saturate at max
			if result != math.MaxUint8 {
				t.Errorf("SaturatingAddU8(%d, %d) = %d, want %d (saturated)", a, b, result, math.MaxUint8)
			}
		} else {
			// Normal result
			if result != uint8(expected) {
				t.Errorf("SaturatingAddU8(%d, %d) = %d, want %d", a, b, result, uint8(expected))
			}
		}
	})
}

// FuzzSaturatingMulU8 tests saturating multiplication for uint8
func FuzzSaturatingMulU8(f *testing.F) {
	f.Add(uint8(0), uint8(0))
	f.Add(uint8(255), uint8(1))
	f.Add(uint8(16), uint8(16))
	f.Add(uint8(255), uint8(255))

	f.Fuzz(func(t *testing.T, a, b uint8) {
		result := SaturatingMulU8(a, b)

		// Calculate expected result
		expected := uint16(a) * uint16(b)

		if expected > math.MaxUint8 {
			// Should saturate at max
			if result != math.MaxUint8 {
				t.Errorf("SaturatingMulU8(%d, %d) = %d, want %d (saturated)", a, b, result, math.MaxUint8)
			}
		} else {
			// Normal result
			if result != uint8(expected) {
				t.Errorf("SaturatingMulU8(%d, %d) = %d, want %d", a, b, result, uint8(expected))
			}
		}
	})
}

// FuzzSaturatingSubU8 tests saturating subtraction for uint8
func FuzzSaturatingSubU8(f *testing.F) {
	f.Add(uint8(0), uint8(0))
	f.Add(uint8(0), uint8(1))
	f.Add(uint8(100), uint8(50))
	f.Add(uint8(50), uint8(100))

	f.Fuzz(func(t *testing.T, a, b uint8) {
		result := SaturatingSubU8(a, b)

		if a >= b {
			// Normal subtraction
			if result != a-b {
				t.Errorf("SaturatingSubU8(%d, %d) = %d, want %d", a, b, result, a-b)
			}
		} else {
			// Should saturate at zero
			if result != 0 {
				t.Errorf("SaturatingSubU8(%d, %d) = %d, want 0 (saturated)", a, b, result)
			}
		}
	})
}

// FuzzSaturatingAddU32 tests saturating addition for uint32
func FuzzSaturatingAddU32(f *testing.F) {
	f.Add(uint32(0), uint32(0))
	f.Add(uint32(math.MaxUint32), uint32(1))
	f.Add(uint32(1<<31), uint32(1<<31))
	f.Add(uint32(1000000), uint32(2000000))

	f.Fuzz(func(t *testing.T, a, b uint32) {
		result := SaturatingAddU32(a, b)

		// Calculate expected result
		expected := uint64(a) + uint64(b)

		if expected > math.MaxUint32 {
			// Should saturate at max
			if result != math.MaxUint32 {
				t.Errorf("SaturatingAddU32(%d, %d) = %d, want %d (saturated)", a, b, result, math.MaxUint32)
			}
		} else {
			// Normal result
			if result != uint32(expected) {
				t.Errorf("SaturatingAddU32(%d, %d) = %d, want %d", a, b, result, uint32(expected))
			}
		}
	})
}

// FuzzSaturatingMulU32 tests saturating multiplication for uint32
func FuzzSaturatingMulU32(f *testing.F) {
	f.Add(uint32(0), uint32(0))
	f.Add(uint32(math.MaxUint32), uint32(1))
	f.Add(uint32(65536), uint32(65536))
	f.Add(uint32(100000), uint32(100000))

	f.Fuzz(func(t *testing.T, a, b uint32) {
		result := SaturatingMulU32(a, b)

		// Calculate expected result
		expected := uint64(a) * uint64(b)

		if expected > math.MaxUint32 {
			// Should saturate at max
			if result != math.MaxUint32 {
				t.Errorf("SaturatingMulU32(%d, %d) = %d, want %d (saturated)", a, b, result, math.MaxUint32)
			}
		} else {
			// Normal result
			if result != uint32(expected) {
				t.Errorf("SaturatingMulU32(%d, %d) = %d, want %d", a, b, result, uint32(expected))
			}
		}
	})
}

// FuzzSaturatingSubU32 tests saturating subtraction for uint32
func FuzzSaturatingSubU32(f *testing.F) {
	f.Add(uint32(0), uint32(0))
	f.Add(uint32(0), uint32(1))
	f.Add(uint32(1000000), uint32(500000))
	f.Add(uint32(500000), uint32(1000000))

	f.Fuzz(func(t *testing.T, a, b uint32) {
		result := SaturatingSubU32(a, b)

		if a >= b {
			// Normal subtraction
			if result != a-b {
				t.Errorf("SaturatingSubU32(%d, %d) = %d, want %d", a, b, result, a-b)
			}
		} else {
			// Should saturate at zero
			if result != 0 {
				t.Errorf("SaturatingSubU32(%d, %d) = %d, want 0 (saturated)", a, b, result)
			}
		}
	})
}

// FuzzSaturatingPow tests saturating power operation
func FuzzSaturatingPow(f *testing.F) {
	f.Add(uint64(0), uint32(0))
	f.Add(uint64(2), uint32(0))
	f.Add(uint64(2), uint32(1))
	f.Add(uint64(2), uint32(63))
	f.Add(uint64(2), uint32(64))
	f.Add(uint64(10), uint32(10))

	f.Fuzz(func(t *testing.T, n uint64, m uint32) {
		// Limit exponent to prevent excessive computation
		if m > 100 {
			t.Skip("Exponent too large")
		}

		result := SaturatingPow(n, m)

		// Special cases
		if m == 0 {
			if result != 1 {
				t.Errorf("SaturatingPow(%d, 0) = %d, want 1", n, result)
			}
			return
		}

		if m == 1 {
			if result != n {
				t.Errorf("SaturatingPow(%d, 1) = %d, want %d", n, result, n)
			}
			return
		}

		if n == 0 {
			if result != 0 {
				t.Errorf("SaturatingPow(0, %d) = %d, want 0", m, result)
			}
			return
		}

		if n == 1 {
			if result != 1 {
				t.Errorf("SaturatingPow(1, %d) = %d, want 1", m, result)
			}
			return
		}

		// For small enough values, verify exact result
		// Otherwise just check it doesn't panic and saturates properly
		if result == math.MaxUint64 {
			// Saturated - verify this was necessary
			// Simple overflow check: if n^m would overflow, result should be MaxUint64
			var testResult uint64 = 1
			for i := uint32(0); i < m; i++ {
				if testResult > math.MaxUint64/n {
					// Would overflow, saturation is correct
					return
				}
				testResult *= n
			}
			// If we get here without overflow, saturation was wrong
			t.Errorf("SaturatingPow(%d, %d) saturated unnecessarily, actual result would be %d", n, m, testResult)
		}
	})
}

// FuzzCheckedAddU128 tests CheckedAddU128 for overflow detection
func FuzzCheckedAddU128(f *testing.F) {
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0))
	f.Add(uint64(math.MaxUint64), uint64(math.MaxUint64), uint64(0), uint64(0))
	f.Add(uint64(math.MaxUint64), uint64(math.MaxUint64), uint64(0), uint64(1))
	f.Add(uint64(1<<63), uint64(0), uint64(1<<63), uint64(0))

	f.Fuzz(func(t *testing.T, aHi, aLo, bHi, bLo uint64) {
		a := wide.NewUint128(aLo, aHi)
		b := wide.NewUint128(bLo, bHi)

		result, err := CheckedAddU128(a, b)

		// Check if overflow should occur
		// For uint128, overflow occurs when result < a (wraps around)
		expectedResult := a.Add(b)

		if expectedResult.Cmp(a) == -1 {
			// Overflow occurred
			if err == nil {
				t.Errorf("CheckedAddU128 should detect overflow")
			}
		} else {
			// No overflow
			if err != nil {
				t.Errorf("CheckedAddU128 returned error but should succeed")
			}
			if result.Cmp(expectedResult) != 0 {
				t.Errorf("CheckedAddU128 returned incorrect result")
			}
		}
	})
}

// FuzzCheckedMulU128 tests CheckedMulU128 for overflow detection
func FuzzCheckedMulU128(f *testing.F) {
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0))
	f.Add(uint64(math.MaxUint64), uint64(math.MaxUint64), uint64(0), uint64(1))
	f.Add(uint64(1<<32), uint64(0), uint64(1<<32), uint64(0))
	f.Add(uint64(1000000), uint64(0), uint64(1000000), uint64(0))

	f.Fuzz(func(t *testing.T, aHi, aLo, bHi, bLo uint64) {
		a := wide.NewUint128(aLo, aHi)
		b := wide.NewUint128(bLo, bHi)
		zero := wide.Uint128FromUint64(0)

		result, err := CheckedMulU128(a, b)

		// Special case: if either is zero, result should be zero with no error
		if a.Cmp(zero) == 0 || b.Cmp(zero) == 0 {
			if err != nil {
				t.Errorf("CheckedMulU128 with zero operand should not error")
			}
			if result.Cmp(zero) != 0 {
				t.Errorf("CheckedMulU128 with zero operand should return zero")
			}
			return
		}

		// Calculate expected result
		expectedResult := a.Mul(b)

		// Check for overflow by dividing back: if result/a != b, overflow occurred
		quotient := expectedResult.Div(a)
		shouldOverflow := quotient.Cmp(b) != 0

		if shouldOverflow {
			// Overflow should be detected
			if err == nil {
				t.Errorf("CheckedMulU128 should detect overflow")
			}
		} else {
			// No overflow
			if err != nil {
				t.Errorf("CheckedMulU128 returned error but should succeed")
			}
			if result.Cmp(expectedResult) != 0 {
				t.Errorf("CheckedMulU128 returned incorrect result")
			}
		}
	})
}

// FuzzCheckedDivU128 tests CheckedDivU128 for division by zero
func FuzzCheckedDivU128(f *testing.F) {
	f.Add(uint64(0), uint64(0), uint64(0), uint64(1))
	f.Add(uint64(math.MaxUint64), uint64(math.MaxUint64), uint64(0), uint64(1))
	f.Add(uint64(1000), uint64(0), uint64(0), uint64(0))
	f.Add(uint64(1000), uint64(0), uint64(3), uint64(0))

	f.Fuzz(func(t *testing.T, aHi, aLo, bHi, bLo uint64) {
		a := wide.NewUint128(aLo, aHi)
		b := wide.NewUint128(bLo, bHi)

		result, err := CheckedDivU128(a, b)

		if b.Cmp(wide.Uint128FromUint64(0)) == 0 {
			// Division by zero
			if err == nil {
				t.Errorf("CheckedDivU128 should detect division by zero")
			}
		} else {
			// Valid division
			if err != nil {
				t.Errorf("CheckedDivU128 returned error but should succeed")
			}
			expectedResult := a.Div(b)
			if result.Cmp(expectedResult) != 0 {
				t.Errorf("CheckedDivU128 returned incorrect result")
			}
		}
	})
}
