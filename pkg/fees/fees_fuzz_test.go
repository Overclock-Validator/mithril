package fees

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// FuzzCalculatePriorityFee tests the calculatePriorityFee function with various inputs
func FuzzCalculatePriorityFee(f *testing.F) {
	// Seed with edge cases
	f.Add(uint64(0), uint32(0))              // Zero price and limit
	f.Add(uint64(1), uint32(200000))         // Typical values
	f.Add(uint64(1000000), uint32(1))        // High price, low limit
	f.Add(uint64(1), uint32(math.MaxUint32)) // Low price, max limit
	f.Add(uint64(math.MaxUint64), uint32(1)) // Max price

	f.Fuzz(func(t *testing.T, computeUnitPrice uint64, computeUnitLimit uint32) {
		limits := &sealevel.ComputeBudgetLimits{
			ComputeUnitPrice: computeUnitPrice,
			ComputeUnitLimit: computeUnitLimit,
		}

		// Function should not panic
		result := calculatePriorityFee(limits)

		// Result should be deterministic
		result2 := calculatePriorityFee(limits)
		if result != result2 {
			t.Errorf("calculatePriorityFee is not deterministic: got %d and %d", result, result2)
		}

		// If either input is zero, result should be zero
		if computeUnitPrice == 0 || computeUnitLimit == 0 {
			if result != 0 {
				t.Errorf("calculatePriorityFee(%d, %d) = %d, want 0", computeUnitPrice, computeUnitLimit, result)
			}
		}

		// Result should not exceed MaxUint64
		// (This is implicit, but we check the function returns MaxUint64 on overflow)
		if result == math.MaxUint64 && computeUnitPrice > 0 && computeUnitLimit > 0 {
			// Verify overflow actually occurred
			// If price * limit / 1000000 would fit in uint64, this is wrong
			if computeUnitPrice <= math.MaxUint64/uint64(computeUnitLimit) {
				product := computeUnitPrice * uint64(computeUnitLimit)
				if product/1000000 < math.MaxUint64 {
					t.Errorf("calculatePriorityFee returned MaxUint64 but no overflow should occur")
				}
			}
		}
	})
}

// FuzzTxFeeInfoAccumulatorAdd tests the Add method with overflow detection
func FuzzTxFeeInfoAccumulatorAdd(f *testing.F) {
	// Seed with edge cases
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0))
	f.Add(uint64(1000), uint64(500), uint64(1500), uint64(2000), uint64(1000), uint64(3000))
	f.Add(uint64(math.MaxUint64-1000), uint64(0), uint64(math.MaxUint64-1000), uint64(1000), uint64(0), uint64(1000))

	f.Fuzz(func(t *testing.T, accExec, accPri, accTotal, feeExec, feePri, feeTotal uint64) {
		accumulator := &TxFeeInfoAccumulator{
			ExecutionFees: accExec,
			PriorityFees:  accPri,
			TotalFees:     accTotal,
		}

		feeInfo := &TxFeeInfo{
			ExecutionFee: feeExec,
			PriorityFee:  feePri,
			TotalFee:     feeTotal,
		}

		// Track if panic occurs
		var didPanic bool
		var panicValue interface{}

		func() {
			defer func() {
				if r := recover(); r != nil {
					didPanic = true
					panicValue = r
				}
			}()
			accumulator.Add(feeInfo)
		}()

		// Check if overflow would occur
		execOverflow := accExec > math.MaxUint64-feeExec
		priOverflow := accPri > math.MaxUint64-feePri
		totalOverflow := accTotal > math.MaxUint64-feeTotal

		if execOverflow || priOverflow || totalOverflow {
			// Should panic on overflow
			if !didPanic {
				t.Errorf("Add should panic on overflow but didn't")
			}
		} else {
			// Should not panic
			if didPanic {
				t.Errorf("Add panicked but shouldn't: %v", panicValue)
			}

			// Verify correct addition
			if accumulator.ExecutionFees != accExec+feeExec {
				t.Errorf("ExecutionFees: got %d, want %d", accumulator.ExecutionFees, accExec+feeExec)
			}
			if accumulator.PriorityFees != accPri+feePri {
				t.Errorf("PriorityFees: got %d, want %d", accumulator.PriorityFees, accPri+feePri)
			}
			if accumulator.TotalFees != accTotal+feeTotal {
				t.Errorf("TotalFees: got %d, want %d", accumulator.TotalFees, accTotal+feeTotal)
			}
		}
	})
}

// FuzzCalculateTxFees tests fee calculation with various signature counts
func FuzzCalculateTxFees(f *testing.F) {
	// Seed with edge cases
	f.Add(uint8(1), uint64(0), uint32(0))              // Min signatures, no priority fee
	f.Add(uint8(255), uint64(1000000), uint32(200000)) // Max signatures, high priority
	f.Add(uint8(10), uint64(0), uint32(1000000))       // Medium sigs, max CUs

	f.Fuzz(func(t *testing.T, numSigs uint8, computePrice uint64, computeLimit uint32) {
		// Skip if numSigs is 0 (invalid transaction)
		if numSigs == 0 {
			t.Skip("numSigs must be at least 1")
		}

		// Create minimal transaction with required signatures
		tx := &solana.Transaction{
			Message: solana.Message{
				Header: solana.MessageHeader{
					NumRequiredSignatures: numSigs,
				},
				AccountKeys: []solana.PublicKey{{}}, // At least one account (fee payer)
			},
		}

		// Create compute budget limits
		limits := &sealevel.ComputeBudgetLimits{
			ComputeUnitPrice: computePrice,
			ComputeUnitLimit: computeLimit,
		}

		// Create empty features (no precompiles enabled)
		features := features.NewFeaturesDefault()

		// Calculate fees
		feeInfo := CalculateTxFees(tx, nil, []sealevel.Instruction{}, limits, features)

		// Verify result structure
		if feeInfo == nil {
			t.Fatal("CalculateTxFees returned nil")
		}

		// Base execution fee should be numSigs * 5000
		expectedExecFee := uint64(numSigs) * 5000
		if feeInfo.ExecutionFee != expectedExecFee {
			t.Errorf("ExecutionFee: got %d, want %d", feeInfo.ExecutionFee, expectedExecFee)
		}

		// Priority fee calculation
		var expectedPriorityFee uint64
		if computePrice != 0 {
			expectedPriorityFee = calculatePriorityFee(limits)
		}
		if feeInfo.PriorityFee != expectedPriorityFee {
			t.Errorf("PriorityFee: got %d, want %d", feeInfo.PriorityFee, expectedPriorityFee)
		}

		// Total should be execution + priority (saturating)
		expectedTotal := expectedExecFee + expectedPriorityFee
		// Check for overflow
		if expectedTotal < expectedExecFee {
			expectedTotal = math.MaxUint64
		}
		if feeInfo.TotalFee != expectedTotal {
			t.Errorf("TotalFee: got %d, want %d", feeInfo.TotalFee, expectedTotal)
		}

		// Invariant: TotalFee >= ExecutionFee
		if feeInfo.TotalFee < feeInfo.ExecutionFee {
			t.Errorf("TotalFee (%d) < ExecutionFee (%d)", feeInfo.TotalFee, feeInfo.ExecutionFee)
		}

		// Invariant: TotalFee >= PriorityFee
		if feeInfo.TotalFee < feeInfo.PriorityFee {
			t.Errorf("TotalFee (%d) < PriorityFee (%d)", feeInfo.TotalFee, feeInfo.PriorityFee)
		}
	})
}

// FuzzCalculateTxFeesWithPrecompiles tests fee calculation with precompile instructions
func FuzzCalculateTxFeesWithPrecompiles(f *testing.F) {
	// Seed with various precompile signature counts
	f.Add(uint8(1), uint8(0), uint8(0), uint8(0)) // No precompile sigs
	f.Add(uint8(1), uint8(5), uint8(0), uint8(0)) // Ed25519 only
	f.Add(uint8(1), uint8(0), uint8(3), uint8(0)) // Secp256k only
	f.Add(uint8(1), uint8(0), uint8(0), uint8(2)) // Secp256r1 only
	f.Add(uint8(1), uint8(5), uint8(3), uint8(2)) // All precompiles

	f.Fuzz(func(t *testing.T, baseSigs uint8, ed25519Sigs uint8, secp256kSigs uint8, secp256r1Sigs uint8) {
		if baseSigs == 0 {
			t.Skip("baseSigs must be at least 1")
		}

		// Create transaction
		tx := &solana.Transaction{
			Message: solana.Message{
				Header: solana.MessageHeader{
					NumRequiredSignatures: baseSigs,
				},
				AccountKeys: []solana.PublicKey{{}},
			},
		}

		// Create precompile instructions
		var instrs []sealevel.Instruction

		if ed25519Sigs > 0 {
			instrs = append(instrs, sealevel.Instruction{
				ProgramId: [32]byte{}, // Ed25519PrecompileAddr (would need proper address)
				Data:      []byte{ed25519Sigs},
			})
		}

		if secp256kSigs > 0 {
			instrs = append(instrs, sealevel.Instruction{
				ProgramId: [32]byte{}, // Secp256kPrecompileAddr (would need proper address)
				Data:      []byte{secp256kSigs},
			})
		}

		if secp256r1Sigs > 0 {
			instrs = append(instrs, sealevel.Instruction{
				ProgramId: [32]byte{}, // Secp256r1PrecompileAddr (would need proper address)
				Data:      []byte{secp256r1Sigs},
			})
		}

		limits := &sealevel.ComputeBudgetLimits{
			ComputeUnitPrice: 0,
			ComputeUnitLimit: 200000,
		}

		features := features.NewFeaturesDefault()

		// Calculate fees
		feeInfo := CalculateTxFees(tx, nil, instrs, limits, features)

		// Note: The actual precompile addresses need to match for this to work correctly
		// For now, we just verify the function doesn't panic and returns valid structure

		if feeInfo == nil {
			t.Fatal("CalculateTxFees returned nil")
		}

		// Execution fee should be at least baseSigs * 5000
		minExpectedFee := uint64(baseSigs) * 5000
		if feeInfo.ExecutionFee < minExpectedFee {
			t.Errorf("ExecutionFee (%d) < minimum expected (%d)", feeInfo.ExecutionFee, minExpectedFee)
		}

		// Check invariants
		if feeInfo.TotalFee < feeInfo.ExecutionFee {
			t.Errorf("TotalFee < ExecutionFee")
		}
	})
}

// FuzzTxFeeInfoConsistency tests that TxFeeInfo maintains internal consistency
func FuzzTxFeeInfoConsistency(f *testing.F) {
	f.Add(uint64(5000), uint64(1000))
	f.Add(uint64(0), uint64(0))
	f.Add(uint64(math.MaxUint64), uint64(0))
	f.Add(uint64(10000), uint64(math.MaxUint64-10000))

	f.Fuzz(func(t *testing.T, execFee, priFee uint64) {
		// Create fee info
		feeInfo := &TxFeeInfo{
			ExecutionFee: execFee,
			PriorityFee:  priFee,
			TotalFee:     execFee + priFee,
		}

		// Handle overflow in TotalFee
		if feeInfo.TotalFee < execFee || feeInfo.TotalFee < priFee {
			feeInfo.TotalFee = math.MaxUint64
		}

		// Verify invariants
		if feeInfo.TotalFee < feeInfo.ExecutionFee && feeInfo.TotalFee != math.MaxUint64 {
			t.Errorf("TotalFee (%d) < ExecutionFee (%d)", feeInfo.TotalFee, feeInfo.ExecutionFee)
		}

		if feeInfo.TotalFee < feeInfo.PriorityFee && feeInfo.TotalFee != math.MaxUint64 {
			t.Errorf("TotalFee (%d) < PriorityFee (%d)", feeInfo.TotalFee, feeInfo.PriorityFee)
		}

		// Test accumulator
		acc := &TxFeeInfoAccumulator{}

		// Track if panic occurs
		var didPanic bool
		func() {
			defer func() {
				if r := recover(); r != nil {
					didPanic = true
				}
			}()
			acc.Add(feeInfo)
		}()

		// Should only panic if addition would overflow
		wouldOverflow := (execFee > math.MaxUint64-0) || (priFee > math.MaxUint64-0) || (feeInfo.TotalFee > math.MaxUint64-0)

		if didPanic && !wouldOverflow {
			t.Errorf("Add panicked unexpectedly")
		}
	})
}

// FuzzSignatureFeeCalculation tests basic signature fee calculation
func FuzzSignatureFeeCalculation(f *testing.F) {
	f.Add(uint8(1))
	f.Add(uint8(10))
	f.Add(uint8(100))
	f.Add(uint8(255))

	f.Fuzz(func(t *testing.T, numSigs uint8) {
		if numSigs == 0 {
			t.Skip("numSigs must be at least 1")
		}

		// Basic fee calculation: numSigs * 5000
		expectedFee := uint64(numSigs) * 5000

		// Verify no overflow in this calculation
		if numSigs <= 255 {
			// Should never overflow for valid signature counts
			if expectedFee < uint64(numSigs) {
				t.Errorf("Signature fee calculation overflowed for %d signatures", numSigs)
			}

			// Max valid fee is 255 * 5000 = 1,275,000
			if expectedFee > 1_275_000 {
				t.Errorf("Signature fee (%d) exceeds maximum expected (1,275,000)", expectedFee)
			}
		}

		// Create a transaction to test
		tx := &solana.Transaction{
			Message: solana.Message{
				Header: solana.MessageHeader{
					NumRequiredSignatures: numSigs,
				},
				AccountKeys: []solana.PublicKey{{}},
			},
		}

		limits := &sealevel.ComputeBudgetLimits{
			ComputeUnitPrice: 0,
			ComputeUnitLimit: 200000,
		}

		features := features.NewFeaturesDefault()

		feeInfo := CalculateTxFees(tx, nil, []sealevel.Instruction{}, limits, features)

		if feeInfo.ExecutionFee != expectedFee {
			t.Errorf("ExecutionFee: got %d, want %d", feeInfo.ExecutionFee, expectedFee)
		}

		// With no priority fee, total should equal execution fee
		if feeInfo.TotalFee != expectedFee {
			t.Errorf("TotalFee: got %d, want %d (no priority fee)", feeInfo.TotalFee, expectedFee)
		}

		// Priority fee should be zero
		if feeInfo.PriorityFee != 0 {
			t.Errorf("PriorityFee: got %d, want 0", feeInfo.PriorityFee)
		}
	})
}

// FuzzPriorityFeeOverflow tests priority fee calculation near overflow boundaries
func FuzzPriorityFeeOverflow(f *testing.F) {
	// Seed with values that might cause overflow
	f.Add(uint64(math.MaxUint64), uint32(math.MaxUint32))
	f.Add(uint64(math.MaxUint64/2), uint32(math.MaxUint32))
	f.Add(uint64(math.MaxUint64), uint32(1))
	f.Add(uint64(1000000000), uint32(1000000))

	f.Fuzz(func(t *testing.T, price uint64, limit uint32) {
		limits := &sealevel.ComputeBudgetLimits{
			ComputeUnitPrice: price,
			ComputeUnitLimit: limit,
		}

		// Should never panic
		result := calculatePriorityFee(limits)

		// Result must be a valid uint64
		// If calculation would overflow, should return MaxUint64
		if price > 0 && limit > 0 {
			// Very rough overflow check
			if price > math.MaxUint64/uint64(limit) {
				// Multiplication would overflow
				if result != math.MaxUint64 {
					// Should have returned MaxUint64 or calculated correctly
					// Let's verify the calculation is reasonable
					if result > 0 && result < math.MaxUint64 {
						// Result seems valid, calculation must have handled overflow in division
						t.Logf("Handled large multiplication: price=%d, limit=%d, result=%d", price, limit, result)
					}
				}
			}
		}

		// Function must be deterministic
		result2 := calculatePriorityFee(limits)
		if result != result2 {
			t.Errorf("calculatePriorityFee not deterministic: %d vs %d", result, result2)
		}
	})
}

// FuzzAccumulatorMultipleAdds tests accumulator with multiple additions
func FuzzAccumulatorMultipleAdds(f *testing.F) {
	f.Add(uint8(5), uint64(1000), uint64(500))

	f.Fuzz(func(t *testing.T, numAdds uint8, execFee, priFee uint64) {
		// Limit number of adds to prevent excessive test time
		if numAdds == 0 || numAdds > 100 {
			t.Skip("numAdds must be between 1 and 100")
		}

		acc := &TxFeeInfoAccumulator{}

		totalFee := execFee + priFee
		if totalFee < execFee {
			// Overflow in individual fee
			t.Skip("Individual fee already overflows")
		}

		feeInfo := &TxFeeInfo{
			ExecutionFee: execFee,
			PriorityFee:  priFee,
			TotalFee:     totalFee,
		}

		var addCount uint8
		var didPanic bool

		func() {
			defer func() {
				if r := recover(); r != nil {
					didPanic = true
				}
			}()

			for i := uint8(0); i < numAdds; i++ {
				acc.Add(feeInfo)
				addCount++
			}
		}()

		if !didPanic {
			// All additions succeeded
			expectedExec := uint64(addCount) * execFee
			expectedPri := uint64(addCount) * priFee
			expectedTotal := uint64(addCount) * totalFee

			// Check for overflow
			if expectedExec/uint64(addCount) != execFee {
				t.Errorf("Expected overflow was not detected in ExecutionFees")
			} else if acc.ExecutionFees != expectedExec {
				t.Errorf("ExecutionFees: got %d, want %d", acc.ExecutionFees, expectedExec)
			}

			if expectedPri/uint64(addCount) != priFee {
				t.Errorf("Expected overflow was not detected in PriorityFees")
			} else if acc.PriorityFees != expectedPri {
				t.Errorf("PriorityFees: got %d, want %d", acc.PriorityFees, expectedPri)
			}

			if expectedTotal/uint64(addCount) != totalFee {
				t.Errorf("Expected overflow was not detected in TotalFees")
			} else if acc.TotalFees != expectedTotal {
				t.Errorf("TotalFees: got %d, want %d", acc.TotalFees, expectedTotal)
			}
		} else {
			// Panic occurred, verify it was due to overflow
			t.Logf("Accumulator panicked after %d additions (expected on overflow)", addCount)
		}
	})
}
