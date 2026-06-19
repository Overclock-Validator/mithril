package cu

import (
	"testing"
)

// FuzzComputeMeterConsume tests compute unit consumption with various costs
func FuzzComputeMeterConsume(f *testing.F) {
	// Seed with edge cases
	f.Add(uint64(100000), uint64(0))      // No consumption
	f.Add(uint64(100000), uint64(50000))  // Half budget
	f.Add(uint64(100000), uint64(100000)) // Exact budget
	f.Add(uint64(100000), uint64(100001)) // Exceed by 1
	f.Add(uint64(100000), uint64(200000)) // Double budget
	f.Add(uint64(0), uint64(1))           // Zero budget
	f.Add(uint64(1), uint64(1))           // Minimal budget

	f.Fuzz(func(t *testing.T, budget uint64, cost uint64) {
		cm := NewComputeMeter(budget)

		// Initial state checks
		if cm.Remaining() != budget {
			t.Errorf("Initial remaining should equal budget: got %d, expected %d",
				cm.Remaining(), budget)
		}
		if cm.Used() != 0 {
			t.Errorf("Initial used should be 0: got %d", cm.Used())
		}

		// Consume and check result
		err := cm.Consume(cost)

		if cost <= budget {
			// Should succeed
			if err != nil {
				t.Errorf("Consume(%d) with budget %d should succeed, got error: %v",
					cost, budget, err)
			}

			// Check remaining units
			expectedRemaining := budget - cost
			if cm.Remaining() != expectedRemaining {
				t.Errorf("After consuming %d from %d budget, remaining should be %d, got %d",
					cost, budget, expectedRemaining, cm.Remaining())
			}

			// Check used units
			if cm.Used() != cost {
				t.Errorf("After consuming %d, used should be %d, got %d",
					cost, cost, cm.Used())
			}
		} else {
			// Should fail with ErrComputeExceeded
			if err != ErrComputeExceeded {
				t.Errorf("Consume(%d) with budget %d should return ErrComputeExceeded, got: %v",
					cost, budget, err)
			}

			// Meter should be zeroed on exceeded
			if cm.Remaining() != 0 {
				t.Errorf("After exceeding budget, remaining should be 0, got %d",
					cm.Remaining())
			}
		}

		// Invariant: Used + Remaining should equal starting balance (unless exceeded)
		if err == nil && cm.Used()+cm.Remaining() != budget {
			t.Errorf("Invariant violated: Used(%d) + Remaining(%d) != Budget(%d)",
				cm.Used(), cm.Remaining(), budget)
		}
	})
}

// FuzzComputeMeterMultipleConsume tests sequential consumption patterns
func FuzzComputeMeterMultipleConsume(f *testing.F) {
	f.Add(uint64(100000), uint64(1000), uint64(2000), uint64(3000))
	f.Add(uint64(10), uint64(5), uint64(5), uint64(5))
	f.Add(uint64(1000), uint64(100), uint64(900), uint64(100))

	f.Fuzz(func(t *testing.T, budget uint64, cost1 uint64, cost2 uint64, cost3 uint64) {
		cm := NewComputeMeter(budget)

		totalCost := uint64(0)
		costs := []uint64{cost1, cost2, cost3}

		for i, cost := range costs {
			err := cm.Consume(cost)

			if totalCost+cost <= budget {
				// Should succeed
				if err != nil {
					t.Errorf("Consume #%d (%d) should succeed with remaining budget, got error: %v",
						i+1, cost, err)
					break
				}
				totalCost += cost

				// Check invariants
				if cm.Used() != totalCost {
					t.Errorf("After %d consumptions totaling %d, used should be %d, got %d",
						i+1, totalCost, totalCost, cm.Used())
				}

				expectedRemaining := budget - totalCost
				if cm.Remaining() != expectedRemaining {
					t.Errorf("After consuming %d from %d budget, remaining should be %d, got %d",
						totalCost, budget, expectedRemaining, cm.Remaining())
				}
			} else {
				// Should fail
				if err != ErrComputeExceeded {
					t.Errorf("Consume #%d (%d) should exceed budget, got: %v",
						i+1, cost, err)
				}
				// After exceeding, meter should be 0
				if cm.Remaining() != 0 {
					t.Errorf("After exceeding budget, remaining should be 0, got %d",
						cm.Remaining())
				}
				break
			}
		}
	})
}

// FuzzComputeMeterDisable tests disabled meter behavior
func FuzzComputeMeterDisable(f *testing.F) {
	f.Add(uint64(1000), uint64(5000)) // Cost exceeds budget
	f.Add(uint64(100), uint64(50))    // Normal case
	f.Add(uint64(0), uint64(1000))    // Zero budget
	f.Add(uint64(1), uint64(1000000)) // Huge cost

	f.Fuzz(func(t *testing.T, budget uint64, cost uint64) {
		cm := NewComputeMeter(budget)

		// Disable the meter
		cm.Disable()

		// Consume should always succeed when disabled
		err := cm.Consume(cost)
		if err != nil {
			t.Errorf("Consume should succeed when disabled, got error: %v", err)
		}

		// Meter values should not change when disabled
		if cm.Remaining() != budget {
			t.Errorf("Disabled meter remaining should stay at %d, got %d",
				budget, cm.Remaining())
		}

		if cm.Used() != 0 {
			t.Errorf("Disabled meter used should stay at 0, got %d", cm.Used())
		}

		// Re-enable and verify normal behavior resumes
		cm.Enable()

		err = cm.Consume(cost)
		if cost <= budget {
			if err != nil {
				t.Errorf("After re-enabling, consume(%d) with budget %d should succeed, got: %v",
					cost, budget, err)
			}
		} else {
			if err != ErrComputeExceeded {
				t.Errorf("After re-enabling, consume(%d) with budget %d should fail, got: %v",
					cost, budget, err)
			}
		}
	})
}

// FuzzComputeMeterBoundaryConditions tests edge cases
func FuzzComputeMeterBoundaryConditions(f *testing.F) {
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(18446744073709551615)) // uint64 max

	f.Fuzz(func(t *testing.T, budget uint64) {
		cm := NewComputeMeter(budget)

		// Test consuming 0
		err := cm.Consume(0)
		if err != nil {
			t.Errorf("Consuming 0 should always succeed, got error: %v", err)
		}
		if cm.Remaining() != budget {
			t.Errorf("After consuming 0, remaining should still be %d, got %d",
				budget, cm.Remaining())
		}

		// Reset meter
		cm = NewComputeMeter(budget)

		// Test consuming exactly the budget
		err = cm.Consume(budget)
		if err != nil {
			t.Errorf("Consuming exact budget should succeed, got error: %v", err)
		}
		if cm.Remaining() != 0 {
			t.Errorf("After consuming exact budget, remaining should be 0, got %d",
				cm.Remaining())
		}
		if cm.Used() != budget {
			t.Errorf("After consuming exact budget, used should be %d, got %d",
				budget, cm.Used())
		}

		// Try consuming more from exhausted meter
		err = cm.Consume(1)
		if err != ErrComputeExceeded {
			t.Errorf("Consuming from exhausted meter should fail, got: %v", err)
		}
	})
}

// FuzzComputeMeterDefaultBudget tests the default meter creation
func FuzzComputeMeterDefaultBudget(f *testing.F) {
	f.Add(uint64(10000))
	f.Add(uint64(200000))
	f.Add(uint64(400000))

	f.Fuzz(func(t *testing.T, cost uint64) {
		cm := NewComputeMeterDefault()

		// Default budget should be 200,000
		expectedBudget := uint64(200000)
		if cm.Remaining() != expectedBudget {
			t.Errorf("Default meter should have budget %d, got %d",
				expectedBudget, cm.Remaining())
		}

		err := cm.Consume(cost)

		if cost <= expectedBudget {
			if err != nil {
				t.Errorf("Consuming %d from default budget should succeed, got: %v",
					cost, err)
			}
			if cm.Remaining() != expectedBudget-cost {
				t.Errorf("After consuming %d, remaining should be %d, got %d",
					cost, expectedBudget-cost, cm.Remaining())
			}
		} else {
			if err != ErrComputeExceeded {
				t.Errorf("Consuming %d (exceeds default budget) should fail, got: %v",
					cost, err)
			}
		}
	})
}

// FuzzComputeMeterStateConsistency tests state consistency across operations
func FuzzComputeMeterStateConsistency(f *testing.F) {
	f.Add(uint64(50000), uint8(10))

	f.Fuzz(func(t *testing.T, budget uint64, numOps uint8) {
		if numOps == 0 {
			t.Skip("Need at least one operation")
		}

		cm := NewComputeMeter(budget)
		originalBudget := budget

		// Perform random sequence of small consumptions
		totalConsumed := uint64(0)
		// Convert to uint64 BEFORE adding to avoid uint8 overflow
		// (numOps=255 + 1 would wrap to 0 in uint8 arithmetic)
		divisor := uint64(numOps) + 1
		if divisor == 0 {
			// Should never happen after fix, but safeguard
			t.Skip("Invalid divisor")
		}
		costPerOp := budget / divisor // Ensure we don't exceed immediately

		for i := uint8(0); i < numOps; i++ {
			err := cm.Consume(costPerOp)

			if totalConsumed+costPerOp <= originalBudget {
				// Should succeed
				if err != nil {
					t.Errorf("Operation %d: consume(%d) should succeed, got: %v",
						i, costPerOp, err)
					break
				}
				totalConsumed += costPerOp

				// Verify state invariants at each step
				if cm.Used() != totalConsumed {
					t.Errorf("Operation %d: used should be %d, got %d",
						i, totalConsumed, cm.Used())
				}

				expectedRemaining := originalBudget - totalConsumed
				if cm.Remaining() != expectedRemaining {
					t.Errorf("Operation %d: remaining should be %d, got %d",
						i, expectedRemaining, cm.Remaining())
				}

				// Invariant check
				if cm.Used()+cm.Remaining() != originalBudget {
					t.Errorf("Operation %d: used(%d) + remaining(%d) != budget(%d)",
						i, cm.Used(), cm.Remaining(), originalBudget)
				}
			}
		}
	})
}

// FuzzComputeMeterOverflowProtection tests protection against arithmetic overflow
func FuzzComputeMeterOverflowProtection(f *testing.F) {
	f.Add(uint64(18446744073709551615), uint64(18446744073709551615)) // uint64 max

	f.Fuzz(func(t *testing.T, budget uint64, cost uint64) {
		cm := NewComputeMeter(budget)

		// This should not panic or cause overflow
		err := cm.Consume(cost)

		// Should either succeed or return proper error
		if err != nil && err != ErrComputeExceeded {
			t.Errorf("Consume should return nil or ErrComputeExceeded, got: %v", err)
		}

		// State should always be valid
		if cm.Used() > budget {
			t.Errorf("Used(%d) should never exceed budget(%d)", cm.Used(), budget)
		}

		if cm.Remaining() > budget {
			t.Errorf("Remaining(%d) should never exceed budget(%d)",
				cm.Remaining(), budget)
		}
	})
}
