package sealevel

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
)

// FuzzInstructionAccountIndexing tests account index resolution
func FuzzInstructionAccountIndexing(f *testing.F) {
	f.Add(uint64(0), uint8(5))
	f.Add(uint64(10), uint8(10))
	f.Add(uint64(100), uint8(1))

	f.Fuzz(func(t *testing.T, index uint64, numAccts uint8) {
		if numAccts > 50 {
			numAccts = numAccts % 50
		}
		if numAccts == 0 {
			numAccts = 1
		}

		instrCtx := &InstructionCtx{
			ProgramAccounts: make([]uint64, numAccts),
		}

		for i := uint8(0); i < numAccts; i++ {
			instrCtx.ProgramAccounts[i] = uint64(i)
		}

		// Test index resolution - calls real function
		_, err := instrCtx.IndexOfProgramAccountInTransaction(index)

		// Verify bounds checking
		if index >= uint64(numAccts) {
			if err == nil {
				t.Error("Expected error for out-of-bounds index")
			}
		} else {
			if err != nil {
				t.Errorf("Unexpected error for valid index: %v", err)
			}
		}
	})
}

// FuzzComputeMeterConsumption tests compute budget tracking
func FuzzComputeMeterConsumption(f *testing.F) {
	f.Add(uint64(200000), uint64(1000))
	f.Add(uint64(200000), uint64(100000))
	f.Add(uint64(200000), uint64(200))

	f.Fuzz(func(t *testing.T, limit uint64, cost uint64) {
		// Limit to reasonable values
		if limit > 1000000 {
			limit = limit % 1000000
		}
		if limit == 0 {
			limit = 1
		}

		// Test real compute meter functions
		meter := cu.NewComputeMeter(limit)

		// Get initial state
		remaining := meter.Remaining()

		if remaining != limit {
			t.Error("Initial remaining should equal limit")
		}

		// Try to consume compute units - calls real function
		err := meter.Consume(cost)

		if cost > remaining {
			// Should fail
			if err == nil {
				t.Error("Expected error when consuming more than remaining")
			}
		} else {
			// Should succeed
			if err != nil {
				t.Errorf("Unexpected error consuming valid amount: %v", err)
			}

			// Verify remaining decreased (or stayed same if cost was 0)
			newRemaining := meter.Remaining()
			expectedRemaining := remaining - cost
			if newRemaining != expectedRemaining {
				t.Errorf("Remaining should be %d after consuming %d from %d, got %d", expectedRemaining, cost, remaining, newRemaining)
			}
		}
	})
}
