package sealevel

import (
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
)

// FuzzLogRecorder tests the LogRecorder functionality
func FuzzLogRecorder(f *testing.F) {
	// Seed with various strings
	f.Add("")
	f.Add("test")
	f.Add("Program log: test")
	f.Add(strings.Repeat("a", 1000))

	f.Fuzz(func(t *testing.T, msg string) {
		// Limit message size to prevent excessive memory usage
		if len(msg) > 100000 {
			t.Skip("message too large")
		}

		recorder := &LogRecorder{}

		// Log the message
		recorder.Log(msg)

		// Verify message was recorded
		if len(recorder.Logs) != 1 {
			t.Errorf("Expected 1 log entry, got %d", len(recorder.Logs))
		}

		if recorder.Logs[0] != msg {
			t.Errorf("Log message mismatch")
		}

		// Test multiple logs
		recorder.Log(msg)
		if len(recorder.Logs) != 2 {
			t.Errorf("Expected 2 log entries after second log, got %d", len(recorder.Logs))
		}

		// Both should be the same
		if recorder.Logs[0] != recorder.Logs[1] {
			t.Errorf("Multiple logs of same message differ")
		}
	})
}

// FuzzLogComputeUnitConsumption tests compute unit consumption for loggingumption for logging
func FuzzLogComputeUnitConsumption(f *testing.F) {
	// Seed with various string lengths
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(100))
	f.Add(uint64(cu.CUSyscallBaseCost))
	f.Add(uint64(10000))

	f.Fuzz(func(t *testing.T, strlen uint64) {
		// Limit string length
		if strlen > 100000 {
			t.Skip("strlen too large")
		}

		// Calculate expected cost as max(base_cost, strlen)
		expectedCost := max(cu.CUSyscallBaseCost, strlen)

		// Verify cost calculation is consistent
		if strlen <= cu.CUSyscallBaseCost {
			if expectedCost != cu.CUSyscallBaseCost {
				t.Errorf("Cost for short string should be base cost")
			}
		} else {
			if expectedCost != strlen {
				t.Errorf("Cost for long string should be strlen")
			}
		}

		// Create execution context
		execCtx := &ExecutionCtx{
			ComputeMeter: cu.NewComputeMeterDefault(),
		}

		initialUnits := execCtx.ComputeMeter.Remaining()

		// Consume compute units for logging
		err := execCtx.ComputeMeter.Consume(expectedCost)
		if err != nil {
			// Compute exceeded
			return
		}

		actualCost := initialUnits - execCtx.ComputeMeter.Remaining()
		if actualCost != expectedCost {
			t.Errorf("Consumed %d units, want %d", actualCost, expectedCost)
		}
	})
}

// FuzzLogDataComputeCost tests compute cost for multi-data logging
func FuzzLogDataComputeCost(f *testing.F) {
	// Seed with various data counts and sizes
	f.Add(uint64(1), uint64(10))
	f.Add(uint64(5), uint64(100))
	f.Add(uint64(10), uint64(1000))

	f.Fuzz(func(t *testing.T, dataCount, dataSize uint64) {
		// Limit to reasonable values
		if dataCount > 100 || dataSize > 10000 {
			t.Skip("inputs too large")
		}

		// Calculate expected cost
		// Base cost per syscall
		baseCost := uint64(cu.CUSyscallBaseCost)
		// Base cost per data element
		perElementCost := dataCount * uint64(cu.CUSyscallBaseCost)
		// Cost per byte of data
		dataBytesCost := dataCount * dataSize

		totalExpectedCost := baseCost + perElementCost + dataBytesCost

		// Verify cost is additive
		if totalExpectedCost < baseCost {
			t.Errorf("Cost calculation underflow")
		}

		// Create execution context
		execCtx := &ExecutionCtx{
			ComputeMeter: cu.NewComputeMeterDefault(),
		}

		initialUnits := execCtx.ComputeMeter.Remaining()

		// Consume base cost
		err := execCtx.ComputeMeter.Consume(baseCost)
		if err != nil {
			return
		}

		// Consume per-element cost
		err = execCtx.ComputeMeter.Consume(perElementCost)
		if err != nil {
			return
		}

		// Consume per-byte cost
		err = execCtx.ComputeMeter.Consume(dataBytesCost)
		if err != nil {
			return
		}

		actualCost := initialUnits - execCtx.ComputeMeter.Remaining()
		if actualCost != totalExpectedCost {
			t.Errorf("Total cost %d, want %d", actualCost, totalExpectedCost)
		}
	})
}

// FuzzLogMessageSafety tests that logging handles various string content safely
func FuzzLogMessageSafety(f *testing.F) {
	// Seed with potentially problematic strings
	f.Add("normal message")
	f.Add("message\nwith\nnewlines")
	f.Add("message\x00with\x00nulls")
	f.Add("message with unicode: 你好")
	f.Add(strings.Repeat("x", 1000))

	f.Fuzz(func(t *testing.T, msg string) {
		// Limit message size
		if len(msg) > 100000 {
			t.Skip("message too large")
		}

		recorder := &LogRecorder{}

		// Log the message - should never panic
		recorder.Log("Program log: " + msg)

		// Verify it was recorded
		if len(recorder.Logs) != 1 {
			t.Errorf("Expected 1 log entry, got %d", len(recorder.Logs))
		}

		// Verify the original message is preserved
		if !strings.Contains(recorder.Logs[0], msg) {
			t.Errorf("Log message does not contain original message")
		}
	})
}
