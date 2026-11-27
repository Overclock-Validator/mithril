package sealevel

import (
	"fmt"
	"testing"
)

func TestInstructionErrorLogMessage(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{InstrErrCustomCode{Code: 1}, "custom program error: 0x1"},
		{fmt.Errorf("wrapped: %w", InstrErrCustomCode{Code: 0x1771}), "custom program error: 0x1771"},
		{InstrErrInsufficientFunds, "insufficient funds for instruction"},
		{InstrErrComputationalBudgetExceeded, "Computational budget exceeded"},
	} {
		if got := instructionErrorLogMessage(tc.err); got != tc.want {
			t.Fatalf("%v: got %q, want %q", tc.err, got, tc.want)
		}
	}
}
