package sealevel

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
)

func TestWrappedCustomErrorNormalizationProof(t *testing.T) {
	wrap := func(err error) error {
		return &sbpf.Exception{Detail: sbpf.ExcSyscallError{Err: err}}
	}
	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{"nil", nil, nil},
		{"named-direct", SystemProgErrResultWithNegativeLamports, SystemProgErrResultWithNegativeLamports},
		{"named-wrapped", wrap(SystemProgErrResultWithNegativeLamports), SystemProgErrResultWithNegativeLamports},
		{"named-fmt-wrapped", wrap(fmt.Errorf("call: %w", SystemProgErrResultWithNegativeLamports)), SystemProgErrResultWithNegativeLamports},
		{"named-zero-wrapped", wrap(SystemProgErrAccountAlreadyInUse), SystemProgErrAccountAlreadyInUse},
		{"typed-wrapped", wrap(InstrErrCustomCode{Code: 1}), InstrErrCustomCode{Code: 1}},
		{"typed-zero-wrapped", wrap(InstrErrCustomCode{Code: 0}), InstrErrCustomCode{Code: 0}},
		{"instruction-direct", InstrErrComputationalBudgetExceeded, InstrErrComputationalBudgetExceeded},
		{"instruction-wrapped", wrap(InstrErrComputationalBudgetExceeded), InstrErrComputationalBudgetExceeded},
		{"vm-out-of-cu", &sbpf.Exception{Detail: sbpf.ExcOutOfCU}, InstrErrProgramFailedToComplete},
		{"compute-meter", cu.ErrComputeExceeded, InstrErrProgramFailedToComplete},
		{"unknown-syscall", wrap(errors.New("unknown syscall failure")), InstrErrProgramFailedToComplete},
		{"bad-memory-access", &sbpf.Exception{Detail: sbpf.NewExcBadAccess(1, 8, false, "test")}, InstrErrProgramFailedToComplete},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := normalizeProgramRunErr(test.err)
			if !errors.Is(got, test.want) {
				t.Fatalf("normalization lost error cause: got %v; want %v", got, test.want)
			}
		})
	}
}
