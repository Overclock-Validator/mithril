package replay

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

func TestWrappedCustomErrorJSONProof(t *testing.T) {
	wrap := func(err error) error {
		return &sbpf.Exception{Detail: fmt.Errorf("call: %w", sbpf.ExcSyscallError{Err: err})}
	}
	index := uint8(1)
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{"named-direct", sealevel.SystemProgErrResultWithNegativeLamports, `{"InstructionError":[1,{"Custom":1}]}`},
		{"named-wrapped", wrap(sealevel.SystemProgErrResultWithNegativeLamports), `{"InstructionError":[1,{"Custom":1}]}`},
		{"typed-wrapped", wrap(sealevel.InstrErrCustomCode{Code: 1}), `{"InstructionError":[1,{"Custom":1}]}`},
		{"fallback-control", sealevel.InstrErrProgramFailedToComplete, `{"InstructionError":[1,"ProgramFailedToComplete"]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := json.Marshal(&TransactionError{ErrorType: TransactionErrorInstructionError, InstructionIndex: &index, InstructionError: test.err})
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != test.want {
				t.Fatalf("wire error mismatch: got %s; want %s", got, test.want)
			}
		})
	}
}
