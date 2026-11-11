package replay_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/require"
)

func TestTransactionCustomErrorJSON(t *testing.T) {
	wrap := func(err error) error {
		return fmt.Errorf("program: %w", &sbpf.Exception{Detail: sbpf.ExcSyscallError{Err: err}})
	}
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{"zero", sealevel.InstrErrCustomCode{Code: 0}, `{"Custom":0}`},
		{"program-code", sealevel.InstrErrCustomCode{Code: 6001}, `{"Custom":6001}`},
		{"max-code", sealevel.InstrErrCustomCode{Code: ^uint32(0)}, `{"Custom":4294967295}`},
		{"wrapped-code", fmt.Errorf("program: %w", sealevel.InstrErrCustomCode{Code: 6001}), `{"Custom":6001}`},
		{"nested-zero", wrap(sealevel.InstrErrCustomCode{Code: 0}), `{"Custom":0}`},
		{"nested-one", wrap(sealevel.InstrErrCustomCode{Code: 1}), `{"Custom":1}`},
		{"nested-max", wrap(sealevel.InstrErrCustomCode{Code: ^uint32(0)}), `{"Custom":4294967295}`},
		{"named-zero", sealevel.SystemProgErrAccountAlreadyInUse, `{"Custom":0}`},
		{"named-code", sealevel.SystemProgErrResultWithNegativeLamports, `{"Custom":1}`},
		{"wrapped-named-code", fmt.Errorf("program: %w", sealevel.SystemProgErrResultWithNegativeLamports), `{"Custom":1}`},
		{"nested-named-zero", wrap(sealevel.SystemProgErrAccountAlreadyInUse), `{"Custom":0}`},
		{"nested-named-one", wrap(sealevel.SystemProgErrResultWithNegativeLamports), `{"Custom":1}`},
		{"unit-variant", sealevel.InstrErrInvalidArgument, `"InvalidArgument"`},
		{"wrapped-unit", fmt.Errorf("program: %w", sealevel.InstrErrInvalidArgument), `"InvalidArgument"`},
		{"nested-compute-budget", wrap(sealevel.InstrErrComputationalBudgetExceeded), `"ComputationalBudgetExceeded"`},
		{"nested-missing-signature", wrap(sealevel.InstrErrMissingRequiredSignature), `"MissingRequiredSignature"`},
		{"nested-privilege", wrap(sealevel.InstrErrPrivilegeEscalation), `"PrivilegeEscalation"`},
		{"nested-vm-failure", wrap(sealevel.InstrErrProgramFailedToComplete), `"ProgramFailedToComplete"`},
		{"borsh-unit", sealevel.InstrErrBorshIoError, `"BorshIoError"`},
		{"nested-borsh-unit", wrap(sealevel.InstrErrBorshIoError), `"BorshIoError"`},
		{"unknown", errors.New("unknown instruction failure"), `"unknown instruction failure"`},
		{"wrapped-unknown", fmt.Errorf("program: %w", errors.New("unknown instruction failure")), `"program: unknown instruction failure"`},
		{"unresolved", sealevel.InstrErrCustom, ""},
		{"wrapped-unresolved", fmt.Errorf("program: %w", sealevel.InstrErrCustom), ""},
		{"nested-unresolved", wrap(sealevel.InstrErrCustom), ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			index := uint8(2)
			status := &replay.TransactionError{
				ErrorType: replay.TransactionErrorInstructionError, InstructionIndex: &index, InstructionError: test.err,
			}
			response := rpcserver.SimulateTransactionResp{Value: rpcserver.SimulateTransactionRespValue{Err: status}}
			encoded, err := json.Marshal(status)
			responseJSON, responseErr := json.Marshal(response)
			if test.want == "" {
				require.ErrorContains(t, err, "custom instruction error has no code")
				require.ErrorContains(t, responseErr, "custom instruction error has no code")
				require.Nil(t, encoded)
				require.Nil(t, responseJSON)
				return
			}
			require.NoError(t, err)
			require.NoError(t, responseErr)
			want := `{"InstructionError":[2,` + test.want + `]}`
			require.JSONEq(t, want, string(encoded))
			var decoded struct {
				Value struct {
					Err json.RawMessage `json:"err"`
				} `json:"value"`
			}
			require.NoError(t, json.Unmarshal(responseJSON, &decoded))
			require.JSONEq(t, want, string(decoded.Value.Err))
		})
	}
}
