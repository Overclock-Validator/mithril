package sealevel

import (
	"errors"
	"fmt"
	"strings"
)

const (
	maxRecordedLogBytes   = 10_000
	maxRecordedLogEntries = 10_000
)

type Logger interface {
	Log(s string)
}

// LogRecorder retains messages in call order. Its zero value is unbounded.
// Logs must not be modified while recording.
type LogRecorder struct {
	Logs []string

	bounded      bool
	bytesWritten int
	truncated    bool
}

// NewBoundedLogRecorder retains fewer than 10,000 content bytes and at most
// 10,001 entries. It inserts one "Log truncated" marker when the byte or
// 10,000-entry threshold is reached. The marker counts toward the entry
// threshold, but not the byte budget. Later messages may fit after a rejection.
// This does not limit allocations made by callers before Log.
func NewBoundedLogRecorder() *LogRecorder {
	return &LogRecorder{bounded: true}
}

func (r *LogRecorder) Log(s string) {
	if !r.bounded {
		r.Logs = append(r.Logs, s)
		return
	}
	if len(r.Logs) >= maxRecordedLogEntries || len(s) >= maxRecordedLogBytes-r.bytesWritten {
		if !r.truncated {
			r.Logs = append(r.Logs, "Log truncated")
			r.truncated = true
		}
		return
	}
	r.bytesWritten += len(s)
	r.Logs = append(r.Logs, strings.Clone(s))
}

// instructionErrorLogMessage matches Agave InstructionError's Display without
// changing Mithril's execution errors or their JSON/numerical representation.
func instructionErrorLogMessage(err error) string {
	for current := err; current != nil; current = errors.Unwrap(current) {
		if IsCustomErr(current) {
			return fmt.Sprintf("custom program error: %#x", TranslateErrToErrCode(current))
		}
	}
	if code, ok := solanaErrCode(err); ok && code >= 0 && code < len(instructionErrorLogMessages) && instructionErrorLogMessages[code] != "" {
		return instructionErrorLogMessages[code]
	}
	return err.Error()
}

// Indexed by the existing Solana InstructionError numerical codes. These are
// protocol log text, not Go error names (Agave SDK instruction-error Display).
var instructionErrorLogMessages = [...]string{
	"generic instruction error",
	"invalid program argument",
	"invalid instruction data",
	"invalid account data for instruction",
	"account data too small for instruction",
	"insufficient funds for instruction",
	"incorrect program id for instruction",
	"missing required signature for instruction",
	"instruction requires an uninitialized account",
	"instruction requires an initialized account",
	"sum of account balances before and after instruction do not match",
	"instruction illegally modified the program id of an account",
	"instruction spent from the balance of an account it does not own",
	"instruction modified data of an account it does not own",
	"instruction changed the balance of a read-only account",
	"instruction modified data of a read-only account",
	"instruction contains duplicate accounts",
	"instruction changed executable bit of an account",
	"instruction modified rent epoch of an account",
	"insufficient account keys for instruction",
	"program other than the account's owner changed the size of the account data",
	"instruction expected an executable account",
	"instruction tries to borrow reference for an account which is already borrowed",
	"instruction left account with an outstanding borrowed reference",
	"instruction modifications of multiply-passed account differ",
	"", // Custom carries its own u32 value.
	"program returned invalid error code",
	"instruction changed executable accounts data",
	"instruction changed the balance of an executable account",
	"executable accounts must be rent exempt",
	"Unsupported program id",
	"Cross-program invocation call depth too deep",
	"An account required by the instruction is missing",
	"Cross-program invocation reentrancy not allowed for this instruction",
	"Length of the seed is too long for address generation",
	"Provided seeds do not result in a valid address",
	"Failed to reallocate account data",
	"Computational budget exceeded",
	"Cross-program invocation with unauthorized signer or writable account",
	"Failed to create program execution environment",
	"Program failed to complete",
	"Program failed to compile",
	"Account is immutable",
	"Incorrect authority provided",
	"Failed to serialize or deserialize account data",
	"An account does not have enough lamports to be rent-exempt",
	"Invalid account owner",
	"Program arithmetic overflowed",
	"Unsupported sysvar",
	"Provided owner is not allowed",
	"Accounts data allocations exceeded the maximum allowed per transaction",
	"Max accounts exceeded",
	"Max instruction trace length exceeded",
	"Builtin programs must consume compute units",
}
