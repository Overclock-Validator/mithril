package sealevel

import "errors"

const (
	// These are optional capture ceilings, not instruction validation limits.
	// Replay currently permits 64 instruction trace entries. Capture happens
	// before Push, so failed invocations need their own entry and byte bounds.
	maxRecordedInnerInstructions = 64
	// Apply the current C/Rust CPI maxima as optional per-entry capture limits.
	// This does not validate or restrict NativeInvoke execution.
	maxRecordedInnerInstructionBytes = maxRecordedInnerInstructions * (MaxCpiInstructionDataLen + MaxCpiInstructionAccounts)
)

var errInnerInstructionCaptureLimit = errors.New("inner instruction capture limit exceeded")

// EnableBoundedInnerInstructions enables optional bounded CPI capture on a
// fresh execution context, before any instruction executes. The default
// RecordInnerInstructions path remains unbounded for existing callers.
func (execCtx *ExecutionCtx) EnableBoundedInnerInstructions() {
	execCtx.RecordInnerInstructions = true
	execCtx.boundedInnerInstructions = true
}

// recordInnerInstruction reports whether it appended an entry.
func (execCtx *ExecutionCtx) recordInnerInstruction(data []byte, instructionAccts []InstructionAccount, programIndex uint8) bool {
	if execCtx.boundedInnerInstructions {
		if execCtx.InnerInstructionsCaptureError != nil {
			return false
		}
		remaining := maxRecordedInnerInstructionBytes - execCtx.innerInstructionBytes
		if len(data) > MaxCpiInstructionDataLen || len(instructionAccts) > MaxCpiInstructionAccounts ||
			len(execCtx.InnerInstrs) >= maxRecordedInnerInstructions || len(data) > remaining || len(instructionAccts) > remaining-len(data) {
			execCtx.invalidateInnerInstructionCapture(errInnerInstructionCaptureLimit)
			return false
		}
		execCtx.innerInstructionBytes += len(data) + len(instructionAccts)
	}

	acctIdxs := make([]uint8, len(instructionAccts))
	for i, account := range instructionAccts {
		acctIdxs[i] = uint8(account.IndexInTransaction)
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	execCtx.InnerInstrs = append(execCtx.InnerInstrs, RecordedInnerInstr{
		TopLevelIdx:    execCtx.currentTopLevelInstrIdx,
		StackHeight:    uint8(execCtx.StackHeight() + 1),
		ProgramIdIndex: programIndex,
		Accounts:       acctIdxs,
		Data:           dataCopy,
	})
	return true
}

// dropLastInnerInstruction removes the entry of an invocation that never
// started. Such a call fails its transaction, so the list still names every
// invocation that ran. (Agave keeps the entry when the failure is the call
// depth limit; that one case differs.)
func (execCtx *ExecutionCtx) dropLastInnerInstruction() {
	last := len(execCtx.InnerInstrs) - 1
	if last < 0 {
		return
	}
	if execCtx.boundedInnerInstructions {
		execCtx.innerInstructionBytes -= len(execCtx.InnerInstrs[last].Data) + len(execCtx.InnerInstrs[last].Accounts)
	}
	execCtx.InnerInstrs = execCtx.InnerInstrs[:last]
}

func (execCtx *ExecutionCtx) invalidateInnerInstructionCapture(err error) {
	if execCtx.boundedInnerInstructions && execCtx.InnerInstructionsCaptureError == nil {
		execCtx.InnerInstructionsCaptureError = err
		execCtx.InnerInstrs = nil
	}
}
