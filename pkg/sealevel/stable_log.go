package sealevel

import (
	"encoding/base64"
	"fmt"

	"github.com/gagliardetto/solana-go"
)

// The BPF lines of Agave's stable log: "consumed" after every program run and
// "return" after a successful run that set return data. ExecuteInstruction
// writes the invoke, success and failed lines. Like those, they are written
// only when the caller records logs (SkipStableLogs is false): simulation,
// history capture, or a debug transaction.

func (execCtx *ExecutionCtx) logConsumed(program solana.PublicKey, consumed, available uint64) {
	execCtx.Log.Log(fmt.Sprintf("Program %s consumed %d of %d compute units", program, consumed, available))
}

func (execCtx *ExecutionCtx) logReturn() {
	programID, data := execCtx.TransactionContext.ReturnData()
	if len(data) > 0 {
		execCtx.Log.Log("Program return: " + programID.String() + " " + base64.StdEncoding.EncodeToString(data))
	}
}
