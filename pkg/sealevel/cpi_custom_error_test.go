package sealevel

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
)

func TestWrappedCustomCPIExecution(t *testing.T) {
	program := benchPubkey(0x91)
	pda, bump, err := solana.FindProgramAddress([][]byte{[]byte("You pass butter")}, program)
	if err != nil {
		t.Fatal(err)
	}
	for _, language := range []string{"c", "rust"} {
		for _, occupied := range []bool{false, true} {
			name := language + "/success"
			var data []byte
			if occupied {
				name = language + "/occupied"
				data = []byte{1}
			}
			t.Run(name, func(t *testing.T) {
				workload := programWorkload{
					elf:     fixtures.Load(t, "sbpf", "cpi_"+language+"_to_system_program_allocate.so"),
					program: program,
					accts: []accounts.Account{
						{Key: a.SystemProgramAddr, Owner: a.NativeLoaderAddr, Executable: true, Lamports: 10000},
						{Key: pda, Owner: a.SystemProgramAddr, Lamports: 10000, Data: data},
					},
					metas:       []AccountMeta{{Pubkey: a.SystemProgramAddr}, {Pubkey: pda, IsSigner: true, IsWritable: true}},
					instruction: []byte{bump},
				}
				ctx, runErr := workloadRunner(t, workload, false)()
				target, err := ctx.TransactionContext.Accounts.GetAccount(2)
				if err != nil {
					t.Fatal(err)
				}
				if target.Lamports != 10000 || target.Owner != a.SystemProgramAddr {
					t.Errorf("allocation changed account balance or owner: got %d, %s", target.Lamports, target.Owner)
				}
				if occupied {
					if !bytes.Equal(target.Data, data) {
						t.Errorf("failed allocation changed account data: got %x, want %x", target.Data, data)
					}
					if !errors.Is(runErr, SystemProgErrAccountAlreadyInUse) {
						t.Errorf("CPI custom cause lost: got %v, want %v", runErr, SystemProgErrAccountAlreadyInUse)
					}
					if !IsCustomErr(runErr) || TranslateErrToErrCode(runErr) != 0 {
						t.Errorf("CPI error is not custom zero: got %v", runErr)
					}
				} else {
					if runErr != nil {
						t.Errorf("allocation failed: %v", runErr)
					}
					if len(target.Data) != 1337 {
						t.Errorf("allocation size: got %d, want 1337", len(target.Data))
					}
				}
				wantData := binary.LittleEndian.AppendUint32(nil, SystemProgramInstrTypeAllocate)
				wantData = binary.LittleEndian.AppendUint64(wantData, 1337)
				if ctx.InnerInstructionsCaptureError != nil || len(ctx.InnerInstrs) != 1 {
					t.Fatalf("CPI capture: got %d instructions, error %v", len(ctx.InnerInstrs), ctx.InnerInstructionsCaptureError)
				}
				inner := ctx.InnerInstrs[0]
				if inner.TopLevelIdx != 0 || inner.StackHeight != 2 || inner.ProgramIdIndex != 1 ||
					!bytes.Equal(inner.Accounts, []byte{2}) || !bytes.Equal(inner.Data, wantData) {
					t.Errorf("unexpected System Allocate CPI: %+v", inner)
				}
				t.Logf("occupied=%t error=%v compute=%d inner=%d bytes=%d", occupied, runErr, ctx.ComputeMeter.Used(), len(ctx.InnerInstrs), len(target.Data))
			})
		}
	}
}
