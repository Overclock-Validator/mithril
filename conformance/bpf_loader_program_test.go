package conformance

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func bpfLoaderTestAccountStateChangesMatch(t *testing.T, execCtx *sealevel.ExecutionCtx, fixture *InstrFixture) bool {
	txCtx := execCtx.TransactionContext
	acctsModified := make([]accounts.Account, 0)

	for idx, touched := range txCtx.Accounts.Touched {
		if touched {
			touchedAcct, err := txCtx.Accounts.GetAccount(uint64(idx))
			assert.NoError(t, err)
			acctsModified = append(acctsModified, *touchedAcct)
		}
	}

	for _, mithrilModifiedAcct := range acctsModified {
		var modifiedAcctFoundInTestcase bool
		for _, fixtureModifiedAcct := range fixture.Output.ModifiedAccounts {
			if solana.PublicKeyFromBytes(fixtureModifiedAcct.Address) == mithrilModifiedAcct.Key {
				modifiedAcctFoundInTestcase = true
				if fixtureModifiedAcct.Lamports != mithrilModifiedAcct.Lamports {
					return false
				}
				if fixtureModifiedAcct.Executable != mithrilModifiedAcct.Executable {
					return false
				}
				// AcctState dropped rent_epoch in protosol v5.4.0.
				if solana.PublicKeyFromBytes(fixtureModifiedAcct.Owner[:]) != solana.PublicKeyFromBytes(mithrilModifiedAcct.Owner[:]) {
					return false
				}

				if !bytes.Equal(fixtureModifiedAcct.Data, mithrilModifiedAcct.Data) {
					fmt.Printf("**** account states did not match\n")
					fmt.Printf("\na: %v\n\n", mithrilModifiedAcct.Data)
					fmt.Printf("b: %v\n\n", fixtureModifiedAcct.Data)
					return false
				}
			}
		}
		if !modifiedAcctFoundInTestcase {
			postBytes := mithrilModifiedAcct.Data
			var preBytes []byte

			foundAcct := false
			for _, acct := range fixture.Input.Accounts {
				if mithrilModifiedAcct.Key == solana.PublicKeyFromBytes(acct.Address) {
					preBytes = acct.Data
					foundAcct = true
					break
				}
			}
			if !foundAcct {
				t.Fatalf("pre-account not found. should never happen.")
			}

			if !bytes.Equal(postBytes, preBytes) {
				fmt.Printf("len(pre) %d vs len(post) %d\n", len(preBytes), len(postBytes))
				return false
			}
		}
	}

	return true
}

func TestConformance_Bpf_Loader_Program(t *testing.T) {
	basePath := "test-vectors/instr/fixtures/bpf-loader-v3"
	fileInfos, err := ioutil.ReadDir(basePath)
	assert.NoError(t, err)

	var fnames []string
	for _, fileInfo := range fileInfos {
		filePath := fmt.Sprintf("%s/%s", basePath, fileInfo.Name())
		fnames = append(fnames, filePath)
	}

	failedTestcases := make([]string, 0)
	var testcaseCounter uint64
	var acctStateFailure uint64
	var returnValueFailure uint64

	acctStateFailureMap := make(map[int]int)
	returnValueFailureMap := make(map[int]int)

	for _, fname := range fnames {
		testcaseCounter++
		in, err := ioutil.ReadFile(fname)
		if err != nil {
			log.Fatalln("Error reading file:", err)
		}

		fixture := &InstrFixture{}
		if err := proto.Unmarshal(in, fixture); err != nil {
			log.Fatalln("Failed to parse fixture:", err)
		}

		fmt.Printf("**** (%s) testcase %d of %d\n", fname, testcaseCounter, len(fnames))

		execCtx, instrAccts := newExecCtxAndInstrAcctsFromFixture(fixture)

		fmt.Printf("prepared instruction accounts:")
		for idx, ia := range instrAccts {
			fmt.Printf("instrAcct %d: %v\n", idx, ia)
		}

		var instrCode int32 = -1
		if len(fixture.Input.Data) >= 4 {
			instrCode = int32(binary.LittleEndian.Uint32(fixture.Input.Data[0:4]))
			fmt.Printf("instruction code: %d\n", instrCode)
		}

		for idx, acct := range fixture.Input.Accounts {
			fmt.Printf("txAcct %d: %s, Lamports: %d\n", idx, solana.PublicKeyFromBytes(acct.Address), acct.Lamports)
		}

		for idx, acct := range fixture.Input.InstrAccounts {
			fmt.Printf("instrAcct %d: %s, isSigner: %t, Executable: %t, Lamports: %d\n", idx, solana.PublicKeyFromBytes(fixture.Input.Accounts[acct.Index].Address), acct.IsSigner, fixture.Input.Accounts[acct.Index].Executable, fixture.Input.Accounts[acct.Index].Lamports)
		}

		err = execCtx.ProcessInstruction(fixture.Input.Data, instrAccts, []uint64{0})

		if !returnValueIsExpectedValue(fixture, err) {
			errMsg := fmt.Sprintf("failed testcase on return value (instrCode %d), %s", instrCode, fname)
			failedTestcases = append(failedTestcases, errMsg)
			returnValueFailure++
			returnValueFailureMap[int(instrCode)]++
		}

		if err == nil {
			if !bpfLoaderTestAccountStateChangesMatch(t, execCtx, fixture) {
				errMsg := fmt.Sprintf("failed testcase on account state check (instrCode %d), %s", instrCode, fname)
				failedTestcases = append(failedTestcases, errMsg)
				acctStateFailure++
				acctStateFailureMap[int(instrCode)]++
			}
		}
	}

	fmt.Printf("\n\n")

	for _, fn := range failedTestcases {
		fmt.Printf("%s\n", fn)
	}

	fmt.Printf("\n\nfailed testcases %d:\n", len(failedTestcases))
	fmt.Printf("return value failures: %d, acct state failures: %d\n\n", returnValueFailure, acctStateFailure)

	for k, v := range returnValueFailureMap {
		fmt.Printf("(return value failures) instrCode: %d, %d failures\n", k, v)
	}

	fmt.Printf("\n")

	for k, v := range acctStateFailureMap {
		fmt.Printf("(acct state failures) instrCode: %d, %d failures\n", k, v)
	}

	assert.Empty(t, failedTestcases, "failing testcases found")
}

func TestConformance_Bpf_Loader_Program_Single_Testcase(t *testing.T) {
	basePath := "test-vectors/instr/fixtures/bpf-loader-v3"
	fn := "5994531cac2587a14054252dc54b9eb1f288f357.fix"

	fname := fmt.Sprintf("%s/%s", basePath, fn)

	in, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Fatalln("Error reading file:", err)
	}

	fixture := &InstrFixture{}
	if err := proto.Unmarshal(in, fixture); err != nil {
		log.Fatalln("Failed to parse fixture:", err)
	}

	execCtx, instrAccts := newExecCtxAndInstrAcctsFromFixture(fixture)

	fmt.Printf("prepared instruction accounts:\n")
	for idx, ia := range instrAccts {
		fmt.Printf("instrAcct %d: %s, owner = %s, isSigner = %t, isWritable = %t\n", idx, solana.PublicKeyFromBytes(fixture.Input.Accounts[ia.IndexInCallee].Address), solana.PublicKeyFromBytes(fixture.Input.Accounts[ia.IndexInCallee].Owner), ia.IsSigner, ia.IsWritable)
	}

	var instrCode int32 = -1
	if len(fixture.Input.Data) >= 4 {
		instrCode = int32(binary.LittleEndian.Uint32(fixture.Input.Data[0:4]))
		fmt.Printf("instruction code: %d\n", instrCode)
	}

	for idx, acct := range fixture.Input.Accounts {
		fmt.Printf("txAcct %d: %s, Lamports: %d\n", idx, solana.PublicKeyFromBytes(acct.Address), acct.Lamports)
	}

	for idx, acct := range fixture.Input.InstrAccounts {
		fmt.Printf("instrAcct %d: %s, isSigner: %t, Executable: %t, Lamports: %d\n", idx, solana.PublicKeyFromBytes(fixture.Input.Accounts[acct.Index].Address), acct.IsSigner, fixture.Input.Accounts[acct.Index].Executable, fixture.Input.Accounts[acct.Index].Lamports)
	}

	err = execCtx.ProcessInstruction(fixture.Input.Data, instrAccts, []uint64{0})

	if !returnValueIsExpectedValue(fixture, err) {
		fmt.Printf("failed testcase on return value (instrCode %d), %s\n", instrCode, fname)
	}

	if err == nil {
		if !bpfLoaderTestAccountStateChangesMatch(t, execCtx, fixture) {
			fmt.Printf("failed testcase on account state check (instrCode %d), %s\n", instrCode, fname)
		}
	}
}
