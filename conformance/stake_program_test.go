package conformance

import (
	"fmt"
	"io/ioutil"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestConformance_Stake_Program(t *testing.T) {
	basePath := "test-vectors/instr/fixtures/stake"
	fileInfos := readFixtureDirOrSkip(t, basePath)

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

		execCtx, instrAccts := newExecCtxAndInstrAcctsFromFixture(fixture)

		printFixtureInfo(fixture)

		err = execCtx.ProcessInstruction(fixture.Input.Data, instrAccts, []uint64{0})

		instrCode := instrCodeFromFixtureInstrData(fixture)

		if !returnValueIsExpectedValue(fixture, err) {
			errMsg := fmt.Sprintf("failed testcase on return value (instrCode %d), %s", instrCode, fname)
			failedTestcases = append(failedTestcases, errMsg)
			returnValueFailure++
			returnValueFailureMap[int(instrCode)]++
		}

		if err == nil {
			if !accountStateChangesMatch(t, execCtx, fixture) {
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

	fmt.Printf("\n\nfailed testcases %d / %d:\n", len(failedTestcases), len(fnames))
	fmt.Printf("return value failures: %d, acct state failures: %d\n\n", returnValueFailure, acctStateFailure)

	for k, v := range returnValueFailureMap {
		fmt.Printf("(return value failures) instrCode: %d, %d failures\n", k, v)
	}

	fmt.Printf("\n")

	for k, v := range acctStateFailureMap {
		fmt.Printf("(acct state failures) instrCode: %d, %d failures\n", k, v)
	}

	hasFailedTestcases := len(failedTestcases) != 0
	assert.Equal(t, false, hasFailedTestcases, "failing testcases found")
}

func TestConformance_Stake_Program_Single_Testcase(t *testing.T) {
	basePath := "test-vectors/instr/fixtures/stake"
	fn := "instr-11Eu3urzXSSgMfk5EvFK3siuxfYhoWa18QBPrXcqiD98xi69QNneMj7qHudtFE8qTUpyCzNsZzMeL95A7e3w5i-0107.fix"

	fname := fmt.Sprintf("%s/%s", basePath, fn)

	in := readFixtureOrSkip(t, fname)

	fixture := &InstrFixture{}
	if err := proto.Unmarshal(in, fixture); err != nil {
		log.Fatalln("Failed to parse fixture:", err)
	}

	execCtx, instrAccts := newExecCtxAndInstrAcctsFromFixture(fixture)

	printFixtureInfo(fixture)

	err := execCtx.ProcessInstruction(fixture.Input.Data, instrAccts, []uint64{0})

	instrCode := instrCodeFromFixtureInstrData(fixture)

	if !returnValueIsExpectedValue(fixture, err) {
		fmt.Printf("failed testcase on return value (instrCode %d), %s\n", instrCode, fname)
	}

	if err == nil {
		if !accountStateChangesMatch(t, execCtx, fixture) {
			fmt.Printf("failed testcase on account state check (instrCode %d), %s\n", instrCode, fname)
		}
	}
}
