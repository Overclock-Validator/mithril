package conformance

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	sealevelPkg "github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

func withoutConformanceStdout(fn func()) {
	if os.Getenv("MITHRIL_CONFORMANCE_VERBOSE") != "" {
		fn()
		return
	}

	devNull, err := os.Open(os.DevNull)
	if err != nil {
		fn()
		return
	}
	defer devNull.Close()

	oldStdout := os.Stdout
	os.Stdout = devNull
	defer func() {
		os.Stdout = oldStdout
	}()

	fn()
}

func instrReturnMatches(fixture *InstrFixture, err error) bool {
	output := fixture.GetOutput()
	if output == nil {
		return err != nil
	}
	if err == nil {
		return output.GetResult() == 0
	}
	if output.GetResult() == 0 {
		return false
	}
	if output.GetResult() == 26 && sealevelPkg.IsCustomErr(err) {
		return uint32(sealevelPkg.TranslateErrToErrCode(err)) == output.GetCustomErr()
	}
	return int32(sealevelPkg.TranslateErrToErrCode(err)+1) == output.GetResult()
}

var firedancerInstrResultNames = map[int32]string{
	0:  "Success",
	1:  "GenericError",
	2:  "InvalidArgument",
	3:  "InvalidInstructionData",
	4:  "InvalidAccountData",
	5:  "AccountDataTooSmall",
	6:  "InsufficientFunds",
	7:  "IncorrectProgramId",
	8:  "MissingRequiredSignature",
	9:  "AccountAlreadyInitialized",
	10: "UninitializedAccount",
	11: "UnbalancedInstruction",
	12: "ModifiedProgramId",
	13: "ExternalAccountLamportSpend",
	14: "ExternalAccountDataModified",
	15: "ReadonlyLamportChange",
	16: "ReadonlyDataModified",
	17: "DuplicateAccountIndex",
	18: "ExecutableModified",
	19: "RentEpochModified",
	20: "NotEnoughAccountKeys",
	21: "AccountDataSizeChanged",
	22: "AccountNotExecutable",
	23: "AccountBorrowFailed",
	24: "AccountBorrowOutstanding",
	25: "DuplicateAccountOutOfSync",
	26: "Custom",
	27: "InvalidError",
	28: "ExecutableDataModified",
	29: "ExecutableLamportChange",
	30: "ExecutableAccountNotRentExempt",
	31: "UnsupportedProgramId",
	32: "CallDepth",
	33: "MissingAccount",
	34: "ReentrancyNotAllowed",
	35: "MaxSeedLengthExceeded",
	36: "InvalidSeeds",
	37: "InvalidRealloc",
	38: "ComputationalBudgetExceeded",
	39: "PrivilegeEscalation",
	40: "ProgramEnvironmentSetupFailure",
	41: "ProgramFailedToComplete",
	42: "ProgramFailedToCompile",
	43: "Immutable",
	44: "IncorrectAuthority",
	45: "BorshIoError",
	46: "AccountNotRentExempt",
	47: "InvalidAccountOwner",
	48: "ArithmeticOverflow",
	49: "UnsupportedSysvar",
	50: "IllegalOwner",
	51: "MaxAccountsDataAllocationsExceeded",
	52: "MaxAccountsExceeded",
	53: "MaxInstructionTraceLengthExceeded",
	54: "BuiltinProgramsMustConsumeComputeUnits",
}

func firedancerInstrResultName(result int32) string {
	if name, ok := firedancerInstrResultNames[result]; ok {
		return name
	}
	return fmt.Sprintf("UnknownResult(%d)", result)
}

func instrResultFromErr(err error) int32 {
	if err == nil {
		return 0
	}
	if sealevelPkg.IsCustomErr(err) {
		return 26
	}
	return int32(sealevelPkg.TranslateErrToErrCode(err) + 1)
}

type conformanceBucket struct {
	key   string
	count int
}

func topConformanceBuckets(counts map[string]int, limit int) []conformanceBucket {
	buckets := make([]conformanceBucket, 0, len(counts))
	for key, count := range counts {
		buckets = append(buckets, conformanceBucket{key: key, count: count})
	}
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].count != buckets[j].count {
			return buckets[i].count > buckets[j].count
		}
		return buckets[i].key < buckets[j].key
	})
	if len(buckets) > limit {
		buckets = buckets[:limit]
	}
	return buckets
}

func returnMismatchBucket(fixture *InstrFixture, err error) string {
	gotResult := instrResultFromErr(err)
	gotCode := 0
	if err != nil {
		gotCode = sealevelPkg.TranslateErrToErrCode(err)
	}
	wantResult := fixture.GetOutput().GetResult()
	return fmt.Sprintf("got=%s(%d) got_code=%d got_err=%v want=%s(%d) want_custom=%d",
		firedancerInstrResultName(gotResult), gotResult, gotCode, err,
		firedancerInstrResultName(wantResult), wantResult, fixture.GetOutput().GetCustomErr())
}

func fixtureProgramSummary(fixture *InstrFixture) string {
	input := fixture.GetInput()
	if input == nil {
		return "missing_input"
	}
	programId := solana.PublicKeyFromBytes(input.GetProgramId())
	for i, acct := range input.GetAccounts() {
		key := solana.PublicKeyFromBytes(acct.GetAddress())
		if key != programId {
			continue
		}
		owner := solana.PublicKeyFromBytes(acct.GetOwner())
		prefixLen := min(len(acct.GetData()), 8)
		return fmt.Sprintf("program_idx=%d owner=%s exec=%v lamports=%d data_len=%d data_prefix=%x",
			i, owner, acct.GetExecutable(), acct.GetLamports(), len(acct.GetData()), acct.GetData()[:prefixLen])
	}
	return fmt.Sprintf("program=%s missing_from_accounts", programId)
}

func newVMProgramExecCtxAndInstrAccts(fixture *InstrFixture) (*sealevelPkg.ExecutionCtx, []sealevelPkg.InstructionAccount, []uint64, error) {
	input := fixture.GetInput()
	if input == nil {
		return nil, nil, nil, fmt.Errorf("missing input")
	}

	accts := make([]accounts.Account, 0, len(input.GetAccounts()))
	for _, acctState := range input.GetAccounts() {
		accts = append(accts, fixtureAcctStateToAccount(acctState))
	}

	transactionAccts := sealevelPkg.NewTransactionAccounts(accts)
	instrAccts := instructionAcctsFromFixture(fixture, *transactionAccts)

	txCtx := sealevelPkg.NewTransactionCtx(*transactionAccts, 8, 128)
	txCtx.ComputeBudgetLimits = &sealevelPkg.ComputeBudgetLimits{
		UpdatedHeapBytes:   sealevelPkg.MinHeapFrameBytes,
		ComputeUnitLimit:   sealevelPkg.MaxComputeUnitLimit,
		LoadedAccountBytes: sealevelPkg.MaxLoadedAccountsDataSizeBytes,
	}

	programId := solana.PublicKeyFromBytes(input.GetProgramId())
	txCtx.AllInstructions = append(txCtx.AllInstructions, sealevelPkg.Instruction{
		Data:      input.GetData(),
		ProgramId: programId,
	})

	execCtx := sealevelPkg.ExecutionCtx{
		TransactionContext: txCtx,
		ComputeMeter:       cu.NewComputeMeter(input.GetCuAvail()),
		Log:                &sealevelPkg.LogRecorder{},
	}
	execCtx.Accounts = accounts.NewMemAccounts()
	execCtx.Features = *parsePBFeatures(input.GetFeatures())

	withoutConformanceStdout(func() {
		configureSysvarsFromFixture(&execCtx, fixture)
	})

	programCacheDb := &accountsdb.AccountsDb{}
	programCacheDb.InitCaches()

	slotAccounts := accounts.NewMemAccounts()
	for i := range accts {
		acct := accts[i]
		key := [32]byte(acct.Key)
		if err := slotAccounts.SetAccount(&key, &acct); err != nil {
			return nil, nil, nil, err
		}
	}

	// protosol v5.4.0 removed slot_context from InstrContext, so the corpus no
	// longer pins a slot for the instruction harness. Keep the sentinel the
	// previous code already used whenever the fixture supplied none.
	slot := ^uint64(0)
	execCtx.SlotCtx = &sealevelPkg.SlotCtx{
		Accounts:      slotAccounts,
		ParentAccts:   accounts.NewMemAccounts(),
		AccountsDb:    programCacheDb,
		Slot:          slot,
		AcctMapsMu:    &sync.Mutex{},
		ModifiedAccts: make(map[solana.PublicKey]bool),
		WritableAccts: make(map[solana.PublicKey]bool),
		Features:      &execCtx.Features,
	}

	programIndex, err := txCtx.IndexOfAccount(programId)
	if err != nil {
		return nil, nil, nil, err
	}

	return &execCtx, instrAccts, []uint64{programIndex}, nil
}

func TestConformance_VMPrograms_Firedancer(t *testing.T) {
	basePath := "test-vectors/instr/fixtures/vm-programs"

	entries, err := os.ReadDir(basePath)
	if err != nil {
		t.Skipf("test-vectors not available: %v", err)
	}

	var fixtures []string
	filter := os.Getenv("MITHRIL_CONFORMANCE_FIXTURE")
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".fix") {
			if filter != "" && !strings.Contains(entry.Name(), filter) {
				continue
			}
			fixtures = append(fixtures, filepath.Join(basePath, entry.Name()))
		}
	}
	sort.Strings(fixtures)

	if len(fixtures) == 0 {
		t.Skip("no .fix fixtures found")
	}

	t.Logf("Found %d VM program fixtures", len(fixtures))

	var (
		total                int
		pass                 int
		parseErrors          int
		setupErrors          int
		returnMismatches     int
		accountMismatches    int
		returnDataMismatches int
		panics               int
	)
	var failures []string
	panicBuckets := make(map[string]int)
	returnBuckets := make(map[string]int)

	for _, fixturePath := range fixtures {
		total++
		name := filepath.Base(fixturePath)

		data, err := os.ReadFile(fixturePath)
		if err != nil {
			parseErrors++
			failures = append(failures, fmt.Sprintf("READ_ERROR %s: %v", name, err))
			continue
		}

		fixture, err := unmarshalFiredancerInstrFixture(data)
		if err != nil {
			parseErrors++
			failures = append(failures, fmt.Sprintf("PARSE_ERROR %s: %v", name, err))
			continue
		}

		var execCtx *sealevelPkg.ExecutionCtx
		var execErr error
		var accountStateMatches bool
		var returnDataMatches bool
		var didPanic bool

		func() {
			defer func() {
				if r := recover(); r != nil {
					didPanic = true
					panics++
					panicBuckets[fmt.Sprint(r)]++
					panicMsg := fmt.Sprintf("PANIC %s: %v", name, r)
					if os.Getenv("MITHRIL_CONFORMANCE_STACKS") != "" {
						panicMsg = fmt.Sprintf("%s\n%s", panicMsg, debug.Stack())
					}
					failures = append(failures, panicMsg)
				}
			}()

			var instrAccts []sealevelPkg.InstructionAccount
			var programIndices []uint64
			execCtx, instrAccts, programIndices, err = newVMProgramExecCtxAndInstrAccts(fixture)
			if err != nil {
				setupErrors++
				failures = append(failures, fmt.Sprintf("SETUP_ERROR %s: %v", name, err))
				return
			}

			withoutConformanceStdout(func() {
				execErr = execCtx.ProcessInstruction(fixture.GetInput().GetData(), instrAccts, programIndices)
			})

			if execErr == nil {
				accountStateMatches = accountStateChangesMatch(t, execCtx, fixture)
				_, gotReturnData := execCtx.TransactionContext.ReturnData()
				returnDataMatches = bytes.Equal(gotReturnData, fixture.GetOutput().GetReturnData())
			}
		}()

		if didPanic || err != nil {
			continue
		}

		if !instrReturnMatches(fixture, execErr) {
			returnMismatches++
			returnBuckets[returnMismatchBucket(fixture, execErr)]++
			gotResult := instrResultFromErr(execErr)
			wantResult := fixture.GetOutput().GetResult()
			failures = append(failures, fmt.Sprintf("RETURN_MISMATCH %s: got=%s(%d) got_err=%v want=%s(%d) custom=%d %s",
				name, firedancerInstrResultName(gotResult), gotResult, execErr,
				firedancerInstrResultName(wantResult), wantResult, fixture.GetOutput().GetCustomErr(), fixtureProgramSummary(fixture)))
			continue
		}
		if execErr == nil && !accountStateMatches {
			accountMismatches++
			failures = append(failures, fmt.Sprintf("ACCOUNT_MISMATCH %s", name))
			continue
		}
		if execErr == nil && !returnDataMatches {
			returnDataMismatches++
			failures = append(failures, fmt.Sprintf("RETURN_DATA_MISMATCH %s", name))
			continue
		}

		pass++
	}

	sort.Strings(failures)

	t.Logf("\n=== VM Program Conformance Results ===")
	t.Logf("Total fixtures:          %d", total)
	t.Logf("Passed:                  %d", pass)
	t.Logf("Parse errors:            %d", parseErrors)
	t.Logf("Setup errors:            %d", setupErrors)
	t.Logf("Return mismatches:       %d", returnMismatches)
	t.Logf("Account mismatches:      %d", accountMismatches)
	t.Logf("Return data mismatches:  %d", returnDataMismatches)
	t.Logf("Panics:                  %d", panics)

	if len(failures) > 0 {
		limit := 50
		if envLimit := os.Getenv("MITHRIL_CONFORMANCE_FAILURE_LIMIT"); envLimit != "" {
			if parsed, err := strconv.Atoi(envLimit); err == nil && parsed > 0 {
				limit = parsed
			}
		}
		t.Logf("\n=== First %d failures ===", limit)
		if len(failures) < limit {
			limit = len(failures)
		}
		for _, failure := range failures[:limit] {
			t.Logf("  %s", failure)
		}
	}

	if len(panicBuckets) > 0 {
		t.Logf("\n=== Panic Buckets ===")
		for _, bucket := range topConformanceBuckets(panicBuckets, 20) {
			t.Logf("  %dx %s", bucket.count, bucket.key)
		}
	}

	if len(returnBuckets) > 0 {
		t.Logf("\n=== Return Mismatch Buckets ===")
		for _, bucket := range topConformanceBuckets(returnBuckets, 20) {
			t.Logf("  %dx %s", bucket.count, bucket.key)
		}
	}

	if pass != total {
		t.Errorf("VM program conformance failures: %d/%d failed", total-pass, total)
	}
}
