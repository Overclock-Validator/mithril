package conformance

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
)

func fixtureAcctStateToAccount(acctState *AcctState) accounts.Account {
	var acct accounts.Account
	// See createProgramAcct: the field is gone from the schema, and its implied
	// value for a converted account is the maximum, not zero.
	acct.RentEpoch = math.MaxUint64
	acct.Key = solana.PublicKeyFromBytes(acctState.Address[:])
	acct.Lamports = acctState.Lamports
	acct.Data = acctState.Data
	acct.Executable = acctState.Executable
	copy(acct.Owner[:], acctState.Owner)
	return acct
}

// readFixtureDirOrSkip lists a fixture directory, skipping the test when the
// pinned corpus does not carry that set.
//
// Upstream removed the native address-lookup-table, config and stake fixtures;
// those programs are BPF-migrated on mainnet and their coverage now lives in
// unit tests. The suites that read them asserted the directory existed, so they
// failed permanently against any corpus revision that no longer ships it. A
// missing fixture set is a property of the pinned revision, not a defect in
// Mithril, so skipping is the accurate signal.
func readFixtureDirOrSkip(t *testing.T, basePath string) []os.FileInfo {
	t.Helper()
	fileInfos, err := ioutil.ReadDir(basePath)
	if err != nil {
		t.Skipf("fixture set %q not present in the pinned corpus: %v", basePath, err)
	}
	return fileInfos
}

// readFixtureOrSkip reads a single fixture, skipping when it is absent.
//
// The single-testcase variants previously called log.Fatalln here, which exits
// the process rather than failing the test: one missing fixture killed the
// whole binary and every test after it silently never ran. That is the same
// failure mode that once hid 19 BPF-loader failures behind a panicking test.
func readFixtureOrSkip(t *testing.T, fname string) []byte {
	t.Helper()
	in, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Skipf("fixture %q not present in the pinned corpus: %v", fname, err)
	}
	return in
}

func createProgramAcct(programId []byte) accounts.Account {
	programKey := solana.PublicKeyFromBytes(programId)
	// protosol v5.4.0 dropped rent_epoch from AcctState, but converted accounts
	// are defined as carrying the maximum value rather than zero. Leaving the Go
	// zero here would silently model every account as rent-paying.
	programAcct := accounts.Account{Key: programKey, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: math.MaxUint64}
	return programAcct
}

// fixtureDemotesProgramID reports whether a key called as this fixture's
// program must be demoted to read-only.
//
// Agave's harness does not apply this itself — it builds an Instruction from
// the fixture's raw flags (svm/src/conformance/instr/context.rs). The demotion
// happens downstream: execute_instr_with_callback obtains a sanitized_message
// from prepare_invoke_context_fields, and process_message ->
// prepare_top_level_instructions builds each InstructionAccount with
// message.is_writable(index), whose cache applies
//
//	is_program_id_write_demoted = is_key_called_as_program(..)
//	                              && !is_upgradeable_loader_present(account_keys)
//
// (solana-message 4.2.3, src/lib.rs:198-203).
//
// Mithril reaches the runtime without a message layer here, so the rule has to
// be applied where the instruction accounts are built. pkg/replay already
// implements the identical rule for the production path
// (pkg/replay/transaction.go:114 and isWritableForInstr at :195); this keeps
// the conformance harness consistent with it.
//
// Verified against Agave v4.2.0-rc.0 (the Alpenglow cluster release) by running
// fixture 008b7940dedeec1f02b4c38e6f1b1c4be741e7c6_3491250 through Agave's own
// harness with the privilege loop instrumented. Agave reported
// caller(writable=false) for the program account where Mithril reported true,
// which is exactly this demotion.
func fixtureDemotesProgramID(fixture *InstrFixture) bool {
	for _, acct := range fixture.Input.Accounts {
		if bytes.Equal(acct.Address, a.BpfLoaderUpgradeableAddr[:]) {
			return false
		}
	}
	return true
}

func instructionAcctsFromFixture(fixture *InstrFixture, transactionAccts sealevel.TransactionAccounts) []sealevel.InstructionAccount {
	accts := fixture.Input.Accounts
	fixtureInstrAccts := fixture.Input.InstrAccounts

	programID := fixture.Input.ProgramId
	demoteProgramID := fixtureDemotesProgramID(fixture)

	acctMetas := make([]sealevel.AccountMeta, 0)
	for count := 0; count < len(fixtureInstrAccts); count++ {
		thisInstrAcct := fixtureInstrAccts[count]
		acctKey := accts[thisInstrAcct.Index].Address

		isWritable := fixtureInstrAccts[count].IsWritable
		if demoteProgramID && bytes.Equal(acctKey, programID) {
			isWritable = false
		}

		acctMeta := sealevel.AccountMeta{Pubkey: solana.PublicKeyFromBytes(acctKey), IsSigner: fixtureInstrAccts[count].IsSigner, IsWritable: isWritable}
		acctMetas = append(acctMetas, acctMeta)
	}

	instructionAccts := sealevel.InstructionAcctsFromAccountMetas(acctMetas, transactionAccts)
	return instructionAccts
}

func configureSysvars(execCtx *sealevel.ExecutionCtx, fixture *InstrFixture) {
	configureSysvarsWithDefaults(execCtx, fixture, true)
}

func configureSysvarsFromFixture(execCtx *sealevel.ExecutionCtx, fixture *InstrFixture) {
	configureSysvarsWithDefaults(execCtx, fixture, false)
}

func configureSysvarsWithDefaults(execCtx *sealevel.ExecutionCtx, fixture *InstrFixture, synthesizeDefaults bool) {
	/// clock
	var foundClockSysvar bool
	for _, acct := range fixture.Input.Accounts {
		if solana.PublicKeyFromBytes(acct.Address) == sealevel.SysvarClockAddr {
			clockAcct := fixtureAcctStateToAccount(acct)
			fmt.Printf("adding state for sysvar: Clock. len of sysvar data = %d, len of sysvar struct %d\n", len(clockAcct.Data), sealevel.SysvarClockStructLen)

			if clockAcct.Lamports != 0 {
				execCtx.Accounts.SetAccount(&sealevel.SysvarClockAddr, &clockAcct)
			}

			_, err := sealevel.ReadClockSysvar(execCtx)
			if err == nil {
				foundClockSysvar = true
			}
		}
	}

	if !foundClockSysvar && synthesizeDefaults {
		fmt.Printf("******** setting default clock sysvar\n")
		var clock sealevel.SysvarClock
		clock.Slot = 10
		clockAcct := accounts.Account{}
		clockAcct.Lamports = 1
		execCtx.Accounts.SetAccount(&sealevel.SysvarClockAddr, &clockAcct)
		sealevel.WriteClockSysvar(&execCtx.Accounts, clock)
	}

	clock, _ := sealevel.ReadClockSysvar(execCtx)
	fmt.Printf("clock sysvar just set: %+v\n", clock)

	/// rent
	var foundRentSysvar bool
	for _, acct := range fixture.Input.Accounts {
		if solana.PublicKeyFromBytes(acct.Address) == sealevel.SysvarRentAddr {
			fmt.Printf("adding state for sysvar: Rent\n")
			rentAcct := fixtureAcctStateToAccount(acct)
			fmt.Printf("len: %d\n", len(rentAcct.Data))

			execCtx.Accounts.SetAccount(&sealevel.SysvarRentAddr, &rentAcct)

			_, err := sealevel.ReadRentSysvar(execCtx)
			if err == nil {
				foundRentSysvar = true
			}
			break
		}
	}

	if !foundRentSysvar && synthesizeDefaults {
		var rent sealevel.SysvarRent
		rent.LamportsPerUint8Year = 3480
		rent.ExemptionThreshold = 2.0
		rent.BurnPercent = 50

		rentAcct := accounts.Account{}
		rentAcct.Lamports = 1
		execCtx.Accounts.SetAccount(&sealevel.SysvarRentAddr, &rentAcct)
		sealevel.WriteRentSysvar(&execCtx.Accounts, rent)
	}

	rent, _ := sealevel.ReadRentSysvar(execCtx)
	execCtx.TransactionContext.Rent = rent

	/// SlotHashes
	for _, acct := range fixture.Input.Accounts {
		if solana.PublicKeyFromBytes(acct.Address) == sealevel.SysvarSlotHashesAddr {
			slotHashesAcct := fixtureAcctStateToAccount(acct)
			fmt.Printf("adding state for sysvar: SlotHashes (len = %d)\n", len(slotHashesAcct.Data))
			execCtx.Accounts.SetAccount(&sealevel.SysvarSlotHashesAddr, &slotHashesAcct)
		}
	}

	/// StakeHistory
	for _, acct := range fixture.Input.Accounts {
		if solana.PublicKeyFromBytes(acct.Address) == sealevel.SysvarStakeHistoryAddr {
			fmt.Printf("adding state for sysvar: StakeHistory\n")
			stakeHistoryAcct := fixtureAcctStateToAccount(acct)
			execCtx.Accounts.SetAccount(&sealevel.SysvarStakeHistoryAddr, &stakeHistoryAcct)
		}
	}

	/// EpochSchedule
	var foundEpochScheduleSysvar bool
	for _, acct := range fixture.Input.Accounts {
		if solana.PublicKeyFromBytes(acct.Address) == sealevel.SysvarEpochScheduleAddr {
			fmt.Printf("adding state for sysvar: SysvarEpochSchedule\n")
			epochScheduleAcct := fixtureAcctStateToAccount(acct)
			var epochSchedule sealevel.SysvarEpochSchedule
			err := epochSchedule.UnmarshalWithDecoder(bin.NewBinDecoder(epochScheduleAcct.Data))
			if err == nil {
				execCtx.Accounts.SetAccount(&sealevel.SysvarEpochScheduleAddr, &epochScheduleAcct)
				foundEpochScheduleSysvar = true
			}

		}
	}

	if !foundEpochScheduleSysvar && synthesizeDefaults {
		fmt.Printf("******** adding default epoch schedule sysvar\n")
		epochSchedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000, Warmup: true, FirstNormalEpoch: 14, FirstNormalSlot: 524256}

		epochScheduleAcct := accounts.Account{}
		epochScheduleAcct.Lamports = 1
		execCtx.Accounts.SetAccount(&sealevel.SysvarEpochScheduleAddr, &epochScheduleAcct)
		sealevel.WriteEpochScheduleSysvar(&execCtx.Accounts, epochSchedule)
	}

	/// EpochRewards
	for _, acct := range fixture.Input.Accounts {
		if solana.PublicKeyFromBytes(acct.Address) == sealevel.SysvarEpochRewardsAddr {
			fmt.Printf("adding state for sysvar: SysvarEpochRewards\n")
			epochRewardsAcct := fixtureAcctStateToAccount(acct)
			var epochRewards sealevel.SysvarEpochRewards
			if err := epochRewards.UnmarshalWithDecoder(bin.NewBinDecoder(epochRewardsAcct.Data)); err == nil {
				execCtx.Accounts.SetAccount(&sealevel.SysvarEpochRewardsAddr, &epochRewardsAcct)
			}
		}
	}

	/// LastRestartSlot
	for _, acct := range fixture.Input.Accounts {
		if solana.PublicKeyFromBytes(acct.Address) == sealevel.SysvarLastRestartSlotAddr {
			lastRestartSlotAcct := fixtureAcctStateToAccount(acct)
			if len(lastRestartSlotAcct.Data) == sealevel.SysvarLastRestartSlotStructLen {
				execCtx.Accounts.SetAccount(&sealevel.SysvarLastRestartSlotAddr, &lastRestartSlotAcct)
			}
		}
	}

	/// RecentBlockhashes
	for _, acct := range fixture.Input.Accounts {
		if solana.PublicKeyFromBytes(acct.Address) == sealevel.SysvarRecentBlockHashesAddr {
			fmt.Printf("adding state for sysvar: SysvarRecentBlockhashes\n")
			recentBlockhashesAcct := fixtureAcctStateToAccount(acct)
			execCtx.Accounts.SetAccount(&sealevel.SysvarRecentBlockHashesAddr, &recentBlockhashesAcct)
			rbh, err := sealevel.ReadRecentBlockHashesSysvar(execCtx)
			if err == nil {
				if len(rbh) != 0 {
					execCtx.Blockhash = rbh[len(rbh)-1].Blockhash
					execCtx.PrevLamportsPerSignature = rbh[len(rbh)-1].FeeCalculator.LamportsPerSignature
				}
			} else {
				execCtx.PrevLamportsPerSignature = 5000
			}
		}
	}
}

func parseAndConfigureFeatures(execCtx *sealevel.ExecutionCtx, fixture *InstrFixture) {
	f := features.NewFeaturesDefault()
	execCtx.Features = *f

	// Not every fixture carries an epoch context; the precompile corpus mostly
	// does not. Treat a missing one as "no features beyond the defaults" rather
	// than dereferencing through it.
	if fixture.Input == nil || fixture.Input.Features == nil {
		return
	}

	verbose := os.Getenv("MITHRIL_CONFORMANCE_VERBOSE") != ""
	for _, ftr := range fixture.Input.Features.Features {
		for _, featureGate := range features.AllFeatureGates {
			featureIdInt := binary.LittleEndian.Uint64(featureGate.Address[:8])
			if featureIdInt == ftr {
				if verbose {
					fmt.Printf("enabling feature %s\n", featureGate.Name)
				}
				execCtx.Features.EnableFeature(featureGate, 0)
			}
		}
	}
}

func newExecCtxAndInstrAcctsFromFixture(fixture *InstrFixture) (*sealevel.ExecutionCtx, []sealevel.InstructionAccount) {

	programAcct := createProgramAcct(fixture.Input.ProgramId)
	accts := make([]accounts.Account, 0)

	for count := 0; count < len(fixture.Input.Accounts); count++ {
		acct := fixtureAcctStateToAccount(fixture.Input.Accounts[count])
		accts = append(accts, acct)
	}

	acctsForTx := make([]accounts.Account, 0)
	acctsForTx = append(acctsForTx, programAcct)
	acctsForTx = append(acctsForTx, accts...)

	transactionAccts := sealevel.NewTransactionAccounts(acctsForTx)
	instrAccts := instructionAcctsFromFixture(fixture, *transactionAccts)

	txCtx := sealevel.NewTransactionCtx(*transactionAccts, 5, 64)
	instr := sealevel.Instruction{Data: fixture.Input.Data}
	txCtx.AllInstructions = append(txCtx.AllInstructions, instr)

	execCtx := sealevel.ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(fixture.Input.CuAvail), Log: &sealevel.LogRecorder{}}
	execCtx.Accounts = accounts.NewMemAccounts()
	configureSysvars(&execCtx, fixture)
	parseAndConfigureFeatures(&execCtx, fixture)

	return &execCtx, instrAccts
}

func returnValueIsExpectedValue(fixture *InstrFixture, err error) bool {
	fmt.Printf("assertReturnValueIsExpected: err %s, result %d, customErr %d\n", err, fixture.Output.Result, fixture.Output.CustomErr)
	if err == nil && fixture.Output.Result == 0 {
		fmt.Printf("err == nil && fixture.Output.Result == 0\n")
		return true
	} else if err == nil && fixture.Output.Result != 0 {
		fmt.Printf("mithril returned success, and testcase reported %d\n", fixture.Output.Result)
		return false
	}

	// for errors other than instruction errors, the custom error field is used
	if fixture.Output.Result == 26 && sealevel.IsCustomErr(err) {
		return uint32(sealevel.TranslateErrToErrCode(err)) == fixture.Output.CustomErr
	} else {
		// plus 1 because firedancer err codes are deliberately off-by-one to allow for signaling success via 0
		// whilst InstrErrGenericErr is also 0 in Agave
		returnedSolanaErrCode := int32(sealevel.TranslateErrToErrCode(err) + 1)
		matches := fixture.Output.Result == returnedSolanaErrCode
		if !matches {
			fmt.Printf("mithril returned %s, result %d, customErr %d\n", err, fixture.Output.Result, fixture.Output.CustomErr)
			return false
		} else {
			return true
		}
	}
}

func precompileReturnValueIsExpectedValue(fixture *InstrFixture, err error) bool {
	if err == nil && fixture.Output.Result == 0 {
		fmt.Printf("err == nil && fixture.Output.Result == 0\n")
		return true
	} else if err == nil && fixture.Output.Result != 0 {
		fmt.Printf("mithril returned success, and testcase reported %d\n", fixture.Output.Result)
		return false
	} else if fixture.Output.Result == 0 && err != nil {
		return false
	}

	returnedSolanaErrCode := int32(sealevel.TranslateErrToErrCode(err) + 1)
	matches := fixture.Output.Result == returnedSolanaErrCode
	if !matches {
		fmt.Printf("mithril returned %s, result %d\n", err, fixture.Output.Result-1)
		return false
	} else {
		return true
	}

}

func accountStateChangesMatch(t *testing.T, execCtx *sealevel.ExecutionCtx, fixture *InstrFixture) bool {
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
		for modifiedAcctIdx, fixtureModifiedAcct := range fixture.Output.ModifiedAccounts {
			if solana.PublicKeyFromBytes(fixtureModifiedAcct.Address) == mithrilModifiedAcct.Key {
				modifiedAcctFoundInTestcase = true
				if fixtureModifiedAcct.Lamports != mithrilModifiedAcct.Lamports {
					return false
				}
				if fixtureModifiedAcct.Executable != mithrilModifiedAcct.Executable {
					return false
				}
				// AcctState dropped rent_epoch in protosol v5.4.0, so the
				// corpus no longer carries an expected value to compare.
				if solana.PublicKeyFromBytes(fixtureModifiedAcct.Owner[:]) != solana.PublicKeyFromBytes(mithrilModifiedAcct.Owner[:]) {
					return false
				}

				if !bytes.Equal(fixtureModifiedAcct.Data, mithrilModifiedAcct.Data) {
					fmt.Printf("**** %d: account states did not match\n", modifiedAcctIdx)
					fmt.Printf("\na (%d bytes): %+v\n\n", len(mithrilModifiedAcct.Data), mithrilModifiedAcct.Data)
					fmt.Printf("b (%d bytes): %+v\n\n", len(fixtureModifiedAcct.Data), fixtureModifiedAcct.Data)

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

func instrCodeFromFixtureInstrData(fixture *InstrFixture) int32 {
	var instrCode int32 = -1
	if len(fixture.Input.Data) >= 4 {
		instrCode = int32(binary.LittleEndian.Uint32(fixture.Input.Data[0:4]))
	}
	return instrCode
}

func printFixtureInfo(fixture *InstrFixture) {
	instrCode := instrCodeFromFixtureInstrData(fixture)
	fmt.Printf("instruction code: %d\n", instrCode)

	for idx, acct := range fixture.Input.Accounts {
		fmt.Printf("txAcct %d: %s, Owner: %s, Lamports: %d\n", idx, solana.PublicKeyFromBytes(acct.Address), solana.PublicKeyFromBytes(acct.Owner), acct.Lamports)
	}

	for idx, acct := range fixture.Input.InstrAccounts {
		fmt.Printf("instrAcct %d: %s, isSigner: %t, isWritable: %t, Executable: %t, Owner: %s, Lamports: %d\n", idx, solana.PublicKeyFromBytes(fixture.Input.Accounts[acct.Index].Address), acct.IsSigner, acct.IsWritable, fixture.Input.Accounts[acct.Index].Executable, solana.PublicKeyFromBytes(fixture.Input.Accounts[acct.Index].Owner), fixture.Input.Accounts[acct.Index].Lamports)
	}
}
