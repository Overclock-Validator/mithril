package sealevel

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecute_Tx_Sysvar_Instructions_Serialization_Test(t *testing.T) {

	instr1, err := newTestSetComputeUnitLimit(MaxComputeUnitLimit)
	assert.NoError(t, err)
	instr2, err := newTestSetComputeUnitLimit(1000)
	assert.NoError(t, err)

	serializedData, err := marshalInstructions([]Instruction{instr1, instr2})
	require.NoError(t, err)
	fmt.Printf("size of data: %d\n", len(serializedData))
}

func reformatHexBytes(hexBytes []byte) string {
	var str string

	str += "["
	for idx, b := range hexBytes {
		shouldHaveComma := idx != len(hexBytes)-1
		if shouldHaveComma {
			str += fmt.Sprintf("%x, ", b)
		} else {
			str += fmt.Sprintf("%x", b)
		}
	}

	str += "]"
	return str
}

func TestExecute_Tx_Sysvar_Instructions_Bpf_Test(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "solana_sbf_rust_instruction_introspection.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instr1, err := newTestSetComputeUnitLimit(0x1338)
	assert.NoError(t, err)
	instr1.Accounts = append(instr1.Accounts, AccountMeta{Pubkey: a.VoteProgramAddr, IsWritable: true, IsSigner: false})
	instr1.Accounts = append(instr1.Accounts, AccountMeta{Pubkey: a.StakeProgramAddr, IsWritable: false, IsSigner: true})
	instr2, err := newTestSetComputeUnitLimit(0x1337)
	instr2.Accounts = append(instr2.Accounts, AccountMeta{Pubkey: a.AddressLookupTableAddr, IsWritable: true, IsSigner: true})
	instr2.Accounts = append(instr2.Accounts, AccountMeta{Pubkey: a.Secp256kPrecompileAddr, IsWritable: false, IsSigner: false})
	assert.NoError(t, err)

	sysvarInstructionsData, err := marshalInstructions([]Instruction{instr1, instr2})
	require.NoError(t, err)
	sysvarInstructionsAcct := accounts.Account{Key: SysvarInstructionsAddr, Lamports: 1, Data: sysvarInstructionsData, Owner: a.SysvarOwnerAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 1)
	instrData[0] = 0

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, sysvarInstructionsAcct})

	acctMetas := []AccountMeta{{Pubkey: sysvarInstructionsAcct.Key, IsSigner: false, IsWritable: false}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.Curve25519SyscallEnabled, 0)
	execCtx.Features = *f

	execCtx.Accounts = accounts.NewMemAccounts()

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.NoError(t, err)

	// instruction 1 program id & instr data
	expected := fmt.Sprintf("Program log: instruction1 program_id: %s", instr1.ProgramId)
	assert.Equal(t, expected, log.Logs[0])
	expected = fmt.Sprintf("Program log: instruction1 data: %s", reformatHexBytes(instr1.Data))
	assert.Equal(t, expected, log.Logs[1])

	// instruction 1, account 1
	expected = fmt.Sprintf("Program log: instruction1 account 1: pubkey: %s", instr1.Accounts[0].Pubkey)
	assert.Equal(t, expected, log.Logs[2])
	expected = fmt.Sprintf("Program log: instruction1 account 1: is_writable: %t", instr1.Accounts[0].IsWritable)
	assert.Equal(t, expected, log.Logs[3])
	expected = fmt.Sprintf("Program log: instruction1 account 1: is_signer: %t", instr1.Accounts[0].IsSigner)
	assert.Equal(t, expected, log.Logs[4])

	// instruction 1, account 2
	expected = fmt.Sprintf("Program log: instruction1 account 2: pubkey: %s", instr1.Accounts[1].Pubkey)
	assert.Equal(t, expected, log.Logs[5])
	expected = fmt.Sprintf("Program log: instruction1 account 2: is_writable: %t", instr1.Accounts[1].IsWritable)
	assert.Equal(t, expected, log.Logs[6])
	expected = fmt.Sprintf("Program log: instruction1 account 2: is_signer: %t", instr1.Accounts[1].IsSigner)
	assert.Equal(t, expected, log.Logs[7])

	// instruction 2 program id & instr data
	expected = fmt.Sprintf("Program log: instruction2 program_id: %s", instr2.ProgramId)
	assert.Equal(t, expected, log.Logs[8])
	expected = fmt.Sprintf("Program log: instruction2 data: %s", reformatHexBytes(instr2.Data))
	assert.Equal(t, expected, log.Logs[9])

	// instruction 2, account 1
	expected = fmt.Sprintf("Program log: instruction2 account 1: pubkey: %s", instr2.Accounts[0].Pubkey)
	assert.Equal(t, expected, log.Logs[10])
	expected = fmt.Sprintf("Program log: instruction2 account 1: is_writable: %t", instr2.Accounts[0].IsWritable)
	assert.Equal(t, expected, log.Logs[11])
	expected = fmt.Sprintf("Program log: instruction2 account 1: is_signer: %t", instr2.Accounts[0].IsSigner)
	assert.Equal(t, expected, log.Logs[12])

	// instruction 2, account 2
	expected = fmt.Sprintf("Program log: instruction2 account 2: pubkey: %s", instr2.Accounts[1].Pubkey)
	assert.Equal(t, expected, log.Logs[13])
	expected = fmt.Sprintf("Program log: instruction2 account 2: is_writable: %t", instr2.Accounts[1].IsWritable)
	assert.Equal(t, expected, log.Logs[14])
	expected = fmt.Sprintf("Program log: instruction2 account 2: is_signer: %t", instr2.Accounts[1].IsSigner)
	assert.Equal(t, expected, log.Logs[15])

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func TestMarshalInstructionsRejectsInstructionOffsetOverflow(t *testing.T) {
	makeInstruction := func(numAccountRefs int, data []byte) Instruction {
		accountMeta := AccountMeta{Pubkey: a.SystemProgramAddr}
		instruction := Instruction{
			ProgramId: a.SystemProgramAddr,
			Accounts:  make([]AccountMeta, numAccountRefs),
			Data:      data,
		}
		for i := range instruction.Accounts {
			instruction.Accounts[i] = accountMeta
		}
		return instruction
	}
	makeInstructions := func(paddingData []byte) []Instruction {
		instructions := make([]Instruction, 0, 9)
		for range 7 {
			instructions = append(instructions, makeInstruction(255, nil))
		}
		instructions = append(instructions, makeInstruction(191, paddingData))
		instructions = append(instructions, makeInstruction(255, nil))
		return instructions
	}

	boundaryData := make([]byte, 19)
	serialized, err := marshalInstructions(makeInstructions(boundaryData))
	require.NoError(t, err)
	require.Greater(t, len(serialized), math.MaxUint16)

	overflowData := make([]byte, 20)
	_, err = marshalInstructions(makeInstructions(overflowData))
	require.ErrorIs(t, err, ErrInstructionsSysvarInstructionIndexOutOfBounds)

	_, err = MakeInstructionsSysvarAccount(makeInstructions(overflowData))
	require.ErrorIs(t, err, ErrInstructionsSysvarInstructionIndexOutOfBounds)
}
