package sealevel

import (
	"bytes"
	_ "embed"
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
)

// BPF loader tests

func TestExecute_Tx_BpfLoader_InitializeBuffer_Success(t *testing.T) {

	// buffer account
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcctData := make([]byte, 500)
	binary.LittleEndian.PutUint32(bufferAcctData, UpgradeableLoaderStateTypeUninitialized)
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: []byte(bufferAcctData), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// authority account
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: false, IsWritable: true}, // uninit buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}} // authority account
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)

	instrData := make([]byte, 4)
	binary.LittleEndian.AppendUint32(instrData, UpgradeableLoaderInstrTypeInitializeBuffer)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	// check account state after initialize instruction
	bufferAcctPost, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)
	bufferAcctPostData := bufferAcctPost.Data
	bufferAcctPostState, err := UnmarshalUpgradeableLoaderState(bufferAcctPostData)
	assert.NoError(t, err)
	assert.Equal(t, uint32(UpgradeableLoaderStateTypeBuffer), bufferAcctPostState.Type)
	assert.Equal(t, authorityAcct.Key, *bufferAcctPostState.Buffer.AuthorityAddress)
}

func TestExecute_Tx_BpfLoader_InitializeBuffer_Buffer_Acct_Already_Initialize_Failure(t *testing.T) {

	// buffer account
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcctData := make([]byte, 500)
	binary.LittleEndian.PutUint32(bufferAcctData, UpgradeableLoaderStateTypeBuffer) // buffer acct already initialized
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: []byte(bufferAcctData), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// authority account
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: false, IsWritable: true}, // already initialize buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}} // authority account
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)

	instrData := make([]byte, 4)
	binary.LittleEndian.AppendUint32(instrData, UpgradeableLoaderInstrTypeInitializeBuffer)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})

	assert.Equal(t, InstrErrAccountAlreadyInitialized, err)
}

func TestExecute_Tx_BpfLoader_Write_Success(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority account
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// bpf loader write instruction
	var writeInstr UpgradeableLoaderInstrWrite
	instrWriter := new(bytes.Buffer)
	instrEncoder := bin.NewBinEncoder(instrWriter)
	writeInstr.Offset = 20
	writeInstr.Bytes = make([]byte, 100)
	for count := 0; count < 100; count++ {
		writeInstr.Bytes[count] = 0x61
	}

	err = writeInstr.MarshalWithEncoder(instrEncoder)
	assert.NoError(t, err)

	instrData := instrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}} // authority account
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	bufferAcctPost, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)

	// check the new account state for presence of the newly written data ('a' x 100)
	startingOffset := upgradeableLoaderSizeOfBufferMetaData + uint64(writeInstr.Offset)
	bufferAcctBytesPost := bufferAcctPost.Data[startingOffset : startingOffset+uint64(len(writeInstr.Bytes))]
	isEqual := bytes.Equal(writeInstr.Bytes, bufferAcctBytesPost)
	assert.Equal(t, true, isEqual)
}

func TestExecute_Tx_BpfLoader_Write_Offset_Too_Large_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority account
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// bpf loader write instruction
	var writeInstr UpgradeableLoaderInstrWrite
	instrWriter := new(bytes.Buffer)
	instrEncoder := bin.NewBinEncoder(instrWriter)
	writeInstr.Offset = 600 // offset too large for buffer acct data size
	writeInstr.Bytes = make([]byte, 100)
	for count := 0; count < 100; count++ {
		writeInstr.Bytes[count] = 0x61
	}

	err = writeInstr.MarshalWithEncoder(instrEncoder)
	assert.NoError(t, err)

	instrData := instrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}} // authority account
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrAccountDataTooSmall, err)
}

func TestExecute_Tx_BpfLoader_Write_Buffer_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority account
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// bpf loader write instruction
	var writeInstr UpgradeableLoaderInstrWrite
	instrWriter := new(bytes.Buffer)
	instrEncoder := bin.NewBinEncoder(instrWriter)
	writeInstr.Offset = 20
	writeInstr.Bytes = make([]byte, 100)
	for count := 0; count < 100; count++ {
		writeInstr.Bytes[count] = 0x61
	}

	err = writeInstr.MarshalWithEncoder(instrEncoder)
	assert.NoError(t, err)

	instrData := instrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: false, IsWritable: true}} // authority account, not a signer
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_Write_Incorrect_Authority_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// incorrect authority account
	incorrectAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	incorrectAuthorityPubkey := incorrectAuthorityPrivKey.PublicKey()
	incorrectAuthorityAcct := accounts.Account{Key: incorrectAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// bpf loader write instruction
	var writeInstr UpgradeableLoaderInstrWrite
	instrWriter := new(bytes.Buffer)
	instrEncoder := bin.NewBinEncoder(instrWriter)
	writeInstr.Offset = 20
	writeInstr.Bytes = make([]byte, 100)
	for count := 0; count < 100; count++ {
		writeInstr.Bytes[count] = 0x61
	}

	err = writeInstr.MarshalWithEncoder(instrEncoder)
	assert.NoError(t, err)

	instrData := instrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, incorrectAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: incorrectAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // incorrec authority account for the buffer
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_Not_Enough_Instr_Accts_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	authorityPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivkey.PublicKey()

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}} // properly initialized buffer acct

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingAccount, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_Buffer_Success(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	// check account state after SetAuthority instruction; new authority addr should be newAuthorityAcct
	bufferAcctPost, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)
	bufferAcctPostData := bufferAcctPost.Data
	bufferAcctPostState, err := UnmarshalUpgradeableLoaderState(bufferAcctPostData)
	assert.NoError(t, err)
	assert.Equal(t, uint32(UpgradeableLoaderStateTypeBuffer), bufferAcctPostState.Type)
	assert.Equal(t, newAuthorityAcct.Key, *bufferAcctPostState.Buffer.AuthorityAddress)
}

func TestExecute_Tx_BpfLoader_SetAuthority_ProgramData_Success(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	programDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataWriter.Bytes()
	programDataData := make([]byte, 500, 500)
	copy(programDataData, programDataAcctBytes)
	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	// check account state after SetAuthority instruction; new authority addr should be newAuthorityAcct
	bufferAcctPost, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)
	bufferAcctPostData := bufferAcctPost.Data
	bufferAcctPostState, err := UnmarshalUpgradeableLoaderState(bufferAcctPostData)
	assert.NoError(t, err)
	assert.Equal(t, uint32(UpgradeableLoaderStateTypeProgramData), bufferAcctPostState.Type)
	assert.Equal(t, newAuthorityAcct.Key, *bufferAcctPostState.ProgramData.UpgradeAuthorityAddress)
}

func TestExecute_Tx_BpfLoader_SetAuthority_Buffer_Immutable_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: nil}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrImmutable, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_Buffer_Wrong_Upgrade_Authority_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// incorrect authority account
	incorrectAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	incorrectAuthorityPubkey := incorrectAuthorityPrivKey.PublicKey()
	incorrectAuthorityAcct := accounts.Account{Key: incorrectAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, incorrectAuthorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: incorrectAuthorityAcct.Key, IsSigner: true, IsWritable: true}, // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}}       // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_Buffer_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: false, IsWritable: true},   // authority for the account, but not a signer
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_Buffer_No_New_Authority_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // authority for the account
	} // no new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_Buffer_Uninitialized_Account_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeUninitialized} // account is uninitialized, hence ineligible for SetAuthority instr
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // uninitialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_ProgramData_Immutable_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	programDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: nil}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataWriter.Bytes()
	programDataData := make([]byte, 500, 500)
	copy(programDataData, programDataAcctBytes)
	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrImmutable, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_ProgramData_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	programDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataWriter.Bytes()
	programDataData := make([]byte, 500, 500)
	copy(programDataData, programDataAcctBytes)
	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: authorityAcct.Key, IsSigner: false, IsWritable: true},   // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_ProgramData_Wrong_Authority_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// incorrect authority account
	incorrectAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	incorrectAuthorityPubkey := incorrectAuthorityPrivKey.PublicKey()
	incorrectAuthorityAcct := accounts.Account{Key: incorrectAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// programdata account
	programDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataWriter.Bytes()
	programDataData := make([]byte, 500, 500)
	copy(programDataData, programDataAcctBytes)
	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, incorrectAuthorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: incorrectAuthorityAcct.Key, IsSigner: false, IsWritable: true}, // incorrect authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}}        // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_Not_Enough_Instr_Accts_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	authorityPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivkey.PublicKey()

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}} // properly initialized buffer acct

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{Features: *f, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingAccount, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_Buffer_Success(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{Features: *f, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	// check account state after SetAuthority instruction; new authority addr should be newAuthorityAcct
	bufferAcctPost, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)
	bufferAcctPostData := bufferAcctPost.Data
	bufferAcctPostState, err := UnmarshalUpgradeableLoaderState(bufferAcctPostData)
	assert.NoError(t, err)
	assert.Equal(t, uint32(UpgradeableLoaderStateTypeBuffer), bufferAcctPostState.Type)
	assert.Equal(t, newAuthorityAcct.Key, *bufferAcctPostState.Buffer.AuthorityAddress)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_ProgramData_Success(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	programDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataWriter.Bytes()
	programDataData := make([]byte, 500, 500)
	copy(programDataData, programDataAcctBytes)
	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{Features: *f, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	// check account state after SetAuthority instruction; new authority addr should be newAuthorityAcct
	bufferAcctPost, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)
	bufferAcctPostData := bufferAcctPost.Data
	bufferAcctPostState, err := UnmarshalUpgradeableLoaderState(bufferAcctPostData)
	assert.NoError(t, err)
	assert.Equal(t, uint32(UpgradeableLoaderStateTypeProgramData), bufferAcctPostState.Type)
	assert.Equal(t, newAuthorityAcct.Key, *bufferAcctPostState.ProgramData.UpgradeAuthorityAddress)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_Buffer_Immutable_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: nil}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{Features: *f, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrImmutable, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_Buffer_Wrong_Upgrade_Authority_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// incorrect authority account
	incorrectAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	incorrectAuthorityPubkey := incorrectAuthorityPrivKey.PublicKey()
	incorrectAuthorityAcct := accounts.Account{Key: incorrectAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, incorrectAuthorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: incorrectAuthorityAcct.Key, IsSigner: true, IsWritable: true}, // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}}       // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{Features: *f, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_Buffer_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: false, IsWritable: true},   // authority for the account, but not a signer
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{Features: *f, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_Buffer_New_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},     // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: false, IsWritable: true}} // new authority but not a signer
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{Features: *f, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_Buffer_Uninitialized_Account_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// buffer account
	bufferWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeUninitialized} // account is uninitialized, hence ineligible for SetAuthority instr
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // uninitialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{Features: *f, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_ProgramData_Immutable_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	programDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: nil}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataWriter.Bytes()
	programDataData := make([]byte, 500, 500)
	copy(programDataData, programDataAcctBytes)
	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{Features: *f, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrImmutable, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_ProgramData_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	programDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataWriter.Bytes()
	programDataData := make([]byte, 500, 500)
	copy(programDataData, programDataAcctBytes)
	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: authorityAcct.Key, IsSigner: false, IsWritable: true},   // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{Features: *f, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_ProgramData_New_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	programDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataWriter.Bytes()
	programDataData := make([]byte, 500, 500)
	copy(programDataData, programDataAcctBytes)
	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},     // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: false, IsWritable: true}} // new authority, but not a signer
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{Features: *f, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_ProgramData_Wrong_Authority_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// incorrect authority account
	incorrectAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	incorrectAuthorityPubkey := incorrectAuthorityPrivKey.PublicKey()
	incorrectAuthorityAcct := accounts.Account{Key: incorrectAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// programdata account
	programDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataWriter.Bytes()
	programDataData := make([]byte, 500, 500)
	copy(programDataData, programDataAcctBytes)
	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, incorrectAuthorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: incorrectAuthorityAcct.Key, IsSigner: false, IsWritable: true}, // incorrect authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}}        // new authority
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{Features: *f, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_Close_Buffer_Success(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// buffer account
	bufferDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferDataWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferDataWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, dstAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit buffer account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}} // buffer account's authority

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	dstAcctPostInstr, err := txCtx.Accounts.GetAccount(2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1337), dstAcctPostInstr.Lamports) // ensure destination account received the buffer account's lamports (1337 lamports)

	bufferAcctPostInstr, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)
	bufferAcctStatePostInstr, err := UnmarshalUpgradeableLoaderState(bufferAcctPostInstr.Data)
	assert.NoError(t, err)
	assert.Equal(t, uint32(UpgradeableLoaderStateTypeUninitialized), bufferAcctStatePostInstr.Type) // ensure that buffer acct is now uninitialized
	assert.Equal(t, uint64(0), bufferAcctPostInstr.Lamports)                                        // ensure that uninit acct now has 0 lamports
}

func TestExecute_Tx_BpfLoader_Close_Buffer_Immutable_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// buffer account
	bufferDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferDataWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: nil}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferDataWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, dstAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit buffer account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}} // buffer account's authority

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrImmutable, err)
}

func TestExecute_Tx_BpfLoader_Close_Buffer_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// buffer account
	bufferDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferDataWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferDataWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, dstAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},        // account to deposit buffer account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: false, IsWritable: true}} // buffer account's authority

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_Close_Buffer_Wrong_Authority_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()

	// incorrect authority
	// authority pubkey for the buffer acct
	incorrectAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	incorrectAuthorityPubkey := incorrectAuthorityPrivKey.PublicKey()
	incorrectAuthorityAcct := accounts.Account{Key: incorrectAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// buffer account
	bufferDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferDataWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferDataWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, dstAcct, incorrectAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},                // account to deposit buffer account's lamports into upon close
		{Pubkey: incorrectAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // incorrect buffer account authority

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_Close_Uninitialized_Success(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// uninit account
	uninitDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(uninitDataWriter)
	uninitAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeUninitialized}
	err := uninitAcctState.MarshalWithEncoder(encoder)
	uninitAcctBytes := uninitDataWriter.Bytes()
	uninitData := make([]byte, 4, 4)
	copy(uninitData, uninitAcctBytes)
	uninitAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	uninitPubkey := uninitAcctPrivKey.PublicKey()
	uninitAcct := accounts.Account{Key: uninitPubkey, Lamports: 1337, Data: uninitData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, uninitAcct, dstAcct})

	acctMetas := []AccountMeta{{Pubkey: uninitAcct.Key, IsSigner: true, IsWritable: true}, // uninitialized acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true}} // account to uninit account's lamports into upon close

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	dstAcctPostInstr, err := txCtx.Accounts.GetAccount(2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1337), dstAcctPostInstr.Lamports) // ensure destination account received the uninit account's lamports (1337 lamports)

	uninitAcctPostInstr, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)
	uninitAcctStatePostInstr, err := UnmarshalUpgradeableLoaderState(uninitAcctPostInstr.Data)
	assert.NoError(t, err)
	assert.Equal(t, uint32(UpgradeableLoaderStateTypeUninitialized), uninitAcctStatePostInstr.Type) // ensure that uninit acct is still uninitialized
	assert.Equal(t, uint64(0), uninitAcctPostInstr.Lamports)                                        // ensure that uninit acct now has 0 lamports
}

func TestExecute_Tx_BpfLoader_Close_Recipient_Same_As_Account_Being_Closed_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// uninit account
	uninitDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(uninitDataWriter)
	uninitAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeUninitialized}
	err := uninitAcctState.MarshalWithEncoder(encoder)
	uninitAcctBytes := uninitDataWriter.Bytes()
	uninitData := make([]byte, 4, 4)
	copy(uninitData, uninitAcctBytes)
	uninitAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	uninitPubkey := uninitAcctPrivKey.PublicKey()
	uninitAcct := accounts.Account{Key: uninitPubkey, Lamports: 1337, Data: uninitData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, uninitAcct, uninitAcct})

	acctMetas := []AccountMeta{{Pubkey: uninitAcct.Key, IsSigner: true, IsWritable: true}, // uninitialized acct to be closed
		{Pubkey: uninitAcct.Key, IsSigner: true, IsWritable: true}} // receiving acct, but same as account being closed

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}

func TestExecute_Tx_BpfLoader_Close_Buffer_Not_Enough_Accounts(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()

	// buffer account
	bufferDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferDataWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferDataWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, dstAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true}} // account to deposit buffer account's lamports into upon close

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingAccount, err)
}

func TestExecute_Tx_BpfLoader_Close_ProgramData_Success(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	bufferDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferDataWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey, Slot: 1337}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferDataWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: bufferPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, bufferAcct, dstAcct, authorityAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit programdata account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // programdata account's authority
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: true}}   // program acct associated with the programdata acct

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 0
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	dstAcctPostInstr, err := txCtx.Accounts.GetAccount(2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1337), dstAcctPostInstr.Lamports) // ensure destination account received the buffer account's lamports (1337 lamports)

	bufferAcctPostInstr, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)
	bufferAcctStatePostInstr, err := UnmarshalUpgradeableLoaderState(bufferAcctPostInstr.Data)
	assert.NoError(t, err)
	assert.Equal(t, uint32(UpgradeableLoaderStateTypeUninitialized), bufferAcctStatePostInstr.Type) // ensure that buffer acct is now uninitialized
	assert.Equal(t, uint64(0), bufferAcctPostInstr.Lamports)                                        // ensure that uninit acct now has 0 lamports
}

func TestExecute_Tx_BpfLoader_Close_ProgramData_Not_Enough_Accounts_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	bufferDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferDataWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey, Slot: 1337}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferDataWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: bufferPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, bufferAcct, dstAcct, authorityAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit programdata account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}} // programdata account's authority

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 0
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingAccount, err)
}

func TestExecute_Tx_BpfLoader_Close_ProgramData_Program_Acct_Not_Writable_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	bufferDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferDataWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey, Slot: 1337}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferDataWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: bufferPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, bufferAcct, dstAcct, authorityAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit programdata account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // programdata account's authority
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: false}}  // program acct associated with the programdata acct, but not writable

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 0
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}

func TestExecute_Tx_BpfLoader_Close_ProgramData_Program_Acct_Wrong_Owner_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	bufferDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferDataWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey, Slot: 1337}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferDataWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: bufferPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: a.SystemProgramAddr, Executable: true, RentEpoch: 100} // wrong owner

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, bufferAcct, dstAcct, authorityAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit programdata account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // programdata account's authority
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: true}}   // program acct associated with the programdata acct, but is wrongly owned by system program instead of loader

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 0
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectProgramId, err)
}

func TestExecute_Tx_BpfLoader_Close_ProgramData_Already_Deployed_In_This_Block_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	bufferDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferDataWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey, Slot: 1337}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferDataWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: bufferPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, bufferAcct, dstAcct, authorityAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit programdata account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // programdata account's authority
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: true}}   // program acct associated with the programdata acct

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1337 // same slot as in the programdata Slot field
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}

func TestExecute_Tx_BpfLoader_Close_ProgramData_ProgramData_Not_A_Program_Acct_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	bufferDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferDataWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey, Slot: 1337}}
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferDataWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeUninitialized} // uninitialized acct
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, bufferAcct, dstAcct, authorityAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit programdata account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // programdata account's authority
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: true}}   // program acct associated with the programdata acct

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 0
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}

func TestExecute_Tx_BpfLoader_Close_ProgramData_Nonclosable_Account_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	bufferDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferDataWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: a.SystemProgramAddr}} // trying to close Program acct, which isn't possible
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferDataWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: bufferPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, bufferAcct, dstAcct, authorityAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit programdata account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // programdata account's authority
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: true}}   // program acct associated with the programdata acct

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 0
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)
	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}

func TestExecute_Tx_BpfLoader_ExtendProgram_Success(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	//authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	programDataDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey, Slot: 1337}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataDataWriter.Bytes()
	validProgramBytes := fixtures.Load(t, "sbpf", "noop_aligned.so")
	programDataData := make([]byte, len(programDataAcctBytes)+len(validProgramBytes))
	copy(programDataData, programDataAcctBytes)
	copy(programDataData[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)
	origProgramDataLen := len(programDataData)

	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 100000000000, Data: programDataData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	var extendProgram UpgradeableLoaderInstrExtendProgram
	extendProgram.AdditionalBytes = 12
	extendProgramWriter := new(bytes.Buffer)
	extendProgramEncoder := bin.NewBinEncoder(extendProgramWriter)
	err = extendProgram.MarshalWithEncoder(extendProgramEncoder)
	assert.NoError(t, err)
	instrData := extendProgramWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, programDataAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // programdata acct
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: true}} // program acct associated with the programdata acct

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}

	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1234
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 1
	rent.ExemptionThreshold = 1
	rent.BurnPercent = 0

	rentAcct := accounts.Account{}
	rentAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.Accounts, rent)

	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	postAcct, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)

	// check that the account size has been incremented by 12
	assert.Equal(t, origProgramDataLen+12, len(postAcct.Data))

	// check that the program bytes following the programdata metadata are still the same as before
	lenOfOrigProgramBytes := len(programDataData[upgradeableLoaderSizeOfProgramDataMetaData:])
	for count := upgradeableLoaderSizeOfProgramDataMetaData; count < lenOfOrigProgramBytes; count++ {
		assert.Equal(t, programDataData[count], postAcct.Data[count])
	}

	// ensure the new data is filled with 0's
	for count := (upgradeableLoaderSizeOfProgramDataMetaData + lenOfOrigProgramBytes); count < (upgradeableLoaderSizeOfProgramDataMetaData + lenOfOrigProgramBytes + 12); count++ {
		assert.Equal(t, byte(0), postAcct.Data[count])
	}

	// ensure account state is correct
	postAcctState, err := UnmarshalUpgradeableLoaderState(postAcct.Data)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1234), postAcctState.ProgramData.Slot)
	assert.Equal(t, authorityPubkey, *postAcctState.ProgramData.UpgradeAuthorityAddress)
}

func TestExecute_Tx_BpfLoader_ExtendProgram_Extend_By_Zero_Bytes_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	//authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	programDataDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey, Slot: 1337}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataDataWriter.Bytes()
	validProgramBytes := fixtures.Load(t, "sbpf", "noop_aligned.so")
	programDataData := make([]byte, len(programDataAcctBytes)+len(validProgramBytes))
	copy(programDataData, programDataAcctBytes)
	copy(programDataData[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 100000000000, Data: programDataData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	var extendProgram UpgradeableLoaderInstrExtendProgram
	extendProgram.AdditionalBytes = 0
	extendProgramWriter := new(bytes.Buffer)
	extendProgramEncoder := bin.NewBinEncoder(extendProgramWriter)
	err = extendProgram.MarshalWithEncoder(extendProgramEncoder)
	assert.NoError(t, err)
	instrData := extendProgramWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, programDataAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // programdata acct
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: true}} // program acct associated with the programdata acct

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}

	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1234
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 1
	rent.ExemptionThreshold = 1
	rent.BurnPercent = 0

	rentAcct := accounts.Account{}
	rentAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.Accounts, rent)

	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidInstructionData, err)
}

func TestExecute_Tx_BpfLoader_ExtendProgram_With_Rent_Exemption_Payment_Not_Enough_Keys_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	//authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	programDataDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey, Slot: 1337}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataDataWriter.Bytes()
	validProgramBytes := fixtures.Load(t, "sbpf", "noop_aligned.so")
	programDataData := make([]byte, len(programDataAcctBytes)+len(validProgramBytes))
	copy(programDataData, programDataAcctBytes)
	copy(programDataData[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)
	//origProgramDataLen := len(programDataData)

	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	var extendProgram UpgradeableLoaderInstrExtendProgram
	extendProgram.AdditionalBytes = 200000
	extendProgramWriter := new(bytes.Buffer)
	extendProgramEncoder := bin.NewBinEncoder(extendProgramWriter)
	err = extendProgram.MarshalWithEncoder(extendProgramEncoder)
	assert.NoError(t, err)
	instrData := extendProgramWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, programDataAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // programdata acct
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: true}} // program acct associated with the programdata acct

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}

	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1234
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 1
	rent.ExemptionThreshold = 1
	rent.BurnPercent = 0

	rentAcct := accounts.Account{}
	rentAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.Accounts, rent)

	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrNotEnoughAccountKeys, err)
}

func TestExecute_Tx_BpfLoader_ExtendProgram_With_Rent_Exemption_Payment_Success(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	//authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	programDataDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey, Slot: 1337}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataDataWriter.Bytes()
	validProgramBytes := fixtures.Load(t, "sbpf", "noop_aligned.so")
	programDataData := make([]byte, len(programDataAcctBytes)+len(validProgramBytes))
	copy(programDataData, programDataAcctBytes)
	copy(programDataData[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)
	origProgramDataLen := len(programDataData)

	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}
	origProgramDataBalance := uint64(0)

	// system acct
	systemAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 0, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// payer acct
	payerPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	payerPubkey := payerPrivateKey.PublicKey()
	payerAcct := accounts.Account{Key: payerPubkey, Lamports: 1_000_000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}
	origPayerBalance := uint64(1_000_000)

	// program account
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	var extendProgram UpgradeableLoaderInstrExtendProgram
	extendProgram.AdditionalBytes = 200000
	extendProgramWriter := new(bytes.Buffer)
	extendProgramEncoder := bin.NewBinEncoder(extendProgramWriter)
	err = extendProgram.MarshalWithEncoder(extendProgramEncoder)
	assert.NoError(t, err)
	instrData := extendProgramWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, programDataAcct, programAcct, systemAcct, payerAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // programdata acct
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: true},
		{Pubkey: systemAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: payerAcct.Key, IsSigner: true, IsWritable: true}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}

	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1234
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 1
	rent.ExemptionThreshold = 1
	rent.BurnPercent = 0

	rentAcct := accounts.Account{}
	rentAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.Accounts, rent)

	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	programDataPost, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)

	// check that the account size has been incremented by 200000
	assert.Equal(t, origProgramDataLen+int(extendProgram.AdditionalBytes), len(programDataPost.Data))

	// check that the program bytes following the programdata metadata are still the same as before
	lenOfOrigProgramBytes := len(programDataData[upgradeableLoaderSizeOfProgramDataMetaData:])
	for count := upgradeableLoaderSizeOfProgramDataMetaData; count < lenOfOrigProgramBytes; count++ {
		assert.Equal(t, programDataData[count], programDataPost.Data[count])
	}

	// ensure the new data is filled with 0's
	for count := (upgradeableLoaderSizeOfProgramDataMetaData + lenOfOrigProgramBytes); count < (upgradeableLoaderSizeOfProgramDataMetaData + lenOfOrigProgramBytes + 12); count++ {
		assert.Equal(t, byte(0), programDataPost.Data[count])
	}

	// ensure account state is correct
	postAcctState, err := UnmarshalUpgradeableLoaderState(programDataPost.Data)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1234), postAcctState.ProgramData.Slot)
	assert.Equal(t, authorityPubkey, *postAcctState.ProgramData.UpgradeAuthorityAddress)

	// check that the increase in the programdata account lamports is the same as the decrease in payer acct lamports
	payerAcctPost, err := txCtx.Accounts.GetAccount(4)
	assert.NoError(t, err)

	assert.Equal(t, programDataPost.Lamports-origProgramDataBalance, origPayerBalance-payerAcctPost.Lamports)
}

func TestExecute_Tx_BpfLoader_Upgrade_Success(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	programDataDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey, Slot: 1337}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataDataWriter.Bytes()
	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()

	validProgramBytes := fixtures.Load(t, "sbpf", "noop_aligned.so")

	programDataBuffer := make([]byte, upgradeableLoaderSizeOfProgramDataMetaData+len(validProgramBytes))
	copy(programDataBuffer, programDataAcctBytes)
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataBuffer, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// buffer account containing program bytes
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	bufferStateWriter := new(bytes.Buffer)
	bufferStateEncoder := bin.NewBinEncoder(bufferStateWriter)
	err = bufferAcctState.MarshalWithEncoder(bufferStateEncoder)
	assert.NoError(t, err)
	bufferStateWriter.Write(validProgramBytes)
	bufferStateBytes := bufferStateWriter.Bytes()

	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1000000, Data: bufferStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// spill acct
	spillPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	spillPubkey := spillPrivKey.PublicKey()
	spillAcct := accounts.Account{Key: spillPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeUpgrade)

	fakeClockAcct := accounts.Account{Key: SysvarClockAddr, Lamports: 0, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}
	fakeRent := accounts.Account{Key: SysvarRentAddr, Lamports: 0, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, programDataAcct, programAcct, bufferAcct, spillAcct, fakeRent, fakeClockAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: false, IsWritable: true}, // programdata acct
		{Pubkey: programAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: bufferAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: spillAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: fakeRent.Key, IsSigner: false, IsWritable: true},      //rent
		{Pubkey: fakeClockAcct.Key, IsSigner: false, IsWritable: true}, // clock
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},  // authority
	}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}

	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1234
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 1
	rent.ExemptionThreshold = 1
	rent.BurnPercent = 0

	rentAcct := accounts.Account{Lamports: 1}
	execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.Accounts, rent)

	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	// check if program bytes have been written into the ProgramData account
	programDataAcctPost, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)
	newProgramBytes := programDataAcctPost.Data[upgradeableLoaderSizeOfProgramDataMetaData:]

	assert.Equal(t, len(newProgramBytes), len(validProgramBytes))

	for count := 0; count < len(newProgramBytes); count++ {
		assert.Equal(t, newProgramBytes[count], validProgramBytes[count])
	}
}

func TestExecute_Tx_BpfLoader_Upgrade_Buffer_Wrong_Authority_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	programDataDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(programDataDataWriter)
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey, Slot: 1337}}
	err = programDataAcctState.MarshalWithEncoder(encoder)
	programDataAcctBytes := programDataDataWriter.Bytes()
	programDataAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataAcctPrivKey.PublicKey()

	validProgramBytes := fixtures.Load(t, "sbpf", "noop_aligned.so")

	programDataBuffer := make([]byte, upgradeableLoaderSizeOfProgramDataMetaData+len(validProgramBytes))
	copy(programDataBuffer, programDataAcctBytes)
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataBuffer, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// buffer account containing program bytes
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &programDataPubkey}} // wrong authority in buffer
	bufferStateWriter := new(bytes.Buffer)
	bufferStateEncoder := bin.NewBinEncoder(bufferStateWriter)
	err = bufferAcctState.MarshalWithEncoder(bufferStateEncoder)
	assert.NoError(t, err)
	bufferStateWriter.Write(validProgramBytes)
	bufferStateBytes := bufferStateWriter.Bytes()

	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1000000, Data: bufferStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// spill acct
	spillPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	spillPubkey := spillPrivKey.PublicKey()
	spillAcct := accounts.Account{Key: spillPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeUpgrade)

	fakeClockAcct := accounts.Account{Key: SysvarClockAddr, Lamports: 1, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}
	fakeRent := accounts.Account{Key: SysvarRentAddr, Lamports: 1, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, programDataAcct, programAcct, bufferAcct, spillAcct, fakeRent, fakeClockAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: false, IsWritable: true}, // programdata acct
		{Pubkey: programAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: bufferAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: spillAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: fakeRent.Key, IsSigner: false, IsWritable: true},      //rent
		{Pubkey: fakeClockAcct.Key, IsSigner: false, IsWritable: true}, // clock
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},  // authority
	}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}

	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1234
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 1
	rent.ExemptionThreshold = 1
	rent.BurnPercent = 0
	rentAcct := accounts.Account{Lamports: 1}
	execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.Accounts, rent)

	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_DeployWithMaxDataLen_Success(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	systemAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 0, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// buffer account containing program bytes
	validProgramBytes := fixtures.Load(t, "sbpf", "noop_aligned.so")
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}
	bufferStateWriter := new(bytes.Buffer)
	bufferStateEncoder := bin.NewBinEncoder(bufferStateWriter)
	err = bufferAcctState.MarshalWithEncoder(bufferStateEncoder)
	assert.NoError(t, err)
	bufferStateWriter.Write(validProgramBytes)
	bufferStateBytes := bufferStateWriter.Bytes()

	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 20000, Data: bufferStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// spill acct
	payerPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	payerPubkey := payerPrivKey.PublicKey()
	payerAcct := accounts.Account{Key: payerPubkey, Lamports: 200000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeUninitialized}
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
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	seed := make([][]byte, 1)
	seed[0] = make([]byte, solana.PublicKeyLength)
	copy(seed[0], programPubkey[:])
	programDataPubkey, _, err := solana.FindProgramAddress(seed, a.BpfLoaderUpgradeableAddr)
	assert.NoError(t, err)
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	instrWriter := new(bytes.Buffer)
	instrEncoder := bin.NewBinEncoder(instrWriter)
	var deploy UpgradeableLoaderInstrDeployWithMaxDataLen
	deploy.MaxDataLen = 50000
	err = deploy.MarshalWithEncoder(instrEncoder)
	instrData := instrWriter.Bytes()

	fakeClockAcct := accounts.Account{Key: SysvarClockAddr, Lamports: 1, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}
	fakeRent := accounts.Account{Key: SysvarRentAddr, Lamports: 1, Data: programBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, payerAcct, programDataAcct, programAcct, bufferAcct, fakeRent, fakeClockAcct, systemAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: payerAcct.Key, IsSigner: true, IsWritable: true}, // programdata acct
		{Pubkey: programDataAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: programAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: bufferAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: fakeRent.Key, IsSigner: false, IsWritable: true},      //rent
		{Pubkey: fakeClockAcct.Key, IsSigner: false, IsWritable: true}, // clock
		{Pubkey: a.SystemProgramAddr, IsSigner: false, IsWritable: true},
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // authority
	}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}

	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1234
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 1
	rent.ExemptionThreshold = 1
	rent.BurnPercent = 0

	rentAcct := accounts.Account{}
	rentAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.Accounts, rent)

	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	// check programdata account state after instr; does it contain the new program bytes?
	programDataPost, err := txCtx.Accounts.GetAccount(2)
	assert.NoError(t, err)
	programBytesPost := programDataPost.Data[upgradeableLoaderSizeOfProgramDataMetaData:]
	for count := 0; count < len(validProgramBytes); count++ {
		assert.Equal(t, validProgramBytes[count], programBytesPost[count])
	}

	programDataStatePost, err := UnmarshalUpgradeableLoaderState(programDataPost.Data)
	assert.NoError(t, err)
	assert.Equal(t, authorityPubkey, *programDataStatePost.ProgramData.UpgradeAuthorityAddress)
}

func TestExecute_Tx_BpfLoader_Invoke_Bpf_Program_Success(t *testing.T) {

	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "get_stack_height.so")
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

	instrData := make([]byte, 0)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct})

	acctMetas := []AccountMeta{{Pubkey: programAcct.Key, IsSigner: false, IsWritable: false}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}

	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1234
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 1
	rent.ExemptionThreshold = 1
	rent.BurnPercent = 0

	rentAcct := accounts.Account{}
	rentAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.Accounts, rent)

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	initializeLegacyBankFixture(t, &execCtx)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)
}
