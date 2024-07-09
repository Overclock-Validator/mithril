package sealevel

import (
	"bytes"
	_ "embed"
	"encoding/binary"
	"testing"

	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"go.firedancer.io/radiance/pkg/accounts"
	"go.firedancer.io/radiance/pkg/cu"
	"go.firedancer.io/radiance/pkg/features"
	"go.firedancer.io/radiance/pkg/global"
)

// BPF loader tests

func TestExecute_Tx_BpfLoader_InitializeBuffer_Success(t *testing.T) {

	// buffer account
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcctData := make([]byte, 500)
	binary.LittleEndian.PutUint32(bufferAcctData, UpgradeableLoaderStateTypeUninitialized)
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: []byte(bufferAcctData), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// authority account
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: SystemProgramAddr, Executable: false, RentEpoch: 100}

	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: false, IsWritable: true}, // uninit buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}} // authority account
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)

	instrData := make([]byte, 4)
	binary.LittleEndian.AppendUint32(instrData, UpgradeableLoaderInstrTypeInitializeBuffer)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})

	assert.Equal(t, nil, err)
}

func TestExecute_Tx_BpfLoader_InitializeBuffer_Buffer_Acct_Already_Initialize_Failure(t *testing.T) {

	// buffer account
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcctData := make([]byte, 500)
	binary.LittleEndian.PutUint32(bufferAcctData, UpgradeableLoaderStateTypeBuffer) // buffer acct already initialized
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: []byte(bufferAcctData), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// authority account
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: SystemProgramAddr, Executable: false, RentEpoch: 100}

	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: false, IsWritable: true}, // already initialize buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}} // authority account
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)

	instrData := make([]byte, 4)
	binary.LittleEndian.AppendUint32(instrData, UpgradeableLoaderInstrTypeInitializeBuffer)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})

	assert.Equal(t, InstrErrAccountAlreadyInitialized, err)
}

func TestExecute_Tx_BpfLoader_Write_Success(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority account
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: SystemProgramAddr, Executable: false, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

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
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)
}

func TestExecute_Tx_BpfLoader_Write_Offset_Too_Large_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority account
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: SystemProgramAddr, Executable: false, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

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
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrAccountDataTooSmall, err)
}

func TestExecute_Tx_BpfLoader_Write_Buffer_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority account
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: SystemProgramAddr, Executable: false, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

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
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_Write_Incorrect_Authority_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// incorrect authority account
	incorrectAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	incorrectAuthorityPubkey := incorrectAuthorityPrivKey.PublicKey()
	incorrectAuthorityAcct := accounts.Account{Key: incorrectAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: SystemProgramAddr, Executable: false, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

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
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_Not_Enough_Instr_Accts_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}} // properly initialized buffer acct

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrNotEnoughAccountKeys, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_Buffer_Success(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_ProgramData_Success(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_Buffer_Immutable_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrImmutable, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_Buffer_Wrong_Upgrade_Authority_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// incorrect authority account
	incorrectAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	incorrectAuthorityPubkey := incorrectAuthorityPrivKey.PublicKey()
	incorrectAuthorityAcct := accounts.Account{Key: incorrectAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: SystemProgramAddr, Executable: false, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, incorrectAuthorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: incorrectAuthorityAcct.Key, IsSigner: true, IsWritable: true}, // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}}       // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_Buffer_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: false, IsWritable: true},   // authority for the account, but not a signer
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_Buffer_No_New_Authority_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // authority for the account
	} // no new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_Buffer_Uninitialized_Account_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // uninitialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_ProgramData_Immutable_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrImmutable, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_ProgramData_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: authorityAcct.Key, IsSigner: false, IsWritable: true},   // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_SetAuthority_ProgramData_Wrong_Authority_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// incorrect authority account
	incorrectAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	incorrectAuthorityPubkey := incorrectAuthorityPrivKey.PublicKey()
	incorrectAuthorityAcct := accounts.Account{Key: incorrectAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: SystemProgramAddr, Executable: false, RentEpoch: 100}

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
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthority)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, incorrectAuthorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: incorrectAuthorityAcct.Key, IsSigner: false, IsWritable: true}, // incorrect authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}}        // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_Not_Enough_Instr_Accts_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}} // properly initialized buffer acct

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), GlobalCtx: global.GlobalCtx{Features: *f}}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrNotEnoughAccountKeys, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_Buffer_Success(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), GlobalCtx: global.GlobalCtx{Features: *f}}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_ProgramData_Success(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), GlobalCtx: global.GlobalCtx{Features: *f}}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_Buffer_Immutable_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), GlobalCtx: global.GlobalCtx{Features: *f}}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrImmutable, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_Buffer_Wrong_Upgrade_Authority_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// incorrect authority account
	incorrectAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	incorrectAuthorityPubkey := incorrectAuthorityPrivKey.PublicKey()
	incorrectAuthorityAcct := accounts.Account{Key: incorrectAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: SystemProgramAddr, Executable: false, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, incorrectAuthorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: incorrectAuthorityAcct.Key, IsSigner: true, IsWritable: true}, // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}}       // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), GlobalCtx: global.GlobalCtx{Features: *f}}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_Buffer_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: false, IsWritable: true},   // authority for the account, but not a signer
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), GlobalCtx: global.GlobalCtx{Features: *f}}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_Buffer_New_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},     // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: false, IsWritable: true}} // new authority but not a signer
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), GlobalCtx: global.GlobalCtx{Features: *f}}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_Buffer_Uninitialized_Account_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the buffer acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 0, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // uninitialized buffer acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), GlobalCtx: global.GlobalCtx{Features: *f}}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_ProgramData_Immutable_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},    // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), GlobalCtx: global.GlobalCtx{Features: *f}}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrImmutable, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_ProgramData_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: authorityAcct.Key, IsSigner: false, IsWritable: true},   // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), GlobalCtx: global.GlobalCtx{Features: *f}}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_ProgramData_New_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, authorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},     // authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: false, IsWritable: true}} // new authority, but not a signer
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), GlobalCtx: global.GlobalCtx{Features: *f}}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_SetAuthorityChecked_ProgramData_Wrong_Authority_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the programdata acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()

	// new authority pubkey for the programdata acct
	newAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newAuthorityPubkey := newAuthorityPrivKey.PublicKey()
	newAuthorityAcct := accounts.Account{Key: newAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// incorrect authority account
	incorrectAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	incorrectAuthorityPubkey := incorrectAuthorityPrivKey.PublicKey()
	incorrectAuthorityAcct := accounts.Account{Key: incorrectAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: SystemProgramAddr, Executable: false, RentEpoch: 100}

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
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeSetAuthorityChecked)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, programDataAcct, incorrectAuthorityAcct, newAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: programDataAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: incorrectAuthorityAcct.Key, IsSigner: false, IsWritable: true}, // incorrect authority for the account
		{Pubkey: newAuthorityAcct.Key, IsSigner: true, IsWritable: true}}        // new authority
	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableBpfLoaderSetAuthorityCheckedIx, 0)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), GlobalCtx: global.GlobalCtx{Features: *f}}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_Close_Buffer_Success(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, dstAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit buffer account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}} // buffer account's authority

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	dstAcctPostInstr, err := txCtx.Accounts.GetAccount(2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1337), dstAcctPostInstr.Lamports) // ensure destination account received the buffer account's lamports (1337 lamports)

	bufferAcctPostInstr, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)
	bufferAcctStatePostInstr, err := unmarshalUpgradeableLoaderState(bufferAcctPostInstr.Data)
	assert.NoError(t, err)
	assert.Equal(t, uint32(UpgradeableLoaderStateTypeUninitialized), bufferAcctStatePostInstr.Type) // ensure that buffer acct is now uninitialized
	assert.Equal(t, uint64(0), bufferAcctPostInstr.Lamports)                                        // ensure that uninit acct now has 0 lamports
}

func TestExecute_Tx_BpfLoader_Close_Buffer_Immutable_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, dstAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit buffer account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}} // buffer account's authority

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrImmutable, err)
}

func TestExecute_Tx_BpfLoader_Close_Buffer_Authority_Didnt_Sign_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, dstAcct, authorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},        // account to deposit buffer account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: false, IsWritable: true}} // buffer account's authority

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_BpfLoader_Close_Buffer_Wrong_Authority_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()

	// incorrect authority
	// authority pubkey for the buffer acct
	incorrectAuthorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	incorrectAuthorityPubkey := incorrectAuthorityPrivKey.PublicKey()
	incorrectAuthorityAcct := accounts.Account{Key: incorrectAuthorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, dstAcct, incorrectAuthorityAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},                // account to deposit buffer account's lamports into upon close
		{Pubkey: incorrectAuthorityAcct.Key, IsSigner: true, IsWritable: true}} // incorrect buffer account authority

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectAuthority, err)
}

func TestExecute_Tx_BpfLoader_Close_Uninitialized_Success(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

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
	uninitAcct := accounts.Account{Key: uninitPubkey, Lamports: 1337, Data: uninitData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, uninitAcct, dstAcct})

	acctMetas := []AccountMeta{{Pubkey: uninitAcct.Key, IsSigner: true, IsWritable: true}, // uninitialized acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true}} // account to uninit account's lamports into upon close

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	dstAcctPostInstr, err := txCtx.Accounts.GetAccount(2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1337), dstAcctPostInstr.Lamports) // ensure destination account received the uninit account's lamports (1337 lamports)

	uninitAcctPostInstr, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)
	uninitAcctStatePostInstr, err := unmarshalUpgradeableLoaderState(uninitAcctPostInstr.Data)
	assert.NoError(t, err)
	assert.Equal(t, uint32(UpgradeableLoaderStateTypeUninitialized), uninitAcctStatePostInstr.Type) // ensure that uninit acct is still uninitialized
	assert.Equal(t, uint64(0), uninitAcctPostInstr.Lamports)                                        // ensure that uninit acct now has 0 lamports
}

func TestExecute_Tx_BpfLoader_Close_Recipient_Same_As_Account_Being_Closed_Failure(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

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
	uninitAcct := accounts.Account{Key: uninitPubkey, Lamports: 1337, Data: uninitData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, uninitAcct, uninitAcct})

	acctMetas := []AccountMeta{{Pubkey: uninitAcct.Key, IsSigner: true, IsWritable: true}, // uninitialized acct to be closed
		{Pubkey: uninitAcct.Key, IsSigner: true, IsWritable: true}} // receiving acct, but same as account being closed

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}

func TestExecute_Tx_BpfLoader_Close_Buffer_Not_Enough_Accounts(t *testing.T) {
	// bpf loader acct
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: programAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, bufferAcct, dstAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized buffer acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true}} // account to deposit buffer account's lamports into upon close

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrNotEnoughAccountKeys, err)
}

func TestExecute_Tx_BpfLoader_Close_ProgramData_Success(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: bufferPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, bufferAcct, dstAcct, authorityAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit programdata account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // programdata account's authority
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: true}}   // program acct associated with the programdata acct

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 0
	clockAcct := accounts.Account{}
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	dstAcctPostInstr, err := txCtx.Accounts.GetAccount(2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1337), dstAcctPostInstr.Lamports) // ensure destination account received the buffer account's lamports (1337 lamports)

	bufferAcctPostInstr, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)
	bufferAcctStatePostInstr, err := unmarshalUpgradeableLoaderState(bufferAcctPostInstr.Data)
	assert.NoError(t, err)
	assert.Equal(t, uint32(UpgradeableLoaderStateTypeUninitialized), bufferAcctStatePostInstr.Type) // ensure that buffer acct is now uninitialized
	assert.Equal(t, uint64(0), bufferAcctPostInstr.Lamports)                                        // ensure that uninit acct now has 0 lamports
}

func TestExecute_Tx_BpfLoader_Close_ProgramData_Not_Enough_Accounts_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: bufferPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, bufferAcct, dstAcct, authorityAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit programdata account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}} // programdata account's authority

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 0
	clockAcct := accounts.Account{}
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrNotEnoughAccountKeys, err)
}

func TestExecute_Tx_BpfLoader_Close_ProgramData_Program_Acct_Not_Writable_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: bufferPubkey}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, bufferAcct, dstAcct, authorityAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit programdata account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // programdata account's authority
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: false}}  // program acct associated with the programdata acct, but not writable

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 0
	clockAcct := accounts.Account{}
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}

func TestExecute_Tx_BpfLoader_Close_ProgramData_Program_Acct_Wrong_Owner_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

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
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: SystemProgramAddr, Executable: true, RentEpoch: 100} // wrong owner

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, bufferAcct, dstAcct, authorityAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit programdata account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // programdata account's authority
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: true}}   // program acct associated with the programdata acct, but is wrongly owned by system program instead of loader

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 0
	clockAcct := accounts.Account{}
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrIncorrectProgramId, err)
}

func TestExecute_Tx_BpfLoader_Close_ProgramData_Already_Deployed_In_This_Block_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

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
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, bufferAcct, dstAcct, authorityAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit programdata account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // programdata account's authority
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: true}}   // program acct associated with the programdata acct

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1337 // same slot as in the programdata Slot field
	clockAcct := accounts.Account{}
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}

func TestExecute_Tx_BpfLoader_Close_ProgramData_ProgramData_Not_A_Program_Acct_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

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
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

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
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, bufferAcct, dstAcct, authorityAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit programdata account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // programdata account's authority
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: true}}   // program acct associated with the programdata acct

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 0
	clockAcct := accounts.Account{}
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}

func TestExecute_Tx_BpfLoader_Close_ProgramData_Nonclosable_Account_Failure(t *testing.T) {
	// bpf loader acct
	loaderAcctData := make([]byte, 500, 500)
	loaderAcct := accounts.Account{Key: BpfLoaderUpgradeableAddr, Lamports: 0, Data: loaderAcctData, Owner: NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// authority pubkey for the buffer acct
	authorityPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authorityPubkey := authorityPrivKey.PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	// programdata account
	bufferDataWriter := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(bufferDataWriter)
	bufferAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: SystemProgramAddr}} // trying to close Program acct, which isn't possible
	err = bufferAcctState.MarshalWithEncoder(encoder)
	bufferAcctBytes := bufferDataWriter.Bytes()
	bufferData := make([]byte, 500, 500)
	copy(bufferData, bufferAcctBytes)
	bufferAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	bufferPubkey := bufferAcctPrivKey.PublicKey()
	bufferAcct := accounts.Account{Key: bufferPubkey, Lamports: 1337, Data: bufferData, Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	dstPrivkey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	dstPubkey := dstPrivkey.PublicKey()
	dstAcct := accounts.Account{Key: dstPubkey, Lamports: 0, Data: make([]byte, 0), Owner: BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

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
	programAcct := accounts.Account{Key: programPubkey, Lamports: 0, Data: programBytes, Owner: BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instrData, UpgradeableLoaderInstrTypeClose)
	transactionAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, bufferAcct, dstAcct, authorityAcct, programAcct})

	acctMetas := []AccountMeta{{Pubkey: bufferAcct.Key, IsSigner: true, IsWritable: true}, // properly initialized programdata acct
		{Pubkey: dstAcct.Key, IsSigner: true, IsWritable: true},       // account to deposit programdata account's lamports into upon close
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true}, // programdata account's authority
		{Pubkey: programAcct.Key, IsSigner: true, IsWritable: true}}   // program acct associated with the programdata acct

	instructionAccts := instructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTestTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 0
	clockAcct := accounts.Account{}
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}