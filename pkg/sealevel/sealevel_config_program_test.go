package sealevel

import (
	"bytes"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Config program tests

func TestExecute_Tx_Config_Program_Success(t *testing.T) {
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.ConfigProgramAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	configAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	configAcctPubkey := configAcctPrivKey.PublicKey()

	var configKeys []ConfigKey

	var ck ConfigKey
	ck.Pubkey = configAcctPubkey
	ck.IsSigner = true
	configKeys = append(configKeys, ck)

	ckBytes := marshalConfigKeys(configKeys)

	instrData := make([]byte, len(ckBytes)+100, len(ckBytes)+100)
	copy(instrData, ckBytes)
	instrData = append(instrData, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"...)

	acctBytes := make([]byte, len(ckBytes)+200, len(ckBytes)+200)
	copy(acctBytes, ckBytes)

	configAcct := accounts.Account{Key: configAcctPubkey, Lamports: 0, Data: acctBytes, Owner: a.ConfigProgramAddr, Executable: false, RentEpoch: 100}

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, configAcct})

	acctMetas := []AccountMeta{{Pubkey: configAcct.Key, IsSigner: true, IsWritable: true}}
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)

	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	require.NoError(t, err)
	acct, err := txCtx.Accounts.GetAccount(1)
	require.NoError(t, err)

	hasNewData := bytes.Equal(acct.Data[:len(instrData)], instrData)

	assert.Equal(t, true, hasNewData)
}

func TestExecute_Tx_Config_Program_With_Additional_Signer_Success(t *testing.T) {

	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.ConfigProgramAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	configAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	configAcctPubkey := configAcctPrivKey.PublicKey()

	authSignerPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authSignerPubKey := authSignerPrivKey.PublicKey()
	authSignerAcct := accounts.Account{Key: authSignerPubKey, Lamports: 0, Data: make([]byte, 500, 500), Owner: a.ConfigProgramAddr, Executable: false, RentEpoch: 100}

	// config account
	var configKeys []ConfigKey
	var ck ConfigKey
	ck.Pubkey = authSignerPubKey
	ck.IsSigner = true
	configKeys = append(configKeys, ck)
	acctBytes := marshalConfigKeys(configKeys)
	configAcct := accounts.Account{Key: configAcctPubkey, Lamports: 0, Data: acctBytes, Owner: a.ConfigProgramAddr, Executable: false, RentEpoch: 100}

	// instruction data
	var instrDataConfigKeys []ConfigKey
	var instrDataCk ConfigKey
	instrDataCk.Pubkey = authSignerPubKey
	instrDataCk.IsSigner = true
	instrDataConfigKeys = append(instrDataConfigKeys, instrDataCk)
	instrData := marshalConfigKeys(instrDataConfigKeys)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, configAcct, authSignerAcct})

	acctMetas := []AccountMeta{{Pubkey: configAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: authSignerAcct.Key, IsSigner: true, IsWritable: true}}
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)

	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.NoError(t, err)
}

func TestExecute_Tx_Config_Program_With_Additional_Account_But_Not_As_Signer_Failure(t *testing.T) {

	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.ConfigProgramAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	configAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	configAcctPubkey := configAcctPrivKey.PublicKey()

	authSignerPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authSignerPubKey := authSignerPrivKey.PublicKey()
	authSignerAcct := accounts.Account{Key: authSignerPubKey, Lamports: 0, Data: make([]byte, 500, 500), Owner: a.ConfigProgramAddr, Executable: false, RentEpoch: 100}

	// config account
	var configKeys []ConfigKey
	var ck ConfigKey
	ck.Pubkey = authSignerPubKey
	ck.IsSigner = true
	configKeys = append(configKeys, ck)
	acctBytes := marshalConfigKeys(configKeys)
	configAcct := accounts.Account{Key: configAcctPubkey, Lamports: 0, Data: acctBytes, Owner: a.ConfigProgramAddr, Executable: false, RentEpoch: 100}

	// instruction data
	var instrDataConfigKeys []ConfigKey
	var instrDataCk ConfigKey
	instrDataCk.Pubkey = authSignerPubKey
	instrDataCk.IsSigner = true
	instrDataConfigKeys = append(instrDataConfigKeys, instrDataCk)
	instrData := marshalConfigKeys(instrDataConfigKeys)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, configAcct, authSignerAcct})

	acctMetas := []AccountMeta{{Pubkey: configAcct.Key, IsSigner: true, IsWritable: true},
		{Pubkey: authSignerAcct.Key, IsSigner: false, IsWritable: true}}
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)

	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_Config_Program_Without_Config_Signer_Failure(t *testing.T) {
	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.ConfigProgramAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	configAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	configAcctPubkey := configAcctPrivKey.PublicKey()

	var configKeys []ConfigKey

	for i := 0; i < 5; i++ {
		var ck ConfigKey
		ck.Pubkey = configAcctPubkey
		ck.IsSigner = true
		configKeys = append(configKeys, ck)
	}

	ckBytes := marshalConfigKeys(configKeys)

	instrData := make([]byte, len(ckBytes)+100, len(ckBytes)+100)
	copy(instrData, ckBytes)
	instrData = append(instrData, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"...)

	acctBytes := make([]byte, len(ckBytes)+200, len(ckBytes)+200)
	copy(acctBytes, ckBytes)

	configAcct := accounts.Account{Key: configAcctPubkey, Lamports: 0, Data: acctBytes, Owner: a.ConfigProgramAddr, Executable: false, RentEpoch: 100}

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, configAcct})

	acctMetas := []AccountMeta{{Pubkey: configAcct.Key, IsSigner: false, IsWritable: true}}
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)

	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_Config_Program_Without_Additional_Signer_Failure(t *testing.T) {

	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.ConfigProgramAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	configAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	configAcctPubkey := configAcctPrivKey.PublicKey()
	authSignerPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authSignerPubKey := authSignerPrivKey.PublicKey()

	// config account
	var configKeys []ConfigKey
	var ck ConfigKey
	ck.Pubkey = authSignerPubKey
	ck.IsSigner = true
	configKeys = append(configKeys, ck)
	acctBytes := marshalConfigKeys(configKeys)
	configAcct := accounts.Account{Key: configAcctPubkey, Lamports: 0, Data: acctBytes, Owner: a.ConfigProgramAddr, Executable: false, RentEpoch: 100}

	// incorrect signer
	randomPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	randomPubKey := randomPrivKey.PublicKey()
	randomPubKeyAcct := accounts.Account{Key: randomPubKey, Lamports: 0, Data: acctBytes, Owner: a.ConfigProgramAddr, Executable: false, RentEpoch: 100}

	// instruction data
	var instrDataConfigKeys []ConfigKey
	var instrDataCk ConfigKey
	instrDataCk.Pubkey = randomPubKey
	instrDataCk.IsSigner = true
	configKeys = append(instrDataConfigKeys, instrDataCk)
	instrData := marshalConfigKeys(instrDataConfigKeys)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, configAcct, randomPubKeyAcct})

	acctMetas := []AccountMeta{{Pubkey: configAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: randomPubKeyAcct.Key, IsSigner: true, IsWritable: true}}
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)

	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_Config_Program_Duplicate_New_Keys_Failure(t *testing.T) {

	programAcctData := make([]byte, 500, 500)
	programAcct := accounts.Account{Key: a.ConfigProgramAddr, Lamports: 0, Data: programAcctData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	configAcctPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	configAcctPubkey := configAcctPrivKey.PublicKey()

	authSignerPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	authSignerPubKey := authSignerPrivKey.PublicKey()
	authSignerAcct := accounts.Account{Key: authSignerPubKey, Lamports: 0, Data: make([]byte, 500, 500), Owner: a.ConfigProgramAddr, Executable: false, RentEpoch: 100}

	// config account
	var configKeys []ConfigKey
	var ck ConfigKey
	ck.Pubkey = authSignerPubKey
	ck.IsSigner = true
	configKeys = append(configKeys, ck)
	configKeysData := marshalConfigKeys(configKeys)
	acctBytes := make([]byte, len(configKeysData)+500)
	copy(acctBytes, configKeysData)
	configAcct := accounts.Account{Key: configAcctPubkey, Lamports: 0, Data: acctBytes, Owner: a.ConfigProgramAddr, Executable: false, RentEpoch: 100}

	// instruction data
	var instrDataConfigKeys []ConfigKey
	var instrDataCk ConfigKey
	instrDataCk.Pubkey = authSignerPubKey
	instrDataCk.IsSigner = true
	instrDataConfigKeys = append(instrDataConfigKeys, instrDataCk)
	instrDataConfigKeys = append(instrDataConfigKeys, instrDataCk) // duplicate keys in update data - should cause a failure (InvalidArgument)
	instrData := marshalConfigKeys(instrDataConfigKeys)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, configAcct, authSignerAcct})

	acctMetas := []AccountMeta{{Pubkey: configAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: authSignerAcct.Key, IsSigner: true, IsWritable: true},
		{Pubkey: authSignerAcct.Key, IsSigner: true, IsWritable: true}}
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)

	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}
