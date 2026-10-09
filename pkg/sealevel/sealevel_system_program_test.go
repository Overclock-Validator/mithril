package sealevel

import (
	"bytes"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
)

func marshalCreateAccountAllowPrefundInstr(t *testing.T, lamports uint64, space uint64, owner solana.PublicKey) []byte {
	t.Helper()

	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	err := encoder.WriteUint32(SystemProgramInstrTypeCreateAccountAllowPrefund, bin.LE)
	assert.NoError(t, err)

	err = encoder.WriteUint64(lamports, bin.LE)
	assert.NoError(t, err)

	err = encoder.WriteUint64(space, bin.LE)
	assert.NoError(t, err)

	err = encoder.WriteBytes(owner[:], false)
	assert.NoError(t, err)

	return buf.Bytes()
}

func newSystemProgramTestExecCtx(t *testing.T, transactionAccts *TransactionAccounts, enabledFeatures ...features.FeatureGate) (*TransactionCtx, *ExecutionCtx) {
	t.Helper()

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := &ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}

	execCtx.Accounts = accounts.NewMemAccounts()

	var clock SysvarClock
	clock.Slot = 1234
	clockAcct := accounts.Account{Lamports: 1}
	err := execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	assert.NoError(t, err)
	WriteClockSysvar(&execCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 1
	rent.ExemptionThreshold = 1
	rent.BurnPercent = 0

	rentAcct := accounts.Account{Lamports: 1}
	err = execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	assert.NoError(t, err)
	WriteRentSysvar(&execCtx.Accounts, rent)

	f := features.NewFeaturesDefault()
	for _, feature := range enabledFeatures {
		f.EnableFeature(feature, 0)
	}
	execCtx.Features = *f

	return txCtx, execCtx
}

func TestExecute_Tx_System_Program_CreateAccount_Success(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// funding acct
	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()
	fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// new acct
	newAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newPubkey := newAcctPrivateKey.PublicKey()
	newAcct := accounts.Account{Key: newPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	var createAcct SystemInstrCreateAccount
	createAcct.Lamports = 1234
	createAcct.Owner = a.BpfLoaderUpgradeableAddr
	createAcct.Space = 1234

	createAcctInstrWriter := new(bytes.Buffer)
	createAcctEncoder := bin.NewBinEncoder(createAcctInstrWriter)

	err = createAcct.MarshalWithEncoder(createAcctEncoder)
	assert.NoError(t, err)
	instrBytes := createAcctInstrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, fundingAcct, newAcct})

	acctMetas := []AccountMeta{{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true},
		{Pubkey: newAcct.Key, IsSigner: true, IsWritable: true}}

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

	//mlog.Log.Debugf("pubkey: %s, %s", fundingAcct.Key, newAcct.Key)
	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.NoError(t, err)

	newAcctPost, err := txCtx.Accounts.GetAccount(2)
	assert.NoError(t, err)

	// check new account has lamports, space and owner as expected
	assert.Equal(t, createAcct.Lamports, newAcctPost.Lamports)
	assert.Equal(t, createAcct.Space, uint64(len(newAcctPost.Data)))
	assert.Equal(t, createAcct.Owner, solana.PublicKeyFromBytes(newAcctPost.Owner[:]))

	fundingAcctPost, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)

	// check that the funder account balance has changed accordingly
	assert.Equal(t, fundingAcct.Lamports-createAcct.Lamports, fundingAcctPost.Lamports)
}

func TestExecute_Tx_System_Program_CreateAccount_Not_Enough_Accts_Failure(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// funding acct
	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()
	fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	var createAcct SystemInstrCreateAccount
	createAcct.Lamports = 1234
	createAcct.Owner = a.BpfLoaderUpgradeableAddr
	createAcct.Space = 1234

	createAcctInstrWriter := new(bytes.Buffer)
	createAcctEncoder := bin.NewBinEncoder(createAcctInstrWriter)

	err = createAcct.MarshalWithEncoder(createAcctEncoder)
	assert.NoError(t, err)
	instrBytes := createAcctInstrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, fundingAcct})

	acctMetas := []AccountMeta{{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true}}

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

	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingAccount, err)
}

func TestExecute_Tx_System_Program_CreateAccount_New_Acct_Has_Lamports_Failure(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// funding acct
	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()
	fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// new acct
	newAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newPubkey := newAcctPrivateKey.PublicKey()
	newAcct := accounts.Account{Key: newPubkey, Lamports: 1000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	var createAcct SystemInstrCreateAccount
	createAcct.Lamports = 1234
	createAcct.Owner = a.BpfLoaderUpgradeableAddr
	createAcct.Space = 1234

	createAcctInstrWriter := new(bytes.Buffer)
	createAcctEncoder := bin.NewBinEncoder(createAcctInstrWriter)

	err = createAcct.MarshalWithEncoder(createAcctEncoder)
	assert.NoError(t, err)
	instrBytes := createAcctInstrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, fundingAcct, newAcct})

	acctMetas := []AccountMeta{{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true},
		{Pubkey: newAcct.Key, IsSigner: true, IsWritable: true}}

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

	//mlog.Log.Debugf("pubkey: %s, %s", fundingAcct.Key, newAcct.Key)
	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, SystemProgErrAccountAlreadyInUse, err)
}

func TestExecute_Tx_System_Program_CreateAccount_New_Acct_Not_Signer_Failure(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// funding acct
	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()
	fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// new acct
	newAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newPubkey := newAcctPrivateKey.PublicKey()
	newAcct := accounts.Account{Key: newPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	var createAcct SystemInstrCreateAccount
	createAcct.Lamports = 1234
	createAcct.Owner = a.BpfLoaderUpgradeableAddr
	createAcct.Space = 1234

	createAcctInstrWriter := new(bytes.Buffer)
	createAcctEncoder := bin.NewBinEncoder(createAcctInstrWriter)

	err = createAcct.MarshalWithEncoder(createAcctEncoder)
	assert.NoError(t, err)
	instrBytes := createAcctInstrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, fundingAcct, newAcct})

	acctMetas := []AccountMeta{{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true},
		{Pubkey: newAcct.Key, IsSigner: false, IsWritable: true}}

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

	//mlog.Log.Debugf("pubkey: %s, %s", fundingAcct.Key, newAcct.Key)
	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_System_Program_CreateAccount_Too_Much_Space_Allocated_Failure(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// funding acct
	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()
	fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// new acct
	newAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newPubkey := newAcctPrivateKey.PublicKey()
	newAcct := accounts.Account{Key: newPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	var createAcct SystemInstrCreateAccount
	createAcct.Lamports = 1234
	createAcct.Owner = a.BpfLoaderUpgradeableAddr
	createAcct.Space = SystemProgMaxPermittedDataLen + 10

	createAcctInstrWriter := new(bytes.Buffer)
	createAcctEncoder := bin.NewBinEncoder(createAcctInstrWriter)

	err = createAcct.MarshalWithEncoder(createAcctEncoder)
	assert.NoError(t, err)
	instrBytes := createAcctInstrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, fundingAcct, newAcct})

	acctMetas := []AccountMeta{{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true},
		{Pubkey: newAcct.Key, IsSigner: true, IsWritable: true}}

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

	//mlog.Log.Debugf("pubkey: %s, %s", fundingAcct.Key, newAcct.Key)
	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, SystemProgErrInvalidAccountDataLength, err)
}

func TestExecute_Tx_System_Program_CreateAccount_New_Acct_Has_Data_Failure(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// funding acct
	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()
	fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// new acct
	newAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newPubkey := newAcctPrivateKey.PublicKey()
	newAcct := accounts.Account{Key: newPubkey, Lamports: 0, Data: make([]byte, 1000), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	var createAcct SystemInstrCreateAccount
	createAcct.Lamports = 1234
	createAcct.Owner = a.BpfLoaderUpgradeableAddr
	createAcct.Space = 1234

	createAcctInstrWriter := new(bytes.Buffer)
	createAcctEncoder := bin.NewBinEncoder(createAcctInstrWriter)

	err = createAcct.MarshalWithEncoder(createAcctEncoder)
	assert.NoError(t, err)
	instrBytes := createAcctInstrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, fundingAcct, newAcct})

	acctMetas := []AccountMeta{{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true},
		{Pubkey: newAcct.Key, IsSigner: true, IsWritable: true}}

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

	//mlog.Log.Debugf("pubkey: %s, %s", fundingAcct.Key, newAcct.Key)
	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, SystemProgErrAccountAlreadyInUse, err)
}

func TestExecute_Tx_System_Program_CreateAccount_New_Acct_Not_Owned_By_System_Failure(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// funding acct
	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()
	fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// new acct
	newAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newPubkey := newAcctPrivateKey.PublicKey()
	newAcct := accounts.Account{Key: newPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	var createAcct SystemInstrCreateAccount
	createAcct.Lamports = 1234
	createAcct.Owner = a.BpfLoaderUpgradeableAddr
	createAcct.Space = 1234

	createAcctInstrWriter := new(bytes.Buffer)
	createAcctEncoder := bin.NewBinEncoder(createAcctInstrWriter)

	err = createAcct.MarshalWithEncoder(createAcctEncoder)
	assert.NoError(t, err)
	instrBytes := createAcctInstrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, fundingAcct, newAcct})

	acctMetas := []AccountMeta{{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true},
		{Pubkey: newAcct.Key, IsSigner: true, IsWritable: true}}

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

	//mlog.Log.Debugf("pubkey: %s, %s", fundingAcct.Key, newAcct.Key)
	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, SystemProgErrAccountAlreadyInUse, err)
}

func TestExecute_Tx_System_Program_CreateAccount_Funding_Acct_Not_Signer(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// funding acct
	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()
	fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// new acct
	newAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newPubkey := newAcctPrivateKey.PublicKey()
	newAcct := accounts.Account{Key: newPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	var createAcct SystemInstrCreateAccount
	createAcct.Lamports = 1234
	createAcct.Owner = a.BpfLoaderUpgradeableAddr
	createAcct.Space = 1234

	createAcctInstrWriter := new(bytes.Buffer)
	createAcctEncoder := bin.NewBinEncoder(createAcctInstrWriter)

	err = createAcct.MarshalWithEncoder(createAcctEncoder)
	assert.NoError(t, err)
	instrBytes := createAcctInstrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, fundingAcct, newAcct})

	acctMetas := []AccountMeta{{Pubkey: fundingAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: newAcct.Key, IsSigner: true, IsWritable: true}}

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

	//mlog.Log.Debugf("pubkey: %s, %s", fundingAcct.Key, newAcct.Key)
	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_System_Program_CreateAccountAllowPrefund_Success(t *testing.T) {
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	newOwnerPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newOwner := newOwnerPrivKey.PublicKey()

	newAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newPubkey := newAcctPrivateKey.PublicKey()

	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()

	t.Run("with payer", func(t *testing.T) {
		newAcct := accounts.Account{Key: newPubkey, Lamports: 100, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}
		fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 100, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}
		instrBytes := marshalCreateAccountAllowPrefundInstr(t, 50, 2, newOwner)

		transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, newAcct, fundingAcct})
		acctMetas := []AccountMeta{
			{Pubkey: newAcct.Key, IsSigner: true, IsWritable: true},
			{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true},
		}
		instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)
		txCtx, execCtx := newSystemProgramTestExecCtx(t, transactionAccts, features.CreateAccountAllowPrefund)

		err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
		assert.NoError(t, err)

		newAcctPost, err := txCtx.Accounts.GetAccount(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(150), newAcctPost.Lamports)
		assert.Equal(t, newOwner, solana.PublicKeyFromBytes(newAcctPost.Owner[:]))
		assert.Equal(t, []byte{0, 0}, newAcctPost.Data)

		fundingAcctPost, err := txCtx.Accounts.GetAccount(2)
		assert.NoError(t, err)
		assert.Equal(t, uint64(50), fundingAcctPost.Lamports)
	})

	t.Run("without payer", func(t *testing.T) {
		newAcct := accounts.Account{Key: newPubkey, Lamports: 100, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}
		instrBytes := marshalCreateAccountAllowPrefundInstr(t, 0, 2, newOwner)

		transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, newAcct})
		acctMetas := []AccountMeta{{Pubkey: newAcct.Key, IsSigner: true, IsWritable: true}}
		instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)
		txCtx, execCtx := newSystemProgramTestExecCtx(t, transactionAccts, features.CreateAccountAllowPrefund)

		err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
		assert.NoError(t, err)

		newAcctPost, err := txCtx.Accounts.GetAccount(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(100), newAcctPost.Lamports)
		assert.Equal(t, newOwner, solana.PublicKeyFromBytes(newAcctPost.Owner[:]))
		assert.Equal(t, []byte{0, 0}, newAcctPost.Data)
	})
}

func TestExecute_Tx_System_Program_CreateAccountAllowPrefund_Feature_Gate_Off_Failure(t *testing.T) {
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	newOwnerPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newOwner := newOwnerPrivKey.PublicKey()

	newAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newPubkey := newAcctPrivateKey.PublicKey()
	newAcct := accounts.Account{Key: newPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()
	fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 100, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	instrBytes := marshalCreateAccountAllowPrefundInstr(t, 50, 0, newOwner)
	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, newAcct, fundingAcct})
	acctMetas := []AccountMeta{
		{Pubkey: newAcct.Key, IsSigner: true, IsWritable: true},
		{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true},
	}
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)
	_, execCtx := newSystemProgramTestExecCtx(t, transactionAccts)

	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidInstructionData, err)
}

func TestExecute_Tx_System_Program_CreateAccountAllowPrefund_Already_In_Use_Failure(t *testing.T) {
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	newOwnerPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newOwner := newOwnerPrivKey.PublicKey()

	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()
	fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 100, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	instrBytes := marshalCreateAccountAllowPrefundInstr(t, 50, 2, newOwner)

	t.Run("account has data", func(t *testing.T) {
		newAcctPrivateKey, err := solana.NewRandomPrivateKey()
		assert.NoError(t, err)
		newPubkey := newAcctPrivateKey.PublicKey()
		newAcct := accounts.Account{Key: newPubkey, Lamports: 0, Data: []byte{0}, Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

		transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, newAcct, fundingAcct})
		acctMetas := []AccountMeta{
			{Pubkey: newAcct.Key, IsSigner: true, IsWritable: true},
			{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true},
		}
		instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)
		_, execCtx := newSystemProgramTestExecCtx(t, transactionAccts, features.CreateAccountAllowPrefund)

		err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
		assert.Equal(t, SystemProgErrAccountAlreadyInUse, err)
	})

	t.Run("account owned by another program", func(t *testing.T) {
		newAcctPrivateKey, err := solana.NewRandomPrivateKey()
		assert.NoError(t, err)
		newPubkey := newAcctPrivateKey.PublicKey()
		newAcct := accounts.Account{Key: newPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

		transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, newAcct, fundingAcct})
		acctMetas := []AccountMeta{
			{Pubkey: newAcct.Key, IsSigner: true, IsWritable: true},
			{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true},
		}
		instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)
		_, execCtx := newSystemProgramTestExecCtx(t, transactionAccts, features.CreateAccountAllowPrefund)

		err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
		assert.Equal(t, SystemProgErrAccountAlreadyInUse, err)
	})
}

func TestExecute_Tx_System_Program_CreateAccountAllowPrefund_Missing_Signer_Failure(t *testing.T) {
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	newOwnerPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newOwner := newOwnerPrivKey.PublicKey()

	newAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newPubkey := newAcctPrivateKey.PublicKey()

	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()

	instrBytes := marshalCreateAccountAllowPrefundInstr(t, 50, 2, newOwner)

	t.Run("payer not signed", func(t *testing.T) {
		newAcct := accounts.Account{Key: newPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}
		fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 100, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

		transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, newAcct, fundingAcct})
		acctMetas := []AccountMeta{
			{Pubkey: newAcct.Key, IsSigner: true, IsWritable: true},
			{Pubkey: fundingAcct.Key, IsSigner: false, IsWritable: true},
		}
		instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)
		_, execCtx := newSystemProgramTestExecCtx(t, transactionAccts, features.CreateAccountAllowPrefund)

		err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
		assert.Equal(t, InstrErrMissingRequiredSignature, err)
	})

	t.Run("new account not signed", func(t *testing.T) {
		newAcct := accounts.Account{Key: newPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}
		fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 100, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

		transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, newAcct, fundingAcct})
		acctMetas := []AccountMeta{
			{Pubkey: newAcct.Key, IsSigner: false, IsWritable: true},
			{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true},
		}
		instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)
		_, execCtx := newSystemProgramTestExecCtx(t, transactionAccts, features.CreateAccountAllowPrefund)

		err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
		assert.Equal(t, InstrErrMissingRequiredSignature, err)
	})
}

func TestExecute_Tx_System_Program_Assign_Success(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// new acct
	newAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newPubkey := newAcctPrivateKey.PublicKey()
	newAcct := accounts.Account{Key: newPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	assignInstrWriter := new(bytes.Buffer)
	assignEncoder := bin.NewBinEncoder(assignInstrWriter)

	var assign SystemInstrAssign
	assign.Owner = a.BpfLoaderUpgradeableAddr
	err = assign.MarshalWithEncoder(assignEncoder)
	assert.NoError(t, err)
	instrBytes := assignInstrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, newAcct})

	acctMetas := []AccountMeta{{Pubkey: newAcct.Key, IsSigner: true, IsWritable: true}}

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

	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	acctPost, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)

	assert.Equal(t, a.BpfLoaderUpgradeableAddr, acctPost.Owner)
}

func TestExecute_Tx_System_Program_Assign_Not_Signer_Failure(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// new acct
	newAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	newPubkey := newAcctPrivateKey.PublicKey()
	newAcct := accounts.Account{Key: newPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	assignInstrWriter := new(bytes.Buffer)
	assignEncoder := bin.NewBinEncoder(assignInstrWriter)

	var assign SystemInstrAssign
	assign.Owner = a.BpfLoaderUpgradeableAddr
	err = assign.MarshalWithEncoder(assignEncoder)
	assert.NoError(t, err)
	instrBytes := assignInstrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, newAcct})

	acctMetas := []AccountMeta{{Pubkey: newAcct.Key, IsSigner: false, IsWritable: true}}

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

	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_System_Program_Transfer_Success(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// funding acct
	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()
	fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// recipient acct
	recipientPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	recipientPubkey := recipientPrivateKey.PublicKey()
	recipientAcct := accounts.Account{Key: recipientPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	var transfer SystemInstrTransfer
	transfer.Lamports = 1337

	transferInstrWriter := new(bytes.Buffer)
	transferEncoder := bin.NewBinEncoder(transferInstrWriter)

	err = transfer.MarshalWithEncoder(transferEncoder)
	assert.NoError(t, err)
	instrBytes := transferInstrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, fundingAcct, recipientAcct})

	acctMetas := []AccountMeta{{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true},
		{Pubkey: recipientAcct.Key, IsSigner: false, IsWritable: true}}

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

	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.NoError(t, err)

	fundingAcctPost, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)
	assert.Equal(t, fundingAcct.Lamports-transfer.Lamports, fundingAcctPost.Lamports)

	recipientAcctPost, err := txCtx.Accounts.GetAccount(2)
	assert.NoError(t, err)
	assert.Equal(t, transfer.Lamports, recipientAcctPost.Lamports)
}

func TestExecute_Tx_System_Program_Transfer_From_Not_Signer_Failure(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// funding acct
	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()
	fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// recipient acct
	recipientPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	recipientPubkey := recipientPrivateKey.PublicKey()
	recipientAcct := accounts.Account{Key: recipientPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	var transfer SystemInstrTransfer
	transfer.Lamports = 1337

	transferInstrWriter := new(bytes.Buffer)
	transferEncoder := bin.NewBinEncoder(transferInstrWriter)

	err = transfer.MarshalWithEncoder(transferEncoder)
	assert.NoError(t, err)
	instrBytes := transferInstrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, fundingAcct, recipientAcct})

	acctMetas := []AccountMeta{{Pubkey: fundingAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: recipientAcct.Key, IsSigner: false, IsWritable: true}}

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

	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}

func TestExecute_Tx_System_Program_Transfer_From_Has_Data_Failure(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// funding acct
	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()
	fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 10000, Data: make([]byte, 100), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// recipient acct
	recipientPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	recipientPubkey := recipientPrivateKey.PublicKey()
	recipientAcct := accounts.Account{Key: recipientPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	var transfer SystemInstrTransfer
	transfer.Lamports = 1337

	transferInstrWriter := new(bytes.Buffer)
	transferEncoder := bin.NewBinEncoder(transferInstrWriter)

	err = transfer.MarshalWithEncoder(transferEncoder)
	assert.NoError(t, err)
	instrBytes := transferInstrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, fundingAcct, recipientAcct})

	acctMetas := []AccountMeta{{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true},
		{Pubkey: recipientAcct.Key, IsSigner: false, IsWritable: true}}

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

	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrInvalidArgument, err)
}

func TestExecute_Tx_System_Program_Transfer_Not_Enough_Lamports_In_From_Acct(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// funding acct
	fundingAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fundingPubkey := fundingAcctPrivateKey.PublicKey()
	fundingAcct := accounts.Account{Key: fundingPubkey, Lamports: 100, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	// recipient acct
	recipientPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	recipientPubkey := recipientPrivateKey.PublicKey()
	recipientAcct := accounts.Account{Key: recipientPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	var transfer SystemInstrTransfer
	transfer.Lamports = 1000000

	transferInstrWriter := new(bytes.Buffer)
	transferEncoder := bin.NewBinEncoder(transferInstrWriter)

	err = transfer.MarshalWithEncoder(transferEncoder)
	assert.NoError(t, err)
	instrBytes := transferInstrWriter.Bytes()

	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, fundingAcct, recipientAcct})

	acctMetas := []AccountMeta{{Pubkey: fundingAcct.Key, IsSigner: true, IsWritable: true},
		{Pubkey: recipientAcct.Key, IsSigner: false, IsWritable: true}}

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

	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, SystemProgErrResultWithNegativeLamports, err)
}

func TestExecute_Tx_System_Program_AssignWithSeed_Success(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// base acct
	basePrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	basePubkey := basePrivateKey.PublicKey()
	baseAcct := accounts.Account{Key: basePubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	assignedPubkey, err := solana.CreateWithSeed(basePubkey, "seed", a.BpfLoaderUpgradeableAddr)
	assert.NoError(t, err)

	assignedAcct := accounts.Account{Key: assignedPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	assert.NoError(t, err)
	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, assignedAcct, baseAcct})

	acctMetas := []AccountMeta{{Pubkey: assignedAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: baseAcct.Key, IsSigner: true, IsWritable: true}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	instrWriter := new(bytes.Buffer)
	instrEncoder := bin.NewBinEncoder(instrWriter)
	var assignWithSeed SystemInstrAssignWithSeed
	assignWithSeed.Base = basePubkey
	assignWithSeed.Owner = a.BpfLoaderUpgradeableAddr
	assignWithSeed.Seed = "seed"
	err = assignWithSeed.MarshalWithEncoder(instrEncoder)
	assert.NoError(t, err)

	instrBytes := instrWriter.Bytes()

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

	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	assignedAcctPost, err := txCtx.Accounts.GetAccount(1)
	assert.NoError(t, err)

	assert.Equal(t, assignWithSeed.Owner, solana.PublicKeyFromBytes(assignedAcctPost.Owner[:]))
}

func TestExecute_Tx_System_Program_AssignWithSeed_Addr_Doesnt_Match_Derived_Addr_Failure(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// base acct
	basePrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	basePubkey := basePrivateKey.PublicKey()
	baseAcct := accounts.Account{Key: basePubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// base acct
	wrongBasePrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	wrongBasePubkey := wrongBasePrivateKey.PublicKey()
	wrongBaseAcct := accounts.Account{Key: wrongBasePubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	assert.NoError(t, err)
	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, wrongBaseAcct, baseAcct})

	acctMetas := []AccountMeta{{Pubkey: wrongBaseAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: baseAcct.Key, IsSigner: true, IsWritable: true}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	instrWriter := new(bytes.Buffer)
	instrEncoder := bin.NewBinEncoder(instrWriter)
	var assignWithSeed SystemInstrAssignWithSeed
	assignWithSeed.Base = basePubkey
	assignWithSeed.Owner = a.BpfLoaderUpgradeableAddr
	assignWithSeed.Seed = "seed"
	err = assignWithSeed.MarshalWithEncoder(instrEncoder)
	assert.NoError(t, err)

	instrBytes := instrWriter.Bytes()

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

	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, SystemProgErrAddressWithSeedMismatch, err)
}

func TestExecute_Tx_System_Program_AssignWithSeed_Base_Not_Signer_Failure(t *testing.T) {

	// system program acct
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 100000000, Data: make([]byte, 0), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	// base acct
	basePrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	basePubkey := basePrivateKey.PublicKey()
	baseAcct := accounts.Account{Key: basePubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	assignedPubkey, err := solana.CreateWithSeed(basePubkey, "seed", a.BpfLoaderUpgradeableAddr)
	assert.NoError(t, err)

	assignedAcct := accounts.Account{Key: assignedPubkey, Lamports: 0, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	assert.NoError(t, err)
	transactionAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, assignedAcct, baseAcct})

	acctMetas := []AccountMeta{{Pubkey: assignedAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: baseAcct.Key, IsSigner: false, IsWritable: true}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	instrWriter := new(bytes.Buffer)
	instrEncoder := bin.NewBinEncoder(instrWriter)
	var assignWithSeed SystemInstrAssignWithSeed
	assignWithSeed.Base = basePubkey
	assignWithSeed.Owner = a.BpfLoaderUpgradeableAddr
	assignWithSeed.Seed = "seed"
	err = assignWithSeed.MarshalWithEncoder(instrEncoder)
	assert.NoError(t, err)

	instrBytes := instrWriter.Bytes()

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

	err = execCtx.ProcessInstruction(instrBytes, instructionAccts, []uint64{0})
	assert.Equal(t, InstrErrMissingRequiredSignature, err)
}
