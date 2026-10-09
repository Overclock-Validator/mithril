package sealevel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
)

var splTokenProgramAddr = base58.MustDecodeFromString("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")

// load the spl token program bytes from file, create an account object (loader 2) that contains the ELF bytes
// and is appropriately owned by the old loader (version 2), and then add this account to the account manager object.
// We also return the account object itself so that we can use it in the various instruction calls we'll make to the
// spl token program later.
func setupSplTokenProgramAccount(t *testing.T, accts *accounts.Accounts) accounts.Account {
	programBytes := fixtures.Load(t, "sbpf", "spl-token.so")
	splTokenAcct := accounts.Account{Key: splTokenProgramAddr, Lamports: 1, Data: programBytes, Owner: a.BpfLoader2Addr, Executable: true, RentEpoch: 100}

	pk := [32]byte(splTokenProgramAddr)
	err := (*accts).SetAccount(&pk, &splTokenAcct)
	assert.NoError(t, err)

	return splTokenAcct
}

// create new ExecutionCtx (equiv. to InvokeContext in Agave)
func newExecCtx(t *testing.T, log *LogRecorder) *ExecutionCtx {
	accts := accounts.NewMemAccounts()
	execCtx := ExecutionCtx{Log: log, ComputeMeter: cu.NewComputeMeter(10000000000), Accounts: accts}
	f := features.NewFeaturesDefault()
	execCtx.Features = *f

	return &execCtx
}

// create a new rent sysvar with default configuration
func newDefaultRentSysvar(accts *accounts.Accounts) accounts.Account {
	rent := SysvarRent{LamportsPerUint8Year: 3480, ExemptionThreshold: 2.0, BurnPercent: 50}

	rentAcct := accounts.Account{Key: SysvarRentAddr, Lamports: 1, Owner: a.SysvarOwnerAddr}
	(*accts).SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(accts, rent)

	return rentAcct
}

func newRandomAccountWithOwnerAndSizeAndLamports(owner solana.PublicKey, size uint64, lamports uint64) accounts.Account {
	privKey, err := solana.NewRandomPrivateKey()
	if err != nil {
		panic("create random private key failed")
	}
	pubkey := privKey.PublicKey()

	data := make([]byte, size)
	acct := accounts.Account{Key: pubkey, Lamports: lamports, Data: data, Owner: owner, Executable: false, RentEpoch: 100}

	return acct
}

const (
	initializeMintInstr    = 0
	initializeAccountInstr = 1
	transferInstr          = 3
	mintToInstr            = 7
)

type splTokenProgramInitializeMint struct {
	Decimals        byte
	MintAuthority   solana.PublicKey
	FreezeAuthority *solana.PublicKey
}

type splTokenProgramMintTo struct {
	Amount uint64
}

type splTokenProgramTransfer struct {
	Amount uint64
}

func (initMint *splTokenProgramInitializeMint) Marshal() []byte {
	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.LittleEndian, byte(initializeMintInstr))
	if err != nil {
		panic("error marshaling InitializeMint instruction")
	}

	err = binary.Write(buf, binary.LittleEndian, initMint.Decimals)
	if err != nil {
		panic("error marshaling InitializeMint instruction")
	}

	err = binary.Write(buf, binary.LittleEndian, initMint.MintAuthority)
	if err != nil {
		panic("error marshaling InitializeMint instruction")
	}

	if initMint.FreezeAuthority == nil {
		err = binary.Write(buf, binary.LittleEndian, byte(0))
		if err != nil {
			panic("error marshaling InitializeMint instruction")
		}
	} else {
		err = binary.Write(buf, binary.LittleEndian, byte(1))
		if err != nil {
			panic("error marshaling InitializeMint instruction")
		}

		err = binary.Write(buf, binary.LittleEndian, initMint.FreezeAuthority)
		if err != nil {
			panic("error marshaling InitializeMint instruction")
		}
	}

	return buf.Bytes()
}

func (mintTo *splTokenProgramMintTo) Marshal() []byte {
	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.LittleEndian, byte(mintToInstr))
	if err != nil {
		panic("error marshaling MintTo instruction")
	}

	err = binary.Write(buf, binary.LittleEndian, mintTo.Amount)
	if err != nil {
		panic("error serializing MintTo instruction")
	}

	return buf.Bytes()
}

func (transfer *splTokenProgramTransfer) Marshal() []byte {
	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.LittleEndian, byte(transferInstr))
	if err != nil {
		panic("error marshaling Transfer instruction")
	}

	err = binary.Write(buf, binary.LittleEndian, transfer.Amount)
	if err != nil {
		panic("error serializing Transfer instruction")
	}

	return buf.Bytes()
}

func newInitializeMintInstructionBytes(decimals byte, mintAuth solana.PublicKey, freezeAuth *solana.PublicKey) []byte {
	initMintInstr := splTokenProgramInitializeMint{Decimals: decimals, MintAuthority: mintAuth, FreezeAuthority: freezeAuth}
	instrBytes := initMintInstr.Marshal()
	return instrBytes
}

func newMintToInstructionBytes(amount uint64) []byte {
	mintTo := splTokenProgramMintTo{Amount: amount}
	instrBytes := mintTo.Marshal()
	return instrBytes
}

func newTransferToInstructionBytes(amount uint64) []byte {
	transfer := splTokenProgramTransfer{Amount: amount}
	instrBytes := transfer.Marshal()
	return instrBytes
}

func extractTokenAmountFromAccountBlob(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data[64:])
}

func Test_Spl_Token_Program_Demo(t *testing.T) {
	var log LogRecorder
	execCtx := newExecCtx(t, &log)

	splTokenProgramAcct := setupSplTokenProgramAccount(t, &execCtx.Accounts)

	//  InitializeMint: create accounts to serve as a) mint account, and b) mint authority account
	mintAcct := newRandomAccountWithOwnerAndSizeAndLamports(splTokenProgramAddr, 82, 100000000)
	mintAuthority := newRandomAccountWithOwnerAndSizeAndLamports(a.SystemProgramAddr, 0, 100000000)

	rent := newDefaultRentSysvar(&execCtx.Accounts)
	initMintInstrData := newInitializeMintInstructionBytes(6, mintAuthority.Key, nil)

	// InitializeMint: transaction accounts; token program, the mint account, and the rent sysvar
	transactionAccts := NewTransactionAccounts([]accounts.Account{splTokenProgramAcct, mintAcct, rent})

	// InitializeMint: instruction accounts: the mint account and the rent sysvar
	acctMetas := []AccountMeta{{Pubkey: mintAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: SysvarRentAddr, IsSigner: false, IsWritable: false}}
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)
	execCtx.TransactionContext = NewTransactionCtx(*transactionAccts, 5, 64)
	initializeLegacyBankFixture(t, execCtx)

	// InitializeMint: execute SPL token InitializeMint instruction
	err := execCtx.ProcessInstruction(initMintInstrData, instructionAccts, []uint64{0})
	assert.NoError(t, err)

	mintAcctPost, err := execCtx.TransactionContext.Accounts.GetAccount(1)
	assert.NoError(t, err)

	mintAcct = *mintAcctPost

	// InitializeAccount: create a new ExecutionCtx because we're executing a new instruction
	execCtx = newExecCtx(t, &log)
	rent = newDefaultRentSysvar(&execCtx.Accounts)

	// InitializeAccount: create accounts to serve as a) token account, and b) the token account's owner
	tokenAcct := newRandomAccountWithOwnerAndSizeAndLamports(splTokenProgramAddr, 165, 10000000)
	tokenOwner := newRandomAccountWithOwnerAndSizeAndLamports(a.SystemProgramAddr, 0, 10000000)
	initAccountInstrData := make([]byte, 1)
	initAccountInstrData[0] = initializeAccountInstr

	// InitializeAccount: instruction accounts: token account to initialize, the mint,
	// the token account's owner, rent sysvar
	transactionAccts = NewTransactionAccounts([]accounts.Account{splTokenProgramAcct, tokenAcct, mintAcct, tokenOwner, rent})
	acctMetas = []AccountMeta{{Pubkey: tokenAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: mintAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: tokenOwner.Key, IsSigner: false, IsWritable: true},
		{Pubkey: SysvarRentAddr, IsSigner: false, IsWritable: false}}

	instructionAccts = InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)
	execCtx.TransactionContext = NewTransactionCtx(*transactionAccts, 5, 64)
	initializeLegacyBankFixture(t, execCtx)

	// InitializeAccount: execute SPL token InitializeMint instruction
	err = execCtx.ProcessInstruction(initAccountInstrData, instructionAccts, []uint64{0})
	assert.NoError(t, err)

	tokenAcctPost, err := execCtx.TransactionContext.Accounts.GetAccount(1)
	assert.NoError(t, err)
	tokenAcct = *tokenAcctPost

	// InitializeAccount: create a new ExecutionCtx because we're executing a new instruction
	execCtx = newExecCtx(t, &log)
	rent = newDefaultRentSysvar(&execCtx.Accounts)

	// InitializeAccount: create accounts to serve as a) token account, and b) the token account's owner
	dstTokenAcct := newRandomAccountWithOwnerAndSizeAndLamports(splTokenProgramAddr, 165, 10000000)
	dstTokenOwner := newRandomAccountWithOwnerAndSizeAndLamports(a.SystemProgramAddr, 0, 10000000)
	initAccountInstrData = make([]byte, 1)
	initAccountInstrData[0] = initializeAccountInstr

	// InitializeAccount: instruction accounts: token account to initialize, the mint,
	// the token account's owner, rent sysvar
	transactionAccts = NewTransactionAccounts([]accounts.Account{splTokenProgramAcct, dstTokenAcct, mintAcct, dstTokenOwner, rent})
	acctMetas = []AccountMeta{{Pubkey: dstTokenAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: mintAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: dstTokenOwner.Key, IsSigner: false, IsWritable: true},
		{Pubkey: SysvarRentAddr, IsSigner: false, IsWritable: false}}

	instructionAccts = InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)
	execCtx.TransactionContext = NewTransactionCtx(*transactionAccts, 5, 64)
	initializeLegacyBankFixture(t, execCtx)

	// InitializeAccount: execute SPL token InitializeMint instruction
	err = execCtx.ProcessInstruction(initAccountInstrData, instructionAccts, []uint64{0})
	assert.NoError(t, err)

	dstTokenAcctPost, err := execCtx.TransactionContext.Accounts.GetAccount(1)
	assert.NoError(t, err)
	dstTokenAcct = *dstTokenAcctPost

	// MintTo: create a new ExecutionCtx because we're executing a new instruction
	execCtx = newExecCtx(t, &log)
	rent = newDefaultRentSysvar(&execCtx.Accounts)

	// MintTo: instruction accounts: the mint account, account to mint tokens to, mint authority
	transactionAccts = NewTransactionAccounts([]accounts.Account{splTokenProgramAcct, mintAcct, tokenAcct, mintAuthority})
	acctMetas = []AccountMeta{{Pubkey: mintAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: tokenAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: mintAuthority.Key, IsSigner: true, IsWritable: true}}

	instructionAccts = InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)
	execCtx.TransactionContext = NewTransactionCtx(*transactionAccts, 5, 64)
	initializeLegacyBankFixture(t, execCtx)

	// MintTo: serialize up a MintTo instruction
	numTokensToMint := uint64(61616161)
	mintToInstrData := newMintToInstructionBytes(numTokensToMint)

	// MintTo: execute SPL token MintTo instruction
	err = execCtx.ProcessInstruction(mintToInstrData, instructionAccts, []uint64{0})
	assert.NoError(t, err)

	tokenAcctPost, err = execCtx.TransactionContext.Accounts.GetAccount(2)
	assert.NoError(t, err)
	tokenAcct = *tokenAcctPost

	// Transfer: create a new ExecutionCtx because we're executing a new instruction
	execCtx = newExecCtx(t, &log)
	rent = newDefaultRentSysvar(&execCtx.Accounts)

	// Transfer: instruction accounts: source token account, destination token, source account's owner
	transactionAccts = NewTransactionAccounts([]accounts.Account{splTokenProgramAcct, tokenAcct, dstTokenAcct, tokenOwner})
	acctMetas = []AccountMeta{{Pubkey: tokenAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: dstTokenAcct.Key, IsSigner: false, IsWritable: true},
		{Pubkey: tokenOwner.Key, IsSigner: true, IsWritable: true}}

	instructionAccts = InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)
	execCtx.TransactionContext = NewTransactionCtx(*transactionAccts, 5, 64)
	initializeLegacyBankFixture(t, execCtx)

	// Transfer: serialize up a Transfer instruction
	numTokensToTransfer := uint64(1337)
	transferInstrData := newTransferToInstructionBytes(numTokensToTransfer)

	// Transfer: execute SPL token Transfer instruction
	err = execCtx.ProcessInstruction(transferInstrData, instructionAccts, []uint64{0})
	assert.NoError(t, err)

	// Transfer: check that the token balances in the src and dst accounts are as expected
	dstAcctPost, err := execCtx.TransactionContext.Accounts.GetAccount(2)
	assert.NoError(t, err)

	srcAcctPost, err := execCtx.TransactionContext.Accounts.GetAccount(1)
	assert.NoError(t, err)

	srcBalancePostTransfer := extractTokenAmountFromAccountBlob(srcAcctPost.Data)
	dstBalancePostTransfer := extractTokenAmountFromAccountBlob(dstAcctPost.Data)

	assert.Equal(t, numTokensToMint, srcBalancePostTransfer+dstBalancePostTransfer)
	assert.Equal(t, uint64(numTokensToTransfer), dstBalancePostTransfer)
	assert.Equal(t, numTokensToMint-numTokensToTransfer, srcBalancePostTransfer)

	fmt.Printf("src account num tokens after transfer: %d\n", extractTokenAmountFromAccountBlob(srcAcctPost.Data))
	fmt.Printf("dst account num tokens after transfer: %d\n", extractTokenAmountFromAccountBlob(dstAcctPost.Data))

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}
