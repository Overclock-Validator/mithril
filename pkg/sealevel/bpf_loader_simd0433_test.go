package sealevel

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// runOversizedProgramDataUpgrade upgrades a program whose ProgramData holds
// `slack` bytes more than the new ELF needs and returns the post-upgrade
// ProgramData account, the ELF and the Rent used.
func runOversizedProgramDataUpgrade(t *testing.T, setToElfLength bool, slack int) (*accounts.Account, []byte, SysvarRent) {
	t.Helper()
	loaderAcct := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Data: make([]byte, 500), Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	authorityPubkey := solana.NewWallet().PublicKey()
	authorityAcct := accounts.Account{Key: authorityPubkey, Data: []byte{}, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	elf := fixtures.Load(t, "sbpf", "noop_aligned.so")

	var programDataState bytes.Buffer
	require.NoError(t, (&UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData,
		ProgramData: UpgradeableLoaderStateProgramData{UpgradeAuthorityAddress: &authorityPubkey, Slot: 1337}}).MarshalWithEncoder(bin.NewBinEncoder(&programDataState)))
	programDataPubkey := solana.NewWallet().PublicKey()
	programDataData := make([]byte, upgradeableLoaderSizeOfProgramDataMetaData+len(elf)+slack)
	copy(programDataData, programDataState.Bytes())
	for i := upgradeableLoaderSizeOfProgramDataMetaData; i < len(programDataData); i++ {
		programDataData[i] = 0xAB // stale bytes from the previous program
	}
	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataData, Owner: a.BpfLoaderUpgradeableAddr, RentEpoch: 100}

	var bufferData bytes.Buffer
	require.NoError(t, (&UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer,
		Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authorityPubkey}}).MarshalWithEncoder(bin.NewBinEncoder(&bufferData)))
	bufferData.Write(elf)
	bufferAcct := accounts.Account{Key: solana.NewWallet().PublicKey(), Lamports: 100_000_000, Data: bufferData.Bytes(), Owner: a.BpfLoaderUpgradeableAddr, RentEpoch: 100}

	spillAcct := accounts.Account{Key: solana.NewWallet().PublicKey(), Data: []byte{}, Owner: a.BpfLoaderUpgradeableAddr, RentEpoch: 100}

	var programData bytes.Buffer
	require.NoError(t, (&UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram,
		Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataPubkey}}).MarshalWithEncoder(bin.NewBinEncoder(&programData)))
	programAcct := accounts.Account{Key: solana.NewWallet().PublicKey(), Data: programData.Bytes(), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	fakeClock := accounts.Account{Key: SysvarClockAddr, Data: programData.Bytes(), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}
	fakeRent := accounts.Account{Key: SysvarRentAddr, Data: programData.Bytes(), Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	txAccts := NewTransactionAccounts([]accounts.Account{loaderAcct, programDataAcct, programAcct, bufferAcct, spillAcct, fakeRent, fakeClock, authorityAcct})
	metas := []AccountMeta{
		{Pubkey: programDataAcct.Key, IsWritable: true},
		{Pubkey: programAcct.Key, IsWritable: true},
		{Pubkey: bufferAcct.Key, IsWritable: true},
		{Pubkey: spillAcct.Key, IsWritable: true},
		{Pubkey: fakeRent.Key, IsWritable: true},
		{Pubkey: fakeClock.Key, IsWritable: true},
		{Pubkey: authorityAcct.Key, IsSigner: true, IsWritable: true},
	}
	instrAccts := InstructionAcctsFromAccountMetas(metas, *txAccts)

	txCtx := NewTransactionCtx(*txAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault()}
	execCtx.Accounts = accounts.NewMemAccounts()
	clockAcct := accounts.Account{Lamports: 1}
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, SysvarClock{Slot: 1234})
	rent := SysvarRent{LamportsPerUint8Year: 2575, ExemptionThreshold: 1, BurnPercent: 50}
	rentAcct := accounts.Account{Lamports: 1}
	execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.Accounts, rent)
	initializeLegacyBankFixture(t, &execCtx)
	if setToElfLength {
		execCtx.Features.EnableFeature(features.LoaderV3SetProgramDataToElfLength, 0)
	}

	instrData := binary.LittleEndian.AppendUint32(nil, UpgradeableLoaderInstrTypeUpgrade)
	require.NoError(t, execCtx.ProcessInstruction(instrData, instrAccts, []uint64{0}))

	post, err := txCtx.Accounts.GetAccount(1)
	require.NoError(t, err)
	return post, elf, rent
}

// SIMD-0433: with LoaderV3SetProgramDataToElfLength, Upgrade shrinks an
// oversized ProgramData to header+ELF and funds exactly that length's rent.
func TestUpgradeSetsProgramDataToElfLength(t *testing.T) {
	post, elf, rent := runOversizedProgramDataUpgrade(t, true, 4096)
	wantLen := upgradeableLoaderSizeOfProgramDataMetaData + len(elf)
	require.Len(t, post.Data, wantLen)
	require.Equal(t, elf, post.Data[upgradeableLoaderSizeOfProgramDataMetaData:])
	require.Equal(t, max(rent.MinimumBalance(uint64(wantLen)), 1), post.Lamports)
}

// Before SIMD-0433 the ProgramData length is kept and the slack zeroed.
func TestUpgradeKeepsProgramDataLengthWithoutSimd0433(t *testing.T) {
	const slack = 4096
	post, elf, rent := runOversizedProgramDataUpgrade(t, false, slack)
	wantLen := upgradeableLoaderSizeOfProgramDataMetaData + len(elf) + slack
	require.Len(t, post.Data, wantLen)
	require.Equal(t, elf, post.Data[upgradeableLoaderSizeOfProgramDataMetaData:upgradeableLoaderSizeOfProgramDataMetaData+len(elf)])
	require.Equal(t, make([]byte, slack), post.Data[upgradeableLoaderSizeOfProgramDataMetaData+len(elf):])
	require.Equal(t, max(rent.MinimumBalance(uint64(wantLen)), 1), post.Lamports)
}
