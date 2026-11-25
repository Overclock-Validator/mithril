package sealevel

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type loaderBufferTest struct {
	bufferOwner    solana.PublicKey
	bufferReadonly bool
	bufferExec     bool
	wrongAuthority bool
	notBufferState bool
	payerLamports  uint64
}

func marshalLoaderState(t *testing.T, state UpgradeableLoaderState) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, state.MarshalWithEncoder(bin.NewBinEncoder(&buf)))
	return buf.Bytes()
}

func loaderBufferAccount(t *testing.T, key, authority solana.PublicKey, tt loaderBufferTest) accounts.Account {
	t.Helper()
	bufferAuthority := authority
	if tt.wrongAuthority {
		bufferAuthority = solana.PublicKey{0xee}
	}
	state := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeBuffer, Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &bufferAuthority}}
	if tt.notBufferState {
		state = UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeUninitialized}
	}
	data := append(marshalLoaderState(t, state), fixtures.Load(t, "sbpf", "noop_aligned.so")...)
	if tt.notBufferState {
		data = bytes.Repeat([]byte{0}, upgradeableLoaderSizeOfBufferMetaData+16)
	}
	return accounts.Account{Key: key, Lamports: 20000, Data: data, Owner: tt.bufferOwner, Executable: tt.bufferExec, RentEpoch: 100}
}

func processLoaderBufferInstruction(t *testing.T, txAccts *TransactionAccounts, metas []AccountMeta, data []byte) error {
	t.Helper()
	txCtx := NewTransactionCtx(*txAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), Accounts: accounts.NewMemAccounts()}
	require.NoError(t, execCtx.Accounts.SetAccount(&SysvarClockAddr, &accounts.Account{Lamports: 1}))
	WriteClockSysvar(&execCtx.Accounts, SysvarClock{Slot: 1234})
	require.NoError(t, execCtx.Accounts.SetAccount(&SysvarRentAddr, &accounts.Account{Lamports: 1}))
	WriteRentSysvar(&execCtx.Accounts, SysvarRent{LamportsPerUint8Year: 1, ExemptionThreshold: 1})
	initializeLegacyBankFixture(t, &execCtx)
	return execCtx.ProcessInstruction(data, InstructionAcctsFromAccountMetas(metas, *txAccts), []uint64{0})
}

func runDeployWithBuffer(t *testing.T, tt loaderBufferTest) error {
	t.Helper()
	authority := solana.PublicKey{1}
	payerKey := solana.PublicKey{2}
	programKey := solana.PublicKey{3}
	bufferKey := solana.PublicKey{4}
	programDataKey, _, err := solana.FindProgramAddress([][]byte{programKey[:]}, a.BpfLoaderUpgradeableAddr)
	require.NoError(t, err)

	programData := make([]byte, 5000)
	copy(programData, marshalLoaderState(t, UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeUninitialized}))
	payerLamports := tt.payerLamports
	if payerLamports == 0 {
		payerLamports = 200000
	}

	txAccts := NewTransactionAccounts([]accounts.Account{
		{Key: a.BpfLoaderUpgradeableAddr, Owner: a.NativeLoaderAddr, Executable: true},
		{Key: payerKey, Lamports: payerLamports, Owner: a.SystemProgramAddr},
		{Key: programDataKey, Owner: a.SystemProgramAddr},
		{Key: programKey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr},
		loaderBufferAccount(t, bufferKey, authority, tt),
		{Key: SysvarRentAddr, Lamports: 1},
		{Key: SysvarClockAddr, Lamports: 1},
		{Key: a.SystemProgramAddr, Owner: a.NativeLoaderAddr, Executable: true},
		{Key: authority, Owner: a.SystemProgramAddr},
	})
	metas := []AccountMeta{
		{Pubkey: payerKey, IsSigner: true, IsWritable: true},
		{Pubkey: programDataKey, IsWritable: true},
		{Pubkey: programKey, IsWritable: true},
		{Pubkey: bufferKey, IsWritable: !tt.bufferReadonly},
		{Pubkey: SysvarRentAddr},
		{Pubkey: SysvarClockAddr},
		{Pubkey: a.SystemProgramAddr},
		{Pubkey: authority, IsSigner: true},
	}

	var instr bytes.Buffer
	require.NoError(t, (&UpgradeableLoaderInstrDeployWithMaxDataLen{MaxDataLen: 50000}).MarshalWithEncoder(bin.NewBinEncoder(&instr)))
	return processLoaderBufferInstruction(t, txAccts, metas, instr.Bytes())
}

func runUpgradeWithBuffer(t *testing.T, tt loaderBufferTest) error {
	t.Helper()
	authority := solana.PublicKey{1}
	programDataKey := solana.PublicKey{2}
	programKey := solana.PublicKey{3}
	bufferKey := solana.PublicKey{4}
	spillKey := solana.PublicKey{5}

	programData := make([]byte, upgradeableLoaderSizeOfProgramDataMetaData+len(fixtures.Load(t, "sbpf", "noop_aligned.so")))
	copy(programData, marshalLoaderState(t, UpgradeableLoaderState{
		Type:        UpgradeableLoaderStateTypeProgramData,
		ProgramData: UpgradeableLoaderStateProgramData{Slot: 1, UpgradeAuthorityAddress: &authority},
	}))
	program := marshalLoaderState(t, UpgradeableLoaderState{
		Type:    UpgradeableLoaderStateTypeProgram,
		Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataKey},
	})

	txAccts := NewTransactionAccounts([]accounts.Account{
		{Key: a.BpfLoaderUpgradeableAddr, Owner: a.NativeLoaderAddr, Executable: true},
		{Key: programDataKey, Data: programData, Owner: a.BpfLoaderUpgradeableAddr},
		{Key: programKey, Data: program, Owner: a.BpfLoaderUpgradeableAddr, Executable: true},
		loaderBufferAccount(t, bufferKey, authority, tt),
		{Key: spillKey, Owner: a.SystemProgramAddr},
		{Key: SysvarRentAddr, Lamports: 1},
		{Key: SysvarClockAddr, Lamports: 1},
		{Key: authority, Owner: a.SystemProgramAddr},
	})
	metas := []AccountMeta{
		{Pubkey: programDataKey, IsWritable: true},
		{Pubkey: programKey, IsWritable: true},
		{Pubkey: bufferKey, IsWritable: !tt.bufferReadonly},
		{Pubkey: spillKey, IsWritable: true},
		{Pubkey: SysvarRentAddr},
		{Pubkey: SysvarClockAddr},
		{Pubkey: authority, IsSigner: true},
	}
	return processLoaderBufferInstruction(t, txAccts, metas, binary.LittleEndian.AppendUint32(nil, UpgradeableLoaderInstrTypeUpgrade))
}

// Match Agave's buffer account checks for DeployWithMaxDataLen and Upgrade,
// including their precedence over the buffer state and authority checks.
// Reference: anza-xyz/agave programs/bpf_loader/src/lib.rs.
func TestLoaderBufferAccountChecks(t *testing.T) {
	foreign := solana.PublicKey{0xaa}
	loader := a.BpfLoaderUpgradeableAddr
	cases := []struct {
		name string
		tt   loaderBufferTest
		want error
	}{
		{"readonly buffer", loaderBufferTest{bufferOwner: loader, bufferReadonly: true}, InstrErrInvalidArgument},
		{"readonly checked before owner", loaderBufferTest{bufferOwner: foreign, bufferReadonly: true}, InstrErrInvalidArgument},
		{"foreign owner", loaderBufferTest{bufferOwner: foreign}, InstrErrIncorrectProgramId},
		{"owner checked before authority", loaderBufferTest{bufferOwner: foreign, wrongAuthority: true}, InstrErrIncorrectProgramId},
		{"owner checked before state", loaderBufferTest{bufferOwner: foreign, notBufferState: true}, InstrErrIncorrectProgramId},
		{"wrong authority", loaderBufferTest{bufferOwner: loader, wrongAuthority: true}, InstrErrIncorrectAuthority},
		{"not a buffer", loaderBufferTest{bufferOwner: loader, notBufferState: true}, InstrErrInvalidArgument},
	}
	for _, tc := range cases {
		t.Run("deploy/"+tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, runDeployWithBuffer(t, tc.tt))
		})
		t.Run("upgrade/"+tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, runUpgradeWithBuffer(t, tc.tt))
		})
	}
}

// Draining the buffer into the payer has to fail the instruction as soon as
// either side of the transfer fails, as Agave's `?` operators do.
func TestDeployPropagatesBufferDrainErrors(t *testing.T) {
	loader := a.BpfLoaderUpgradeableAddr
	t.Run("payer overflow", func(t *testing.T) {
		err := runDeployWithBuffer(t, loaderBufferTest{bufferOwner: loader, payerLamports: math.MaxUint64})
		require.Equal(t, InstrErrArithmeticOverflow, err)
	})
	t.Run("executable buffer", func(t *testing.T) {
		err := runDeployWithBuffer(t, loaderBufferTest{bufferOwner: loader, bufferExec: true})
		require.Equal(t, InstrErrExecutableLamportChange, err)
	})
}
