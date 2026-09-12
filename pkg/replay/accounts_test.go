package replay

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSimd186SlotCtx returns a minimal SlotCtx wired for the SIMD-186
// account loader path: empty MemAccounts and the
// FormalizeLoadedTransactionDataSize gate enabled.
func newSimd186SlotCtx() *sealevel.SlotCtx {
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)
	mem := accounts.NewMemAccounts()
	return &sealevel.SlotCtx{
		Accounts: mem,
		Features: feats,
	}
}

// TestLoadAndValidateTxAcctsSimd186_FabricatesDefaultForMissingAccount
// asserts that the SIMD-186 loader returns an empty System-owned default
// for a pubkey absent from local state instead of panicking, matching
// Agave's load_transaction_account behavior.
func TestLoadAndValidateTxAcctsSimd186_FabricatesDefaultForMissingAccount(t *testing.T) {
	slotCtx := newSimd186SlotCtx()

	missingKey := testPubkey(42)
	tx := &solana.Transaction{
		Message: solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys: []solana.PublicKey{missingKey},
		},
	}
	instrsAcct := &accounts.Account{Key: sealevel.SysvarInstructionsAddr}

	require.NotPanics(t, func() {
		txAccts, _, loadedSize, err := loadAndValidateTxAcctsSimd186(
			slotCtx,
			nil, // derive transaction account metadata
			tx,
			nil, // instrs — no instructions
			instrsAcct,
			math.MaxUint32,
		)
		require.NoError(t, err)
		assert.Zero(t, loadedSize)
		require.NotNil(t, txAccts)
		require.Len(t, txAccts.Accounts, 1)

		fabricated := txAccts.Accounts[0]
		assert.Equal(t, missingKey, fabricated.Key)
		assert.Equal(t, addresses.SystemProgramAddr, fabricated.Owner)
		assert.Equal(t, uint64(0), fabricated.Lamports)
		assert.Equal(t, uint64(math.MaxUint64), fabricated.RentEpoch, "fabricated default must use rent-exempt epoch")
		assert.False(t, fabricated.Executable)
		assert.Empty(t, fabricated.Data)
	})
}

// TestLoadAndValidateTxAcctsSimd186_LoadedAccountTakesPrecedence
// confirms that an account already present in slotCtx is returned
// unchanged — fabrication only kicks in for the missing case.
func TestLoadAndValidateTxAcctsSimd186_LoadedAccountTakesPrecedence(t *testing.T) {
	slotCtx := newSimd186SlotCtx()
	mem, ok := slotCtx.Accounts.(accounts.MemAccounts)
	require.True(t, ok)

	loadedKey := testPubkey(7)
	loaded := &accounts.Account{
		Key:       loadedKey,
		Owner:     addresses.NativeLoaderAddr,
		Lamports:  100_000,
		RentEpoch: 50,
	}
	require.NoError(t, mem.SetAccountWithoutLock(loadedKey, loaded))

	tx := &solana.Transaction{
		Message: solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys: []solana.PublicKey{loadedKey},
		},
	}
	instrsAcct := &accounts.Account{Key: sealevel.SysvarInstructionsAddr}

	txAccts, _, loadedSize, err := loadAndValidateTxAcctsSimd186(
		slotCtx, nil, tx, nil, instrsAcct, math.MaxUint32,
	)
	require.NoError(t, err)
	assert.Equal(t, uint32(txAcctBaseSize), loadedSize)
	require.Len(t, txAccts.Accounts, 1)
	got := txAccts.Accounts[0]
	assert.Equal(t, loadedKey, got.Key)
	assert.Equal(t, uint64(100_000), got.Lamports, "loaded account must not be replaced by fabricated default")
	assert.Equal(t, addresses.NativeLoaderAddr, got.Owner)
	assert.Equal(t, uint64(50), got.RentEpoch)
}

// TestLoadAndValidateTxAcctsSimd186_ProgramRejectsFabricatedDefault is
// the security guardrail: when a tx names a non-existent pubkey as the
// program ID, Pass-1 fabricates a default System-owned account, but
// Pass-2 program validation must reject it (lamports==0 short-circuit)
// rather than letting an empty fabricated account act as a callable
// program.
func TestLoadAndValidateTxAcctsSimd186_ProgramRejectsFabricatedDefault(t *testing.T) {
	slotCtx := newSimd186SlotCtx()

	missingProgram := testPubkey(99)
	tx := &solana.Transaction{
		Message: solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys: []solana.PublicKey{missingProgram},
			Instructions: []solana.CompiledInstruction{
				{ProgramIDIndex: 0, Accounts: nil, Data: nil},
			},
		},
	}
	instrs := []sealevel.Instruction{
		{ProgramId: missingProgram, Data: nil, Accounts: nil},
	}
	instrsAcct := &accounts.Account{Key: sealevel.SysvarInstructionsAddr}

	_, _, _, err := loadAndValidateTxAcctsSimd186(
		slotCtx, nil, tx, instrs, instrsAcct, math.MaxUint32,
	)
	require.Error(t, err, "fabricated default with lamports=0 must be rejected as a program")
	assert.Equal(t, TxErrProgramAccountNotFound, err)
}

// TestLoadAndValidateTxAcctsSimd186_MixedLoadedAndMissing covers a tx
// with both a loaded account (the fee payer) and a missing account
// (the new account being created) — the canonical create_account
// simulate pattern.
func TestLoadAndValidateTxAcctsSimd186_MixedLoadedAndMissing(t *testing.T) {
	slotCtx := newSimd186SlotCtx()
	mem, ok := slotCtx.Accounts.(accounts.MemAccounts)
	require.True(t, ok)

	feePayerKey := testPubkey(1)
	feePayer := &accounts.Account{
		Key:       feePayerKey,
		Owner:     addresses.SystemProgramAddr,
		Lamports:  10 * 1_000_000_000, // 10 SOL
		RentEpoch: math.MaxUint64,
	}
	require.NoError(t, mem.SetAccountWithoutLock(feePayerKey, feePayer))

	missingKey := testPubkey(2) // account being created — does not exist locally
	tx := &solana.Transaction{
		Message: solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys: []solana.PublicKey{feePayerKey, missingKey},
		},
	}
	instrsAcct := &accounts.Account{Key: sealevel.SysvarInstructionsAddr}

	txAccts, _, loadedSize, err := loadAndValidateTxAcctsSimd186(
		slotCtx, nil, tx, nil, instrsAcct, math.MaxUint32,
	)
	require.NoError(t, err)
	assert.Equal(t, uint32(txAcctBaseSize), loadedSize)
	require.Len(t, txAccts.Accounts, 2)

	assert.Equal(t, uint64(10_000_000_000), txAccts.Accounts[0].Lamports, "fee payer should be untouched")
	assert.Equal(t, uint64(0), txAccts.Accounts[1].Lamports, "missing account should be fabricated default")
	assert.Equal(t, addresses.SystemProgramAddr, txAccts.Accounts[1].Owner)
	assert.Equal(t, uint64(math.MaxUint64), txAccts.Accounts[1].RentEpoch)
}

func TestLoadedAcctSizeAccumulatorSimd186MalformedUpgradeableAccount(t *testing.T) {
	accumulator := NewLoadedAcctSizeAccumulatorSimd186(
		newSimd186SlotCtx(),
		math.MaxUint32,
		nil,
	)
	account := &accounts.Account{
		Key:      testPubkey(88),
		Owner:    addresses.BpfLoaderUpgradeableAddr,
		Lamports: 1,
		Data:     []byte{0xff},
	}

	require.NotPanics(t, func() {
		require.NoError(t, accumulator.collectAcct(account))
	})
	assert.Equal(t, uint32(txAcctBaseSize+len(account.Data)), accumulator.loadedAccountsDataSize())
}

func TestLoadedAcctSizeAccumulatorSimd186MissingProgramDataWithoutDatabase(t *testing.T) {
	programDataKey := testPubkey(89)
	programState, err := marshalUpgradeableLoaderStateSized(&sealevel.UpgradeableLoaderState{
		Type: sealevel.UpgradeableLoaderStateTypeProgram,
		Program: sealevel.UpgradeableLoaderStateProgram{
			ProgramDataAddress: programDataKey,
		},
	}, upgradeableLoaderProgramStateSize)
	require.NoError(t, err)
	account := &accounts.Account{
		Key:      testPubkey(88),
		Owner:    addresses.BpfLoaderUpgradeableAddr,
		Lamports: 1,
		Data:     programState,
	}
	accumulator := NewLoadedAcctSizeAccumulatorSimd186(
		newSimd186SlotCtx(),
		math.MaxUint32,
		[]solana.PublicKey{account.Key},
	)

	require.NotPanics(t, func() {
		require.NoError(t, accumulator.collectAcct(account))
	})
	assert.Equal(t, uint32(txAcctBaseSize+len(programState)), accumulator.loadedAccountsDataSize())
}
