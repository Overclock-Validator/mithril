package replay

import (
	"encoding/json"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrecomputedInstructionAccountsMatchLegacyResolution(t *testing.T) {
	payer := testPubkey(1)
	destination := testPubkey(2)
	tx := &solana.Transaction{
		Message: solana.Message{
			Header: solana.MessageHeader{
				NumRequiredSignatures:       1,
				NumReadonlyUnsignedAccounts: 1,
			},
			// Include a duplicate key and duplicate instruction references. The
			// old resolver selected the first transaction index for a pubkey.
			AccountKeys: []solana.PublicKey{payer, destination, destination, addresses.SystemProgramAddr},
			Instructions: []solana.CompiledInstruction{{
				ProgramIDIndex: 3,
				Accounts:       []uint16{0, 2, 2},
			}},
		},
	}

	feats := features.NewFeaturesDefault()
	instrs, precomputed, _, err := instrsAndAcctMetasFromTx(tx, feats)
	require.NoError(t, err)
	require.Len(t, instrs, 1)
	require.Len(t, precomputed, 1)

	txAccts := sealevel.NewTransactionAccounts([]accounts.Account{
		{Key: payer},
		{Key: destination},
		{Key: destination},
		{Key: addresses.SystemProgramAddr},
	})
	legacy := sealevel.InstructionAcctsFromAccountMetas(instrs[0].Accounts, *txAccts)
	assert.Equal(t, legacy, precomputed[0])
}

func TestInstructionsSysvarAccountIndex(t *testing.T) {
	tx := &solana.Transaction{Message: solana.Message{AccountKeys: []solana.PublicKey{testPubkey(1)}}}
	assert.Equal(t, -1, instructionsSysvarAccountIndex(tx))

	tx.Message.AccountKeys = append(tx.Message.AccountKeys, sealevel.SysvarInstructionsAddr, sealevel.SysvarInstructionsAddr)
	assert.Equal(t, 1, instructionsSysvarAccountIndex(tx), "the first duplicate matches TransactionCtx.IndexOfAccount semantics")
}

// TestLoadAndExecuteTransaction_RejectsUnsignedTx confirms the sanitize
// guard at the entry of LoadAndExecuteTransaction returns a clean
// SanitizeFailure when a malformed (zero-signature) tx is submitted —
// avoiding the index-out-of-range panic at tx.Signatures[0].
func TestLoadAndExecuteTransaction_RejectsUnsignedTx(t *testing.T) {
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)
	slotCtx := &sealevel.SlotCtx{Features: feats}

	tx := &solana.Transaction{
		Message: solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 0},
			AccountKeys: []solana.PublicKey{testPubkey(1)},
		},
		// Signatures intentionally empty — this would have panicked at
		// transaction_processing_pure.go before the guard.
	}

	require.NotPanics(t, func() {
		out := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
			SlotCtx:      slotCtx,
			Transaction:  tx,
			IsSimulation: true,
		})
		require.NotNil(t, out.ProcessingResult.TransactionError)
		assert.Equal(t, TransactionErrorSanitizeFailure, out.ProcessingResult.TransactionError.ErrorType)
	})
}

func TestLoadAndExecuteTransaction_GatesV1UntilFeatureActivation(t *testing.T) {
	feats := features.NewFeaturesDefault()
	slotCtx := &sealevel.SlotCtx{Features: feats}
	msg := solana.Message{
		Header: solana.MessageHeader{
			NumRequiredSignatures:       1,
			NumReadonlyUnsignedAccounts: 1,
		},
		AccountKeys: []solana.PublicKey{testPubkey(1), testPubkey(2)},
		Instructions: []solana.CompiledInstruction{{
			ProgramIDIndex: 1,
		}},
	}
	_, err := msg.SetVersion(solana.MessageVersionV1)
	require.NoError(t, err)
	tx := &solana.Transaction{Message: msg, Signatures: []solana.Signature{{}}}

	out := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:     slotCtx,
		Transaction: tx,
	})
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorUnsupportedVersion, out.ProcessingResult.TransactionError.ErrorType)
	assert.ErrorIs(t, out.ProcessingResult.TransactionError.InstructionError, TxErrUnsupportedVersion)
}

func TestLoadAndExecuteTransaction_GatesV1BeforeSanitizing(t *testing.T) {
	feats := features.NewFeaturesDefault()
	msg := solana.Message{
		Header:      solana.MessageHeader{NumRequiredSignatures: 1},
		AccountKeys: []solana.PublicKey{testPubkey(1), testPubkey(1)}, // V1 forbids duplicate addresses.
	}
	_, err := msg.SetVersion(solana.MessageVersionV1)
	require.NoError(t, err)
	tx := &solana.Transaction{Message: msg, Signatures: []solana.Signature{{}}}

	out := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:     &sealevel.SlotCtx{Features: feats},
		Transaction: tx,
	})
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorUnsupportedVersion, out.ProcessingResult.TransactionError.ErrorType)
}

// TestLoadAndExecuteTransaction_RejectsInsufficientSignatures covers the
// other Agave-sanitize case: header declares more required signatures
// than the tx actually carries.
func TestLoadAndExecuteTransaction_RejectsInsufficientSignatures(t *testing.T) {
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)
	slotCtx := &sealevel.SlotCtx{Features: feats}

	tx := &solana.Transaction{
		Message: solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 2},
			AccountKeys: []solana.PublicKey{testPubkey(1), testPubkey(2)},
		},
		Signatures: []solana.Signature{{}}, // 1 sig but header demands 2
	}

	require.NotPanics(t, func() {
		out := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
			SlotCtx:      slotCtx,
			Transaction:  tx,
			IsSimulation: true,
		})
		require.NotNil(t, out.ProcessingResult.TransactionError)
		assert.Equal(t, TransactionErrorSanitizeFailure, out.ProcessingResult.TransactionError.ErrorType)
	})
}

// TestLoadAndExecuteTransaction_RejectsZeroNumRequiredSignatures matches
// Agave's Message::sanitize rule: every tx must declare at least one
// required signer (the fee payer). A header with NumRequiredSignatures=0
// is malformed even if Signatures is non-empty.
func TestLoadAndExecuteTransaction_RejectsZeroNumRequiredSignatures(t *testing.T) {
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)
	slotCtx := &sealevel.SlotCtx{Features: feats}

	tx := &solana.Transaction{
		Message: solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 0},
			AccountKeys: []solana.PublicKey{testPubkey(1)},
		},
		Signatures: []solana.Signature{{}}, // signatures present but header says 0
	}

	out := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:      slotCtx,
		Transaction:  tx,
		IsSimulation: true,
	})
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorSanitizeFailure, out.ProcessingResult.TransactionError.ErrorType)
	// InstructionError is intentionally nil so the RPC renderer falls
	// back to ErrorType.String() and emits Agave-format "SanitizeFailure".
	assert.Nil(t, out.ProcessingResult.TransactionError.InstructionError)
	assert.Equal(t, "SanitizeFailure", out.ProcessingResult.TransactionError.ErrorType.String())
}

// TestLoadAndExecuteTransaction_RejectsMoreSigsThanKeys covers Agave's
// Transaction::sanitize Tx-2 rule: len(signatures) <= len(account_keys).
// Without this guard, a tx with empty AccountKeys but non-empty
// Signatures would later panic at fee-payer access (AccountKeys[0])
// or at fees.go:106 ("no fee payer").
func TestLoadAndExecuteTransaction_RejectsMoreSigsThanKeys(t *testing.T) {
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)
	slotCtx := &sealevel.SlotCtx{Features: feats}

	tx := &solana.Transaction{
		Message: solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys: nil, // no keys at all
		},
		Signatures: []solana.Signature{{}}, // 1 sig but 0 keys
	}

	require.NotPanics(t, func() {
		out := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
			SlotCtx:      slotCtx,
			Transaction:  tx,
			IsSimulation: true,
		})
		require.NotNil(t, out.ProcessingResult.TransactionError)
		assert.Equal(t, TransactionErrorSanitizeFailure, out.ProcessingResult.TransactionError.ErrorType)
		assert.Nil(t, out.ProcessingResult.TransactionError.InstructionError)
	})
}

// TestLoadAndExecuteTransaction_RejectsZeroWritableSigners covers
// Agave's Message::sanitize Msg-2 rule (legacy.rs:149):
// NumReadonlySignedAccounts >= NumRequiredSignatures is rejected because
// it leaves no writable signer (no fee-payer). Without this guard,
// fee deduction operates on a read-only account, producing wrong
// simulate output.
func TestLoadAndExecuteTransaction_RejectsZeroWritableSigners(t *testing.T) {
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)
	slotCtx := &sealevel.SlotCtx{Features: feats}

	tx := &solana.Transaction{
		Message: solana.Message{
			Header: solana.MessageHeader{
				NumRequiredSignatures:       1,
				NumReadonlySignedAccounts:   1, // ← only signer is read-only
				NumReadonlyUnsignedAccounts: 0,
			},
			AccountKeys: []solana.PublicKey{testPubkey(1)},
		},
		Signatures: []solana.Signature{{}},
	}

	out := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:      slotCtx,
		Transaction:  tx,
		IsSimulation: true,
	})
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorSanitizeFailure, out.ProcessingResult.TransactionError.ErrorType)
}

// TestLoadAndExecuteTransaction_RejectsOOBInstructionAccountIndex covers
// the CRITICAL panic vector: solana-go's
// CompiledInstruction.ResolveInstructionAccounts dereferences metas[acct]
// without a bounds check (transaction.go:148-150). A user-submitted tx
// with an out-of-range index (e.g. Accounts:[42] when only 1 key exists)
// would crash the RPC worker. The sanitize guard rejects such txs.
func TestLoadAndExecuteTransaction_RejectsOOBInstructionAccountIndex(t *testing.T) {
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)
	slotCtx := &sealevel.SlotCtx{Features: feats}

	tx := &solana.Transaction{
		Message: solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys: []solana.PublicKey{testPubkey(1)},
			Instructions: []solana.CompiledInstruction{
				{ProgramIDIndex: 0, Accounts: []uint16{42}, Data: nil}, // 42 >> len(keys)=1
			},
		},
		Signatures: []solana.Signature{{}},
	}

	require.NotPanics(t, func() {
		out := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
			SlotCtx:      slotCtx,
			Transaction:  tx,
			IsSimulation: true,
		})
		require.NotNil(t, out.ProcessingResult.TransactionError)
		assert.Equal(t, TransactionErrorSanitizeFailure, out.ProcessingResult.TransactionError.ErrorType)
		assert.Nil(t, out.ProcessingResult.TransactionError.InstructionError)
	})
}

// TestLoadAndExecuteTransaction_RejectsOOBProgramIDIndex covers the
// related but bounds-checked path: ProgramIDIndex out of range is
// rejected by solana-go's ResolveProgramIDIndex with a clean error,
// but only AFTER instructions are parsed. Our sanitize catches it
// up-front so no parsing is wasted.
func TestLoadAndExecuteTransaction_RejectsOOBProgramIDIndex(t *testing.T) {
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)
	slotCtx := &sealevel.SlotCtx{Features: feats}

	tx := &solana.Transaction{
		Message: solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys: []solana.PublicKey{testPubkey(1)},
			Instructions: []solana.CompiledInstruction{
				{ProgramIDIndex: 99, Accounts: nil, Data: nil}, // 99 >> 1
			},
		},
		Signatures: []solana.Signature{{}},
	}

	require.NotPanics(t, func() {
		out := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
			SlotCtx:      slotCtx,
			Transaction:  tx,
			IsSimulation: true,
		})
		require.NotNil(t, out.ProcessingResult.TransactionError)
		assert.Equal(t, TransactionErrorSanitizeFailure, out.ProcessingResult.TransactionError.ErrorType)
	})
}

// TestLoadAndExecuteTransaction_RejectsTooManyInstructions covers the
// instruction-count cap. The cap is feature-gated on
// StaticInstructionLimit (matching block-replay's check at
// transaction.go:453-457) so pre-activation networks behave identically
// to Agave (mid-execution failure rather than SanitizeFailure).
func TestLoadAndExecuteTransaction_RejectsTooManyInstructions(t *testing.T) {
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)
	feats.EnableFeature(features.StaticInstructionLimit, 0)
	slotCtx := &sealevel.SlotCtx{Features: feats}

	instrs := make([]solana.CompiledInstruction, 65) // > 64 cap
	for i := range instrs {
		instrs[i] = solana.CompiledInstruction{ProgramIDIndex: 0, Accounts: nil, Data: nil}
	}
	tx := &solana.Transaction{
		Message: solana.Message{
			Header:       solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys:  []solana.PublicKey{testPubkey(1)},
			Instructions: instrs,
		},
		Signatures: []solana.Signature{{}},
	}

	out := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:      slotCtx,
		Transaction:  tx,
		IsSimulation: true,
	})
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorSanitizeFailure, out.ProcessingResult.TransactionError.ErrorType)
}

// TestTransactionError_MarshalJSON_UnitVariant verifies that unit-variant
// TransactionErrors marshal to a bare JSON string matching Agave's serde
// rendering (e.g. "BlockhashNotFound", not "TxErrBlockhashNotFound").
func TestTransactionError_MarshalJSON_UnitVariant(t *testing.T) {
	cases := map[TransactionErrorType]string{
		TransactionErrorBlockhashNotFound:      `"BlockhashNotFound"`,
		TransactionErrorSanitizeFailure:        `"SanitizeFailure"`,
		TransactionErrorAccountNotFound:        `"AccountNotFound"`,
		TransactionErrorProgramAccountNotFound: `"ProgramAccountNotFound"`,
	}
	for in, want := range cases {
		got, err := json.Marshal(&TransactionError{ErrorType: in})
		require.NoError(t, err)
		assert.Equal(t, want, string(got), "variant %d", in)
	}
}

// TestTransactionError_MarshalJSON_InstructionError verifies the
// {"InstructionError":[idx, "VariantName"]} tuple shape that Agave emits.
// Critical for Anchor IDL error decoding to work.
func TestTransactionError_MarshalJSON_InstructionError(t *testing.T) {
	idx := uint8(2)
	got, err := json.Marshal(&TransactionError{
		ErrorType:        TransactionErrorInstructionError,
		InstructionIndex: &idx,
		InstructionError: sealevel.InstrErrInvalidArgument,
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"InstructionError":[2,"InvalidArgument"]}`, string(got))
}

// TestTransactionError_MarshalJSON_CustomCarriesShape verifies the
// {"InstructionError":[idx, {"Custom":N}]} object shape Agave uses for
// program-defined error codes. Mithril does not yet preserve the program-
// specific u32, so the inner code is 0 — but the SHAPE is correct so
// Anchor / web3.js parsers don't break on the response.
func TestTransactionError_MarshalJSON_CustomCarriesShape(t *testing.T) {
	idx := uint8(0)
	got, err := json.Marshal(&TransactionError{
		ErrorType:        TransactionErrorInstructionError,
		InstructionIndex: &idx,
		InstructionError: sealevel.InstrErrCustom,
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"InstructionError":[0,{"Custom":0}]}`, string(got))
}

// TestTransactionError_MarshalJSON_TupleStructVariants covers the
// account_index struct variants (InsufficientFundsForRent,
// ProgramExecutionTemporarilyRestricted) and the u8 tuple variant
// (DuplicateInstruction).
func TestTransactionError_MarshalJSON_TupleStructVariants(t *testing.T) {
	ai := uint8(3)
	got, err := json.Marshal(&TransactionError{
		ErrorType:    TransactionErrorInsufficientFundsForRent,
		AccountIndex: &ai,
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"InsufficientFundsForRent":{"account_index":3}}`, string(got))

	idx := uint8(5)
	got, err = json.Marshal(&TransactionError{
		ErrorType:        TransactionErrorDuplicateInstruction,
		InstructionIndex: &idx,
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"DuplicateInstruction":5}`, string(got))
}

func TestLoadAndExecuteTransaction_RentFailureCarriesNonPayerAccountIndex(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()

	rent := sealevel.NewDefaultRentSysvar()
	destination, err := slotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	// Legacy rent rules reject crediting a rent-paying account while it remains
	// below the exemption threshold. The transfer credits eight lamports, so
	// account 1—not the fee payer at account 0—must be named in the error.
	destination.Lamports = rent.MinimumBalance(0) - 10
	require.NoError(t, slotCtx.SetAccount(txfixture.DestPubkey(), destination))

	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(7))
	require.NoError(t, err)
	output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:     slotCtx,
		Transaction: tx,
	})

	require.NotNil(t, output.ProcessingResult.TransactionError)
	txErr := output.ProcessingResult.TransactionError
	assert.Equal(t, TransactionErrorInsufficientFundsForRent, txErr.ErrorType)
	require.NotNil(t, txErr.AccountIndex)
	assert.Equal(t, uint8(1), *txErr.AccountIndex)
}

// TestTransactionErrorType_String spot-checks the Agave-format wire
// names for variants the simulate handler can fall back to when the
// inner Go error is nil.
func TestTransactionErrorType_String(t *testing.T) {
	cases := map[TransactionErrorType]string{
		TransactionErrorSanitizeFailure:         "SanitizeFailure",
		TransactionErrorBlockhashNotFound:       "BlockhashNotFound",
		TransactionErrorAccountNotFound:         "AccountNotFound",
		TransactionErrorProgramAccountNotFound:  "ProgramAccountNotFound",
		TransactionErrorInsufficientFundsForFee: "InsufficientFundsForFee",
	}
	for in, want := range cases {
		assert.Equal(t, want, in.String(), "variant %d should stringify to Agave name", in)
	}
}
