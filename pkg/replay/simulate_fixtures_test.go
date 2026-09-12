package replay

import (
	"encoding/base64"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Fixtures captured from live mainnet RPC validation. Each base64 string is
// a serialized transaction that exercises a specific simulate-path branch.
// Constants live in the test file so the suite is self-contained and runs
// without any external state.
const (
	// Single transfer with a missing recipient/system pubkey. Without the
	// SIMD-186 fabricate-default fix, the loader panicked.
	fixtureMissingAccount = "ARH9cDfHfnReizlJ7wUiQUDjBHEmFqxOwkrd58jKTtaSiWDD12ojfj+Q6EFIZdtZPEhjaFBjGEKaso9l8vStIgoBAAEDm5dS3l4ndvoMPGwAgbDa2etV4ba8GhFmzEEoWLnQUzLlsqZyE2J9rbpagOWcYxTmla0sPZSdLWedHDC5wCQ+QwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAgIAAQwCAAAAAQAAAAAAAAA="

	// Tx with NumRequiredSignatures=0 — sanitize must reject before
	// anything tries to index Signatures[0] / AccountKeys[0].
	fixtureZeroSig = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

	// Out-of-range instruction account index.
	fixtureOOBAccount = "AcvGOK/HCvm+jp0iIyg/CBjxoYXidL7Eua6PZr/gYq6SnXSwFBBClGqoT3Dq8SkekT05ydTutpEnhaCXZTkyoQQBAAABm5dS3l4ndvoMPGwAgbDa2etV4ba8GhFmzEEoWLnQUzIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAASoA"

	// NumReadonlySignedAccounts >= NumRequiredSignatures (no writable
	// signer); sanitize must reject.
	fixtureZeroWritableSigners = "AXP1E+UKbcHEW0T4v81ty2POfQ3VR7omBlei30O7AjHX/Me4P2Zidwhl8acF3jrYONVVV2kv8bbCjyLhq/SZVAYBAQABm5dS3l4ndvoMPGwAgbDa2etV4ba8GhFmzEEoWLnQUzIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

	// Header declares 2 required sigs, transaction carries 1.
	fixtureInsufficientSigs = "AVW49xPgdaW13hcaexkKxn9aruubS1QPM2i4Gfyvcj+/ZCml3HapzjPxsKXvpsph6bhHIziIQS+wW7il9prJ4AgCAAACm5dS3l4ndvoMPGwAgbDa2etV4ba8GhFmzEEoWLnQUzLlsqZyE2J9rbpagOWcYxTmla0sPZSdLWedHDC5wCQ+QwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="

	// 2 signatures but only 1 account key.
	fixtureSigsGreaterThanKeys = "AkoD9kRSzxiTCsWNc0tek1JI5Capj2LNsTI0MTUTYwuF15j7urkfyWvnyPQ87UHzaMHkChQ3TA7CBhuNwmojVgVKA/ZEUs8YkwrFjXNLXpNSSOQmqY9izbEyNDE1E2MLhdeY+7q5H8lr58j0PO1B82jB5AoUN0wOwgYbjcJqI1YFAQAAAZuXUt5eJ3b6DDxsAIGw2tnrVeG2vBoRZsxBKFi50FMyAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

	// ProgramIDIndex points outside AccountKeys range.
	fixtureOOBProgram = "AT2SwP5wteXeZK5dcicARe9tPzofzhoUDRBgMS+By+o8CwEidmOMyFLPlqDk1AY8PP0dk5u9zkBCCAhiRXRfVQIBAAABm5dS3l4ndvoMPGwAgbDa2etV4ba8GhFmzEEoWLnQUzIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFjAAA="

	// Tx with no instructions. Should pass sanitize and proceed to
	// fee-payer balance check; payer has 0 SOL → AccountNotFound
	fixtureNoInstructions = "AUoD9kRSzxiTCsWNc0tek1JI5Capj2LNsTI0MTUTYwuF15j7urkfyWvnyPQ87UHzaMHkChQ3TA7CBhuNwmojVgUBAAABm5dS3l4ndvoMPGwAgbDa2etV4ba8GhFmzEEoWLnQUzIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

	// Self-transfer: fee payer also receives.
	fixtureSelfTransfer = "AQaoIo/np/BZLN6khLJM2yEybInNs68QaywxRtscCSguw5/qEYsSrYorN0y0zWSGaGrgFrlD5Oaz96CMsAO3iw8BAAECm5dS3l4ndvoMPGwAgbDa2etV4ba8GhFmzEEoWLnQUzIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQECAAAMAgAAAAEAAAAAAAAA"

	// Duplicate AccountKeys entries.
	fixtureDuplicateKeys = "ARL1Vif1R7PRgEj1eZZzM1uhhoiPseT/kr/MjAVxurE8GtpLADDL6pfDVEege4anhvnZC/dMZPlRkTTgSUr2awIBAAEDm5dS3l4ndvoMPGwAgbDa2etV4ba8GhFmzEEoWLnQUzKbl1LeXid2+gw8bACBsNrZ61XhtrwaEWbMQShYudBTMgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAgIAAQwCAAAAAQAAAAAAAAA="
)

func decodeFixtureTx(t *testing.T, b64 string) *solana.Transaction {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(b64)
	require.NoError(t, err)
	tx, err := solana.TransactionFromBytes(raw)
	require.NoError(t, err)
	return tx
}

// simulateFixtureSlotCtx returns a SlotCtx with the SIMD-186 feature
// enabled and an empty MemAccounts. Suitable for fixtures that exercise
// the sanitize / loader path without needing a populated accountsdb.
//
// FeeRateGovernor is non-nil because LoadAndExecuteTransaction reaches
// for it during execution context construction; production callers
// always set it.
func simulateFixtureSlotCtx() *sealevel.SlotCtx {
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)
	return &sealevel.SlotCtx{
		Features:        feats,
		Accounts:        accounts.NewMemAccounts(),
		FeeRateGovernor: &sealevel.FeeRateGovernor{},
	}
}

func runSimulateFixture(t *testing.T, b64 string) LoadAndExecuteTransactionOutput {
	t.Helper()
	defer withEmptyRecentBlockhashesSysvarFixture()()
	return LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:      simulateFixtureSlotCtx(),
		Transaction:  decodeFixtureTx(t, b64),
		IsSimulation: true,
	})
}

// withEmptyRecentBlockhashesSysvarFixture seeds the global cache so
// IsTransactionAgeValid does not nil-deref. Production seeds this via
// cacheConstantSysvars before any RPC call.
func withEmptyRecentBlockhashesSysvarFixture() func() {
	prev := sealevel.SysvarCache.RecentBlockHashes.Sysvar
	empty := sealevel.SysvarRecentBlockhashes{}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &empty
	return func() { sealevel.SysvarCache.RecentBlockHashes.Sysvar = prev }
}

// Agave validates the fee payer before loading the remaining accounts, so the
// empty fixture bank deterministically reports AccountNotFound rather than a
// later program-load failure. Either way, malformed local state must never
// panic the simulation worker.
func TestSimulateFixture_MissingAccount(t *testing.T) {
	out := runSimulateFixture(t, fixtureMissingAccount)
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorAccountNotFound, out.ProcessingResult.TransactionError.ErrorType)
}

func TestSimulateFixture_ZeroSig(t *testing.T) {
	out := runSimulateFixture(t, fixtureZeroSig)
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorSanitizeFailure, out.ProcessingResult.TransactionError.ErrorType)
}

func TestSimulateFixture_OOBAccount(t *testing.T) {
	out := runSimulateFixture(t, fixtureOOBAccount)
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorSanitizeFailure, out.ProcessingResult.TransactionError.ErrorType)
}

func TestSimulateFixture_ZeroWritableSigners(t *testing.T) {
	out := runSimulateFixture(t, fixtureZeroWritableSigners)
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorSanitizeFailure, out.ProcessingResult.TransactionError.ErrorType)
}

func TestSimulateFixture_InsufficientSigs(t *testing.T) {
	out := runSimulateFixture(t, fixtureInsufficientSigs)
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorSanitizeFailure, out.ProcessingResult.TransactionError.ErrorType)
}

func TestSimulateFixture_SigsGreaterThanKeys(t *testing.T) {
	out := runSimulateFixture(t, fixtureSigsGreaterThanKeys)
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorSanitizeFailure, out.ProcessingResult.TransactionError.ErrorType)
}

func TestSimulateFixture_OOBProgram(t *testing.T) {
	out := runSimulateFixture(t, fixtureOOBProgram)
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorSanitizeFailure, out.ProcessingResult.TransactionError.ErrorType)
}

func TestSimulateFixture_NoInstructions(t *testing.T) {
	out := runSimulateFixture(t, fixtureNoInstructions)
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorAccountNotFound, out.ProcessingResult.TransactionError.ErrorType)
}

func TestSimulateFixture_SelfTransfer(t *testing.T) {
	out := runSimulateFixture(t, fixtureSelfTransfer)
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorAccountNotFound, out.ProcessingResult.TransactionError.ErrorType)
}

func TestSimulateFixture_DuplicateKeys(t *testing.T) {
	out := runSimulateFixture(t, fixtureDuplicateKeys)
	require.NotNil(t, out.ProcessingResult.TransactionError)
	assert.Equal(t, TransactionErrorAccountNotFound, out.ProcessingResult.TransactionError.ErrorType)
}
