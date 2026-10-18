package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func accountLockTestKeys(n int) solana.PublicKeySlice {
	keys := make(solana.PublicKeySlice, n)
	for i := range keys {
		keys[i] = solana.NewWallet().PublicKey()
	}
	return keys
}

func accountLockTestTx(keys solana.PublicKeySlice) *solana.Transaction {
	return &solana.Transaction{Message: solana.Message{AccountKeys: keys}}
}

func accountLockTestV0Tx(t *testing.T, static solana.PublicKeySlice, lookups int) *solana.Transaction {
	t.Helper()
	tx := accountLockTestTx(static)
	_, err := tx.Message.SetVersion(solana.MessageVersionV0)
	require.NoError(t, err)
	indexes := make(solana.Uint8SliceAsNum, lookups)
	for i := range indexes {
		indexes[i] = uint8(i)
	}
	tx.Message.AddressTableLookups = solana.MessageAddressTableLookupSlice{{
		AccountKey:      solana.NewWallet().PublicKey(),
		ReadonlyIndexes: indexes,
	}}
	return tx
}

func lockFeatures(increaseLimit bool) *features.Features {
	f := features.NewFeaturesDefault()
	if increaseLimit {
		f.EnableFeature(features.IncreaseTxAccountLockLimit, 0)
	}
	return f
}

func requireLockError(t *testing.T, want TransactionErrorType, got *TransactionError) {
	t.Helper()
	require.NotNil(t, got)
	assert.Equal(t, want, got.ErrorType, "got %s", got.ErrorType.String())
	switch want {
	case TransactionErrorTooManyAccountLocks:
		assert.ErrorIs(t, got.InstructionError, TxErrTooManyAccountLocks)
	case TransactionErrorAccountLoadedTwice:
		assert.ErrorIs(t, got.InstructionError, TxErrAccountLoadedTwice)
	}
}

func TestTransactionAccountLockLimit(t *testing.T) {
	assert.Equal(t, 64, TransactionAccountLockLimit(nil))
	assert.Equal(t, 64, TransactionAccountLockLimit(lockFeatures(false)))
	assert.Equal(t, MaxTxAccountLocks, TransactionAccountLockLimit(lockFeatures(true)))
}

func TestValidateAccountLocksAcceptsUniqueKeys(t *testing.T) {
	for _, n := range []int{1, 3, accountLockSetThreshold - 1, accountLockSetThreshold, 64} {
		assert.Nil(t, ValidateAccountLocks(accountLockTestTx(accountLockTestKeys(n)), lockFeatures(false)), "n=%d", n)
	}
}

func TestValidateAccountLocksRejectsDuplicateKeys(t *testing.T) {
	// Both duplicate-detection strategies: pairwise below the set threshold, set above.
	for _, n := range []int{2, 3, accountLockSetThreshold - 1, accountLockSetThreshold, 64} {
		keys := accountLockTestKeys(n)
		keys[n-1] = keys[0]
		requireLockError(t, TransactionErrorAccountLoadedTwice, ValidateAccountLocks(accountLockTestTx(keys), lockFeatures(true)))
	}
}

func TestValidateAccountLocksLimitFollowsFeature(t *testing.T) {
	tx := accountLockTestTx(accountLockTestKeys(65))
	requireLockError(t, TransactionErrorTooManyAccountLocks, ValidateAccountLocks(tx, lockFeatures(false)))
	assert.Nil(t, ValidateAccountLocks(tx, lockFeatures(true)))

	tx = accountLockTestTx(accountLockTestKeys(MaxTxAccountLocks + 1))
	requireLockError(t, TransactionErrorTooManyAccountLocks, ValidateAccountLocks(tx, lockFeatures(true)))
}

func TestValidateAccountLocksTooManyTakesPrecedence(t *testing.T) {
	keys := accountLockTestKeys(65)
	keys[64] = keys[0]
	requireLockError(t, TransactionErrorTooManyAccountLocks, ValidateAccountLocks(accountLockTestTx(keys), lockFeatures(false)))
}

func TestValidateAccountLocksCountsUnresolvedLookups(t *testing.T) {
	tx := accountLockTestV0Tx(t, accountLockTestKeys(2), 63)
	requireLockError(t, TransactionErrorTooManyAccountLocks, ValidateAccountLocks(tx, lockFeatures(false)))
	assert.Nil(t, ValidateAccountLocks(tx, lockFeatures(true)))

	tx = accountLockTestV0Tx(t, accountLockTestKeys(2), 62)
	assert.Nil(t, ValidateAccountLocks(tx, lockFeatures(false)))
}

func TestValidateAccountLocksChecksUnresolvedStaticKeys(t *testing.T) {
	static := accountLockTestKeys(3)
	static[2] = static[1]
	tx := accountLockTestV0Tx(t, static, 1)
	requireLockError(t, TransactionErrorAccountLoadedTwice, ValidateAccountLocks(tx, lockFeatures(true)))
}

func TestValidateAccountLocksChecksResolvedLookupKeys(t *testing.T) {
	static := accountLockTestKeys(2)
	tx := accountLockTestV0Tx(t, static, 2)
	table := tx.Message.AddressTableLookups[0].AccountKey
	// The second looked-up address repeats a static key.
	addresses := solana.PublicKeySlice{solana.NewWallet().PublicKey(), static[1]}
	require.NoError(t, tx.Message.SetAddressTables(map[solana.PublicKey]solana.PublicKeySlice{table: addresses}))
	require.NoError(t, tx.Message.ResolveLookups())
	require.Len(t, tx.Message.AccountKeys, 4)

	requireLockError(t, TransactionErrorAccountLoadedTwice, ValidateAccountLocks(tx, lockFeatures(true)))
}
