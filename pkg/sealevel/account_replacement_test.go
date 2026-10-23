package sealevel

import (
	"bytes"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAccountDataReplacementIsolation(t *testing.T) {
	for _, size := range []int{0, 2, 5, 9000} {
		ctx := serializationTestContext(t, 1)
		tx := ctx.TransactionContext
		ix, err := tx.CurrentInstructionCtx()
		require.NoError(t, err)
		acct, err := ix.BorrowInstructionAccount(tx, 0)
		require.NoError(t, err)
		original := acct.Account
		before := original.Clone()
		tx.Accounts.Shared[1] = true
		var copied []uint64
		tx.Accounts.OnFirstWriteClone = func(n uint64) { copied = append(copied, n) }
		replacement := bytes.Repeat([]byte{42}, size)
		require.NoError(t, acct.SetData(ctx.Features, replacement))
		require.NotSame(t, original, acct.Account)
		require.Same(t, acct.Account, tx.Accounts.Accounts[1])
		require.Equal(t, before, original)
		require.False(t, tx.Accounts.Shared[1])
		require.True(t, tx.Accounts.Touched[1])
		require.Equal(t, []uint64{0}, copied)
		require.EqualValues(t, size-5, tx.AccountsResizeDelta)
		require.Equal(t, replacement, acct.Data())
		// Adoption is intentional: detach at the existing block-end boundary, not
		// at each CPI. This test guards against accidentally changing that lifetime.
		if size > 0 {
			replacement[0] = 43
			require.Equal(t, byte(43), acct.Data()[0])
		}
		first := acct.Account
		require.NoError(t, acct.SetData(ctx.Features, []byte{1, 2, 3}))
		require.Same(t, first, acct.Account)
		require.Equal(t, []uint64{0}, copied)
		require.EqualValues(t, -2, tx.AccountsResizeDelta)
		acct.Drop()
		// A duplicate instruction reference observes the same replacement.
		duplicate, err := ix.BorrowInstructionAccount(tx, 1)
		require.NoError(t, err)
		require.Same(t, first, duplicate.Account)
		require.Equal(t, []byte{1, 2, 3}, duplicate.Data())
		duplicate.Drop()
		require.Equal(t, before, original)
	}
}

func TestAccountDataReplacementErrorsDoNotTouch(t *testing.T) {
	for _, kind := range []string{"readonly", "external", "external_resize", "executable", "oversized", "invalid_index"} {
		t.Run(kind, func(t *testing.T) {
			ctx := serializationTestContext(t, 1)
			tx := ctx.TransactionContext
			ix, err := tx.CurrentInstructionCtx()
			require.NoError(t, err)
			acct, err := ix.BorrowInstructionAccount(tx, 0)
			require.NoError(t, err)
			defer tx.Accounts.Unlock(1)
			data := []byte{9, 8, 7, 6, 5}
			var want error
			switch kind {
			case "readonly":
				ix.InstructionAccounts[0].IsWritable = false
				want = InstrErrReadonlyDataModified
			case "external":
				acct.Account.Owner = [32]byte{77}
				want = InstrErrExternalAccountDataModified
			case "external_resize":
				acct.Account.Owner = [32]byte{77}
				data = nil
				want = InstrErrAccountDataSizeChanged
			case "executable":
				ctx.Features.DisableFeature(features.RemoveAccountsExecutableFlagChecks)
				acct.Account.Executable = true
				want = InstrErrExecutableDataModified
			case "oversized":
				data = make([]byte, MaxPermittedDataLength+1)
				want = InstrErrInvalidRealloc
			case "invalid_index":
				acct.IndexInTransaction = 99
				want = InstrErrNotEnoughAccountKeys
			}
			original, before := acct.Account, acct.Account.Clone()
			tx.Accounts.Shared[1] = true
			tx.AccountsResizeDelta = 123
			tx.Accounts.OnFirstWriteClone = func(uint64) { t.Fatal("failed replacement cloned") }
			require.ErrorIs(t, acct.SetData(ctx.Features, data), want)
			require.Same(t, original, acct.Account)
			require.Same(t, original, tx.Accounts.Accounts[1])
			require.Equal(t, before, original)
			require.True(t, tx.Accounts.Shared[1])
			require.False(t, tx.Accounts.Touched[1])
			require.EqualValues(t, 123, tx.AccountsResizeDelta)
		})
	}
}

func TestAccountDataReplacementAfterOrdinaryWrite(t *testing.T) {
	ctx := serializationTestContext(t, 1)
	tx := ctx.TransactionContext
	ix, err := tx.CurrentInstructionCtx()
	require.NoError(t, err)
	acct, err := ix.BorrowInstructionAccount(tx, 0)
	require.NoError(t, err)
	defer acct.Drop()
	original := acct.Account
	tx.Accounts.Shared[1] = true
	var copied []uint64
	tx.Accounts.OnFirstWriteClone = func(n uint64) { copied = append(copied, n) }
	require.NoError(t, acct.SetLamports(2, ctx.Features))
	require.Equal(t, []uint64{5}, copied)
	require.NoError(t, acct.SetData(ctx.Features, []byte{42}))
	require.Equal(t, []uint64{5}, copied)
	require.EqualValues(t, 1, original.Lamports)
	require.Equal(t, []byte{1, 2, 3, 4, 5}, original.Data)
}

func TestAccountDataReplacementDoesNotChangeOrdinaryTouch(t *testing.T) {
	original := &accounts.Account{Data: []byte{1, 2, 3}}
	tx := NewTransactionAccountsFromRefs([]*accounts.Account{original}, []bool{true})
	var copied uint64
	tx.OnFirstWriteClone = func(n uint64) { copied += n }
	clone, err := tx.Touch(0)
	require.NoError(t, err)
	clone.Data[0] = 9
	require.Equal(t, byte(1), original.Data[0])
	require.EqualValues(t, 3, copied)
}
