package sealevel

import (
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type missingSharedParentSource struct{ *programReadSource }

func (s missingSharedParentSource) GetAccountShared(uint64, solana.PublicKey) (*accounts.Account, error) {
	return nil, nil
}

func TestParentAccountReadOnlyOwnershipAndFallbacks(t *testing.T) {
	key := benchPubkey(0x92)
	source := &programReadSource{}
	original := &accounts.Account{Key: key, Lamports: 100, Data: []byte{1, 2, 3}}
	source.account.Store(original)
	ctx := &SlotCtx{Slot: 42, UnrootedRead: source}
	shared, err := ctx.ParentAccountReadOnly(key)
	require.NoError(t, err)
	require.Same(t, original, shared)
	mutable := shared.Clone()
	mutable.Data[0]++
	require.Equal(t, byte(1), original.Data[0])

	for _, reader := range []AccountReader{ownedProgramReadSource{source}, missingSharedParentSource{source}} {
		ctx.UnrootedRead = reader
		owned, err := ctx.ParentAccountReadOnly(key)
		require.NoError(t, err)
		require.NotSame(t, original, owned)
		owned.Data[0]++
		require.Equal(t, byte(1), original.Data[0])
	}

	ctx.UnrootedRead = source
	source.err = errors.New("parent read failed")
	_, err = ctx.ParentAccountReadOnly(key)
	require.ErrorIs(t, err, source.err)
	source.err = nil
	bank, err := NewBankSysvars(42, clockSysvarTestAccount(SysvarClock{Slot: 42}))
	require.NoError(t, err)
	require.NoError(t, ctx.PublishBankSysvars(bank))
	reads := source.shared.Load()
	got, err := ctx.ParentAccountReadOnly(solana.PublicKey(SysvarClockAddr))
	require.NoError(t, err)
	want, err := ctx.GetAccountFromAccountsDb(solana.PublicKey(SysvarClockAddr))
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, reads, source.shared.Load(), "bank sysvars must bypass the shared source")
	// A bank-pinned missing sysvar must not fall through to unrelated source data.
	_, err = ctx.ParentAccountReadOnly(solana.PublicKey(SysvarRentAddr))
	require.Error(t, err)
	require.Equal(t, reads, source.shared.Load())
}
