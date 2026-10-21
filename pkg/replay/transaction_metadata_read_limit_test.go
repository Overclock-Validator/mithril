package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type metadataLimitedReader struct {
	account *accounts.Account
	limits  []uint64
}

func (*metadataLimitedReader) GetAccount(uint64, solana.PublicKey) (*accounts.Account, error) {
	panic("metadata attempted an unbounded backing read")
}

func (reader *metadataLimitedReader) GetAccountWithDataLimit(_ uint64, _ solana.PublicKey, limit uint64) (*accounts.Account, error) {
	reader.limits = append(reader.limits, limit)
	if reader.account == nil {
		return nil, accountsdb.ErrNoAccount
	}
	if uint64(len(reader.account.Data)) > limit {
		return nil, accountsdb.ErrAccountDataLimit
	}
	return reader.account.Clone(), nil
}

type metadataUnboundedReader struct{}

func (metadataUnboundedReader) GetAccount(uint64, solana.PublicKey) (*accounts.Account, error) {
	panic("metadata must refuse an unbounded reader")
}

func TestTransactionMetadataBackingReadLimit(t *testing.T) {
	key := solana.PublicKey{1}
	view := accounts.NewMemAccounts()
	reader := &metadataLimitedReader{account: &accounts.Account{Key: key, Lamports: 1, Data: make([]byte, metadataAccountDataLimit+1)}}
	ctx := &sealevel.SlotCtx{Accounts: view, UnrootedRead: reader}
	got, err := transactionMetadataAccount(ctx, key, nil)
	require.ErrorIs(t, err, accountsdb.ErrAccountDataLimit)
	require.Nil(t, got)
	require.Equal(t, []uint64{metadataAccountDataLimit}, reader.limits)

	// Borrowing an already resident account adds no account-data copy. Its size
	// must not cause an otherwise safe metadata read to consult an older view.
	require.NoError(t, view.SetAccountWithoutLock(key, reader.account))
	got, err = transactionMetadataAccount(ctx, key, nil)
	require.NoError(t, err)
	require.Same(t, reader.account, got)
	require.Len(t, reader.limits, 1)
	ctx.Accounts = accounts.NewMemAccounts()
	ctx.UnrootedRead = metadataUnboundedReader{}
	got, err = transactionMetadataAccount(ctx, key, nil)
	require.ErrorContains(t, err, "requires bounded backing account reads")
	require.Nil(t, got)
}

func TestTransactionMetadataHydrationReadBudget(t *testing.T) {
	key := solana.PublicKey{1}
	reader := &metadataLimitedReader{account: &accounts.Account{Key: key, Lamports: 1, Data: []byte{1, 2, 3}}}
	ctx := &sealevel.SlotCtx{Accounts: accounts.NewMemAccounts(), UnrootedRead: reader}
	remaining := uint64(5)
	got, err := transactionMetadataAccount(ctx, key, &remaining)
	require.NoError(t, err)
	require.Equal(t, reader.account, got)
	require.Equal(t, uint64(2), remaining)
	got, err = transactionMetadataAccount(ctx, key, &remaining)
	require.ErrorIs(t, err, accountsdb.ErrAccountDataLimit)
	require.Nil(t, got)
	require.Equal(t, uint64(2), remaining, "refused reads must not underflow the remaining budget")
	require.Equal(t, []uint64{5, 2}, reader.limits)

	reader.account = &accounts.Account{Key: key}
	remaining = 0
	got, err = transactionMetadataAccount(ctx, key, &remaining)
	require.NoError(t, err)
	require.Zero(t, got.Lamports, "zero-byte tombstone remains authoritative")
	require.Empty(t, got.Data)
	reader.account = nil
	got, err = transactionMetadataAccount(ctx, key, &remaining)
	require.NoError(t, err)
	require.Nil(t, got, "bounded reader may still prove absence")
}
