package replay

import (
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type boundedTestDurable struct {
	sharedAccountDurable
	reads int
}

func (d *boundedTestDurable) GetAccountWithDataLimit(_ uint64, _ solana.PublicKey, limit uint64) (*accounts.Account, error) {
	d.reads++
	if d.acct == nil {
		return nil, accountsdb.ErrNoAccount
	}
	if uint64(len(d.acct.Data)) > limit {
		return nil, fmt.Errorf("%w: test account", accountsdb.ErrAccountDataLimit)
	}
	return d.acct, nil
}

func TestUnrootedTailGetAccountWithDataLimit(t *testing.T) {
	key := solana.PublicKey{1}
	durable := &boundedTestDurable{sharedAccountDurable: sharedAccountDurable{acct: &accounts.Account{Key: key, Lamports: 1, Data: []byte{1, 2}}}}
	tail := newUnrootedTail(durable, nil, 1, 1, "")
	got, err := tail.GetAccountWithDataLimit(100, key, 1)
	require.ErrorIs(t, err, accountsdb.ErrAccountDataLimit)
	require.Nil(t, got)
	got, err = tail.GetAccountWithDataLimit(100, key, 2)
	require.NoError(t, err)
	got.Data[0] = 3
	require.Equal(t, byte(1), durable.acct.Data[0])

	tail.Add(101, []*accounts.Account{{Key: key, Lamports: 2, Data: []byte{4, 5, 6}}}, nil)
	reads := durable.reads
	_, err = tail.GetAccountWithDataLimit(101, key, 2)
	require.ErrorIs(t, err, accountsdb.ErrAccountDataLimit)
	require.Equal(t, reads, durable.reads, "an oversized newest value must not fall through")
	got, err = tail.GetAccountWithDataLimit(101, key, 3)
	require.NoError(t, err)
	got.Data[0] = 9
	retained, ok := tail.overlay.Lookup(key)
	require.True(t, ok)
	require.Equal(t, byte(4), retained.Data[0])
	tail.Add(102, []*accounts.Account{{Key: key}}, nil)
	got, err = tail.GetAccountWithDataLimit(102, key, 0)
	require.NoError(t, err)
	require.Zero(t, got.Lamports)
	require.Empty(t, got.Data)
	require.Equal(t, reads, durable.reads)

	durable.acct = nil
	_, err = tail.GetAccountWithDataLimit(102, solana.PublicKey{9}, 0)
	require.ErrorIs(t, err, accountsdb.ErrNoAccount)
	unsupported := newUnrootedTail(&sharedAccountDurable{}, nil, 1, 1, "")
	_, err = unsupported.GetAccountWithDataLimit(100, key, 64)
	require.ErrorContains(t, err, "does not support bounded reads")
}
