package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/stretchr/testify/require"
)

func TestUnrootedSharedProgramAccountOwnership(t *testing.T) {
	key := testKey(91)
	durable := &sharedAccountDurable{acct: &accounts.Account{Key: key, Data: []byte{1, 2, 3}}}
	tail := newUnrootedTail(durable, &fakeCommitter{}, 512, 1, "")
	borrowed, err := tail.GetAccountShared(8, key)
	require.NoError(t, err)
	require.Same(t, durable.acct, borrowed)
	owned, err := tail.GetAccount(8, key)
	require.NoError(t, err)
	owned.Data[0] = 99
	require.Equal(t, []byte{1, 2, 3}, borrowed.Data)

	tail.overlay.Add(9, []*accounts.Account{{Key: key, Data: []byte{4, 5, 6}}})
	first, err := tail.GetAccountShared(10, key)
	require.NoError(t, err)
	require.Equal(t, []byte{4, 5, 6}, first.Data)
	again, err := tail.GetAccountShared(10, key)
	require.NoError(t, err)
	require.Same(t, first, again)
	owned, err = tail.GetAccount(10, key)
	require.NoError(t, err)
	owned.Data[0] = 99
	require.Equal(t, []byte{4, 5, 6}, first.Data)

	// A later publication must not mutate a view held by a loader/nested call.
	tail.overlay.Add(10, []*accounts.Account{{Key: key, Data: []byte{7, 8, 9}}})
	latest, err := tail.GetAccountShared(11, key)
	require.NoError(t, err)
	require.Equal(t, []byte{7, 8, 9}, latest.Data)
	require.Equal(t, []byte{4, 5, 6}, first.Data)
	require.Equal(t, []byte{1, 2, 3}, borrowed.Data)
}
