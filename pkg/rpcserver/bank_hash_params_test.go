package rpcserver

import (
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/cockroachdb/pebble/v2"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func newRPCBankHashDB(t *testing.T) *accountsdb.AccountsDb {
	t.Helper()
	store, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return &accountsdb.AccountsDb{BankHashStore: store}
}

func TestBankHashParametersExactSlots(t *testing.T) {
	db := newRPCBankHashDB(t)
	server := &RpcServer{acctsDb: db}
	for i, slot := range []uint64{0, 10, 1<<53 + 1, ^uint64(0)} {
		hash := [32]byte{byte(i + 1)}
		require.NoError(t, db.StoreBankHashForSlot(slot, hash[:]))
		got, err := server.GetBankHash(t.Context(), jsonrpc.RawParams(fmt.Sprintf(`[%d]`, slot)))
		require.NoError(t, err)
		require.Equal(t, base58.Encode(hash[:]), got)
	}
	_, err := server.GetBankHash(t.Context(), jsonrpc.RawParams(`[11]`))
	require.Error(t, err, "a missing stored bank hash must remain unavailable")
}

func TestBankHashParametersRejectInvalid(t *testing.T) {
	db := newRPCBankHashDB(t)
	hash := [32]byte{10}
	require.NoError(t, db.StoreBankHashForSlot(10, hash[:]))
	server := &RpcServer{acctsDb: db}
	for _, raw := range []string{`[]`, `[10.9]`, `[-1]`, `[18446744073709551616]`, `[null]`, `["10"]`, `[true]`, `[10,11]`, `[1e1]`, `{}`, `null`} {
		t.Run(raw, func(t *testing.T) {
			_, err := server.GetBankHash(t.Context(), jsonrpc.RawParams(raw))
			var invalid *InvalidParamsError
			require.ErrorAs(t, err, &invalid)
			wire, err := invalid.ToJSONRPCError()
			require.NoError(t, err)
			require.Equal(t, rpcCodeInvalidParams, wire.Code)
		})
	}
}
