package rpcserver

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/cockroachdb/pebble/v2"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
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

func putRPCBankHash(t *testing.T, db *accountsdb.AccountsDb, slot uint64, hash [32]byte) {
	require.NoError(t, db.BankHashStore.Set(binary.LittleEndian.AppendUint64(nil, slot), hash[:], nil))
}

func TestBankHashParametersExactSlots(t *testing.T) {
	db := newRPCBankHashDB(t)
	server := &RpcServer{acctsDb: db}
	for i, slot := range []uint64{0, 10, 1<<53 + 1, ^uint64(0)} {
		hash := [32]byte{byte(i + 1)}
		putRPCBankHash(t, db, slot, hash)
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
	const exactSlot = 1<<53 + 1
	putRPCBankHash(t, db, exactSlot, hash)
	server := NewRpcServer(db, 0, &sealevel.SysvarEpochSchedule{}, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	exact := fmt.Sprintf("[%d]", exactSlot)
	for _, raw := range []string{exact, `[]`, `[10.9]`, `[-1]`, `[18446744073709551616]`, `[null]`, `["10"]`, `[true]`, `[10,11]`, `[1e1]`, `{}`, `null`} {
		t.Run(raw, func(t *testing.T) {
			resp := httptest.NewRecorder()
			body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getBankHash","params":%s}`, raw)
			server.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
			var reply struct {
				Result string                `json:"result"`
				Error  *jsonrpc.JSONRPCError `json:"error"`
			}
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &reply))
			if raw == exact {
				require.Nil(t, reply.Error)
				require.Equal(t, base58.Encode(hash[:]), reply.Result)
				return
			}
			require.NotNil(t, reply.Error)
			require.Equal(t, rpcCodeInvalidParams, reply.Error.Code)
		})
	}
}
