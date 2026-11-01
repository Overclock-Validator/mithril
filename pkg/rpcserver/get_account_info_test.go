package rpcserver

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestGetAccountInfoParameterHTTP(t *testing.T) {
	db := newRPCAccountsDB(t)
	key := solana.PublicKey{87}
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 10, Delta: []*accounts.Account{{Key: key, Lamports: 42, Data: []byte("abcd")}}}}, 10, nil, nil)
	require.NoError(t, err)
	server := NewRpcServer(db, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000}, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	server.SetRootedBankState(10, 10, 0)
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)
	for _, test := range []struct {
		config string
		code   int
		data   string
	}{
		{`{"encoding":"base64","minContextSlot":10}`, 0, "YWJjZA=="},
		{`{"encoding":"base64","minContextSlot":10,"dataSlice":{"offset":1,"length":2}}`, 0, "YmM="},
		{`{"encoding":"base64","dataSlice":{"offset":18446744073709551615,"length":1}}`, 0, ""},
		{`{"encoding":"base64","commitment":null,"minContextSlot":null,"dataSlice":null}`, 0, "YWJjZA=="},
		{`{"minContextSlot":9007199254740993}`, -32016, ""},
		{`{"minContextSlot":1.0000000000000001}`, -32602, ""},
		{`{"dataSlice":{"offset":-1,"length":1}}`, -32602, ""},
		{`{"dataSlice":{"offset":0,"length":1.5}}`, -32602, ""},
		{`{"dataSlice":{}}`, -32602, ""},
		{`{"dataSlice":{"offset":0,"length":null}}`, -32602, ""},
	} {
		t.Run(test.config, func(t *testing.T) {
			body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":[%q,%s]}`, key.String(), test.config)
			response, err := endpoint.Client().Post(endpoint.URL, "application/json", strings.NewReader(body))
			require.NoError(t, err)
			defer response.Body.Close()
			var got struct {
				Result GetAccountInfoResp `json:"result"`
				Error  *struct {
					Code int `json:"code"`
				} `json:"error"`
			}
			require.NoError(t, json.NewDecoder(response.Body).Decode(&got))
			if test.code != 0 {
				require.NotNil(t, got.Error)
				require.Equal(t, test.code, got.Error.Code)
				return
			}
			require.Nil(t, got.Error)
			require.Equal(t, uint64(10), got.Result.Context.Slot)
			require.NotNil(t, got.Result.Value)
			require.Equal(t, []interface{}{test.data, "base64"}, got.Result.Value.Data)
		})
	}
}

func TestGetAccountInfoAccountExistence(t *testing.T) {
	for _, state := range []string{"never-created", "funded-empty-data", "deleted"} {
		t.Run(state, func(t *testing.T) {
			db := newRPCAccountsDB(t)
			server := &RpcServer{acctsDb: db}
			address := solana.PublicKey{87}
			slot := uint64(10)
			lamports := uint64(0)
			server.SetRootedBankState(slot, slot, 0)
			if state != "never-created" {
				lamports = 42
				_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{{Key: address, Lamports: lamports}}}}, slot, nil, nil)
				require.NoError(t, err)
				// Prime the account-read cache before deleting, and confirm that a live
				// account with empty data is not mistaken for a missing placeholder.
				funded, err := server.GetAccountInfo(t.Context(), mustRawParams(t, []interface{}{address.String()}))
				require.NoError(t, err)
				require.NotNil(t, funded.Value)
				require.Equal(t, lamports, funded.Value.Lamports)
			}
			if state == "deleted" {
				slot++
				lamports = 0
				// A zero-lamport tombstone is absent even if it retains payload bytes.
				_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{{Key: address, Data: []byte{1, 2, 3}}}}}, slot, nil, nil)
				require.NoError(t, err)
				server.SetRootedBankState(slot, slot, 0)
			}
			for _, encoding := range []string{"", "base58", "base64", "base64+zstd", "jsonParsed"} {
				params := []interface{}{address.String()}
				if encoding != "" {
					params = append(params, map[string]interface{}{"encoding": encoding})
				}
				got, err := server.GetAccountInfo(t.Context(), mustRawParams(t, params))
				require.NoError(t, err)
				require.Equal(t, slot, got.Context.Slot)
				if lamports == 0 {
					require.Nil(t, got.Value, "state=%s encoding=%s", state, encoding)
					wire, err := json.Marshal(got)
					require.NoError(t, err)
					var decoded map[string]json.RawMessage
					require.NoError(t, json.Unmarshal(wire, &decoded))
					require.Equal(t, "null", string(decoded["value"]), "value must be present as JSON null")
				} else {
					require.NotNil(t, got.Value)
					require.Equal(t, lamports, got.Value.Lamports)
				}
			}
			balance, err := server.GetBalance(t.Context(), mustRawParams(t, []interface{}{address.String()}))
			require.NoError(t, err)
			require.Equal(t, slot, balance.Context.Slot)
			require.Equal(t, lamports, balance.Value)

			params := mustRawParams(t, []interface{}{address.String(), map[string]interface{}{"minContextSlot": slot + 1}})
			_, err = server.GetAccountInfo(t.Context(), params)
			var minSlotErr *MinContextSlotNotReachedError
			require.ErrorAs(t, err, &minSlotErr, "missing accounts must still enforce minContextSlot")
			require.Equal(t, slot, minSlotErr.ContextSlot)
			_, err = server.GetBalance(t.Context(), params)
			require.ErrorAs(t, err, &minSlotErr)
		})
	}
}
