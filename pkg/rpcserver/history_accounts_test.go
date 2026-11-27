package rpcserver

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/history"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestAccountReadsAtPastSlot(t *testing.T) {
	db := newRPCAccountsDB(t)
	a, b, quiet := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{
		{Key: a, Lamports: 10, Data: []byte{1}}, {Key: quiet, Lamports: 77, Data: []byte{7, 7}},
	}}}, 100, nil, nil)
	require.NoError(t, err)
	hs, err := history.Open(t.TempDir(), history.Options{SegmentChunks: 2})
	require.NoError(t, err)
	db.SetHistory(hs)
	// a changes at 101 and 105; b appears at 103; quiet never changes again.
	_, err = db.CommitBatch([]accounts.SlotDelta{
		{Slot: 101, Delta: []*accounts.Account{{Key: a, Lamports: 11, Data: []byte{1}}}},
		{Slot: 103, Delta: []*accounts.Account{{Key: b, Lamports: 5}}},
	}, 103, nil, nil)
	require.NoError(t, err)
	_, err = db.CommitBatch([]accounts.SlotDelta{
		{Slot: 105, Delta: []*accounts.Account{{Key: a, Lamports: 12, Data: []byte{1, 2}}}},
	}, 106, nil, nil)
	require.NoError(t, err)

	server := &RpcServer{acctsDb: db}
	server.SetRootedBankState(106, 90, 0)
	balanceAt := func(k solana.PublicKey, slot uint64) (uint64, error) {
		resp, err := server.GetBalance(t.Context(), mustRawParams(t, []any{k.String(), map[string]any{"slot": slot}}))
		if err == nil {
			require.Equal(t, slot, resp.Context.Slot)
		}
		return resp.Value, err
	}
	for _, tc := range []struct {
		key  solana.PublicKey
		slot uint64
		want uint64
	}{
		{a, 101, 11}, {a, 104, 11}, {a, 105, 12}, {a, 106, 12},
		{b, 102, 0}, {b, 103, 5}, {b, 106, 5},
		{quiet, 101, 77}, {quiet, 106, 77}, // unchanged: read from the durable store
	} {
		got, err := balanceAt(tc.key, tc.slot)
		require.NoError(t, err, "%s at %d", tc.key, tc.slot)
		require.Equal(t, tc.want, got, "%s at %d", tc.key, tc.slot)
	}
	_, err = balanceAt(a, 100)
	require.ErrorContains(t, err, "outside recorded account history")
	_, err = balanceAt(a, 107)
	require.ErrorContains(t, err, "outside recorded account history")

	info, err := server.GetAccountInfo(t.Context(), mustRawParams(t, []any{a.String(), map[string]any{"slot": 104, "encoding": "base64"}}))
	require.NoError(t, err)
	require.NotNil(t, info.Value)
	require.EqualValues(t, 11, info.Value.Lamports)
	require.EqualValues(t, 1, info.Value.Space)

	// getMultipleAccounts runs over HTTP (it takes a response permit).
	httpServer := NewRpcServer(db, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 512}, solana.Hash{})
	require.NoError(t, httpServer.listener.Close())
	httpServer.SetRootedBankState(106, 90, 0)
	endpoint := httptest.NewServer(httpServer)
	t.Cleanup(endpoint.Close)
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getMultipleAccounts","params":[["%s","%s","%s"],{"slot":102}]}`, a, b, quiet)
	response, err := endpoint.Client().Post(endpoint.URL, "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer response.Body.Close()
	var multi struct {
		Result *GetMultipleAccountsResp `json:"result"`
		Error  any                      `json:"error"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&multi))
	require.Nil(t, multi.Error)
	require.EqualValues(t, 102, multi.Result.Context.Slot)
	require.EqualValues(t, 11, multi.Result.Value[0].Lamports)
	require.Nil(t, multi.Result.Value[1])
	require.EqualValues(t, 77, multi.Result.Value[2].Lamports)

	// Without history the extension is refused.
	plain := &RpcServer{acctsDb: newRPCAccountsDB(t)}
	plain.SetRootedBankState(1, 1, 0)
	_, err = plain.GetBalance(t.Context(), mustRawParams(t, []any{a.String(), map[string]any{"slot": 1}}))
	require.ErrorContains(t, err, "account history is not enabled")
}
