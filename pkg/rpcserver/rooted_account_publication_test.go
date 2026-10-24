package rpcserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestGetBalanceHTTPWaitsForFailedRewindRepair(t *testing.T) {
	db := newRPCAccountsDB(t)
	key := solana.PublicKey{204}
	for _, slot := range []uint64{10, 20} {
		_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{{Key: key, Lamports: slot}}}}, slot, nil, nil)
		require.NoError(t, err)
	}
	server := NewRpcServer(db, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}, solana.Hash{1})
	t.Cleanup(func() { _ = server.listener.Close() })
	server.SetRootedBankState(20, 20, 0)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
		defer cancel()
		server.ServeHTTP(w, r.WithContext(ctx))
	}))
	t.Cleanup(endpoint.Close)
	endpoint.Client().Timeout = time.Second
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "getBalance", "params": []any{key.String()}})
	require.NoError(t, err)
	call := func() (*GetBalanceResp, *rpcProbeError) {
		t.Helper()
		response, err := endpoint.Client().Post(endpoint.URL, "application/json", bytes.NewReader(request))
		require.NoError(t, err)
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		t.Logf("HTTP status=%d body=%s", response.StatusCode, body)
		var reply struct {
			Result *GetBalanceResp `json:"result"`
			Error  *rpcProbeError  `json:"error"`
		}
		require.NoError(t, json.Unmarshal(body, &reply))
		return reply.Result, reply.Error
	}
	rewoundPath := filepath.Join(db.AcctsDir, "rewound")
	require.NoError(t, os.WriteFile(rewoundPath, nil, 0o644))
	_, err = db.RewindToBatchBoundary(10)
	require.Error(t, err)
	result, rpcError := call()
	require.Nil(t, result, "partial rewind must not publish an account under the previous bank")
	require.NotNil(t, rpcError)
	require.Contains(t, rpcError.Message, context.DeadlineExceeded.Error())

	require.NoError(t, os.Remove(rewoundPath))
	_, err = db.RewindToBatchBoundary(10)
	require.NoError(t, err)
	server.SetRootedBankState(10, 10, 0)
	result, rpcError = call()
	require.Nil(t, rpcError)
	require.NotNil(t, result)
	require.Equal(t, uint64(10), result.Context.Slot)
	require.Equal(t, uint64(10), result.Value)
}

func TestRootedAccountRPCsWaitForFailedRewindRepair(t *testing.T) {
	key := solana.PublicKey{203}
	for _, test := range []struct {
		name string
		read func(context.Context, *RpcServer) error
	}{
		{"balance", func(ctx context.Context, server *RpcServer) error {
			_, err := server.GetBalance(ctx, mustRawParams(t, []any{key.String()}))
			return err
		}},
		{"account info", func(ctx context.Context, server *RpcServer) error {
			_, err := server.GetAccountInfo(ctx, mustRawParams(t, []any{key.String(), map[string]any{"encoding": "base64"}}))
			return err
		}},
		{"block production", func(ctx context.Context, server *RpcServer) error {
			_, err := server.GetBlockProduction(ctx, nil)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := newRPCAccountsDB(t)
			for _, slot := range []uint64{10, 20} {
				_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{{Key: key, Lamports: slot}}}}, slot, nil, nil)
				require.NoError(t, err)
			}
			server := &RpcServer{acctsDb: db, epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}}
			server.SetRootedBankState(20, 20, 0)
			// Fail forensic-directory creation after rewind's atomic index undo.
			rewoundPath := filepath.Join(db.AcctsDir, "rewound")
			require.NoError(t, os.WriteFile(rewoundPath, nil, 0o644))
			_, err := db.RewindToBatchBoundary(10)
			require.Error(t, err)
			_, stable := db.CommittedAccountVersion()
			require.False(t, stable)
			require.Equal(t, uint64(20), db.DurableThrough())
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			require.ErrorIs(t, test.read(ctx, server), context.DeadlineExceeded,
				"partial rewind must not publish account values under the previous bank")

			require.NoError(t, os.Remove(rewoundPath))
			_, err = db.RewindToBatchBoundary(10)
			require.NoError(t, err)
			server.SetRootedBankState(10, 10, 0)
			rooted, account, err := server.readRootedAccount(t.Context(), key)
			require.NoError(t, err)
			require.Equal(t, uint64(10), rooted.Slot)
			require.Equal(t, uint64(10), account.Lamports)
		})
	}
}
