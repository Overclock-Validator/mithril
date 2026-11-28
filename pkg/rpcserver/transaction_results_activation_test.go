package rpcserver

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func activationCache(t *testing.T) *txstatus.ResultCache {
	t.Helper()
	cache, err := txstatus.NewResultCache(txstatus.ResultBankID{Slot: 20, Blockhash: solana.Hash{20}})
	require.NoError(t, err)
	return cache
}

func TestSignatureStatusesInitBeforeStart(t *testing.T) {
	server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}, solana.Hash{1})
	require.NoError(t, server.InitTransactionResults(nil))
	require.False(t, server.requiresHTTPOnly())
	cache := activationCache(t)
	require.NoError(t, server.InitTransactionResults(cache))
	require.NoError(t, server.InitTransactionResults(cache))
	require.True(t, server.requiresHTTPOnly(), "status-only profile must use owned HTTP lifecycle")
	require.Error(t, server.InitTransactionResults(activationCache(t)))
	require.Error(t, server.InitTransactionResults(nil))
	require.Same(t, cache, server.resultCache)
	require.Nil(t, server.transactionHistory, "status-only initialization must not open a history store")
	require.NoError(t, server.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		joined, err := server.Shutdown(ctx)
		require.True(t, joined)
		require.NoError(t, err)
	})
	require.Error(t, server.InitTransactionResults(cache))
	_, port, err := net.SplitHostPort(server.listener.Addr().String())
	require.NoError(t, err)
	endpoint := "http://" + net.JoinHostPort("127.0.0.1", port)
	for _, name := range []string{"InitTransactionResults", "initTransactionResults", "Mithril.InitTransactionResults"} {
		body, err := historyHTTPPost(endpoint, `{"jsonrpc":"2.0","id":1,"method":"`+name+`","params":[]}`)
		require.NoError(t, err)
		var response struct {
			Error struct{ Code int } `json:"error"`
		}
		require.NoError(t, json.Unmarshal(body, &response))
		require.Equal(t, -32601, response.Error.Code, "initializer must not enter reflected RPC registration")
	}
	joined, err := server.Shutdown(t.Context())
	require.True(t, joined)
	require.NoError(t, err)
	require.Error(t, server.InitTransactionResults(cache))
}
