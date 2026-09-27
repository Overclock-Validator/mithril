package rpcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestGetSlotHTTP(t *testing.T) {
	oldSlot := global.Slot()
	t.Cleanup(func() { global.SetSlot(oldSlot) })
	global.SetSlot(150)
	server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 512}, solana.Hash{})
	t.Cleanup(func() { require.NoError(t, server.listener.Close()) })
	server.SetRootedBankState(100, 90, 0)
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	check := func(t *testing.T, params string, want uint64, code int) {
		t.Helper()
		body := `{"jsonrpc":"2.0","id":7,"method":"getSlot"`
		if params != "" {
			body += `,"params":` + params
		}
		resp, err := httpServer.Client().Post(httpServer.URL, "application/json", strings.NewReader(body+`}`))
		require.NoError(t, err)
		defer resp.Body.Close()
		var got struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      int             `json:"id"`
			Result  json.RawMessage `json:"result"`
			Error   *struct {
				Code int `json:"code"`
				Data struct {
					ContextSlot uint64 `json:"contextSlot"`
				} `json:"data"`
			} `json:"error"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
		require.Equal(t, "2.0", got.JSONRPC)
		require.Equal(t, 7, got.ID)
		if code != 0 {
			require.NotNil(t, got.Error)
			require.Equal(t, code, got.Error.Code)
			require.Empty(t, got.Result)
			if code == -32016 {
				require.Equal(t, want, got.Error.Data.ContextSlot)
			}
			return
		}
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Nil(t, got.Error)
		require.Equal(t, fmt.Sprint(want), string(got.Result))
	}

	for _, tc := range []struct {
		name   string
		params string
		want   uint64
		code   int
	}{
		{"omitted", "", 100, 0},
		{"empty", `[]`, 100, 0},
		{"null params", `null`, 100, 0},
		{"null config", `[null]`, 100, 0},
		{"empty config", `[{}]`, 100, 0},
		{"null fields", `[{"commitment":null,"minContextSlot":null}]`, 100, 0},
		{"finalized", `[{"commitment":"finalized"}]`, 100, 0},
		{"confirmed", `[{"commitment":"confirmed"}]`, 100, 0},
		{"processed", `[{"commitment":"processed"}]`, 150, 0},
		{"root boundary", `[{"minContextSlot":100}]`, 100, 0},
		{"root too old", `[{"minContextSlot":101}]`, 100, -32016},
		{"confirmed too old", `[{"commitment":"confirmed","minContextSlot":101}]`, 100, -32016},
		{"processed boundary", `[{"commitment":"processed","minContextSlot":150}]`, 150, 0},
		{"processed too old", `[{"commitment":"processed","minContextSlot":151}]`, 150, -32016},
		{"unknown field", `[{"futureOption":true}]`, 100, 0},
		{"extra argument", `[{},{}]`, 0, -32602},
		{"scalar config", `[true]`, 0, -32602},
		{"array config", `[[]]`, 0, -32602},
		{"unknown commitment", `[{"commitment":"invalid"}]`, 0, -32602},
		{"empty commitment", `[{"commitment":""}]`, 0, -32602},
		{"typed commitment", `[{"commitment":1}]`, 0, -32602},
		{"negative slot", `[{"minContextSlot":-1}]`, 0, -32602},
		{"fractional slot", `[{"minContextSlot":1.5}]`, 0, -32602},
		{"string slot", `[{"minContextSlot":"100"}]`, 0, -32602},
		{"overflow slot", `[{"minContextSlot":18446744073709551616}]`, 0, -32602},
	} {
		t.Run(tc.name, func(t *testing.T) { check(t, tc.params, tc.want, tc.code) })
	}

	t.Run("exact uint64", func(t *testing.T) {
		server.SetRootedBankState(1<<53, 0, 0)
		check(t, `[{"minContextSlot":9007199254740993}]`, 1<<53, -32016)
		server.SetRootedBankState(^uint64(0), 0, 0)
		check(t, `[{"minContextSlot":18446744073709551615}]`, ^uint64(0), 0)
	})
	t.Run("no rooted bank", func(t *testing.T) {
		server.rootedBank.Store(nil)
		check(t, `[]`, 0, 1)
		check(t, `[{"commitment":"processed"}]`, 150, 0)
	})
	t.Run("zero slot", func(t *testing.T) {
		server.SetRootedBankState(0, 0, 0)
		check(t, `[]`, 0, 0)
	})
	t.Run("canceled request", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := server.GetSlot(ctx, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}
