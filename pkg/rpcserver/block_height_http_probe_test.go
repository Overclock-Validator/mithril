package rpcserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestBlockHeightBankHTTP(t *testing.T) {
	oldSlot, oldHeight := global.Slot(), global.BlockHeight()
	t.Cleanup(func() {
		global.SetSlot(oldSlot)
		global.SetBlockHeight(oldHeight)
		replay.ResetChainTip()
	})
	global.SetSlot(900)
	global.SetBlockHeight(800)
	server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 512}, solana.Hash{})
	t.Cleanup(func() { require.NoError(t, server.listener.Close()) })
	server.SetRootedBankState(100, 70, 0)
	sysvars, err := sealevel.NewBankSysvars(150)
	require.NoError(t, err)
	bank := &sealevel.SlotCtx{Slot: 150, Blockhash: [32]byte{2}}
	require.NoError(t, bank.PublishBankSysvars(sysvars))
	replay.UpdateChainTipFromSlotCtxWithBankMetadata(bank, nil, nil, replay.ChainTipIdentity{}, replay.ChainTipBankMetadata{BlockHeight: 90})
	server.SetSlotCtx(bank)
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	for _, tc := range []struct {
		name, params string
		want, root   uint64
		code         int
		missing      string
	}{
		{name: "omitted", want: 70},
		{name: "null", params: `[null]`, want: 70},
		{name: "finalized", params: `[{"commitment":"finalized"}]`, want: 70},
		{name: "confirmed", params: `[{"commitment":"confirmed"}]`, want: 70},
		{name: "processed", params: `[{"commitment":"processed"}]`, want: 90},
		{name: "minimum boundary", params: `[{"minContextSlot":100}]`, want: 70},
		{name: "root minimum", params: `[{"minContextSlot":101}]`, want: 100, code: -32016},
		{name: "processed minimum", params: `[{"commitment":"processed","minContextSlot":151}]`, want: 150, code: -32016},
		{name: "bad commitment", params: `[{"commitment":"unknown"}]`, code: -32602},
		{name: "bad minimum", params: `[{"minContextSlot":1.5}]`, code: -32602},
		{name: "extra config", params: `[{},{}]`, code: -32602},
		{name: "exact uint64", params: `[{"minContextSlot":9007199254740993}]`, root: 1 << 53, want: 1 << 53, code: -32016},
		{name: "missing root height", code: 1, missing: "root"},
		{name: "missing processed height", params: `[{"commitment":"processed"}]`, code: 1, missing: "processed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server.SetRootedBankState(100, 70, 0)
			if tc.root != 0 {
				server.SetRootedBankState(tc.root, 70, 0)
			}
			if tc.missing == "root" {
				server.SetRootedBankState(100, 0, 0)
			} else if tc.missing == "processed" {
				replay.UpdateChainTipFromSlotCtxWithBankMetadata(bank, nil, nil, replay.ChainTipIdentity{}, replay.ChainTipBankMetadata{})
			}
			body := `{"jsonrpc":"2.0","id":1,"method":"getBlockHeight"`
			if tc.params != "" {
				body += `,"params":` + tc.params
			}
			response, err := httpServer.Client().Post(httpServer.URL, "application/json", strings.NewReader(body+`}`))
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, http.StatusOK, response.StatusCode)
			var got struct {
				JSONRPC string  `json:"jsonrpc"`
				ID      int     `json:"id"`
				Result  *uint64 `json:"result"`
				Error   *struct {
					Code int `json:"code"`
					Data struct {
						ContextSlot uint64 `json:"contextSlot"`
					} `json:"data"`
				} `json:"error"`
			}
			require.NoError(t, json.NewDecoder(response.Body).Decode(&got))
			require.Equal(t, "2.0", got.JSONRPC)
			require.Equal(t, 1, got.ID)
			if tc.code != 0 {
				require.NotNil(t, got.Error)
				require.Equal(t, tc.code, got.Error.Code)
				require.Nil(t, got.Result)
				if tc.code == -32016 {
					require.Equal(t, tc.want, got.Error.Data.ContextSlot)
				}
			} else {
				require.Nil(t, got.Error)
				require.NotNil(t, got.Result)
				require.Equal(t, tc.want, *got.Result)
			}
		})
	}
}
