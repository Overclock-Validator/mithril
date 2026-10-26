package rpcserver

import (
	"context"
	"encoding/json"
	"math"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type blockhashReadContext struct {
	context.Context
	checks int
	action func()
}

func (ctx *blockhashReadContext) Err() error {
	ctx.checks++
	if ctx.checks == 2 {
		ctx.action()
	}
	return ctx.Context.Err()
}

func TestLatestBlockhashBankHTTP(t *testing.T) {
	oldSlot, oldHeight, oldHash := global.Slot(), global.BlockHeight(), global.LatestBlockHash()
	t.Cleanup(func() {
		global.SetSlot(oldSlot)
		global.SetBlockHeight(oldHeight)
		global.SetLatestBlockHash(oldHash)
		replay.ResetChainTip()
	})
	global.SetSlot(900)
	global.SetBlockHeight(800)
	global.SetLatestBlockHash([32]byte{9})
	rootHash := solana.Hash{1}
	recent := sealevel.SysvarRecentBlockhashes{{Blockhash: [32]byte(rootHash)}}
	acct := &accounts.Account{Key: solana.PublicKey(sealevel.SysvarRecentBlockHashesAddr), Lamports: 1, Data: recent.MustMarshal()}
	db := newRPCAccountsDB(t)
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{acct}}}, 100, nil, nil)
	require.NoError(t, err)
	server := NewRpcServer(db, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 512}, solana.Hash{})
	require.NoError(t, server.listener.Close())
	server.SetRootedBankState(100, 70, 0)
	sysvars, err := sealevel.NewBankSysvars(150)
	require.NoError(t, err)
	bank := &sealevel.SlotCtx{Slot: 150, Blockhash: [32]byte{2}}
	require.NoError(t, bank.PublishBankSysvars(sysvars))
	replay.UpdateChainTipFromSlotCtxWithBankMetadata(bank, nil, nil, replay.ChainTipIdentity{}, replay.ChainTipBankMetadata{BlockHeight: 90})
	server.SetSlotCtx(bank)
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)

	for _, tc := range []struct {
		name, params string
		slot, expiry uint64
		hash         solana.Hash
		code         int
	}{
		{name: "default rooted generation", slot: 100, expiry: 220, hash: rootHash},
		{name: "confirmed", params: `[{"commitment":"confirmed"}]`, slot: 100, expiry: 220, hash: rootHash},
		{name: "finalized", params: `[{"commitment":"finalized"}]`, slot: 100, expiry: 220, hash: rootHash},
		{name: "processed", params: `[{"commitment":"processed"}]`, slot: 150, expiry: 240, hash: solana.Hash{2}},
		{name: "root minimum", params: `[{"minContextSlot":101}]`, slot: 100, code: -32016},
		{name: "processed minimum", params: `[{"commitment":"processed","minContextSlot":151}]`, slot: 150, code: -32016},
		{name: "invalid commitment", params: `[{"commitment":"unknown"}]`, code: -32602},
		{name: "fractional minimum", params: `[{"minContextSlot":1.5}]`, code: -32602},
		{name: "extra config", params: `[{},{}]`, code: -32602},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash"`
			if tc.params != "" {
				body += `,"params":` + tc.params
			}
			response, err := endpoint.Client().Post(endpoint.URL, "application/json", strings.NewReader(body+`}`))
			require.NoError(t, err)
			defer response.Body.Close()
			var got struct {
				Result *GetLatestBlockhashResp `json:"result"`
				Error  *struct {
					Code int `json:"code"`
					Data struct {
						ContextSlot uint64 `json:"contextSlot"`
					} `json:"data"`
				} `json:"error"`
			}
			require.NoError(t, json.NewDecoder(response.Body).Decode(&got))
			if tc.code != 0 {
				require.NotNil(t, got.Error)
				require.Equal(t, tc.code, got.Error.Code)
				require.Nil(t, got.Result)
				if tc.code == -32016 {
					require.Equal(t, tc.slot, got.Error.Data.ContextSlot)
				}
				return
			}
			require.Nil(t, got.Error)
			require.NotNil(t, got.Result)
			require.Equal(t, tc.slot, got.Result.Context.Slot)
			require.NotNil(t, got.Result.Value)
			require.Equal(t, tc.hash.String(), got.Result.Value.Blockhash)
			require.Equal(t, tc.expiry, got.Result.Value.LastValidBlockHeight)
		})
	}
}

func TestLatestBlockhashUnavailableBankData(t *testing.T) {
	recent := sealevel.SysvarRecentBlockhashes{{Blockhash: [32]byte{1}}}
	zeroHash := sealevel.SysvarRecentBlockhashes{{}}
	for _, tc := range []struct {
		name         string
		data         []byte
		slot, height uint64
		params       jsonrpc.RawParams
		valid        bool
	}{
		{name: "missing sysvar", slot: 100, height: 70},
		{name: "empty queue", data: make([]byte, 8), slot: 100, height: 70},
		{name: "malformed queue", data: []byte{1}, slot: 100, height: 70},
		{name: "zero hash", data: zeroHash.MustMarshal(), slot: 100, height: 70},
		{name: "missing height", data: recent.MustMarshal(), slot: 100},
		{name: "expiry overflow", data: recent.MustMarshal(), slot: 100, height: math.MaxUint64 - 149},
		{name: "expiry boundary", data: recent.MustMarshal(), slot: 100, height: math.MaxUint64 - 150, valid: true},
		{name: "genesis height", data: recent.MustMarshal(), valid: true},
		{name: "unavailable processed bank", params: jsonrpc.RawParams(`[{"commitment":"processed"}]`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newRPCAccountsDB(t)
			if tc.data != nil {
				acct := &accounts.Account{Key: solana.PublicKey(sealevel.SysvarRecentBlockHashesAddr), Lamports: 1, Data: tc.data}
				_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: tc.slot, Delta: []*accounts.Account{acct}}}, tc.slot, nil, nil)
				require.NoError(t, err)
			}
			server := &RpcServer{acctsDb: db}
			server.SetRootedBankState(tc.slot, tc.height, 0)
			got, err := server.GetLatestBlockhash(t.Context(), tc.params)
			if !tc.valid {
				require.Error(t, err)
				require.Nil(t, got.Value)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.height+150, got.Value.LastValidBlockHeight)
		})
	}
	t.Run("missing root", func(t *testing.T) {
		_, err := (&RpcServer{}).GetLatestBlockhash(t.Context(), nil)
		require.Error(t, err)
	})
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := (&RpcServer{}).GetLatestBlockhash(ctx, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestLatestBlockhashReselectsPublishedAccountBank(t *testing.T) {
	for _, publish := range []bool{true, false} {
		t.Run(map[bool]string{true: "published", false: "unpublished"}[publish], func(t *testing.T) {
			db := newRPCAccountsDB(t)
			commit := func(slot uint64, hash byte) {
				recent := sealevel.SysvarRecentBlockhashes{{Blockhash: [32]byte{hash}}}
				acct := &accounts.Account{Key: solana.PublicKey(sealevel.SysvarRecentBlockHashesAddr), Lamports: 1, Data: recent.MustMarshal()}
				_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{acct}}}, slot, nil, nil)
				require.NoError(t, err)
			}
			commit(10, 1)
			server := &RpcServer{acctsDb: db}
			server.SetRootedBankState(10, 7, 0)
			timeout := 100 * time.Millisecond
			if publish {
				timeout = 2 * time.Second
			}
			deadline, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()
			ctx := &blockhashReadContext{Context: deadline, action: func() {
				commit(20, 2)
				if publish {
					server.SetRootedBankState(20, 8, 0)
				}
			}}
			got, err := server.GetLatestBlockhash(ctx, nil)
			require.GreaterOrEqual(t, ctx.checks, 2, "publication change must occur during the request")
			if !publish {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Nil(t, got.Value)
				return
			}
			require.NoError(t, err)
			require.Equal(t, uint64(20), got.Context.Slot)
			require.Equal(t, solana.Hash{2}.String(), got.Value.Blockhash)
			require.Equal(t, uint64(158), got.Value.LastValidBlockHeight)
		})
	}
}
