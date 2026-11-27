package rpcserver

import (
	"context"
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/stretchr/testify/require"
)

func TestBlockHeightBankContract(t *testing.T) {
	oldSlot, oldHeight := global.Slot(), global.BlockHeight()
	t.Cleanup(func() {
		global.SetSlot(oldSlot)
		global.SetBlockHeight(oldHeight)
		replay.ResetChainTip()
	})
	global.SetSlot(900)
	global.SetBlockHeight(800)
	server := &RpcServer{}
	server.SetRootedBankState(100, 70, 0)
	publish := func(slot, height uint64) {
		sysvars, err := sealevel.NewBankSysvars(slot)
		require.NoError(t, err)
		bank := &sealevel.SlotCtx{Slot: slot, Blockhash: [32]byte{2}}
		require.NoError(t, bank.PublishBankSysvars(sysvars))
		replay.UpdateChainTipFromSlotCtxWithBankMetadata(bank, nil, nil, replay.ChainTipIdentity{}, replay.ChainTipBankMetadata{BlockHeight: height})
		server.SetSlotCtx(bank)
	}
	publish(150, 90)
	for _, tc := range []struct {
		name, params     string
		want             uint64
		invalid, minimum bool
	}{
		{name: "omitted", want: 70},
		{name: "empty", params: `[]`, want: 70},
		{name: "null params", params: `null`, want: 70},
		{name: "null config", params: `[null]`, want: 70},
		{name: "null fields", params: `[{"commitment":null,"minContextSlot":null}]`, want: 70},
		{name: "confirmed", params: `[{"commitment":"confirmed"}]`, want: 70},
		{name: "finalized boundary", params: `[{"commitment":"finalized","minContextSlot":100}]`, want: 70},
		{name: "processed", params: `[{"commitment":"processed","minContextSlot":150}]`, want: 90},
		{name: "root too old", params: `[{"minContextSlot":101}]`, want: 100, minimum: true},
		{name: "processed too old", params: `[{"commitment":"processed","minContextSlot":151}]`, want: 150, minimum: true},
		{name: "extra config", params: `[{},{}]`, invalid: true},
		{name: "scalar", params: `[true]`, invalid: true},
		{name: "array", params: `[[]]`, invalid: true},
		{name: "unknown commitment", params: `[{"commitment":"invalid"}]`, invalid: true},
		{name: "fractional minimum", params: `[{"minContextSlot":1.5}]`, invalid: true},
		{name: "negative minimum", params: `[{"minContextSlot":-1}]`, invalid: true},
		{name: "overflow minimum", params: `[{"minContextSlot":18446744073709551616}]`, invalid: true},
		{name: "trailing JSON", params: `[] []`, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			height, err := server.GetBlockHeight(t.Context(), jsonrpc.RawParams(tc.params))
			if tc.invalid {
				var invalid *InvalidParamsError
				require.True(t, errors.As(err, &invalid), "error: %v", err)
			} else if tc.minimum {
				var minimum *MinContextSlotNotReachedError
				require.True(t, errors.As(err, &minimum), "error: %v", err)
				require.Equal(t, tc.want, minimum.ContextSlot)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, height)
			}
		})
	}
	t.Run("exact uint64", func(t *testing.T) {
		server.SetRootedBankState(1<<53, 70, 0)
		defer server.SetRootedBankState(100, 70, 0)
		_, err := server.GetBlockHeight(t.Context(), jsonrpc.RawParams(`[{"minContextSlot":9007199254740993}]`))
		var minimum *MinContextSlotNotReachedError
		require.True(t, errors.As(err, &minimum))
		require.Equal(t, uint64(1<<53), minimum.ContextSlot)
	})
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := server.GetBlockHeight(ctx, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("no root", func(t *testing.T) {
		server.rootedBank.Store(nil)
		defer server.SetRootedBankState(100, 70, 0)
		_, err := server.GetBlockHeight(t.Context(), jsonrpc.RawParams(`[]`))
		require.Error(t, err)
	})
	t.Run("zero bank", func(t *testing.T) {
		server.SetRootedBankState(0, 0, 0)
		defer server.SetRootedBankState(100, 70, 0)
		got, err := server.GetBlockHeight(t.Context(), nil)
		require.NoError(t, err)
		require.Zero(t, got)
	})
	t.Run("missing root height", func(t *testing.T) {
		server.SetRootedBankState(100, 0, 0)
		defer server.SetRootedBankState(100, 70, 0)
		_, err := server.GetBlockHeight(t.Context(), nil)
		require.Error(t, err)
	})
	t.Run("missing processed height", func(t *testing.T) {
		publish(150, 0)
		defer publish(150, 90)
		_, err := server.GetBlockHeight(t.Context(), jsonrpc.RawParams(`[{"commitment":"processed"}]`))
		require.Error(t, err)
	})
	t.Run("reset and replacement generation", func(t *testing.T) {
		params := jsonrpc.RawParams(`[{"commitment":"processed"}]`)
		replay.ResetChainTip()
		_, err := server.GetBlockHeight(t.Context(), params)
		require.Error(t, err)
		publish(140, 85)
		got, err := server.GetBlockHeight(t.Context(), params)
		require.NoError(t, err)
		require.Equal(t, uint64(85), got)
		server.InvalidateHealth()
		_, err = server.GetBlockHeight(t.Context(), params)
		require.Error(t, err)
	})
}
