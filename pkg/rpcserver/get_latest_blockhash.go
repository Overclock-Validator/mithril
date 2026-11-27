package rpcserver

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
)

type GetLatestBlockhashRespContext struct {
	Slot uint64 `json:"slot"`
}

type GetLatestBlockhashRespValue struct {
	Blockhash            string `json:"blockhash"`
	LastValidBlockHeight uint64 `json:"lastValidBlockHeight"`
}

type GetLatestBlockhashResp struct {
	Context GetLatestBlockhashRespContext `json:"context"`
	Value   *GetLatestBlockhashRespValue  `json:"value"`
}

// GetLatestBlockhash reports the selected bank's blockhash, height and slot
// together, so the expiry height always belongs to the returned blockhash.
func (rpcServer *RpcServer) GetLatestBlockhash(ctx context.Context, p jsonrpc.RawParams) (GetLatestBlockhashResp, error) {
	conf, err := scalarConfig(ctx, "getLatestBlockhash", p)
	if err != nil {
		return GetLatestBlockhashResp{}, err
	}
	view, err := rpcServer.currentView(ctx, conf)
	if err != nil {
		return GetLatestBlockhashResp{}, err
	}
	if view.Blockhash == (solana.Hash{}) || (view.Slot > 0 && view.BlockHeight == 0) {
		return GetLatestBlockhashResp{}, fmt.Errorf("node has no blockhash context available for slot %d", view.Slot)
	}
	if view.BlockHeight > ^uint64(0)-maxProcessingAge {
		return GetLatestBlockhashResp{}, fmt.Errorf("blockhash expiry height overflows at slot %d", view.Slot)
	}
	val := &GetLatestBlockhashRespValue{
		Blockhash:            base58.Encode(view.Blockhash[:]),
		LastValidBlockHeight: view.BlockHeight + maxProcessingAge,
	}
	return GetLatestBlockhashResp{Context: GetLatestBlockhashRespContext{Slot: view.Slot}, Value: val}, nil
}
