package rpcserver

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-jsonrpc"
)

// GetBlockHeight returns the block height of the bank at the requested
// commitment, defaulting to finalized.
func (rpcServer *RpcServer) GetBlockHeight(ctx context.Context, p jsonrpc.RawParams) (uint64, error) {
	conf, err := scalarConfig(ctx, "getBlockHeight", p)
	if err != nil {
		return 0, err
	}
	view, err := rpcServer.currentView(ctx, conf)
	if err != nil {
		return 0, err
	}
	if view.Slot > 0 && view.BlockHeight == 0 {
		return 0, fmt.Errorf("node has no block height available for slot %d", view.Slot)
	}
	return view.BlockHeight, nil
}
