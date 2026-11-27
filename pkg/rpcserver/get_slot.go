package rpcserver

import (
	"context"

	"github.com/filecoin-project/go-jsonrpc"
)

// GetSlot returns the slot at the requested commitment, defaulting to finalized.
func (rpcServer *RpcServer) GetSlot(ctx context.Context, p jsonrpc.RawParams) (uint64, error) {
	conf, err := scalarConfig(ctx, "getSlot", p)
	if err != nil {
		return 0, err
	}
	view, err := rpcServer.currentView(ctx, conf)
	if err != nil {
		return 0, err
	}
	return view.Slot, nil
}
