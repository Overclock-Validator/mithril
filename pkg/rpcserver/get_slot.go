package rpcserver

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-jsonrpc"
)

// GetSlot returns the slot at the requested commitment, defaulting to finalized.
func (rpcServer *RpcServer) GetSlot(ctx context.Context, p jsonrpc.RawParams) (uint64, error) {
	config, err := parseBankReadConfig(ctx, "getSlot", p)
	if err != nil {
		return 0, err
	}
	var slot uint64
	if config.Commitment != nil && *config.Commitment == "processed" {
		rpcServer.slotCtxMu.RLock()
		defer rpcServer.slotCtxMu.RUnlock()
		if !rpcServer.healthReady || rpcServer.slotCtx == nil {
			return 0, fmt.Errorf("node has no processed bank available")
		}
		slot = rpcServer.slotCtx.Slot
	} else {
		// Match getEpochInfo's published rooted view for confirmed and finalized.
		rooted, ok := rpcServer.getRootedBankState()
		if !ok {
			return 0, fmt.Errorf("node has no rooted bank available")
		}
		slot = rooted.Slot
	}
	if config.MinContextSlot != nil && slot < *config.MinContextSlot {
		return 0, &MinContextSlotNotReachedError{ContextSlot: slot}
	}
	return slot, nil
}
