package rpcserver

import (
	"context"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/filecoin-project/go-jsonrpc"
)

// GetSlot returns the slot at the requested commitment, defaulting to finalized.
func (rpcServer *RpcServer) GetSlot(ctx context.Context, p jsonrpc.RawParams) (uint64, error) {
	config, err := parseBankReadConfig(ctx, "getSlot", p)
	if err != nil {
		return 0, err
	}
	slot := global.Slot()
	if config.Commitment == nil || *config.Commitment != "processed" {
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
