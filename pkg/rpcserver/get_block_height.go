package rpcserver

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-jsonrpc"
)

func (rpcServer *RpcServer) GetBlockHeight(ctx context.Context, p jsonrpc.RawParams) (uint64, error) {
	config, err := parseBankReadConfig(ctx, "getBlockHeight", p)
	if err != nil {
		return 0, err
	}
	var slot, height uint64
	if config.Commitment != nil && *config.Commitment == "processed" {
		bank, err := rpcServer.processedBankContext()
		if err != nil {
			return 0, err
		}
		slot, height = bank.Slot, bank.BlockHeight
	} else {
		bank, ok := rpcServer.getRootedBankState()
		if !ok {
			return 0, fmt.Errorf("node has no rooted bank available")
		}
		slot, height = bank.Slot, bank.BlockHeight
	}
	if slot > 0 && height == 0 {
		return 0, fmt.Errorf("node has no block height available for slot %d", slot)
	}
	if config.MinContextSlot != nil && slot < *config.MinContextSlot {
		return 0, &MinContextSlotNotReachedError{ContextSlot: slot}
	}
	return height, nil
}
