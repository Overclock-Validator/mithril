package rpcserver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/filecoin-project/go-jsonrpc"
)

// GetSlot returns the slot at the requested commitment, defaulting to finalized.
func (rpcServer *RpcServer) GetSlot(ctx context.Context, p jsonrpc.RawParams) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var params []json.RawMessage
	if len(p) != 0 {
		if err := json.Unmarshal(p, &params); err != nil {
			return 0, &InvalidParamsError{Message: fmt.Sprintf("decoding params: %v", err)}
		}
	}
	if len(params) > 1 {
		return 0, &InvalidParamsError{Message: "getSlot accepts at most one config object"}
	}
	var config struct {
		Commitment     *string `json:"commitment"`
		MinContextSlot *uint64 `json:"minContextSlot"`
	}
	if len(params) == 1 {
		if err := json.Unmarshal(params[0], &config); err != nil {
			return 0, &InvalidParamsError{Message: fmt.Sprintf("invalid getSlot config: %v", err)}
		}
	}
	commitment := "finalized"
	if config.Commitment != nil {
		commitment = *config.Commitment
	}
	if commitment != "processed" && commitment != "confirmed" && commitment != "finalized" {
		return 0, &InvalidParamsError{Message: "invalid commitment"}
	}

	slot := global.Slot()
	if commitment != "processed" {
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
