package rpcserver

import (
	"context"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/filecoin-project/go-jsonrpc"
)

type GetEpochInfoResp struct {
	AbsoluteSlot     uint64 `json:"absoluteSlot"`
	BlockHeight      uint64 `json:"blockHeight"`
	Epoch            uint64 `json:"epoch"`
	SlotIndex        uint64 `json:"slotIndex"`
	SlotsInEpoch     uint64 `json:"slotsInEpoch"`
	TransactionCount uint64 `json:"transactionCount"`
}

func (rpcServer *RpcServer) GetEpochInfo(ctx context.Context, p jsonrpc.RawParams) (GetEpochInfoResp, error) {
	params, err := decodeOptionalParams(p)
	if err != nil {
		return GetEpochInfoResp{}, &InvalidParamsError{Message: fmt.Sprintf("decoding params: %v", err)}
	}
	if len(params) > 1 {
		return GetEpochInfoResp{}, &InvalidParamsError{Message: "getEpochInfo accepts at most one config object"}
	}
	commitment := "finalized"
	var minContextSlot *uint64
	if len(params) == 1 && params[0] != nil {
		config, ok := params[0].(map[string]interface{})
		if !ok {
			return GetEpochInfoResp{}, &InvalidParamsError{Message: "invalid getEpochInfo config"}
		}
		if raw, exists := config["commitment"]; exists {
			value, ok := raw.(string)
			if !ok || (value != "processed" && value != "confirmed" && value != "finalized") {
				return GetEpochInfoResp{}, &InvalidParamsError{Message: "invalid commitment"}
			}
			commitment = value
		}
		if raw, exists := config["minContextSlot"]; exists {
			value, err := rpcUint64(raw, "minContextSlot")
			if err != nil {
				return GetEpochInfoResp{}, err
			}
			minContextSlot = &value
		}
	}
	if rpcServer.epochSchedule == nil {
		return GetEpochInfoResp{}, fmt.Errorf("node has no epoch schedule available")
	}
	slot := global.Slot()
	blockHeight := global.BlockHeight()
	transactionCount := global.TransactionCount()
	if commitment != "processed" {
		rooted, ok := rpcServer.getRootedBankState()
		if !ok {
			return GetEpochInfoResp{}, fmt.Errorf("node has no rooted bank available")
		}
		slot, blockHeight, transactionCount = rooted.Slot, rooted.BlockHeight, rooted.TransactionCount
	}
	if minContextSlot != nil && slot < *minContextSlot {
		return GetEpochInfoResp{}, &MinContextSlotNotReachedError{ContextSlot: slot}
	}
	epoch := rpcServer.epochSchedule.GetEpoch(slot)
	slotIndex := slot - rpcServer.epochSchedule.FirstSlotInEpoch(epoch)

	resp := GetEpochInfoResp{
		AbsoluteSlot:     slot,
		BlockHeight:      blockHeight,
		Epoch:            epoch,
		SlotIndex:        slotIndex,
		SlotsInEpoch:     432000,
		TransactionCount: transactionCount,
	}

	return resp, nil
}
