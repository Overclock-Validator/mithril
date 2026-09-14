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
	params, err := jsonrpc.DecodeParams[[]interface{}](p)
	if err != nil {
		return GetEpochInfoResp{}, fmt.Errorf("decoding params: %w", err)
	}

	_ = params

	slot := global.Slot()
	epoch, slotIndex := rpcServer.epochSchedule.GetEpochAndSlotIndex(slot)

	resp := GetEpochInfoResp{
		AbsoluteSlot:     slot,
		BlockHeight:      global.BlockHeight(),
		Epoch:            epoch,
		SlotIndex:        slotIndex,
		SlotsInEpoch:     rpcServer.epochSchedule.SlotsInEpoch(epoch),
		TransactionCount: global.TransactionCount(),
	}

	return resp, nil
}
