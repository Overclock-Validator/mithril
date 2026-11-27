package rpcserver

import (
	"context"
	"fmt"

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
	conf, err := scalarConfig(ctx, "getEpochInfo", p)
	if err != nil {
		return GetEpochInfoResp{}, err
	}
	if rpcServer.epochSchedule == nil {
		return GetEpochInfoResp{}, fmt.Errorf("node has no epoch schedule available")
	}
	// One view carries slot, block height and transaction count of the same
	// completed bank; replay updates the process globals at different points.
	view, err := rpcServer.currentView(ctx, conf)
	if err != nil {
		return GetEpochInfoResp{}, err
	}
	epoch, slotIndex := rpcServer.epochSchedule.GetEpochAndSlotIndex(view.Slot)
	return GetEpochInfoResp{
		AbsoluteSlot:     view.Slot,
		BlockHeight:      view.BlockHeight,
		Epoch:            epoch,
		SlotIndex:        slotIndex,
		SlotsInEpoch:     rpcServer.epochSchedule.SlotsInEpoch(epoch),
		TransactionCount: view.TransactionCount,
	}, nil
}
