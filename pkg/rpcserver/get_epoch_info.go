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

	// Report one completed bank: the separate globals change at different
	// points of a block. Before the first completed bank, fall back to them.
	bank, ok := global.LastCompletedBank()
	if !ok {
		bank = global.CompletedBank{Slot: global.Slot(), BlockHeight: global.BlockHeight(), TransactionCount: global.TransactionCount()}
	}
	epoch, slotIndex := rpcServer.epochSchedule.GetEpochAndSlotIndex(bank.Slot)

	resp := GetEpochInfoResp{
		AbsoluteSlot:     bank.Slot,
		BlockHeight:      bank.BlockHeight,
		Epoch:            epoch,
		SlotIndex:        slotIndex,
		SlotsInEpoch:     rpcServer.epochSchedule.SlotsInEpoch(epoch),
		TransactionCount: bank.TransactionCount,
	}

	return resp, nil
}
