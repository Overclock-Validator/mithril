package rpcserver

import (
	"context"
	"encoding/json"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/blockhistory"
	"github.com/filecoin-project/go-jsonrpc"
)

type blocksConfig struct {
	Commitment     *string `json:"commitment"`
	MinContextSlot *uint64 `json:"minContextSlot"`
}

// GetBlocks enumerates retained rooted summaries. Missing coverage is an error,
// not evidence of a skipped slot; transaction payloads may have a shorter window.
// The shared HTTP query permit covers enumeration, serialization and the write.
func (server *RpcServer) GetBlocks(ctx context.Context, p jsonrpc.RawParams) ([]uint64, error) {
	start, end, config, err := parseBlocksParams(p)
	if err != nil {
		return nil, err
	}
	if err := acquireQueryResponse(ctx); err != nil {
		return nil, err
	}
	server.historyRecoveryMu.RLock()
	defer server.historyRecoveryMu.RUnlock()
	if server.historyRecoveryPending || server.blockHistory == nil || server.acctsDb == nil {
		return nil, &BlockNotAvailableError{Slot: start}
	}
	bank := server.rootedBank.Load()
	version, stable := server.acctsDb.CommittedAccountVersion()
	if bank == nil || !stable || server.rootedPublicationPending(bank.Slot) || server.blockHistory.Rooted() != bank.Slot {
		return nil, &BlockNotAvailableError{Slot: start}
	}
	if config.MinContextSlot != nil && bank.Slot < *config.MinContextSlot {
		return nil, &MinContextSlotNotReachedError{ContextSlot: bank.Slot}
	}
	last := uint64(math.MaxUint64)
	if start <= math.MaxUint64-blockhistory.MaxBlocksRange {
		last = start + blockhistory.MaxBlocksRange
	}
	if end != nil {
		last = *end
	}
	last = min(last, bank.Slot)
	if last >= start && last-start > blockhistory.MaxBlocksRange {
		return nil, &InvalidParamsError{Message: "slot range too large; max 500000"}
	}
	slots, unavailable, err := server.blockHistory.Blocks(ctx, start, last, bank.Slot)
	latest, stable := server.acctsDb.CommittedAccountVersion()
	if !stable || latest != version || server.rootedBank.Load() != bank ||
		server.rootedPublicationPending(bank.Slot) || server.blockHistory.Rooted() != bank.Slot {
		return nil, &BlockNotAvailableError{Slot: start}
	}
	if err != nil {
		return nil, blockHistoryRPCError(unavailable, err)
	}
	return slots, nil
}

func parseBlocksParams(p jsonrpc.RawParams) (uint64, *uint64, blocksConfig, error) {
	invalid := func(message string) (uint64, *uint64, blocksConfig, error) {
		return 0, nil, blocksConfig{}, &InvalidParamsError{Message: message}
	}
	params, err := jsonrpc.DecodeParams[[]json.RawMessage](p)
	if err != nil || len(params) < 1 || len(params) > 3 {
		return invalid("getBlocks requires a start slot, optional end slot and config")
	}
	var start, end *uint64
	if err := json.Unmarshal(params[0], &start); err != nil || start == nil {
		return invalid("invalid start slot")
	}
	var config blocksConfig
	if len(params) >= 2 {
		if err := json.Unmarshal(params[1], &end); err != nil {
			end = nil
			if err := json.Unmarshal(params[1], &config); err != nil {
				return invalid("invalid end slot or config")
			}
			if config.Commitment != nil && *config.Commitment != "processed" && *config.Commitment != "confirmed" && *config.Commitment != "finalized" {
				return invalid("invalid commitment")
			}
		}
	}
	if len(params) == 3 {
		var third *blocksConfig
		if err := json.Unmarshal(params[2], &third); err != nil {
			return invalid("invalid getBlocks config")
		}
		if third != nil {
			config = *third
		}
	}
	if config.Commitment != nil && *config.Commitment != "confirmed" && *config.Commitment != "finalized" {
		return invalid("getBlocks requires confirmed or finalized commitment")
	}
	return *start, end, config, nil
}
