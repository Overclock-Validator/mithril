package rpcserver

import (
	"context"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

const maxSlotStatusSlots = 256

type SlotStatusValue struct {
	Slot    uint64 `json:"slot"`
	Status  string `json:"status"`
	BlockID string `json:"blockId,omitempty"`
}

type GetSlotStatusResp struct {
	Context GetLatestBlockhashRespContext `json:"context"`
	Value   struct {
		// NotarizationSource is "certificate" when the notarized level comes
		// from verified notarization certificates, or "finality" when this
		// node only learns finality (a non-voting node without forwarded
		// certificates) and serves confirmed from the finalized bank.
		NotarizationSource string            `json:"notarizationSource"`
		Slots              []SlotStatusValue `json:"slots"`
	} `json:"value"`
}

// GetSlotStatus is a Mithril extension reporting each slot's Alpenglow level
// (processed, notarized, finalized, durable), or skipped or unknown.
func (rpcServer *RpcServer) GetSlotStatus(ctx context.Context, p jsonrpc.RawParams) (GetSlotStatusResp, error) {
	params, err := decodeParams(p)
	if err != nil {
		return GetSlotStatusResp{}, err
	}
	if len(params) < 1 {
		return GetSlotStatusResp{}, &InvalidParamsError{Message: "getSlotStatus requires an array of slots"}
	}
	raw, ok := params[0].([]interface{})
	if !ok {
		return GetSlotStatusResp{}, &InvalidParamsError{Message: "getSlotStatus requires an array of slots"}
	}
	if len(raw) > maxSlotStatusSlots {
		return GetSlotStatusResp{}, &InvalidParamsError{Message: fmt.Sprintf("Too many inputs provided; max %d", maxSlotStatusSlots)}
	}
	slots := make([]uint64, len(raw))
	for i, v := range raw {
		f, ok := v.(float64)
		if !ok || f < 0 || f != float64(uint64(f)) {
			return GetSlotStatusResp{}, &InvalidParamsError{Message: "invalid slot"}
		}
		slots[i] = uint64(f)
	}
	processed, certificate, statuses, err := rpcServer.bankViews.SlotStatuses(slots)
	if err != nil {
		return GetSlotStatusResp{}, viewError(err)
	}
	if err := rpcServer.resolveSlotHistory(ctx, statuses); err != nil {
		return GetSlotStatusResp{}, err
	}
	var resp GetSlotStatusResp
	resp.Context.Slot = processed
	resp.Value.NotarizationSource = "finality"
	if certificate {
		resp.Value.NotarizationSource = "certificate"
	}
	resp.Value.Slots = make([]SlotStatusValue, len(statuses))
	for i, st := range statuses {
		resp.Value.Slots[i] = SlotStatusValue{Slot: st.Slot, Status: st.Status}
		if st.HasBlockID {
			resp.Value.Slots[i].BlockID = st.BlockID.String()
		}
	}
	return resp, nil
}

// resolveSlotHistory settles slots older than the retained views against the
// finalized bank's SlotHistory sysvar.
func (rpcServer *RpcServer) resolveSlotHistory(ctx context.Context, statuses []bankview.SlotStatus) error {
	needed := false
	for _, st := range statuses {
		needed = needed || st.FromHistory
	}
	if !needed {
		return nil
	}
	return rpcServer.readView(ctx, commitmentConfig{level: bankview.Finalized}, func(r *bankview.Read) error {
		var history sealevel.SysvarSlotHistory
		if err := readSysvar(r, solana.PublicKey(sealevel.SysvarSlotHistoryAddr), &history); err != nil {
			return err
		}
		for i := range statuses {
			if !statuses[i].FromHistory {
				continue
			}
			switch history.Check(statuses[i].Slot) {
			case sealevel.SlotHistoryFound:
			case sealevel.SlotHistoryNotFound:
				statuses[i].Status = "skipped"
			default:
				statuses[i].Status = "unknown"
			}
		}
		return nil
	})
}
