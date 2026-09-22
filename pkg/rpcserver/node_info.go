package rpcserver

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/version"
	"github.com/filecoin-project/go-jsonrpc"
)

// healthCheckSlotDistance is the default Solana RPC health-check distance.
const healthCheckSlotDistance = 128

type GetVersionResp struct {
	SolanaCore string  `json:"solana-core"`
	FeatureSet *uint32 `json:"feature-set"`
}

type GetIdentityResp struct {
	Identity string `json:"identity"`
}

func (rpcServer *RpcServer) GetHealth(ctx context.Context, p jsonrpc.RawParams) (string, error) {
	if err := validateNoParams(ctx, "getHealth", p); err != nil {
		return "", err
	}

	slotCtx := rpcServer.getSlotCtx()
	if slotCtx == nil {
		return nodeHealth(false, 0, 0)
	}
	return nodeHealth(true, slotCtx.Slot, global.WallClockSlot())
}

func nodeHealth(ready bool, localSlot, clusterSlot uint64) (string, error) {
	if !ready {
		return "", &NodeUnhealthyError{}
	}
	if clusterSlot > localSlot && clusterSlot-localSlot > healthCheckSlotDistance {
		behind := clusterSlot - localSlot
		return "", &NodeUnhealthyError{NumSlotsBehind: &behind}
	}
	return "ok", nil
}

func (rpcServer *RpcServer) GetVersion(ctx context.Context, p jsonrpc.RawParams) (GetVersionResp, error) {
	if err := validateNoParams(ctx, "getVersion", p); err != nil {
		return GetVersionResp{}, err
	}
	return GetVersionResp{SolanaCore: version.Version}, nil
}

func (rpcServer *RpcServer) GetIdentity(ctx context.Context, p jsonrpc.RawParams) (GetIdentityResp, error) {
	if err := validateNoParams(ctx, "getIdentity", p); err != nil {
		return GetIdentityResp{}, err
	}
	if rpcServer.identity == "" {
		return GetIdentityResp{}, errors.New("validator identity is not configured")
	}
	return GetIdentityResp{Identity: rpcServer.identity}, nil
}

func validateNoParams(ctx context.Context, method string, p jsonrpc.RawParams) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(p) == 0 {
		return nil
	}
	params, err := jsonrpc.DecodeParams[[]json.RawMessage](p)
	if err != nil || len(params) != 0 {
		return &InvalidParamsError{Message: method + " does not accept parameters"}
	}
	return nil
}
