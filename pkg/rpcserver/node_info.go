package rpcserver

import (
	"context"
	"encoding/json"
	"errors"

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

	rpcServer.slotCtxMu.RLock()
	slotCtx, healthSlot, ready := rpcServer.slotCtx, rpcServer.healthSlot, rpcServer.healthReady
	rpcServer.slotCtxMu.RUnlock()
	if !ready || slotCtx == nil || healthSlot == nil {
		return nodeHealth(false, 0, 0)
	}
	clusterSlot, fresh := healthSlot()
	return nodeHealth(fresh, slotCtx.Slot, clusterSlot)
}

// SetHealthSlotSource uses the block source's cached network tip without polling per request.
func (rpcServer *RpcServer) SetHealthSlotSource(source func() (uint64, bool)) {
	rpcServer.slotCtxMu.Lock()
	rpcServer.healthSlot = source
	rpcServer.slotCtxMu.Unlock()
}

// SetIdentitySource supplies the current node identity; nil uses SetIdentity.
func (rpcServer *RpcServer) SetIdentitySource(source func() string) {
	rpcServer.slotCtxMu.Lock()
	rpcServer.identitySource = source
	rpcServer.slotCtxMu.Unlock()
}

// InvalidateHealth marks replay unavailable without discarding the last RPC bank.
func (rpcServer *RpcServer) InvalidateHealth() {
	rpcServer.slotCtxMu.Lock()
	rpcServer.healthReady = false
	rpcServer.slotCtxMu.Unlock()
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
	rpcServer.slotCtxMu.RLock()
	identity, source := rpcServer.identity, rpcServer.identitySource
	rpcServer.slotCtxMu.RUnlock()
	if source != nil {
		identity = source()
	}
	if identity == "" {
		return GetIdentityResp{}, errors.New("validator identity is not configured")
	}
	return GetIdentityResp{Identity: identity}, nil
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
