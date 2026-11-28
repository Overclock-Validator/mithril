package rpcserver

import (
	"context"

	"github.com/filecoin-project/go-jsonrpc"
)

// healthCheckSlotDistance is the default Solana RPC health-check distance.
const healthCheckSlotDistance = 128

// GetHealth reports whether replay is ready and close to the observed network tip.
func (rpcServer *RpcServer) GetHealth(ctx context.Context, p jsonrpc.RawParams) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if params, err := decodeOptionalParams(p); err != nil || len(params) != 0 {
		return "", &InvalidParamsError{Message: "getHealth does not accept parameters"}
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

// InvalidateHealth marks replay unavailable without discarding the last RPC bank.
func (rpcServer *RpcServer) InvalidateHealth() {
	rpcServer.slotCtxMu.Lock()
	rpcServer.healthReady = false
	rpcServer.executionGeneration++
	rpcServer.executionInvalidated = true
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
