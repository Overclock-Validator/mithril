package rpcserver

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Overclock-Validator/mithril/pkg/version"
	"github.com/filecoin-project/go-jsonrpc"
)

// GetVersionResp is the node's software and feature-set version.
type GetVersionResp struct {
	SolanaCore string  `json:"solana-core"`
	FeatureSet *uint32 `json:"feature-set"`
}

// GetIdentityResp contains the node's public identity.
type GetIdentityResp struct {
	Identity string `json:"identity"`
}

// SetIdentity sets the fallback public identity.
func (rpcServer *RpcServer) SetIdentity(identity string) {
	rpcServer.slotCtxMu.Lock()
	rpcServer.identity = identity
	rpcServer.slotCtxMu.Unlock()
}

// SetIdentitySource supplies the current node identity; nil uses SetIdentity.
func (rpcServer *RpcServer) SetIdentitySource(source func() string) {
	rpcServer.slotCtxMu.Lock()
	rpcServer.identitySource = source
	rpcServer.slotCtxMu.Unlock()
}

// GetVersion returns the node's software version.
func (rpcServer *RpcServer) GetVersion(ctx context.Context, p jsonrpc.RawParams) (GetVersionResp, error) {
	if err := validateNoParams(ctx, "getVersion", p); err != nil {
		return GetVersionResp{}, err
	}
	return GetVersionResp{SolanaCore: version.Version}, nil
}

// GetIdentity returns the node's current public identity.
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
