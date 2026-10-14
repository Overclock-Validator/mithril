package rpcserver

import (
	"encoding/json"
	"fmt"

	"github.com/filecoin-project/go-jsonrpc"
)

// JSON-RPC error codes that match Agave's solana_rpc_client_types.
const (
	// -32602 is the JSON-RPC standard "Invalid params" code.
	rpcCodeInvalidParams jsonrpc.ErrorCode = -32602
	// -32002 matches Agave's SendTransactionPreflightFailure.
	rpcCodeSendTransactionPreflightFailure jsonrpc.ErrorCode = -32002
	// -32005 is the Solana RPC NodeUnhealthy error code.
	rpcCodeNodeUnhealthy jsonrpc.ErrorCode = -32005
	// -32016 is Agave's reserved code for MinContextSlotNotReached.
	rpcCodeMinContextSlotNotReached jsonrpc.ErrorCode = -32016
)

type NodeUnhealthyError struct {
	NumSlotsBehind *uint64
}

func (e *NodeUnhealthyError) Error() string {
	if e.NumSlotsBehind != nil {
		return fmt.Sprintf("Node is behind by %d slots", *e.NumSlotsBehind)
	}
	return "Node is unhealthy"
}

func (e *NodeUnhealthyError) ToJSONRPCError() (jsonrpc.JSONRPCError, error) {
	return jsonrpc.JSONRPCError{
		Code:    rpcCodeNodeUnhealthy,
		Message: e.Error(),
		Data: struct {
			NumSlotsBehind *uint64 `json:"numSlotsBehind"`
		}{NumSlotsBehind: e.NumSlotsBehind},
	}, nil
}

func (e *NodeUnhealthyError) FromJSONRPCError(rpcErr jsonrpc.JSONRPCError) error {
	if rpcErr.Code != rpcCodeNodeUnhealthy {
		return fmt.Errorf("unexpected code %d for NodeUnhealthyError", rpcErr.Code)
	}
	if rpcErr.Data == nil {
		e.NumSlotsBehind = nil
		return nil
	}
	raw, err := json.Marshal(rpcErr.Data)
	if err != nil {
		return fmt.Errorf("re-encoding NodeUnhealthyError data: %w", err)
	}
	var payload struct {
		NumSlotsBehind *uint64 `json:"numSlotsBehind"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("decoding NodeUnhealthyError data: %w", err)
	}
	e.NumSlotsBehind = payload.NumSlotsBehind
	return nil
}

type MinContextSlotNotReachedError struct {
	ContextSlot uint64
}

func (e *MinContextSlotNotReachedError) Error() string {
	return "Minimum context slot has not been reached"
}

func (e *MinContextSlotNotReachedError) ToJSONRPCError() (jsonrpc.JSONRPCError, error) {
	return jsonrpc.JSONRPCError{
		Code:    rpcCodeMinContextSlotNotReached,
		Message: e.Error(),
		Data:    map[string]uint64{"contextSlot": e.ContextSlot},
	}, nil
}

func (e *MinContextSlotNotReachedError) FromJSONRPCError(rpcErr jsonrpc.JSONRPCError) error {
	if rpcErr.Code != rpcCodeMinContextSlotNotReached {
		return fmt.Errorf("unexpected code %d for MinContextSlotNotReachedError", rpcErr.Code)
	}
	if rpcErr.Data == nil {
		return nil
	}
	// `Data` is interface{}; round-trip through JSON to read camelCase.
	raw, err := json.Marshal(rpcErr.Data)
	if err != nil {
		return fmt.Errorf("re-encoding MinContextSlotNotReachedError data: %w", err)
	}
	var payload struct {
		ContextSlot uint64 `json:"contextSlot"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("decoding MinContextSlotNotReachedError data: %w", err)
	}
	e.ContextSlot = payload.ContextSlot
	return nil
}

type InvalidParamsError struct {
	Message string
}

func (e *InvalidParamsError) Error() string { return e.Message }

func (e *InvalidParamsError) ToJSONRPCError() (jsonrpc.JSONRPCError, error) {
	return jsonrpc.JSONRPCError{
		Code:    rpcCodeInvalidParams,
		Message: e.Message,
	}, nil
}

func (e *InvalidParamsError) FromJSONRPCError(rpcErr jsonrpc.JSONRPCError) error {
	if rpcErr.Code != rpcCodeInvalidParams {
		return fmt.Errorf("unexpected code %d for InvalidParamsError", rpcErr.Code)
	}
	e.Message = rpcErr.Message
	return nil
}

type SendTransactionPreflightFailureError struct {
	Message string
	Result  SimulateTransactionRespValue
}

func (e *SendTransactionPreflightFailureError) Error() string { return e.Message }

func (e *SendTransactionPreflightFailureError) ToJSONRPCError() (jsonrpc.JSONRPCError, error) {
	return jsonrpc.JSONRPCError{
		Code:    rpcCodeSendTransactionPreflightFailure,
		Message: e.Message,
		Data:    e.Result,
	}, nil
}

func (e *SendTransactionPreflightFailureError) FromJSONRPCError(rpcErr jsonrpc.JSONRPCError) error {
	if rpcErr.Code != rpcCodeSendTransactionPreflightFailure {
		return fmt.Errorf("unexpected code %d for SendTransactionPreflightFailureError", rpcErr.Code)
	}
	e.Message = rpcErr.Message
	if rpcErr.Data == nil {
		e.Result = SimulateTransactionRespValue{}
		return nil
	}
	raw, err := json.Marshal(rpcErr.Data)
	if err != nil {
		return fmt.Errorf("re-encoding SendTransactionPreflightFailureError data: %w", err)
	}
	if err := json.Unmarshal(raw, &e.Result); err != nil {
		return fmt.Errorf("decoding SendTransactionPreflightFailureError data: %w", err)
	}
	return nil
}

func rpcErrorRegistry() jsonrpc.Errors {
	errs := jsonrpc.NewErrors()
	errs.Register(rpcCodeInvalidParams, new(*InvalidParamsError))
	errs.Register(rpcCodeSendTransactionPreflightFailure, new(*SendTransactionPreflightFailureError))
	errs.Register(rpcCodeNodeUnhealthy, new(*NodeUnhealthyError))
	errs.Register(rpcCodeMinContextSlotNotReached, new(*MinContextSlotNotReachedError))
	return errs
}
