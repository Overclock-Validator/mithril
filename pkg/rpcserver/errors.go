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
	// -32004 matches Agave's BlockNotAvailable error.
	rpcCodeBlockNotAvailable jsonrpc.ErrorCode = -32004
	// -32016 is Agave's reserved code for MinContextSlotNotReached.
	rpcCodeMinContextSlotNotReached jsonrpc.ErrorCode = -32016
	// -32007 matches Agave's SlotSkipped error.
	rpcCodeSlotSkipped jsonrpc.ErrorCode = -32007
)

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

type SlotSkippedError struct {
	Slot uint64
}

type BlockNotAvailableError struct {
	Slot uint64
}

func (e *BlockNotAvailableError) Error() string {
	return fmt.Sprintf("Block not available for slot %d", e.Slot)
}

func (e *BlockNotAvailableError) ToJSONRPCError() (jsonrpc.JSONRPCError, error) {
	return jsonrpc.JSONRPCError{Code: rpcCodeBlockNotAvailable, Message: e.Error()}, nil
}

func (e *BlockNotAvailableError) FromJSONRPCError(rpcErr jsonrpc.JSONRPCError) error {
	if rpcErr.Code != rpcCodeBlockNotAvailable {
		return fmt.Errorf("unexpected code %d for BlockNotAvailableError", rpcErr.Code)
	}
	return nil
}

func (e *SlotSkippedError) Error() string {
	return fmt.Sprintf("Slot %d was skipped", e.Slot)
}

func (e *SlotSkippedError) ToJSONRPCError() (jsonrpc.JSONRPCError, error) {
	return jsonrpc.JSONRPCError{Code: rpcCodeSlotSkipped, Message: e.Error()}, nil
}

func (e *SlotSkippedError) FromJSONRPCError(rpcErr jsonrpc.JSONRPCError) error {
	if rpcErr.Code != rpcCodeSlotSkipped {
		return fmt.Errorf("unexpected code %d for SlotSkippedError", rpcErr.Code)
	}
	return nil
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
	errs.Register(rpcCodeBlockNotAvailable, new(*BlockNotAvailableError))
	errs.Register(rpcCodeMinContextSlotNotReached, new(*MinContextSlotNotReachedError))
	errs.Register(rpcCodeSlotSkipped, new(*SlotSkippedError))
	return errs
}
