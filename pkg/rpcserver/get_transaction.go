package rpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/blockhistory"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

// GetTransactionResp uses the same retained transaction and metadata as GetBlock.
type GetTransactionResp struct {
	historyTransactionResponse
	Slot      uint64 `json:"slot"`
	BlockTime *int64 `json:"blockTime"`
}

func (server *RpcServer) GetTransaction(ctx context.Context, p jsonrpc.RawParams) (*GetTransactionResp, error) {
	params, err := jsonrpc.DecodeParams[[]json.RawMessage](p)
	if err != nil || len(params) < 1 || len(params) > 2 {
		return nil, &InvalidParamsError{Message: "getTransaction requires a signature and optional config"}
	}
	var encodedSignature string
	if err := json.Unmarshal(params[0], &encodedSignature); err != nil || len(encodedSignature) > 88 {
		return nil, &InvalidParamsError{Message: "invalid transaction signature"}
	}
	signature, err := solana.SignatureFromBase58(encodedSignature)
	if err != nil {
		return nil, &InvalidParamsError{Message: "invalid transaction signature"}
	}
	config, err := decodeTransactionHistoryConfig(params)
	if err != nil {
		return nil, err
	}
	if server.transactionHistory == nil {
		return nil, &transactionHistoryNotAvailableError{}
	}
	if err := acquireHistoryResponse(ctx); err != nil {
		return nil, err
	}
	summary, record, found, err := server.readTransactionHistoryTransaction(ctx, signature)
	if err != nil || !found {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	transaction, err := encodeHistoryTransaction(record, config.encoding, config.maxVersion)
	if err != nil {
		return nil, err
	}
	return &GetTransactionResp{historyTransactionResponse: transaction, Slot: summary.Slot, BlockTime: summary.BlockTime}, nil
}

func decodeTransactionHistoryConfig(params []json.RawMessage) (blockHistoryConfig, error) {
	var config struct {
		Encoding                       *string `json:"encoding"`
		Commitment                     *string `json:"commitment"`
		MaxSupportedTransactionVersion *uint8  `json:"maxSupportedTransactionVersion"`
	}
	if len(params) == 2 {
		if err := json.Unmarshal(params[1], &config); err != nil {
			var fieldError *json.UnmarshalTypeError
			if errors.As(err, &fieldError) && fieldError.Field != "" {
				return blockHistoryConfig{}, &InvalidParamsError{Message: "invalid " + fieldError.Field}
			}
			var encoding string
			if err := json.Unmarshal(params[1], &encoding); err != nil {
				return blockHistoryConfig{}, &InvalidParamsError{Message: "invalid getTransaction config"}
			}
			config.Encoding = &encoding
		}
	}
	if config.Commitment != nil && *config.Commitment != "confirmed" && *config.Commitment != "finalized" {
		return blockHistoryConfig{}, &InvalidParamsError{Message: "getTransaction requires confirmed or finalized commitment"}
	}
	encoding := "json"
	if config.Encoding != nil {
		encoding = *config.Encoding
	}
	if encoding != "json" && encoding != "base64" {
		return blockHistoryConfig{}, &InvalidParamsError{Message: "transaction history supports json and base64 encoding"}
	}
	return blockHistoryConfig{encoding: encoding, maxVersion: config.MaxSupportedTransactionVersion}, nil
}

// readTransactionHistoryTransaction selects a unique canonical candidate. An
// orphan is not a match. SignatureRefs checks every indexed receipt, so damage
// to any candidate fails the query instead of returning a partial lookup.
func (server *RpcServer) readTransactionHistoryTransaction(ctx context.Context, signature solana.Signature) (summary blockhistory.Record, record transactionhistory.Record, found bool, err error) {
	for attempt := 0; ; attempt++ {
		summary, record, found, err = server.readTransactionHistoryTransactionOnce(ctx, signature)
		if err != errHistoryViewChanged {
			return summary, record, found, err
		}
		if attempt == 1 {
			return blockhistory.Record{}, transactionhistory.Record{}, false, transactionhistory.ErrUnavailable
		}
		if err = waitForRootedPublication(ctx); err != nil {
			return blockhistory.Record{}, transactionhistory.Record{}, false, err
		}
	}
}

func (server *RpcServer) readTransactionHistoryTransactionOnce(ctx context.Context, signature solana.Signature) (summary blockhistory.Record, record transactionhistory.Record, found bool, err error) {
	snapshot, release, err := server.acquireTransactionHistory(ctx)
	if err != nil {
		return summary, record, false, err
	}
	defer func() {
		if releaseErr := release(); releaseErr != nil {
			err = errors.Join(err, releaseErr)
		}
	}()
	refs, err := snapshot.SignatureRefs(signature)
	if err != nil {
		return summary, record, false, err
	}
	var selected *transactionhistory.Ref
	server.historyRecoveryMu.RLock()
	if server.acctsDb == nil {
		server.historyRecoveryMu.RUnlock()
		return summary, record, false, transactionhistory.ErrUnavailable
	}
	version, _ := server.acctsDb.CommittedAccountVersion()
	stamp := historyReadStamp{bank: server.rootedBank.Load(), version: version, durable: server.acctsDb.DurableThrough()}
	if stamp.bank == nil || !server.historyStampValid(stamp) {
		server.historyRecoveryMu.RUnlock()
		return summary, record, false, transactionhistory.ErrUnavailable
	}
	summary, selected, err = server.selectTransactionHistoryRef(refs)
	if err == nil && !server.historyStampValid(stamp) {
		err = server.historyReadChange(stamp)
	}
	server.historyRecoveryMu.RUnlock()
	if err != nil {
		return summary, record, false, err
	}
	if selected == nil {
		return summary, record, false, nil
	}
	verified, err := server.selectTransactionHistoryBlock(snapshot, selected.Block)
	if err != nil {
		return summary, record, false, err
	}
	server.historyRecoveryMu.RLock()
	var latest *transactionhistory.Ref
	summary, latest, err = server.selectTransactionHistoryRef(refs)
	if err == nil {
		if latest == nil || *latest != *selected || summary.ParentSlot != verified.ParentSlot || summary.PreviousBlockhash != verified.PreviousBlockhash {
			err = transactionhistory.ErrUnavailable
		} else if !server.historyStampValid(stamp) {
			err = server.historyReadChange(stamp)
		}
	}
	server.historyRecoveryMu.RUnlock()
	if err != nil {
		return summary, record, false, err
	}
	record, err = snapshot.Transaction(*selected)
	return summary, record, err == nil, err
}

// The caller holds the recovery gate. Selection reads only in-memory summaries;
// completion, bankhash and payload reads happen outside the gate.
func (server *RpcServer) selectTransactionHistoryRef(refs []transactionhistory.Ref) (blockhistory.Record, *transactionhistory.Ref, error) {
	if server.historyRecoveryPending || server.blockHistory == nil || server.acctsDb == nil {
		return blockhistory.Record{}, nil, transactionhistory.ErrUnavailable
	}
	bank := server.rootedBank.Load()
	version, stable := server.acctsDb.CommittedAccountVersion()
	stamp := historyReadStamp{bank: bank, version: version, durable: server.acctsDb.DurableThrough()}
	if bank == nil || !stable || !server.historyStampValid(stamp) {
		return blockhistory.Record{}, nil, transactionhistory.ErrUnavailable
	}
	var result blockhistory.Record
	var selected *transactionhistory.Ref
	for _, ref := range refs {
		if ref.Block.Slot > bank.Slot || ref.Block.Slot > server.acctsDb.DurableThrough() {
			continue
		}
		summary, err := server.blockHistory.Get(ref.Block.Slot)
		if errors.Is(err, blockhistory.ErrNotAvailable) || errors.Is(err, blockhistory.ErrSlotSkipped) {
			continue
		}
		if err != nil {
			return blockhistory.Record{}, nil, err
		}
		hash, err := solana.HashFromBase58(summary.Blockhash)
		if err != nil {
			return blockhistory.Record{}, nil, transactionhistory.ErrUnavailable
		}
		if [32]byte(hash) != ref.Block.Blockhash {
			continue
		}
		if selected != nil {
			return blockhistory.Record{}, nil, fmt.Errorf("multiple canonical transaction records: %w", transactionhistory.ErrUnavailable)
		}
		result, selected = summary, &ref
	}
	if !server.historyStampValid(stamp) {
		return blockhistory.Record{}, nil, server.historyReadChange(stamp)
	}
	return result, selected, nil
}

type transactionHistoryNotAvailableError struct{}

func (*transactionHistoryNotAvailableError) Error() string {
	return "Transaction history is not available from this node"
}

func (e *transactionHistoryNotAvailableError) ToJSONRPCError() (jsonrpc.JSONRPCError, error) {
	return jsonrpc.JSONRPCError{Code: -32011, Message: e.Error()}, nil
}

func (*transactionHistoryNotAvailableError) FromJSONRPCError(rpcErr jsonrpc.JSONRPCError) error {
	if rpcErr.Code != -32011 {
		return fmt.Errorf("unexpected code %d for unavailable transaction history", rpcErr.Code)
	}
	return nil
}
