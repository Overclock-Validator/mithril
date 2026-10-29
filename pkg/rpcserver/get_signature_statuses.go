package rpcserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/blockhistory"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

type GetSignatureStatusesResp struct {
	Context GetBalanceRespContext   `json:"context"`
	Value   []*SignatureStatusValue `json:"value"`
}

type SignatureStatusValue struct {
	Slot               uint64                     `json:"slot"`
	Confirmations      *uint64                    `json:"confirmations"`
	Err                json.RawMessage            `json:"err"`
	Status             map[string]json.RawMessage `json:"status"`
	ConfirmationStatus string                     `json:"confirmationStatus"`
}

// signatureStatusUnavailableError is Mithril's bounded optional-status policy,
// not an Agave-standard error or a statement that the entire node is unhealthy.
type signatureStatusUnavailableError struct{}

func (*signatureStatusUnavailableError) Error() string { return "Signature status unavailable" }
func (e *signatureStatusUnavailableError) ToJSONRPCError() (jsonrpc.JSONRPCError, error) {
	return jsonrpc.JSONRPCError{Code: -32000, Message: e.Error()}, nil
}
func (*signatureStatusUnavailableError) FromJSONRPCError(err jsonrpc.JSONRPCError) error {
	if err.Code != -32000 {
		return fmt.Errorf("unexpected signature status error code %d", err.Code)
	}
	return nil
}

// GetSignatureStatuses uses selected accepted outcomes, not ambient ChainTip.
// Exact-identity cache owner notifications must coordinate accepted results,
// certified durable roots and recovery before this capability is enabled.
// Nonroot results are processed, never inferred confirmed.
func (server *RpcServer) GetSignatureStatuses(ctx context.Context, p jsonrpc.RawParams) (GetSignatureStatusesResp, error) {
	signatures, history, err := decodeSignatureStatusParams(p)
	if err != nil {
		return GetSignatureStatusesResp{}, err
	}
	if history && server.transactionHistory == nil {
		return GetSignatureStatusesResp{}, &transactionHistoryNotAvailableError{}
	}
	if err := acquireQueryResponse(ctx); err != nil {
		return GetSignatureStatusesResp{}, &signatureStatusUnavailableError{}
	}
	view, err := server.readSignatureStatuses(ctx, signatures, history)
	if err != nil {
		return GetSignatureStatusesResp{}, &signatureStatusUnavailableError{}
	}
	response := GetSignatureStatusesResp{Context: GetBalanceRespContext{APIVersion: "mithril 0.1", Slot: view.Tip.Slot},
		Value: make([]*SignatureStatusValue, len(signatures))}
	for i, result := range view.Results {
		if !result.Found {
			continue
		}
		value := &SignatureStatusValue{Slot: result.Slot, Err: json.RawMessage("null"),
			Status: map[string]json.RawMessage{"Ok": json.RawMessage("null")}, ConfirmationStatus: "processed"}
		if result.Finalized {
			value.ConfirmationStatus = "finalized"
		} else {
			value.Confirmations = new(uint64)
		}
		if len(result.Error) > 0 {
			value.Err = result.Error
			value.Status = map[string]json.RawMessage{"Err": result.Error}
		}
		response.Value[i] = value
	}
	return response, nil
}

func decodeSignatureStatusParams(p jsonrpc.RawParams) ([]solana.Signature, bool, error) {
	invalid := func(message string) ([]solana.Signature, bool, error) {
		return nil, false, &InvalidParamsError{Message: message}
	}
	if len(p) > 32<<10 {
		return invalid("getSignatureStatuses parameters exceed 32 KiB")
	}
	params, err := jsonrpc.DecodeParams[[]json.RawMessage](p)
	if err != nil || len(params) < 1 || len(params) > 2 {
		return invalid("getSignatureStatuses requires signatures and optional config")
	}
	var values []json.RawMessage
	if err := json.Unmarshal(params[0], &values); err != nil || values == nil || len(values) > 256 {
		return invalid("getSignatureStatuses requires an array of at most 256 signatures")
	}
	signatures := make([]solana.Signature, len(values))
	for i, value := range values {
		var encoded string
		if err := json.Unmarshal(value, &encoded); err != nil || len(encoded) > 88 {
			return invalid("invalid transaction signature")
		}
		signatures[i], err = solana.SignatureFromBase58(encoded)
		if err != nil {
			return invalid("invalid transaction signature")
		}
	}
	var search bool
	if len(params) == 2 && !bytes.Equal(bytes.TrimSpace(params[1]), []byte("null")) {
		var config struct {
			Search *bool `json:"searchTransactionHistory"`
		}
		if err := json.Unmarshal(params[1], &config); err != nil || config.Search == nil {
			return invalid("invalid getSignatureStatuses config")
		}
		search = *config.Search
	}
	return signatures, search, nil
}

type historyReadStamp struct {
	bank             *rootedBankState
	version, durable uint64
}

// The caller holds the recovery gate. This publication fence reads only atomics;
// optional storage reads must finish before the caller checks it again.
func (server *RpcServer) historyStampValid(stamp historyReadStamp) bool {
	version, stable := server.acctsDb.CommittedAccountVersion()
	return !server.historyRecoveryPending && stable && stamp.version == version &&
		stamp.bank == server.rootedBank.Load() && stamp.durable == server.acctsDb.DurableThrough()
}

// The caller proved stamp valid before reading and holds the recovery gate.
// A regressed or currently unstable view remains an ordinary refusal.
func (server *RpcServer) historyReadChange(stamp historyReadStamp) error {
	if server.historyRecoveryPending || server.acctsDb == nil || stamp.bank == nil {
		return transactionhistory.ErrUnavailable
	}
	version, stable := server.acctsDb.CommittedAccountVersion()
	bank, durable := server.rootedBank.Load(), server.acctsDb.DurableThrough()
	if stable && bank != nil && bank.Slot >= stamp.bank.Slot && durable >= stamp.durable &&
		(version != stamp.version || bank != stamp.bank || durable != stamp.durable) {
		return errHistoryViewChanged
	}
	return transactionhistory.ErrUnavailable
}

func (server *RpcServer) readSignatureStatuses(ctx context.Context, signatures []solana.Signature, history bool) (txstatus.ResultView, error) {
	budget := transactionhistory.NewReadBudget()
	for attempt := 0; ; attempt++ {
		view, err := server.readSignatureStatusesOnce(ctx, signatures, history, budget)
		if err != errHistoryViewChanged {
			return view, err
		}
		if attempt == 1 {
			return txstatus.ResultView{}, transactionhistory.ErrUnavailable
		}
		if err := waitForRootedPublication(ctx); err != nil {
			return txstatus.ResultView{}, err
		}
	}
}

func (server *RpcServer) readSignatureStatusesOnce(ctx context.Context, signatures []solana.Signature, history bool, budget *transactionhistory.ReadBudget) (txstatus.ResultView, error) {
	if err := ctx.Err(); err != nil {
		return txstatus.ResultView{}, err
	}
	server.historyRecoveryMu.RLock()
	if server.historyRecoveryPending || server.acctsDb == nil || server.resultCache == nil {
		server.historyRecoveryMu.RUnlock()
		return txstatus.ResultView{}, transactionhistory.ErrUnavailable
	}
	version, stable := server.acctsDb.CommittedAccountVersion()
	stamp := historyReadStamp{bank: server.rootedBank.Load(), version: version, durable: server.acctsDb.DurableThrough()}
	if !stable || stamp.bank == nil || !server.historyStampValid(stamp) {
		server.historyRecoveryMu.RUnlock()
		return txstatus.ResultView{}, transactionhistory.ErrUnavailable
	}
	var view txstatus.ResultView
	var uncertain bool
	var err error
	if history {
		view, uncertain, err = server.resultCache.LookupForHistory(signatures)
	} else {
		view, err = server.resultCache.Lookup(signatures)
	}
	valid := err == nil && view.Root.Slot == stamp.bank.Slot &&
		view.Root.Slot <= stamp.durable && view.Tip.Slot >= view.Root.Slot
	if valid && !server.historyStampValid(stamp) {
		err = server.historyReadChange(stamp)
	}
	server.historyRecoveryMu.RUnlock()
	if !valid {
		return txstatus.ResultView{}, transactionhistory.ErrUnavailable
	}
	if err != nil {
		return txstatus.ResultView{}, err
	}
	missing := false
	for _, result := range view.Results {
		missing = missing || !result.Found
	}
	if history && missing {
		if err := server.fillHistoricalStatuses(ctx, signatures, &view, stamp, budget); err != nil {
			return txstatus.ResultView{}, err
		}
	}
	if uncertain {
		for _, result := range view.Results {
			if !result.Found {
				return txstatus.ResultView{}, transactionhistory.ErrUnavailable
			}
		}
	}
	// A snapshot is already closed before this fence. Cache mutation never
	// holds its mutex while acquiring this gate in reverse order.
	server.historyRecoveryMu.RLock()
	latest, lookupErr := server.resultCache.Lookup(nil)
	valid = lookupErr == nil && latest.Generation == view.Generation && latest.Tip == view.Tip && latest.Root == view.Root
	if valid && !server.historyStampValid(stamp) {
		err = server.historyReadChange(stamp)
	}
	if !valid && lookupErr == nil && server.resultCache.ChangedOnlyForward(view) &&
		(server.historyStampValid(stamp) || server.historyReadChange(stamp) == errHistoryViewChanged) {
		err = errHistoryViewChanged
	}
	server.historyRecoveryMu.RUnlock()
	if !valid {
		if err == errHistoryViewChanged {
			return txstatus.ResultView{}, err
		}
		return txstatus.ResultView{}, transactionhistory.ErrUnavailable
	}
	if err != nil {
		return txstatus.ResultView{}, err
	}
	if err := ctx.Err(); err != nil {
		return txstatus.ResultView{}, err
	}
	return view, nil
}

func (server *RpcServer) fillHistoricalStatuses(ctx context.Context, signatures []solana.Signature, view *txstatus.ResultView, stamp historyReadStamp, budget *transactionhistory.ReadBudget) (err error) {
	if server.blockHistory == nil {
		return transactionhistory.ErrUnavailable
	}
	snapshot, release, err := server.acquireTransactionHistory(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, release()) }()
	for i, signature := range signatures {
		if view.Results[i].Found {
			continue
		}
		candidates, err := snapshot.SignatureCandidates(ctx, signature, budget)
		if err != nil {
			return err
		}
		var selected *transactionhistory.Ref
		for _, candidate := range candidates {
			ref := candidate.Ref
			if ref.Block.Slot > stamp.bank.Slot || ref.Block.Slot > stamp.durable {
				continue
			}
			if err := budget.Charge(ctx, 1, 0); err != nil {
				return err
			}
			summary, err := server.blockHistory.Get(ref.Block.Slot)
			if errors.Is(err, blockhistory.ErrNotAvailable) || errors.Is(err, blockhistory.ErrSlotSkipped) {
				continue
			}
			if err != nil {
				return err
			}
			hash, err := solana.HashFromBase58(summary.Blockhash)
			if err != nil {
				return transactionhistory.ErrUnavailable
			}
			if [32]byte(hash) != ref.Block.Blockhash {
				continue
			}
			previous, err := solana.HashFromBase58(summary.PreviousBlockhash)
			if err != nil || candidate.Header.ParentSlot != summary.ParentSlot ||
				candidate.Header.PreviousBlockhash != [32]byte(previous) ||
				candidate.Header.CaptureVersion != transactionhistory.CaptureVersion {
				return transactionhistory.ErrUnavailable
			}
			// Fixed application-copy charge, not Pebble engine I/O/allocations.
			if err := budget.Charge(ctx, 1, 32); err != nil {
				return err
			}
			bankhash, err := server.acctsDb.GetBankHashForSlot(ref.Block.Slot)
			if err != nil || !bytes.Equal(bankhash, candidate.Header.Bankhash[:]) {
				return transactionhistory.ErrUnavailable
			}
			if selected != nil {
				return transactionhistory.ErrUnavailable
			}
			selected = &ref
		}
		if selected == nil {
			continue
		}
		record, err := snapshot.TransactionWithBudget(ctx, *selected, budget)
		if err != nil {
			return err
		}
		if err := budget.Charge(ctx, 0, uint64(len(record.Metadata))); err != nil {
			return err
		}
		metadata, err := transactionhistory.UnmarshalMetadata(record.Metadata)
		if err != nil {
			return err
		}
		result := txstatus.CachedResult{Slot: selected.Block.Slot, Found: true, Finalized: true}
		if !bytes.Equal(bytes.TrimSpace(metadata.Error), []byte("null")) {
			result.Error = metadata.Error
		}
		view.Results[i] = result
	}
	return nil
}
