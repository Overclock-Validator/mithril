package rpcserver

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
)

type SimulateTransactionResp struct {
	Context SimulateTransactionRespContext `json:"context"`
	Value   SimulateTransactionRespValue   `json:"value"`
}

type SimulateTransactionRespContext struct {
	ApiVersion string `json:"apiVersion"`
	Slot       uint64 `json:"slot"`
}

// SimulateTransactionRespValue mirrors Agave's RpcSimulateTransactionResult
// field-for-field. Every field is emitted unconditionally as either a real
// value or null; no omitempty. Pointer types model Agave's Option<T> so
// nil → JSON null and a populated value → the value.
//
// `Err` is intentionally left as interface{}: it is a true sum type
// (nil | string | *replay.TransactionError), and *replay.TransactionError
// carries a custom MarshalJSON. Wrapping it in a concrete struct would
// only force a custom MarshalJSON without adding type safety.
type SimulateTransactionRespValue struct {
	Err                    interface{}                  `json:"err"`
	Logs                   *[]string                    `json:"logs"`
	Accounts               *[]*AccountInfoPayload       `json:"accounts"`
	UnitsConsumed          *uint64                      `json:"unitsConsumed"`
	ReturnData             *ReturnDataPayload           `json:"returnData"`
	InnerInstructions      *[]InnerInstructionGroup     `json:"innerInstructions"`
	ReplacementBlockhash   *ReplacementBlockhashPayload `json:"replacementBlockhash"`
	LoadedAccountsDataSize *uint32                      `json:"loadedAccountsDataSize"`
	Fee                    *uint64                      `json:"fee"`
	PreBalances            *[]uint64                    `json:"preBalances"`
	PostBalances           *[]uint64                    `json:"postBalances"`
	PreTokenBalances       *[]TokenBalancePayload       `json:"preTokenBalances"`
	PostTokenBalances      *[]TokenBalancePayload       `json:"postTokenBalances"`
	LoadedAddresses        *LoadedAddressesPayload      `json:"loadedAddresses"`
}

type AccountInfoPayload struct {
	Data       interface{} `json:"data"`
	Executable bool        `json:"executable"`
	Lamports   uint64      `json:"lamports"`
	Owner      string      `json:"owner"`
	RentEpoch  uint64      `json:"rentEpoch"`
	Space      int         `json:"space"`
}

type ReturnDataPayload struct {
	ProgramId string   `json:"programId"`
	Data      []string `json:"data"`
}

type InnerInstructionGroup struct {
	Index        uint8              `json:"index"`
	Instructions []InnerInstruction `json:"instructions"`
}

// InnerInstruction matches Agave's UiCompiledInstruction. `Data` is
// base58-encoded; `StackHeight` is nil when not tracked. `StackHeight`
// is `Option<u32>` in Agave's wire format; using *uint32 here so the
// field's JSON-number range matches the spec exactly even though
// real-world CPI depth never exceeds the 4-deep cap.
type InnerInstruction struct {
	ProgramIdIndex uint8   `json:"programIdIndex"`
	Accounts       []uint8 `json:"accounts"`
	Data           string  `json:"data"`
	StackHeight    *uint32 `json:"stackHeight"`
}

type ReplacementBlockhashPayload struct {
	Blockhash            string `json:"blockhash"`
	LastValidBlockHeight uint64 `json:"lastValidBlockHeight"`
}

type LoadedAddressesPayload struct {
	Readonly []string `json:"readonly"`
	Writable []string `json:"writable"`
}

type TokenBalancePayload struct {
	AccountIndex  uint8                `json:"accountIndex"`
	Mint          string               `json:"mint"`
	Owner         string               `json:"owner,omitempty"`
	ProgramId     string               `json:"programId,omitempty"`
	UiTokenAmount UiTokenAmountPayload `json:"uiTokenAmount"`
}

type UiTokenAmountPayload struct {
	Amount         string   `json:"amount"`
	Decimals       uint8    `json:"decimals"`
	UiAmount       *float64 `json:"uiAmount"`
	UiAmountString string   `json:"uiAmountString"`
}

type simulateTransactionConfig struct {
	sigVerify              bool
	replaceRecentBlockhash bool
	innerInstructions      bool
	encoding               string
	commitment             string
	minContextSlot         *uint64
	accounts               *simulateAccountsConfig
}

type simulateAccountsConfig struct {
	addresses []string
	encoding  string
}

func (rpcServer *RpcServer) SimulateTransaction(ctx context.Context, p jsonrpc.RawParams) (SimulateTransactionResp, error) {
	metrics.GlobalSimulate.TotalCalls.Inc()
	defer metrics.GlobalSimulate.HandlerLatency.AddTimingSince(time.Now())

	params, err := jsonrpc.DecodeParams[[]interface{}](p)
	if err != nil {
		return SimulateTransactionResp{}, &InvalidParamsError{Message: fmt.Sprintf("decoding params: %v", err)}
	}

	if len(params) < 1 {
		return SimulateTransactionResp{}, &InvalidParamsError{Message: "simulateTransaction requires a transaction string as first parameter"}
	}

	txStr, ok := params[0].(string)
	if !ok {
		return SimulateTransactionResp{}, &InvalidParamsError{Message: "simulateTransaction requires a transaction string as first parameter"}
	}

	conf := parseSimulateConfig(params)

	// Validate the encoding name BEFORE attempting tx decode so a bad
	// encoding surfaces as "unsupported encoding: …" not "failed to decode".
	// Agave order: into_binary_encoding() runs before decode_and_deserialize.
	if conf.encoding != "" && conf.encoding != "base58" && conf.encoding != "base64" {
		return SimulateTransactionResp{}, &InvalidParamsError{
			Message: fmt.Sprintf("unsupported encoding: %s", conf.encoding),
		}
	}

	// Reject oversized encoded inputs BEFORE decode, matching Agave's
	// version-sensitive decode_and_deserialize limits.
	maxEncodedSize := maxBase58TxSize
	maxRawSize := legacyTransactionSize
	if conf.encoding == "base58" {
		if len(txStr) > maxEncodedSize {
			return SimulateTransactionResp{}, &InvalidParamsError{
				Message: fmt.Sprintf("base58 encoded solana_transaction too large: %d bytes (max: encoded/raw %d/%d)", len(txStr), maxEncodedSize, maxRawSize),
			}
		}
	} else {
		maxEncodedSize, maxRawSize = base64TransactionSizeLimits(txStr)
		if len(txStr) > maxEncodedSize {
			return SimulateTransactionResp{}, &InvalidParamsError{
				Message: fmt.Sprintf("base64 encoded solana_transaction too large: %d bytes (max: encoded/raw %d/%d)", len(txStr), maxEncodedSize, maxRawSize),
			}
		}
	}

	var wire []byte
	if conf.encoding == "base58" {
		wire, err = base58.Decode(txStr)
	} else {
		wire, err = base64.StdEncoding.DecodeString(txStr)
	}
	if err != nil {
		return SimulateTransactionResp{}, &InvalidParamsError{Message: fmt.Sprintf("failed to decode transaction: %v", err)}
	}
	if len(wire) > maxRawSize {
		return SimulateTransactionResp{}, &InvalidParamsError{
			Message: fmt.Sprintf("decoded solana_transaction too large: %d bytes (max: %d bytes)", len(wire), maxRawSize),
		}
	}
	tx, err := decodeTransactionExact(wire)
	if err != nil {
		return SimulateTransactionResp{}, &InvalidParamsError{Message: fmt.Sprintf("failed to decode transaction: %v", err)}
	}

	// Conflict check runs after decode so a bad tx surfaces as "failed to
	// decode" rather than masking the underlying issue.
	if conf.sigVerify && conf.replaceRecentBlockhash {
		return SimulateTransactionResp{}, &InvalidParamsError{
			Message: "sigVerify may not be used with replaceRecentBlockhash",
		}
	}

	if conf.accounts != nil && conf.accounts.encoding != "" {
		switch conf.accounts.encoding {
		case "base58", "binary":
			return SimulateTransactionResp{}, &InvalidParamsError{
				Message: "base58 encoding not supported",
			}
		}
	}

	if conf.minContextSlot != nil && global.Slot() < *conf.minContextSlot {
		return SimulateTransactionResp{}, &MinContextSlotNotReachedError{
			ContextSlot: *conf.minContextSlot,
		}
	}

	var replacementBlockhash *ReplacementBlockhashPayload
	if conf.replaceRecentBlockhash {
		latestBlockhash := global.LatestBlockHash()
		tx.Message.RecentBlockhash = solana.Hash(latestBlockhash)
		replacementBlockhash = &ReplacementBlockhashPayload{
			Blockhash:            base58.Encode(latestBlockhash[:]),
			LastValidBlockHeight: global.BlockHeight(),
		}
	}

	slotCtx := rpcServer.getSlotCtx()
	if slotCtx == nil {
		return SimulateTransactionResp{}, fmt.Errorf("node is not ready for simulation")
	}

	// ALT resolve runs before sigVerify so the failure response carries the
	// resolved loadedAddresses, not empty arrays.
	if err := replay.ResolveAddrTableLookupsForTx(ctx, rpcServer.acctsDb, slotCtx.Slot, tx); err != nil {
		metrics.GlobalSimulate.AddressLookupFails.Inc()
		return earlyFailResponse("AddressLookupTableNotFound", replacementBlockhash, conf, tx), nil
	}

	// RuntimeTransaction::try_create sanitizes unconditionally in Agave's
	// simulateTransaction path. Do the same after ALT resolution so malformed
	// transactions are rejected as invalid params before replay applies any
	// transaction-version feature gate. This must not depend on sigVerify.
	if err := txverify.SanitizeTransaction(tx); err != nil {
		metrics.GlobalSimulate.SanitizeFailures.Inc()
		return SimulateTransactionResp{}, errInvalidSanitizedTransaction
	}

	// Cap uses post-ALT-resolve key count; pre-resolve would reject valid
	// requests for versioned txs.
	if conf.accounts != nil && len(conf.accounts.addresses) > len(tx.Message.AccountKeys) {
		return SimulateTransactionResp{}, &InvalidParamsError{
			Message: fmt.Sprintf("Too many accounts provided; max %d", len(tx.Message.AccountKeys)),
		}
	}

	// Verify signatures if requested. Runs AFTER ALT resolve (matches
	// Agave's order: sanitize → verify) so the failure response reports
	// the fully-resolved loadedAddresses, not empty arrays.
	if conf.sigVerify {
		err = txverify.VerifyTransaction(tx)
		if err != nil {
			return earlyFailResponse("SignatureFailure", replacementBlockhash, conf, tx), nil
		}
	}

	output := replay.LoadAndExecuteTransaction(replay.LoadAndExecuteTransactionInput{
		SlotCtx:                 slotCtx,
		Transaction:             tx,
		TxMeta:                  nil,
		IsSimulation:            true,
		RecordInnerInstructions: conf.innerInstructions,
	})
	preAccountSnapshots, postAccountSnapshots := simulationAccountSnapshotsForOutput(slotCtx, tx, output)
	preBalances := output.PreBalances
	if preBalances == nil {
		preBalances = balancesFromAccountSnapshots(preAccountSnapshots)
	}
	postBalances := postBalancesFromExecCtx(output.ExecCtx)
	if postBalances == nil {
		postBalances = nonExecutedSimulationPostBalances(output, preBalances)
	}

	// Default to empty slice so JSON marshals "logs":[] not null when the
	// simulator ran.
	logs := []string{}
	if output.ExecCtx != nil {
		if logRecorder, ok := output.ExecCtx.Log.(*sealevel.LogRecorder); ok && logRecorder != nil && logRecorder.Logs != nil {
			logs = logRecorder.Logs
		}
	}
	logs = clampLogs(logs)

	resp := SimulateTransactionResp{
		Context: SimulateTransactionRespContext{
			ApiVersion: "mithril 0.1",
			Slot:       global.Slot(),
		},
		Value: SimulateTransactionRespValue{
			Logs:                 ptrSlice(logs),
			ReplacementBlockhash: replacementBlockhash,
			InnerInstructions:    nil,
			PreBalances:          ptrSlice(preBalances),
			PostBalances:         ptrSlice(postBalances),
			PreTokenBalances:     ptrSliceTokenBalance(tokenBalancesFromAccounts(preAccountSnapshots, rpcServer.acctsDb, slotCtx.Slot)),
			PostTokenBalances:    ptrSliceTokenBalance(tokenBalancesFromAccounts(postAccountSnapshots, rpcServer.acctsDb, slotCtx.Slot)),
			LoadedAddresses:      loadedAddressesFromTx(tx),
		},
	}
	if processingOutputChargesFee(output) {
		fee := output.FeeInfo.TotalFee
		resp.Value.Fee = &fee
	}

	if output.ProcessingResult.TransactionError != nil {
		txErr := output.ProcessingResult.TransactionError
		resp.Value.Err = txErr

		// InstructionError / InsufficientFundsForRent reach this path AFTER
		// execution started, so execCtx carries real CU/data-size/logs/CPIs.
		// Agave drops SIMD-0290 no-ops from simulation, so its externally visible
		// result is the original fee-payer error with nil fee and zero resource
		// usage. Replay still retains the requested maxima for block accounting.
		executionRan := output.ExecCtx != nil &&
			(txErr.ErrorType == replay.TransactionErrorInstructionError ||
				txErr.ErrorType == replay.TransactionErrorInsufficientFundsForRent)

		if output.ProcessedAsNoOp {
			zeroUnits := uint64(0)
			zeroSize := uint32(0)
			resp.Value.UnitsConsumed = &zeroUnits
			resp.Value.LoadedAccountsDataSize = &zeroSize
		} else if executionRan {
			units := output.ExecCtx.ComputeMeter.Used()
			resp.Value.UnitsConsumed = &units
			dataSize := output.LoadedAccountsDataSize
			resp.Value.LoadedAccountsDataSize = &dataSize
			if logRecorder, ok := output.ExecCtx.Log.(*sealevel.LogRecorder); ok && logRecorder != nil && logRecorder.Logs != nil {
				clamped := clampLogs(logRecorder.Logs)
				resp.Value.Logs = &clamped
			}
		} else {
			zeroUnits := uint64(0)
			resp.Value.UnitsConsumed = &zeroUnits
			dataSize := failureLoadedAccountsDataSize(output)
			resp.Value.LoadedAccountsDataSize = &dataSize
		}

		if conf.innerInstructions {
			var rendered []InnerInstructionGroup
			if executionRan {
				rendered = renderInnerInstructions(replay.AssembleInnerInstructions(output.ExecCtx))
			} else {
				rendered = []InnerInstructionGroup{}
			}
			resp.Value.InnerInstructions = &rendered
		}

		// On result.is_err() the post-state isn't meaningful — return
		// a null-filled array of the requested length.
		if conf.accounts != nil {
			nullAccounts := make([]*AccountInfoPayload, len(conf.accounts.addresses))
			resp.Value.Accounts = &nullAccounts
		}
		switch txErr.ErrorType {
		case replay.TransactionErrorSanitizeFailure:
			metrics.GlobalSimulate.SanitizeFailures.Inc()
		default:
			metrics.GlobalSimulate.Errors.Inc()
		}
		return resp, nil
	}

	if output.ProcessingResult.ProcessedTransaction == nil {
		// Defensive: pure-execution returned neither error nor processed tx.
		// Agave parity: if accounts.addresses was provided, emit nulls.
		if conf.accounts != nil {
			nullAccounts := make([]*AccountInfoPayload, len(conf.accounts.addresses))
			resp.Value.Accounts = &nullAccounts
		}
		return resp, nil
	}

	processedTx := output.ProcessingResult.ProcessedTransaction

	if processedTx.TransactionType == replay.ProcessedTransactionTypeExecuted && processedTx.Executed != nil {
		executed := processedTx.Executed
		units := executed.ExecutionDetails.ExecutedUnits
		resp.Value.UnitsConsumed = &units
		dataSize := executed.LoadedTransaction.LoadedAccountsDataSize
		resp.Value.LoadedAccountsDataSize = &dataSize

		if executed.ExecutionDetails.ReturnData != nil {
			rd := executed.ExecutionDetails.ReturnData
			clamped := clampReturnData(rd.Data)
			resp.Value.ReturnData = &ReturnDataPayload{
				ProgramId: base58.Encode(rd.ProgramId[:]),
				Data:      []string{base64.StdEncoding.EncodeToString(clamped), "base64"},
			}
		}

		if conf.innerInstructions {
			rendered := renderInnerInstructions(executed.ExecutionDetails.InnerInstructions)
			resp.Value.InnerInstructions = &rendered
		}

		// Pass the structured Status through; its MarshalJSON renders the
		// tuple shape (e.g. {"InstructionError":[idx,{"Custom":N}]}). A
		// Go-stringified form would break clients.
		if executed.ExecutionDetails.Status != nil {
			resp.Value.Err = executed.ExecutionDetails.Status
			metrics.GlobalSimulate.Errors.Inc()
		} else {
			metrics.GlobalSimulate.Successes.Inc()
		}
	}

	// In-band InstructionError (Status != nil on an executed tx) is still
	// result.is_err(), so accounts must be null-filled.
	if conf.accounts != nil {
		processedTx := output.ProcessingResult.ProcessedTransaction
		execErr := processedTx != nil && processedTx.Executed != nil &&
			processedTx.Executed.ExecutionDetails.Status != nil
		if execErr {
			nullAccounts := make([]*AccountInfoPayload, len(conf.accounts.addresses))
			resp.Value.Accounts = &nullAccounts
		} else {
			resp.Value.Accounts = renderRequestedAccounts(conf, output.ExecCtx, rpcServer.acctsDb, slotCtx.Slot)
		}
	}

	return resp, nil
}

func parseSimulateConfig(params []interface{}) simulateTransactionConfig {
	conf := simulateTransactionConfig{
		encoding: "base64",
	}

	if len(params) < 2 {
		return conf
	}

	confMap, ok := params[1].(map[string]interface{})
	if !ok {
		return conf
	}

	if sigVerify, ok := confMap["sigVerify"].(bool); ok {
		conf.sigVerify = sigVerify
	}

	if replaceRecentBlockhash, ok := confMap["replaceRecentBlockhash"].(bool); ok {
		conf.replaceRecentBlockhash = replaceRecentBlockhash
	}

	if encoding, ok := confMap["encoding"].(string); ok {
		conf.encoding = encoding
	}

	if commitment, ok := confMap["commitment"].(string); ok {
		conf.commitment = commitment
	}

	if minSlot, ok := confMap["minContextSlot"].(float64); ok && minSlot >= 0 {
		v := uint64(minSlot)
		conf.minContextSlot = &v
	}

	if innerInstr, ok := confMap["innerInstructions"].(bool); ok {
		conf.innerInstructions = innerInstr
	}

	if accountsObj, ok := confMap["accounts"].(map[string]interface{}); ok {
		acctConf := &simulateAccountsConfig{}
		if addresses, ok := accountsObj["addresses"].([]interface{}); ok {
			for _, addr := range addresses {
				if addrStr, ok := addr.(string); ok {
					acctConf.addresses = append(acctConf.addresses, addrStr)
				}
			}
		}
		if encoding, ok := accountsObj["encoding"].(string); ok {
			acctConf.encoding = encoding
		}
		conf.accounts = acctConf
	}

	return conf
}

// postBalancesFromExecCtx returns the lamport balance of every account key
// in the transaction's order after execution. Returns nil when the
// execution context is unavailable (e.g. early sanitize/load failure).
func postBalancesFromExecCtx(execCtx *sealevel.ExecutionCtx) []uint64 {
	if execCtx == nil {
		return nil
	}
	accts := execCtx.TransactionContext.Accounts.Accounts
	out := make([]uint64, len(accts))
	for i, acct := range accts {
		out[i] = acct.Lamports
	}
	return out
}

// simulationAccountSnapshots mirrors Agave's balance collector around
// transaction processing. It runs only when an early return prevented the
// execution path from loading accounts; absent accounts remain nil entries.
func simulationAccountSnapshots(slotCtx *sealevel.SlotCtx, tx *solana.Transaction) []*accounts.Account {
	if slotCtx == nil || tx == nil {
		return nil
	}
	snapshots := make([]*accounts.Account, len(tx.Message.AccountKeys))
	for i, key := range tx.Message.AccountKeys {
		acct, err := slotCtx.GetAccount(key)
		if err != nil && (slotCtx.UnrootedRead != nil || slotCtx.AccountsDb != nil) {
			acct, err = slotCtx.GetAccountFromAccountsDb(key)
		}
		if err == nil && acct != nil {
			snapshots[i] = acct
		}
	}
	return snapshots
}

func balancesFromAccountSnapshots(snapshots []*accounts.Account) []uint64 {
	if snapshots == nil {
		return nil
	}
	balances := make([]uint64, len(snapshots))
	for i, acct := range snapshots {
		if acct != nil {
			balances[i] = acct.Lamports
		}
	}
	return balances
}

// simulationAccountSnapshotsForOutput selects the same account views Agave's
// balance collector exposes around processing. Early no-op, fee-payer, and
// fees-only results have no ExecutionCtx, but their token accounts are still
// observed and remain unchanged.
func simulationAccountSnapshotsForOutput(
	slotCtx *sealevel.SlotCtx,
	tx *solana.Transaction,
	output replay.LoadAndExecuteTransactionOutput,
) (pre []*accounts.Account, post []*accounts.Account) {
	pre = output.PreAccountSnapshots
	if pre == nil {
		pre = simulationAccountSnapshots(slotCtx, tx)
	}
	post = postExecAccounts(output.ExecCtx)
	if post == nil {
		post = pre
	}
	return pre, post
}

// processingOutputChargesFee reports whether Agave represents this result as
// a committed Executed or FeesOnly transaction. Validation failures (including
// dropped SIMD-0290 no-ops) expose a null fee even if calculation completed.
func processingOutputChargesFee(output replay.LoadAndExecuteTransactionOutput) bool {
	if output.FeeInfo == nil || output.ProcessedAsNoOp {
		return false
	}
	if output.ProcessingResult.TransactionError == nil || output.ExecCtx != nil {
		return true
	}
	switch output.ProcessingResult.TransactionError.ErrorType {
	case replay.TransactionErrorMaxLoadedAccountsDataSizeExceeded,
		replay.TransactionErrorInvalidProgramForExecution,
		replay.TransactionErrorProgramAccountNotFound:
		return true
	default:
		return false
	}
}

// nonExecutedSimulationPostBalances derives the observable rollback state for
// results that never produced an ExecutionCtx. No-ops and unprocessable
// failures leave balances unchanged. Processable fees-only load failures debit
// the payer; advancing a durable nonce does not change any lamport balance.
func nonExecutedSimulationPostBalances(output replay.LoadAndExecuteTransactionOutput, pre []uint64) []uint64 {
	post := append([]uint64(nil), pre...)
	if output.ProcessedAsNoOp || output.FeeInfo == nil || output.ProcessingResult.TransactionError == nil || len(post) == 0 {
		return post
	}
	switch output.ProcessingResult.TransactionError.ErrorType {
	case replay.TransactionErrorMaxLoadedAccountsDataSizeExceeded,
		replay.TransactionErrorInvalidProgramForExecution,
		replay.TransactionErrorProgramAccountNotFound:
		if post[0] >= output.FeeInfo.TotalFee {
			post[0] -= output.FeeInfo.TotalFee
		}
	}
	return post
}

func failureLoadedAccountsDataSize(output replay.LoadAndExecuteTransactionOutput) uint32 {
	if output.ExecCtx != nil {
		return output.LoadedAccountsDataSize
	}
	if output.ProcessingResult.TransactionError == nil {
		return 0
	}
	switch output.ProcessingResult.TransactionError.ErrorType {
	case replay.TransactionErrorMaxLoadedAccountsDataSizeExceeded,
		replay.TransactionErrorInvalidProgramForExecution,
		replay.TransactionErrorProgramAccountNotFound:
		// The loader has already selected the feature-correct value: rollback
		// account data before the amendment, or the partial SIMD-0186
		// accumulator after it.
		return output.LoadedAccountsDataSize
	default:
		return 0
	}
}

// postExecAccounts returns the live post-execution transaction accounts
// (or nil when the execution context never came up). Used to feed the
// token-balance extractor without requiring callers to pierce the
// execCtx struct themselves.
func postExecAccounts(execCtx *sealevel.ExecutionCtx) []*accounts.Account {
	if execCtx == nil {
		return nil
	}
	return execCtx.TransactionContext.Accounts.Accounts
}

// Agave-parity limits applied to simulate response payloads. Values
// match `solana_runtime::bank::TransactionSimulationResult` clamps so
// large outputs don't bloat RPC responses.
const (
	maxSimulateLogBytes        = 10 * 1024
	maxSimulateReturnDataBytes = 1024
)

// clampLogs truncates a log slice so the total byte size of all
// concatenated log lines stays under maxSimulateLogBytes. When the cap
// is hit, a synthetic "Log truncated" line is appended (matching Agave).
func clampLogs(logs []string) []string {
	if logs == nil {
		return logs
	}
	total := 0
	for i, l := range logs {
		total += len(l)
		if total > maxSimulateLogBytes {
			out := make([]string, 0, i+1)
			out = append(out, logs[:i]...)
			out = append(out, "Log truncated")
			return out
		}
	}
	return logs
}

// clampReturnData truncates raw return-data bytes to the spec limit.
// Programs may emit larger blobs at run time, but the wire format never
// exceeds 1024 bytes per Agave's TransactionReturnData.
func clampReturnData(data []byte) []byte {
	if len(data) <= maxSimulateReturnDataBytes {
		return data
	}
	out := make([]byte, maxSimulateReturnDataBytes)
	copy(out, data[:maxSimulateReturnDataBytes])
	return out
}

// earlyFailResponse builds a response for paths that fail before the
// simulator runs (sigVerify failure, ALT not found). Matches Agave's
// SignatureFailure fixture: every required field present, balances null,
// logs and loadedAddresses populated as empty containers, innerInstructions
// as [] when caller asked for it and null otherwise.
//
// `tx` is consulted only for loadedAddresses: when ALT resolution has
// completed (sigVerify-fail path), the resolved writable/readonly arrays
// are emitted. For pre-ALT failures (ALT-not-found), tx may be nil or
// not-yet-resolved, in which case loadedAddresses is the empty payload.
func earlyFailResponse(errStr string, replacementBlockhash *ReplacementBlockhashPayload, conf simulateTransactionConfig, tx *solana.Transaction) SimulateTransactionResp {
	zeroUnits := uint64(0)
	zeroSize := uint32(0)
	emptyLogs := []string{}
	loaded := &LoadedAddressesPayload{Readonly: []string{}, Writable: []string{}}
	if tx != nil {
		loaded = loadedAddressesFromTx(tx)
	}
	resp := SimulateTransactionResp{
		Context: SimulateTransactionRespContext{
			ApiVersion: "mithril 0.1",
			Slot:       global.Slot(),
		},
		Value: SimulateTransactionRespValue{
			Err:                    errStr,
			Logs:                   &emptyLogs,
			UnitsConsumed:          &zeroUnits,
			LoadedAccountsDataSize: &zeroSize,
			ReplacementBlockhash:   replacementBlockhash,
			LoadedAddresses:        loaded,
		},
	}
	if conf.innerInstructions {
		empty := []InnerInstructionGroup{}
		resp.Value.InnerInstructions = &empty
	}
	if conf.accounts != nil {
		nullAccounts := make([]*AccountInfoPayload, len(conf.accounts.addresses))
		resp.Value.Accounts = &nullAccounts
	}
	return resp
}

// ptrSlice returns a pointer to its argument. Lifts a slice into the
// Some/None shape Agave uses on the wire (nil → JSON null, non-nil → JSON array).
func ptrSlice[T any](s []T) *[]T {
	if s == nil {
		return nil
	}
	return &s
}

func ptrSliceTokenBalance(s []TokenBalancePayload) *[]TokenBalancePayload {
	if s == nil {
		return nil
	}
	return &s
}

// renderRequestedAccounts builds the response slice for `accounts.addresses`
// in caller-specified order. Lookup precedence:
//  1. post-execution transaction context (most up-to-date for tx accounts)
//  2. accountsdb fallback (for addresses NOT touched by the tx)
//
// Each entry is nil (JSON null) when the address can't be resolved.
// Always returns a slice of length len(conf.accounts.addresses), matching
// Agave so clients can index by request order.
func renderRequestedAccounts(conf simulateTransactionConfig, execCtx *sealevel.ExecutionCtx, db *accountsdb.AccountsDb, slot uint64) *[]*AccountInfoPayload {
	accts := make([]*AccountInfoPayload, len(conf.accounts.addresses))
	encodingType := GetAccountEncodingBase64
	if conf.accounts.encoding != "" {
		encodingType, _ = parseGetAcctDataEncodingType(conf.accounts.encoding)
	}
	acctConf := &GetAccountInfoConfig{EncodingType: &encodingType}

	for i, addrStr := range conf.accounts.addresses {
		pk, err := solana.PublicKeyFromBase58(addrStr)
		if err != nil {
			continue
		}

		var resolved *accounts.Account
		if execCtx != nil {
			for idx, acct := range execCtx.TransactionContext.Accounts.Accounts {
				if acct.Key == pk {
					resolved = execCtx.TransactionContext.Accounts.Accounts[idx]
					break
				}
			}
		}
		if resolved == nil && db != nil {
			if dbAcct, dbErr := db.GetAccount(slot, pk); dbErr == nil {
				resolved = dbAcct
			}
		}
		if resolved == nil {
			continue
		}

		encodedData, encErr := encodeAcctDataWithConfig(resolved.Data, acctConf)
		if encErr != nil {
			continue
		}
		accts[i] = &AccountInfoPayload{
			Data:       encodedData,
			Executable: resolved.Executable,
			Lamports:   resolved.Lamports,
			Owner:      base58.Encode(resolved.Owner[:]),
			RentEpoch:  resolved.RentEpoch,
			Space:      len(resolved.Data),
		}
	}
	return &accts
}

// renderInnerInstructions converts replay's inner-instruction shape into
// the typed payload clients expect. Field encoding matches Agave:
//   - data: base58 (not base64) — what UiCompiledInstruction emits
//   - stackHeight: nil when not tracked, *uint8 when known
func renderInnerInstructions(lists []replay.InnerInstructionsList) []InnerInstructionGroup {
	if len(lists) == 0 {
		return []InnerInstructionGroup{}
	}
	out := make([]InnerInstructionGroup, 0, len(lists))
	for _, l := range lists {
		instrs := make([]InnerInstruction, 0, len(l.Instructions))
		for _, ix := range l.Instructions {
			accts := make([]uint8, len(ix.Accounts))
			copy(accts, ix.Accounts)
			entry := InnerInstruction{
				ProgramIdIndex: ix.ProgramIdIndex,
				Accounts:       accts,
				Data:           base58.Encode(ix.Data),
			}
			if ix.StackHeight > 0 {
				h := uint32(ix.StackHeight)
				entry.StackHeight = &h
			}
			instrs = append(instrs, entry)
		}
		out = append(out, InnerInstructionGroup{
			Index:        l.Index,
			Instructions: instrs,
		})
	}
	return out
}

// loadedAddressesFromTx splits the ALT-resolved keys into writable and
// readonly groups using solana-go's post-resolve ordering:
//
//	[static] + [writable from each lookup, in order] + [readonly from each lookup, in order]
//
// Returns an empty payload (rather than nil) for legacy or no-lookup txs
// to keep the response shape stable for clients.
func loadedAddressesFromTx(tx *solana.Transaction) *LoadedAddressesPayload {
	out := &LoadedAddressesPayload{Readonly: []string{}, Writable: []string{}}
	if tx.Message.GetVersion() != solana.MessageVersionV0 || tx.Message.AddressTableLookups.NumLookups() == 0 {
		return out
	}

	totalLookups := tx.Message.AddressTableLookups.NumLookups()
	writableCount := tx.Message.AddressTableLookups.NumWritableLookups()
	if len(tx.Message.AccountKeys) < totalLookups {
		return out // not yet resolved; return empty rather than panic
	}

	staticEnd := len(tx.Message.AccountKeys) - totalLookups
	writableEnd := staticEnd + writableCount

	for i := staticEnd; i < writableEnd; i++ {
		out.Writable = append(out.Writable, tx.Message.AccountKeys[i].String())
	}
	for i := writableEnd; i < len(tx.Message.AccountKeys); i++ {
		out.Readonly = append(out.Readonly, tx.Message.AccountKeys[i].String())
	}
	return out
}
