package mcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/DataDog/zstd"
	mithrilbase58 "github.com/Overclock-Validator/mithril/pkg/base58"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	mrbase58 "github.com/mr-tron/base58"
)

// maxRPCResponseBytes caps buffered JSON-RPC responses at 8 MB to limit memory
// use if the node returns an oversized body.
const maxRPCResponseBytes = 8 * 1024 * 1024

const (
	defaultAccountDataBytes   = 4 * 1024
	maxAccountDataBytes       = 64 * 1024
	maxAccountSpaceBytes      = 10 * 1024 * 1024
	maxEncodedAccountChars    = 128 * 1024
	maxCompressedDataBytes    = maxAccountDataBytes + 4*1024
	maxRPCAPIVersionBytes     = 128
	maxJSONExactInteger       = 1<<53 - 1
	maxTransactionBytes       = 1232
	maxBase58TransactionChars = 1683
	maxBase64TransactionChars = 1644
)

// mithrilRPCClient is a minimal Solana JSON-RPC client for the local node.
type mithrilRPCClient struct {
	endpoint string
}

func newMithrilRPCClient(endpoint string) (*mithrilRPCClient, error) {
	if _, err := validateURL(endpoint); err != nil {
		return nil, err
	}
	return &mithrilRPCClient{endpoint: endpoint}, nil
}

type jsonRPCError struct {
	Code    *int    `json:"code"`
	Message *string `json:"message"`
}

type rpcHTTPStatusError struct {
	code int
}

func (e *rpcHTTPStatusError) Error() string {
	return fmt.Sprintf("unexpected HTTP status %d", e.code)
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   json.RawMessage `json:"error"`
}

// call issues a JSON-RPC request and returns the raw result. The endpoint is
// validated again and its safe DNS answer is pinned to the connection.
func (c *mithrilRPCClient) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	u, err := validateURL(c.endpoint)
	if err != nil {
		return nil, err
	}
	reqBody, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": method, "params": params,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(reqBody))
	if err != nil {
		return nil, sanitizeHTTPError(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doPinnedRequest(ctx, req, outboundHTTPTimeout)
	if err != nil {
		return nil, sanitizeHTTPError(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &rpcHTTPStatusError{code: resp.StatusCode}
	}
	body, err := readCappedBody(resp, maxRPCResponseBytes)
	if err != nil {
		return nil, err
	}

	var r jsonRPCResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, errors.New("failed to parse RPC response")
	}
	if r.JSONRPC != "2.0" {
		return nil, errors.New("RPC response has invalid or missing jsonrpc version")
	}
	var responseID int64
	if len(r.ID) == 0 || json.Unmarshal(r.ID, &responseID) != nil || responseID != 1 {
		return nil, errors.New("RPC response id does not match request id 1")
	}
	resultPresent := len(r.Result) != 0
	errorPresent := len(r.Error) != 0
	if resultPresent == errorPresent {
		return nil, errors.New("RPC response must contain exactly one of result or error")
	}
	if errorPresent {
		var rpcErr jsonRPCError
		if bytes.Equal(bytes.TrimSpace(r.Error), []byte("null")) || json.Unmarshal(r.Error, &rpcErr) != nil || rpcErr.Code == nil || rpcErr.Message == nil {
			return nil, errors.New("RPC response contains an invalid error object")
		}
		// Bound strings returned by a compromised or faulty node.
		msg := redactUntrustedText(*rpcErr.Message)
		if rs := []rune(msg); len(rs) > 256 {
			msg = string(rs[:256])
		}
		return nil, fmt.Errorf("RPC error %d: %s", *rpcErr.Code, msg)
	}
	if bytes.Equal(bytes.TrimSpace(r.Result), []byte("null")) {
		return nil, errors.New("RPC returned null result")
	}
	return r.Result, nil
}

// epochInfoRPC matches the node's getEpochInfo response.
type epochInfoRPC struct {
	BlockHeight      *uint64 `json:"blockHeight"`
	Epoch            *uint64 `json:"epoch"`
	SlotIndex        *uint64 `json:"slotIndex"`
	SlotsInEpoch     *uint64 `json:"slotsInEpoch"`
	AbsoluteSlot     *uint64 `json:"absoluteSlot"`
	TransactionCount *uint64 `json:"transactionCount"`
}

// SlotInfo is the tool-facing slot and epoch summary.
type SlotInfo struct {
	BlockHeight      uint64 `json:"block_height"`
	Epoch            uint64 `json:"epoch"`
	SlotIndex        uint64 `json:"slot_index"`
	SlotsInEpoch     uint64 `json:"slots_in_epoch"`
	AbsoluteSlot     uint64 `json:"absolute_slot"`
	TransactionCount uint64 `json:"transaction_count"`
	Consistency      string `json:"consistency"`
	Finality         string `json:"finality"`
}

func (c *mithrilRPCClient) getSlotInfo(ctx context.Context) (SlotInfo, error) {
	raw, err := c.call(ctx, "getEpochInfo", []any{})
	if err != nil {
		return SlotInfo{}, err
	}
	var e epochInfoRPC
	if err := json.Unmarshal(raw, &e); err != nil {
		return SlotInfo{}, errors.New("failed to parse getEpochInfo response")
	}
	if e.BlockHeight == nil || e.Epoch == nil || e.SlotIndex == nil || e.SlotsInEpoch == nil || e.AbsoluteSlot == nil || e.TransactionCount == nil {
		return SlotInfo{}, errors.New("getEpochInfo response is missing required fields")
	}
	return SlotInfo{
		BlockHeight:      *e.BlockHeight,
		Epoch:            *e.Epoch,
		SlotIndex:        *e.SlotIndex,
		SlotsInEpoch:     *e.SlotsInEpoch,
		AbsoluteSlot:     *e.AbsoluteSlot,
		TransactionCount: *e.TransactionCount,
		Consistency:      "node_reported_non_atomic",
		Finality:         "local_unfinalized",
	}, nil
}

// getBankHash fetches the bank hash for a slot. Mithril's getBankHash REQUIRES a
// slot (no "latest" form), so callers resolve the slot first.
func (c *mithrilRPCClient) getBankHash(ctx context.Context, slot uint64) (string, error) {
	raw, err := c.call(ctx, "getBankHash", []any{slot})
	if err != nil {
		return "", err
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", errors.New("getBankHash did not return a string")
	}
	if err := validateHash(s); err != nil {
		return "", fmt.Errorf("getBankHash returned invalid bank hash: %w", err)
	}
	return s, nil
}

// BlockhashInfo is the tool-facing latest-blockhash summary.
type BlockhashInfo struct {
	Slot                 uint64  `json:"slot"`
	Blockhash            string  `json:"blockhash,omitempty"`
	LastValidBlockHeight *uint64 `json:"last_valid_block_height,omitempty"`
	Status               string  `json:"status"`
	Consistency          string  `json:"consistency"`
	Finality             string  `json:"finality"`
}

// latestBlockhashRPC parses getLatestBlockhash's nested context/value shape.
type latestBlockhashRPC struct {
	Context *struct {
		Slot *uint64 `json:"slot"`
	} `json:"context"`
	Value *struct {
		Blockhash            string  `json:"blockhash"`
		LastValidBlockHeight *uint64 `json:"lastValidBlockHeight"`
	} `json:"value"`
}

func (c *mithrilRPCClient) getLatestBlockhash(ctx context.Context) (BlockhashInfo, error) {
	raw, err := c.call(ctx, "getLatestBlockhash", []any{})
	if err != nil {
		return BlockhashInfo{}, err
	}
	var r latestBlockhashRPC
	if err := json.Unmarshal(raw, &r); err != nil {
		return BlockhashInfo{}, errors.New("failed to parse getLatestBlockhash response")
	}
	if r.Context == nil || r.Value == nil {
		return BlockhashInfo{}, errors.New("getLatestBlockhash response is missing context or value")
	}
	if r.Context.Slot == nil || r.Value.LastValidBlockHeight == nil {
		return BlockhashInfo{}, errors.New("getLatestBlockhash response is missing required numeric fields")
	}
	decoded, err := decodeHash(r.Value.Blockhash)
	if err != nil {
		return BlockhashInfo{}, fmt.Errorf("getLatestBlockhash returned invalid blockhash: %w", err)
	}
	lastValidBlockHeight := *r.Value.LastValidBlockHeight
	out := BlockhashInfo{
		Slot:                 *r.Context.Slot,
		Blockhash:            r.Value.Blockhash,
		LastValidBlockHeight: &lastValidBlockHeight,
		Status:               "ready",
		Consistency:          "node_reported_non_atomic",
		Finality:             "local_unfinalized",
	}
	if decoded == ([32]byte{}) {
		out.Blockhash = ""
		out.LastValidBlockHeight = nil
		out.Status = "not_ready"
	}
	return out, nil
}

func (c *mithrilRPCClient) getBlockHeight(ctx context.Context) (uint64, error) {
	raw, err := c.call(ctx, "getBlockHeight", []any{})
	if err != nil {
		return 0, err
	}
	var h uint64
	if err := json.Unmarshal(raw, &h); err != nil {
		return 0, errors.New("getBlockHeight did not return a number")
	}
	return h, nil
}

type simulationConfig struct {
	Encoding               string
	SigVerify              bool
	ReplaceRecentBlockhash bool
	MinContextSlot         *uint64
}

func (cfg simulationConfig) toJSON() map[string]any {
	out := map[string]any{
		"encoding":               cfg.Encoding,
		"sigVerify":              cfg.SigVerify,
		"replaceRecentBlockhash": cfg.ReplaceRecentBlockhash,
	}
	if cfg.MinContextSlot != nil {
		out["minContextSlot"] = *cfg.MinContextSlot
	}
	return out
}

func (c *mithrilRPCClient) simulateTransaction(ctx context.Context, transaction string, cfg simulationConfig) (json.RawMessage, error) {
	if err := validateEncodedTransaction(transaction, cfg.Encoding); err != nil {
		return nil, err
	}
	return c.call(ctx, "simulateTransaction", []any{transaction, cfg.toJSON()})
}

func validateEncodedTransaction(transaction, encoding string) error {
	limit := maxBase64TransactionChars
	if encoding == "base58" {
		limit = maxBase58TransactionChars
	}
	if len(transaction) > limit {
		return fmt.Errorf("transaction exceeds %d-character %s limit", limit, encoding)
	}
	if transaction == "" {
		return errors.New("transaction must not be empty")
	}
	var (
		decoded []byte
		err     error
	)
	switch encoding {
	case "base64":
		decoded, err = base64.StdEncoding.DecodeString(transaction)
	case "base58":
		decoded, err = mrbase58.Decode(transaction)
	default:
		return errors.New("encoding must be base64 or base58")
	}
	if err != nil {
		return fmt.Errorf("transaction is not valid %s", encoding)
	}
	if len(decoded) > maxTransactionBytes {
		return fmt.Errorf("decoded transaction exceeds %d-byte limit", maxTransactionBytes)
	}
	return nil
}

func validateSimulationResult(raw json.RawMessage) error {
	var result map[string]json.RawMessage
	if err := json.Unmarshal(raw, &result); err != nil || result == nil {
		return errors.New("simulateTransaction returned an invalid result object")
	}
	contextRaw, contextOK := result["context"]
	valueRaw, valueOK := result["value"]
	if !contextOK || !valueOK {
		return errors.New("simulateTransaction result is missing context or value")
	}
	var contextObject map[string]json.RawMessage
	if err := json.Unmarshal(contextRaw, &contextObject); err != nil || contextObject == nil {
		return errors.New("simulateTransaction result has invalid context")
	}
	slotRaw, ok := contextObject["slot"]
	var slot uint64
	if !ok || json.Unmarshal(slotRaw, &slot) != nil {
		return errors.New("simulateTransaction result has invalid or missing context.slot")
	}
	var valueObject map[string]json.RawMessage
	if err := json.Unmarshal(valueRaw, &valueObject); err != nil || valueObject == nil {
		return errors.New("simulateTransaction result has invalid value")
	}
	if _, ok := valueObject["err"]; !ok {
		return errors.New("simulateTransaction result is missing value.err")
	}
	return nil
}

func validateHash(value string) error {
	decoded, err := decodeHash(value)
	if err != nil {
		return err
	}
	if decoded == ([32]byte{}) {
		return errors.New("all-zero hash is not valid")
	}
	return nil
}

func decodeHash(value string) ([32]byte, error) {
	if len(value) < 32 || len(value) > 44 {
		return [32]byte{}, errors.New("expected a base58-encoded 32-byte value")
	}
	decoded, err := mithrilbase58.DecodeFromString(value)
	if err != nil {
		return [32]byte{}, errors.New("expected a base58-encoded 32-byte value")
	}
	return decoded, nil
}

func validatePubkey(value string) error {
	if len(value) < 32 || len(value) > 44 {
		return errors.New("expected a base58-encoded 32-byte public key")
	}
	if _, err := mithrilbase58.DecodeFromString(value); err != nil {
		return errors.New("expected a base58-encoded 32-byte public key")
	}
	return nil
}

type accountDataSlice struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

type accountContextRPC struct {
	APIVersion string  `json:"apiVersion,omitempty"`
	Slot       *uint64 `json:"slot"`
}

type accountValueRPC struct {
	Data       json.RawMessage `json:"data"`
	Executable bool            `json:"executable"`
	Lamports   *uint64         `json:"lamports"`
	Owner      string          `json:"owner"`
	RentEpoch  *uint64         `json:"rentEpoch"`
	Space      *uint64         `json:"space"`
}

type accountInfoRPC struct {
	Context *accountContextRPC `json:"context"`
	Value   *accountValueRPC   `json:"value"`
}

func (c *mithrilRPCClient) getAccountInfo(ctx context.Context, pubkey, encoding string, dataSlice accountDataSlice) (accountInfoRPC, error) {
	cfg := map[string]any{"dataSlice": dataSlice}
	if encoding != "" {
		cfg["encoding"] = encoding
	}
	raw, err := c.call(ctx, "getAccountInfo", []any{pubkey, cfg})
	if err != nil {
		return accountInfoRPC{}, err
	}
	var result accountInfoRPC
	if err := json.Unmarshal(raw, &result); err != nil {
		return accountInfoRPC{}, errors.New("failed to parse getAccountInfo response")
	}
	if result.Context == nil {
		return accountInfoRPC{}, errors.New("getAccountInfo response is missing context")
	}
	if result.Context.Slot == nil {
		return accountInfoRPC{}, errors.New("getAccountInfo response context is missing slot")
	}
	result.Context.APIVersion, _ = truncateUTF8Bytes(redactUntrustedText(result.Context.APIVersion), maxRPCAPIVersionBytes)
	if result.Value != nil && (result.Value.Lamports == nil || result.Value.RentEpoch == nil || result.Value.Space == nil) {
		return accountInfoRPC{}, errors.New("getAccountInfo response value is missing required numeric fields")
	}
	return result, nil
}

func validateAccountData(encoded, encoding string, expectedLength uint64) error {
	if len(encoded) > maxEncodedAccountChars {
		return fmt.Errorf("encoded account data exceeds %d-character limit", maxEncodedAccountChars)
	}
	var decoded []byte
	var err error
	switch encoding {
	case "base58":
		decoded, err = mrbase58.Decode(encoded)
	case "base64":
		decoded, err = base64.StdEncoding.DecodeString(encoded)
	case "base64+zstd":
		var compressed []byte
		compressed, err = base64.StdEncoding.DecodeString(encoded)
		if err == nil && len(compressed) > maxCompressedDataBytes {
			return fmt.Errorf("compressed account data exceeds %d-byte limit", maxCompressedDataBytes)
		}
		if err == nil {
			// DecompressInto cannot grow this destination. The +1 detects a node
			// returning more bytes than the requested slice without risking a
			// decompression-bomb allocation.
			destination := make([]byte, int(expectedLength)+1)
			var n int
			n, err = zstd.DecompressInto(destination, compressed)
			if err == nil {
				decoded = destination[:n]
			}
		}
	default:
		return errors.New("unsupported account data encoding")
	}
	if err != nil {
		return fmt.Errorf("account data is not valid %s", encoding)
	}
	if uint64(len(decoded)) != expectedLength {
		return fmt.Errorf("decoded account data length is %d, want %d", len(decoded), expectedLength)
	}
	return nil
}

type bankHashInput struct {
	Slot uint64 `json:"slot" jsonschema:"exact replayed slot to query; required because the current in-progress slot may not have a persisted bank hash yet"`
}

type bankHashOutput struct {
	Slot     uint64 `json:"slot"`
	BankHash string `json:"bank_hash"`
}

type blockHeightOutput struct {
	BlockHeight uint64 `json:"block_height"`
	Finality    string `json:"finality"`
}

type rpcEmptyInput struct{}

type accountInfoInput struct {
	Pubkey     string  `json:"pubkey" jsonschema:"base58-encoded 32-byte account public key"`
	Encoding   string  `json:"encoding,omitempty" jsonschema:"base58, base64, or base64+zstd (default base64)"`
	DataOffset uint64  `json:"data_offset,omitempty" jsonschema:"account-data byte offset (default 0)"`
	DataLength *uint64 `json:"data_length,omitempty" jsonschema:"account-data bytes to return (default 4096, max 65536; 0 returns metadata only)"`
}

type simulateTransactionInput struct {
	Transaction            string  `json:"transaction" jsonschema:"wire-format transaction encoded as base64 (default) or base58"`
	Encoding               string  `json:"encoding,omitempty" jsonschema:"base64 (default) or base58"`
	SigVerify              bool    `json:"sig_verify,omitempty" jsonschema:"verify transaction signatures (default false)"`
	ReplaceRecentBlockhash *bool   `json:"replace_recent_blockhash,omitempty" jsonschema:"replace the transaction blockhash with Mithril's latest blockhash (default true)"`
	MinContextSlot         *uint64 `json:"min_context_slot,omitempty" jsonschema:"fail if Mithril has not reached this slot"`
}

type accountValueOutput struct {
	Data          [2]string `json:"data"`
	Executable    bool      `json:"executable"`
	Lamports      string    `json:"lamports"`
	Owner         string    `json:"owner"`
	RentEpoch     string    `json:"rent_epoch"`
	Space         uint64    `json:"space"`
	DataOffset    uint64    `json:"data_offset"`
	DataLength    uint64    `json:"data_length"`
	DataTruncated bool      `json:"data_truncated"`
}

type accountContextOutput struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Slot       uint64 `json:"slot"`
}

type accountInfoOutput struct {
	Found       bool                 `json:"found"`
	Context     accountContextOutput `json:"context"`
	Value       *accountValueOutput  `json:"value,omitempty"`
	Finality    string               `json:"finality"`
	Consistency string               `json:"consistency"`
}

func registerRPCTools(server *mcpsdk.Server, cfg Config) {
	rpcURL := cfg.RPCURL
	safeRPCURL := sanitizeURLForDisplay(rpcURL)
	client, clientErr := newMithrilRPCClient(rpcURL)
	simulationGate := make(chan struct{}, 1)

	addTool(server, cfg, &mcpsdk.Tool{
		Name:        "mithril_get_slot_info",
		Annotations: annReadOnlyNetwork,
		Description: "Read Mithril's current slot, epoch, block height, and transaction count. Values are local, unfinalized, and not atomic.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, _ rpcEmptyInput) (*mcpsdk.CallToolResult, SlotInfo, error) {
		if clientErr != nil {
			return nil, SlotInfo{}, fmt.Errorf("RPC client init for %s failed: %w", safeRPCURL, clientErr)
		}
		info, err := client.getSlotInfo(ctx)
		if err != nil {
			return nil, SlotInfo{}, fmt.Errorf("RPC getEpochInfo via %s failed: %w", safeRPCURL, err)
		}
		return nil, info, nil
	})

	addTool(server, cfg, &mcpsdk.Tool{
		Name:        "mithril_get_bank_hash",
		Annotations: annReadOnlyNetwork,
		Description: "Read the persisted bank hash for an explicit replayed slot. The in-progress slot may not be persisted yet; a mismatch requires operator review.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, in bankHashInput) (*mcpsdk.CallToolResult, bankHashOutput, error) {
		if in.Slot > maxJSONExactInteger {
			return nil, bankHashOutput{}, fmt.Errorf("slot exceeds Mithril RPC's exact-integer limit %d", maxJSONExactInteger)
		}
		if clientErr != nil {
			return nil, bankHashOutput{}, fmt.Errorf("RPC client init for %s failed: %w", safeRPCURL, clientErr)
		}
		hash, err := client.getBankHash(ctx, in.Slot)
		if err != nil {
			return nil, bankHashOutput{}, fmt.Errorf("RPC getBankHash via %s failed: %w", safeRPCURL, err)
		}
		return nil, bankHashOutput{Slot: in.Slot, BankHash: hash}, nil
	})

	addTool(server, cfg, &mcpsdk.Tool{
		Name:        "mithril_get_latest_blockhash",
		Annotations: annReadOnlyNetwork,
		Description: "Read Mithril's latest blockhash and last-valid block height. status=not_ready means replay has not initialized either value yet. Values are local, unfinalized, and not atomic.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, _ rpcEmptyInput) (*mcpsdk.CallToolResult, BlockhashInfo, error) {
		if clientErr != nil {
			return nil, BlockhashInfo{}, fmt.Errorf("RPC client init for %s failed: %w", safeRPCURL, clientErr)
		}
		bh, err := client.getLatestBlockhash(ctx)
		if err != nil {
			return nil, BlockhashInfo{}, fmt.Errorf("RPC getLatestBlockhash via %s failed: %w", safeRPCURL, err)
		}
		return nil, bh, nil
	})

	addTool(server, cfg, &mcpsdk.Tool{
		Name:        "mithril_get_block_height",
		Annotations: annReadOnlyNetwork,
		Description: "Read Mithril's current block height. It counts non-skipped slots, differs from the slot number, and reflects local unfinalized state.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, _ rpcEmptyInput) (*mcpsdk.CallToolResult, blockHeightOutput, error) {
		if clientErr != nil {
			return nil, blockHeightOutput{}, fmt.Errorf("RPC client init for %s failed: %w", safeRPCURL, clientErr)
		}
		h, err := client.getBlockHeight(ctx)
		if err != nil {
			return nil, blockHeightOutput{}, fmt.Errorf("RPC getBlockHeight via %s failed: %w", safeRPCURL, err)
		}
		return nil, blockHeightOutput{BlockHeight: h, Finality: "local_unfinalized"}, nil
	})

	addTool(server, cfg, &mcpsdk.Tool{
		Name:         "mithril_simulate_transaction",
		Annotations:  annRuntimeDiagnostic,
		OutputSchema: dynamicObjectOutputSchema,
		Description:  "Diagnostic profile only. Simulate a bounded transaction against Mithril's local unfinalized bank. It is never broadcast or persisted, but it executes node code and consumes runtime resources.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, in simulateTransactionInput) (*mcpsdk.CallToolResult, any, error) {
		encoding := in.Encoding
		if encoding == "" {
			encoding = "base64"
		}
		if encoding != "base64" && encoding != "base58" {
			return nil, nil, errors.New("encoding must be base64 or base58")
		}
		if in.MinContextSlot != nil && *in.MinContextSlot > maxJSONExactInteger {
			return nil, nil, fmt.Errorf("min_context_slot exceeds Mithril RPC's exact-integer limit %d", maxJSONExactInteger)
		}
		replaceRecentBlockhash := true
		if in.ReplaceRecentBlockhash != nil {
			replaceRecentBlockhash = *in.ReplaceRecentBlockhash
		}
		if in.SigVerify && replaceRecentBlockhash {
			return nil, nil, errors.New("sig_verify cannot be combined with replace_recent_blockhash")
		}
		if err := validateEncodedTransaction(in.Transaction, encoding); err != nil {
			return nil, nil, err
		}
		if clientErr != nil {
			return nil, nil, fmt.Errorf("RPC client init for %s failed: %w", safeRPCURL, clientErr)
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		select {
		case simulationGate <- struct{}{}:
			defer func() { <-simulationGate }()
		default:
			return nil, nil, errors.New("another transaction simulation is already running")
		}
		raw, err := client.simulateTransaction(ctx, in.Transaction, simulationConfig{
			Encoding:               encoding,
			SigVerify:              in.SigVerify,
			ReplaceRecentBlockhash: replaceRecentBlockhash,
			MinContextSlot:         in.MinContextSlot,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("RPC simulateTransaction via %s failed: %w", safeRPCURL, err)
		}
		if err := validateSimulationResult(raw); err != nil {
			return nil, nil, err
		}
		raw = redactRawJSON(raw)
		raw = json.RawMessage(bytes.Clone(raw))
		return &mcpsdk.CallToolResult{
			Content:           []mcpsdk.Content{&mcpsdk.TextContent{Text: string(raw)}},
			StructuredContent: raw,
		}, nil, nil
	})

	addTool(server, cfg, &mcpsdk.Tool{
		Name:        "mithril_get_account_info",
		Annotations: annRuntimeDiagnostic,
		Description: "Diagnostic profile only. Read bounded account data from local replay state. Results are unfinalized; found=false can mean a storage error, and dataSlice adds an info log.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, in accountInfoInput) (*mcpsdk.CallToolResult, accountInfoOutput, error) {
		if err := validatePubkey(in.Pubkey); err != nil {
			return nil, accountInfoOutput{}, fmt.Errorf("invalid pubkey: %w", err)
		}
		// An explicit base64 default keeps the node's response shape stable.
		encoding := in.Encoding
		if encoding == "" {
			encoding = "base64"
		}
		if encoding != "base58" && encoding != "base64" && encoding != "base64+zstd" {
			return nil, accountInfoOutput{}, errors.New("encoding must be base58, base64, or base64+zstd")
		}
		dataLength := uint64(defaultAccountDataBytes)
		if in.DataLength != nil {
			dataLength = *in.DataLength
		}
		if dataLength > maxAccountDataBytes {
			return nil, accountInfoOutput{}, fmt.Errorf("data_length %d exceeds %d-byte limit", dataLength, maxAccountDataBytes)
		}
		if in.DataOffset > maxAccountSpaceBytes || dataLength > maxAccountSpaceBytes-in.DataOffset {
			return nil, accountInfoOutput{}, fmt.Errorf("requested data range exceeds Mithril's %d-byte account limit", maxAccountSpaceBytes)
		}
		if clientErr != nil {
			return nil, accountInfoOutput{}, fmt.Errorf("RPC client init for %s failed: %w", safeRPCURL, clientErr)
		}
		result, err := client.getAccountInfo(ctx, in.Pubkey, encoding, accountDataSlice{Offset: in.DataOffset, Length: dataLength})
		if err != nil {
			return nil, accountInfoOutput{}, fmt.Errorf("RPC getAccountInfo via %s failed: %w", safeRPCURL, err)
		}
		out := accountInfoOutput{
			Found: result.Value != nil,
			Context: accountContextOutput{
				APIVersion: result.Context.APIVersion,
				Slot:       *result.Context.Slot,
			},
			Finality:    "local_unfinalized",
			Consistency: "node_reported_non_atomic",
		}
		if result.Value == nil {
			out.Consistency = "node_rpc_absence_or_storage_error"
			return nil, out, nil
		}
		if err := validatePubkey(result.Value.Owner); err != nil {
			return nil, accountInfoOutput{}, fmt.Errorf("getAccountInfo returned invalid owner: %w", err)
		}
		if *result.Value.Space > maxAccountSpaceBytes {
			return nil, accountInfoOutput{}, fmt.Errorf("getAccountInfo returned account space above Mithril's %d-byte limit", maxAccountSpaceBytes)
		}
		var tuple []string
		if err := json.Unmarshal(result.Value.Data, &tuple); err != nil || len(tuple) != 2 || tuple[1] != encoding {
			return nil, accountInfoOutput{}, fmt.Errorf("getAccountInfo returned invalid data tuple for encoding %q", encoding)
		}
		data := [2]string{tuple[0], tuple[1]}
		actualLength := uint64(0)
		if in.DataOffset < *result.Value.Space {
			actualLength = min(dataLength, *result.Value.Space-in.DataOffset)
		}
		if err := validateAccountData(tuple[0], encoding, actualLength); err != nil {
			return nil, accountInfoOutput{}, fmt.Errorf("getAccountInfo returned invalid data: %w", err)
		}
		out.Value = &accountValueOutput{
			Data:          data,
			Executable:    result.Value.Executable,
			Lamports:      fmt.Sprintf("%d", *result.Value.Lamports),
			Owner:         result.Value.Owner,
			RentEpoch:     fmt.Sprintf("%d", *result.Value.RentEpoch),
			Space:         *result.Value.Space,
			DataOffset:    in.DataOffset,
			DataLength:    actualLength,
			DataTruncated: in.DataOffset > 0 || actualLength < *result.Value.Space,
		}
		return nil, out, nil
	})
}
