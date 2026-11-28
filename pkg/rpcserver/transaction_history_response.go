package rpcserver

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/tpu/wire"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

type historyTransactionResponse struct {
	Transaction any                    `json:"transaction"`
	Meta        historyTransactionMeta `json:"meta"`
	Version     any                    `json:"version,omitempty"`
}

type historyTransactionMeta struct {
	Error                json.RawMessage            `json:"err"`
	Status               map[string]json.RawMessage `json:"status"`
	Fee                  uint64                     `json:"fee"`
	PreBalances          []uint64                   `json:"preBalances"`
	PostBalances         []uint64                   `json:"postBalances"`
	InnerInstructions    []InnerInstructionGroup    `json:"innerInstructions"`
	LogMessages          []string                   `json:"logMessages"`
	PreTokenBalances     []TokenBalancePayload      `json:"preTokenBalances"`
	PostTokenBalances    []TokenBalancePayload      `json:"postTokenBalances"`
	Rewards              []json.RawMessage          `json:"rewards"`
	LoadedAddresses      *LoadedAddressesPayload    `json:"loadedAddresses"`
	ReturnData           *ReturnDataPayload         `json:"returnData,omitempty"`
	ComputeUnitsConsumed uint64                     `json:"computeUnitsConsumed"`
}

type historyRawTransaction struct {
	Signatures []string          `json:"signatures"`
	Message    historyRawMessage `json:"message"`
}

type historyRawMessage struct {
	AccountKeys         solana.PublicKeySlice                  `json:"accountKeys"`
	Header              solana.MessageHeader                   `json:"header"`
	RecentBlockhash     solana.Hash                            `json:"recentBlockhash"`
	Instructions        []InnerInstruction                     `json:"instructions"`
	AddressTableLookups *solana.MessageAddressTableLookupSlice `json:"addressTableLookups,omitempty"`
	TransactionConfig   *solana.TransactionConfig              `json:"transactionConfig,omitempty"`
}

type historyUnsupportedVersionError struct {
	Version uint8
	message string
}

func (e *historyUnsupportedVersionError) Error() string {
	if e.message != "" {
		return e.message
	}
	return fmt.Sprintf("Transaction version (%d) is not supported by the requesting client. Please try the request again with the following configuration parameter: \"maxSupportedTransactionVersion\": %d", e.Version, e.Version)
}

func (e *historyUnsupportedVersionError) ToJSONRPCError() (jsonrpc.JSONRPCError, error) {
	return jsonrpc.JSONRPCError{Code: -32015, Message: e.Error()}, nil
}

func (e *historyUnsupportedVersionError) FromJSONRPCError(rpcErr jsonrpc.JSONRPCError) error {
	if rpcErr.Code != -32015 {
		return fmt.Errorf("unexpected code %d for unsupported transaction version", rpcErr.Code)
	}
	e.message = rpcErr.Message
	return nil
}

// encodeHistoryTransaction renders one retained receipt for either history RPC.
// The caller must first establish canonical eligibility; encoding proves none.
func encodeHistoryTransaction(record transactionhistory.Record, encoding string, maxVersion *uint8) (historyTransactionResponse, error) {
	var result historyTransactionResponse
	if encoding == "" {
		encoding = "json"
	}
	if encoding != "json" && encoding != "base64" {
		return result, &InvalidParamsError{Message: "transaction history supports json and base64 encoding"}
	}
	view, err := wire.Sanitize(record.Transaction)
	if err != nil || !bytes.Equal(view.FirstSignature(), record.FirstSignature[:]) {
		return result, transactionhistory.ErrUnavailable
	}
	// Sanitize wire counts before the SDK allocates decoded instruction vectors.
	tx, err := solana.TransactionFromBytes(record.Transaction)
	if err != nil {
		return result, transactionhistory.ErrUnavailable
	}
	version := tx.Message.GetVersion()
	if version == solana.MessageVersionLegacy {
		if maxVersion != nil {
			result.Version = "legacy"
		}
	} else {
		number := uint8(version - solana.MessageVersionV0)
		if maxVersion == nil || number > *maxVersion {
			return result, &historyUnsupportedVersionError{Version: number}
		}
		result.Version = number
	}
	metadata, err := transactionhistory.UnmarshalMetadata(record.Metadata)
	if err != nil {
		return historyTransactionResponse{}, transactionhistory.ErrUnavailable
	}
	static := len(tx.Message.AccountKeys)
	if len(metadata.AccountKeys) != static+tx.Message.NumLookups() ||
		!slices.Equal(tx.Message.AccountKeys, metadata.AccountKeys[:static]) {
		return historyTransactionResponse{}, transactionhistory.ErrUnavailable
	}
	for _, group := range metadata.InnerInstructions {
		if int(group.Index) >= len(tx.Message.Instructions) {
			return historyTransactionResponse{}, transactionhistory.ErrUnavailable
		}
	}
	if encoding == "base64" {
		result.Transaction = [2]string{base64.StdEncoding.EncodeToString(record.Transaction), "base64"}
	} else {
		message := historyRawMessage{
			AccountKeys: tx.Message.AccountKeys, Header: tx.Message.Header,
			RecentBlockhash: tx.Message.RecentBlockhash,
			Instructions:    make([]InnerInstruction, len(tx.Message.Instructions)),
		}
		for i, instruction := range tx.Message.Instructions {
			height := uint32(1)
			message.Instructions[i] = InnerInstruction{
				ProgramIdIndex: uint8(instruction.ProgramIDIndex), Accounts: instruction.Accounts,
				Data: historyBase58Encode(instruction.Data), StackHeight: &height,
			}
			if message.Instructions[i].Accounts == nil {
				message.Instructions[i].Accounts = []uint16{}
			}
		}
		switch version {
		case solana.MessageVersionV0:
			lookups := tx.Message.AddressTableLookups
			if lookups == nil {
				lookups = solana.MessageAddressTableLookupSlice{}
			}
			message.AddressTableLookups = &lookups
		case solana.MessageVersionV1:
			message.TransactionConfig = &tx.Message.TransactionConfig
		}
		signatures := make([]string, len(tx.Signatures))
		for i, signature := range tx.Signatures {
			signatures[i] = signature.String()
		}
		result.Transaction = historyRawTransaction{Signatures: signatures, Message: message}
	}
	// Use the existing ALT-order renderer, but only with the retained resolution.
	resolved := *tx
	resolved.Message.AccountKeys = metadata.AccountKeys
	status := "Err"
	if bytes.Equal(bytes.TrimSpace(metadata.Error), []byte("null")) {
		status = "Ok"
	}
	result.Meta = historyTransactionMeta{
		Error: metadata.Error, Status: map[string]json.RawMessage{status: metadata.Error}, Fee: metadata.Fee,
		PreBalances: metadata.PreBalances, PostBalances: metadata.PostBalances,
		InnerInstructions: make([]InnerInstructionGroup, len(metadata.InnerInstructions)),
		PreTokenBalances:  historyTokenPayloads(metadata.PreTokenBalances),
		PostTokenBalances: historyTokenPayloads(metadata.PostTokenBalances),
		LoadedAddresses:   loadedAddressesFromTx(&resolved), ComputeUnitsConsumed: metadata.ComputeUnitsConsumed,
	}
	for i, group := range metadata.InnerInstructions {
		out := InnerInstructionGroup{Index: group.Index, Instructions: make([]InnerInstruction, len(group.Instructions))}
		for j, instruction := range group.Instructions {
			accounts := make([]uint16, len(instruction.Accounts))
			for k, index := range instruction.Accounts {
				accounts[k] = uint16(index)
			}
			out.Instructions[j] = InnerInstruction{ProgramIdIndex: instruction.ProgramIDIndex,
				Accounts: accounts, Data: historyBase58Encode(instruction.Data)}
			if instruction.StackHeight != 0 {
				height := instruction.StackHeight
				out.Instructions[j].StackHeight = &height
			}
		}
		result.Meta.InnerInstructions[i] = out
	}
	if metadata.ReturnData != nil {
		result.Meta.ReturnData = &ReturnDataPayload{ProgramId: metadata.ReturnData.ProgramID.String(),
			Data: []string{base64.StdEncoding.EncodeToString(metadata.ReturnData.Data), "base64"}}
	}
	// Capture version 1 has no complete logs or transaction rewards. Keep these
	// null, and omit unrecorded cost units rather than presenting invented values.
	return result, nil
}

func historyTokenPayloads(balances []transactionhistory.TokenBalance) []TokenBalancePayload {
	out := make([]TokenBalancePayload, len(balances))
	for i, balance := range balances {
		out[i] = TokenBalancePayload{
			AccountIndex: uint8(balance.AccountIndex), Mint: balance.Mint.String(),
			Owner: balance.Owner.String(), ProgramId: balance.ProgramID.String(),
			UiTokenAmount: UiTokenAmountPayload{
				Amount: strconv.FormatUint(balance.Amount, 10), Decimals: balance.Decimals,
				UiAmount:       uiAmountForRaw(balance.Amount, balance.Decimals),
				UiAmountString: uiAmountStringForRaw(balance.Amount, balance.Decimals),
			},
		}
	}
	return out
}

// historyBase58Encode uses the standard library's base conversion for bounded
// instruction data. Its digit alphabet differs from Solana's Bitcoin alphabet.
func historyBase58Encode(data []byte) string {
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	var out strings.Builder
	out.Grow(len(data)*138/100 + 1)
	for _, value := range data {
		if value != 0 {
			break
		}
		out.WriteByte('1')
	}
	value := new(big.Int).SetBytes(data)
	if value.Sign() == 0 {
		return out.String()
	}
	for _, digit := range value.Text(58) {
		var index rune
		switch {
		case digit <= '9':
			index = digit - '0'
		case digit >= 'a':
			index = digit - 'a' + 10
		default:
			index = digit - 'A' + 36
		}
		out.WriteByte(alphabet[index])
	}
	return out.String()
}
