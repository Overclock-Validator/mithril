package rpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

const maxMultipleAccountsDataBytes = 16 << 20

// GetMultipleAccountsResp preserves request order and one rooted context.
type GetMultipleAccountsResp struct {
	Context GetAccountInfoRespContext  `json:"context"`
	Value   []*GetAccountInfoRespValue `json:"value"`
}

// GetMultipleAccounts serves one published rooted view over HTTP.
// Its response permit covers serialization and the final write.
// Confirmed and finalized use that root; processed commitment is unsupported.
func (rpcServer *RpcServer) GetMultipleAccounts(ctx context.Context, p jsonrpc.RawParams) (GetMultipleAccountsResp, error) {
	if len(p) > 16<<10 {
		return GetMultipleAccountsResp{}, &InvalidParamsError{Message: "getMultipleAccounts parameters exceed 16 KiB"}
	}
	params, err := jsonrpc.DecodeParams[[]json.RawMessage](p)
	if err != nil || len(params) < 1 || len(params) > 2 {
		return GetMultipleAccountsResp{}, &InvalidParamsError{Message: "getMultipleAccounts requires keys and optional config"}
	}
	var keyValues []json.RawMessage
	if err := json.Unmarshal(params[0], &keyValues); err != nil || keyValues == nil || len(keyValues) > 100 {
		return GetMultipleAccountsResp{}, &InvalidParamsError{Message: "getMultipleAccounts requires an array of at most 100 keys"}
	}
	keys := make([]solana.PublicKey, len(keyValues))
	for i, value := range keyValues {
		var key string
		if err := json.Unmarshal(value, &key); err != nil || len(key) > 44 {
			return GetMultipleAccountsResp{}, &InvalidParamsError{Message: "invalid account key"}
		}
		keys[i], err = solana.PublicKeyFromBase58(key)
		if err != nil {
			return GetMultipleAccountsResp{}, &InvalidParamsError{Message: "invalid base58 encoding"}
		}
	}
	encoding := GetAccountEncodingBase64
	conf, err := parseAccountConfig(params, &encoding)
	if err != nil {
		return GetMultipleAccountsResp{}, err
	}
	// Parsed account decoding is not covered by this method's data budget.
	if conf.EncodingType != nil && *conf.EncodingType == GetAccountEncodingJson {
		return GetMultipleAccountsResp{}, &InvalidParamsError{Message: "getMultipleAccounts jsonParsed is not supported"}
	}
	if err := acquireQueryResponse(ctx); err != nil {
		return GetMultipleAccountsResp{}, err
	}
	remaining := uint64(maxMultipleAccountsDataBytes)
	rooted, accountSet, err := rpcServer.readRootedAccountsWithBudget(ctx, keys, &remaining)
	if err != nil {
		if errors.Is(err, accountsdb.ErrAccountDataLimit) {
			return GetMultipleAccountsResp{}, &InvalidParamsError{Message: "getMultipleAccounts exceeds 16 MiB account data budget"}
		}
		return GetMultipleAccountsResp{}, fmt.Errorf("read accounts at rooted slot %d: %w", rooted.Slot, err)
	}
	if conf.MinContextSlot > rooted.Slot {
		return GetMultipleAccountsResp{}, &MinContextSlotNotReachedError{ContextSlot: rooted.Slot}
	}
	response := GetMultipleAccountsResp{Context: GetAccountInfoRespContext{ApiVersion: "mithril 0.1", Slot: rooted.Slot},
		Value: make([]*GetAccountInfoRespValue, len(keys))}
	for i, acct := range accountSet {
		if err := ctx.Err(); err != nil {
			return GetMultipleAccountsResp{}, err
		}
		response.Value[i], err = accountInfoValue(acct, conf)
		if err != nil {
			return GetMultipleAccountsResp{}, err
		}
	}
	return response, nil
}
