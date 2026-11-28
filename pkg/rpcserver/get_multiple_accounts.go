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

const maxMultipleAccountsParsedBytes = 32 << 20

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
	if err := acquireQueryResponse(ctx); err != nil {
		return GetMultipleAccountsResp{}, err
	}
	owner := ctx.Value(queryResponseKey{}).(*historyResponseOwner)
	ctx, cancel := context.WithDeadline(ctx, owner.deadline)
	defer cancel()
	remaining := uint64(maxMultipleAccountsDataBytes)
	rooted, accountSet, err := rpcServer.readRootedAccountsWithBudget(ctx, keys, conf.MinContextSlot, &remaining)
	if err != nil {
		if _, ok := err.(*MinContextSlotNotReachedError); ok {
			return GetMultipleAccountsResp{}, err
		}
		if errors.Is(err, accountsdb.ErrAccountDataLimit) {
			return GetMultipleAccountsResp{}, &InvalidParamsError{Message: "getMultipleAccounts exceeds 16 MiB account data budget"}
		}
		return GetMultipleAccountsResp{}, fmt.Errorf("read accounts at rooted slot %d: %w", rooted.Slot, err)
	}
	response := GetMultipleAccountsResp{Context: GetAccountInfoRespContext{Slot: rooted.Slot},
		Value: make([]*GetAccountInfoRespValue, len(keys))}
	parsedRemaining := maxMultipleAccountsParsedBytes
	for i, acct := range accountSet {
		if err := ctx.Err(); err != nil {
			return GetMultipleAccountsResp{}, err
		}
		response.Value[i], err = accountInfoValue(acct, conf)
		if err != nil {
			return GetMultipleAccountsResp{}, err
		}
		if conf.EncodingType != nil && *conf.EncodingType == GetAccountEncodingJson {
			// Charge actual JSON values, including duplicates, nulls and base64
			// fallbacks. This bounds retained output, not marshal scratch space.
			encoded, err := json.Marshal(response.Value[i])
			if err != nil {
				return GetMultipleAccountsResp{}, err
			}
			if len(encoded) > parsedRemaining {
				return GetMultipleAccountsResp{}, &InvalidParamsError{Message: "getMultipleAccounts exceeds 32 MiB parsed response budget"}
			}
			parsedRemaining -= len(encoded)
		}
	}
	if err := ctx.Err(); err != nil {
		return GetMultipleAccountsResp{}, err
	}
	return response, nil
}
