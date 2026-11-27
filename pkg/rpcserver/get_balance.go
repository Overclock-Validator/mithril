package rpcserver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

type GetBalanceResp struct {
	Context GetAccountInfoRespContext `json:"context"`
	Value   uint64                    `json:"value"`
}

// GetBalanceRespContext is the context of responses that report only a slot.
type GetBalanceRespContext struct {
	APIVersion string `json:"apiVersion"`
	Slot       uint64 `json:"slot"`
}

func (rpcServer *RpcServer) GetBalance(ctx context.Context, p jsonrpc.RawParams) (GetBalanceResp, error) {
	if err := ctx.Err(); err != nil {
		return GetBalanceResp{}, err
	}
	var params []json.RawMessage
	if len(p) != 0 {
		if err := json.Unmarshal(p, &params); err != nil {
			return GetBalanceResp{}, &InvalidParamsError{Message: fmt.Sprintf("decoding params: %v", err)}
		}
	}
	if len(params) < 1 || len(params) > 2 {
		return GetBalanceResp{}, &InvalidParamsError{Message: "getBalance requires an address and optional config"}
	}
	var address string
	if err := json.Unmarshal(params[0], &address); err != nil {
		return GetBalanceResp{}, &InvalidParamsError{Message: "getBalance requires an address as first parameter"}
	}
	pk, err := solana.PublicKeyFromBase58(address)
	if err != nil {
		return GetBalanceResp{}, &InvalidParamsError{Message: "Invalid param: Invalid"}
	}
	conf, err := parseAccountConfig(params, nil)
	if err != nil {
		return GetBalanceResp{}, err
	}
	if conf.AtSlot != nil {
		accts, err := rpcServer.accountsAtSlot(ctx, []solana.PublicKey{pk}, *conf.AtSlot, bankview.NoDataLimit)
		if err != nil {
			return GetBalanceResp{}, err
		}
		resp := GetBalanceResp{Context: accountContext(*conf.AtSlot)}
		if accts[0] != nil {
			resp.Value = accts[0].Lamports
		}
		return resp, nil
	}
	var resp GetBalanceResp
	err = rpcServer.readView(ctx, accountReadConfig(conf), func(r *bankview.Read) error {
		acct, err := r.Account(pk)
		if err != nil {
			return err
		}
		resp = GetBalanceResp{Context: accountContext(r.View.Slot)}
		if acct != nil {
			resp.Value = acct.Lamports
		}
		return nil
	})
	return resp, err
}

func firstString(params []interface{}) (string, bool) {
	if len(params) < 1 {
		return "", false
	}
	s, ok := params[0].(string)
	return s, ok
}
