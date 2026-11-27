package rpcserver

import (
	"context"
	"encoding/binary"
	"strconv"

	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/rpcindex"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

type UITokenAmount struct {
	Amount         string   `json:"amount"`
	Decimals       uint8    `json:"decimals"`
	UIAmount       *float64 `json:"uiAmount"`
	UIAmountString string   `json:"uiAmountString"`
}

type GetTokenAmountResp struct {
	Context GetAccountInfoRespContext `json:"context"`
	Value   UITokenAmount             `json:"value"`
}

func uiTokenAmount(amount uint64, decimals uint8) UITokenAmount {
	s := rpcindex.UIAmountString(amount, decimals)
	f, _ := strconv.ParseFloat(s, 64)
	return UITokenAmount{Amount: strconv.FormatUint(amount, 10), Decimals: decimals, UIAmount: &f, UIAmountString: s}
}

func (rpcServer *RpcServer) tokenAmountMethod(ctx context.Context, p jsonrpc.RawParams, name string, read func(r *bankview.Read, pk solana.PublicKey) (uint64, uint8, error)) (GetTokenAmountResp, error) {
	params, err := decodeParams(p)
	if err != nil {
		return GetTokenAmountResp{}, err
	}
	s, ok := firstString(params)
	if !ok {
		return GetTokenAmountResp{}, &InvalidParamsError{Message: name + " requires an address"}
	}
	pk, err := solana.PublicKeyFromBase58(s)
	if err != nil {
		return GetTokenAmountResp{}, &InvalidParamsError{Message: "Invalid param: Invalid"}
	}
	commitment, _, err := parseCommitmentConfig(params, 1)
	if err != nil {
		return GetTokenAmountResp{}, err
	}
	var resp GetTokenAmountResp
	err = rpcServer.readView(ctx, commitment, func(r *bankview.Read) error {
		amount, decimals, err := read(r, pk)
		if err != nil {
			return err
		}
		resp = GetTokenAmountResp{Context: accountContext(r.View.Slot), Value: uiTokenAmount(amount, decimals)}
		return nil
	})
	return resp, err
}

// GetTokenAccountBalance returns a token account's balance at the selected bank.
func (rpcServer *RpcServer) GetTokenAccountBalance(ctx context.Context, p jsonrpc.RawParams) (GetTokenAmountResp, error) {
	return rpcServer.tokenAmountMethod(ctx, p, "getTokenAccountBalance", func(r *bankview.Read, pk solana.PublicKey) (uint64, uint8, error) {
		acct, err := r.Account(pk)
		if err != nil {
			return 0, 0, err
		}
		mint, _, amount, _, ok := rpcindex.TokenAccountState(acct)
		if !ok {
			return 0, 0, &InvalidParamsError{Message: "Invalid param: not a Token account"}
		}
		mintAcct, err := r.Account(mint)
		if err != nil {
			return 0, 0, err
		}
		decimals, ok := rpcindex.MintDecimals(mintAcct)
		if !ok {
			return 0, 0, &InvalidParamsError{Message: "Invalid param: mint could not be unpacked"}
		}
		return amount, decimals, nil
	})
}

// GetTokenSupply returns a mint's supply at the selected bank.
func (rpcServer *RpcServer) GetTokenSupply(ctx context.Context, p jsonrpc.RawParams) (GetTokenAmountResp, error) {
	return rpcServer.tokenAmountMethod(ctx, p, "getTokenSupply", func(r *bankview.Read, pk solana.PublicKey) (uint64, uint8, error) {
		acct, err := r.Account(pk)
		if err != nil {
			return 0, 0, err
		}
		decimals, ok := rpcindex.MintDecimals(acct)
		if !ok {
			return 0, 0, &InvalidParamsError{Message: "Invalid param: not a Token mint"}
		}
		return binary.LittleEndian.Uint64(acct.Data[36:44]), decimals, nil
	})
}
