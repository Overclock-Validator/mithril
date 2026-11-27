package rpcserver

import (
	"context"

	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

type IsBlockhashValidResp struct {
	Context GetLatestBlockhashRespContext `json:"context"`
	Value   bool                          `json:"value"`
}

// IsBlockhashValid reports whether the selected bank would still accept a
// transaction using blockhash. The bank's RecentBlockhashes sysvar holds the
// last maxProcessingAge blockhashes, which is the processing window.
func (rpcServer *RpcServer) IsBlockhashValid(ctx context.Context, p jsonrpc.RawParams) (IsBlockhashValidResp, error) {
	params, err := decodeParams(p)
	if err != nil {
		return IsBlockhashValidResp{}, err
	}
	s, ok := firstString(params)
	if !ok {
		return IsBlockhashValidResp{}, &InvalidParamsError{Message: "isBlockhashValid requires a blockhash string"}
	}
	blockhash, err := solana.HashFromBase58(s)
	if err != nil {
		return IsBlockhashValidResp{}, &InvalidParamsError{Message: "Invalid param: Invalid"}
	}
	commitment, _, err := parseCommitmentConfig(params, 1)
	if err != nil {
		return IsBlockhashValidResp{}, err
	}
	var resp IsBlockhashValidResp
	err = rpcServer.readView(ctx, commitment, func(r *bankview.Read) error {
		valid, err := blockhashValidAt(r, blockhash)
		if err != nil {
			return err
		}
		resp = IsBlockhashValidResp{Context: GetLatestBlockhashRespContext{Slot: r.View.Slot}, Value: valid}
		return nil
	})
	return resp, err
}
