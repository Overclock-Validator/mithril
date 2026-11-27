package rpcserver

import (
	"context"

	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

func (rpcServer *RpcServer) GetMinimumBalanceForRentExemption(ctx context.Context, p jsonrpc.RawParams) (uint64, error) {
	params, err := decodeParams(p)
	if err != nil {
		return 0, err
	}
	if len(params) < 1 {
		return 0, &InvalidParamsError{Message: "getMinimumBalanceForRentExemption requires a data length"}
	}
	length, ok := params[0].(float64)
	if !ok || length < 0 || length != float64(uint64(length)) {
		return 0, &InvalidParamsError{Message: "invalid data length"}
	}
	commitment, _, err := parseCommitmentConfig(params, 1)
	if err != nil {
		return 0, err
	}
	var minimum uint64
	err = rpcServer.readView(ctx, commitment, func(r *bankview.Read) error {
		var rent sealevel.SysvarRent
		if err := readSysvar(r, solana.PublicKey(sealevel.SysvarRentAddr), &rent); err != nil {
			return err
		}
		minimum = rent.MinimumBalance(uint64(length))
		return nil
	})
	return minimum, err
}
