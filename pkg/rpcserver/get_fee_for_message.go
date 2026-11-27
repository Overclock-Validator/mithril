package rpcserver

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/filecoin-project/go-jsonrpc"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

type GetFeeForMessageResp struct {
	Context GetLatestBlockhashRespContext `json:"context"`
	Value   *uint64                       `json:"value"`
}

// GetFeeForMessage quotes the fee the selected bank would charge. Like Agave,
// it returns null when the message's blockhash is no longer valid there.
func (rpcServer *RpcServer) GetFeeForMessage(ctx context.Context, p jsonrpc.RawParams) (GetFeeForMessageResp, error) {
	params, err := decodeParams(p)
	if err != nil {
		return GetFeeForMessageResp{}, err
	}
	encoded, ok := firstString(params)
	if !ok {
		return GetFeeForMessageResp{}, &InvalidParamsError{Message: "getFeeForMessage requires a base64 message"}
	}
	if len(encoded) > maxBase64TxSize {
		return GetFeeForMessageResp{}, &InvalidParamsError{Message: "message too large"}
	}
	wire, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return GetFeeForMessageResp{}, &InvalidParamsError{Message: fmt.Sprintf("invalid base64 message: %v", err)}
	}
	var msg solana.Message
	decoder := bin.NewBinDecoder(wire)
	if err := msg.UnmarshalWithDecoder(decoder); err != nil {
		return GetFeeForMessageResp{}, &InvalidParamsError{Message: fmt.Sprintf("invalid message: %v", err)}
	}
	if decoder.HasRemaining() {
		return GetFeeForMessageResp{}, &InvalidParamsError{Message: "invalid message: trailing bytes"}
	}
	commitment, _, err := parseCommitmentConfig(params, 1)
	if err != nil {
		return GetFeeForMessageResp{}, err
	}
	var resp GetFeeForMessageResp
	err = rpcServer.readView(ctx, commitment, func(r *bankview.Read) error {
		resp = GetFeeForMessageResp{Context: GetLatestBlockhashRespContext{Slot: r.View.Slot}}
		valid, err := blockhashValidAt(r, msg.RecentBlockhash)
		if err != nil || !valid {
			return err
		}
		feeInfo, err := fees.FeeForTransaction(&solana.Transaction{Message: msg}, r.View.Features)
		if err != nil {
			return &InvalidParamsError{Message: fmt.Sprintf("invalid message: %v", err)}
		}
		resp.Value = &feeInfo.TotalFee
		return nil
	})
	return resp, err
}
