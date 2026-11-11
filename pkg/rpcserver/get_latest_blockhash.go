package rpcserver

import (
	"context"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/sysvar"
)

type GetLatestBlockhashRespContext struct {
	Slot uint64 `json:"slot"`
}

type GetLatestBlockhashRespValue struct {
	Blockhash            string `json:"blockhash"`
	LastValidBlockHeight uint64 `json:"lastValidBlockHeight"`
}

type GetLatestBlockhashResp struct {
	Context GetLatestBlockhashRespContext `json:"context"`
	Value   *GetLatestBlockhashRespValue  `json:"value"`
}

func (rpcServer *RpcServer) GetLatestBlockhash(ctx context.Context, p jsonrpc.RawParams) (GetLatestBlockhashResp, error) {
	config, err := parseBankReadConfig(ctx, "getLatestBlockhash", p)
	if err != nil {
		return GetLatestBlockhashResp{}, err
	}

	var slot, height uint64
	var blockhash solana.Hash
	if config.Commitment != nil && *config.Commitment == "processed" {
		bank, err := rpcServer.processedBankContext()
		if err != nil {
			return GetLatestBlockhashResp{}, err
		}
		slot, height, blockhash = bank.Slot, bank.BlockHeight, bank.LastBlockhash
	} else {
		bank, acct, err := rpcServer.readRootedAccount(ctx, solana.PublicKey(sealevel.SysvarRecentBlockHashesAddr))
		if err != nil {
			return GetLatestBlockhashResp{}, fmt.Errorf("read rooted recent blockhashes: %w", err)
		}
		var recent sealevel.SysvarRecentBlockhashes
		if err := recent.UnmarshalWithDecoder(bin.NewBinDecoder(acct.Data)); err != nil {
			return GetLatestBlockhashResp{}, fmt.Errorf("decode rooted recent blockhashes: %w", err)
		}
		if len(recent) == 0 {
			return GetLatestBlockhashResp{}, fmt.Errorf("node has no recent blockhash available")
		}
		slot, height, blockhash = bank.Slot, bank.BlockHeight, solana.Hash(recent[0].Blockhash)
	}
	if blockhash == (solana.Hash{}) || (slot > 0 && height == 0) {
		return GetLatestBlockhashResp{}, fmt.Errorf("node has no blockhash context available for slot %d", slot)
	}
	if config.MinContextSlot != nil && slot < *config.MinContextSlot {
		return GetLatestBlockhashResp{}, &MinContextSlotNotReachedError{ContextSlot: slot}
	}
	if height > ^uint64(0)-sysvar.MaxProcessingAge {
		return GetLatestBlockhashResp{}, fmt.Errorf("blockhash expiry height overflows at slot %d", slot)
	}
	value := &GetLatestBlockhashRespValue{Blockhash: blockhash.String(), LastValidBlockHeight: height + sysvar.MaxProcessingAge}
	return GetLatestBlockhashResp{Context: GetLatestBlockhashRespContext{Slot: slot}, Value: value}, nil
}
