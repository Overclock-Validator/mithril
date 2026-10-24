package rpcserver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

type bankReadConfig struct {
	Commitment     *string `json:"commitment"`
	MinContextSlot *uint64 `json:"minContextSlot"`
}

func parseBankReadConfig(ctx context.Context, method string, p jsonrpc.RawParams) (bankReadConfig, error) {
	var config bankReadConfig
	if err := ctx.Err(); err != nil {
		return config, err
	}
	var params []json.RawMessage
	if len(p) != 0 {
		if err := json.Unmarshal(p, &params); err != nil {
			return config, &InvalidParamsError{Message: fmt.Sprintf("decoding params: %v", err)}
		}
	}
	if len(params) > 1 {
		return config, &InvalidParamsError{Message: method + " accepts at most one config object"}
	}
	if len(params) == 1 {
		if err := json.Unmarshal(params[0], &config); err != nil {
			return config, &InvalidParamsError{Message: fmt.Sprintf("invalid %s config: %v", method, err)}
		}
	}
	if config.Commitment != nil && *config.Commitment != "processed" && *config.Commitment != "confirmed" && *config.Commitment != "finalized" {
		return config, &InvalidParamsError{Message: "invalid commitment"}
	}
	return config, nil
}

// processedBankContext keeps response fields in one replay generation. The
// health lock prevents accepting a bank while replay invalidates its readiness.
func (rpcServer *RpcServer) processedBankContext() (replay.ChainTipSnapshot, error) {
	rpcServer.slotCtxMu.RLock()
	defer rpcServer.slotCtxMu.RUnlock()
	if !rpcServer.healthReady {
		return replay.ChainTipSnapshot{}, fmt.Errorf("node has no processed bank available")
	}
	bank := replay.ChainTipParentContext()
	if bank.Generation == 0 || bank.BankSysvars == nil || bank.BankSysvars.Slot() != bank.Slot || bank.LastBlockhash == (solana.Hash{}) {
		return replay.ChainTipSnapshot{}, fmt.Errorf("node has no processed bank available")
	}
	return bank, nil
}
