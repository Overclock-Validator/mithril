package rpcserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/filecoin-project/go-jsonrpc"
)

// maxProcessingAge is how many blocks a blockhash stays valid for
// transactions (Agave's MAX_PROCESSING_AGE).
const maxProcessingAge = 150

// BankViews is the publisher replay uses to expose completed banks.
func (rpcServer *RpcServer) BankViews() *bankview.Publisher {
	return &rpcServer.bankViews
}

// commitmentConfig is the commitment and minContextSlot pair most methods take
// in their trailing config object.
type commitmentConfig struct {
	level          bankview.Level
	minContextSlot uint64
}

// parseCommitmentConfig reads commitment and minContextSlot from params[idx]
// when present. Other fields are left to the caller.
func parseCommitmentConfig(params []interface{}, idx int) (commitmentConfig, map[string]interface{}, error) {
	conf := commitmentConfig{level: bankview.Finalized}
	if len(params) <= idx || params[idx] == nil {
		return conf, nil, nil
	}
	m, ok := params[idx].(map[string]interface{})
	if !ok {
		return conf, nil, &InvalidParamsError{Message: "invalid config object"}
	}
	if raw, ok := m["commitment"]; ok && raw != nil {
		s, ok := raw.(string)
		if !ok {
			return conf, nil, &InvalidParamsError{Message: "invalid commitment"}
		}
		level, err := bankview.ParseCommitment(s)
		if err != nil {
			return conf, nil, &InvalidParamsError{Message: err.Error()}
		}
		conf.level = level
	}
	if raw, ok := m["minContextSlot"]; ok && raw != nil {
		f, ok := raw.(float64)
		if !ok || f < 0 || f != float64(uint64(f)) {
			return conf, nil, &InvalidParamsError{Message: "invalid minContextSlot"}
		}
		conf.minContextSlot = uint64(f)
	}
	return conf, m, nil
}

// decodeParams decodes a JSON-RPC positional parameter list. Missing params
// decode as an empty list.
func decodeParams(p jsonrpc.RawParams) ([]interface{}, error) {
	if len(p) == 0 {
		return nil, nil
	}
	params, err := jsonrpc.DecodeParams[[]interface{}](p)
	if err != nil {
		return nil, &InvalidParamsError{Message: fmt.Sprintf("decoding params: %v", err)}
	}
	return params, nil
}

// viewError maps bank-view failures to JSON-RPC errors clients understand.
func viewError(err error) error {
	var mcs *bankview.MinContextSlotError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &mcs):
		return &MinContextSlotNotReachedError{ContextSlot: mcs.ContextSlot}
	case errors.Is(err, bankview.ErrNotReady), errors.Is(err, bankview.ErrInconsistent):
		return &NodeUnhealthyError{}
	}
	return err
}

// currentView selects the view for a method that only needs bank scalars.
// It still validates the generation, so a bank from an abandoned fork is
// never reported between an unwind and its republication.
func (rpcServer *RpcServer) currentView(ctx context.Context, conf commitmentConfig) (*bankview.View, error) {
	var view *bankview.View
	err := rpcServer.readView(ctx, conf, func(r *bankview.Read) error {
		view = r.View
		return nil
	})
	return view, err
}

// readView runs fn against a validated view; see bankview.Publisher.Do.
func (rpcServer *RpcServer) readView(ctx context.Context, conf commitmentConfig, fn func(*bankview.Read) error) error {
	return viewError(rpcServer.bankViews.Do(ctx, conf.level, conf.minContextSlot, fn))
}

// scalarConfig parses the single optional config object of a method that
// reads only bank scalars, with the strict errors of parseBankReadConfig, and
// returns the bank selection it asks for.
func scalarConfig(ctx context.Context, method string, p jsonrpc.RawParams) (commitmentConfig, error) {
	config, err := parseBankReadConfig(ctx, method, p)
	if err != nil {
		return commitmentConfig{}, err
	}
	conf := commitmentConfig{level: bankview.Finalized}
	if config.Commitment != nil {
		if conf.level, err = bankview.ParseCommitment(*config.Commitment); err != nil {
			return commitmentConfig{}, &InvalidParamsError{Message: err.Error()}
		}
	}
	if config.MinContextSlot != nil {
		conf.minContextSlot = *config.MinContextSlot
	}
	return conf, nil
}
