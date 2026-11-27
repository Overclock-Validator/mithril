package node

import (
	"errors"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
)

func validateSignatureStatuses(enabled bool, cluster, mode string, port int) error {
	if !enabled {
		return nil
	}
	if cluster != "alpenglow" || mode != "verifying" {
		return errors.New("RPC signature statuses require Alpenglow verifying mode")
	}
	if port < 1 || port > 65535 {
		return errors.New("RPC signature statuses require an RPC port between 1 and 65535")
	}
	return nil
}

func newNodeTransactionResults(enabled bool, cluster, mode string, port int, s *state.MithrilState, resume *replay.ResumeState) (*txstatus.ResultCache, *replay.TransactionResultSession, error) {
	if err := validateSignatureStatuses(enabled, cluster, mode, port); err != nil {
		return nil, nil, err
	}
	if !enabled {
		return nil, nil, nil
	}
	parent, err := replay.TransactionResultSeed(s, resume)
	if err != nil {
		return nil, nil, err
	}
	cache, err := txstatus.NewResultCache(parent)
	if err != nil {
		return nil, nil, err
	}
	session, err := replay.NewTransactionResultSession(cache, mode)
	if err != nil {
		return nil, nil, err
	}
	return cache, session, nil
}
