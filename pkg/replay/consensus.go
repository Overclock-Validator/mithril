package replay

import (
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/state"
)

// ConsensusOpts optionally carries the Alpenglow certificate engine. Replay is
// always Alpenglow; a nil Engine (or nil opts) uses delegated (RPC-attested)
// finality.
type ConsensusOpts struct {
	Engine consensusengine.Engine

	// TransactionStatusCheckpointAfterCommit performs advisory sidecar
	// retention after AccountsDB has durably selected a checkpoint reference.
	// Replay logs and ignores its error because the account fold is committed.
	TransactionStatusCheckpointAfterCommit func(*state.TransactionStatusCheckpointRef) error
}
