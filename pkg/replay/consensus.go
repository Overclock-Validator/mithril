package replay

import (
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/state"
)

// ConsensusOpts selects replay's protocol semantics and optionally carries the
// Alpenglow certificate engine. Alpenglow with a nil Engine uses delegated
// (RPC-attested) finality; a nil opts value is classic verifying replay.
type ConsensusOpts struct {
	Alpenglow bool
	Engine    consensusengine.Engine

	// TransactionHistory is caller-owned across replay retries. The caller
	// must Stop it before closing its store.
	TransactionHistory *TransactionHistorySession

	// TransactionResults retains lightweight outcomes independently of history.
	TransactionResults *TransactionResultSession

	// TransactionStatusCheckpointAfterCommit performs advisory sidecar
	// retention after AccountsDB has durably selected a checkpoint reference.
	// Replay logs and ignores its error because the account fold is committed.
	TransactionStatusCheckpointAfterCommit func(*state.TransactionStatusCheckpointRef) error
}
