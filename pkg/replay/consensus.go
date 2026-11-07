package replay

import (
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

// ConsensusOpts optionally carries the Alpenglow certificate engine. Replay is
// always Alpenglow; a nil Engine (or nil opts) uses delegated (RPC-attested)
// finality.
type ConsensusOpts struct {
	Engine consensusengine.Engine

	// Progress is optional read-only telemetry; it cannot influence replay.
	Progress *Progress
	// CertificateObservationVoteAccount is a fixed public account, not the
	// runtime identity. Zero disables compact certificate capture.
	CertificateObservationVoteAccount solana.PublicKey
	// LeaderOutcomeIdentity is fixed at startup. Identity changes do not relabel
	// earlier schedule assignments; the public projection hides a stale binding.
	LeaderOutcomeIdentity solana.PublicKey
	// AccountObservationIdentity is the configured public identity at startup.
	// Zero disables account observations, including for ephemeral identities.
	AccountObservationIdentity solana.PublicKey

	// TransactionStatusCheckpointAfterCommit performs advisory sidecar
	// retention after AccountsDB has durably selected a checkpoint reference.
	// Replay logs and ignores its error because the account fold is committed.
	TransactionStatusCheckpointAfterCommit func(*state.TransactionStatusCheckpointRef) error
}
