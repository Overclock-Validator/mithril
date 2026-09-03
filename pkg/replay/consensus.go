package replay

import (
	"errors"
	"fmt"
	"strings"

	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

// ConsensusOpts selects replay's protocol semantics and optionally carries the
// Alpenglow certificate engine. Alpenglow with a nil Engine uses delegated
// (RPC-attested) finality; a nil opts value is classic verifying replay.
type ConsensusOpts struct {
	Alpenglow bool
	Engine    consensusengine.Engine
	// WorkingSetMaxRetainedBytes is the conservative account-layer memory
	// threshold. Zero selects DefaultWorkingSetMaxRetainedBytes.
	WorkingSetMaxRetainedBytes uint64
	// SnapshotParentAlpenglowBlockID anchors fresh replay when ResumeState is
	// nil. ResumeState carries and supersedes this value after a rooted fold.
	SnapshotParentAlpenglowBlockID    solana.Hash
	HasSnapshotParentAlpenglowBlockID bool

	// TransactionStatusCheckpointAfterCommit performs advisory sidecar
	// retention after AccountsDB has durably selected a checkpoint reference.
	// Replay logs and ignores its error because the account fold is committed.
	TransactionStatusCheckpointAfterCommit func(*state.TransactionStatusCheckpointRef) error
}

// alpenglowReplayParentAnchor resolves the consensus identity used to bind
// status-cache restore and the first live block. A rooted resume checkpoint is
// authoritative after replay has advanced; otherwise the caller-provided hash
// must exactly match the snapshot seed persisted in MithrilState.
func alpenglowReplayParentAnchor(
	mithrilState *state.MithrilState,
	resumeState *ResumeState,
	opts *ConsensusOpts,
) (uint64, solana.Hash, error) {
	if opts == nil || !opts.Alpenglow {
		return 0, solana.Hash{}, errors.New("Alpenglow replay parent requested outside Alpenglow mode")
	}
	if resumeState != nil {
		if !resumeState.HasParentAlpenglowBlockID || resumeState.ParentAlpenglowBlockID == (solana.Hash{}) {
			return 0, solana.Hash{}, fmt.Errorf(
				"rooted replay checkpoint at slot %d has no Alpenglow block ID",
				resumeState.ParentSlot,
			)
		}
		return resumeState.ParentSlot, resumeState.ParentAlpenglowBlockID, nil
	}
	if mithrilState == nil {
		return 0, solana.Hash{}, errors.New("fresh Alpenglow replay has no snapshot state")
	}
	if !opts.HasSnapshotParentAlpenglowBlockID || opts.SnapshotParentAlpenglowBlockID == (solana.Hash{}) {
		return 0, solana.Hash{}, fmt.Errorf(
			"fresh Alpenglow replay at snapshot slot %d has no snapshot block ID",
			mithrilState.ManifestParentSlot,
		)
	}
	encoded := strings.TrimSpace(mithrilState.ManifestParentAlpenglowBlockID)
	if encoded == "" || encoded != opts.SnapshotParentAlpenglowBlockID.String() {
		return 0, solana.Hash{}, fmt.Errorf(
			"snapshot Alpenglow block ID does not match the persisted manifest seed at slot %d",
			mithrilState.ManifestParentSlot,
		)
	}
	return mithrilState.ManifestParentSlot, opts.SnapshotParentAlpenglowBlockID, nil
}
