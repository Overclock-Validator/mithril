package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestAlpenglowReplayParentAnchorUsesExactSnapshotOrResumeIdentity(t *testing.T) {
	snapshotID := solana.Hash{1, 2, 3}
	mithrilState := &state.MithrilState{
		ManifestParentSlot:             50,
		ManifestParentAlpenglowBlockID: snapshotID.String(),
	}
	opts := &ConsensusOpts{
		Alpenglow:                         true,
		SnapshotParentAlpenglowBlockID:    snapshotID,
		HasSnapshotParentAlpenglowBlockID: true,
	}

	slot, blockID, err := alpenglowReplayParentAnchor(mithrilState, nil, opts)
	require.NoError(t, err)
	require.Equal(t, uint64(50), slot)
	require.Equal(t, snapshotID, blockID)

	resumeID := solana.Hash{9, 8, 7}
	resume := &ResumeState{
		ParentSlot:                75,
		ParentAlpenglowBlockID:    resumeID,
		HasParentAlpenglowBlockID: true,
	}
	slot, blockID, err = alpenglowReplayParentAnchor(mithrilState, resume, opts)
	require.NoError(t, err)
	require.Equal(t, uint64(75), slot)
	require.Equal(t, resumeID, blockID)
}

func TestAlpenglowReplayParentAnchorFailsClosed(t *testing.T) {
	validID := solana.Hash{1}
	validState := &state.MithrilState{
		ManifestParentSlot:             50,
		ManifestParentAlpenglowBlockID: validID.String(),
	}

	_, _, err := alpenglowReplayParentAnchor(validState, nil, nil)
	require.Error(t, err)
	_, _, err = alpenglowReplayParentAnchor(validState, nil, &ConsensusOpts{Alpenglow: true})
	require.ErrorContains(t, err, "has no snapshot block ID")
	_, _, err = alpenglowReplayParentAnchor(
		validState,
		nil,
		&ConsensusOpts{
			Alpenglow:                         true,
			SnapshotParentAlpenglowBlockID:    solana.Hash{2},
			HasSnapshotParentAlpenglowBlockID: true,
		},
	)
	require.ErrorContains(t, err, "does not match")
	_, _, err = alpenglowReplayParentAnchor(
		validState,
		&ResumeState{ParentSlot: 75},
		&ConsensusOpts{Alpenglow: true},
	)
	require.ErrorContains(t, err, "has no Alpenglow block ID")
}
