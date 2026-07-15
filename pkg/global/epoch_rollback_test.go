package global

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

func TestDropEpochVoteStateSnapshotsAfter(t *testing.T) {
	ClearEpochVoteStateSnapshots()
	t.Cleanup(ClearEpochVoteStateSnapshots)
	for epoch := uint64(4); epoch <= 6; epoch++ {
		PutEpochVoteStateSnapshot(epoch, map[solana.PublicKey]*sealevel.VoteStateVersions{
			{byte(epoch)}: {},
		})
	}

	DropEpochVoteStateSnapshotsAfter(5)

	if EpochVoteStateSnapshot(4) == nil || EpochVoteStateSnapshot(5) == nil {
		t.Fatal("selected epoch history was cleared")
	}
	if EpochVoteStateSnapshot(6) != nil {
		t.Fatal("future epoch snapshot survived rollback cleanup")
	}
}
