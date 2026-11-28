package replay

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Checkpoint reopening must preserve rooted coverage and duplicate detection.
func TestTransactionStatusCheckpointAfterRoot(t *testing.T) {
	for _, tc := range []struct {
		name    string
		seed    int
		through uint64
	}{
		{"imported_short", 201, 204},
		{"imported_observed_shape", 201, 298},
		{"imported_full_window", 201, 302},
		{"already_full_control", 300, 303},
		{"fresh_genesis", 0, 4},
		{"fresh_full_window", 0, 300},
		{"fresh_pruned_window", 0, 301},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewTransactionStatusCache()
			first := uint64(1)
			if tc.seed != 0 {
				roots := make([]txstatus.SnapshotSlotDelta, tc.seed)
				for i := range roots {
					roots[i] = txstatus.SnapshotSlotDelta{Slot: uint64(i), IsRoot: true}
				}
				var err error
				cache, err = NewTransactionStatusCacheFromAgaveSnapshot(roots, uint64(tc.seed-1))
				require.NoError(t, err)
				first = uint64(tc.seed)
			}
			require.True(t, cache.CoverageComplete())
			earliest := statusCacheTestTransaction(0x21, 0x31, 0x41)
			latest := statusCacheTestTransaction(0x22, 0x32, 0x42)
			for slot := first; slot <= tc.through; slot++ {
				block := statusCacheTestBlock(slot)
				if slot == first {
					block.Transactions = []*solana.Transaction{earliest}
				}
				if slot == tc.through {
					block.Transactions = []*solana.Transaction{latest}
				}
				require.NoError(t, cache.CommitBlock(block))
				if slot == tc.through-2 {
					require.False(t, cache.Root(slot), "already-complete coverage must not transition")
					require.False(t, cache.Root(slot), "same root must not double-count")
				}
			}
			snapshot, err := cache.CaptureSnapshotThrough(tc.through)
			require.NoError(t, err)
			// Subsequent root/prune must not mutate this captured checkpoint.
			require.False(t, cache.Root(tc.through))
			payload, err := snapshot.MarshalBinary()
			require.NoError(t, err)
			count := min(tc.through, uint64(maxTransactionStatusRoots))
			if tc.seed != 0 {
				count = min(tc.through+1, uint64(maxTransactionStatusRoots))
			}
			t.Logf("checkpoint encoded rooted=%d nodes=%d expected=%d", binary.LittleEndian.Uint16(payload[5:7]), binary.LittleEndian.Uint16(payload[7:9]), count)
			restored, err := NewTransactionStatusCacheFromSnapshot(payload)
			require.NoError(t, err, "a cache must restore its own acknowledged-root checkpoint")
			require.Equal(t, uint16(count), restored.rootedSinceSeed)
			require.Equal(t, tc.through, restored.RootedThrough())
			require.True(t, restored.CoverageComplete())
			require.Len(t, restored.nodesThroughLocked(tc.through), int(count))
			for _, tx := range []*solana.Transaction{earliest, latest} {
				retry := *tx
				retry.Signatures = []solana.Signature{{0xff}}
				next := statusCacheTestBlock(tc.through+1, &retry)
				if first < tc.through+1-count {
					// The fresh 301-bank case prunes the first of its banks.
					if tx == earliest {
						require.NoError(t, restored.ValidateBlock(next))
						continue
					}
				}
				var already *AncestorAlreadyProcessedTransactionMessagesError
				require.ErrorAs(t, restored.ValidateBlock(next), &already)
			}
		})
	}
}
