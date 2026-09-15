package alpenglow

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObserverPendingStatsMatchFullScan(t *testing.T) {
	// Exercise eviction, duplicate certificates, out-of-order and hashless
	// replay, and retained matches/mismatches with and without retention.
	for _, retention := range []int{0, 1, 8, 64} {
		t.Run(fmt.Sprint(retention), func(t *testing.T) {
			o := NewObserverWithConfig(ObserverConfig{MaxTrackedVotes: 8,
				MaxTrackedCertificates: retention, MaxTrackedReplayBlocks: retention})
			rng := rand.New(rand.NewSource(37))
			for i := 0; i < 1000; i++ {
				slot := uint64(rng.Intn(80) + 1)
				switch rng.Intn(5) {
				case 0, 1:
					typ := CertificateNotarize
					if i%3 == 0 {
						typ = CertificateFinalizeFast
					}
					_, err := o.ObserveCertificate(Certificate{Type: typ, Slot: slot,
						BlockHash: testHash(byte(slot%3 + 1)), IncludedStake: 80, TotalStake: 100})
					require.NoError(t, err)
				case 2:
					block := BlockID{Slot: slot}
					if i%5 != 0 {
						block.Hash = testHash(byte(slot%4 + 1))
					}
					o.ObserveReplayBlock(ReplayBlockObservation{Block: block})
				case 3:
					_, err := o.ObserveVote(VoteMessage{Vote: NewSkipVote(slot), Rank: 1})
					require.NoError(t, err)
				case 4:
					o.ObserveReplayResult(ReplayResultObservation{Slot: slot})
				}
				o.Snapshot()
				o.mu.RLock()
				cached, scanned := o.pendingStats, o.certificateReplayPendingStatsLocked()
				o.mu.RUnlock()
				require.Equal(t, scanned, cached, "operation %d", i)
			}
		})
	}
}

func TestObserverConcurrentSnapshotsAndReconciliation(t *testing.T) {
	o := NewObserver()
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for slot := uint64(1); slot <= 100; slot++ {
				switch worker {
				case 0:
					o.ObserveReplayBlock(ReplayBlockObservation{Block: BlockID{Slot: slot, Hash: testHash(1)}})
				case 1:
					_, err := o.ObserveCertificate(Certificate{Type: CertificateNotarize, Slot: slot,
						BlockHash: testHash(1), IncludedStake: 80, TotalStake: 100})
					if err != nil {
						t.Error(err)
					}
				case 2:
					_, err := o.ObserveVote(VoteMessage{Vote: NewSkipVote(slot), Rank: 1})
					if err != nil {
						t.Error(err)
					}
				case 3:
					o.Snapshot()
				}
			}
		}(worker)
	}
	wg.Wait()
	snapshot := o.Snapshot()
	require.Equal(t, uint64(100), snapshot.CertificateReplayMatches)
	require.Zero(t, snapshot.CertificateReplayPending)
}
