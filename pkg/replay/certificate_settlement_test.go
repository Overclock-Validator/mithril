package replay

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func settlementFixture(t *testing.T, tail *unrootedTail, slots ...uint64) (*CertificateOutcomes, map[uint64]solana.Hash) {
	t.Helper()
	var subject solana.PublicKey
	subject[0] = 1
	r := NewCertificateOutcomes(subject)
	ids := make(map[uint64]solana.Hash, len(slots))
	for _, slot := range slots {
		c := outcomeCarrier(slot, byte(slot+20), subject)
		c.Bankhash = solana.Hash(tail.bankhashes[slot])
		require.True(t, r.Observe(c))
		ids[slot] = c.BlockID
	}
	return r, ids
}

func TestCertificateSettlementFlush(t *testing.T) {
	for _, tc := range []struct {
		name               string
		fail               uint64
		committed, pending uint64
	}{
		{"success", 0, 4, 0},
		{"later failure", 17, 2, 2},
		{"first failure", 15, 0, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := &fakeCommitter{durable: accounts.NewMemAccounts(), failOn: tc.fail}
			tail := asyncTestTail(fc, 15, 16, 17, 18)
			r, ids := settlementFixture(t, tail, 15, 16, 17, 18)
			before := captureCertificateFlush(tail)
			through, ctx, err := tail.flush(18)
			if tc.fail == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if through > 0 && ctx != nil {
				r.settleFlush(before, tail, ids, through, time.Unix(100, 0))
			}
			s := r.Snapshot()
			require.Equal(t, tc.committed, s.RewardIncluded)
			require.Equal(t, tc.committed, s.Carriers)
			require.Equal(t, tc.pending, s.Pending)
			require.Len(t, fc.committed, int(tc.committed))
		})
	}
}

func TestCertificateSettlementAsyncMatchesFlush(t *testing.T) {
	at := time.Unix(100, 0)
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := asyncTestTail(fc, 15, 16, 17)
	r, ids := settlementFixture(t, tail, 15, 16, 17)
	job, err := tail.buildFoldJob(17, false)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.NoError(t, runFoldJob(fc, job))
	require.Zero(t, r.Snapshot().Carriers, "durable write alone is not an applied fold")
	require.NotNil(t, tail.applyFoldJob(job))
	r.settleFold(job.bankhashes, ids, job.slotCount, at)

	syncTail := asyncTestTail(&fakeCommitter{durable: accounts.NewMemAccounts()}, 15, 16, 17)
	syncOutcomes, syncIDs := settlementFixture(t, syncTail, 15, 16, 17)
	before := captureCertificateFlush(syncTail)
	through, ctx, err := syncTail.flush(16)
	require.NoError(t, err)
	require.NotNil(t, ctx)
	syncOutcomes.settleFlush(before, syncTail, syncIDs, through, at)
	require.Equal(t, syncOutcomes.Snapshot(), r.Snapshot())
}

func TestCertificateSettlementMissingEvidenceAndLimit(t *testing.T) {
	at := time.Unix(100, 0)
	t.Run("missing ID is unknown", func(t *testing.T) {
		tail := asyncTestTail(&fakeCommitter{durable: accounts.NewMemAccounts()}, 15, 16)
		r, ids := settlementFixture(t, tail, 15, 16)
		delete(ids, 16)
		before := captureCertificateFlush(tail)
		through, _, err := tail.flush(16)
		require.NoError(t, err)
		r.settleFlush(before, tail, ids, through, at)
		s := r.Snapshot()
		require.Equal(t, uint64(1), s.RewardIncluded)
		require.Equal(t, uint64(1), s.RewardUnknown)
		require.Equal(t, uint64(1), s.Gaps)
	})
	t.Run("balanced missing and extra hashes", func(t *testing.T) {
		tail := asyncTestTail(&fakeCommitter{durable: accounts.NewMemAccounts()}, 15, 16)
		r, ids := settlementFixture(t, tail, 15, 16)
		delete(tail.bankhashes, 15)
		tail.bankhashes[100] = testHash(100)
		before := captureCertificateFlush(tail)
		require.True(t, before.valid)
		through, ctx, err := tail.flush(16)
		require.NoError(t, err)
		require.NotNil(t, ctx)
		r.settleFlush(before, tail, ids, through, at)
		s := r.Snapshot()
		require.True(t, s.CoverageIncomplete)
		require.Equal(t, uint64(1), s.Dropped)
		require.Zero(t, s.Pending)
		require.Zero(t, s.Carriers)
	})
	for _, name := range []string{"missing bankhash", "over limit"} {
		t.Run(name, func(t *testing.T) {
			fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
			var slots []uint64
			n := 2
			if name == "over limit" {
				n = certificatePendingLimit + 1
			}
			for i := 0; i < n; i++ {
				slots = append(slots, uint64(i+10))
			}
			tail := asyncTestTail(fc, slots...)
			r, ids := settlementFixture(t, tail, slots[:2]...)
			if name == "missing bankhash" {
				delete(tail.bankhashes, slots[0])
			}
			before := captureCertificateFlush(tail)
			require.False(t, before.valid)
			through, ctx, err := tail.flush(slots[len(slots)-1])
			require.NoError(t, err)
			require.NotNil(t, ctx)
			r.settleFlush(before, tail, ids, through, at)
			s := r.Snapshot()
			require.True(t, s.CoverageIncomplete)
			require.Equal(t, uint64(1), s.Dropped)
			require.Zero(t, s.Pending)
			require.Zero(t, s.Carriers)
		})
	}
}

func TestCertificateSettlementRewindAndProgressCopy(t *testing.T) {
	tail := asyncTestTail(&fakeCommitter{durable: accounts.NewMemAccounts()}, 15, 16)
	r, ids := settlementFixture(t, tail, 15, 16)
	r.RewindFrom(16)
	tail.unwind(16)
	tail.Add(16, nil, testHashBytes(60))
	ids[16] = outcomeHash(80)
	replacement := outcomeCarrier(16, 80, r.subject)
	replacement.Bankhash = solana.Hash(tail.bankhashes[16])
	require.True(t, r.Observe(replacement))
	r.settleFold(tail.bankhashes, ids, 2, time.Unix(100, 0))
	require.Equal(t, uint64(2), r.Snapshot().RewardIncluded)
	require.Zero(t, r.Snapshot().Gaps)
	var progress Progress
	progress.begin(5, nil)
	progress.certificateOutcomes(r.Snapshot())
	old, ok := progress.Snapshot()
	require.True(t, ok)
	r.loseCoverage(time.Unix(101, 0))
	progress.certificateOutcomes(r.Snapshot())
	require.False(t, old.CertificateOutcomes.CoverageIncomplete)
	progress.begin(5, nil)
	current, ok := progress.Snapshot()
	require.True(t, ok)
	require.Equal(t, CertificateOutcomeSummary{}, current.CertificateOutcomes)
	require.Equal(t, uint64(2), old.CertificateOutcomes.RewardIncluded)
}
