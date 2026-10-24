package accountsdb

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompactionPinsIndependentOfDirectoryOrder(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	r1 := commitTestBatch(t, db, 110, foldAcct(1, 10, nil), foldAcct(2, 20, nil))
	r2 := commitTestBatch(t, db, 120, foldAcct(1, 11, nil))
	r3 := commitTestBatch(t, db, 130, foldAcct(2, 21, nil))
	// Interrupted rewind: parked suffixes must pin their own segments and
	// their undo targets regardless of where readdir places them.
	parked := segmentManifestPath(db.AcctsDir, 130, r3.FileId) + ".rewound"
	require.NoError(t, os.Rename(segmentManifestPath(db.AcctsDir, 130, r3.FileId), parked))
	entries, err := readCompactionDirectory(db.AcctsDir)
	require.NoError(t, err)
	wantPins, wantHeaders, err := db.compactionPinSet(2, 1, entries)
	require.NoError(t, err)
	require.Contains(t, wantPins.whole, r1.FileId)
	require.Contains(t, wantPins.whole, r2.FileId)
	require.Contains(t, wantPins.whole, r3.FileId)
	require.NotContains(t, wantHeaders, r3.FileId)
	for i := range entries {
		rotated := append(slices.Clone(entries[i:]), entries[:i]...)
		slices.Reverse(rotated)
		pins, headers, err := db.compactionPinSet(2, 1, rotated)
		require.NoError(t, err)
		require.Equal(t, wantPins, pins)
		require.Equal(t, wantHeaders, headers)
	}
	data, err := os.ReadFile(parked)
	require.NoError(t, err)
	data[len(data)-1] ^= 1
	require.NoError(t, os.WriteFile(parked, data, 0600))
	_, _, err = db.compactionPinSet(2, 1, entries)
	require.ErrorContains(t, err, "cannot establish rewind pins")
	slices.Reverse(entries)
	_, _, err = db.compactionPinSet(2, 1, entries)
	require.ErrorContains(t, err, "cannot establish rewind pins")
}

func BenchmarkCompactionDirectory(b *testing.B) {
	for _, count := range []int{5000, 50000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			db, _ := newFoldTestDb(b)
			defer db.CloseDb()
			// Uncommitted/orphan data files are discovered but never reclaimed.
			// This isolates directory/pin/candidate bookkeeping from account I/O.
			for i := range count {
				require.NoError(b, os.WriteFile(filepath.Join(db.AcctsDir, SegmentDataName(100, uint64(i+1000))), nil, 0600))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stats, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 64})
				if err != nil || stats.CandidatesScanned != 0 {
					b.Fatalf("unexpected reclamation: %+v, %v", stats, err)
				}
			}
		})
	}
}
