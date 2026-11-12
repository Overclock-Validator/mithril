//go:build linux

package accountsdb

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSparseCompatibilityNoticeAfterPublicationOncePerOpen(t *testing.T) {
	db, dir := newFoldTestDb(t)
	t.Cleanup(func() { db.CloseDb() })
	big := bytes.Repeat([]byte{0x5a}, 8<<20)
	first := commitTestBatch(t, db, 110, foldAcct(1, 10, big), foldAcct(2, 20, []byte("keep first")))
	second := commitTestBatch(t, db, 120, foldAcct(3, 30, big), foldAcct(4, 40, []byte("keep second")))
	commitTestBatch(t, db, 130, foldAcct(1, 11, nil), foldAcct(3, 31, nil))
	commitTestBatch(t, db, 140, foldAcct(1, 12, nil), foldAcct(3, 32, nil))
	commitTestBatch(t, db, 150, foldAcct(1, 13, nil), foldAcct(3, 33, nil))
	paths := []string{
		segmentManifestPath(db.AcctsDir, 110, first.FileId),
		segmentManifestPath(db.AcctsDir, 120, second.FileId),
	}
	warnings := 0
	db.compactHooks.afterSparseCompatibilityWarning = func() {
		warnings++
		// The warning must describe an already-published sparse marker, not
		// merely an attempted conversion. A retry may publish either source first.
		sparse := 0
		for _, path := range paths {
			m, err := ReadSegmentManifest(path)
			require.NoError(t, err)
			if m.Kind == ManifestKindSparse {
				sparse++
			}
		}
		require.Positive(t, sparse)
	}

	// Fail the marker write before any conversion, regardless of candidate order.
	// The old manifests and file contents must remain intact and unannounced.
	for _, path := range paths {
		require.NoError(t, os.Mkdir(path+".tmp", 0o700))
	}
	cfg := CompactionConfig{RewindHorizonBatches: 1, MaxMoveBytesPerCycle: 1}
	_, err := db.CompactOnce(cfg)
	require.ErrorContains(t, err, "create manifest tmp")
	require.Zero(t, warnings)
	for _, path := range paths {
		m, err := ReadSegmentManifest(path)
		require.NoError(t, err)
		require.Equal(t, ManifestKindFold, m.Kind)
		require.NoError(t, os.Remove(path+".tmp"))
	}

	stats, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Equal(t, 2, stats.FilesSparsified)
	require.Equal(t, 1, warnings, "two converted files need only one store warning")
	_, err = db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Equal(t, 1, warnings, "later compaction cycles must not repeat the warning")

	// A new process/open must still tell its operator about an earlier conversion.
	db = reopenFoldTestDb(t, db, dir)
	_, err = db.RecoverFoldState()
	require.NoError(t, err)
	db.compactHooks.afterSparseCompatibilityWarning = func() { warnings++ }
	for range 2 {
		_, err = db.CompactOnce(cfg)
		require.NoError(t, err)
	}
	require.Equal(t, 2, warnings, "existing sparse markers should warn once on the new open")
}
