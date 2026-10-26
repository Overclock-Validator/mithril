package accountsdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListFoldAndParkedManifestsBatchedSelection(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 1500; i++ {
		f, err := os.CreateTemp(dir, "unrelated-")
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
	for _, m := range []*SegmentManifest{
		{Kind: ManifestKindFold, BatchSeq: 2, ThroughSlot: 20, FileId: 2},
		{Kind: ManifestKindFold, BatchSeq: 1, ThroughSlot: 10, FileId: 1},
		{Kind: ManifestKindCompact, ThroughSlot: 30, FileId: 3},
		{Kind: ManifestKindFold, BatchSeq: 3, ThroughSlot: 40, FileId: 4},
	} {
		require.NoError(t, WriteSegmentManifest(dir, m))
	}
	require.NoError(t, os.Rename(filepath.Join(dir, "40.4.manifest"), filepath.Join(dir, "40.4.manifest.rewound")))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.manifest"), []byte("invalid-header"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ignored.manifest.tmp"), nil, 0600))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "directory.manifest"), 0700))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "directory.manifest.rewound"), 0700))
	headers, parked, err := ListFoldAndParkedManifests(dir)
	require.NoError(t, err)
	require.Len(t, headers, 3)
	require.Equal(t, uint64(0), headers[0].BatchSeq)
	require.Equal(t, filepath.Join(dir, "bad.manifest"), headers[0].Path)
	require.Equal(t, uint64(1), headers[1].BatchSeq)
	require.Equal(t, uint64(2), headers[2].BatchSeq)
	require.Equal(t, []string{filepath.Join(dir, "40.4.manifest.rewound")}, parked)
	_, _, err = ListFoldAndParkedManifests(filepath.Join(dir, "missing"))
	require.ErrorIs(t, err, os.ErrNotExist)
}
