package accountsdb

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// Run through a local Go test overlay. All database writes go to t.TempDir.
func TestCapturedMainnetAppendMatchesFullMap(t *testing.T) {
	fixture := os.Getenv("MITHRIL_PIN_MANIFEST_FIXTURE")
	if fixture == "" {
		t.Skip("set MITHRIL_PIN_MANIFEST_FIXTURE to the six matching manifests")
	}
	files, err := filepath.Glob(filepath.Join(fixture, "*.manifest"))
	require.NoError(t, err)
	require.Len(t, files, 6)
	sort.Strings(files)
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	var previous uint64
	for i, path := range files {
		manifest, err := ReadSegmentManifest(path)
		require.NoError(t, err)
		meta := foldMeta{BatchSeq: manifest.BatchSeq, ThroughSlot: manifest.ThroughSlot, FileId: manifest.FileId}
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(db.AcctsDir, filepath.Base(path)), data, 0600))
		if i == 0 {
			require.NoError(t, db.Index.Set(metaKeyCompactionHistoryFloor, encodeCompactionHistoryFloor(meta), pebble.Sync))
		} else {
			require.Equal(t, previous+1, meta.BatchSeq)
		}
		previous = meta.BatchSeq
		require.NoError(t, db.Index.Set(metaKeyLastBatch, encodeFoldMeta(meta), pebble.Sync))
		inv, err := db.scanCompactionInventory()
		require.NoError(t, err)
		var measured RewindScanStats
		actual, headers, err := db.compactionPinSet(meta, 2048, inv, &measured)
		require.NoError(t, err)
		if i > 0 {
			require.Equal(t, "append", measured.Path)
			require.EqualValues(t, 1, measured.DecodedManifests)
			require.EqualValues(t, len(manifest.Records), measured.DecodedRecords)
		}
		if i == 0 {
			require.Equal(t, "full", measured.Path)
		}
		cached := db.compactPinCache
		db.compactPinCache = nil
		reference, referenceHeaders, err := db.compactionPinSet(meta, 2048, inv, &RewindScanStats{})
		require.NoError(t, err)
		require.True(t, reflect.DeepEqual(reference.whole, actual.whole), "whole-file pins at %d", meta.ThroughSlot)
		require.True(t, reflect.DeepEqual(reference.records, actual.records), "all record offsets and keys at %d", meta.ThroughSlot)
		require.True(t, reflect.DeepEqual(referenceHeaders, headers), "all manifest headers at %d", meta.ThroughSlot)
		db.compactPinCache = cached
		t.Logf("slot=%d seq=%d mode=%s manifest_records=%d maps_equal=true", meta.ThroughSlot, meta.BatchSeq, measured.Path, len(manifest.Records))
	}
}
