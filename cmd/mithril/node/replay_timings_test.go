package node

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/stretchr/testify/require"
)

type timingTestRow struct {
	Slot    int    `json:"slot"`
	Payload string `json:"payload"`
}

func readTimingTestRows(t *testing.T, filename string) []timingTestRow {
	t.Helper()
	f, err := os.Open(filename)
	require.NoError(t, err)
	defer f.Close()
	var reader io.Reader = f
	if strings.HasSuffix(filename, ".gz") {
		compressed, err := gzip.NewReader(f)
		require.NoError(t, err)
		defer compressed.Close()
		reader = compressed
	}
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.True(t, bytes.HasSuffix(data, []byte{'\n'}), "incomplete last row in %s", filename)
	var rows []timingTestRow
	for _, line := range bytes.Split(bytes.TrimSuffix(data, []byte{'\n'}), []byte{'\n'}) {
		var row timingTestRow
		require.NoError(t, json.Unmarshal(line, &row), "split JSON row in %s", filename)
		rows = append(rows, row)
	}
	return rows
}

func waitTimingArchives(t *testing.T, dir string, count int) []string {
	t.Helper()
	var archives []string
	require.Eventually(t, func() bool {
		var err error
		archives, err = filepath.Glob(filepath.Join(dir, "replay_timings-*.jsonl.gz"))
		if err != nil || len(archives) != count {
			return false
		}
		// Compression publishes the gzip before removing its source. Waiting
		// for those sources to disappear also waits for compression to finish.
		pending, err := filepath.Glob(filepath.Join(dir, "replay_timings-*.jsonl"))
		return err == nil && len(pending) == 0
	}, 5*time.Second, 10*time.Millisecond)
	return archives
}

func TestReplayTimingsRotationPreservesCompleteRows(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "replay_timings.jsonl")
	w, err := createReplayTimingsWriter(filename, mlog.LogConfig{MaxSizeMB: 1, MaxBackups: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	encoder := json.NewEncoder(w)
	const count = 1200
	payload := strings.Repeat("x", 1500)
	for slot := 0; slot < count; slot++ {
		require.NoError(t, encoder.Encode(timingTestRow{Slot: slot, Payload: payload}))
	}
	require.NoError(t, w.Close())
	files := append(waitTimingArchives(t, dir, 1), filename)
	var slots []int
	for _, name := range files {
		for _, row := range readTimingTestRows(t, name) {
			require.Equal(t, payload, row.Payload)
			slots = append(slots, row.Slot)
		}
	}
	require.Len(t, slots, count)
	for slot := range count {
		require.Equal(t, slot, slots[slot])
	}
	info, err := os.Stat(filename)
	require.NoError(t, err)
	require.LessOrEqual(t, info.Size(), int64(1<<20))
}

func TestReplayTimingsRetentionBoundsBackups(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "replay_timings.jsonl")
	w, err := createReplayTimingsWriter(filename, mlog.LogConfig{MaxSizeMB: 1, MaxBackups: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	encoder := json.NewEncoder(w)
	for slot := range 6 {
		require.NoError(t, encoder.Encode(timingTestRow{Slot: slot, Payload: strings.Repeat("x", 600<<10)}))
		// Lumberjack backup names use millisecond timestamps; keep successive
		// forced rotations distinct even on a fast temporary filesystem.
		time.Sleep(2 * time.Millisecond)
	}
	require.NoError(t, w.Close())
	files := append(waitTimingArchives(t, dir, 2), filename)
	var slots []int
	for _, name := range files {
		for _, row := range readTimingTestRows(t, name) {
			slots = append(slots, row.Slot)
		}
	}
	require.Equal(t, []int{3, 4, 5}, slots)
}

func TestReplayTimingsUnlimitedSizeAndAppend(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "replay_timings.jsonl")
	for slot := range 2 {
		w, err := createReplayTimingsWriter(filename, mlog.LogConfig{MaxSizeMB: 0, MaxBackups: 1})
		require.NoError(t, err)
		require.NoError(t, json.NewEncoder(w).Encode(timingTestRow{Slot: slot, Payload: strings.Repeat("x", 600<<10)}))
		require.NoError(t, w.Close())
	}
	rows := readTimingTestRows(t, filename)
	require.Len(t, rows, 2)
	require.Equal(t, 0, rows[0].Slot)
	require.Equal(t, 1, rows[1].Slot)
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, 1)
	info, err := os.Stat(filename)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(1<<20))
}

func TestReplayTimingsReportsOpenFailure(t *testing.T) {
	w, err := createReplayTimingsWriter(t.TempDir(), mlog.LogConfig{MaxSizeMB: 1})
	require.Error(t, err)
	require.Nil(t, w)
}
