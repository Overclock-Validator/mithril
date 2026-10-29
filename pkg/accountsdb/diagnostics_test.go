package accountsdb

import (
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CommitBatch's steps are disjoint, so they add up to no more than the call.
func TestCommitBatchReportsDisjointStepTimings(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	commitTestBatch(t, db, 110, foldAcct(1, 1, make([]byte, 4096)))
	started := time.Now()
	res := commitTestBatch(t, db, 120, foldAcct(1, 2, make([]byte, 4096)), foldAcct(2, 3, []byte("x")))
	wall := time.Since(started)
	tm := res.Timings
	sum := tm.LockWait + tm.Dedupe + tm.FileID + tm.SegmentWrite + tm.UndoCapture + tm.Manifest + tm.BankhashRows + tm.IndexCommit + tm.CacheRefresh
	assert.LessOrEqual(t, sum, wall)
	assert.Positive(t, tm.SegmentWrite, "the segment is written and fsynced")
	assert.Positive(t, tm.Manifest, "the manifest is written and fsynced")
	assert.Positive(t, tm.IndexCommit)
	assert.Contains(t, tm.String(), "segment_write=")
}

// Pebble flushes of the accounts index reach the diagnostics summary.
func TestPebbleFlushReachesDiagnostics(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	metrics.DiagnosticsSummary() // start a fresh window
	commitTestBatch(t, db, 110, foldAcct(1, 1, []byte("a")))
	require.NoError(t, db.Index.Flush())
	m := regexp.MustCompile(`task pebble\.index\.flush runs=(\d+)`).FindStringSubmatch(metrics.DiagnosticsSummary())
	require.NotNil(t, m)
	runs, _ := strconv.Atoi(m[1])
	assert.Positive(t, runs)
}
