package accountsdb

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// recordWriteback replaces the device call with a recorder for one test.
func recordWriteback(t *testing.T, fail error) *[][2]int64 {
	t.Helper()
	var calls [][2]int64
	prev := segmentWriteback
	segmentWriteback = func(_ *os.File, from, to int64) error {
		calls = append(calls, [2]int64{from, to})
		return fail
	}
	t.Cleanup(func() { segmentWriteback = prev })
	return &calls
}

func setFoldWriteback(t *testing.T, step int64) {
	t.Helper()
	prev := foldWritebackBytes
	foldWritebackBytes = step
	t.Cleanup(func() { foldWritebackBytes = prev })
}

func bigAccountData(b byte) []byte { return bytes.Repeat([]byte{b, b + 1, b + 2}, 700_001) } // about 2 MiB

func checkWritebackFoldAccounts(t *testing.T, db *AccountsDb) {
	t.Helper()
	keys := []solana.PublicKey{{1}, {2}, {3}, {4}}
	out, err := db.GetAccountsBatch(context.Background(), 100, keys)
	require.NoError(t, err)
	for i, want := range [][]byte{bigAccountData(1), bigAccountData(2), bigAccountData(3), []byte("small")} {
		require.True(t, bytes.Equal(want, out[i].Data), "account %d", i+1)
		require.EqualValues(t, (i+1)*11, out[i].Lamports)
	}
}

func commitWritebackFold(t *testing.T, db *AccountsDb) BatchCommitResult {
	t.Helper()
	res := commitTestBatch(t, db, 100, foldAcct(1, 11, bigAccountData(1)), foldAcct(2, 22, bigAccountData(2)), foldAcct(3, 33, bigAccountData(3)), foldAcct(4, 44, []byte("small")))
	require.EqualValues(t, 4, res.Keys)
	require.Equal(t, res.Bytes, res.Writeback.SegmentBytes)
	return res
}

// A fold written in bounded writeback steps, through the real device call,
// stores exactly what an ordinary fold stores: the accounts read back intact
// and survive a reopen. The result says what the stepping did; where the call
// is not implemented it says so and the fold still completes.
func TestFoldWithBoundedWriteback(t *testing.T) {
	setFoldWriteback(t, 1<<20)
	db, dir := newFoldTestDb(t)
	res := commitWritebackFold(t, db)
	require.Positive(t, res.Timings.SegmentSync)
	require.LessOrEqual(t, res.Timings.SegmentSync, res.Timings.SegmentWrite)
	require.Equal(t, "none", db.foldPhase.at(time.Now()).String())
	wb := res.Writeback
	require.EqualValues(t, 1<<20, wb.StepBytes)
	if runtime.GOOS == "linux" {
		require.Empty(t, wb.Err)
		require.GreaterOrEqual(t, wb.Calls, 3)
		require.Greater(t, wb.HandedBytes, int64(4<<20))
		require.LessOrEqual(t, wb.HandedBytes, wb.SegmentBytes)
		require.Contains(t, wb.String(), "writeback_step_mb=1 ")
		require.Contains(t, wb.String(), "writeback_err=none")
	} else {
		require.NotEmpty(t, wb.Err)
		require.Zero(t, wb.Calls)
		require.Zero(t, wb.HandedBytes)
	}

	checkWritebackFoldAccounts(t, db)
	db.InitCaches() // drop cached copies so the second read comes from the file
	checkWritebackFoldAccounts(t, db)
	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	checkWritebackFoldAccounts(t, db)
}

// A device call that fails ends the stepping for that segment after one
// attempt; the fold completes through its final fsync and reports the failure.
func TestFoldReportsWritebackFailure(t *testing.T) {
	setFoldWriteback(t, 1<<20)
	calls := recordWriteback(t, errors.New("no ranges here"))
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	res := commitWritebackFold(t, db)
	require.Len(t, *calls, 1)
	wb := res.Writeback
	require.EqualValues(t, 1<<20, wb.StepBytes)
	require.Zero(t, wb.Calls)
	require.Zero(t, wb.HandedBytes)
	require.Equal(t, "no ranges here", wb.Err)
	require.Contains(t, wb.String(), `writeback_calls=0 writeback_handed_bytes=0 `)
	require.Contains(t, wb.String(), `writeback_err="no ranges here"`)
	db.InitCaches()
	checkWritebackFoldAccounts(t, db)
}

// With the setting off the device call is never made and the report says so.
func TestFoldWithoutWriteback(t *testing.T) {
	setFoldWriteback(t, 0)
	calls := recordWriteback(t, nil)
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	res := commitWritebackFold(t, db)
	require.Empty(t, *calls)
	require.Equal(t, FoldWriteback{SegmentBytes: res.Bytes}, res.Writeback)
	require.Contains(t, res.Writeback.String(), "writeback_step_mb=0 writeback_calls=0 writeback_handed_bytes=0 ")
}

// The writer hands bytes over only at step boundaries, each call covering
// exactly the bytes written since the previous one, and leaves the bytes
// untouched.
func TestSegmentFileWriterSteps(t *testing.T) {
	calls := recordWriteback(t, nil)
	f, err := os.Create(filepath.Join(t.TempDir(), "seg"))
	require.NoError(t, err)
	defer f.Close()
	w := &segmentFileWriter{f: f, step: 4096}
	chunk := bytes.Repeat([]byte{7}, 1500)
	for i := 0; i < 10; i++ {
		n, err := w.Write(chunk)
		require.NoError(t, err)
		require.Equal(t, len(chunk), n)
		require.LessOrEqual(t, w.handed, w.written)
		require.Less(t, w.written-w.handed, w.step+int64(len(chunk)))
	}
	require.Equal(t, [][2]int64{{0, 4500}, {4500, 9000}, {9000, 13500}}, *calls)
	require.Equal(t, FoldWriteback{StepBytes: 4096, Calls: 3, HandedBytes: 13500, SegmentBytes: 15000}, w.report())
	require.NoError(t, f.Sync())
	got, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{7}, 15000), got)

	// Step 0 never hands anything over.
	*calls = nil
	w = &segmentFileWriter{f: f}
	_, err = w.Write(chunk)
	require.NoError(t, err)
	require.Empty(t, *calls)
	require.Zero(t, w.handed)
}

// The real device call succeeds on a regular file where it is implemented.
func TestBoundedWritebackOnRegularFile(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("sync_file_range is Linux only")
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "seg"))
	require.NoError(t, err)
	defer f.Close()
	_, err = f.Write(make([]byte, 1<<20))
	require.NoError(t, err)
	require.NoError(t, boundedWriteback(f, 0, 512<<10))
	require.NoError(t, boundedWriteback(f, 512<<10, 1<<20))
}

func TestFoldPhaseSnapshot(t *testing.T) {
	var p foldPhase
	require.Equal(t, "none", p.at(time.Now()).String())
	p.set(foldPhaseSegmentSync)
	snap := p.at(time.Now().Add(35 * time.Millisecond))
	require.Regexp(t, `^segment_sync\+3[5-9]ms$`, snap.String())
	// A snapshot keeps the phase and start time it was taken with.
	p.set(foldPhaseIndexCommit)
	require.Regexp(t, `^segment_sync\+3[5-9]ms$`, snap.String())
	require.Regexp(t, `^index_commit\+`, p.at(time.Now()).String())
	// A clock read taken just before the phase began does not go negative.
	require.Equal(t, "index_commit+0s", p.at(time.Now().Add(-time.Second)).String())
	p.set(foldPhaseIdle)
	require.Equal(t, "none", p.at(time.Now()).String())
}

// Phase and start time travel in one word, so readers racing a writer always
// decode a known phase and a start time that has already passed.
func TestFoldPhaseConcurrentReads(t *testing.T) {
	var p foldPhase
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for phase := uint64(0); ; phase = (phase + 1) % uint64(len(foldPhaseNames)) {
			select {
			case <-stop:
				return
			default:
				p.set(phase)
			}
		}
	}()
	for i := 0; i < 200_000; i++ {
		snap := p.at(time.Now())
		phase := snap.packed >> foldPhaseSinceBits
		require.Less(t, phase, uint64(len(foldPhaseNames)))
		began := time.Duration(snap.packed & (1<<foldPhaseSinceBits - 1))
		require.LessOrEqual(t, began, time.Since(foldPhaseBase))
	}
	close(stop)
	<-done
}
