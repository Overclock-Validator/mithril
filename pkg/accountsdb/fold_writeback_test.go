package accountsdb

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// A fold written in bounded writeback steps stores exactly what an ordinary
// fold stores: the accounts read back intact and survive a reopen.
func TestFoldWithBoundedWriteback(t *testing.T) {
	prev := foldWritebackBytes
	foldWritebackBytes = 1 << 20
	defer func() { foldWritebackBytes = prev }()

	db, dir := newFoldTestDb(t)
	big := func(b byte) []byte { return bytes.Repeat([]byte{b, b + 1, b + 2}, 700_001) } // about 2 MiB each
	res := commitTestBatch(t, db, 100, foldAcct(1, 11, big(1)), foldAcct(2, 22, big(2)), foldAcct(3, 33, big(3)), foldAcct(4, 44, []byte("small")))
	require.EqualValues(t, 4, res.Keys)
	require.Positive(t, res.Timings.SegmentSync)
	require.LessOrEqual(t, res.Timings.SegmentSync, res.Timings.SegmentWrite)
	require.Equal(t, foldPhaseNames[foldPhaseIdle], db.foldPhase.describe())

	check := func(db *AccountsDb) {
		keys := []solana.PublicKey{{1}, {2}, {3}, {4}}
		out, err := db.GetAccountsBatch(context.Background(), 100, keys)
		require.NoError(t, err)
		for i, want := range [][]byte{big(1), big(2), big(3), []byte("small")} {
			require.True(t, bytes.Equal(want, out[i].Data), "account %d", i+1)
			require.EqualValues(t, (i+1)*11, out[i].Lamports)
		}
	}
	check(db)
	db.InitCaches() // drop cached copies so the second read comes from the file
	check(db)
	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	check(db)
}

// The writer hands bytes over only at step boundaries, tracks what it handed
// over, and leaves the bytes untouched.
func TestSegmentFileWriterSteps(t *testing.T) {
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
	require.NoError(t, w.err)
	require.EqualValues(t, 15000, w.written)
	require.EqualValues(t, 13500, w.handed) // steps completed at 4500, 9000 and 13500 bytes
	require.NoError(t, f.Sync())
	got, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{7}, 15000), got)

	// Step 0 never hands anything over.
	w = &segmentFileWriter{f: f}
	_, err = w.Write(chunk)
	require.NoError(t, err)
	require.Zero(t, w.handed)
}

func TestFoldPhaseDescribe(t *testing.T) {
	var p foldPhase
	require.Equal(t, "none", p.describe())
	p.set(foldPhaseSegmentSync)
	require.Regexp(t, `^segment_sync\+\d+(\.\d+)?(ms|s|µs|ns)?$|^segment_sync\+0s$`, p.describe())
	p.set(foldPhaseIdle)
	require.Equal(t, "none", p.describe())
}
