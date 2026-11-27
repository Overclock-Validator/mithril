package accountsdb

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type scannedRecord struct {
	key      solana.PublicKey
	lamports uint64
}

func scanAll(t *testing.T, db *AccountsDb, cfg ScanRecordsConfig, during func()) ([]scannedRecord, ScanStats) {
	t.Helper()
	var mu sync.Mutex
	var once sync.Once
	var got []scannedRecord
	stats, err := db.ScanRecords(context.Background(), cfg, func(pubkey, _ [32]byte, lamports uint64, _ []byte) {
		if during != nil {
			once.Do(during)
		}
		mu.Lock()
		got = append(got, scannedRecord{solana.PublicKey(pubkey), lamports})
		mu.Unlock()
	})
	require.NoError(t, err)
	return got, stats
}

func TestScanRecordsVisitsEveryVersionOfSelectedPrograms(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	commitTestBatch(t, db, 10, foldAcct(1, 100, []byte{1}), foldAcct(2, 200, nil))
	commitTestBatch(t, db, 20, foldAcct(1, 150, []byte{2}))

	got, stats := scanAll(t, db, ScanRecordsConfig{Programs: [][32]byte{{1, 1}}}, nil)
	require.ElementsMatch(t, []scannedRecord{{solana.PublicKey{1}, 100}, {solana.PublicKey{1}, 150}}, got,
		"both versions of the selected program's account, nothing else")
	require.EqualValues(t, 3, stats.Records)

	got, _ = scanAll(t, db, ScanRecordsConfig{}, nil)
	require.Len(t, got, 3)
}

// Compaction moves live records into a new file named after the source slot
// and deletes the source. A scan that has not read the source yet must find
// the records in the new file.
func TestScanRecordsFollowsFilesCompactionReplaces(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	for i := byte(1); i <= 6; i++ {
		commitTestBatch(t, db, uint64(i)*10, foldAcct(i, uint64(i)*100, nil))
	}
	inv, err := db.scanCompactionInventory()
	require.NoError(t, err)
	moved := 0
	move := func() {
		// Rename every data file to a fresh file ID with the same slot,
		// as compaction's output would be named.
		for _, f := range inv.data {
			from := db.appendVecPath(f.slot, f.fileId)
			to := db.appendVecPath(f.slot, f.fileId+1000)
			if os.Rename(from, to) == nil {
				moved++
			}
		}
	}
	got, stats := scanAll(t, db, ScanRecordsConfig{Workers: 1}, move)
	require.Positive(t, moved)
	seen := map[solana.PublicKey]bool{}
	for _, r := range got {
		seen[r.key] = true
	}
	for i := byte(1); i <= 6; i++ {
		require.True(t, seen[solana.PublicKey{i}], "account %d missed", i)
	}
	require.GreaterOrEqual(t, stats.Passes, 2)
}

func TestScanRecordsIgnoresNewerFoldSegments(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	commitTestBatch(t, db, 10, foldAcct(1, 100, nil))
	got, _ := scanAll(t, db, ScanRecordsConfig{}, func() {
		// A fold that commits during the scan holds only newer writes.
		commitTestBatch(t, db, 20, foldAcct(2, 200, nil))
	})
	require.Equal(t, []scannedRecord{{solana.PublicKey{1}, 100}}, got)
}

func TestScanRecordsRespectsCancellation(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	commitTestBatch(t, db, 10, foldAcct(1, 100, nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := db.ScanRecords(ctx, ScanRecordsConfig{}, func(_, _ [32]byte, _ uint64, _ []byte) {})
	require.ErrorIs(t, err, context.Canceled)
}
